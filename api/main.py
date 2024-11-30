from fastapi import FastAPI, HTTPException, Body
import logging
import asyncio
from contextlib import suppress
from typing import Dict, Optional
from api.plugins.plugin_loader import PluginLoader
from api.plugins.base_plugin import BasePlugin
from api.config.nocodb import (
    NOCODB_BASE_URL,
    NOCODB_TOKEN,
    NOCODB_PROJECT_ID,
    NOCODB_HISTORY_FILTERS_TABLE_ID,
    NOCODB_HISTORY_FILTERS_VIEW_ID,
    NOCODB_INTERACTED_USERS_TABLE_ID,
    NOCODB_INTERACTED_USERS_VIEW_ID
)
import os
import sys
from datetime import datetime, timedelta
from api.history import HistoryManager, Interaction
from pydantic import BaseModel
import psutil
import json
import yaml
from typing import Any, Dict

app = FastAPI()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs', 'api.log'))
    ]
)
logger = logging.getLogger(__name__)

# Initialize plugin loader
plugin_loader = PluginLoader()

# Track active tasks
active_tasks = set()

# Track sync plugin
sync_plugin = None

# Track active sessions and their status
active_sessions: Dict[str, dict] = {}

# Session timeout in minutes
SESSION_TIMEOUT_MINUTES = 120

def register_task(task):
    """Register an active task"""
    active_tasks.add(task)
    task.add_done_callback(active_tasks.discard)

async def cleanup_tasks():
    """Cleanup all active tasks"""
    tasks = list(active_tasks)
    if not tasks:
        return
        
    logger.info(f"Cleaning up {len(tasks)} active tasks...")
    for task in tasks:
        if not task.done():
            task.cancel()
            
    # Wait for all tasks to complete with a timeout
    with suppress(asyncio.TimeoutError, asyncio.CancelledError):
        await asyncio.wait(tasks, timeout=5.0)

class SessionRequest(BaseModel):
    account: str

class SessionStatus(BaseModel):
    account: str
    status: str
    start_time: Optional[datetime]
    last_interaction: Optional[datetime]
    total_interactions: int = 0
    errors: Optional[str] = None
    process_info: Optional[dict] = None
    memory_usage_mb: Optional[float] = None
    cpu_percent: Optional[float] = None
    uptime_minutes: Optional[float] = None
    is_responsive: bool = True

class BotStats(BaseModel):
    account: str
    total_interactions_24h: int = 0
    successful_interactions_24h: int = 0
    failed_interactions_24h: int = 0
    success_rate_24h: float = 0.0
    average_response_time_ms: float = 0.0
    uptime_hours: float = 0.0
    total_sessions: int = 0
    current_session_duration: Optional[float] = None
    memory_usage_trend: list = []
    cpu_usage_trend: list = []
    error_count_24h: int = 0

class InteractionLimits(BaseModel):
    """Model for interaction limits"""
    account: str
    likes_limit: int
    follow_limit: int
    unfollow_limit: int
    comments_limit: int
    pm_limit: int
    watch_limit: int
    success_limit: int
    total_limit: int
    scraped_limit: int
    crashes_limit: int
    time_delta_session: int

class AccountInfo(BaseModel):
    """Model for account information"""
    username: str
    total_posts: int = 0
    total_followers: int = 0
    total_following: int = 0
    last_session_time: Optional[datetime] = None
    is_active: bool = False
    config_exists: bool = True

class AccountConfig(BaseModel):
    """Model for account configuration"""
    username: str
    app_id: str = "com.instagram.android"
    use_cloned_app: bool = False
    allow_untested_ig_version: bool = False
    screen_sleep: bool = True
    screen_record: bool = False
    speed_multiplier: float = 1.0
    debug: bool = True
    close_apps: bool = False
    kill_atx_agent: bool = False
    restart_atx_agent: bool = False
    disable_block_detection: bool = False
    disable_filters: bool = False
    dont_type: bool = False
    use_nocodb: bool = True
    init_db: bool = True
    total_crashes_limit: int = 5
    count_app_crashes: bool = False
    shuffle_jobs: bool = True
    truncate_sources: str = "2-5"
    # Action configurations
    blogger_followers: list[str] = []
    watch_video_time: str = "15-35"
    watch_photo_time: str = "3-4"
    delete_interacted_users: bool = True
    # Optional fields
    device: Optional[str] = None
    scrape_to_file: Optional[str] = None
    can_reinteract_after: Optional[int] = None
    feed: Optional[str] = None
    unfollow: Optional[str] = None
    unfollow_any: Optional[str] = None
    unfollow_non_followers: Optional[str] = None

    class Config:
        alias_generator = lambda string: string.replace('_', '-')
        allow_population_by_field_name = True

class UpdateAccountConfig(BaseModel):
    """Model for account configuration updates"""
    config: Dict[str, Any]

class ConfigEntry(BaseModel):
    """Model for a single configuration entry"""
    key: str
    value: Any

class ArrayConfigEntry(BaseModel):
    """Model for array configuration entry"""
    key: str
    item: Any

async def check_session_timeout():
    """Background task to check for session timeouts"""
    while True:
        try:
            current_time = datetime.now()
            for account, session in active_sessions.items():
                if session['status'] != 'running':
                    continue
                
                # Check last interaction time
                last_interaction = session.get('last_interaction', session.get('start_time'))
                if last_interaction:
                    idle_time = current_time - last_interaction
                    if idle_time > timedelta(minutes=SESSION_TIMEOUT_MINUTES):
                        logger.warning(f"Session timeout for account {account} after {idle_time}")
                        # Stop the session
                        await stop_session(SessionRequest(account=account))
            
            # Check every minute
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Error in session timeout checker: {str(e)}")
            await asyncio.sleep(60)

@app.on_event("startup")
async def startup_event():
    """Initialize API on startup"""
    try:
        # Create logs directory if it doesn't exist
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        logs_dir = os.path.join(base_dir, 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        
        # Initialize plugins (temporarily disabled)
        # await plugin_loader.load_plugins()
        
        # Start session timeout checker
        asyncio.create_task(check_session_timeout())
        
        logger.info("API startup complete")
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup plugins and resources on shutdown"""
    await cleanup_tasks()
    logger.info("All tasks cleaned up.")

@app.get("/")
async def root():
    return {"message": "Welcome to the Instagram Bot API"}

@app.post("/save_history_filters")
async def save_history_filters(account: str, filters: Dict):
    """Save history filters for an account"""
    # Implementation for saving history filters
    pass

@app.get("/get_history_filters")
async def get_history_filters(account: str):
    """Get history filters for an account"""
    # Implementation for getting history filters
    pass

@app.post("/save_interaction")
async def save_interaction(account: str, username: str, interaction_type: str):
    """Save an interaction with a user"""
    # Implementation for saving interaction
    pass

@app.get("/get_interactions")
async def get_interactions(account: str, interaction_type: Optional[str] = None):
    """Get interactions for an account"""
    # Implementation for getting interactions
    pass

@app.delete("/clear_history")
async def clear_history(account: str, history_type: Optional[str] = None):
    """Clear history for an account"""
    # Implementation for clearing history
    pass

@app.post("/start_session")
async def start_session(request: SessionRequest):
    """Start a bot session for the specified account"""
    try:
        # Get absolute paths
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_dir, "accounts", request.account, "config.yml")
        python_path = os.path.join(base_dir, "venv", "Scripts", "python.exe")
        run_script = os.path.join(base_dir, "run.py")
        
        logger.info(f"Starting session for account {request.account} with config {config_path}")
        
        if not os.path.exists(config_path):
            error_msg = f"Configuration not found for account: {request.account} at path: {config_path}"
            logger.error(error_msg)
            raise HTTPException(status_code=404, detail=error_msg)
            
        # Create a background task to run the bot
        cmd = [python_path, "-v", run_script, "--config", config_path, "--use-nocodb", "--debug"]
        logger.info(f"Running command: {' '.join(cmd)}")
        logger.info(f"Working directory: {base_dir}")
        logger.info(f"Environment PYTHONPATH: {os.environ.get('PYTHONPATH')}")
        
        try:
            import subprocess
            import threading
            
            # Set up environment variables
            env = os.environ.copy()
            project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            env["PYTHONPATH"] = project_dir
            
            def run_process_in_thread():
                try:
                    logger.info(f"Starting process with command: {' '.join(cmd)}")
                    logger.info(f"Working directory: {base_dir}")
                    logger.info(f"PYTHONPATH: {env['PYTHONPATH']}")
                    
                    # Create log file for the bot
                    log_file = os.path.join(project_dir, "logs", f"{request.account}.log")
                    os.makedirs(os.path.dirname(log_file), exist_ok=True)
                    
                    process = subprocess.Popen(
                        cmd,
                        cwd=base_dir,
                        env=env,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        bufsize=1,
                        universal_newlines=True
                    )
                    
                    def log_output(pipe, prefix, log_file):
                        with open(log_file, 'a', encoding='utf-8', buffering=1) as f:
                            try:
                                for line in pipe:
                                    line = line.strip()
                                    if line:
                                        logger.info(f"{prefix}: {line}")
                                        f.write(f"{prefix}: {line}\n")
                                        f.flush()
                            except Exception as e:
                                logger.error(f"Error in log_output thread: {str(e)}", exc_info=True)
                    
                    # Start threads to continuously read and log output
                    stdout_thread = threading.Thread(target=log_output, args=(process.stdout, "STDOUT", log_file))
                    stderr_thread = threading.Thread(target=log_output, args=(process.stderr, "STDERR", log_file))
                    stdout_thread.daemon = True
                    stderr_thread.daemon = True
                    stdout_thread.start()
                    stderr_thread.start()
                    
                    logger.info(f"Process started with PID: {process.pid}")
                    return process
                    
                except Exception as e:
                    logger.error(f"Failed to start process: {str(e)}", exc_info=True)
                    return None

            # Start the process in a separate thread
            process = run_process_in_thread()
            if process is None:
                raise HTTPException(status_code=500, detail="Failed to start bot process")

            logger.info(f"Successfully started session for account: {request.account}")
            active_sessions[request.account] = {
                'status': 'running',
                'start_time': datetime.now(),
                'process': process
            }
            return {"message": f"Started session for account: {request.account}", "status": "running", "pid": process.pid}
            
        except Exception as e:
            error_msg = f"Failed to start process: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise HTTPException(status_code=500, detail=error_msg)
            
    except Exception as e:
        error_msg = f"Error starting session: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)

@app.post("/stop_session")
async def stop_session(request: SessionRequest):
    """Stop a running bot session for the specified account"""
    account = request.account
    logger.info(f"Attempting to stop session for account: {account}")
    
    if account not in active_sessions:
        raise HTTPException(status_code=404, detail="No active session found for this account")
    
    try:
        session = active_sessions[account]
        
        # Terminate the process if it exists
        if 'process' in session and session['process']:
            process = session['process']
            try:
                # Try to terminate the main process
                if process.pid:
                    parent = psutil.Process(process.pid)
                    children = parent.children(recursive=True)
                    for child in children:
                        child.terminate()
                    parent.terminate()
                    
                    # Wait for processes to terminate
                    gone, alive = psutil.wait_procs([parent] + children, timeout=3)
                    
                    # Force kill if still alive
                    for p in alive:
                        p.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired) as e:
                logger.warning(f"Process termination warning for {account}: {str(e)}")
        
        # Cancel any running tasks for this session
        if 'task' in session:
            session['task'].cancel()
            with suppress(asyncio.CancelledError):
                await session['task']
        
        # Clean up session resources
        if 'plugin' in session:
            await session['plugin'].cleanup()
        
        # Update session status
        session['status'] = 'stopped'
        session['end_time'] = datetime.now()
        session['process'] = None
        
        logger.info(f"Successfully stopped session for account: {account}")
        return {"status": "success", "message": f"Session stopped for account {account}"}
    
    except Exception as e:
        logger.error(f"Error stopping session for account {account}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stop session: {str(e)}"
        )

def get_process_info(pid: int) -> dict:
    """Get detailed process information"""
    try:
        process = psutil.Process(pid)
        with process.oneshot():
            memory_info = process.memory_info()
            cpu_percent = process.cpu_percent(interval=0.1)
            create_time = datetime.fromtimestamp(process.create_time())
            children = process.children(recursive=True)
            
            child_info = []
            total_child_memory = 0
            for child in children:
                try:
                    with child.oneshot():
                        child_memory = child.memory_info().rss / 1024 / 1024  # MB
                        total_child_memory += child_memory
                        child_info.append({
                            'pid': child.pid,
                            'memory_mb': round(child_memory, 2),
                            'cpu_percent': child.cpu_percent(interval=0.1)
                        })
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            return {
                'main_process': {
                    'pid': process.pid,
                    'memory_mb': round(memory_info.rss / 1024 / 1024, 2),  # Convert to MB
                    'cpu_percent': cpu_percent,
                    'create_time': create_time,
                    'status': process.status()
                },
                'child_processes': child_info,
                'total_memory_mb': round(memory_info.rss / 1024 / 1024 + total_child_memory, 2),
                'total_processes': len(children) + 1
            }
    except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
        logger.warning(f"Could not get process info for PID {pid}: {str(e)}")
        return None

@app.get("/session_status")
async def get_session_status(account: str):
    """Get the current status of a bot session with detailed process information"""
    logger.info(f"Retrieving detailed session status for account: {account}")
    
    if account not in active_sessions:
        return SessionStatus(
            account=account,
            status="inactive",
            start_time=None,
            last_interaction=None,
            total_interactions=0,
            is_responsive=False
        )
    
    session = active_sessions[account]
    current_time = datetime.now()
    
    # Get process information
    process_info = None
    memory_usage_mb = None
    cpu_percent = None
    uptime_minutes = None
    is_responsive = False
    
    if 'process' in session and session['process'] and session['process'].pid:
        process_info = get_process_info(session['process'].pid)
        if process_info:
            memory_usage_mb = process_info['total_memory_mb']
            cpu_percent = process_info['main_process']['cpu_percent']
            if session.get('start_time'):
                uptime_minutes = (current_time - session['start_time']).total_seconds() / 60
            is_responsive = True
    
    # Check if session should timeout
    if session['status'] == 'running':
        last_interaction = session.get('last_interaction', session.get('start_time'))
        if last_interaction:
            idle_time = current_time - last_interaction
            if idle_time > timedelta(minutes=SESSION_TIMEOUT_MINUTES):
                session['status'] = 'timeout_pending'
                asyncio.create_task(stop_session(SessionRequest(account=account)))
    
    return SessionStatus(
        account=account,
        status=session.get('status', 'unknown'),
        start_time=session.get('start_time'),
        last_interaction=session.get('last_interaction'),
        total_interactions=session.get('total_interactions', 0),
        errors=session.get('errors'),
        process_info=process_info,
        memory_usage_mb=memory_usage_mb,
        cpu_percent=cpu_percent,
        uptime_minutes=round(uptime_minutes, 2) if uptime_minutes else None,
        is_responsive=is_responsive
    )

@app.post("/test_interaction")
async def test_interaction():
    """Test endpoint to create a sample interaction"""
    # Implementation for test interaction
    pass

@app.get("/bot_stats")
async def get_bot_stats(account: str) -> BotStats:
    """
    Get comprehensive bot statistics for the specified account.
    Includes 24-hour metrics, performance data, and resource usage trends.
    """
    try:
        # Get history manager and initialize it
        history_manager = HistoryManager()
        
        # Get current timestamp and 24 hours ago
        now = datetime.now()
        twenty_four_hours_ago = now - timedelta(hours=24)
        
        # Get all interactions in the last 24 hours
        interactions = history_manager.get_interactions(account, start_time=twenty_four_hours_ago)
        
        # Calculate interaction statistics
        total_interactions = len(interactions)
        successful_interactions = sum(1 for i in interactions if not i.error)
        failed_interactions = total_interactions - successful_interactions
        success_rate = (successful_interactions / total_interactions * 100) if total_interactions > 0 else 0
        
        # Calculate average response time (from successful interactions)
        response_times = [i.duration for i in interactions if i.duration and not i.error]
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        
        # Get current session info
        session_info = active_sessions.get(account, {})
        start_time = session_info.get('start_time')
        current_duration = (now - start_time).total_seconds() / 3600 if start_time else None
        
        # Get process info for resource usage trends
        process_info = None
        if session_info.get('pid'):
            try:
                process = psutil.Process(session_info['pid'])
                process_info = await get_process_info(session_info['pid'])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        # Compile stats
        stats = BotStats(
            account=account,
            total_interactions_24h=total_interactions,
            successful_interactions_24h=successful_interactions,
            failed_interactions_24h=failed_interactions,
            success_rate_24h=success_rate,
            average_response_time_ms=avg_response_time * 1000,  # Convert to milliseconds
            uptime_hours=current_duration or 0.0,
            total_sessions=1 if start_time else 0,  # Basic implementation, could be enhanced
            current_session_duration=current_duration,
            memory_usage_trend=[process_info['memory_usage_mb']] if process_info else [],
            cpu_usage_trend=[process_info['cpu_percent']] if process_info else [],
            error_count_24h=failed_interactions
        )
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting bot stats for account {account}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get bot statistics: {str(e)}"
        )

@app.get("/accounts")
async def get_accounts() -> list[AccountInfo]:
    """
    Get list of all configured accounts and their basic information.
    Returns account usernames, profile stats, and session status.
    """
    try:
        accounts_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'accounts')
        accounts = []
        
        # List all account directories
        for account_name in os.listdir(accounts_dir):
            account_dir = os.path.join(accounts_dir, account_name)
            if not os.path.isdir(account_dir):
                continue
                
            # Get session data if available
            session_file = os.path.join(account_dir, 'sessions.json')
            profile_stats = {"posts": 0, "followers": 0, "following": 0}
            last_session_time = None
            
            if os.path.exists(session_file):
                try:
                    with open(session_file, 'r') as f:
                        sessions = json.loads(f.read())
                        if sessions:
                            # Get latest session
                            latest_session = sessions[-1]
                            # Get profile stats from latest session
                            profile_stats = latest_session.get('profile', profile_stats)
                            # Get session start time
                            start_time_str = latest_session.get('start_time')
                            if start_time_str:
                                last_session_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S.%f")
                except (json.JSONDecodeError, ValueError) as e:
                    logger.error(f"Error parsing session file for account {account_name}: {str(e)}")
            
            # Check if config exists
            config_exists = os.path.exists(os.path.join(account_dir, 'config.yml'))
            
            # Create account info
            account_info = AccountInfo(
                username=account_name,
                total_posts=profile_stats.get('posts', 0),
                total_followers=profile_stats.get('followers', 0),
                total_following=profile_stats.get('following', 0),
                last_session_time=last_session_time,
                is_active=account_name in active_sessions,
                config_exists=config_exists
            )
            accounts.append(account_info)
        
        return accounts
        
    except Exception as e:
        logger.error(f"Error getting accounts list: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get accounts list: {str(e)}"
        )

@app.get("/interaction_limits")
async def get_interaction_limits(account: str) -> InteractionLimits:
    """
    Get current interaction limits for the specified account.
    Returns all configured limits including likes, follows, comments, PMs, etc.
    """
    try:
        # Get session data from the account's sessions.json
        session_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                  'accounts', account, 'sessions.json')
        
        if not os.path.exists(session_file):
            raise HTTPException(
                status_code=404,
                detail=f"Account {account} not found or has no session configuration"
            )
            
        with open(session_file, 'r') as f:
            session_data = json.loads(f.read())
            
        # Get the most recent session
        if not session_data:
            raise HTTPException(
                status_code=404,
                detail=f"No sessions found for account {account}"
            )
            
        latest_session = session_data[-1]
        args = latest_session.get('args', {})
        
        # Parse limit ranges (e.g. "120-150" -> 150)
        def parse_limit(limit_str):
            if not limit_str:
                return 0
            try:
                if isinstance(limit_str, (int, float)):
                    return int(limit_str)
                parts = str(limit_str).split('-')
                return int(parts[-1])  # Take the upper limit
            except (ValueError, IndexError):
                return 0
            
        # Extract limits from session args
        limits = InteractionLimits(
            account=account,
            likes_limit=parse_limit(args.get('total_likes_limit', 0)),
            follow_limit=parse_limit(args.get('total_follows_limit', 0)),
            unfollow_limit=parse_limit(args.get('total_unfollows_limit', 0)),
            comments_limit=parse_limit(args.get('total_comments_limit', 0)),
            pm_limit=parse_limit(args.get('total_pm_limit', 0)),
            watch_limit=parse_limit(args.get('total_watches_limit', 0)),
            success_limit=parse_limit(args.get('total_successful_interactions_limit', 0)),
            total_limit=parse_limit(args.get('total_interactions_limit', 0)),
            scraped_limit=parse_limit(args.get('total_scraped_limit', 0)),
            crashes_limit=parse_limit(args.get('total_crashes_limit', 0)),
            time_delta_session=parse_limit(args.get('time_delta_session', 0))
        )
        
        return limits
        
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing session file for account {account}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to parse session configuration: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error getting interaction limits for account {account}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get interaction limits: {str(e)}"
        )

@app.get("/account_config/{account}")
async def get_account_config(account: str) -> AccountConfig:
    """
    Get configuration for a specific account.
    Returns all settings from the account's config.yml file.
    """
    try:
        config_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'accounts',
            account,
            'config.yml'
        )
        
        if not os.path.exists(config_file):
            raise HTTPException(
                status_code=404,
                detail=f"Configuration file not found for account {account}"
            )
            
        # Read and parse YAML config
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
            
        # Convert YAML keys to match Pydantic model (replace hyphens with underscores)
        converted_config = {}
        for key, value in config_data.items():
            new_key = key.replace('-', '_')
            converted_config[new_key] = value
            
        # Create AccountConfig instance
        try:
            config = AccountConfig(**converted_config)
            return config
        except ValueError as e:
            logger.error(f"Error validating config for account {account}: {str(e)}")
            raise HTTPException(
                status_code=422,
                detail=f"Invalid configuration format: {str(e)}"
            )
            
    except yaml.YAMLError as e:
        logger.error(f"Error parsing config file for account {account}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to parse configuration file: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error getting config for account {account}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get account configuration: {str(e)}"
        )

@app.put("/account_config/{account}")
async def update_account_config(account: str, update: UpdateAccountConfig):
    """
    Update configuration for a specific account.
    Saves the provided settings to the account's config.yml file.
    """
    try:
        # Construct path to account config
        accounts_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "accounts")
        account_dir = os.path.join(accounts_dir, account)
        config_path = os.path.join(account_dir, "config.yml")

        # Verify account directory exists
        if not os.path.exists(account_dir):
            raise HTTPException(status_code=404, detail=f"Account {account} not found")

        # Read existing config
        try:
            with open(config_path, 'r') as f:
                current_config = yaml.safe_load(f) or {}
        except FileNotFoundError:
            current_config = {}

        # Convert keys to use hyphens instead of underscores
        updated_config = {k.replace('_', '-'): v for k, v in update.config.items()}
        
        # Update config with new values
        current_config.update(updated_config)

        # Remove username from config if present to avoid duplicate
        current_config.pop('username', None)

        # Validate config using Pydantic model
        try:
            AccountConfig(username=account, **{k.replace('-', '_'): v for k, v in current_config.items()})
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid configuration: {str(e)}")

        # Save updated config
        with open(config_path, 'w') as f:
            yaml.safe_dump(current_config, f, default_flow_style=False)

        logger.info(f"Updated configuration for account {account}")
        return {"status": "success", "message": f"Configuration updated for account {account}"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating configuration for account {account}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to update configuration: {str(e)}")

@app.post("/account_config/{account}/add")
async def add_config_entry(account: str, entry: ConfigEntry):
    """
    Add or update a single configuration entry.
    For example: add a new configuration value like watch-video-time.
    """
    try:
        accounts_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "accounts")
        account_dir = os.path.join(accounts_dir, account)
        config_path = os.path.join(account_dir, "config.yml")

        if not os.path.exists(account_dir):
            raise HTTPException(status_code=404, detail=f"Account {account} not found")

        # Read file while preserving format
        try:
            lines = read_file_lines(config_path)
            current_config = yaml.safe_load(''.join(lines)) or {}
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"Configuration file not found")

        # Convert key to use hyphens
        key = entry.key.replace('_', '-')

        # Update the lines while preserving format
        new_lines = update_yaml_value_in_lines(lines, key, entry.value)

        # Validate the complete configuration
        try:
            new_config = yaml.safe_load(''.join(new_lines))
            config_dict = {k.replace('-', '_'): v for k, v in new_config.items()}
            config_dict.pop('username', None)
            AccountConfig(username=account, **config_dict)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid configuration: {str(e)}")

        # Write back the updated lines
        write_file_lines(config_path, new_lines)

        logger.info(f"Added configuration entry {key} for account {account}")
        return {"status": "success", "message": f"Added configuration entry {key}"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding configuration entry for account {account}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to add configuration entry: {str(e)}")

@app.post("/account_config/{account}/array/add")
async def add_array_item(account: str, entry: ArrayConfigEntry):
    """
    Add an item to an array configuration entry.
    For example: add a new username to blogger-followers.
    """
    try:
        accounts_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "accounts")
        account_dir = os.path.join(accounts_dir, account)
        config_path = os.path.join(account_dir, "config.yml")

        if not os.path.exists(account_dir):
            raise HTTPException(status_code=404, detail=f"Account {account} not found")

        # Read file while preserving format
        try:
            lines = read_file_lines(config_path)
            current_config = yaml.safe_load(''.join(lines)) or {}
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"Configuration file not found")

        # Convert key to use hyphens
        key = entry.key.replace('_', '-')

        # Get current array value
        current_value = current_config.get(key, [])
        if isinstance(current_value, str):
            try:
                import ast
                current_value = ast.literal_eval(current_value)
            except:
                current_value = []
        if not isinstance(current_value, list):
            current_value = []

        # Add new item if not present
        if entry.item not in current_value:
            current_value.append(entry.item)

        # Update the lines while preserving format
        new_lines = update_yaml_value_in_lines(lines, key, current_value)

        # Validate the complete configuration
        try:
            new_config = yaml.safe_load(''.join(new_lines))
            config_dict = {k.replace('-', '_'): v for k, v in new_config.items()}
            config_dict.pop('username', None)
            AccountConfig(username=account, **config_dict)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid configuration: {str(e)}")

        # Write back the updated lines
        write_file_lines(config_path, new_lines)

        logger.info(f"Added item to array {key} for account {account}")
        return {"status": "success", "message": f"Added item to array {key}"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding array item for account {account}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to add array item: {str(e)}")

@app.delete("/account_config/{account}/add")
async def remove_config_entry(account: str, key: str):
    """
    Remove a single configuration entry.
    For example: remove a configuration value completely.
    """
    try:
        accounts_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "accounts")
        account_dir = os.path.join(accounts_dir, account)
        config_path = os.path.join(account_dir, "config.yml")

        if not os.path.exists(account_dir):
            raise HTTPException(status_code=404, detail=f"Account {account} not found")

        # Read existing config
        try:
            with open(config_path, 'r') as f:
                current_config = yaml.safe_load(f) or {}
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"Configuration file not found")

        # Convert key to use hyphens
        key = key.replace('_', '-')

        # Remove entry if it exists
        if key not in current_config:
            raise HTTPException(status_code=404, detail=f"Configuration entry {key} not found")

        current_config.pop(key)

        # Validate config
        try:
            config_dict = {k.replace('-', '_'): v for k, v in current_config.items()}
            config_dict.pop('username', None)
            AccountConfig(username=account, **config_dict)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid configuration: {str(e)}")

        # Save updated config
        with open(config_path, 'w') as f:
            yaml.safe_dump(current_config, f, default_flow_style=False)

        logger.info(f"Removed configuration entry {key} for account {account}")
        return {"status": "success", "message": f"Removed configuration entry {key}"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error removing configuration entry for account {account}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to remove configuration entry: {str(e)}")

@app.post("/account_config/{account}/array/add")
async def add_array_item(account: str, entry: ArrayConfigEntry):
    """
    Add an item to an array configuration entry.
    For example: add a new username to blogger-followers.
    """
    try:
        accounts_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "accounts")
        account_dir = os.path.join(accounts_dir, account)
        config_path = os.path.join(account_dir, "config.yml")

        if not os.path.exists(account_dir):
            raise HTTPException(status_code=404, detail=f"Account {account} not found")

        # Read file while preserving format
        try:
            lines = read_file_lines(config_path)
            current_config = yaml.safe_load(''.join(lines)) or {}
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"Configuration file not found")

        # Convert key to use hyphens
        key = entry.key.replace('_', '-')

        # Get current array value
        current_value = current_config.get(key, [])
        if isinstance(current_value, str):
            try:
                import ast
                current_value = ast.literal_eval(current_value)
            except:
                current_value = []
        if not isinstance(current_value, list):
            current_value = []

        # Add new item if not present
        if entry.item not in current_value:
            current_value.append(entry.item)

        # Update the lines while preserving format
        new_lines = update_yaml_value_in_lines(lines, key, current_value)

        # Validate the complete configuration
        try:
            new_config = yaml.safe_load(''.join(new_lines))
            config_dict = {k.replace('-', '_'): v for k, v in new_config.items()}
            config_dict.pop('username', None)
            AccountConfig(username=account, **config_dict)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid configuration: {str(e)}")

        # Write back the updated lines
        write_file_lines(config_path, new_lines)

        logger.info(f"Added item to array {key} for account {account}")
        return {"status": "success", "message": f"Added item to array {key}"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding array item for account {account}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to add array item: {str(e)}")

@app.delete("/account_config/{account}/array/{key}/remove")
async def remove_array_item(account: str, key: str, entry: ArrayConfigEntry):
    """
    Remove an item from an array configuration entry.
    For example: remove a username from blogger-followers.
    """
    try:
        accounts_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "accounts")
        account_dir = os.path.join(accounts_dir, account)
        config_path = os.path.join(account_dir, "config.yml")

        if not os.path.exists(account_dir):
            raise HTTPException(status_code=404, detail=f"Account {account} not found")

        # Read file while preserving format
        try:
            lines = read_file_lines(config_path)
            current_config = yaml.safe_load(''.join(lines)) or {}
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"Configuration file not found")

        # Convert key to use hyphens
        key = key.replace('_', '-')

        # Get current array value
        current_value = current_config.get(key, [])
        if isinstance(current_value, str):
            try:
                import ast
                current_value = ast.literal_eval(current_value)
            except:
                current_value = []
        if not isinstance(current_value, list):
            current_value = []

        # Remove item if it exists
        if entry.item not in current_value:
            raise HTTPException(status_code=404, detail=f"Item not found in array {key}")
            
        current_value.remove(entry.item)

        # Update the lines while preserving format
        new_lines = update_yaml_value_in_lines(lines, key, current_value)

        # Validate the complete configuration
        try:
            new_config = yaml.safe_load(''.join(new_lines))
            config_dict = {k.replace('-', '_'): v for k, v in new_config.items()}
            config_dict.pop('username', None)
            AccountConfig(username=account, **config_dict)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid configuration: {str(e)}")

        # Write back the updated lines
        write_file_lines(config_path, new_lines)

        logger.info(f"Removed item from array {key} for account {account}")
        return {"status": "success", "message": f"Removed item from array {key}"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error removing array item for account {account}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to remove array item: {str(e)}")

def read_file_lines(file_path: str) -> list[str]:
    """Read file and return lines while preserving format"""
    with open(file_path, 'r') as f:
        return f.readlines()

def write_file_lines(file_path: str, lines: list[str]):
    """Write lines back to file"""
    with open(file_path, 'w') as f:
        f.writelines(lines)

def update_yaml_value_in_lines(lines: list[str], key: str, value: Any) -> list[str]:
    """Update a specific key's value in the YAML lines while preserving format"""
    new_lines = []
    key_found = False
    
    for line in lines:
        if line.strip().startswith(f"{key}:"):
            # Preserve any inline comments
            comment = ""
            if "#" in line:
                comment = " " + line.split("#", 1)[1].rstrip()
            
            # Format the value appropriately
            if isinstance(value, list):
                formatted_value = str(value).replace("'", '"')
            else:
                formatted_value = str(value)
            
            new_lines.append(f"{key}: {formatted_value}{comment}\n")
            key_found = True
        else:
            new_lines.append(line)
    
    if not key_found:
        # Add new key at the end of the appropriate section
        new_lines.append(f"{key}: {value}\n")
    
    return new_lines
