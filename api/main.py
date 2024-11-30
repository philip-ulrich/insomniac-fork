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
        raise HTTPException(status_code=500, detail=f"Failed to stop session: {str(e)}")

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
