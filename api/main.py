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
from datetime import datetime
from api.history import HistoryManager, Interaction
from pydantic import BaseModel

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

@app.on_event("startup")
async def startup_event():
    """Initialize API on startup"""
    try:
        # Create logs directory if it doesn't exist
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        logs_dir = os.path.join(base_dir, 'logs')
        os.makedirs(logs_dir, exist_ok=True)
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

class SessionRequest(BaseModel):
    account: str

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
            return {"message": f"Started session for account: {request.account}", "status": "running", "pid": process.pid}
            
        except Exception as e:
            error_msg = f"Failed to start process: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise HTTPException(status_code=500, detail=error_msg)
            
    except Exception as e:
        error_msg = f"Error starting session: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)

@app.post("/test_interaction")
async def test_interaction():
    """Test endpoint to create a sample interaction"""
    # Implementation for test interaction
    pass
