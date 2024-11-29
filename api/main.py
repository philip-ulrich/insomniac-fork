from fastapi import FastAPI, HTTPException
import logging
import asyncio
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
from datetime import datetime
from api.history import HistoryManager, Interaction

app = FastAPI()
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
    """Initialize plugins on startup"""
    global sync_plugin
    sync_plugin = plugin_loader.load_sync_plugin()
    if sync_plugin:
        logger.info("Sync plugin loaded successfully.")

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
async def start_session(account: str):
    """Start a bot session for the specified account"""
    try:
        config_path = f"accounts/{account}/config.yml"
        logger.info(f"Starting session for account {account} with config {config_path}")
        
        if not os.path.exists(config_path):
            error_msg = f"Configuration not found for account: {account} at path: {config_path}"
            logger.error(error_msg)
            raise HTTPException(status_code=404, detail=error_msg)
            
        # Create a background task to run the bot
        cmd = ["venv/Scripts/python", "run.py", "--config", config_path, "--use-nocodb", "--debug"]
        logger.info(f"Executing command: {' '.join(cmd)}")
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Register the task
            task = asyncio.create_task(process.communicate())
            register_task(task)
            
            logger.info(f"Successfully started session for account: {account}")
            return {"message": f"Started session for account: {account}", "status": "running"}
            
        except Exception as e:
            error_msg = f"Failed to start process: {str(e)}"
            logger.error(error_msg)
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
