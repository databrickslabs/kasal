"""
Helper functions for task callbacks in CrewAI.

This module provides utility functions for configuring callbacks for CrewAI tasks.
"""

from typing import Any, Dict, List, Optional

from src.core.logger import LoggerManager
from src.engines.crewai.callbacks.streaming_callbacks import JobOutputCallback

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().crew


def configure_task_callbacks(
    task_key: str,
    job_id: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None
) -> List[Any]:
    """
    Configure callbacks for a specific task.
    
    Args:
        task_key: Unique task identifier
        job_id: Optional job identifier
        config: Optional configuration dictionary
        
    Returns:
        List of configured callbacks
    """
    callbacks = []
    
    # Add job output callback if job_id is provided
    if job_id:
        logger.info(f"Adding streaming callback for task {task_key} in job {job_id}")
        streaming_callback = JobOutputCallback(
            job_id=job_id,
            task_key=task_key,
            config=config
        )
        callbacks.append(streaming_callback)
    
    # Add additional callbacks based on config
    if config and "callbacks" in config:
        for callback_config in config["callbacks"]:
            callback_type = callback_config.get("type")
            callback_params = callback_config.get("params", {})
            
            # Add task_key to callback params
            callback_params["task_key"] = task_key
            
            # Handle different callback types here
            # This is a placeholder for future callback types
            logger.debug(f"Adding callback of type {callback_type} for task {task_key}")
    
    return callbacks 