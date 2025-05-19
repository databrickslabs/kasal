"""
Helper functions for task callbacks in CrewAI.

This module provides utility functions for configuring callbacks for CrewAI tasks.
"""

import os
from typing import Any, Dict, List, Optional

from src.core.logger import LoggerManager
from src.engines.crewai.callbacks.streaming_callbacks import JobOutputCallback
from src.engines.crewai.callbacks.output_combiner_callbacks import OutputCombinerCallback

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().crew

def configure_process_output_handler(
    job_id: str,
    output_dir: Optional[str] = None,
    db = None,
    config: Optional[Dict[str, Any]] = None
) -> List[Any]:
    """
    Configure process output handlers for a job.
    
    Args:
        job_id: Unique job identifier
        output_dir: Optional directory for output files
        db: Optional database session
        config: Optional configuration dictionary
        
    Returns:
        List of configured process handlers
    """
    handlers = []
    
    # Add streaming callback if job_id is provided
    if job_id:
        logger.info(f"Adding streaming callback for job {job_id}")
        streaming_callback = JobOutputCallback(job_id=job_id, config=config)
        handlers.append(streaming_callback)
    
    # Add output combiner if DB and output_dir provided
    if db and output_dir:
        logger.info(f"Adding output combiner callback for job {job_id}")
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            
        output_combiner = OutputCombinerCallback(
            job_id=job_id,
            output_dir=output_dir,
            db=db
        )
        handlers.append(output_combiner)
    
    return handlers

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