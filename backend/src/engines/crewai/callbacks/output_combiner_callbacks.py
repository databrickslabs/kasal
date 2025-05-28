"""
Output combiner callbacks for CrewAI engine.

This module provides callbacks for combining output from multiple tasks into a single file.
"""
from typing import Any, Dict, List, Optional
import logging
import os
import glob
from datetime import datetime
from pathlib import Path
from sqlalchemy.orm import Session

from src.engines.crewai.callbacks.base import CrewAICallback
from src.services.task_tracking_service import TaskTrackingService
from src.schemas.task_tracking import TaskStatusEnum
from src.repositories.output_combiner_repository import OutputCombinerRepository, get_output_combiner_repository

logger = logging.getLogger(__name__)

class OutputCombinerCallback(CrewAICallback):
    """
    Callback that combines output files from multiple tasks into a single file.
    This callback only performs the combination when all tasks for a job have completed.
    """
    
    def __init__(self, job_id: str, output_dir: str = None, db: Session = None, **kwargs):
        """
        Initialize the OutputCombinerCallback.
        
        Args:
            job_id: The ID of the job
            output_dir: Directory containing output files (defaults to standard output dir)
            db: Database session (legacy, use repository property instead)
            **kwargs: Additional arguments passed to the parent class
        """
        super().__init__(**kwargs)
        self.job_id = job_id
        self.output_dir = output_dir or os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 
            "output"
        )
        
        # Initialize repository if db is provided
        self._repository = None
        if db:
            self._repository = get_output_combiner_repository(db)
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"Output combiner using directory: {self.output_dir}")
    
    @property
    def repository(self) -> OutputCombinerRepository:
        """Get the repository instance."""
        return self._repository
    
    @repository.setter
    def repository(self, value: OutputCombinerRepository):
        """Set the repository instance."""
        self._repository = value
        
    async def execute(self, output: Any) -> Any:
        """
        Execute the callback to combine task outputs if all tasks are complete.
        
        This callback:
        1. Checks if all tasks for the job have completed
        2. If so, orders tasks by dependencies
        3. Reads their output files
        4. Combines them into a single markdown file
        
        Args:
            output: The output from the previous callback
            
        Returns:
            output: The original output (this callback doesn't modify the output)
        """
        if not self.repository:
            logger.error("Repository not set for OutputCombinerCallback")
            return output
            
        try:
            # Get all task statuses
            all_tasks = await self.repository.get_all_task_statuses()
            
            # Check if all tasks are complete
            all_complete = all(task.status == 'completed' for task in all_tasks)
            
            if not all_complete:
                logger.info("Not all tasks are complete yet, skipping output combination")
                return output
            
            # Create output directory if it doesn't exist
            os.makedirs(self.output_dir, exist_ok=True)
            
            # Create combined output file
            combined_file_path = os.path.join(self.output_dir, f"combined_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md")
            
            with open(combined_file_path, 'w') as combined_file:
                # Write header
                combined_file.write("# Combined Task Outputs\n\n")
                
                # Process each task in order (they're already ordered by dependency due to get_all_task_statuses)
                for task_status in all_tasks:
                    task_id = task_status.task_id
                    task_name = task_id  # Default to task ID if name not available
                    
                    # Try to get more information about the task
                    task_info = self.repository.get_run_by_job_id(self.job_id).inputs.get('tasks_yaml', {}).get(task_id, {})
                    if task_info:
                        task_name = task_info.get('name', task_id)
                    
                    # Write task header with status
                    task_header = f"\n## Task: {task_name} (Status: {task_status.status})\n\n"
                    combined_file.write(task_header)
                    
                    # Look for output files related to this task
                    # Extract the task number from task_id (task_task-7 → 7)
                    task_short_id = None
                    if task_id.startswith("task_task-"):
                        # Handle format like 'task_task-7'
                        try:
                            task_short_id = task_id.split("-")[1]
                        except IndexError:
                            task_short_id = None
                    elif "_" in task_id:
                        # Handle format like 'task_7'
                        parts = task_id.split("_")
                        task_short_id = parts[-1]
                        # If the last part still has a dash, extract the number
                        if "-" in task_short_id:
                            task_short_id = task_short_id.split("-")[-1]
                    else:
                        task_short_id = task_id
                    
                    logger.info(f"Extracted task short ID: '{task_short_id}' from '{task_id}'")
                    
                    # Find all files that match the task pattern
                    # Format: job_TIMESTAMP_task-NUMBER.md
                    all_task_files = glob.glob(os.path.join(self.output_dir, f"job_*_task-{task_short_id}.md"))
                    
                    if all_task_files:
                        # Sort files by timestamp (newest first)
                        all_task_files.sort(reverse=True)
                        
                        # Read the most recent file
                        with open(all_task_files[0], 'r') as task_file:
                            content = task_file.read()
                            
                            # Check if the task has markdown enabled
                            task_config = task_info.get('config', {})
                            if task_config.get('markdown', False):
                                # Content is already in markdown format
                                combined_file.write(content)
                            else:
                                # Convert content to markdown format
                                combined_file.write(f"```\n{content}\n```\n")
                    else:
                        combined_file.write("No output file found for this task.\n")
                    
                    combined_file.write("\n---\n")  # Add separator between tasks
                
                logger.info(f"Combined output written to {combined_file_path}")
            
            return output
            
        except Exception as e:
            logger.error(f"Error combining task outputs: {str(e)}", exc_info=True)
            return output 