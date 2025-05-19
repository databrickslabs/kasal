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
            # Get task tracking service with repository access
            from src.repositories.task_tracking_repository import TaskTrackingRepository
            task_tracking_repository = TaskTrackingRepository(self.repository.db)
            task_tracking_service = TaskTrackingService.for_crew_with_repo(task_tracking_repository)
            
            # Always get all tasks for this job - needed regardless of execution path
            all_tasks = task_tracking_service.get_all_task_statuses(self.job_id)
            
            # Check if we're being called at job completion (from job_runner.py)
            if self.task_key == "job_completion":
                logger.info(f"=== Running OutputCombinerCallback at job completion for job {self.job_id} ===")
                # Skip the task completion check since we know the job has completed
                force_combine = True
            else:
                # Normal execution from task callback - check if all tasks are complete
                # Count completed and total tasks
                total_tasks = len(all_tasks)
                completed_tasks = sum(1 for task in all_tasks if task.status == TaskStatusEnum.COMPLETED)
                
                # If not all tasks are complete, log and return original output
                force_combine = completed_tasks >= total_tasks
                if not force_combine:
                    logger.info(f"Skipping output combination: {completed_tasks}/{total_tasks} tasks completed for job {self.job_id}")
                    return output
                logger.info(f"=== Starting OutputCombinerCallback for job {self.job_id}: All {total_tasks} tasks have completed ===")
                
            logger.info(f"Using output directory: {self.output_dir}")
            
            # Get run information from repository
            db_run = self.repository.get_run_by_job_id(self.job_id)
            if not db_run:
                logger.error(f"No run record found for job_id: {self.job_id}")
                return output
                
            # Create a timestamp for the combined output file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            combined_output_path = os.path.join(self.output_dir, f"combined_{self.job_id}_{timestamp}.md")
            
            # Prepare header for the combined output
            header = f"""# Combined Output for Job: {self.job_id}
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Run Name: {db_run.run_name or 'Unnamed Run'}
Status: {db_run.status}

---

"""
            # Create the combined output file
            with open(combined_output_path, 'w') as combined_file:
                # Write header
                combined_file.write(header)
                
                # Process each task in order (they're already ordered by dependency due to get_all_task_statuses)
                for task_status in all_tasks:
                    task_id = task_status.task_id
                    task_name = task_id  # Default to task ID if name not available
                    
                    # Try to get more information about the task
                    task_info = db_run.inputs.get('tasks_yaml', {}).get(task_id, {})
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
                    logger.info(f"Found {len(all_task_files)} files matching pattern 'job_*_task-{task_short_id}.md'")
                    
                    # If we didn't find files and task_short_id might have additional text, try to extract just the number
                    if not all_task_files and task_short_id:
                        # Try to extract just the numeric part if there's non-numeric content
                        import re
                        numeric_match = re.search(r'\d+', task_short_id)
                        if numeric_match:
                            numeric_id = numeric_match.group(0)
                            all_task_files = glob.glob(os.path.join(self.output_dir, f"job_*_task-{numeric_id}.md"))
                            logger.info(f"Trying numeric-only ID: Found {len(all_task_files)} files matching pattern 'job_*_task-{numeric_id}.md'")
                    
                    # Show all MD files in directory for debugging
                    all_md_files = glob.glob(os.path.join(self.output_dir, "*.md"))
                    logger.info(f"All MD files in directory ({len(all_md_files)} total):")
                    for f in all_md_files:
                        logger.info(f"  - {os.path.basename(f)}")
                    
                    # Sort the files by modification time (most recent first) to get the latest output
                    task_output_files = sorted(
                        all_task_files,
                        key=lambda x: os.path.getmtime(x),
                        reverse=True
                    )
                    
                    # Process each output file for this task
                    for output_file in task_output_files:
                        try:
                            logger.info(f"Processing output file: {output_file}")
                            combined_file.write(f"### Output from {os.path.basename(output_file)}\n\n")
                            
                            # Read and include the file content
                            with open(output_file, 'r') as task_file:
                                content = task_file.read()
                                combined_file.write(content)
                                combined_file.write("\n\n")
                                
                        except Exception as e:
                            error_msg = f"Error processing output file {output_file}: {str(e)}"
                            logger.error(error_msg)
                            combined_file.write(f"**Error processing file: {str(e)}**\n\n")
                    
                    # If no output files were found for this task
                    if not task_output_files:
                        combined_file.write(f"*No output files found for task {task_id}*\n\n")
            
            logger.info(f"Combined output saved to: {combined_output_path}")
            
            # Store the combined output path in metadata
            self.metadata["combined_output_path"] = combined_output_path
            
            # Return the original output
            return output
            
        except Exception as e:
            logger.error(f"Error in OutputCombinerCallback: {str(e)}", exc_info=True)
            # Even if we fail, return the original output
            return output 