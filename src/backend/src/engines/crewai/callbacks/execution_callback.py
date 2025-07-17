"""
Execution-scoped callback system for CrewAI.

This module provides callback functions that are scoped to specific executions,
replacing global event listeners to prevent cross-contamination between concurrent crews.
"""

import logging
from typing import Any, Optional, Dict
from datetime import datetime, timezone

# Import queue services
from src.services.execution_logs_queue import enqueue_log
from src.services.trace_queue import get_trace_queue

# Import group context
from src.utils.user_context import GroupContext, UserContext

logger = logging.getLogger(__name__)


def create_execution_callbacks(job_id: str, config: Dict[str, Any] = None, group_context: GroupContext = None):
    """
    Create execution-scoped callback functions for a specific CrewAI execution.
    
    These callbacks are passed directly to crew.kickoff() and are automatically
    scoped to the specific execution, preventing cross-contamination.
    
    Args:
        job_id: Unique identifier for the execution
        config: Optional configuration dictionary
        group_context: Group context for multi-tenant isolation
        
    Returns:
        Tuple of (step_callback, task_callback) functions
    """
    log_prefix = f"[ExecutionCallback][{job_id}]"
    trace_queue = get_trace_queue()
    
    logger.info(f"{log_prefix} Creating execution-scoped callbacks")
    
    def step_callback(step_output):
        """
        Called after each agent step during execution.
        
        Args:
            step_output: The output from an agent step
        """
        try:
            logger.debug(f"{log_prefix} Step callback triggered")
            
            # Extract information from step output
            timestamp = datetime.now(timezone.utc)
            
            # Format step information
            if hasattr(step_output, 'output'):
                content = str(step_output.output)
            elif hasattr(step_output, 'raw'):
                content = str(step_output.raw)
            else:
                content = str(step_output)
            
            # Extract agent information if available
            agent_name = "Unknown Agent"
            if hasattr(step_output, 'agent'):
                agent_name = getattr(step_output.agent, 'role', 'Unknown Agent')
            
            # Limit content length for logging
            content_preview = content[:500] + "..." if len(content) > 500 else content
            
            # Log the step
            log_message = f"[STEP] Agent: {agent_name} - Output: {content_preview}"
            
            # Enqueue to execution logs with group context
            try:
                enqueue_log(
                    execution_id=job_id,
                    content=log_message,
                    timestamp=timestamp,
                    group_context=group_context
                )
            except Exception as log_error:
                logger.error(f"{log_prefix} Failed to enqueue execution log: {log_error}")
            
            # Enqueue to trace queue for detailed analysis
            trace_data = {
                "job_id": job_id,
                "event_source": agent_name,
                "event_context": "agent_step",
                "event_type": "agent_execution",  # Use important event type
                "timestamp": timestamp.isoformat(),
                "output_content": content,
                "extra_data": {"type": "step_callback"}
            }
            
            # Add group context if available
            if group_context:
                trace_data["group_id"] = group_context.primary_group_id
                trace_data["group_email"] = group_context.group_email
            
            try:
                trace_queue.put_nowait(trace_data)
                logger.debug(f"{log_prefix} Step trace enqueued successfully")
            except Exception as trace_error:
                logger.error(f"{log_prefix} Failed to enqueue step trace: {trace_error}")
            
        except Exception as e:
            logger.error(f"{log_prefix} Error in step_callback: {e}", exc_info=True)
    
    def task_callback(task_output):
        """
        Called after each task completion during execution.
        
        Args:
            task_output: The output from a completed task
        """
        try:
            logger.debug(f"{log_prefix} Task callback triggered")
            
            # Extract information from task output
            timestamp = datetime.now(timezone.utc)
            
            # Get task information
            task_description = "Unknown Task"
            if hasattr(task_output, 'description'):
                task_description = task_output.description
            elif hasattr(task_output, 'task') and hasattr(task_output.task, 'description'):
                task_description = task_output.task.description
            
            # Get output content
            if hasattr(task_output, 'raw'):
                content = str(task_output.raw)
            elif hasattr(task_output, 'output'):
                content = str(task_output.output)
            else:
                content = str(task_output)
            
            # Get agent information if available
            agent_name = "Unknown Agent"
            if hasattr(task_output, 'agent'):
                agent_name = getattr(task_output.agent, 'role', 'Unknown Agent')
            
            # Limit content length for logging
            task_preview = task_description[:100] + "..." if len(task_description) > 100 else task_description
            content_preview = content[:500] + "..." if len(content) > 500 else content
            
            # Log the task completion
            log_message = f"[TASK COMPLETED] Task: {task_preview} - Agent: {agent_name} - Output: {content_preview}"
            
            # Enqueue to execution logs with group context
            try:
                enqueue_log(
                    execution_id=job_id,
                    content=log_message,
                    timestamp=timestamp,
                    group_context=group_context
                )
            except Exception as log_error:
                logger.error(f"{log_prefix} Failed to enqueue execution log: {log_error}")
            
            # Enqueue to trace queue for detailed analysis
            trace_data = {
                "job_id": job_id,
                "event_source": "task",
                "event_context": task_description,
                "event_type": "task_completed",
                "timestamp": timestamp.isoformat(),
                "output_content": content,
                "extra_data": {
                    "type": "task_callback",
                    "agent_role": agent_name,
                    "task_description": task_description
                }
            }
            
            # Add group context if available
            if group_context:
                trace_data["group_id"] = group_context.primary_group_id
                trace_data["group_email"] = group_context.group_email
            
            try:
                trace_queue.put_nowait(trace_data)
                logger.debug(f"{log_prefix} Task trace enqueued successfully")
            except Exception as trace_error:
                logger.error(f"{log_prefix} Failed to enqueue task trace: {trace_error}")
            
        except Exception as e:
            logger.error(f"{log_prefix} Error in task_callback: {e}", exc_info=True)
    
    logger.info(f"{log_prefix} Execution-scoped callbacks created successfully")
    return step_callback, task_callback


def create_crew_callbacks(job_id: str, config: Dict[str, Any] = None, group_context: GroupContext = None):
    """
    Create crew-level callback functions for logging crew lifecycle events.
    
    Args:
        job_id: Unique identifier for the execution
        config: Optional configuration dictionary
        group_context: Group context for multi-tenant isolation
        
    Returns:
        Dictionary of crew callback functions
    """
    log_prefix = f"[CrewCallback][{job_id}]"
    
    def on_crew_start():
        """Called when crew execution starts."""
        try:
            timestamp = datetime.now(timezone.utc)
            log_message = f"[CREW STARTED] Execution {job_id} started"
            
            try:
                enqueue_log(
                    execution_id=job_id,
                    content=log_message,
                    timestamp=timestamp,
                    group_context=group_context
                )
            except Exception as log_error:
                logger.error(f"{log_prefix} Failed to enqueue execution log: {log_error}")
            
            # Also create trace for crew start
            from src.services.trace_queue import get_trace_queue
            trace_queue = get_trace_queue()
            trace_data = {
                "job_id": job_id,
                "event_source": "crew",
                "event_context": f"execution-{job_id}",
                "event_type": "crew_started",
                "timestamp": timestamp.isoformat(),
                "output_content": f"Crew execution {job_id} started",
                "extra_data": {"type": "crew_callback"}
            }
            
            if group_context:
                trace_data["group_id"] = group_context.primary_group_id
                trace_data["group_email"] = group_context.group_email
            
            try:
                trace_queue.put_nowait(trace_data)
            except Exception as trace_error:
                logger.error(f"{log_prefix} Failed to enqueue crew start trace: {trace_error}")
            
            logger.info(f"{log_prefix} Crew execution started")
            
        except Exception as e:
            logger.error(f"{log_prefix} Error in on_crew_start: {e}", exc_info=True)
    
    def on_crew_complete(result):
        """Called when crew execution completes."""
        try:
            timestamp = datetime.now(timezone.utc)
            
            # Format result for logging
            result_preview = str(result)[:500] + "..." if len(str(result)) > 500 else str(result)
            log_message = f"[CREW COMPLETED] Execution {job_id} completed - Result: {result_preview}"
            
            try:
                enqueue_log(
                    execution_id=job_id,
                    content=log_message,
                    timestamp=timestamp,
                    group_context=group_context
                )
            except Exception as log_error:
                logger.error(f"{log_prefix} Failed to enqueue execution log: {log_error}")
            
            # Also create trace for crew completion
            from src.services.trace_queue import get_trace_queue
            trace_queue = get_trace_queue()
            trace_data = {
                "job_id": job_id,
                "event_source": "crew",
                "event_context": f"execution-{job_id}",
                "event_type": "crew_completed",
                "timestamp": timestamp.isoformat(),
                "output_content": result_preview,
                "extra_data": {"type": "crew_callback"}
            }
            
            if group_context:
                trace_data["group_id"] = group_context.primary_group_id
                trace_data["group_email"] = group_context.group_email
            
            try:
                trace_queue.put_nowait(trace_data)
            except Exception as trace_error:
                logger.error(f"{log_prefix} Failed to enqueue crew completion trace: {trace_error}")
            
            logger.info(f"{log_prefix} Crew execution completed")
            
        except Exception as e:
            logger.error(f"{log_prefix} Error in on_crew_complete: {e}", exc_info=True)
    
    def on_crew_error(error):
        """Called when crew execution fails."""
        try:
            timestamp = datetime.now(timezone.utc)
            log_message = f"[CREW FAILED] Execution {job_id} failed - Error: {str(error)}"
            
            try:
                enqueue_log(
                    execution_id=job_id,
                    content=log_message,
                    timestamp=timestamp,
                    group_context=group_context
                )
            except Exception as log_error:
                logger.error(f"{log_prefix} Failed to enqueue execution log: {log_error}")
            
            logger.error(f"{log_prefix} Crew execution failed: {error}")
            
        except Exception as e:
            logger.error(f"{log_prefix} Error in on_crew_error: {e}", exc_info=True)
    
    return {
        'on_start': on_crew_start,
        'on_complete': on_crew_complete,
        'on_error': on_crew_error
    }


def log_crew_initialization(job_id: str, config: Dict[str, Any] = None, group_context: GroupContext = None):
    """
    Log crew initialization with configuration details.
    
    Args:
        job_id: Unique identifier for the execution
        config: Configuration dictionary
        group_context: Group context for multi-tenant isolation
    """
    try:
        timestamp = datetime.now(timezone.utc)
        
        # Create sanitized config for logging
        sanitized_config = {}
        if config:
            # Extract safe configuration elements
            for key, value in config.items():
                if key not in ['api_keys', 'tokens', 'passwords']:
                    sanitized_config[key] = value
        
        log_message = f"[CREW INITIALIZED] Job {job_id} - Config: {sanitized_config}"
        
        enqueue_log(
            execution_id=job_id,
            content=log_message,
            timestamp=timestamp,
            group_context=group_context
        )
        
        logger.info(f"[ExecutionCallback][{job_id}] Crew initialization logged")
        
    except Exception as e:
        logger.error(f"[ExecutionCallback][{job_id}] Error logging crew initialization: {e}", exc_info=True)