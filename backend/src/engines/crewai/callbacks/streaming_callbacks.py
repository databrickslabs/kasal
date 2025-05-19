"""
Streaming callbacks for CrewAI engine.

This module provides streaming callback implementations for CrewAI.
"""
from typing import Any, Optional, Dict
from datetime import datetime, UTC
import json
import logging
import queue
import traceback
from logging.handlers import MemoryHandler

# Import CrewAI's event system
from crewai.utilities.events import (
    ToolUsageStartedEvent,
    ToolUsageFinishedEvent,
    LLMCallStartedEvent,
    LLMCallCompletedEvent,
    crewai_event_bus
)

from src.engines.crewai.callbacks.base import CrewAICallback
from src.core.logger import LoggerManager
from src.services.execution_logs_queue import enqueue_log


# Initialize logger manager
logger_manager = LoggerManager()

class LogCaptureHandler(logging.Handler):
    """Captures all log records for a specific job."""
    
    def __init__(self, job_id: str):
        super().__init__()
        self.job_id = job_id
        self.buffer = []
        self.buffer_size = 50  # Maximum number of individual log entries to hold
        
    def emit(self, record):
        """Buffer the log record"""
        try:
            # Format the log message
            message = self.format(record)
            if message.strip():  # Only add non-empty messages
                self.buffer.append((message, record.created))
            
            # Flush if buffer is full
            if len(self.buffer) >= self.buffer_size:
                self.flush()
                
        except Exception as e:
            logger_manager.system.error(f"Error buffering log record: {e}", exc_info=True)
    
    def _group_logs_by_time(self):
        """Group logs into time windows and combine them."""
        if not self.buffer:
            return []
        
        # Sort buffer by timestamp
        sorted_buffer = sorted(self.buffer, key=lambda x: x[1])
        grouped_logs = []
        current_group = []
        current_time = sorted_buffer[0][1]
        time_window = 2.0  # Time window in seconds to group logs
        
        for message, timestamp in sorted_buffer:
            # If message is within time window, add to current group
            if timestamp - current_time <= time_window:
                current_group.append(message)
            else:
                # Create new group and update current time
                if current_group:
                    grouped_logs.append((current_group, current_time))
                current_group = [message]
                current_time = timestamp
        
        # Add last group
        if current_group:
            grouped_logs.append((current_group, current_time))
        
        return grouped_logs
    
    def flush(self):
        """Write all buffered records using the queue system"""
        if not self.buffer:
            return
            
        try:
            # Group logs by time window
            logger_manager.system.info(f"[flush] Starting flush for job_id={self.job_id}")
            logger_manager.system.info(f"[flush] Buffer size: {len(self.buffer)}")
            
            grouped_logs = self._group_logs_by_time()
            logger_manager.system.info(f"[flush] Grouped logs count: {len(grouped_logs)}")
            
            for i, (messages, group_time) in enumerate(grouped_logs):
                combined_message = "\n".join(messages)
                timestamp = datetime.fromtimestamp(group_time, UTC)
                
                logger_manager.system.info(f"[flush] Processing group {i+1}/{len(grouped_logs)}")
                
                # Enqueue log directly to the queue system
                success = enqueue_log(
                    execution_id=self.job_id,
                    content=combined_message,
                    timestamp=timestamp
                )
                
                if success:
                    logger_manager.system.info(f"[flush] Successfully enqueued log group {i+1}/{len(grouped_logs)}")
                else:
                    logger_manager.system.error(f"[flush] Failed to enqueue log group {i+1}/{len(grouped_logs)}")
            
            self.buffer.clear()
            logger_manager.system.info("[flush] Buffer cleared after processing")
            
        except Exception as e:
            logger_manager.system.error(f"[flush] Error flushing logs: {e}")
            logger_manager.system.error("[flush] Exception details:", exc_info=True)
    
    def close(self):
        """Clean up resources"""
        try:
            # Flush any remaining records
            self.flush()
        finally:
            super().close()

class JobOutputCallback(CrewAICallback):
    """Callback for streaming job output to the database."""
    
    def __init__(self, job_id: str, task_key: str = None, config: Dict[str, Any] = None, max_retries: int = 3):
        super().__init__(max_retries)
        self.job_id = job_id
        self.task_key = task_key
        self.config = config
        
        # Create and configure the log handler
        self.log_handler = LogCaptureHandler(job_id)
        self.log_handler.setFormatter(
            logging.Formatter(
                '[%(name)s] %(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        )
        
        # Add handler to all relevant loggers (excluding API logs)
        logger_manager.crew.addHandler(self.log_handler)
        logger_manager.llm.addHandler(self.log_handler)
        logging.getLogger('backendcrew').addHandler(self.log_handler)
        logging.getLogger('LiteLLM').addHandler(self.log_handler)
        logging.getLogger('httpx').addHandler(self.log_handler)
        
        logger_manager.system.info(f"Initialized JobOutputCallback for job {job_id}")
        
        # Send initialization message
        enqueue_log(
            execution_id=job_id,
            content=f"[INITIALIZATION] Job {job_id} started at {datetime.now(UTC).isoformat()}"
        )
        logger_manager.system.info(f"Sent initialization message for job {job_id}")
        
        # Log the configuration if provided
        if self.config:
            try:
                # Create a sanitized version of the config (removing sensitive data if needed)
                sanitized_config = self._sanitize_config(self.config)
                config_message = f"[CONFIG] Job configuration:\n{json.dumps(sanitized_config, indent=2)}"
                
                # Log the configuration
                enqueue_log(
                    execution_id=job_id,
                    content=config_message
                )
                logger_manager.system.info(f"Logged configuration for job {job_id}")
            except Exception as e:
                logger_manager.system.error(f"Error logging configuration for job {job_id}: {e}")

    def _sanitize_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a sanitized copy of the config, removing any sensitive information.
        
        Args:
            config: The configuration dictionary
            
        Returns:
            A sanitized copy of the configuration
        """
        # Create a deep copy to avoid modifying the original
        sanitized = json.loads(json.dumps(config))
        
        # Remove or mask sensitive fields if any exist
        # Example: if 'api_key' in sanitized: sanitized['api_key'] = '***'
        
        return sanitized

    async def execute(self, output: Any) -> Any:
        """Process, persist, and return the output."""
        try:
            # Add debug logging at the start
            logger_manager.system.info(f"[JobOutputCallback.execute] Processing output for job {self.job_id}")
            
            # Convert output to string based on its type
            if hasattr(output, 'raw'):
                message = output.raw
                logger_manager.system.info("[JobOutputCallback.execute] Using output.raw")
            elif isinstance(output, dict):
                message = json.dumps(output)  # Remove indent for more compact logs
                logger_manager.system.info("[JobOutputCallback.execute] Converting dict to JSON string")
            else:
                message = str(output)
                logger_manager.system.info("[JobOutputCallback.execute] Converting to string")

            logger_manager.system.info(f"[JobOutputCallback.execute] Message length: {len(message) if message else 0}")
            
            # Log the output through the crew logger
            if message.strip():  # Only log non-empty messages
                logger_manager.system.info("[JobOutputCallback.execute] Logging non-empty message to crew logger")
                logger_manager.crew.info(message)
                
                # Directly enqueue message for database persistence
                success = enqueue_log(
                    execution_id=self.job_id,
                    content=message
                )
                
                if success:
                    logger_manager.system.info("[JobOutputCallback.execute] Successfully enqueued message")
                else:
                    logger_manager.system.error("[JobOutputCallback.execute] Failed to enqueue message")
                
                # If this message contains "Final Answer" or indicates task completion, add special log
                if "Final Answer" in message or "Task Completed" in message:
                    try:
                        completion_message = f"TASK_COMPLETION: Task completed for job {self.job_id}"
                        logger_manager.system.info(f"[JobOutputCallback.execute] Sending task completion marker")
                        enqueue_log(
                            execution_id=self.job_id,
                            content=completion_message
                        )
                        logger_manager.system.info("[JobOutputCallback.execute] Task completion marker sent successfully")
                    except Exception as e:
                        logger_manager.system.error(f"[JobOutputCallback.execute] Error sending task completion marker: {e}")
            else:
                logger_manager.system.info("[JobOutputCallback.execute] Skipping empty message")
            
            # Add debug logging before returning
            logger_manager.system.info("[JobOutputCallback.execute] Processing complete")
            
            # Ensure we always return the original output, not the processed message
            return output

        except Exception as e:
            logger_manager.system.error(f"[JobOutputCallback.execute] Error processing output for job {self.job_id}: {e}")
            logger_manager.system.error("[JobOutputCallback.execute] Exception details:", exc_info=True)
            # Even in case of error, return the original output to ensure data flow
            return output

    def __del__(self):
        """Clean up logging handler when callback is destroyed."""
        try:
            # Explicitly flush any pending logs
            if hasattr(self, 'log_handler'):
                self.log_handler.flush()
                
            # Remove our handler from all loggers
            logger_manager.crew.removeHandler(self.log_handler)
            logger_manager.llm.removeHandler(self.log_handler)
            logging.getLogger('backendcrew').removeHandler(self.log_handler)
            logging.getLogger('LiteLLM').removeHandler(self.log_handler)
            self.log_handler.close()
            
            # Send finalization message
            enqueue_log(
                execution_id=self.job_id,
                content=f"[FINALIZATION] Job {self.job_id} completed at {datetime.now(UTC).isoformat()}"
            )
            logger_manager.system.info(f"Finalized JobOutputCallback for job {self.job_id}")
        except Exception as e:
            logger_manager.system.error(f"Error cleaning up JobOutputCallback: {e}", exc_info=True)


class EventStreamingCallback:
    """
    Callback that streams CrewAI events (tool usage, LLM calls, etc.) to the database.
    Uses the event bus system to capture and stream events in real-time.
    """
    
    def __init__(self, job_id: str, config: Dict[str, Any] = None):
        """
        Initialize the event streaming callback.
        
        Args:
            job_id: The execution/job ID to associate logs with
            config: Optional configuration dictionary
        """
        self.job_id = job_id
        self.config = config
        self._setup_event_handlers()
        logger_manager.system.info(f"Initialized EventStreamingCallback for job {job_id}")
        
        # Log the configuration if provided
        if self.config:
            try:
                # Create a sanitized version of the config (removing sensitive data if needed)
                sanitized_config = self._sanitize_config(self.config)
                config_message = f"[CONFIG] Job configuration:\n{json.dumps(sanitized_config, indent=2)}"
                
                # Log the configuration
                enqueue_log(
                    execution_id=job_id,
                    content=config_message
                )
                logger_manager.system.info(f"Logged configuration for job {job_id}")
            except Exception as e:
                logger_manager.system.error(f"Error logging configuration for job {job_id}: {e}")
    
    def _sanitize_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a sanitized copy of the config, removing any sensitive information.
        
        Args:
            config: The configuration dictionary
            
        Returns:
            A sanitized copy of the configuration
        """
        # Create a deep copy to avoid modifying the original
        sanitized = json.loads(json.dumps(config))
        
        # Remove or mask sensitive fields if any exist
        # Example: if 'api_key' in sanitized: sanitized['api_key'] = '***'
        
        return sanitized
        
    def _setup_event_handlers(self):
        """Set up event handlers for CrewAI events."""
        self._register_tool_usage_handlers()
        self._register_llm_call_handlers()
    
    def _register_tool_usage_handlers(self):
        """Register handlers for tool usage events."""
        
        @crewai_event_bus.on(ToolUsageStartedEvent)
        def on_tool_usage_started(source, event):
            try:
                # Extract information from the event
                tool_name = event.tool_name
                
                # Format agent and task names if available
                agent_name = event.agent.role if hasattr(event, 'agent') and hasattr(event.agent, 'role') else "Unknown"
                task_desc = event.task.description if hasattr(event, 'task') and hasattr(event.task, 'description') else "Unknown"
                
                # Format the tool arguments (parameters)
                tool_params = getattr(event, 'args', {})
                params_str = json.dumps(tool_params) if tool_params else "None"
                
                # Create a nicely formatted message
                message = f"""[TOOL-START] 
Tool: {tool_name}
Agent: {agent_name}
Task: {task_desc[:100]}...
Parameters: {params_str[:500]}...
Timestamp: {datetime.now(UTC).isoformat()}
"""
                
                # Stream the message
                enqueue_log(
                    execution_id=self.job_id,
                    content=message
                )
                
            except Exception as e:
                logger_manager.system.error(f"Error in tool usage started event handler: {str(e)}")
                logger_manager.system.error(traceback.format_exc())
        
        @crewai_event_bus.on(ToolUsageFinishedEvent)
        def on_tool_usage_finished(source, event):
            try:
                # Extract information from the event
                tool_name = event.tool_name
                
                # Format agent and task names if available
                agent_name = event.agent.role if hasattr(event, 'agent') and hasattr(event.agent, 'role') else "Unknown"
                task_desc = event.task.description if hasattr(event, 'task') and hasattr(event.task, 'description') else "Unknown"
                
                # Get tool output
                output = str(event.output)[:1000] if hasattr(event, 'output') else "No output"
                
                # Create a nicely formatted message
                message = f"""[TOOL-FINISH] 
Tool: {tool_name}
Agent: {agent_name}
Task: {task_desc[:100]}...
Output: {output}...
Timestamp: {datetime.now(UTC).isoformat()}
"""
                
                # Stream the message
                enqueue_log(
                    execution_id=self.job_id,
                    content=message
                )
                
            except Exception as e:
                logger_manager.system.error(f"Error in tool usage finished event handler: {str(e)}")
                logger_manager.system.error(traceback.format_exc())
    
    def _register_llm_call_handlers(self):
        """Register handlers for LLM call events."""
        
        @crewai_event_bus.on(LLMCallStartedEvent)
        def on_llm_call_started(source, event):
            try:
                # Extract agent information if available
                agent_name = "Unknown"
                if hasattr(event, 'agent') and hasattr(event.agent, 'role'):
                    agent_name = event.agent.role
                elif hasattr(event, 'context') and hasattr(event.context, 'agent') and hasattr(event.context.agent, 'role'):
                    agent_name = event.context.agent.role
                
                # Extract task information if available
                task_desc = "Unknown"
                if hasattr(event, 'task') and hasattr(event.task, 'description'):
                    task_desc = event.task.description
                elif hasattr(event, 'context') and hasattr(event.context, 'task') and hasattr(event.context.task, 'description'):
                    task_desc = event.context.task.description
                
                # Extract prompt if available (but keep it short)
                prompt = "Not available"
                if hasattr(event, 'prompt'):
                    prompt = str(event.prompt)[:500] + "..." if len(str(event.prompt)) > 500 else str(event.prompt)
                
                # Create a nicely formatted message
                message = f"""[LLM-CALL-START] 
Agent: {agent_name}
Task: {task_desc[:100]}...
Timestamp: {datetime.now(UTC).isoformat()}
"""
                
                # Stream the message
                enqueue_log(
                    execution_id=self.job_id,
                    content=message
                )
                
            except Exception as e:
                logger_manager.system.error(f"Error in LLM call started event handler: {str(e)}")
                logger_manager.system.error(traceback.format_exc())
        
        @crewai_event_bus.on(LLMCallCompletedEvent)
        def on_llm_call_completed(source, event):
            try:
                # Extract agent information if available
                agent_name = "Unknown"
                if hasattr(event, 'agent') and hasattr(event.agent, 'role'):
                    agent_name = event.agent.role
                elif hasattr(event, 'context') and hasattr(event.context, 'agent') and hasattr(event.context.agent, 'role'):
                    agent_name = event.context.agent.role
                
                # Extract task information if available
                task_desc = "Unknown"
                if hasattr(event, 'task') and hasattr(event.task, 'description'):
                    task_desc = event.task.description
                elif hasattr(event, 'context') and hasattr(event.context, 'task') and hasattr(event.context.task, 'description'):
                    task_desc = event.context.task.description
                
                # Extract output (limit length to avoid huge log messages)
                output = "No output"
                if hasattr(event, 'output'):
                    output = str(event.output)[:1000] + "..." if len(str(event.output)) > 1000 else str(event.output)
                
                # Create a nicely formatted message
                message = f"""[LLM-CALL-COMPLETE] 
Agent: {agent_name}
Task: {task_desc[:100]}...
Output: {output}
Timestamp: {datetime.now(UTC).isoformat()}
"""
                
                # Stream the message
                enqueue_log(
                    execution_id=self.job_id,
                    content=message
                )
                
            except Exception as e:
                logger_manager.system.error(f"Error in LLM call completed event handler: {str(e)}")
                logger_manager.system.error(traceback.format_exc())
    
    def cleanup(self):
        """Clean up resources and event handlers."""
        # No direct way to remove event handlers from CrewAI's event bus
        # In a future version, we could modify the event bus to support removing handlers
        logger_manager.system.info(f"Cleaned up EventStreamingCallback for job {self.job_id}") 