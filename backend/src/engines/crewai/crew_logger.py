"""
Comprehensive logging solution for the CrewAI engine.

This module provides a centralized approach to capturing and routing all CrewAI logs,
including console output, event bus messages, and standard logging.
"""

import logging
import traceback
from typing import Optional, Any, Dict, Callable
import sys
import io
import threading
from contextlib import contextmanager

# Import CrewAI's event system
from crewai.utilities.events import (
    AgentExecutionCompletedEvent,
    AgentExecutionStartedEvent,
    ToolUsageStartedEvent,
    ToolUsageFinishedEvent,
    LLMCallStartedEvent,
    LLMCallCompletedEvent,
    TaskCompletedEvent,
    TaskStartedEvent,
    CrewKickoffStartedEvent,
    CrewKickoffCompletedEvent,
    CrewKickoffFailedEvent,
    crewai_event_bus
)
from crewai.utilities.printer import Printer

# Import core logger
from src.core.logger import LoggerManager

# Import queue services
from src.services.execution_logs_queue import enqueue_log

# Configure logger
logger = logging.getLogger(__name__)

class CrewLogger:
    """
    Comprehensive logger for the CrewAI engine that integrates with the event bus,
    captures stdout/stderr, and routes logs to the appropriate destinations.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """Ensure singleton instance."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(CrewLogger, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self):
        """Initialize the logger if not already initialized."""
        if not getattr(self, '_initialized', False):
            # Get the crew logger from LoggerManager
            self._crew_logger = LoggerManager.get_instance().crew
            
            # Store original print method for Printer class
            self._original_print_method = None
            
            # Initialize event listeners
            self._event_handlers = {}
            
            # Track active job IDs with their handlers
            self._active_jobs = {}
            
            # Set up CrewAI's standard logging redirection
            self._setup_crewai_logging()
            
            # Mark as initialized
            self._initialized = True
    
    def _setup_crewai_logging(self):
        """Set up redirection for CrewAI's standard logging to our crew logger."""
        try:
            # Get CrewAI's loggers
            crewai_logger = logging.getLogger('crewai')
            
            # Configure CrewAI's logger to use our formatting
            crewai_logger.handlers = []
            crewai_logger.propagate = False
            
            # Create a special handler to redirect to our logger
            crew_logger = self._crew_logger
            
            class CrewAIRedirectHandler(logging.Handler):
                def emit(self, record):
                    # Get the log message
                    msg = self.format(record)
                    # Forward to our crew logger with the same level
                    level = record.levelno
                    crew_logger.log(level, f"CREWAI-LOG: {msg}")
            
            # Add the redirect handler to CrewAI's logger
            formatter = logging.Formatter('%(message)s')
            redirect_handler = CrewAIRedirectHandler()
            redirect_handler.setFormatter(formatter)
            crewai_logger.addHandler(redirect_handler)
            
            # Set log level to DEBUG to capture all logs
            crewai_logger.setLevel(logging.DEBUG)
            
            # Also capture other related loggers
            for logger_name in ['langchain', 'httpx', 'openai']:
                try:
                    related_logger = logging.getLogger(logger_name)
                    related_logger.handlers = []
                    related_logger.propagate = False
                    handler_copy = CrewAIRedirectHandler()
                    handler_copy.setFormatter(formatter)
                    related_logger.addHandler(handler_copy)
                    related_logger.setLevel(logging.DEBUG)
                except Exception as related_err:
                    logger.warning(f"Could not set up redirection for {logger_name}: {str(related_err)}")
            
            logger.info("Successfully set up CrewAI logging redirection")
        except Exception as e:
            logger.error(f"Error setting up CrewAI logging redirection: {str(e)}")
    
    def setup_for_job(self, job_id: str) -> None:
        """
        Set up comprehensive logging for a specific job.
        
        Args:
            job_id: The execution/job ID
        """
        if job_id in self._active_jobs:
            logger.warning(f"CrewLogger already set up for job {job_id}")
            return
            
        # Create a handler for this job
        handler = CrewLoggerHandler(job_id=job_id)
        handler.setFormatter(logging.Formatter('[CREW] %(asctime)s - %(levelname)s - %(message)s'))
        
        # Store job info
        self._active_jobs[job_id] = {
            "handler": handler,
            "original_print_method": None
        }
        
        # Attach handler to crew logger
        self._crew_logger.addHandler(handler)
        
        # Log setup confirmation
        self._crew_logger.info(f"CrewLogger set up for job {job_id}")
        
        # Set up event bus listeners
        self._register_event_listeners(job_id)
        
        # Override CrewAI's Printer
        self._patch_printer(job_id)
    
    def cleanup_for_job(self, job_id: str) -> None:
        """
        Clean up logging setup for a specific job.
        
        Args:
            job_id: The execution/job ID
        """
        if job_id not in self._active_jobs:
            logger.warning(f"No CrewLogger setup found for job {job_id}")
            return
            
        job_info = self._active_jobs[job_id]
        
        # Remove handler from crew logger
        self._crew_logger.removeHandler(job_info["handler"])
        
        # Restore original Printer.print method if we were the one who patched it
        if job_info.get("original_print_method"):
            try:
                Printer.print = job_info["original_print_method"]
                self._crew_logger.info(f"Restored original CrewAI Printer for job {job_id}")
            except Exception as e:
                logger.warning(f"Error restoring original CrewAI Printer: {str(e)}")
        
        # Remove job from active jobs
        del self._active_jobs[job_id]
        
        # Log cleanup confirmation (won't go through our handler since it's removed)
        logger.info(f"CrewLogger cleaned up for job {job_id}")
    
    def _register_event_listeners(self, job_id: str) -> None:
        """
        Register listeners for all relevant CrewAI events.
        
        Args:
            job_id: The execution/job ID
        """
        # Helper to create event handlers
        def create_handler(event_type: str, level: str = "info"):
            def handler(source, event):
                try:
                    # Format the message based on event type and contents
                    message = f"EVENT-{event_type}: "
                    
                    # Add role info if available
                    if hasattr(event, 'agent') and hasattr(event.agent, 'role'):
                        message += f"Agent '{event.agent.role}' "
                    
                    # Add task info if available
                    if hasattr(event, 'task') and hasattr(event.task, 'description'):
                        message += f"Task '{event.task.description[:50]}...' "
                    
                    # Add output info if available and appropriate
                    if hasattr(event, 'output') and event.output is not None:
                        if 'Completed' in event_type:
                            output_sample = str(event.output)[:100]
                            message += f"Output: '{output_sample}...'"
                    
                    # Add tool info if available
                    if hasattr(event, 'tool_name'):
                        message += f"Tool: '{event.tool_name}' "
                    
                    # Log the event
                    method = getattr(self._crew_logger, level)
                    method(message)
                    
                    # Special handling for certain events
                    if event_type == "CrewKickoffCompleted":
                        self._crew_logger.info(f"Crew execution completed successfully for job {job_id}")
                        
                except Exception as e:
                    logger.error(f"Error in event handler for {event_type}: {str(e)}", exc_info=True)
            
            return handler
        
        # Register handlers for all event types
        self._register_event_handler(AgentExecutionStartedEvent, create_handler("AgentExecutionStarted"))
        self._register_event_handler(AgentExecutionCompletedEvent, create_handler("AgentExecutionCompleted"))
        self._register_event_handler(ToolUsageStartedEvent, create_handler("ToolUsageStarted"))
        self._register_event_handler(ToolUsageFinishedEvent, create_handler("ToolUsageFinished"))
        self._register_event_handler(LLMCallStartedEvent, create_handler("LLMCallStarted"))
        self._register_event_handler(LLMCallCompletedEvent, create_handler("LLMCallCompleted"))
        self._register_event_handler(TaskStartedEvent, create_handler("TaskStarted"))
        self._register_event_handler(TaskCompletedEvent, create_handler("TaskCompleted"))
        self._register_event_handler(CrewKickoffStartedEvent, create_handler("CrewKickoffStarted"))
        self._register_event_handler(CrewKickoffCompletedEvent, create_handler("CrewKickoffCompleted"))
        self._register_event_handler(CrewKickoffFailedEvent, create_handler("CrewKickoffFailed", "error"))
    
    def _register_event_handler(self, event_type, handler: Callable) -> None:
        """
        Register a handler for a specific event type.
        
        Args:
            event_type: The event type class
            handler: The handler function
        """
        try:
            crewai_event_bus.on(event_type)(handler)
            logger.debug(f"Registered handler for {event_type.__name__}")
        except Exception as e:
            logger.error(f"Error registering event handler for {event_type.__name__}: {str(e)}")
    
    def _patch_printer(self, job_id: str) -> None:
        """
        Patch CrewAI's Printer class to redirect output to our logger.
        
        Args:
            job_id: The execution/job ID
        """
        try:
            # Save the original print method
            original_print_method = Printer.print
            
            # Store in active jobs
            self._active_jobs[job_id]["original_print_method"] = original_print_method
            
            # Create reference to crew logger for use in custom_print
            crew_logger = self._crew_logger
            
            # Override CrewAI's print method to redirect to our logger
            def custom_print(self, content: str, color: Optional[str] = None):
                # Log to our crew logger
                crew_logger.info(f"CREW-PRINT: {content}")
                # Call the original method to maintain normal behavior
                original_print_method(self, content, color)
            
            # Apply the override
            Printer.print = custom_print
            self._crew_logger.info(f"Successfully redirected CrewAI's Printer output to crew logger for job {job_id}")
            
        except Exception as e:
            self._crew_logger.warning(f"Could not redirect CrewAI's print output: {str(e)}. Some logs may not be captured.")

    @contextmanager
    def capture_stdout_stderr(self, job_id: str):
        """
        Context manager to capture stdout and stderr during execution.
        
        Args:
            job_id: The execution/job ID
            
        Yields:
            None
        """
        # Set up stdout/stderr capture
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()
        
        try:
            # Redirect stdout/stderr
            sys.stdout = stdout_capture
            sys.stderr = stderr_capture
            
            # Yield control back to caller
            yield
            
        finally:
            # Restore original stdout/stderr
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            
            # Process captured output
            stdout_content = stdout_capture.getvalue()
            stderr_content = stderr_capture.getvalue()
            
            # Log any stdout/stderr content as separate lines
            if stdout_content:
                for line in stdout_content.splitlines():
                    if line.strip():
                        self._crew_logger.info(f"CREW-STDOUT: {line.strip()}")
            
            if stderr_content:
                for line in stderr_content.splitlines():
                    if line.strip():
                        self._crew_logger.error(f"CREW-STDERR: {line.strip()}")
            
            # Clean up
            stdout_capture.close()
            stderr_capture.close()


class CrewLoggerHandler(logging.Handler):
    """
    Custom logging handler that captures logs from the crew logger
    and redirects them to the job_output_queue.
    """
    
    def __init__(self, job_id: str):
        """
        Initialize the handler with a job ID.
        
        Args:
            job_id: The execution/job ID to associate logs with
        """
        super().__init__()
        self.job_id = job_id
        
    def emit(self, record: logging.LogRecord):
        """
        Process a log record by sending it to the job output queue.
        
        Args:
            record: The logging record to process
        """
        try:
            # Format the log message
            log_message = self.format(record)
            
            # Enqueue the log message with the job ID
            enqueue_log(execution_id=self.job_id, content=log_message)
        except Exception as e:
            # Don't use logging here to avoid potential infinite recursion
            print(f"Error in CrewLoggerHandler.emit: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)


# Create singleton instance
crew_logger = CrewLogger() 