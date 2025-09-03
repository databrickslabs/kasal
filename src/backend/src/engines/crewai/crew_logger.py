"""
Comprehensive logging solution for the CrewAI engine.

This module provides a centralized approach to capturing and routing all CrewAI logs,
including console output, event bus messages, and standard logging.
"""

import logging
import traceback
import warnings
from typing import Optional, Any, Dict
import sys
import io
import threading
from contextlib import contextmanager
from datetime import datetime

# Suppress known deprecation warnings from third-party libraries
warnings.filterwarnings("ignore", category=DeprecationWarning, module="httpx")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="chromadb")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="websockets")
warnings.filterwarnings("ignore", message=".*Use 'content=.*' to upload raw bytes/text content.*")
warnings.filterwarnings("ignore", message=".*Accessing the 'model_fields' attribute on the instance is deprecated.*")
warnings.filterwarnings("ignore", message=".*remove second argument of ws_handler.*")

# Import CrewAI's Printer for output redirection (still needed)
from crewai.utilities.printer import Printer

# Import core logger
from src.core.logger import LoggerManager

# Import queue services
from src.services.execution_logs_queue import enqueue_log

# Import group context
from src.utils.user_context import GroupContext

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
    
    def setup_for_job(self, job_id: str, group_context: GroupContext = None) -> None:
        """
        Set up comprehensive logging for a specific job.
        
        NOTE: Event handling is now done via execution-scoped callbacks passed to crew.kickoff()
        This method now focuses on stdout/stderr capture and printer patching.
        
        Args:
            job_id: The execution/job ID
            group_context: Group context for logging isolation
        """
        if job_id in self._active_jobs:
            logger.warning(f"CrewLogger already set up for job {job_id}")
            return
            
        # Create a handler for this job
        handler = CrewLoggerHandler(job_id=job_id, group_context=group_context)
        handler.setFormatter(logging.Formatter('[CREW] %(asctime)s - %(levelname)s - %(message)s'))
        
        # Store job info
        self._active_jobs[job_id] = {
            "handler": handler,
            "original_print_method": None,
        }
        
        # Attach handler to crew logger
        self._crew_logger.addHandler(handler)
        
        # Log setup confirmation
        self._crew_logger.info(f"CrewLogger set up for job {job_id} (using execution-scoped callbacks)")
        
        # Override CrewAI's Printer (still needed for print() statements)
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
            
            # Create reference to crew logger and active jobs for use in custom_print
            crew_logger = self._crew_logger
            active_jobs = self._active_jobs
            
            # Define helper function for filtering content first
            def _should_filter_content(content: str) -> bool:
                """Filter out noisy/debug content that clutters logs."""
                content_lower = content.lower().strip()
                
                # Filter out debug messages
                if content_lower.startswith('debug:'):
                    return True
                    
                # Filter out LiteLLM info messages
                if 'litellm.info:' in content_lower or 'provider list:' in content_lower:
                    return True
                    
                # Filter out empty lines and separators
                if not content.strip() or content.strip() in ['│', '╭', '╰', '─']:
                    return True
                    
                # Filter out repetitive tenant context debug
                if 'created tenant context:' in content_lower and 'primary_tenant_id' in content_lower:
                    return True
                    
                return False
            
            # Override CrewAI's print method to redirect to our logger
            def custom_print(self, content: str, color: Optional[str] = None):
                # Filter out noise and debug messages
                if content.strip() and not _should_filter_content(content):
                    # Get group context for this job if available
                    group_context = None
                    if job_id in active_jobs:
                        job_handler = active_jobs[job_id]["handler"]
                        group_context = getattr(job_handler, 'group_context', None)
                    
                    # Log with simple formatting
                    log_message = f"CREW: {content.strip()}"
                    crew_logger.info(log_message)
                    # Also directly enqueue to ensure it reaches the execution logs table
                    enqueue_log(
                        execution_id=job_id, 
                        content=f"[CREW] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - {log_message}", 
                        group_context=group_context
                    )
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
            # Restore original stdout/stderr with protection
            try:
                if original_stdout and hasattr(original_stdout, 'write'):
                    sys.stdout = original_stdout
                if original_stderr and hasattr(original_stderr, 'write'):
                    sys.stderr = original_stderr
            except Exception:
                # If restoration fails, ensure we have working streams
                import sys as fallback_sys
                sys.stdout = fallback_sys.__stdout__
                sys.stderr = fallback_sys.__stderr__
            
            # Process captured output
            try:
                stdout_content = stdout_capture.getvalue()
                stderr_content = stderr_capture.getvalue()
            except ValueError:
                # StringIO was closed - handle gracefully
                stdout_content = ""
                stderr_content = ""
            
            # Log any stdout/stderr content directly to ensure they reach the execution logs table
            if stdout_content:
                for line in stdout_content.splitlines():
                    if line.strip():
                        # Get group context for this job if available
                        group_context = None
                        if job_id in self._active_jobs:
                            job_handler = self._active_jobs[job_id]["handler"]
                            group_context = getattr(job_handler, 'group_context', None)
                        
                        # Log both to crew logger AND directly enqueue for database
                        log_message = f"STDOUT: {line.strip()}"
                        self._crew_logger.info(log_message)
                        # Direct enqueue to ensure it reaches the execution logs table
                        enqueue_log(
                            execution_id=job_id, 
                            content=f"[CREW] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - {log_message}", 
                            group_context=group_context
                        )
            
            if stderr_content:
                for line in stderr_content.splitlines():
                    if line.strip():
                        # Get group context for this job if available
                        group_context = None
                        if job_id in self._active_jobs:
                            job_handler = self._active_jobs[job_id]["handler"]
                            group_context = getattr(job_handler, 'group_context', None)
                        
                        # Log both to crew logger AND directly enqueue for database
                        log_message = f"STDERR: {line.strip()}"
                        self._crew_logger.error(log_message)
                        # Direct enqueue to ensure it reaches the execution logs table
                        enqueue_log(
                            execution_id=job_id, 
                            content=f"[CREW] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - ERROR - {log_message}", 
                            group_context=group_context
                        )
            
            # Clean up
            stdout_capture.close()
            stderr_capture.close()


class CrewLoggerHandler(logging.Handler):
    """
    Custom logging handler that captures logs from the crew logger
    and redirects them to the job_output_queue.
    """
    
    def __init__(self, job_id: str, group_context: GroupContext = None):
        """
        Initialize the handler with a job ID.
        
        Args:
            job_id: The execution/job ID to associate logs with
            group_context: Group context for logging isolation
        """
        super().__init__()
        self.job_id = job_id
        self.group_context = group_context
        
    def emit(self, record: logging.LogRecord):
        """
        Process a log record by sending it to the job output queue.
        
        Args:
            record: The logging record to process
        """
        try:
            # Format the log message
            log_message = self.format(record)
            
            # Enqueue the log message with the job ID and group context
            enqueue_log(execution_id=self.job_id, content=log_message, group_context=self.group_context)
            
        except Exception as e:
            # Don't use logging here to avoid potential infinite recursion
            # Use the last resort handler to avoid stream redirection issues
            try:
                import sys
                if hasattr(sys.stderr, 'write') and not sys.stderr.closed:
                    sys.stderr.write(f"Error in CrewLoggerHandler.emit: {e}\n")
                    sys.stderr.flush()
            except:
                # If even that fails, silently ignore to prevent cascading failures
                pass


# Create singleton instance
crew_logger = CrewLogger() 