"""
Comprehensive logging solution for the CrewAI engine.

This module provides a centralized approach to capturing and routing all CrewAI logs,
including console output, event bus messages, and standard logging.
"""

import logging
import traceback
from typing import Optional, Any, Dict
import sys
import io
import threading
from contextlib import contextmanager
from datetime import datetime

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

# Try to import additional events if available
try:
    from crewai.utilities.events import (
        AgentExecutionErrorEvent,
        ToolUsageErrorEvent,
        TaskEvaluationEvent,
        CrewTestStartedEvent,
        CrewTestCompletedEvent,
        CrewTestFailedEvent,
        CrewTrainStartedEvent,
        CrewTrainCompletedEvent,
        CrewTrainFailedEvent
    )
    EXTENDED_EVENTS_AVAILABLE = True
except ImportError:
    # These events might not be available in all CrewAI versions
    EXTENDED_EVENTS_AVAILABLE = False
from crewai.utilities.printer import Printer

# Import core logger
from src.core.logger import LoggerManager

# Import queue services
from src.services.execution_logs_queue import enqueue_log

# Import tenant context
from src.utils.user_context import TenantContext

# Configure logger
logger = logging.getLogger(__name__)

# Log extended events availability after logger is configured
if not EXTENDED_EVENTS_AVAILABLE:
    logger.info("Some extended CrewAI events are not available in this version")

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
    
    def setup_for_job(self, job_id: str, tenant_context: TenantContext = None) -> None:
        """
        Set up comprehensive logging for a specific job.
        
        Args:
            job_id: The execution/job ID
            tenant_context: Tenant context for logging isolation
        """
        if job_id in self._active_jobs:
            logger.warning(f"CrewLogger already set up for job {job_id}")
            return
            
        # Create a handler for this job
        handler = CrewLoggerHandler(job_id=job_id, tenant_context=tenant_context)
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
        Register listeners for all relevant CrewAI events with detailed formatting.
        
        Args:
            job_id: The execution/job ID
        """
        # Register Crew-level events
        self._register_crew_events(job_id)
        
        # Register Agent-level events  
        self._register_agent_events(job_id)
        
        # Register Task-level events
        self._register_task_events(job_id)
        
        # Register Tool usage events
        self._register_tool_events(job_id)
        
        # Register LLM call events
        self._register_llm_events(job_id)
        
        # Register extended events if available
        if EXTENDED_EVENTS_AVAILABLE:
            self._register_extended_events(job_id)
    
    def _register_crew_events(self, job_id: str) -> None:
        """Register crew-level event handlers."""
        
        @crewai_event_bus.on(CrewKickoffStartedEvent)
        def on_crew_kickoff_started(source, event):
            try:
                crew_name = getattr(event, 'crew_name', 'Unknown Crew')
                log_message = f"CREW STARTED: {crew_name} (Job: {job_id})"
                self._crew_logger.info(log_message)
                
                # Also directly enqueue to ensure it reaches the execution logs table
                tenant_context = None
                if job_id in self._active_jobs:
                    job_handler = self._active_jobs[job_id]["handler"]
                    tenant_context = getattr(job_handler, 'tenant_context', None)
                
                enqueue_log(
                    execution_id=job_id, 
                    content=f"[CREW] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - {log_message}", 
                    tenant_context=tenant_context
                )
            except Exception as e:
                self._crew_logger.error(f"Error in crew kickoff started handler: {e}")
        
        @crewai_event_bus.on(CrewKickoffCompletedEvent)  
        def on_crew_kickoff_completed(source, event):
            try:
                crew_name = getattr(event, 'crew_name', 'Unknown Crew')
                output = getattr(event, 'output', 'No output provided')
                output_preview = str(output)[:200] + "..." if len(str(output)) > 200 else str(output)
                log_message = f"CREW COMPLETED: {crew_name} (Job: {job_id}) - Output: {output_preview}"
                self._crew_logger.info(log_message)
                
                # Also directly enqueue to ensure it reaches the execution logs table
                tenant_context = None
                if job_id in self._active_jobs:
                    job_handler = self._active_jobs[job_id]["handler"]
                    tenant_context = getattr(job_handler, 'tenant_context', None)
                
                enqueue_log(
                    execution_id=job_id, 
                    content=f"[CREW] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - {log_message}", 
                    tenant_context=tenant_context
                )
            except Exception as e:
                self._crew_logger.error(f"Error in crew kickoff completed handler: {e}")
        
        @crewai_event_bus.on(CrewKickoffFailedEvent)
        def on_crew_kickoff_failed(source, event):
            try:
                crew_name = getattr(event, 'crew_name', 'Unknown Crew')
                error = getattr(event, 'error', 'Unknown error')
                self._crew_logger.error(f"CREW FAILED: {crew_name} (Job: {job_id}) - Error: {str(error)}")
            except Exception as e:
                self._crew_logger.error(f"Error in crew kickoff failed handler: {e}")
    
    def _register_agent_events(self, job_id: str) -> None:
        """Register agent-level event handlers."""
        
        @crewai_event_bus.on(AgentExecutionStartedEvent)
        def on_agent_execution_started(source, event):
            try:
                agent_role = getattr(event.agent, 'role', 'Unknown Agent') if hasattr(event, 'agent') else 'Unknown Agent'
                task_desc = getattr(event.task, 'description', 'Unknown Task') if hasattr(event, 'task') else 'Unknown Task'
                task_preview = task_desc[:100] + "..." if len(task_desc) > 100 else task_desc
                log_message = f"AGENT STARTED: {agent_role} - Task: {task_preview}"
                self._crew_logger.info(log_message)
                
                # Also directly enqueue to ensure it reaches the execution logs table
                tenant_context = None
                if job_id in self._active_jobs:
                    job_handler = self._active_jobs[job_id]["handler"]
                    tenant_context = getattr(job_handler, 'tenant_context', None)
                
                enqueue_log(
                    execution_id=job_id, 
                    content=f"[CREW] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - {log_message}", 
                    tenant_context=tenant_context
                )
            except Exception as e:
                self._crew_logger.error(f"Error in agent execution started handler: {e}")
        
        @crewai_event_bus.on(AgentExecutionCompletedEvent)
        def on_agent_execution_completed(source, event):
            try:
                agent_role = getattr(event.agent, 'role', 'Unknown Agent') if hasattr(event, 'agent') else 'Unknown Agent'
                task_desc = getattr(event.task, 'description', 'Unknown Task') if hasattr(event, 'task') else 'Unknown Task'
                task_preview = task_desc[:100] + "..." if len(task_desc) > 100 else task_desc
                output = getattr(event, 'output', 'No output')
                output_preview = str(output)[:150] + "..." if len(str(output)) > 150 else str(output)
                self._crew_logger.info(f"AGENT COMPLETED: {agent_role} - Task: {task_preview} - Output: {output_preview}")
            except Exception as e:
                self._crew_logger.error(f"Error in agent execution completed handler: {e}")
    
    def _register_task_events(self, job_id: str) -> None:
        """Register task-level event handlers."""
        
        @crewai_event_bus.on(TaskStartedEvent)
        def on_task_started(source, event):
            try:
                task_desc = getattr(event.task, 'description', 'Unknown Task') if hasattr(event, 'task') else 'Unknown Task'
                agent_role = getattr(event.task.agent, 'role', 'Unknown Agent') if hasattr(event, 'task') and hasattr(event.task, 'agent') else 'Unknown Agent'
                task_preview = task_desc[:120] + "..." if len(task_desc) > 120 else task_desc
                log_message = f"TASK STARTED: {task_preview} (Agent: {agent_role})"
                self._crew_logger.info(log_message)
                
                # Also directly enqueue to ensure it reaches the execution logs table
                tenant_context = None
                if job_id in self._active_jobs:
                    job_handler = self._active_jobs[job_id]["handler"]
                    tenant_context = getattr(job_handler, 'tenant_context', None)
                
                enqueue_log(
                    execution_id=job_id, 
                    content=f"[CREW] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - {log_message}", 
                    tenant_context=tenant_context
                )
            except Exception as e:
                self._crew_logger.error(f"Error in task started handler: {e}")
        
        @crewai_event_bus.on(TaskCompletedEvent)
        def on_task_completed(source, event):
            try:
                task_desc = getattr(event.task, 'description', 'Unknown Task') if hasattr(event, 'task') else 'Unknown Task'
                agent_role = getattr(event.task.agent, 'role', 'Unknown Agent') if hasattr(event, 'task') and hasattr(event.task, 'agent') else 'Unknown Agent'
                task_preview = task_desc[:120] + "..." if len(task_desc) > 120 else task_desc
                output = getattr(event, 'output', 'No output')
                output_preview = str(output)[:150] + "..." if len(str(output)) > 150 else str(output)
                self._crew_logger.info(f"TASK COMPLETED: {task_preview} (Agent: {agent_role}) - Result: {output_preview}")
            except Exception as e:
                self._crew_logger.error(f"Error in task completed handler: {e}")
    
    def _register_tool_events(self, job_id: str) -> None:
        """Register tool usage event handlers."""
        
        @crewai_event_bus.on(ToolUsageStartedEvent)
        def on_tool_usage_started(source, event):
            try:
                tool_name = getattr(event, 'tool_name', 'Unknown Tool')
                agent_role = getattr(event.agent, 'role', 'Unknown Agent') if hasattr(event, 'agent') else 'Unknown Agent'
                tool_args = getattr(event, 'args', {})
                args_preview = str(tool_args)[:100] + "..." if len(str(tool_args)) > 100 else str(tool_args)
                self._crew_logger.info(f"TOOL STARTED: {tool_name} (Agent: {agent_role}) - Args: {args_preview}")
            except Exception as e:
                self._crew_logger.error(f"Error in tool usage started handler: {e}")
        
        @crewai_event_bus.on(ToolUsageFinishedEvent)
        def on_tool_usage_finished(source, event):
            try:
                tool_name = getattr(event, 'tool_name', 'Unknown Tool')
                agent_role = getattr(event.agent, 'role', 'Unknown Agent') if hasattr(event, 'agent') else 'Unknown Agent'
                output = getattr(event, 'output', 'No output')
                output_preview = str(output)[:120] + "..." if len(str(output)) > 120 else str(output)
                self._crew_logger.info(f"TOOL COMPLETED: {tool_name} (Agent: {agent_role}) - Result: {output_preview}")
            except Exception as e:
                self._crew_logger.error(f"Error in tool usage finished handler: {e}")
    
    def _register_llm_events(self, job_id: str) -> None:
        """Register LLM call event handlers."""
        
        @crewai_event_bus.on(LLMCallStartedEvent)
        def on_llm_call_started(source, event):
            try:
                agent_role = getattr(event.agent, 'role', 'Unknown Agent') if hasattr(event, 'agent') else 'Unknown Agent'
                self._crew_logger.info(f"LLM CALL STARTED: Agent {agent_role}")
            except Exception as e:
                self._crew_logger.error(f"Error in LLM call started handler: {e}")
        
        @crewai_event_bus.on(LLMCallCompletedEvent)
        def on_llm_call_completed(source, event):
            try:
                agent_role = getattr(event.agent, 'role', 'Unknown Agent') if hasattr(event, 'agent') else 'Unknown Agent'
                output = getattr(event, 'output', 'No output')
                output_preview = str(output)[:100] + "..." if len(str(output)) > 100 else str(output)
                self._crew_logger.info(f"LLM CALL COMPLETED: Agent {agent_role} - Response: {output_preview}")
            except Exception as e:
                self._crew_logger.error(f"Error in LLM call completed handler: {e}")
    
    def _register_extended_events(self, job_id: str) -> None:
        """Register extended event handlers if available."""
        try:
            # Only register if the events were successfully imported
            if 'AgentExecutionErrorEvent' in globals():
                @crewai_event_bus.on(AgentExecutionErrorEvent)
                def on_agent_execution_error(source, event):
                    try:
                        agent_role = getattr(event.agent, 'role', 'Unknown Agent') if hasattr(event, 'agent') else 'Unknown Agent'
                        error = getattr(event, 'error', 'Unknown error')
                        self._crew_logger.error(f"AGENT ERROR: {agent_role} - Error: {str(error)}")
                    except Exception as e:
                        self._crew_logger.error(f"Error in agent execution error handler: {e}")
            
            if 'ToolUsageErrorEvent' in globals():
                @crewai_event_bus.on(ToolUsageErrorEvent)
                def on_tool_usage_error(source, event):
                    try:
                        tool_name = getattr(event, 'tool_name', 'Unknown Tool')
                        error = getattr(event, 'error', 'Unknown error')
                        self._crew_logger.error(f"TOOL ERROR: {tool_name} - Error: {str(error)}")
                    except Exception as e:
                        self._crew_logger.error(f"Error in tool usage error handler: {e}")
        except Exception as e:
            self._crew_logger.warning(f"Could not register extended events: {e}")
    
    
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
                    # Get tenant context for this job if available
                    tenant_context = None
                    if job_id in active_jobs:
                        job_handler = active_jobs[job_id]["handler"]
                        tenant_context = getattr(job_handler, 'tenant_context', None)
                    
                    # Log with simple formatting
                    log_message = f"CREW: {content.strip()}"
                    crew_logger.info(log_message)
                    # Also directly enqueue to ensure it reaches the execution logs table
                    enqueue_log(
                        execution_id=job_id, 
                        content=f"[CREW] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - {log_message}", 
                        tenant_context=tenant_context
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
            # Restore original stdout/stderr
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            
            # Process captured output
            stdout_content = stdout_capture.getvalue()
            stderr_content = stderr_capture.getvalue()
            
            # Log any stdout/stderr content directly to ensure they reach the execution logs table
            if stdout_content:
                for line in stdout_content.splitlines():
                    if line.strip():
                        # Get tenant context for this job if available
                        tenant_context = None
                        if job_id in self._active_jobs:
                            job_handler = self._active_jobs[job_id]["handler"]
                            tenant_context = getattr(job_handler, 'tenant_context', None)
                        
                        # Log both to crew logger AND directly enqueue for database
                        log_message = f"STDOUT: {line.strip()}"
                        self._crew_logger.info(log_message)
                        # Direct enqueue to ensure it reaches the execution logs table
                        enqueue_log(
                            execution_id=job_id, 
                            content=f"[CREW] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO - {log_message}", 
                            tenant_context=tenant_context
                        )
            
            if stderr_content:
                for line in stderr_content.splitlines():
                    if line.strip():
                        # Get tenant context for this job if available
                        tenant_context = None
                        if job_id in self._active_jobs:
                            job_handler = self._active_jobs[job_id]["handler"]
                            tenant_context = getattr(job_handler, 'tenant_context', None)
                        
                        # Log both to crew logger AND directly enqueue for database
                        log_message = f"STDERR: {line.strip()}"
                        self._crew_logger.error(log_message)
                        # Direct enqueue to ensure it reaches the execution logs table
                        enqueue_log(
                            execution_id=job_id, 
                            content=f"[CREW] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - ERROR - {log_message}", 
                            tenant_context=tenant_context
                        )
            
            # Clean up
            stdout_capture.close()
            stderr_capture.close()


class CrewLoggerHandler(logging.Handler):
    """
    Custom logging handler that captures logs from the crew logger
    and redirects them to the job_output_queue.
    """
    
    def __init__(self, job_id: str, tenant_context: TenantContext = None):
        """
        Initialize the handler with a job ID.
        
        Args:
            job_id: The execution/job ID to associate logs with
            tenant_context: Tenant context for logging isolation
        """
        super().__init__()
        self.job_id = job_id
        self.tenant_context = tenant_context
        
    def emit(self, record: logging.LogRecord):
        """
        Process a log record by sending it to the job output queue.
        
        Args:
            record: The logging record to process
        """
        try:
            # Format the log message
            log_message = self.format(record)
            
            # Enqueue the log message with the job ID and tenant context
            enqueue_log(execution_id=self.job_id, content=log_message, tenant_context=self.tenant_context)
            
        except Exception as e:
            # Don't use logging here to avoid potential infinite recursion
            print(f"Error in CrewLoggerHandler.emit: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)


# Create singleton instance
crew_logger = CrewLogger() 