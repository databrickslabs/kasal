"""
Logging callbacks for CrewAI engine using event listener architecture.

This module provides event listeners for logging agent traces and task completion.
"""
from typing import Any, Optional, Dict
from datetime import datetime, timezone
import logging
import queue
import traceback
import uuid  # Add UUID import
from sqlalchemy import text

# Import CrewAI's event system
from crewai.utilities.events import (
    AgentExecutionCompletedEvent,
    ToolUsageFinishedEvent,
    LLMCallCompletedEvent,
    TaskStartedEvent,
    TaskCompletedEvent,
    CrewKickoffStartedEvent,
    CrewKickoffCompletedEvent
)
from crewai.utilities.events.base_event_listener import BaseEventListener

# Import our queue system
from src.services.trace_queue import get_trace_queue

# Import the job_output_queue
from src.services.execution_logs_queue import enqueue_log, get_job_output_queue

# Import task tracking
from src.services.task_tracking_service import TaskTrackingService
from src.schemas.task_tracking import TaskStatusEnum
from src.core.unit_of_work import SyncUnitOfWork
from src.db.session import SessionLocal
from src.models.task import Task  # Import the Task model directly

logger = logging.getLogger(__name__)

class AgentTraceEventListener(BaseEventListener):
    """Event listener that puts agent traces onto a shared queue for asynchronous processing."""
    
    _init_logged = set()
    # Static task registry to track tasks by job
    _task_registry: Dict[str, Dict[str, str]] = {}
    
    def __init__(self, job_id: str):
        """
        Initialize the event listener.
        
        Args:
            job_id: Unique identifier for the job
        """
        if not job_id or not isinstance(job_id, str):
            raise ValueError("job_id must be a non-empty string")
            
        # Set job_id BEFORE calling super().__init__() 
        # since BaseEventListener calls setup_listeners in its __init__
        self.job_id = job_id
        self._queue = get_trace_queue()
        
        # Initialize task registry for this job
        if job_id not in AgentTraceEventListener._task_registry:
            AgentTraceEventListener._task_registry[job_id] = {}
        
        log_prefix = f"[AgentTraceEventListener][{self.job_id}]"
        if job_id not in AgentTraceEventListener._init_logged:
            logger.info(f"{log_prefix} Initializing with CrewAI event listeners. Using shared trace queue: {id(self._queue)}")
            AgentTraceEventListener._init_logged.add(job_id)
        
        try:
            # Call super().__init__() after setting job_id
            super().__init__()
            logger.info(f"{log_prefix} Successfully registered event listeners")
        except Exception as e:
            logger.error(f"{log_prefix} Error initializing event listener: {e}", exc_info=True)
            raise
    
    def _get_or_create_task_id(self, task_name: str, task_original_id: str) -> str:
        """
        Get an existing task ID from the registry or look up in the database.
        This ensures we use task IDs that match the tasks table.
        
        Args:
            task_name: The name or description of the task
            task_original_id: The original ID from the task
            
        Returns:
            The task ID to use
        """
        registry = AgentTraceEventListener._task_registry[self.job_id]
        registry_key = f"{task_name}:{task_original_id}"
        
        # If we already have this task in our registry, return the ID
        if registry_key in registry:
            return registry[registry_key]
            
        # Try to find the task ID in the database based on the task name/description
        try:
            # Use a dedicated session for this operation
            with SessionLocal() as db:
                # Try exact match by name first
                task = db.query(Task).filter(Task.name == task_name).first()
                
                # If not found by name, try by description (requires a significant substring match)
                if not task and len(task_name) > 20:
                    # Log that we're searching by description
                    logger.info(f"[AgentTraceEventListener][{self.job_id}] Task not found by name, trying description match for: {task_name[:30]}...")
                    
                    # Get all tasks
                    all_tasks = db.query(Task).all()
                    
                    # Check each task's description for a significant match
                    for potential_task in all_tasks:
                        if potential_task.description and task_name in potential_task.description:
                            task = potential_task
                            logger.info(f"[AgentTraceEventListener][{self.job_id}] Found task by description match: {potential_task.id}")
                            break
                
                # If we found a task, use its ID
                if task:
                    task_id = str(task.id)
                    logger.info(f"[AgentTraceEventListener][{self.job_id}] Found existing task in database: {task_name} → {task_id}")
                    registry[registry_key] = task_id
                    return task_id
                else:
                    logger.warning(f"[AgentTraceEventListener][{self.job_id}] Could not find any matching task in database for: {task_name[:50]}")
                    
        except Exception as e:
            logger.error(f"[AgentTraceEventListener][{self.job_id}] Error looking up task ID in database: {e}", exc_info=True)
        
        # If all else fails, create a new UUID
        task_id = str(uuid.uuid4())
        registry[registry_key] = task_id
        logger.info(f"[AgentTraceEventListener][{self.job_id}] Created new task ID: {registry_key} → {task_id}")
        return task_id
    
    def setup_listeners(self, crewai_event_bus):
        """
        Register all event handlers with the CrewAI event bus.
        
        Args:
            crewai_event_bus: The CrewAI event bus
        """
        log_prefix = f"[AgentTraceEventListener][{self.job_id}]"
        logger.info(f"{log_prefix} Setting up event listeners")
        
        @crewai_event_bus.on(AgentExecutionCompletedEvent)
        def on_agent_execution_completed(source, event):
            """Handle agent execution completion events."""
            try:
                agent_name = event.agent.role if hasattr(event.agent, 'role') else "Unknown"
                task_name = event.task.description if hasattr(event.task, 'description') else "Unknown"
                
                logger.info(f"{log_prefix} Event: AgentExecutionCompleted for agent: {agent_name}")
                
                # Format output for storage - store complete output without truncation
                output_content = str(event.output) if event.output is not None else "None"
                
                # Check if task has markdown enabled
                is_markdown = False
                if hasattr(event.task, 'markdown'):
                    is_markdown = event.task.markdown
                elif hasattr(event, 'context') and hasattr(event.context, 'task') and hasattr(event.context.task, 'markdown'):
                    is_markdown = event.context.task.markdown
                
                # Add markdown flag to extra data if enabled
                extra_data = {}
                if is_markdown:
                    extra_data['markdown'] = True
                
                self._enqueue_trace(
                    agent_name=agent_name,
                    task_name=task_name,
                    event_type="agent_execution",
                    output_content=output_content,
                    extra_data=extra_data
                )
            except Exception as e:
                logger.error(f"{log_prefix} Error in on_agent_execution_completed: {e}", exc_info=True)
        
        @crewai_event_bus.on(ToolUsageFinishedEvent)
        def on_tool_usage_finished(source, event):
            """Handle tool usage completion events."""
            try:
                agent_name = event.agent.role if hasattr(event.agent, 'role') else "Unknown"
                # Get task name from context if available, otherwise use "Unknown"
                task_name = "Unknown"
                if hasattr(event, 'context') and hasattr(event.context, 'task'):
                    task_name = event.context.task.description if hasattr(event.context.task, 'description') else "Unknown"
                elif hasattr(event, 'task'):
                    task_name = event.task.description if hasattr(event.task, 'description') else "Unknown"
                
                tool_name = event.tool_name
                
                logger.info(f"{log_prefix} Event: ToolUsageFinished for tool: {tool_name} by agent: {agent_name}")
                
                # Format output for storage - store complete output without truncation
                output_content = str(event.output) if event.output is not None else "None"
                
                self._enqueue_trace(
                    agent_name=agent_name,
                    task_name=task_name,
                    event_type="tool_usage",
                    output_content=output_content,
                    extra_data={"tool_name": tool_name}
                )
            except Exception as e:
                logger.error(f"{log_prefix} Error in on_tool_usage_finished: {e}", exc_info=True)
        
        @crewai_event_bus.on(LLMCallCompletedEvent)
        def on_llm_call_completed(source, event):
            """Handle LLM call completion events."""
            try:
                # CrewAI events might have different structures in different versions
                # For LLMCallCompletedEvent, the agent might be in agent or in context.agent
                if hasattr(event, 'agent'):
                    agent_name = event.agent.role if hasattr(event.agent, 'role') else "Unknown"
                elif hasattr(event, 'context') and hasattr(event.context, 'agent'):
                    agent_name = event.context.agent.role if hasattr(event.context.agent, 'role') else "Unknown"
                else:
                    agent_name = "Unknown"
                
                # Similarly for task
                if hasattr(event, 'task'):
                    task_name = event.task.description if hasattr(event.task, 'description') else "Unknown"
                elif hasattr(event, 'context') and hasattr(event.context, 'task'):
                    task_name = event.context.task.description if hasattr(event.context.task, 'description') else "Unknown"
                else:
                    task_name = "Unknown"
                
                logger.info(f"{log_prefix} Event: LLMCallCompleted for agent: {agent_name}")
                
                # Format output for storage - Check multiple possible attributes for output content
                output_content = "LLM call completed (no output data available)"
                
                # Try to find output in various possible attributes (different CrewAI versions may use different structures)
                if hasattr(event, 'output') and event.output is not None:
                    output_content = str(event.output)
                elif hasattr(event, 'response') and event.response is not None:
                    output_content = str(event.response)
                elif hasattr(event, 'result') and event.result is not None:
                    output_content = str(event.result)
                elif hasattr(event, 'completion') and event.completion is not None:
                    output_content = str(event.completion)
                elif hasattr(event, 'content') and event.content is not None:
                    output_content = str(event.content)
                
                # Log event attributes for debugging
                logger.debug(f"{log_prefix} LLMCallCompletedEvent attributes: {[attr for attr in dir(event) if not attr.startswith('_')]}")
                
                self._enqueue_trace(
                    agent_name=agent_name,
                    task_name=task_name,
                    event_type="llm_call",
                    output_content=output_content
                )
            except Exception as e:
                logger.error(f"{log_prefix} Error in on_llm_call_completed: {e}", exc_info=True)
        
        @crewai_event_bus.on(TaskCompletedEvent)
        def on_task_completed(source, event):
            """Handle task completion events."""
            try:
                # CrewAI events might have different structures in different versions
                # For TaskCompletedEvent, the agent might be in agent or in context.agent
                if hasattr(event, 'agent'):
                    agent_name = event.agent.role if hasattr(event.agent, 'role') else "Unknown"
                elif hasattr(event, 'context') and hasattr(event.context, 'agent'):
                    agent_name = event.context.agent.role if hasattr(event.context.agent, 'role') else "Unknown"
                else:
                    agent_name = "Unknown"
                
                # Similarly for task
                if hasattr(event, 'task'):
                    task_name = event.task.description if hasattr(event.task, 'description') else "Unknown"
                    # Use original task ID from event data for registry lookup
                    task_original_id = str(event.task.id) if hasattr(event.task, 'id') else task_name
                elif hasattr(event, 'context') and hasattr(event.context, 'task'):
                    task_name = event.context.task.description if hasattr(event.context.task, 'description') else "Unknown"
                    task_original_id = str(event.context.task.id) if hasattr(event.context.task, 'id') else task_name
                else:
                    task_name = "Unknown"
                    task_original_id = "unknown_task"
                
                # Get the registered task ID from our registry
                task_id = self._get_or_create_task_id(task_name, task_original_id)
                
                logger.info(f"{log_prefix} Event: TaskCompleted for task: {task_name}")
                
                # Format output for storage - store complete output without truncation
                output_content = str(event.output) if event.output is not None else "None"
                
                self._enqueue_trace(
                    agent_name=agent_name,
                    task_name=task_name,
                    event_type="task_completed",
                    output_content=output_content
                )
                
                # Update task status via service, using the UnitOfWork pattern
                try:
                    # Get UnitOfWork singleton and ensure it's initialized
                    uow = SyncUnitOfWork.get_instance()
                    if not getattr(uow, '_initialized', False):
                        uow.initialize()
                    
                    # Use the TaskTrackingService with the repository from UnitOfWork
                    service = TaskTrackingService.for_crew_with_repo(uow.task_tracking_repository)
                    
                    # Update the task status using our consistent task_id
                    service.update_task_status(
                        job_id=self.job_id, 
                        task_id=task_id, 
                        status=TaskStatusEnum.COMPLETED
                    )
                    
                    # Commit the changes
                    uow.commit()
                    
                    logger.info(f"{log_prefix} Updated task status for {task_id} to COMPLETED")
                except Exception as e:
                    logger.error(f"{log_prefix} Error updating task status: {e}", exc_info=True)
                    
            except Exception as e:
                logger.error(f"{log_prefix} Error in on_task_completed: {e}", exc_info=True)
        
        @crewai_event_bus.on(CrewKickoffStartedEvent)
        def on_crew_kickoff_started(source, event):
            """Handle crew kickoff started events."""
            try:
                logger.info(f"{log_prefix} Event: CrewKickoffStarted for crew: {event.crew_name}")
                
                self._enqueue_trace(
                    agent_name="Crew",
                    task_name="Initialization",
                    event_type="crew_started",
                    output_content=f"Crew '{event.crew_name}' execution started"
                )
                
                # Reset task registry for this job on start to ensure fresh tracking
                AgentTraceEventListener._task_registry[self.job_id] = {}
                logger.info(f"{log_prefix} Reset task registry for job {self.job_id}")
                
            except Exception as e:
                logger.error(f"{log_prefix} Error in on_crew_kickoff_started: {e}", exc_info=True)
        
        @crewai_event_bus.on(CrewKickoffCompletedEvent)
        def on_crew_kickoff_completed(source, event):
            """Handle crew kickoff completion events."""
            try:
                logger.info(f"{log_prefix} Event: CrewKickoffCompleted for crew: {event.crew_name}")
                
                # Format output for storage - store complete output without truncation
                output_content = str(event.output) if event.output is not None else "None"
                
                self._enqueue_trace(
                    agent_name="Crew",
                    task_name="Completion",
                    event_type="crew_completed",
                    output_content=output_content
                )
            except Exception as e:
                logger.error(f"{log_prefix} Error in on_crew_kickoff_completed: {e}", exc_info=True)
                
        @crewai_event_bus.on(TaskStartedEvent)
        def on_task_started(source, event):
            """Handle task started events."""
            try:
                # Extract agent and task information
                if hasattr(event, 'agent'):
                    agent_name = event.agent.role if hasattr(event.agent, 'role') else "Unknown"
                elif hasattr(event, 'context') and hasattr(event.context, 'agent'):
                    agent_name = event.context.agent.role if hasattr(event.context.agent, 'role') else "Unknown"
                else:
                    agent_name = "Unknown"
                
                if hasattr(event, 'task'):
                    task_name = event.task.description if hasattr(event.task, 'description') else "Unknown"
                    task_original_id = str(event.task.id) if hasattr(event.task, 'id') else task_name
                elif hasattr(event, 'context') and hasattr(event.context, 'task'):
                    task_name = event.context.task.description if hasattr(event.context.task, 'description') else "Unknown"
                    task_original_id = str(event.context.task.id) if hasattr(event.context.task, 'id') else task_name
                else:
                    task_name = "Unknown"
                    task_original_id = "unknown_task"
                
                # Get or create a task ID for this task
                task_id = self._get_or_create_task_id(task_name, task_original_id)
                
                logger.info(f"{log_prefix} Event: TaskStarted for task: {task_name} and agent: {agent_name} with ID: {task_id}")
                
                self._enqueue_trace(
                    agent_name=agent_name,
                    task_name=task_name,
                    event_type="task_started",
                    output_content=f"Task '{task_name}' started by agent '{agent_name}'"
                )
                
                # Create or update task status via service, using the UnitOfWork pattern
                try:
                    # Get UnitOfWork singleton and ensure it's initialized
                    uow = SyncUnitOfWork.get_instance()
                    if not getattr(uow, '_initialized', False):
                        uow.initialize()
                    
                    # Use the TaskTrackingService with the repository from UnitOfWork
                    service = TaskTrackingService.for_crew_with_repo(uow.task_tracking_repository)
                    
                    # Check if a task status for this task_id already exists
                    existing_status = service.get_task_status_by_task_id(task_id)
                    
                    if not existing_status:
                        # Create new task status with our consistent task_id
                        service.create_task_status(
                            job_id=self.job_id,
                            task_id=task_id,
                            agent_name=agent_name
                        )
                        logger.info(f"{log_prefix} Created new task status for {task_id} with status RUNNING")
                    else:
                        logger.info(f"{log_prefix} Task status for {task_id} already exists, skipping creation")
                    
                    # Commit the changes
                    uow.commit()
                    
                except Exception as e:
                    logger.error(f"{log_prefix} Error creating task status: {e}", exc_info=True)
                    
            except Exception as e:
                logger.error(f"{log_prefix} Error in on_task_started: {e}", exc_info=True)
    
    def _enqueue_trace(self, agent_name: str, task_name: str, event_type: str, 
                      output_content: str, extra_data: dict = None) -> None:
        """
        Enqueue a trace entry to be processed by the trace writer.
        
        Args:
            agent_name: Name of the agent
            task_name: Name of the task
            event_type: Type of event
            output_content: Content to be logged
            extra_data: Additional metadata
        """
        log_prefix = f"[AgentTraceEventListener][{self.job_id}]"
        try:
            # Log at the beginning to confirm method is called
            logger.info(f"{log_prefix} EVENT[{event_type}] ⏳ Enqueuing trace for agent: {agent_name} and task: {task_name[:30]}...")
            
            timestamp = datetime.now(timezone.utc)
            
            # Create trace data for queue
            trace_data = {
                "job_id": self.job_id,
                "agent_name": agent_name,
                "task_name": task_name,
                "event_type": event_type,
                "timestamp": timestamp.isoformat(),
                "output_content": output_content,
                "extra_data": extra_data or {}
            }
            
            # Log trace data shape before enqueueing
            logger.debug(f"{log_prefix} EVENT[{event_type}] Trace data prepared with keys: {', '.join(trace_data.keys())}")
            logger.debug(f"{log_prefix} EVENT[{event_type}] Queue state before enqueue: Size approximately {self._queue.qsize()}")
            
            # Enqueue the trace data
            self._queue.put_nowait(trace_data)
            
            # Log successful enqueuing with more info
            logger.info(f"{log_prefix} EVENT[{event_type}] ✅ Successfully enqueued trace for Agent: {agent_name}, Task: {task_name[:30]}...")
        except queue.Full:
             logger.error(f"{log_prefix} EVENT[{event_type}] ❌ Trace queue is full! Discarding trace data.")
        except Exception as e:
            logger.error(f"{log_prefix} EVENT[{event_type}] ❌ Error formatting/enqueuing trace: {e}", exc_info=True)

# For backward compatibility, keep the old name as an alias
AgentTraceCallback = AgentTraceEventListener

# Update other callbacks if they exist and were using CrewAICallback
class TaskCompletionLogger(BaseEventListener): # Changed inheritance
    """Logs task completion with detailed output information."""
    
    def __init__(self, job_id: str = None):
        self.job_id = job_id
        super().__init__()
        
    def setup_listeners(self, crewai_event_bus):
        """
        Register task completion listener with the CrewAI event bus.
        
        Args:
            crewai_event_bus: The CrewAI event bus
        """
        log_prefix = f"[TaskCompletionLogger][{self.job_id or 'N/A'}]"
        logger.info(f"{log_prefix} Setting up task completion event listener")
        
        @crewai_event_bus.on(TaskStartedEvent)
        def on_task_started(source, event):
            """Handle task started events."""
            try:
                task = event.task if hasattr(event, 'task') else getattr(event.context, 'task', None)
                agent = event.agent if hasattr(event, 'agent') else getattr(event.context, 'agent', None)
                self.on_task_start(task, agent)
            except Exception as e:
                logger.error(f"{log_prefix} Error in task started event handler: {e}", exc_info=True)
        
        @crewai_event_bus.on(TaskCompletedEvent)
        def on_task_completed(source, event):
            """Handle task completion events."""
            try:
                task = event.task if hasattr(event, 'task') else getattr(event.context, 'task', None)
                task_output = event.output if hasattr(event, 'output') else None
                self.on_task_end(task_output, task)
            except Exception as e:
                logger.error(f"{log_prefix} Error in task completion event handler: {e}", exc_info=True)
    
    def on_task_start(self, task: Any, agent: Any) -> None:
        """Log when a task starts execution."""
        task_id = getattr(task, 'id', 'N/A')
        task_name = getattr(task, 'description', str(task))
        agent_name = getattr(agent, 'role', 'Unknown Agent') if agent else 'Unknown Agent'
        
        log_prefix = f"[TaskCompletionLogger][{task_id}]"
        logger.info(f"nemo task started")
        logger.info(f"{log_prefix} Task '{task_name[:50]}...' started by agent '{agent_name}'")
    
    def on_task_end(self, task_output: Any, task: Any) -> None:
        log_prefix = f"[TaskCompletionLogger][{getattr(task, 'id', 'N/A')}]"
        logger.info(f"nemo task completed")
        logger.info(f"{log_prefix} SYNC on_task_end called.")
        try:
            # Log or process task_output here
            logger.info(f"{log_prefix} Task Output: {repr(task_output)[:500]}...")
        except Exception as e:
            logger.error(f"{log_prefix} Error logging task completion: {e}", exc_info=True)
        # on_task_end usually doesn't return anything

class DetailedOutputLogger(BaseEventListener): # Changed inheritance
    """Logs detailed analysis of the output structure and content."""
    
    def __init__(self, job_id: str = None):
        self.job_id = job_id
        super().__init__()
    
    def setup_listeners(self, crewai_event_bus):
        """
        Register agent step listener with the CrewAI event bus.
        
        Args:
            crewai_event_bus: The CrewAI event bus
        """
        log_prefix = f"[DetailedOutputLogger][{self.job_id or 'N/A'}]"
        logger.info(f"{log_prefix} Setting up agent step event listener")
        
        @crewai_event_bus.on(AgentExecutionCompletedEvent)
        def on_agent_execution_completed(source, event):
            """Handle agent execution completion events."""
            try:
                agent = event.agent
                task = event.task
                agent_output = event.output
                self.on_agent_step(agent_output, agent, task)
            except Exception as e:
                logger.error(f"{log_prefix} Error in agent execution event handler: {e}", exc_info=True)
    
    def on_agent_step(self, agent_output: Any, agent: Any, task: Any) -> Any:
        log_prefix = f"[DetailedOutputLogger][{getattr(task, 'id', 'N/A')}]"
        logger.info(f"{log_prefix} SYNC on_agent_step called for detailed analysis.")
        try:
            logger.info("=== Detailed Output Analysis ===")
            safe_attributes = ['description', 'name', 'expected_output', 'summary', 'raw', 'agent', 'task_key', 'output']
            output_to_analyze = getattr(agent_output, 'output', agent_output)
            logger.info(f"Output Type: {type(output_to_analyze)}")
            
            # ... (rest of analysis code, adapted for synchronous context) ...
            logger.info("=== End Detailed Analysis ===")
        except Exception as e:
            logger.error(f"{log_prefix} Error in detailed output analysis: {e}", exc_info=True)
        return agent_output # Return original output

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
            import sys
            print(f"Error in CrewLoggerHandler.emit: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr) 