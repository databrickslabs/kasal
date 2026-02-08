"""Event-driven logging callbacks for CrewAI engine.

This module implements the primary event listener for capturing and logging
CrewAI agent execution traces. It delegates handler registration to focused
sub-modules in the handlers/ package.

Event Categories (registered via handler modules):
    - **Core Events**: Agent execution, crew kickoff, flow lifecycle
    - **Task Events**: Task started/completed/failed
    - **Tool Events**: Tool usage tracking with inputs/outputs
    - **Memory Events**: Memory save/query operations
    - **Knowledge Events**: Knowledge retrieval operations
    - **LLM Events**: Streaming chunks
    - **Reasoning Events**: Agent reasoning process tracking
    - **Guardrail Events**: LLM output validation

Note:
    This module is compatible with CrewAI 0.177+ and handles version-specific
    event availability gracefully through conditional imports in event_imports.py.
"""

from typing import Any, Optional, Dict, Tuple
from datetime import datetime, timezone
import logging
import os

from src.engines.crewai.callbacks.event_imports import (
    BaseEventListener,
    crewai_event_bus,
    AgentExecutionCompletedEvent,
    MEMORY_EVENTS_AVAILABLE,
    KNOWLEDGE_EVENTS_AVAILABLE,
    TOOL_EVENTS_AVAILABLE,
    TASK_EVENTS_AVAILABLE,
    REASONING_EVENTS_AVAILABLE,
    LLM_GUARDRAIL_EVENTS_AVAILABLE,
)
from src.engines.crewai.callbacks.trace_persistence import TracePersistenceMixin
from src.services.trace_queue import get_trace_queue
from src.services.execution_logs_queue import enqueue_log
from src.services.task_tracking_service import TaskTrackingService
from src.schemas.task_tracking import TaskStatusEnum, TaskStatusCreate

logger = logging.getLogger(__name__)


class EventTypeDetector:
    """Pattern-based event type detection from output content.

    .. deprecated:: 2.0
        With hundreds of tools available, pattern matching is not scalable.
        Kept for backward compatibility only - not used in current implementation.
    """

    TOOL_PATTERNS = [
        (r"Using tool:\s*(\w+)", "tool_name"),
        (r"^Tool:\s*([^\n]+)", "tool_name"),
        (r"^Action:\s*([^\n]+)", "action_name"),
        (r"Calling:\s*([^\n]+)", "tool_name"),
        (r"Executing:\s*([^\n]+)", "tool_name"),
    ]

    TOOL_OUTPUT_PATTERNS = [
        r"Tool Output:\s*(.+)",
        r"Action Output:\s*(.+)",
        r"Result:\s*(.+)",
        r"Response:\s*(.+)",
    ]

    COMPLETION_PATTERNS = [
        r"Final Answer:",
        r"Task Complete:",
        r"Completed:",
        r"FINAL ANSWER:",
        r"## Final Answer",
    ]

    LLM_PATTERNS = [
        r"Thought:",
        r"Thinking:",
        r"Reasoning:",
        r"Analysis:",
        r"Let me",
        r"I need to",
        r"I\'ll",
        r"I will",
    ]

    @classmethod
    def detect_event_type(
        cls, output: str
    ) -> Tuple[str, Optional[str], Optional[Dict]]:
        """Detect event type from output content using pattern matching.

        .. deprecated:: 2.0
            Pattern matching approach is deprecated for event detection.
        """
        import re

        if not output:
            return "agent_execution", None, None

        for pattern, name_key in cls.TOOL_PATTERNS:
            match = re.search(pattern, output, re.IGNORECASE)
            if match:
                tool_name = match.group(1).strip()
                tool_output = None
                for output_pattern in cls.TOOL_OUTPUT_PATTERNS:
                    output_match = re.search(
                        output_pattern, output, re.DOTALL | re.IGNORECASE
                    )
                    if output_match:
                        tool_output = output_match.group(1).strip()
                        break
                extra_data = {
                    "tool_name": tool_name,
                    "tool_output": tool_output,
                    "pattern_matched": pattern,
                }
                return "tool_usage", tool_name, extra_data

        for pattern in cls.COMPLETION_PATTERNS:
            if re.search(pattern, output, re.IGNORECASE):
                answer_match = re.search(
                    f"{pattern}\\s*(.+)", output, re.DOTALL | re.IGNORECASE
                )
                answer = answer_match.group(1).strip() if answer_match else None
                extra_data = {"final_answer": answer, "pattern_matched": pattern}
                return "task_completed", "task_completion", extra_data

        for pattern in cls.LLM_PATTERNS:
            if re.search(pattern, output, re.IGNORECASE):
                return "llm_call", "reasoning", {"pattern_matched": pattern}

        return "agent_execution", None, None


class AgentTraceEventListener(TracePersistenceMixin, BaseEventListener):
    """Primary event listener for capturing and processing CrewAI agent traces.

    This listener delegates event handler registration to focused sub-modules:
        - core_handlers: Agent execution, crew kickoff, flow events
        - memory_knowledge_handlers: Memory and knowledge events
        - tool_handlers: Tool usage events
        - task_handlers: Task lifecycle events
        - advanced_handlers: LLM streaming, reasoning, guardrail events

    Attributes:
        job_id: Unique identifier for the job being tracked
        group_context: Optional multi-tenant group isolation context
        _queue: Shared queue for trace data
        _init_time: Timestamp when listener was initialized
    """

    _init_logged = set()
    _task_registry: Dict[str, Dict[str, str]] = {}
    _active_context: Dict[str, Dict[str, str]] = {}
    _task_start_times: Dict[str, Dict[str, datetime]] = {}
    _active_crew_name: Dict[str, str] = {}
    _tasks_with_execution_trace: Dict[str, set] = {}
    _processed_agent_executions: Dict[str, set] = {}

    def __init__(
        self,
        job_id: str,
        group_context=None,
        register_global_events=False,
        task_event_queue=None,
    ):
        """Initialize the agent trace event listener.

        Args:
            job_id: Unique identifier for the job being tracked.
            group_context: Optional group context for multi-tenant isolation.
            register_global_events: Whether to register global event listeners.
            task_event_queue: Optional multiprocessing.Queue for relaying task
                lifecycle events from subprocess to main process.
        """
        if not job_id or not isinstance(job_id, str):
            raise ValueError("job_id must be a non-empty string")

        self.job_id = job_id
        self.group_context = group_context
        self._task_event_queue = task_event_queue
        logger.debug(
            f"[TRACE_DEBUG] AgentTraceEventListener.__init__ - Getting trace queue for job {job_id}"
        )
        self._queue = get_trace_queue()
        logger.debug(
            f"[TRACE_DEBUG] AgentTraceEventListener.__init__ - Got queue: {type(self._queue)}, size: {self._queue.qsize()}"
        )
        self._init_time = datetime.now(timezone.utc)

        if job_id not in AgentTraceEventListener._task_registry:
            AgentTraceEventListener._task_registry[job_id] = {}
        if job_id not in AgentTraceEventListener._task_start_times:
            AgentTraceEventListener._task_start_times[job_id] = {}

        log_prefix = f"[AgentTraceEventListener][{self.job_id}]"
        if job_id not in AgentTraceEventListener._init_logged:
            logger.info(
                f"{log_prefix} Initializing trace listener at {self._init_time.isoformat()}"
            )
            AgentTraceEventListener._init_logged.add(job_id)

        try:
            if register_global_events:
                super().__init__()
                logger.info(
                    f"{log_prefix} Registered global event listeners (legacy mode)"
                )
            else:
                logger.info(
                    f"{log_prefix} Trace listener initialized for execution-scoped callbacks"
                )
        except Exception as e:
            logger.error(
                f"{log_prefix} Error initializing trace listener: {e}", exc_info=True
            )
            raise

    def _extract_agent_info(
        self, event: AgentExecutionCompletedEvent
    ) -> Tuple[str, str]:
        """Extract agent identification information from event.

        Returns:
            A tuple of (agent_name, agent_id).
        """
        agent_name = "Unknown Agent"
        agent_id = None

        if hasattr(event, "agent"):
            agent = event.agent
            if hasattr(agent, "role"):
                agent_name = agent.role
            elif hasattr(agent, "name"):
                agent_name = agent.name
            else:
                agent_name = str(agent)

            if hasattr(agent, "id"):
                agent_id = agent.id
            elif hasattr(agent, "agent_id"):
                agent_id = agent.agent_id

        return agent_name, agent_id

    def _extract_task_info(self, event) -> Tuple[str, str, str]:
        """Extract task identification and description from event.

        Returns:
            A tuple of (task_name, task_id, task_description).
        """
        task_name = "Unknown Task"
        task_id = None
        task_description = None

        if hasattr(event, "task_id") and event.task_id:
            task_id = event.task_id
            logger.debug(f"Found direct task_id on event: {task_id}")

        if hasattr(event, "task_name") and event.task_name:
            task_name = event.task_name
            task_description = event.task_name
            logger.debug(f"Found direct task_name on event: {task_name}")

        if hasattr(event, "task") and event.task:
            task = event.task
            if task_name == "Unknown Task":
                if hasattr(task, "description") and task.description:
                    task_name = task.description
                    task_description = task.description
                elif hasattr(task, "name") and task.name:
                    task_name = task.name
                    task_description = task.name
                else:
                    task_name = str(task)
                    task_description = str(task)

            if not task_id:
                if hasattr(task, "id") and task.id:
                    task_id = task.id
                elif hasattr(task, "task_id") and task.task_id:
                    task_id = task.task_id

        return task_name, task_id, task_description

    def _update_active_context(
        self, agent_name: str, task_name: str, log_prefix: str, task_id: str = None
    ) -> None:
        """Update the active context for tracking agent and task."""
        if self.job_id not in self._active_context:
            self._active_context[self.job_id] = {}

        if agent_name and agent_name != "Unknown Agent":
            if agent_name not in self._active_context[self.job_id]:
                self._active_context[self.job_id][agent_name] = {}

            if task_name and task_name != "Unknown Task":
                self._active_context[self.job_id][agent_name]["task"] = task_name
                logger.debug(
                    f"{log_prefix} Updated active context for {agent_name} - Task: {task_name}"
                )

            if task_id:
                self._active_context[self.job_id][agent_name]["task_id"] = task_id
                logger.debug(
                    f"{log_prefix} Updated active context for {agent_name} - Task ID: {task_id}"
                )

    def setup_listeners(self, crewai_event_bus):
        """Register all event handlers with the CrewAI event bus.

        Delegates to focused handler modules for each event category.
        """
        from src.engines.crewai.callbacks.handlers import (
            register_core_handlers,
            register_memory_handlers,
            register_knowledge_handlers,
            register_tool_handlers,
            register_task_handlers,
            register_llm_stream_handler,
            register_reasoning_handlers,
            register_guardrail_handlers,
        )

        log_prefix = f"[AgentTraceEventListener][{self.job_id}]"
        logger.info(f"{log_prefix} Setting up refined event listeners for CrewAI")
        logger.debug(f"[TRACE_DEBUG] setup_listeners called for job {self.job_id}")
        logger.debug(f"[TRACE_DEBUG] Event bus type: {type(crewai_event_bus)}")

        # Core: agent execution, crew kickoff, flow events
        register_core_handlers(self, crewai_event_bus, log_prefix)

        # Memory events
        if MEMORY_EVENTS_AVAILABLE:
            register_memory_handlers(self, crewai_event_bus, log_prefix)

        # Knowledge events
        if KNOWLEDGE_EVENTS_AVAILABLE:
            register_knowledge_handlers(self, crewai_event_bus, log_prefix)

        # Tool events
        if TOOL_EVENTS_AVAILABLE:
            register_tool_handlers(self, crewai_event_bus, log_prefix)

        # LLM streaming (always available)
        register_llm_stream_handler(self, crewai_event_bus, log_prefix)

        # Task lifecycle events
        if TASK_EVENTS_AVAILABLE:
            register_task_handlers(self, crewai_event_bus, log_prefix)

        # Reasoning events
        if REASONING_EVENTS_AVAILABLE:
            register_reasoning_handlers(self, crewai_event_bus, log_prefix)

        # Guardrail events
        if LLM_GUARDRAIL_EVENTS_AVAILABLE:
            register_guardrail_handlers(self, crewai_event_bus, log_prefix)

        logger.debug(
            f"[TRACE_DEBUG] All event listeners registered for job {self.job_id}"
        )
        logger.info(f"{log_prefix} Event listener setup complete")

    def _handle_task_completion(self, task_name: str, task_id: str, log_prefix: str):
        """Handle task completion tracking and duration calculation."""
        try:
            task_duration = None
            if (
                self.job_id in self._task_start_times
                and task_id in self._task_start_times[self.job_id]
            ):
                start_time = self._task_start_times[self.job_id][task_id]
                task_duration = (
                    datetime.now(timezone.utc) - start_time
                ).total_seconds()
                logger.info(
                    f"{log_prefix} Task '{task_name}' completed in {task_duration:.2f} seconds"
                )
                del self._task_start_times[self.job_id][task_id]

            self._update_task_status(task_name, TaskStatusEnum.COMPLETED, log_prefix)

        except Exception as e:
            logger.error(
                f"{log_prefix} Error handling task completion: {e}", exc_info=True
            )

    def _update_task_status(
        self, task_name: str, status: TaskStatusEnum, log_prefix: str
    ):
        """Update task status in database with subprocess safety."""
        try:
            is_subprocess = os.environ.get("CREW_SUBPROCESS_MODE") == "true"

            if is_subprocess:
                logger.debug(
                    f"{log_prefix} Updating task status to {status} in database"
                )

                import asyncio
                from concurrent.futures import ThreadPoolExecutor

                def update_task_status_sync():
                    try:
                        logger.warning(
                            f"{log_prefix} Task tracking disabled in sync callback context"
                        )
                        return
                    except Exception as e:
                        logger.error(
                            f"{log_prefix} Error updating task status: {e}",
                            exc_info=True,
                        )

                with ThreadPoolExecutor(max_workers=1) as executor:
                    executor.submit(update_task_status_sync).result(timeout=2)
        except Exception as e:
            logger.error(f"{log_prefix} Error updating task status: {e}", exc_info=True)

    def _cleanup_job_tracking(self) -> None:
        """Clean up job-specific tracking data to prevent memory leaks."""
        log_prefix = f"[AgentTraceEventListener][{self.job_id}]"

        if self.job_id in AgentTraceEventListener._task_registry:
            del AgentTraceEventListener._task_registry[self.job_id]
            logger.debug(f"{log_prefix} Cleaned up task registry")

        if self.job_id in AgentTraceEventListener._active_context:
            del AgentTraceEventListener._active_context[self.job_id]
            logger.debug(f"{log_prefix} Cleaned up active context")

        if self.job_id in AgentTraceEventListener._task_start_times:
            del AgentTraceEventListener._task_start_times[self.job_id]
            logger.debug(f"{log_prefix} Cleaned up task start times")

        if self.job_id in AgentTraceEventListener._active_crew_name:
            del AgentTraceEventListener._active_crew_name[self.job_id]
            logger.debug(f"{log_prefix} Cleaned up active crew name")

        if self.job_id in AgentTraceEventListener._tasks_with_execution_trace:
            del AgentTraceEventListener._tasks_with_execution_trace[self.job_id]
            logger.debug(f"{log_prefix} Cleaned up execution trace tracking")

        if self.job_id in AgentTraceEventListener._processed_agent_executions:
            del AgentTraceEventListener._processed_agent_executions[self.job_id]
            logger.debug(f"{log_prefix} Cleaned up agent execution tracking")

        logger.info(f"{log_prefix} Job tracking cleanup completed")


class TaskCompletionEventListener(BaseEventListener):
    """Specialized listener for task completion tracking.

    .. deprecated:: 2.0
        Task completion is now handled through AgentTraceEventListener.
        This class is maintained for backward compatibility only.
    """

    def __init__(self, job_id: str, group_context=None):
        self.job_id = job_id
        self.group_context = group_context
        self._init_time = datetime.now(timezone.utc)
        logger.info(f"[TaskCompletionEventListener][{self.job_id}] Initialized")

    def setup_listeners(self, crewai_event_bus):
        log_prefix = f"[TaskCompletionEventListener][{self.job_id}]"
        logger.info(
            f"{log_prefix} Task tracking integrated into AgentExecutionCompletedEvent handler"
        )
