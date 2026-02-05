"""Event-driven logging callbacks for CrewAI engine.

This module implements a comprehensive event listener architecture for capturing and
logging CrewAI agent execution traces, task completions, and various operational events.
It provides real-time monitoring and observability for AI agent workflows.

Key Features:
    - Event-based architecture with async processing via queues
    - Support for multiple CrewAI event types (agent, task, tool, memory, knowledge)
    - Real-time trace capture with PostgreSQL persistence
    - Multi-tenant support with group context isolation
    - Subprocess-safe operation for distributed execution

Event Categories:
    - **Core Events**: Crew kickoff, agent execution, task lifecycle
    - **Tool Events**: Tool usage tracking with inputs/outputs
    - **Memory Events**: Memory save/query operations
    - **Knowledge Events**: Knowledge retrieval operations
    - **LLM Events**: Streaming and guardrail events
    - **Reasoning Events**: Agent reasoning process tracking

Note:
    This module is compatible with CrewAI 0.177+ and handles version-specific
    event availability gracefully through conditional imports.
"""

from typing import Any, Optional, Dict, Tuple
from datetime import datetime, timezone
import logging
import re
import json
import queue
import traceback
import uuid
import os
from sqlalchemy import text

# Set up logger first before any imports that might use it
logger = logging.getLogger(__name__)

# Import CrewAI's event system - using new location
from crewai.events import BaseEventListener, crewai_event_bus

# Import all available events based on documentation
# Core Crew Events - Note: CrewKickoffFailedEvent doesn't exist in 0.177
try:
    from crewai.events import (
        CrewKickoffStartedEvent,
        CrewKickoffCompletedEvent,
    )

    CREW_EVENTS_AVAILABLE = True
except ImportError:
    CREW_EVENTS_AVAILABLE = False
    logger.info("Some crew events not available in this CrewAI version")

# Agent Events
try:
    from crewai.events import (
        AgentExecutionStartedEvent,
        AgentExecutionCompletedEvent,
        AgentExecutionErrorEvent,
    )

    AGENT_EVENTS_AVAILABLE = True
except ImportError:
    AGENT_EVENTS_AVAILABLE = False
    logger.info("Some agent events not available in this CrewAI version")

# Task Events - Not exported in main module but exist in types
try:
    from crewai.events.types.task_events import (
        TaskStartedEvent,
        TaskCompletedEvent,
        TaskFailedEvent,
    )

    TASK_EVENTS_AVAILABLE = True
    logger.info("Task events loaded successfully from CrewAI 0.177")
except ImportError:
    TASK_EVENTS_AVAILABLE = False
    logger.info("Task events not available in this CrewAI version")

# Tool Usage Events - Not exported in main module but exist in types
try:
    from crewai.events.types.tool_usage_events import (
        ToolUsageStartedEvent,
        ToolUsageFinishedEvent,
        ToolUsageErrorEvent,
    )

    TOOL_EVENTS_AVAILABLE = True
    logger.info("Tool usage events loaded successfully from CrewAI 0.177")
except ImportError:
    TOOL_EVENTS_AVAILABLE = False
    logger.info("Tool events not available in this CrewAI version")

# LLM Events - Only LLMStreamChunkEvent exists in 0.177
try:
    from crewai.events import LLMStreamChunkEvent

    LLM_EVENTS_AVAILABLE = True
    logger.info("LLM events loaded successfully for CrewAI 0.177")
except ImportError as e:
    LLM_EVENTS_AVAILABLE = False
    logger.warning(f"LLM events not available: {e}")

# Memory Events - Note: MemoryRetrievalStartedEvent doesn't exist in 0.177
try:
    from crewai.events import (
        MemoryQueryStartedEvent,
        MemoryQueryCompletedEvent,
        MemoryQueryFailedEvent,
        MemorySaveStartedEvent,
        MemorySaveCompletedEvent,
        MemorySaveFailedEvent,
        MemoryRetrievalCompletedEvent,  # Note: MemoryRetrievalStartedEvent doesn't exist
    )

    MEMORY_EVENTS_AVAILABLE = True
    logger.info("Memory events loaded successfully for CrewAI 0.177")
except ImportError as e:
    MEMORY_EVENTS_AVAILABLE = False
    logger.warning(f"Memory events not available: {e}")

# Knowledge Events - Only retrieval events exist in 0.177
try:
    from crewai.events import (
        KnowledgeRetrievalStartedEvent,
        KnowledgeRetrievalCompletedEvent,
    )

    KNOWLEDGE_EVENTS_AVAILABLE = True
    logger.info("Knowledge events loaded successfully for CrewAI 0.177")
except ImportError as e:
    KNOWLEDGE_EVENTS_AVAILABLE = False
    logger.warning(f"Knowledge events not available: {e}")

# Reasoning Events - Not exported in main module but exist in types
try:
    from crewai.events.types.reasoning_events import (
        AgentReasoningStartedEvent,
        AgentReasoningCompletedEvent,
        AgentReasoningFailedEvent,
    )

    REASONING_EVENTS_AVAILABLE = True
    logger.info("Reasoning events loaded successfully from CrewAI 0.177")
except ImportError:
    REASONING_EVENTS_AVAILABLE = False
    logger.info("Reasoning events not available in this CrewAI version")

# LLM Guardrail Events - Not exported in main module but exist in types
try:
    from crewai.events.types.llm_guardrail_events import (
        LLMGuardrailStartedEvent,
        LLMGuardrailCompletedEvent,
        LLMGuardrailFailedEvent,
    )

    LLM_GUARDRAIL_EVENTS_AVAILABLE = True
    logger.info("LLM Guardrail events loaded successfully from CrewAI 0.177+")
except ImportError:
    LLM_GUARDRAIL_EVENTS_AVAILABLE = False
    logger.info("LLM Guardrail events not available in this CrewAI version")

# Flow Events - For flow execution tracking
try:
    from crewai.events.types.flow_events import (
        FlowStartedEvent,
        FlowFinishedEvent,
        FlowCreatedEvent,
    )

    FLOW_EVENTS_AVAILABLE = True
    logger.info("Flow events loaded successfully from CrewAI 0.177")
except ImportError:
    FLOW_EVENTS_AVAILABLE = False
    logger.info("Flow events not available in this CrewAI version")

# Import events that we know exist in our version
from crewai.events import (
    AgentExecutionCompletedEvent,
    CrewKickoffStartedEvent,
    CrewKickoffCompletedEvent,
)

# Import our queue system
from src.services.trace_queue import get_trace_queue

# Import the job_output_queue
from src.services.execution_logs_queue import enqueue_log, get_job_output_queue

# Import task tracking
from src.services.task_tracking_service import TaskTrackingService
from src.schemas.task_tracking import TaskStatusEnum, TaskStatusCreate

# Database operations disabled in callbacks (sync context)
# SessionLocal removed - use async_session_factory instead
from src.models.task import Task

# Import shared utilities
from src.engines.crewai.utils.agent_utils import extract_agent_name_from_event


# EventTypeDetector class removed - not needed with 100s of tools
# With hundreds of tools, pattern matching is not maintainable or reliable
# The simplified approach just tracks agent_execution and task_completed events


class EventTypeDetector:
    """Pattern-based event type detection from output content.

    .. deprecated:: 2.0
        With hundreds of tools available, pattern matching is not scalable.
        Kept for reference only - not used in current implementation.
        Use event-specific handlers instead.

    This class attempted to detect event types by analyzing output content
    using regular expression patterns. It could identify tool usage, task
    completion, and LLM reasoning patterns.
    """

    # Tool usage patterns - Order matters, more specific patterns first
    TOOL_PATTERNS = [
        (r"Using tool:\s*(\w+)", "tool_name"),  # Matches "Using tool: ToolName"
        (r"^Tool:\s*([^\n]+)", "tool_name"),  # Matches "Tool: ToolName" at line start
        (
            r"^Action:\s*([^\n]+)",
            "action_name",
        ),  # Matches "Action: action_name" at line start
        (r"Calling:\s*([^\n]+)", "tool_name"),
        (r"Executing:\s*([^\n]+)", "tool_name"),
    ]

    # Tool output patterns
    TOOL_OUTPUT_PATTERNS = [
        r"Tool Output:\s*(.+)",
        r"Action Output:\s*(.+)",
        r"Result:\s*(.+)",
        r"Response:\s*(.+)",
    ]

    # Task completion patterns
    COMPLETION_PATTERNS = [
        r"Final Answer:",
        r"Task Complete:",
        r"Completed:",
        r"FINAL ANSWER:",
        r"## Final Answer",
    ]

    # LLM/Reasoning patterns
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

        Args:
            output: The output content to analyze for event patterns.

        Returns:
            A tuple containing:
                - event_type: Detected event type ('tool_usage', 'task_completed', etc.)
                - context_info: Optional context information (tool name, etc.)
                - extra_data: Optional dictionary with additional extracted data

        Example:
            >>> event_type, context, data = EventTypeDetector.detect_event_type(
            ...     "Using tool: SearchTool\\nTool Output: Results..."
            ... )
            >>> print(event_type)  # 'tool_usage'
            >>> print(context)     # 'SearchTool'
        """
        if not output:
            return "agent_execution", None, None

        # Check for tool usage
        for pattern, name_key in cls.TOOL_PATTERNS:
            match = re.search(pattern, output, re.IGNORECASE)
            if match:
                tool_name = match.group(1).strip()

                # Try to extract tool output
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

        # Check for task completion
        for pattern in cls.COMPLETION_PATTERNS:
            if re.search(pattern, output, re.IGNORECASE):
                # Extract the actual answer
                answer_match = re.search(
                    f"{pattern}\\s*(.+)", output, re.DOTALL | re.IGNORECASE
                )
                answer = answer_match.group(1).strip() if answer_match else None

                extra_data = {"final_answer": answer, "pattern_matched": pattern}

                return "task_completed", "task_completion", extra_data

        # Check for LLM reasoning
        for pattern in cls.LLM_PATTERNS:
            if re.search(pattern, output, re.IGNORECASE):
                return "llm_call", "reasoning", {"pattern_matched": pattern}

        # Default to agent execution
        return "agent_execution", None, None


class AgentTraceEventListener(BaseEventListener):
    """Primary event listener for capturing and processing CrewAI agent traces.

    This listener implements a comprehensive event handling system that captures
    all CrewAI events and enqueues them for asynchronous processing. It maintains
    context across event streams and handles both main process and subprocess modes.

    Attributes:
        job_id: Unique identifier for the job being tracked
        group_context: Optional multi-tenant group isolation context
        _queue: Shared queue for trace data
        _init_time: Timestamp when listener was initialized
        _llm_stream_buffer: Buffer for aggregating LLM streaming chunks
        _task_registry: Static registry tracking tasks by job
        _active_context: Current active agent and task context
        _task_start_times: Task duration tracking

    Note:
        The listener operates in two modes:
        - Main process: Enqueues traces to shared queue
        - Subprocess: Writes directly to database
    """

    _init_logged = set()
    # Static task registry to track tasks by job
    _task_registry: Dict[str, Dict[str, str]] = {}
    # Track current active agent and task for context
    _active_context: Dict[str, Dict[str, str]] = {}
    # Track task start times for duration calculation
    _task_start_times: Dict[str, Dict[str, datetime]] = {}
    # Track active crew name for including in task traces (needed for checkpoint resume)
    _active_crew_name: Dict[str, str] = {}
    # Track tasks that already have execution completion traces (llm_response, agent_execution)
    # to avoid duplicate task_completed traces
    _tasks_with_execution_trace: Dict[str, set] = {}
    # Track agent execution events to prevent duplicates
    # Key: job_id -> set of (agent_name, task_key, output_hash) tuples
    _processed_agent_executions: Dict[str, set] = {}

    def __init__(
        self,
        job_id: str,
        group_context=None,
        register_global_events=False,
    ):
        """Initialize the agent trace event listener.

        Args:
            job_id: Unique identifier for the job being tracked.
                Must be a non-empty string.
            group_context: Optional group context for multi-tenant isolation.
                Contains primary_group_id and group_email for tenant separation.
            register_global_events: Whether to register global event listeners.
                .. deprecated:: 2.0
                    Use execution-scoped callbacks instead.

        Raises:
            ValueError: If job_id is empty or not a string.

        Example:
            >>> listener = AgentTraceEventListener(
            ...     job_id="job_123",
            ...     group_context={"primary_group_id": "grp_456"}
            ... )
        """
        if not job_id or not isinstance(job_id, str):
            raise ValueError("job_id must be a non-empty string")

        # Set job_id and context
        self.job_id = job_id
        self.group_context = group_context
        logger.debug(
            f"[TRACE_DEBUG] AgentTraceEventListener.__init__ - Getting trace queue for job {job_id}"
        )
        self._queue = get_trace_queue()
        logger.debug(
            f"[TRACE_DEBUG] AgentTraceEventListener.__init__ - Got queue: {type(self._queue)}, size: {self._queue.qsize()}"
        )
        self._init_time = datetime.now(timezone.utc)

        # Initialize registries for this job
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

        Attempts to extract agent name and ID from various event attributes,
        falling back to defaults if not available.

        Args:
            event: The AgentExecutionCompletedEvent containing agent data.

        Returns:
            A tuple containing:
                - agent_name: Agent role/name or "Unknown Agent" if not found
                - agent_id: Agent identifier or None if not available

        Note:
            Checks for 'role', 'name', 'id', and 'agent_id' attributes.
        """
        agent_name = "Unknown Agent"
        agent_id = None

        if hasattr(event, "agent"):
            agent = event.agent

            # Try to get role
            if hasattr(agent, "role"):
                agent_name = agent.role
            elif hasattr(agent, "name"):
                agent_name = agent.name
            else:
                agent_name = str(agent)

            # Try to get ID
            if hasattr(agent, "id"):
                agent_id = agent.id
            elif hasattr(agent, "agent_id"):
                agent_id = agent.agent_id

        return agent_name, agent_id

    def _extract_task_info(self, event) -> Tuple[str, str, str]:
        """Extract task identification and description from event.

        Args:
            event: Any CrewAI event that may contain task data.

        Returns:
            A tuple containing:
                - task_name: Task description/name or "Unknown Task"
                - task_id: Task identifier or None
                - task_description: Full task description or None

        Note:
            Checks multiple locations for task info:
            1. Direct event attributes: event.task_id, event.task_name (for memory events)
            2. Nested task object: event.task.id, event.task.description (for agent events)
        """
        task_name = "Unknown Task"
        task_id = None
        task_description = None

        # First, check for direct task_id on the event (CrewAI memory events have this)
        if hasattr(event, "task_id") and event.task_id:
            task_id = event.task_id
            logger.debug(f"Found direct task_id on event: {task_id}")

        # Check for direct task_name on event (some events have this)
        if hasattr(event, "task_name") and event.task_name:
            task_name = event.task_name
            task_description = event.task_name
            logger.debug(f"Found direct task_name on event: {task_name}")

        # Then check for nested task object (traditional agent events)
        if hasattr(event, "task") and event.task:
            task = event.task

            # Try to get description/name if not already found
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

            # Try to get ID from nested task if not already found
            if not task_id:
                if hasattr(task, "id") and task.id:
                    task_id = task.id
                elif hasattr(task, "task_id") and task.task_id:
                    task_id = task.task_id

        return task_name, task_id, task_description

    def _update_active_context(
        self, agent_name: str, task_name: str, log_prefix: str, task_id: str = None
    ) -> None:
        """Update the active context for tracking agent and task.

        Maintains a record of the currently active task PER AGENT for context
        preservation across event streams. This is critical for memory and tool
        events that don't have direct task references.

        Args:
            agent_name: Name of the active agent
            task_name: Name of the active task
            log_prefix: Logging prefix for debug output
            task_id: Optional task ID for correlation

        Note:
            Context is stored per job_id AND per agent to support concurrent
            agent execution within the same job.
        """
        if self.job_id not in self._active_context:
            self._active_context[self.job_id] = {}

        # Store context per agent (to handle concurrent agent execution)
        if agent_name and agent_name != "Unknown Agent":
            if agent_name not in self._active_context[self.job_id]:
                self._active_context[self.job_id][agent_name] = {}

            # Update task name if provided
            if task_name and task_name != "Unknown Task":
                self._active_context[self.job_id][agent_name]["task"] = task_name
                logger.debug(
                    f"{log_prefix} Updated active context for {agent_name} - Task: {task_name}"
                )

            # Update task_id if provided
            if task_id:
                self._active_context[self.job_id][agent_name]["task_id"] = task_id
                logger.debug(
                    f"{log_prefix} Updated active context for {agent_name} - Task ID: {task_id}"
                )

    def setup_listeners(self, crewai_event_bus):
        """Register comprehensive event handlers with CrewAI event bus.

        This method sets up handlers for all available CrewAI event types,
        including core events, tool events, memory/knowledge events, and
        specialized events like reasoning and guardrails.

        Args:
            crewai_event_bus: The CrewAI event bus instance to register
                handlers with. Must support the `on()` decorator pattern.

        Event Handlers Registered:
            - AgentExecutionCompletedEvent: Main agent execution handler
            - CrewKickoffStartedEvent: Crew initialization
            - CrewKickoffCompletedEvent: Crew completion with metrics
            - FlowStartedEvent: Flow execution initialization
            - FlowFinishedEvent: Flow execution completion
            - Memory/Knowledge events: If available in CrewAI version
            - Tool events: If available (usage start/finish/error)
            - LLM streaming: Always registered for real-time output
            - Task events: If available (started/completed/failed)
            - Reasoning events: If available (agent reasoning process)
            - Guardrail events: If available (LLM output validation)

        Note:
            Event availability is version-dependent and checked via
            conditional imports at module level.
        """
        log_prefix = f"[AgentTraceEventListener][{self.job_id}]"
        logger.info(f"{log_prefix} Setting up refined event listeners for CrewAI")
        logger.debug(f"[TRACE_DEBUG] setup_listeners called for job {self.job_id}")
        logger.debug(f"[TRACE_DEBUG] Event bus type: {type(crewai_event_bus)}")

        @crewai_event_bus.on(AgentExecutionStartedEvent)
        def on_agent_execution_started(source, event):
            """
            Handle agent execution start events to capture LLM request.
            This shows the prompt being sent to the LLM.
            """
            logger.debug(
                f"[TRACE_DEBUG] AgentExecutionStartedEvent received for job {self.job_id}"
            )
            try:
                # Extract agent information
                agent_name, agent_id = self._extract_agent_info(event)

                # Extract task information
                task_name, task_id, task_description = self._extract_task_info(event)

                # Get the task prompt (this is only the task description)
                task_prompt = (
                    event.task_prompt if hasattr(event, "task_prompt") else None
                )

                if task_prompt:
                    # Construct the complete LLM prompt by including the agent's system instructions
                    # The actual LLM call includes both system prompt (agent role/goal/backstory) and user prompt (task)
                    agent = event.agent if hasattr(event, "agent") else None
                    complete_prompt_parts = []

                    # Add system prompt (agent's role, goal, backstory)
                    if agent:
                        system_prompt_parts = []
                        if hasattr(agent, "role") and agent.role:
                            system_prompt_parts.append(f"Role: {agent.role}")
                        if hasattr(agent, "goal") and agent.goal:
                            system_prompt_parts.append(f"Goal: {agent.goal}")
                        if hasattr(agent, "backstory") and agent.backstory:
                            system_prompt_parts.append(f"Backstory: {agent.backstory}")

                        if system_prompt_parts:
                            system_prompt = "\n\n".join(system_prompt_parts)
                            complete_prompt_parts.append(
                                "=== SYSTEM PROMPT (Agent Instructions) ===\n"
                                + system_prompt
                            )

                    # Add task prompt (user prompt)
                    complete_prompt_parts.append(
                        "=== TASK PROMPT (User Request) ===\n" + task_prompt
                    )

                    # Combine into complete prompt
                    complete_prompt = "\n\n".join(complete_prompt_parts)

                    logger.info(
                        f"{log_prefix} Event: LLM Request | Agent: {agent_name} | Complete prompt length: {len(complete_prompt)} chars (system: {len(complete_prompt_parts[0]) if len(complete_prompt_parts) > 1 else 0}, task: {len(task_prompt)})"
                    )

                    # Build extra data
                    extra_data = {
                        "agent_role": agent_name,
                        "agent_id": agent_id,
                        "task": task_name,
                        "task_id": task_id,
                        "task_description": task_description,
                        "prompt_length": len(complete_prompt),
                        "system_prompt_length": (
                            len(complete_prompt_parts[0])
                            if len(complete_prompt_parts) > 1
                            else 0
                        ),
                        "task_prompt_length": len(task_prompt),
                        "timestamp": (
                            event.timestamp.isoformat()
                            if hasattr(event, "timestamp") and event.timestamp
                            else None
                        ),
                    }

                    # Include crew_name from active context
                    active_crew = AgentTraceEventListener._active_crew_name.get(
                        self.job_id
                    )
                    if active_crew:
                        extra_data["crew_name"] = active_crew

                    # Enqueue trace for LLM request with complete prompt
                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context="llm_request",
                        event_type="llm_request",
                        output_content=complete_prompt,  # Now includes both system and task prompts
                        extra_data=extra_data,
                    )

                    logger.info(
                        f"{log_prefix} LLM Request trace enqueued with complete prompt"
                    )

            except Exception as e:
                logger.error(
                    f"{log_prefix} Error in on_agent_execution_started: {e}",
                    exc_info=True,
                )

        @crewai_event_bus.on(AgentExecutionCompletedEvent)
        def on_agent_execution_completed(source, event):
            """
            Handle agent execution completion events.
            This consolidated event now encompasses tool usage, task completion, and LLM calls.

            NOTE: This event fires for every agent step, including tool usage.
            We skip creating traces for tool-related steps since we already have
            dedicated ToolExecutionStarted/Finished event handlers.
            """
            logger.debug(
                f"[TRACE_DEBUG] *** HANDLER CALLED *** AgentExecutionCompletedEvent received for job {self.job_id}!"
            )
            logger.info(
                f"{log_prefix} *** HANDLER CALLED *** AgentExecutionCompletedEvent received!"
            )
            try:
                # Extract agent information
                agent_name, agent_id = self._extract_agent_info(event)
                logger.debug(
                    f"{log_prefix} Extracted agent: {agent_name} (ID: {agent_id})"
                )

                # Extract task information
                task_name, task_id, task_description = self._extract_task_info(event)

                # Get output
                output_content = str(event.output) if event.output is not None else ""

                # CRITICAL: Check for duplicate agent_execution events
                # Create a unique key from agent, task, and output hash to detect duplicates
                task_key = task_id or task_name
                output_hash = hash(
                    output_content[:500]
                )  # Hash first 500 chars for efficiency
                execution_key = (agent_name, task_key, output_hash)

                if (
                    self.job_id
                    not in AgentTraceEventListener._processed_agent_executions
                ):
                    AgentTraceEventListener._processed_agent_executions[self.job_id] = (
                        set()
                    )

                if (
                    execution_key
                    in AgentTraceEventListener._processed_agent_executions[self.job_id]
                ):
                    logger.debug(
                        f"{log_prefix} Skipping duplicate agent_execution - already processed: {agent_name} -> {task_name}"
                    )
                    return

                # Mark this execution as processed
                AgentTraceEventListener._processed_agent_executions[self.job_id].add(
                    execution_key
                )

                # CRITICAL: Skip if this is a tool-related step
                # We already have dedicated handlers for ToolExecutionStarted/Finished
                # that create proper tool_usage traces with operation field
                tool_patterns = [
                    "ToolResult(",  # Tool finished result
                    "Tool: ",  # Tool started
                    "Using tool:",
                    "Action:",
                    "Tool Output:",
                    "Action Output:",
                ]

                if any(pattern in output_content for pattern in tool_patterns):
                    logger.debug(
                        f"{log_prefix} Skipping agent_execution - tool event already handled by dedicated handler"
                    )
                    return

                # Enhanced detection for different types of agent executions
                event_type = "agent_execution"
                event_context = task_name
                tool_info = None

                # Check for task completion
                if (
                    "Final Answer:" in output_content
                    or "Task Complete:" in output_content
                ):
                    event_type = "task_completed"
                    event_context = "task_completion"
                # Check for reasoning/thinking patterns
                elif any(
                    pattern in output_content
                    for pattern in [
                        "Thought:",
                        "Thinking:",
                        "Reasoning:",
                        "Analysis:",
                        "Plan:",
                    ]
                ):
                    event_type = "agent_reasoning"
                    event_context = "reasoning"
                # If none of the above patterns match, treat as LLM response
                else:
                    # This is a direct LLM response without tool usage or reasoning markers
                    event_type = "llm_response"
                    event_context = "llm_response"

                logger.info(
                    f"{log_prefix} Event: {event_type} | Agent: {agent_name} | Task: {task_name[:50]}..."
                )

                # Update active context (with task_id for memory event correlation)
                self._update_active_context(agent_name, task_name, log_prefix, task_id)

                # Build comprehensive extra data
                full_extra_data = {
                    "agent_role": agent_name,
                    "agent_id": agent_id,
                    "task": task_name,
                    "task_id": task_id,
                    "task_description": task_description,
                    "timestamp": (
                        event.timestamp.isoformat()
                        if hasattr(event, "timestamp") and event.timestamp
                        else None
                    ),
                }

                # Include crew_name from active context (needed for checkpoint resume)
                active_crew = AgentTraceEventListener._active_crew_name.get(self.job_id)
                if active_crew:
                    full_extra_data["crew_name"] = active_crew

                # Add tool information if detected
                if tool_info:
                    full_extra_data.update(tool_info)

                # Add output length for LLM responses
                if event_type == "llm_response":
                    full_extra_data["output_length"] = len(output_content)

                # Check for markdown flag in task
                if hasattr(event, "task") and hasattr(event.task, "markdown"):
                    full_extra_data["markdown"] = event.task.markdown

                # Handle task completion
                if event_type == "task_completed":
                    self._handle_task_completion(task_name, task_id, log_prefix)

                # Track that this task has an execution completion trace
                # This prevents duplicate task_completed traces from TaskCompletedEvent
                if event_type in ("llm_response", "agent_execution", "task_completed"):
                    if (
                        self.job_id
                        not in AgentTraceEventListener._tasks_with_execution_trace
                    ):
                        AgentTraceEventListener._tasks_with_execution_trace[
                            self.job_id
                        ] = set()
                    # Use task_id or task_name as key
                    task_key = task_id or task_name
                    AgentTraceEventListener._tasks_with_execution_trace[
                        self.job_id
                    ].add(task_key)
                    logger.debug(
                        f"{log_prefix} Marked task {task_key} as having execution trace (type: {event_type})"
                    )

                # Enqueue the trace
                self._enqueue_trace(
                    event_source=agent_name,
                    event_context=event_context,
                    event_type=event_type,
                    output_content=output_content,
                    extra_data=full_extra_data,
                )

                logger.info(
                    f"{log_prefix} *** HANDLER SUCCESS *** Event processed and enqueued"
                )

            except Exception as e:
                logger.error(
                    f"{log_prefix} *** HANDLER ERROR *** Error in on_agent_execution_completed: {e}",
                    exc_info=True,
                )

        @crewai_event_bus.on(CrewKickoffStartedEvent)
        def on_crew_kickoff_started(source, event):
            """Handle crew kickoff start events."""
            try:
                crew_name = event.crew_name if hasattr(event, "crew_name") else "crew"
                inputs = event.inputs if hasattr(event, "inputs") else {}

                logger.info(
                    f"{log_prefix} Event: CrewKickoffStarted | Crew: {crew_name}"
                )

                # Reset registries for this job
                AgentTraceEventListener._task_registry[self.job_id] = {}
                AgentTraceEventListener._task_start_times[self.job_id] = {}

                # Store active crew name for including in task traces (checkpoint resume support)
                AgentTraceEventListener._active_crew_name[self.job_id] = crew_name
                logger.debug(f"{log_prefix} Stored active crew name: {crew_name}")

                # Only create crew-level trace if NOT in flow mode
                # In flow mode, flow_started event provides the top-level context
                import os

                is_flow_mode = (
                    os.environ.get("FLOW_SUBPROCESS_MODE", "false").lower() == "true"
                )

                if not is_flow_mode:
                    # Create trace for crew start (standalone crew execution)
                    self._enqueue_trace(
                        event_source="crew",
                        event_context=crew_name,
                        event_type="crew_started",
                        output_content=f"Crew '{crew_name}' execution started",
                        extra_data={"inputs": inputs} if inputs else None,
                    )
                else:
                    # In flow mode, just log but don't create top-level trace
                    logger.debug(
                        f"{log_prefix} Suppressing crew_started trace in flow mode for crew: {crew_name}"
                    )

            except Exception as e:
                logger.error(
                    f"{log_prefix} Error in on_crew_kickoff_started: {e}", exc_info=True
                )

        @crewai_event_bus.on(CrewKickoffCompletedEvent)
        def on_crew_kickoff_completed(source, event):
            """Handle crew kickoff completion events."""
            try:
                crew_name = event.crew_name if hasattr(event, "crew_name") else "crew"
                output_content = (
                    str(event.output) if hasattr(event, "output") else "Crew completed"
                )
                total_tokens = (
                    event.total_tokens if hasattr(event, "total_tokens") else None
                )

                logger.info(
                    f"{log_prefix} Event: CrewKickoffCompleted | Crew: {crew_name}"
                )

                # Create extra data with completion statistics
                extra_data = {}
                if total_tokens is not None:
                    extra_data["total_tokens"] = total_tokens

                # Calculate execution time
                if self._init_time:
                    execution_time = (
                        datetime.now(timezone.utc) - self._init_time
                    ).total_seconds()
                    extra_data["execution_time_seconds"] = execution_time

                # Log total tokens and execution time
                if extra_data:
                    logger.info(
                        f"{log_prefix} Crew Completed - Total Tokens: {extra_data.get('total_tokens', 'N/A')}, Execution Time: {extra_data.get('execution_time_seconds', 'N/A')} seconds"
                    )
                    # Also enqueue as a separate log for visibility
                    enqueue_log(
                        execution_id=self.job_id,
                        content=f"📊 Crew Statistics: Total Tokens: {extra_data.get('total_tokens', 'N/A')}, Execution Time: {extra_data.get('execution_time_seconds', 'N/A')} seconds",
                        group_context=self.group_context,
                    )

                # Only create crew-level trace if NOT in flow mode
                # In flow mode, flow_completed event provides the top-level context
                import os

                is_flow_mode = (
                    os.environ.get("FLOW_SUBPROCESS_MODE", "false").lower() == "true"
                )

                if not is_flow_mode:
                    # Create trace for crew completion (standalone crew execution)
                    self._enqueue_trace(
                        event_source="crew",
                        event_context=crew_name,
                        event_type="crew_completed",
                        output_content=output_content,
                        extra_data=extra_data if extra_data else None,
                    )
                else:
                    # In flow mode, just log but don't create top-level trace
                    logger.debug(
                        f"{log_prefix} Suppressing crew_completed trace in flow mode for crew: {crew_name}"
                    )

                # Clean up job-specific tracking data to prevent memory leaks
                self._cleanup_job_tracking()

            except Exception as e:
                logger.error(
                    f"{log_prefix} Error in on_crew_kickoff_completed: {e}",
                    exc_info=True,
                )

        # Register flow event handlers if available
        if FLOW_EVENTS_AVAILABLE:

            @crewai_event_bus.on(FlowStartedEvent)
            def on_flow_started(source, event):
                """Handle flow start events."""
                try:
                    flow_name = (
                        event.flow_name if hasattr(event, "flow_name") else "flow"
                    )
                    inputs = event.inputs if hasattr(event, "inputs") else {}

                    logger.info(f"{log_prefix} Event: FlowStarted | Flow: {flow_name}")

                    # Create trace for flow start
                    self._enqueue_trace(
                        event_source="flow",
                        event_context=flow_name,
                        event_type="flow_started",
                        output_content=f"Flow '{flow_name}' execution started",
                        extra_data={"inputs": inputs} if inputs else None,
                    )

                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_flow_started: {e}", exc_info=True
                    )

            @crewai_event_bus.on(FlowFinishedEvent)
            def on_flow_finished(source, event):
                """Handle flow completion events."""
                try:
                    flow_name = (
                        event.flow_name if hasattr(event, "flow_name") else "flow"
                    )
                    result = event.result if hasattr(event, "result") else None
                    output_content = str(result) if result else "Flow completed"

                    logger.info(f"{log_prefix} Event: FlowFinished | Flow: {flow_name}")

                    # Calculate execution time
                    extra_data = {}
                    if self._init_time:
                        execution_time = (
                            datetime.now(timezone.utc) - self._init_time
                        ).total_seconds()
                        extra_data["execution_time_seconds"] = execution_time
                        logger.info(
                            f"{log_prefix} Flow Completed - Execution Time: {execution_time} seconds"
                        )

                    # Create trace for flow completion
                    self._enqueue_trace(
                        event_source="flow",
                        event_context=flow_name,
                        event_type="flow_completed",
                        output_content=output_content,
                        extra_data=extra_data if extra_data else None,
                    )

                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_flow_finished: {e}", exc_info=True
                    )

        # Register memory and knowledge event handlers if available
        if MEMORY_EVENTS_AVAILABLE:

            @crewai_event_bus.on(MemorySaveStartedEvent)
            def on_memory_save_started(source, event):
                """Handle memory save start events.

                Per CrewAI source code:
                - event.value contains the content being saved
                - event.source_type contains the memory type (short_term_memory, long_term_memory, entity_memory)
                We capture this and store it so we can include it in the completed event.
                """
                try:
                    agent_name = extract_agent_name_from_event(event) or "Unknown Agent"
                    # Per CrewAI source: source_type identifies the memory type
                    # e.g., "short_term_memory", "long_term_memory", "entity_memory"
                    source_type = (
                        event.source_type if hasattr(event, "source_type") else None
                    )
                    # Convert source_type to friendly name
                    memory_type = "memory"
                    if source_type:
                        if "short_term" in source_type:
                            memory_type = "short_term"
                        elif "long_term" in source_type:
                            memory_type = "long_term"
                        elif "entity" in source_type:
                            memory_type = "entity"
                        else:
                            memory_type = source_type.replace("_memory", "")

                    # Per CrewAI docs: event.value contains the content being saved
                    memory_value = (
                        str(event.value) if hasattr(event, "value") and event.value else None
                    )

                    # Extract task info for proper task association
                    task_name, task_id, task_description = self._extract_task_info(
                        event
                    )

                    logger.info(
                        f"{log_prefix} Event: MemorySaveStarted | Agent: {agent_name} | Type: {memory_type} | source_type: {source_type} | Task: {task_name}"
                    )
                    if memory_value:
                        logger.info(f"{log_prefix} Memory value (first 200 chars): {memory_value[:200]}")

                    # Store the value so we can use it in the completed event
                    # Use a context key that includes agent and memory_type
                    context_key = f"pending_save_{agent_name}_{memory_type}"
                    if self.job_id not in self._active_context:
                        self._active_context[self.job_id] = {}
                    self._active_context[self.job_id][context_key] = {
                        "value": memory_value,
                        "memory_type": memory_type,
                        "task_name": task_name,
                        "task_id": task_id,
                    }
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_memory_save_started: {e}",
                        exc_info=True,
                    )

            @crewai_event_bus.on(MemorySaveCompletedEvent)
            def on_memory_save_completed(source, event):
                """Handle memory save completion events.

                Per CrewAI source code:
                - event.value contains the saved content
                - event.source_type contains the memory type (short_term_memory, long_term_memory, entity_memory)
                - event.save_time_ms contains the save duration
                """
                try:
                    agent_name = extract_agent_name_from_event(event) or "Unknown Agent"

                    # Per CrewAI source: source_type identifies the memory type
                    source_type = (
                        event.source_type if hasattr(event, "source_type") else None
                    )
                    # Convert source_type to friendly name
                    memory_type = "memory"
                    if source_type:
                        if "short_term" in source_type:
                            memory_type = "short_term"
                        elif "long_term" in source_type:
                            memory_type = "long_term"
                        elif "entity" in source_type:
                            memory_type = "entity"
                        else:
                            memory_type = source_type.replace("_memory", "")

                    # Extract task info for proper task association
                    task_name, task_id, task_description = self._extract_task_info(
                        event
                    )

                    # Try to get the saved value from the started event (stored in context)
                    context_key = f"pending_save_{agent_name}_{memory_type}"
                    pending_save = None
                    if self.job_id in self._active_context:
                        pending_save = self._active_context[self.job_id].pop(context_key, None)

                    # Get the value from the pending context if available
                    saved_value = pending_save.get("value") if pending_save else None

                    # Per CrewAI source: event.value contains the saved content
                    event_value = (
                        str(event.value) if hasattr(event, "value") and event.value else None
                    )

                    # Get metadata for additional context
                    event_metadata = (
                        event.metadata if hasattr(event, "metadata") and event.metadata else {}
                    )

                    logger.info(
                        f"{log_prefix} Event: MemorySaveCompleted | Agent: {agent_name} | Type: {memory_type} | source_type: {source_type} | Task: {task_name}"
                    )

                    # Use event.value first (most reliable), then saved_value from started event
                    output_content = (
                        event_value or saved_value or f"Saved to {memory_type} memory"
                    )

                    # For entity memory batch saves, enhance the display with entity count from metadata
                    entity_count = None
                    if memory_type == "entity" and event_metadata:
                        entity_count = event_metadata.get("entity_count")
                        if entity_count and "Saved" in (output_content or ""):
                            # The output is already "Saved N entities", keep it as is
                            # but log additional info
                            logger.info(f"{log_prefix} Entity batch save: {entity_count} entities")

                    if output_content and len(output_content) > 200:
                        logger.info(f"{log_prefix} Output content (first 200 chars): {output_content[:200]}")
                    else:
                        logger.info(f"{log_prefix} Output content: {output_content}")

                    # Build extra_data with additional metadata
                    extra_data = {
                        "operation": "save_completed",
                        "memory_type": memory_type,
                        "backend": "default",
                        "task_name": task_name,
                        "task_id": task_id,
                    }

                    # Add entity count if available
                    if entity_count:
                        extra_data["entity_count"] = entity_count

                    # Add any relevant metadata fields
                    if event_metadata:
                        for key in ["quality", "expected_output", "agent"]:
                            if key in event_metadata:
                                extra_data[key] = event_metadata[key]

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context=f"saved_{memory_type}",
                        event_type="memory_write",
                        output_content=output_content,
                        extra_data=extra_data,
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_memory_save_completed: {e}",
                        exc_info=True,
                    )

            @crewai_event_bus.on(MemoryQueryStartedEvent)
            def on_memory_query_started(source, event):
                """Handle memory query start events.

                Per CrewAI source code:
                - event.query contains the query text
                - event.limit has the limit
                - event.source_type contains the memory type (short_term_memory, long_term_memory, entity_memory)

                We store the query to include it in the completed event.
                NOTE: We don't save to database - only the completed event is saved.
                """
                try:
                    agent_name = extract_agent_name_from_event(event) or "Unknown Agent"

                    # Per CrewAI source: source_type identifies the memory type (NOT memory_type)
                    source_type = (
                        event.source_type if hasattr(event, "source_type") else None
                    )
                    # Convert source_type to friendly name
                    memory_type = "memory"
                    if source_type:
                        if "short_term" in source_type:
                            memory_type = "short_term"
                        elif "long_term" in source_type:
                            memory_type = "long_term"
                        elif "entity" in source_type:
                            memory_type = "entity"

                    # Per CrewAI source: event.query contains the query text
                    query = str(event.query) if hasattr(event, "query") else "query"
                    query_limit = event.limit if hasattr(event, "limit") else None

                    # Extract task info for proper task association
                    task_name, task_id, task_description = self._extract_task_info(
                        event
                    )

                    logger.info(
                        f"{log_prefix} Event: MemoryQueryStarted | Agent: {agent_name} | Type: {memory_type} | Task: {task_name}"
                    )
                    logger.info(f"{log_prefix} Query: {query[:200] if len(query) > 200 else query}")

                    # Store the query so we can include it in the completed event
                    context_key = f"pending_query_{agent_name}_{memory_type}"
                    if self.job_id not in self._active_context:
                        self._active_context[self.job_id] = {}
                    self._active_context[self.job_id][context_key] = {
                        "query": query,
                        "limit": query_limit,
                        "memory_type": memory_type,
                        "task_name": task_name,
                        "task_id": task_id,
                    }
                    # NOTE: Not saving to database - only the completed event is saved
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_memory_query_started: {e}",
                        exc_info=True,
                    )

            @crewai_event_bus.on(MemoryQueryCompletedEvent)
            def on_memory_query_completed(source, event):
                """Handle memory query completion events.

                Per CrewAI source code:
                - event.query contains the query text
                - event.results contains the query results
                - event.query_time_ms has the duration
                - event.source_type contains the memory type (short_term_memory, long_term_memory, entity_memory)
                """
                try:
                    agent_name = extract_agent_name_from_event(event) or "Unknown Agent"

                    # Per CrewAI source: source_type identifies the memory type (NOT memory_type)
                    source_type = (
                        event.source_type if hasattr(event, "source_type") else None
                    )
                    # Convert source_type to friendly name
                    memory_type = "memory"
                    if source_type:
                        if "short_term" in source_type:
                            memory_type = "short_term"
                        elif "long_term" in source_type:
                            memory_type = "long_term"
                        elif "entity" in source_type:
                            memory_type = "entity"

                    # Extract task info for proper task association
                    task_name, task_id, task_description = self._extract_task_info(
                        event
                    )

                    # Try to get the query from the started event (stored in context)
                    context_key = f"pending_query_{agent_name}_{memory_type}"
                    pending_query = None
                    if self.job_id in self._active_context:
                        pending_query = self._active_context[self.job_id].pop(context_key, None)

                    # Get query from context or from event
                    query = (
                        pending_query.get("query") if pending_query
                        else (str(event.query) if hasattr(event, "query") else None)
                    )

                    # Per CrewAI source: event.results contains the results
                    actual_results = (
                        event.results if hasattr(event, "results") else None
                    )
                    results_count = len(actual_results) if actual_results and hasattr(actual_results, "__len__") else 0

                    # Format results for display - handle empty results more gracefully
                    if actual_results is None:
                        results_str = "No results"
                    elif isinstance(actual_results, list) and len(actual_results) == 0:
                        results_str = f"No matching memories found in {memory_type.replace('_', ' ')} memory"
                    elif isinstance(actual_results, list) and len(actual_results) > 0:
                        # Format each result nicely
                        formatted_results = []
                        for i, result in enumerate(actual_results, 1):
                            if isinstance(result, dict):
                                # Extract key fields from result dict
                                content = result.get("context", result.get("memory", result.get("data", str(result))))
                                score = result.get("score", result.get("relevance", None))
                                if score is not None:
                                    formatted_results.append(f"[{i}] (score: {score:.2f}) {content}")
                                else:
                                    formatted_results.append(f"[{i}] {content}")
                            else:
                                formatted_results.append(f"[{i}] {result}")
                        results_str = "\n".join(formatted_results)
                    else:
                        results_str = str(actual_results)

                    # Get query time if available
                    query_time_ms = event.query_time_ms if hasattr(event, "query_time_ms") else None

                    logger.info(
                        f"{log_prefix} Event: MemoryQueryCompleted | Agent: {agent_name} | Type: {memory_type} | Task: {task_name}"
                    )
                    logger.info(
                        f"{log_prefix} Results count: {results_count}, Query time: {query_time_ms}ms"
                    )
                    if results_str and results_str != "No results":
                        logger.info(f"{log_prefix} Results (first 300 chars): {results_str[:300]}")

                    # Build a more informative output that includes query context
                    if results_count == 0 and query:
                        # Show what was searched when no results found
                        query_preview = query[:200] + "..." if len(query) > 200 else query
                        output_with_context = f"{results_str}\n\nSearch query: {query_preview}"
                    else:
                        output_with_context = results_str

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context=f"memory_query[{memory_type}]",
                        event_type="memory_retrieval",
                        output_content=output_with_context,
                        extra_data={
                            "operation": "query_completed",
                            "memory_type": memory_type,
                            "backend": "default",
                            "task_name": task_name,
                            "task_id": task_id,
                            "query": query,
                            "results_count": results_count,
                            "query_time_ms": query_time_ms,
                        },
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_memory_query_completed: {e}",
                        exc_info=True,
                    )

            @crewai_event_bus.on(MemoryRetrievalCompletedEvent)
            def on_memory_retrieval_completed(source, event):
                """Handle memory retrieval completion events.

                IMPORTANT: This event contains the AGGREGATED memory content from ALL memory types
                (short-term, long-term, entity) combined. This is the actual memory that gets
                injected into the task prompt.

                Per CrewAI source code (agent.py):
                - contextual_memory.build_context_for_task() combines all memory types
                - event.memory_content contains this combined/formatted memory
                - event.retrieval_time_ms has the duration
                - event.source_type is typically "agent" (since agent aggregates all memories)
                """
                try:
                    agent_name = extract_agent_name_from_event(event) or "Unknown Agent"

                    # Extract task info for proper task association
                    task_name, task_id, task_description = self._extract_task_info(
                        event
                    )

                    # Per CrewAI source: event.memory_content has the AGGREGATED content
                    memory_content = (
                        str(event.memory_content)
                        if hasattr(event, "memory_content") and event.memory_content
                        else None
                    )
                    retrieval_time_ms = event.retrieval_time_ms if hasattr(event, "retrieval_time_ms") else None

                    logger.info(
                        f"{log_prefix} Event: MemoryRetrievalCompleted | Agent: {agent_name} | Task: {task_name} | Time: {retrieval_time_ms}ms"
                    )
                    if memory_content:
                        content_preview = memory_content[:500] if len(memory_content) > 500 else memory_content
                        logger.info(f"{log_prefix} Aggregated memory content (first 500 chars): {content_preview}")

                    # Only save if there's actual memory content
                    if memory_content and memory_content.strip():
                        self._enqueue_trace(
                            event_source=agent_name,
                            event_context="memory_context",
                            event_type="memory_context_retrieved",
                            output_content=memory_content,
                            extra_data={
                                "operation": "context_retrieval_completed",
                                "memory_type": "aggregated",
                                "retrieval_time_ms": retrieval_time_ms,
                                "task_name": task_name,
                                "task_id": task_id,
                                "content_length": len(memory_content),
                            },
                        )
                    else:
                        logger.info(f"{log_prefix} No aggregated memory content to save (empty)")
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_memory_retrieval_completed: {e}",
                        exc_info=True,
                    )

        # Register Knowledge Event Handlers if available
        if KNOWLEDGE_EVENTS_AVAILABLE:
            logger.info(f"{log_prefix} Registering knowledge event handlers for CrewAI")

            @crewai_event_bus.on(KnowledgeRetrievalStartedEvent)
            def on_knowledge_retrieval_started(source, event):
                """Handle knowledge retrieval start events."""
                try:
                    agent_name = extract_agent_name_from_event(event) or "Unknown Agent"
                    query = (
                        str(event.query)
                        if hasattr(event, "query")
                        else "knowledge query"
                    )

                    logger.info(
                        f"{log_prefix} Event: KnowledgeRetrievalStarted | Agent: {agent_name}"
                    )

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context="knowledge_retrieval",
                        event_type="knowledge_retrieval_started",
                        output_content=query,  # Just the actual query
                        extra_data={"operation": "retrieval_started", "query": query},
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_knowledge_retrieval_started: {e}",
                        exc_info=True,
                    )

            @crewai_event_bus.on(KnowledgeRetrievalCompletedEvent)
            def on_knowledge_retrieval_completed(source, event):
                """Handle knowledge retrieval completion events."""
                try:
                    agent_name = extract_agent_name_from_event(event) or "Unknown Agent"
                    results = (
                        str(event.results)
                        if hasattr(event, "results")
                        else "knowledge retrieved"
                    )

                    logger.info(
                        f"{log_prefix} Event: KnowledgeRetrievalCompleted | Agent: {agent_name}"
                    )

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context="knowledge_retrieval",
                        event_type="knowledge_retrieval",
                        output_content=results,  # Just the actual results
                        extra_data={"operation": "retrieval_completed"},
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_knowledge_retrieval_completed: {e}",
                        exc_info=True,
                    )

        # Register Tool Event Handlers if available
        if TOOL_EVENTS_AVAILABLE:
            logger.info(
                f"{log_prefix} Registering tool event handlers for CrewAI 0.177"
            )

            @crewai_event_bus.on(ToolUsageStartedEvent)
            def on_tool_usage_started(source, event):
                """Handle tool usage start events."""
                try:
                    # Extract agent and tool information
                    agent_name = event.agent_role or (
                        event.agent.role
                        if event.agent and hasattr(event.agent, "role")
                        else "Unknown Agent"
                    )
                    tool_name = event.tool_name
                    tool_args = event.tool_args

                    # Get task context - first from event, then from active context
                    task_name = event.task_name if hasattr(event, "task_name") and event.task_name else None
                    task_id = event.task_id if hasattr(event, "task_id") and event.task_id else None

                    # Fall back to active context if task info not on event
                    if (not task_name or not task_id) and self.job_id in self._active_context:
                        agent_ctx = self._active_context[self.job_id].get(agent_name, {})
                        if not task_name and agent_ctx.get("task"):
                            task_name = agent_ctx["task"]
                        if not task_id and agent_ctx.get("task_id"):
                            task_id = agent_ctx["task_id"]

                    logger.info(
                        f"{log_prefix} Event: ToolUsageStarted | Agent: {agent_name} | Tool: {tool_name} | Task: {task_name}"
                    )

                    # Create comprehensive extra data with task attribution
                    extra_data = {
                        "tool_name": tool_name,
                        "tool_args": str(tool_args) if tool_args else None,
                        "tool_class": (
                            event.tool_class if hasattr(event, "tool_class") else None
                        ),
                        "agent_role": agent_name,
                        "task_name": task_name,
                        "task_id": task_id,
                        "operation": "tool_started",
                    }

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context=f"tool:{tool_name}",
                        event_type="tool_usage",
                        output_content=(
                            str(tool_args) if tool_args else ""
                        ),  # Just the actual tool arguments
                        extra_data=extra_data,
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_tool_usage_started: {e}",
                        exc_info=True,
                    )

            @crewai_event_bus.on(ToolUsageFinishedEvent)
            def on_tool_usage_finished(source, event):
                """Handle tool usage completion events."""
                try:
                    # Extract agent and tool information
                    agent_name = event.agent_role or (
                        event.agent.role
                        if event.agent and hasattr(event.agent, "role")
                        else "Unknown Agent"
                    )
                    tool_name = event.tool_name
                    tool_output = (
                        str(event.output) if hasattr(event, "output") else "No output"
                    )

                    # Get task context - first from event, then from active context
                    task_name = event.task_name if hasattr(event, "task_name") and event.task_name else None
                    task_id = event.task_id if hasattr(event, "task_id") and event.task_id else None

                    # Fall back to active context if task info not on event
                    if (not task_name or not task_id) and self.job_id in self._active_context:
                        agent_ctx = self._active_context[self.job_id].get(agent_name, {})
                        if not task_name and agent_ctx.get("task"):
                            task_name = agent_ctx["task"]
                        if not task_id and agent_ctx.get("task_id"):
                            task_id = agent_ctx["task_id"]

                    logger.info(
                        f"{log_prefix} Event: ToolUsageFinished | Agent: {agent_name} | Tool: {tool_name} | Task: {task_name}"
                    )

                    # Calculate duration if timestamps available
                    duration = None
                    if hasattr(event, "started_at") and hasattr(event, "finished_at"):
                        duration = (
                            event.finished_at - event.started_at
                        ).total_seconds()

                    # Create comprehensive extra data with task attribution
                    extra_data = {
                        "tool_name": tool_name,
                        "tool_class": (
                            event.tool_class if hasattr(event, "tool_class") else None
                        ),
                        "agent_role": agent_name,
                        "task_name": task_name,
                        "task_id": task_id,
                        "from_cache": (
                            event.from_cache if hasattr(event, "from_cache") else False
                        ),
                        "duration_seconds": duration,
                        "operation": "tool_finished",
                    }

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context=f"tool:{tool_name}",
                        event_type="tool_usage",
                        output_content=tool_output,  # Capture complete tool output
                        extra_data=extra_data,
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_tool_usage_finished: {e}",
                        exc_info=True,
                    )

            @crewai_event_bus.on(ToolUsageErrorEvent)
            def on_tool_usage_error(source, event):
                """Handle tool usage error events."""
                try:
                    # Extract agent and tool information
                    agent_name = event.agent_role or (
                        event.agent.role
                        if event.agent and hasattr(event.agent, "role")
                        else "Unknown Agent"
                    )
                    tool_name = event.tool_name
                    error = (
                        str(event.error) if hasattr(event, "error") else "Unknown error"
                    )

                    logger.error(
                        f"{log_prefix} Event: ToolUsageError | Agent: {agent_name} | Tool: {tool_name} | Error: {error}"
                    )

                    # Create comprehensive extra data
                    extra_data = {
                        "tool_name": tool_name,
                        "tool_args": (
                            str(event.tool_args)
                            if hasattr(event, "tool_args")
                            else None
                        ),
                        "tool_class": (
                            event.tool_class if hasattr(event, "tool_class") else None
                        ),
                        "agent_role": agent_name,
                        "task_name": (
                            event.task_name if hasattr(event, "task_name") else None
                        ),
                        "error": error,
                        "operation": "tool_error",
                    }

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context=f"tool:{tool_name}",
                        event_type="tool_error",
                        output_content=f"Tool error: {error}",
                        extra_data=extra_data,
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_tool_usage_error: {e}", exc_info=True
                    )

        # LLM Streaming is ALWAYS available and critical - register it unconditionally
        @crewai_event_bus.on(LLMStreamChunkEvent)
        def on_llm_stream_chunk(source, event):
            """Handle LLM streaming chunk events."""
            try:
                chunk = str(event.chunk) if hasattr(event, "chunk") else ""

                # Don't filter - capture all LLM streaming
                if not chunk.strip():  # Skip only completely empty chunks
                    return

                agent_name = extract_agent_name_from_event(event)

                # Aggregate chunks to avoid too many individual traces
                # Store chunks in a buffer and flush periodically
                if not hasattr(self, "_llm_stream_buffer"):
                    self._llm_stream_buffer = {}

                if agent_name not in self._llm_stream_buffer:
                    self._llm_stream_buffer[agent_name] = []

                self._llm_stream_buffer[agent_name].append(chunk)

                # Flush buffer when we have enough content or see completion markers
                buffer_content = "".join(self._llm_stream_buffer[agent_name])
                if len(buffer_content) > 500 or any(
                    marker in chunk
                    for marker in ["\n\n", "Final Answer:", "Tool:", "Action:"]
                ):
                    logger.info(
                        f"{log_prefix} Event: LLMStream | Agent: {agent_name} | Size: {len(buffer_content)}"
                    )

                    # Check if this is tool-related
                    event_type = "llm_stream"
                    if any(
                        pattern in buffer_content
                        for pattern in ["Tool:", "Action:", "Using tool:"]
                    ):
                        event_type = "llm_tool_stream"

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context="llm_streaming",
                        event_type=event_type,
                        output_content=buffer_content[
                            :1000
                        ],  # Limit size but keep more content
                        extra_data={
                            "stream_size": len(buffer_content),
                            "chunk_count": len(self._llm_stream_buffer[agent_name]),
                        },
                    )

                    # Clear buffer after flushing
                    self._llm_stream_buffer[agent_name] = []

            except Exception as e:
                logger.error(
                    f"{log_prefix} Error in on_llm_stream_chunk: {e}", exc_info=True
                )

        # Register Task Event Handlers if available
        if TASK_EVENTS_AVAILABLE:
            logger.info(
                f"{log_prefix} Registering task event handlers for CrewAI 0.177"
            )

            @crewai_event_bus.on(TaskStartedEvent)
            def on_task_started(source, event):
                """Handle task start events."""
                try:
                    # Extract task information
                    task = event.task if hasattr(event, "task") else None
                    task_name = (
                        task.description
                        if task and hasattr(task, "description")
                        else "Unknown Task"
                    )
                    task_id = task.id if task and hasattr(task, "id") else None
                    context = str(event.context) if hasattr(event, "context") else None

                    # Extract the frontend task ID if available
                    frontend_task_id = None
                    if task and hasattr(task, "_kasal_task_id"):
                        frontend_task_id = task._kasal_task_id
                        logger.info(
                            f"{log_prefix} [DEBUG] Found _kasal_task_id on task: {frontend_task_id}"
                        )
                        logger.debug(
                            f"{log_prefix} Found frontend task ID: {frontend_task_id}"
                        )
                    else:
                        logger.info(
                            f"{log_prefix} [DEBUG] No _kasal_task_id attribute on task object"
                        )

                    # Extract agent if available
                    agent_name = (
                        task.agent.role
                        if task and hasattr(task, "agent") and task.agent
                        else "System"
                    )

                    logger.info(
                        f"{log_prefix} Event: TaskStarted | Task: {task_name[:50]} | Agent: {agent_name} | Frontend ID: {frontend_task_id}"
                    )

                    # CRITICAL: Update active context so memory events can use correct task info
                    self._update_active_context(
                        agent_name, task_name, log_prefix, task_id
                    )

                    extra_data = {
                        "task_name": task_name,
                        "task_id": task_id,
                        "agent_role": agent_name,
                        "context": context,
                        "operation": "task_started",
                        "agent_task": f"{agent_name} → {task_name}",  # Clear hierarchy
                        "frontend_task_id": frontend_task_id,  # Add frontend task ID
                    }

                    # Include crew_name from active context for flow execution tracking
                    active_crew = AgentTraceEventListener._active_crew_name.get(
                        self.job_id
                    )
                    if active_crew:
                        extra_data["crew_name"] = active_crew

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context="starting_task",
                        event_type="task_started",
                        output_content=task_name,  # Full task name in output
                        extra_data=extra_data,
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_task_started: {e}", exc_info=True
                    )

            @crewai_event_bus.on(TaskCompletedEvent)
            def on_task_completed(source, event):
                """Handle task completion events."""
                try:
                    # Extract task information
                    task = event.task if hasattr(event, "task") else None
                    task_name = (
                        task.description
                        if task and hasattr(task, "description")
                        else "Unknown Task"
                    )
                    task_id = task.id if task and hasattr(task, "id") else None
                    output = (
                        str(event.output)
                        if hasattr(event, "output")
                        else "Task completed"
                    )

                    # NOTE: We no longer skip task_completed events based on execution traces.
                    # task_completed events are needed for SSE broadcasting to update frontend UI
                    # even though they won't be stored in the database.

                    # Extract the frontend task ID if available
                    frontend_task_id = None
                    if task and hasattr(task, "_kasal_task_id"):
                        frontend_task_id = task._kasal_task_id
                        logger.info(
                            f"{log_prefix} [DEBUG] Found _kasal_task_id on task: {frontend_task_id}"
                        )
                        logger.debug(
                            f"{log_prefix} Found frontend task ID: {frontend_task_id}"
                        )
                    else:
                        logger.info(
                            f"{log_prefix} [DEBUG] No _kasal_task_id attribute on task object"
                        )

                    # Extract agent if available
                    agent_name = (
                        task.agent.role
                        if task and hasattr(task, "agent") and task.agent
                        else "System"
                    )

                    logger.info(
                        f"{log_prefix} Event: TaskCompleted | Task: {task_name[:50]} | Agent: {agent_name} | Frontend ID: {frontend_task_id}"
                    )

                    extra_data = {
                        "task_name": task_name,
                        "task_id": task_id,
                        "agent_role": agent_name,
                        "operation": "task_completed",
                        "agent_task": f"{agent_name} → {task_name}",  # Clear hierarchy
                        "frontend_task_id": frontend_task_id,  # Add frontend task ID
                    }

                    # Include crew_name from active context for flow execution tracking
                    active_crew = AgentTraceEventListener._active_crew_name.get(
                        self.job_id
                    )
                    if active_crew:
                        extra_data["crew_name"] = active_crew

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context="completing_task",
                        event_type="task_completed",
                        output_content=output,
                        extra_data=extra_data,
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_task_completed: {e}", exc_info=True
                    )

            @crewai_event_bus.on(TaskFailedEvent)
            def on_task_failed(source, event):
                """Handle task failure events."""
                try:
                    # Extract task information
                    task = event.task if hasattr(event, "task") else None
                    task_name = (
                        task.description
                        if task and hasattr(task, "description")
                        else "Unknown Task"
                    )
                    task_id = task.id if task and hasattr(task, "id") else None
                    error = (
                        str(event.error) if hasattr(event, "error") else "Unknown error"
                    )

                    # Extract agent if available
                    agent_name = (
                        task.agent.role
                        if task and hasattr(task, "agent") and task.agent
                        else "System"
                    )

                    logger.error(
                        f"{log_prefix} Event: TaskFailed | Task: {task_name[:50]} | Agent: {agent_name} | Error: {error}"
                    )

                    extra_data = {
                        "task_name": task_name,
                        "task_id": task_id,
                        "agent_role": agent_name,
                        "error": error,
                        "operation": "task_failed",
                        "agent_task": f"{agent_name} → {task_name}",  # Clear hierarchy
                    }

                    # Include crew_name from active context for flow execution tracking
                    active_crew = AgentTraceEventListener._active_crew_name.get(
                        self.job_id
                    )
                    if active_crew:
                        extra_data["crew_name"] = active_crew

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context="task_error",
                        event_type="task_failed",
                        output_content=f"Task failed: {error}",
                        extra_data=extra_data,
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_task_failed: {e}", exc_info=True
                    )

        # Register Reasoning Event Handlers if available
        if REASONING_EVENTS_AVAILABLE:
            logger.info(
                f"{log_prefix} Registering reasoning event handlers for CrewAI 0.177"
            )

            @crewai_event_bus.on(AgentReasoningStartedEvent)
            def on_reasoning_started(source, event):
                """Handle agent reasoning start events."""
                try:
                    agent_name = (
                        event.agent_role
                        if hasattr(event, "agent_role")
                        else "Unknown Agent"
                    )
                    task_id = event.task_id if hasattr(event, "task_id") else None

                    logger.info(
                        f"{log_prefix} Event: ReasoningStarted | Agent: {agent_name}"
                    )

                    extra_data = {
                        "agent_role": agent_name,
                        "task_id": task_id,
                        "operation": "reasoning_started",
                    }

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context="reasoning",
                        event_type="agent_reasoning",
                        output_content=f"Agent {agent_name} starting reasoning process",
                        extra_data=extra_data,
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_reasoning_started: {e}",
                        exc_info=True,
                    )

            @crewai_event_bus.on(AgentReasoningCompletedEvent)
            def on_reasoning_completed(source, event):
                """Handle agent reasoning completion events."""
                try:
                    agent_name = (
                        event.agent_role
                        if hasattr(event, "agent_role")
                        else "Unknown Agent"
                    )
                    task_id = event.task_id if hasattr(event, "task_id") else None
                    plan = str(event.plan) if hasattr(event, "plan") else None
                    ready = event.ready if hasattr(event, "ready") else False

                    logger.info(
                        f"{log_prefix} Event: ReasoningCompleted | Agent: {agent_name} | Ready: {ready}"
                    )

                    extra_data = {
                        "agent_role": agent_name,
                        "task_id": task_id,
                        "ready": ready,
                        "operation": "reasoning_completed",
                    }

                    output_content = (
                        plan if plan else f"Agent {agent_name} completed reasoning"
                    )

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context="reasoning",
                        event_type="agent_reasoning",
                        output_content=output_content,
                        extra_data=extra_data,
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_reasoning_completed: {e}",
                        exc_info=True,
                    )

            @crewai_event_bus.on(AgentReasoningFailedEvent)
            def on_reasoning_failed(source, event):
                """Handle agent reasoning failure events."""
                try:
                    agent_name = (
                        event.agent_role
                        if hasattr(event, "agent_role")
                        else "Unknown Agent"
                    )
                    task_id = event.task_id if hasattr(event, "task_id") else None
                    error = (
                        str(event.error) if hasattr(event, "error") else "Unknown error"
                    )

                    logger.error(
                        f"{log_prefix} Event: ReasoningFailed | Agent: {agent_name} | Error: {error}"
                    )

                    extra_data = {
                        "agent_role": agent_name,
                        "task_id": task_id,
                        "error": error,
                        "operation": "reasoning_failed",
                    }

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context="reasoning",
                        event_type="agent_reasoning_error",
                        output_content=f"Reasoning failed: {error}",
                        extra_data=extra_data,
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_reasoning_failed: {e}", exc_info=True
                    )

        # Register LLM Guardrail Event Handlers if available
        if LLM_GUARDRAIL_EVENTS_AVAILABLE:
            logger.info(
                f"{log_prefix} Registering LLM guardrail event handlers for CrewAI 0.177"
            )

            @crewai_event_bus.on(LLMGuardrailStartedEvent)
            def on_guardrail_started(source, event):
                """Handle LLM guardrail start events."""
                try:
                    # Extract guardrail info - try to get description if it's an LLMGuardrail
                    guardrail_obj = event.guardrail if hasattr(event, "guardrail") else None
                    guardrail_name = str(guardrail_obj) if guardrail_obj else "guardrail"
                    guardrail_description = None

                    # Try to extract description from LLMGuardrail or GuardrailWrapper
                    if guardrail_obj:
                        if hasattr(guardrail_obj, "description"):
                            guardrail_description = guardrail_obj.description
                        elif hasattr(guardrail_obj, "guardrail") and hasattr(guardrail_obj.guardrail, "config"):
                            # GuardrailWrapper wrapping a code-based guardrail
                            config = guardrail_obj.guardrail.config
                            if isinstance(config, dict):
                                guardrail_description = config.get("description", config.get("type", ""))

                    retry_count = (
                        event.retry_count if hasattr(event, "retry_count") else 0
                    )

                    # Extract task info
                    task_name = event.task_name if hasattr(event, "task_name") else None
                    task_id = event.task_id if hasattr(event, "task_id") else None

                    # Try to extract agent from source or event
                    agent_name = "System"
                    if hasattr(event, "agent_role") and event.agent_role:
                        agent_name = event.agent_role
                    elif hasattr(source, "role"):
                        agent_name = source.role

                    logger.info(
                        f"{log_prefix} Event: GuardrailStarted | Task: {task_name} | Guardrail: {guardrail_name} | Retry: {retry_count}"
                    )

                    extra_data = {
                        "guardrail": guardrail_name,
                        "retry_count": retry_count,
                        "operation": "guardrail_started",
                    }

                    # Add description if available
                    if guardrail_description:
                        extra_data["guardrail_description"] = guardrail_description
                    if task_name:
                        extra_data["task_name"] = task_name
                    if task_id:
                        extra_data["task_id"] = task_id

                    # Create descriptive output content
                    output_content = f"Starting guardrail validation"
                    if task_name:
                        output_content = f"Validating output for task: {task_name}"
                    if guardrail_description:
                        output_content += f"\n\nValidation Criteria:\n{guardrail_description}"
                    if retry_count > 0:
                        output_content += f"\n\n(Retry attempt #{retry_count})"

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context="guardrail_check",
                        event_type="llm_guardrail",
                        output_content=output_content,
                        extra_data=extra_data,
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_guardrail_started: {e}",
                        exc_info=True,
                    )

            @crewai_event_bus.on(LLMGuardrailCompletedEvent)
            def on_guardrail_completed(source, event):
                """Handle LLM guardrail completion events."""
                try:
                    success = event.success if hasattr(event, "success") else False
                    result = event.result if hasattr(event, "result") else None
                    error = str(event.error) if hasattr(event, "error") and event.error else None
                    retry_count = (
                        event.retry_count if hasattr(event, "retry_count") else 0
                    )

                    # Extract task info
                    task_name = event.task_name if hasattr(event, "task_name") else None
                    task_id = event.task_id if hasattr(event, "task_id") else None

                    # Try to extract agent from source or event
                    agent_name = "System"
                    if hasattr(event, "agent_role") and event.agent_role:
                        agent_name = event.agent_role
                    elif hasattr(source, "role"):
                        agent_name = source.role

                    status = "PASSED" if success else "FAILED"
                    logger.info(
                        f"{log_prefix} Event: GuardrailCompleted | Task: {task_name} | Status: {status} | Retries: {retry_count}"
                    )

                    extra_data = {
                        "success": success,
                        "retry_count": retry_count,
                        "operation": "guardrail_completed",
                        "status": status,
                    }

                    if error:
                        extra_data["error"] = error
                    if task_name:
                        extra_data["task_name"] = task_name
                    if task_id:
                        extra_data["task_id"] = task_id

                    # Build detailed output content
                    output_lines = []

                    # Header with status
                    if task_name:
                        output_lines.append(f"Guardrail Validation Result for: {task_name}")
                    else:
                        output_lines.append("Guardrail Validation Result")
                    output_lines.append(f"Status: {status}")

                    # Include the result details
                    if result is not None:
                        result_str = str(result)
                        # Check if result is a tuple (success, message) from code guardrails
                        if isinstance(result, tuple) and len(result) >= 2:
                            output_lines.append(f"\nValidation Output:")
                            output_lines.append(f"  Valid: {result[0]}")
                            output_lines.append(f"  Message: {result[1]}")
                            extra_data["validation_valid"] = result[0]
                            extra_data["validation_message"] = str(result[1])
                        elif result_str and result_str != "None":
                            output_lines.append(f"\nResult Details:")
                            output_lines.append(result_str)
                            extra_data["result"] = result_str

                    # Include error if present
                    if error:
                        output_lines.append(f"\nError: {error}")

                    # Include retry info
                    if retry_count > 0:
                        output_lines.append(f"\n(After {retry_count} retry attempts)")

                    output_content = "\n".join(output_lines)

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context="guardrail_check",
                        event_type="llm_guardrail",
                        output_content=output_content,
                        extra_data=extra_data,
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_guardrail_completed: {e}",
                        exc_info=True,
                    )

            @crewai_event_bus.on(LLMGuardrailFailedEvent)
            def on_guardrail_failed(source, event):
                """Handle LLM guardrail failure events (technical errors)."""
                try:
                    error = str(event.error) if hasattr(event, "error") else "Unknown error"
                    retry_count = (
                        event.retry_count if hasattr(event, "retry_count") else 0
                    )

                    # Extract task info
                    task_name = event.task_name if hasattr(event, "task_name") else None
                    task_id = event.task_id if hasattr(event, "task_id") else None

                    # Extract agent info
                    agent_name = "unknown_agent"
                    if hasattr(event, "agent_role") and event.agent_role:
                        agent_name = event.agent_role
                    elif hasattr(event, "from_agent") and event.from_agent:
                        agent_name = getattr(event.from_agent, "role", "unknown_agent")

                    logger.error(
                        f"{log_prefix} Event: GuardrailFailed | Task: {task_name} | Agent: {agent_name} | Error: {error} | Retries: {retry_count}"
                    )

                    extra_data = {
                        "error": error,
                        "retry_count": retry_count,
                        "operation": "guardrail_failed",
                        "status": "ERROR",
                    }

                    if task_name:
                        extra_data["task_name"] = task_name
                    if task_id:
                        extra_data["task_id"] = task_id

                    # Build detailed output content
                    output_lines = []
                    if task_name:
                        output_lines.append(f"Guardrail Error for task: {task_name}")
                    else:
                        output_lines.append("Guardrail Error")
                    output_lines.append(f"Status: ERROR")
                    output_lines.append(f"\nError Details: {error}")
                    if retry_count > 0:
                        output_lines.append(f"\n(After {retry_count} retry attempts)")

                    output_content = "\n".join(output_lines)

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context="guardrail_check",
                        event_type="llm_guardrail",
                        output_content=output_content,
                        extra_data=extra_data,
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_guardrail_failed: {e}",
                        exc_info=True,
                    )

        # Note: Tool, LLM Call, and Task events from main module are not available in CrewAI 0.177
        # All these are captured through AgentExecutionCompletedEvent or specific type modules
        if False:  # Legacy code - disabled for v0.177
            # These will be enabled when we upgrade to a version that has these events
            pass

            # @crewai_event_bus.on(ToolUsageStartedEvent)
            def on_tool_usage_started(source, event):
                """Handle tool usage start events."""
                try:
                    agent_name = extract_agent_name_from_event(event)
                    tool_name = (
                        str(event.tool_name)
                        if hasattr(event, "tool_name")
                        else "unknown_tool"
                    )
                    tool_input = (
                        str(event.tool_input) if hasattr(event, "tool_input") else ""
                    )

                    logger.info(
                        f"{log_prefix} Event: ToolUsageStarted | Agent: {agent_name} | Tool: {tool_name}"
                    )

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context=f"tool:{tool_name}",
                        event_type="tool_usage",
                        output_content=f"Starting tool: {tool_name}\nInput: {tool_input[:200]}",
                        extra_data={
                            "tool_name": tool_name,
                            "tool_input": tool_input,
                            "status": "started",
                        },
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_tool_usage_started: {e}",
                        exc_info=True,
                    )

            @crewai_event_bus.on(ToolUsageFinishedEvent)
            def on_tool_usage_finished(source, event):
                """Handle tool usage completion events."""
                try:
                    agent_name = extract_agent_name_from_event(event)
                    tool_name = (
                        str(event.tool_name)
                        if hasattr(event, "tool_name")
                        else "unknown_tool"
                    )
                    tool_output = (
                        str(event.output)
                        if hasattr(event, "output")
                        else (
                            str(event.tool_output)
                            if hasattr(event, "tool_output")
                            else ""
                        )
                    )

                    logger.info(
                        f"{log_prefix} Event: ToolUsageFinished | Agent: {agent_name} | Tool: {tool_name}"
                    )

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context=f"tool:{tool_name}",
                        event_type="tool_usage",
                        output_content=f"Tool completed: {tool_name}\nOutput: {tool_output[:500]}",
                        extra_data={
                            "tool_name": tool_name,
                            "tool_output": tool_output,
                            "status": "completed",
                        },
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_tool_usage_finished: {e}",
                        exc_info=True,
                    )

            @crewai_event_bus.on(ToolUsageErrorEvent)
            def on_tool_usage_error(source, event):
                """Handle tool usage error events."""
                try:
                    agent_name = extract_agent_name_from_event(event)
                    tool_name = (
                        str(event.tool_name)
                        if hasattr(event, "tool_name")
                        else "unknown_tool"
                    )
                    error_message = (
                        str(event.error) if hasattr(event, "error") else "Unknown error"
                    )

                    logger.error(
                        f"{log_prefix} Event: ToolUsageError | Agent: {agent_name} | Tool: {tool_name} | Error: {error_message}"
                    )

                    self._enqueue_trace(
                        event_source=agent_name,
                        event_context=f"tool:{tool_name}",
                        event_type="tool_error",
                        output_content=f"Tool error: {tool_name}\nError: {error_message}",
                        extra_data={
                            "tool_name": tool_name,
                            "error": error_message,
                            "status": "error",
                        },
                    )
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Error in on_tool_usage_error: {e}", exc_info=True
                    )

        # Register LLM event handlers if available
        if LLM_EVENTS_AVAILABLE:
            # LLMCallStartedEvent and LLMCallCompletedEvent don't exist in CrewAI 0.177+
            # These events have been removed/deprecated in the current version
            # Only LLMStreamChunkEvent is available for LLM-related events
            pass

            # Legacy code for reference - these events no longer exist:
            # @crewai_event_bus.on(LLMCallStartedEvent)
            # def on_llm_call_started(source, event): ...
            # @crewai_event_bus.on(LLMCallCompletedEvent)
            # def on_llm_call_completed(source, event): ...

        # NOTE: Duplicate task handlers removed - already handled above

        # Log that all listeners have been registered
        logger.debug(
            f"[TRACE_DEBUG] All event listeners registered for job {self.job_id}"
        )
        logger.info(f"{log_prefix} Event listener setup complete")

    def _handle_task_completion(self, task_name: str, task_id: str, log_prefix: str):
        """Handle task completion tracking and duration calculation.

        Calculates task execution duration if start time was recorded and
        updates task status in the database.

        Args:
            task_name: Name/description of the completed task
            task_id: Unique identifier for the task
            log_prefix: Logging prefix for output messages

        Note:
            Task duration is calculated only if a start time was recorded
            in _task_start_times registry.
        """
        try:
            # Calculate task duration if we have start time
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
                # Clean up start time
                del self._task_start_times[self.job_id][task_id]

            # Update task status in database
            self._update_task_status(task_name, TaskStatusEnum.COMPLETED, log_prefix)

        except Exception as e:
            logger.error(
                f"{log_prefix} Error handling task completion: {e}", exc_info=True
            )

    def _update_task_status(
        self, task_name: str, status: TaskStatusEnum, log_prefix: str
    ):
        """Update task status in database with subprocess safety.

        Updates the task status in the database, handling both main process
        and subprocess execution modes appropriately.

        Args:
            task_name: Name of the task to update
            status: New status for the task (TaskStatusEnum)
            log_prefix: Logging prefix for output messages

        Note:
            In subprocess mode (CREW_SUBPROCESS_MODE=true), uses a new
            event loop to avoid conflicts with existing loops.
        """
        try:
            import os

            is_subprocess = os.environ.get("CREW_SUBPROCESS_MODE") == "true"

            if is_subprocess:
                logger.debug(
                    f"{log_prefix} Updating task status to {status} in database"
                )

                import asyncio
                from concurrent.futures import ThreadPoolExecutor

                def update_task_status_sync():
                    """Update task status."""
                    try:
                        # Task tracking disabled - requires async operations
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

        # Clean up all job-specific tracking dictionaries
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

    def _enqueue_trace(
        self,
        event_source: str,
        event_context: str,
        event_type: str,
        output_content: str,
        extra_data: dict = None,
    ) -> None:
        """Enqueue trace data for asynchronous processing.

        Creates a structured trace entry and either enqueues it to the shared
        queue (main process) or writes directly to database (subprocess).

        Args:
            event_source: Source of the event (agent name, "crew", etc.)
            event_context: Context information (task name, tool name, etc.)
            event_type: Type of event ("agent_execution", "tool_usage", etc.)
            output_content: The actual output/content from the event
            extra_data: Optional dictionary with additional metadata

        Note:
            In subprocess mode (CREW_SUBPROCESS_MODE=true), traces are
            written directly to database to avoid queue sharing issues.

        Example:
            >>> self._enqueue_trace(
            ...     event_source="Research Agent",
            ...     event_context="tool:SearchTool",
            ...     event_type="tool_usage",
            ...     output_content="Search results...",
            ...     extra_data={"tool_name": "SearchTool"}
            ... )
        """
        log_prefix = f"[AgentTraceEventListener][{self.job_id}]"
        try:
            timestamp = datetime.now(timezone.utc)
            time_since_init = (timestamp - self._init_time).total_seconds()

            logger.debug(
                f"[TRACE_DEBUG] _enqueue_trace called: event_type={event_type}, source={event_source}"
            )

            logger.debug(
                f"{log_prefix} Enqueuing {event_type} trace | Source: {event_source} | Context: {event_context[:30] if event_context else 'None'}..."
            )

            # Create trace data for database/queue
            trace_data = {
                "job_id": self.job_id,
                "event_source": event_source,
                "event_context": event_context,
                "event_type": event_type,
                "created_at": timestamp.replace(
                    tzinfo=None
                ),  # Convert to naive datetime
                "output": {  # Store as JSON in 'output' field
                    "content": output_content,
                    "time_since_init": time_since_init,
                    "extra_data": extra_data or {},
                },
                # CRITICAL: Store metadata in trace_metadata field for frontend task tracking
                "trace_metadata": extra_data or {},
            }

            # Add group context if available
            if self.group_context:
                trace_data["group_id"] = self.group_context.primary_group_id
                trace_data["group_email"] = self.group_context.group_email

            # Check if we're in subprocess mode
            import os

            is_subprocess = os.environ.get("CREW_SUBPROCESS_MODE") == "true"

            if is_subprocess:
                logger.debug(
                    f"[TRACE_DEBUG] Subprocess mode detected, writing directly to database"
                )
                logger.debug(
                    f"{log_prefix} Writing trace directly to database (subprocess mode)"
                )
                # In subprocess mode, we need to write directly since the queue isn't shared
                self._write_trace_to_db_async(trace_data)
            else:
                logger.debug(f"[TRACE_DEBUG] Main process mode, putting trace on queue")
                logger.debug(
                    f"[TRACE_DEBUG] Queue before put: size={self._queue.qsize()}"
                )
                logger.debug(f"{log_prefix} Putting trace on queue (main process mode)")
                self._queue.put(trace_data)
                logger.debug(
                    f"[TRACE_DEBUG] Queue after put: size={self._queue.qsize()}"
                )

                # Also enqueue to job output queue for real-time streaming
                enqueue_log(
                    self.job_id,
                    {"content": output_content, "timestamp": timestamp.isoformat()},
                )

        except Exception as e:
            logger.error(f"{log_prefix} Error in _enqueue_trace: {e}", exc_info=True)

    def _write_trace_to_db_async(self, trace_data: dict) -> None:
        """Write trace to PostgreSQL database using async operations.

        Handles asynchronous database writes in subprocess mode, creating
        a new event loop and thread to avoid blocking the main execution.

        Args:
            trace_data: Dictionary containing trace information including:
                - job_id: Job identifier
                - event_source: Source of the event
                - event_context: Event context
                - event_type: Type of event
                - output: Output data with content and metadata
                - group_id: Optional group identifier
                - group_email: Optional group email

        Note:
            Uses ThreadPoolExecutor to run async code in a separate thread,
            allowing non-blocking database writes.
        """
        log_prefix = f"[AgentTraceEventListener][{self.job_id}]"
        try:
            import asyncio
            from concurrent.futures import ThreadPoolExecutor
            from src.services.execution_trace_service import ExecutionTraceService

            logger.info(
                f"{log_prefix} Writing trace to PostgreSQL - Event: {trace_data.get('event_type')} | Source: {trace_data.get('event_source')}"
            )

            # Create an async task to write to database
            async def write_async():
                try:
                    import json
                    from uuid import UUID

                    # Custom JSON encoder to handle UUIDs
                    class UUIDEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, UUID):
                                return str(obj)
                            return super().default(obj)

                    # Extract and clean all data to handle UUIDs
                    output_data = trace_data.get("output", {})

                    # Clean the entire output object to handle UUIDs
                    if output_data:
                        cleaned_output = json.loads(
                            json.dumps(output_data, cls=UUIDEncoder)
                        )
                    else:
                        cleaned_output = {}

                    # Extract metadata from cleaned output
                    if isinstance(cleaned_output, dict):
                        extra_data = cleaned_output.get("extra_data", {})
                    else:
                        extra_data = {}

                    # Use the ExecutionTraceService which writes to PostgreSQL
                    from src.db.database_router import get_smart_db_session

                    async for session in get_smart_db_session():
                        trace_service = ExecutionTraceService(session)
                        await trace_service.create_trace(
                            {
                                "job_id": trace_data.get("job_id"),
                                "event_source": trace_data.get("event_source"),
                                "event_context": trace_data.get("event_context"),
                                "event_type": trace_data.get("event_type"),
                                "output": cleaned_output,  # Use cleaned output
                                "trace_metadata": extra_data,
                                "group_id": trace_data.get("group_id"),
                                "group_email": trace_data.get("group_email"),
                            }
                        )
                    logger.info(
                        f"{log_prefix} ✅ Trace written to PostgreSQL successfully"
                    )
                    return True
                except ValueError as e:
                    # This is expected when job doesn't exist - not an error
                    logger.debug(f"{log_prefix} Trace skipped (job doesn't exist): {e}")
                    return False
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Failed to write trace to PostgreSQL: {e}",
                        exc_info=True,
                    )
                    return False

            # Function to run async code in a new thread
            def run_in_thread():
                logger.info(f"{log_prefix} Running async write in thread")
                result = asyncio.run(write_async())
                if result:
                    logger.info(f"{log_prefix} Thread completed successfully")
                else:
                    logger.error(f"{log_prefix} Thread completed with errors")
                return result

            # Use ThreadPoolExecutor to run async code in a separate thread
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(run_in_thread)
                # Don't wait for result - let it run in background
                logger.info(f"{log_prefix} Submitted async write to thread pool")

        except Exception as e:
            logger.error(
                f"{log_prefix} ❌ Error in _write_trace_to_db_async: {e}", exc_info=True
            )

    def _write_trace_to_db(self, trace_data: dict) -> None:
        """Write trace directly to database (for subprocess mode).

        Synchronous database write operation used when running in subprocess
        mode where queue sharing is not available.

        Args:
            trace_data: Dictionary containing trace information to persist

        Note:
            Handles both SQLite and PostgreSQL databases. Will attempt to
            create the execution_trace table if it doesn't exist in SQLite.
        """
        log_prefix = f"[AgentTraceEventListener][{self.job_id}]"
        try:
            # Use the sync SessionLocal which is already configured for the correct database
            # SessionLocal removed - use async_session_factory instead
            from src.models.execution_trace import ExecutionTrace

            logger.debug(f"{log_prefix} Writing trace to database using SessionLocal")

            # Use async session for database operations
            from src.db.session import async_session_factory
            import asyncio

            async def write_trace_async():
                async with async_session_factory() as db:
                    try:
                        trace = ExecutionTrace(**trace_data)
                        db.add(trace)
                        await db.commit()
                        logger.info(
                            f"{log_prefix} ✅ Trace written to database successfully"
                        )
                    except Exception as e:
                        await db.rollback()
                        logger.error(f"{log_prefix} Database error: {e}", exc_info=True)
                        raise

            # Run async operation in sync context
            try:
                asyncio.run(write_trace_async())
            except Exception as e:
                logger.error(f"{log_prefix} Failed to write trace: {e}")

                # If the table doesn't exist in SQLite, try to create it
                if "no such table" in str(e).lower():
                    logger.warning(
                        f"{log_prefix} Table execution_trace doesn't exist, attempting to create it..."
                    )
                    try:
                        # Import Base to get the table definition
                        from src.models.execution_trace import Base
                        from sqlalchemy import inspect

                        # Check if we're using SQLite
                        if "sqlite" in str(db.bind.url):
                            # Create the execution_trace table
                            inspector = inspect(db.bind)
                            if "execution_trace" not in inspector.get_table_names():
                                Base.metadata.tables["execution_trace"].create(db.bind)
                                logger.info(
                                    f"{log_prefix} Created execution_trace table in SQLite"
                                )

                                # Retry the insert
                                trace = ExecutionTrace(**trace_data)
                                db.add(trace)
                                db.commit()
                                logger.info(
                                    f"{log_prefix} ✅ Trace written after creating table"
                                )
                    except Exception as create_error:
                        logger.error(
                            f"{log_prefix} Failed to create table: {create_error}",
                            exc_info=True,
                        )
                raise
            finally:
                db.close()

        except Exception as e:
            logger.error(
                f"{log_prefix} ❌ Error writing trace to database: {e}", exc_info=True
            )


class TaskCompletionEventListener(BaseEventListener):
    """Specialized listener for task completion tracking.

    .. deprecated:: 2.0
        Task completion is now handled through AgentExecutionCompletedEvent.
        This class is maintained for backward compatibility only.

    Attributes:
        job_id: Unique identifier for the job
        group_context: Optional multi-tenant group context
        _init_time: Initialization timestamp

    Note:
        In CrewAI 0.177+, task events are consolidated into the main
        AgentExecutionCompletedEvent handler for simplified processing.
    """

    def __init__(self, job_id: str, group_context=None):
        """Initialize the task completion listener.

        Args:
            job_id: Unique identifier for the job
            group_context: Optional group context for multi-tenant isolation
        """
        self.job_id = job_id
        self.group_context = group_context
        self._init_time = datetime.now(timezone.utc)
        logger.info(f"[TaskCompletionEventListener][{self.job_id}] Initialized")

    def setup_listeners(self, crewai_event_bus):
        """Register task completion event handlers.

        Args:
            crewai_event_bus: The CrewAI event bus instance.

        Note:
            This method is a no-op in current implementation as task
            events are handled by AgentTraceEventListener.
        """
        log_prefix = f"[TaskCompletionEventListener][{self.job_id}]"

        # Task events are now handled through AgentExecutionCompletedEvent
        logger.info(
            f"{log_prefix} Task tracking integrated into AgentExecutionCompletedEvent handler"
        )
