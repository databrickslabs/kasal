"""OTel Event Bridge — subscribes to CrewAI event bus and emits OTel spans.

Bridges CrewAI's event system to the OTel pipeline so each event becomes a
span that flows through SimpleSpanProcessor -> KasalDBSpanExporter -> DB.
This gives 35+ event types in the trace timeline instead of the 2 that
the openinference CrewAI instrumentor provides.
"""

import logging
from typing import Any, Optional

from opentelemetry.trace import Tracer, StatusCode

logger = logging.getLogger(__name__)


# Event type -> (span_name, event_type_string)
# span_name maps to SPAN_NAME_MAP in db_exporter.py
_EVENT_SPAN_MAP = {
    # Crew lifecycle
    "CrewKickoffStartedEvent": ("CrewAI.crew.kickoff", "crew_started"),
    "CrewKickoffCompletedEvent": ("CrewAI.crew.complete", "crew_completed"),
    # Task lifecycle
    "TaskStartedEvent": ("CrewAI.task.execute", "task_started"),
    "TaskCompletedEvent": ("CrewAI.task.complete", "task_completed"),
    "TaskFailedEvent": ("CrewAI.task.fail", "task_failed"),
    # Agent execution
    "AgentExecutionStartedEvent": ("CrewAI.agent.execute", "agent_execution"),
    "AgentExecutionCompletedEvent": ("CrewAI.agent.complete", "llm_response"),
    # Tool usage
    "ToolUsageStartedEvent": ("CrewAI.tool.execute", "tool_usage"),
    "ToolUsageFinishedEvent": ("CrewAI.tool.complete", "tool_usage"),
    "ToolUsageErrorEvent": ("CrewAI.tool.error", "tool_error"),
    # Memory events — map to frontend-expected event_type names
    "MemorySaveStartedEvent": ("kasal.memory.save_started", "memory_write_started"),
    "MemorySaveCompletedEvent": ("kasal.memory.save_completed", "memory_write"),
    "MemoryQueryStartedEvent": ("kasal.memory.query_started", "memory_retrieval_started"),
    "MemoryQueryCompletedEvent": ("kasal.memory.query_completed", "memory_retrieval"),
    "MemoryRetrievalCompletedEvent": ("kasal.memory.retrieval_completed", "memory_retrieval_completed"),
    # Knowledge events — map completed to frontend's knowledge_operation
    "KnowledgeRetrievalStartedEvent": ("kasal.knowledge.retrieval_started", "knowledge_retrieval_started"),
    "KnowledgeRetrievalCompletedEvent": ("kasal.knowledge.retrieval_completed", "knowledge_operation"),
    # Reasoning events — map to frontend's agent_reasoning/agent_reasoning_error
    "AgentReasoningStartedEvent": ("kasal.reasoning.started", "reasoning_started"),
    "AgentReasoningCompletedEvent": ("kasal.reasoning.completed", "agent_reasoning"),
    "AgentReasoningFailedEvent": ("kasal.reasoning.failed", "agent_reasoning_error"),
    # Guardrail events — map completed/failed to frontend's llm_guardrail
    "LLMGuardrailStartedEvent": ("kasal.guardrail.started", "guardrail_started"),
    "LLMGuardrailCompletedEvent": ("kasal.guardrail.completed", "llm_guardrail"),
    "LLMGuardrailFailedEvent": ("kasal.guardrail.failed", "llm_guardrail"),
    # Flow events — map finished to frontend's flow_completed
    "FlowStartedEvent": ("kasal.flow.started", "flow_started"),
    "FlowFinishedEvent": ("kasal.flow.finished", "flow_completed"),
    "FlowCreatedEvent": ("kasal.flow.created", "flow_created"),
    # MCP events
    "MCPConnectionStartedEvent": ("kasal.mcp.connection_started", "mcp_connection_started"),
    "MCPConnectionCompletedEvent": ("kasal.mcp.connection_completed", "mcp_connection_completed"),
    "MCPToolExecutionStartedEvent": ("kasal.mcp.tool_started", "mcp_tool_started"),
    "MCPToolExecutionCompletedEvent": ("kasal.mcp.tool_completed", "mcp_tool_completed"),
    # HITL events
    "HumanFeedbackRequestedEvent": ("kasal.hitl.feedback_requested", "hitl_feedback_requested"),
    "HumanFeedbackReceivedEvent": ("kasal.hitl.feedback_received", "hitl_feedback_received"),
    # LLM call events
    "LLMCallStartedEvent": ("kasal.llm.call_started", "llm_call"),
    "LLMCallCompletedEvent": ("kasal.llm.call_completed", "llm_response"),
    "LLMCallFailedEvent": ("kasal.llm.call_failed", "llm_call_failed"),
}

# Events to skip (too noisy)
_SKIP_EVENTS = {"LLMStreamChunkEvent"}


def _safe_str(val: Any, max_len: int = 500) -> str:
    """Safely convert a value to a truncated string."""
    if val is None:
        return ""
    s = str(val)
    return s[:max_len] if len(s) > max_len else s


def _get_agent_name(event: Any) -> str:
    """Extract agent name from an event object.

    Checks BaseEvent.agent_role first, then event.agent.role, then
    event.task.agent.role as fallback.
    """
    # BaseEvent sets agent_role directly
    agent_role = getattr(event, "agent_role", None)
    if agent_role:
        return str(agent_role)

    # Try event.agent.role
    agent = getattr(event, "agent", None)
    if agent and hasattr(agent, "role") and agent.role:
        return str(agent.role)

    # Try event.task.agent.role
    task = getattr(event, "task", None)
    if task:
        task_agent = getattr(task, "agent", None)
        if task_agent and hasattr(task_agent, "role") and task_agent.role:
            return str(task_agent.role)

    return ""


def _get_task_name(event: Any) -> str:
    """Extract task description from an event object.

    Checks BaseEvent.task_name first, then event.task.description/name.
    """
    # BaseEvent sets task_name directly via _set_task_params
    task_name = getattr(event, "task_name", None)
    if task_name:
        return _safe_str(task_name)

    task = getattr(event, "task", None)
    if task:
        desc = getattr(task, "description", None)
        if desc:
            return _safe_str(desc)
        name = getattr(task, "name", None)
        if name:
            return _safe_str(name)
    return ""


def _get_tool_name(event: Any) -> str:
    """Extract tool name from an event object."""
    tool_name = getattr(event, "tool_name", None) or getattr(event, "tool", None)
    if tool_name:
        return _safe_str(tool_name, 200)
    return ""


def _get_output(event: Any) -> str:
    """Extract output/result content from an event object."""
    for attr in ("output", "result", "results", "response", "content",
                 "message", "value", "memory_content"):
        val = getattr(event, attr, None)
        if val is not None:
            return str(val)
    return ""


class OTelEventBridge:
    """Subscribes to CrewAI event bus and creates OTel spans per event.

    Each span flows through the existing OTel pipeline:
    SimpleSpanProcessor -> KasalDBSpanExporter -> execution_trace DB.

    Args:
        tracer: An OTel Tracer from the execution's TracerProvider.
        job_id: The execution/job ID for logging.
        group_context: Optional group context for tenant isolation.
    """

    def __init__(
        self,
        tracer: Tracer,
        job_id: str,
        group_context: Optional[Any] = None,
    ):
        self._tracer = tracer
        self._job_id = job_id
        self._group_context = group_context
        self._registered_count = 0
        # Captured from CrewKickoffStartedEvent and stamped on all subsequent
        # spans so that task-level traces carry the crew name for flow monitoring.
        self._current_crew_name: Optional[str] = None

    def register(self, event_bus: Any) -> int:
        """Register span-creating handlers for all available CrewAI event types.

        Imports event classes directly from crewai.events (no callback layer dependency).

        Args:
            event_bus: The crewai_event_bus instance.

        Returns:
            Number of event types registered.
        """
        registered = 0

        # All event classes we want to subscribe to, grouped by category.
        # Each tuple: (module_path, class_name)
        _EVENT_CLASSES = [
            # Crew lifecycle
            ("crewai.events", "CrewKickoffStartedEvent"),
            ("crewai.events", "CrewKickoffCompletedEvent"),
            # Agent execution
            ("crewai.events", "AgentExecutionStartedEvent"),
            ("crewai.events", "AgentExecutionCompletedEvent"),
            # Task lifecycle
            ("crewai.events.types.task_events", "TaskStartedEvent"),
            ("crewai.events.types.task_events", "TaskCompletedEvent"),
            ("crewai.events.types.task_events", "TaskFailedEvent"),
            # Tool usage
            ("crewai.events.types.tool_usage_events", "ToolUsageStartedEvent"),
            ("crewai.events.types.tool_usage_events", "ToolUsageFinishedEvent"),
            ("crewai.events.types.tool_usage_events", "ToolUsageErrorEvent"),
            # LLM calls
            ("crewai.events", "LLMCallStartedEvent"),
            ("crewai.events", "LLMCallCompletedEvent"),
            ("crewai.events", "LLMCallFailedEvent"),
            ("crewai.events", "LLMStreamChunkEvent"),
            # Memory
            ("crewai.events", "MemorySaveStartedEvent"),
            ("crewai.events", "MemorySaveCompletedEvent"),
            ("crewai.events", "MemoryQueryStartedEvent"),
            ("crewai.events", "MemoryQueryCompletedEvent"),
            ("crewai.events", "MemoryRetrievalCompletedEvent"),
            # Knowledge
            ("crewai.events", "KnowledgeRetrievalStartedEvent"),
            ("crewai.events", "KnowledgeRetrievalCompletedEvent"),
            # Reasoning
            ("crewai.events.types.reasoning_events", "AgentReasoningStartedEvent"),
            ("crewai.events.types.reasoning_events", "AgentReasoningCompletedEvent"),
            ("crewai.events.types.reasoning_events", "AgentReasoningFailedEvent"),
            # Guardrails
            ("crewai.events.types.llm_guardrail_events", "LLMGuardrailStartedEvent"),
            ("crewai.events.types.llm_guardrail_events", "LLMGuardrailCompletedEvent"),
            ("crewai.events.types.llm_guardrail_events", "LLMGuardrailFailedEvent"),
            # Flow
            ("crewai.events.types.flow_events", "FlowStartedEvent"),
            ("crewai.events.types.flow_events", "FlowFinishedEvent"),
            ("crewai.events.types.flow_events", "FlowCreatedEvent"),
            # MCP
            ("crewai.events", "MCPConnectionStartedEvent"),
            ("crewai.events", "MCPConnectionCompletedEvent"),
            ("crewai.events", "MCPToolExecutionStartedEvent"),
            ("crewai.events", "MCPToolExecutionCompletedEvent"),
            # HITL
            ("crewai.events", "HumanFeedbackRequestedEvent"),
            ("crewai.events", "HumanFeedbackReceivedEvent"),
        ]

        import importlib

        for module_path, class_name in _EVENT_CLASSES:
            try:
                module = importlib.import_module(module_path)
                event_cls = getattr(module, class_name)
                self._register_handler(event_bus, event_cls)
                registered += 1
            except (ImportError, AttributeError):
                logger.debug(
                    f"[OTel-Bridge][{self._job_id}] {class_name} not available, skipping"
                )

        self._registered_count = registered
        logger.info(
            f"[OTel-Bridge][{self._job_id}] Registered {registered} event types on event bus"
        )
        return registered

    def _register_handler(self, event_bus: Any, event_cls: type) -> None:
        """Register a single event handler on the bus."""
        event_name = event_cls.__name__

        if event_name in _SKIP_EVENTS:
            return

        mapping = _EVENT_SPAN_MAP.get(event_name)
        if not mapping:
            logger.debug(
                f"[OTel-Bridge][{self._job_id}] No mapping for {event_name}, skipping"
            )
            return

        span_name, event_type = mapping
        bridge = self  # capture for closure

        @event_bus.on(event_cls)
        def _handler(source: Any, event: Any) -> None:
            bridge._emit_span(span_name, event_type, event)

    def _emit_span(self, span_name: str, event_type: str, event: Any) -> None:
        """Create and immediately end an OTel span for a point-in-time event."""
        logger.info(
            "[OTel-Bridge][%s] _emit_span called: %s / %s",
            self._job_id, span_name, event_type,
        )
        try:
            agent_name = _get_agent_name(event)
            task_name = _get_task_name(event)
            tool_name = _get_tool_name(event)
            output = _get_output(event)

            # Capture crew_name from crew lifecycle events so it can be
            # propagated to task/agent spans that don't carry it themselves.
            event_crew_name = getattr(event, "crew_name", None)
            if event_crew_name:
                self._current_crew_name = str(event_crew_name)

            with self._tracer.start_as_current_span(span_name) as span:
                if not span.is_recording():
                    logger.warning(
                        "[OTel-Bridge][%s] Span not recording for %s (NoOp tracer or not sampled)",
                        self._job_id, span_name,
                    )
                # Core attributes picked up by db_exporter extractors
                span.set_attribute("kasal.event_type", event_type)
                if agent_name:
                    span.set_attribute("kasal.agent_name", agent_name)
                if task_name:
                    span.set_attribute("kasal.task_name", task_name[:500])
                if tool_name:
                    span.set_attribute("kasal.tool_name", tool_name)
                if output:
                    span.set_attribute("kasal.output_content", output)

                # Extra metadata
                self._set_extra_attributes(span, event)

                # Mark failed events with error status
                if "failed" in event_type or "error" in event_type:
                    error_msg = getattr(event, "error", None) or getattr(event, "message", None)
                    span.set_status(StatusCode.ERROR, _safe_str(error_msg, 500))

        except Exception as e:
            logger.error(
                f"[OTel-Bridge][{self._job_id}] Error emitting span "
                f"{span_name}/{event_type}: {e}",
                exc_info=True,
            )

    def _set_extra_attributes(self, span: Any, event: Any) -> None:
        """Set extra kasal.extra.* attributes from CrewAI event fields.

        CrewAI BaseEvent provides: task_id, task_name, agent_role, agent_id,
        source_type. Memory events add: query, value, query_time_ms,
        save_time_ms, results, limit, score_threshold, memory_content.
        """
        # ── Task identification (BaseEvent fields) ──
        # CrewAI events set task_id/task_name via _set_task_params from from_task
        task_id = getattr(event, "task_id", None)
        if task_id:
            span.set_attribute("kasal.extra.task_id", str(task_id))
        task_name = getattr(event, "task_name", None)
        if task_name:
            span.set_attribute("kasal.extra.task_name", _safe_str(task_name, 200))

        # Also check task object directly (some events carry task reference)
        task = getattr(event, "task", None)
        if task:
            if not task_id:
                tid = getattr(task, "id", None)
                if tid:
                    span.set_attribute("kasal.extra.task_id", str(tid))
            if not task_name:
                resolved = getattr(task, "description", None) or getattr(task, "name", None)
                if resolved:
                    span.set_attribute("kasal.extra.task_name", _safe_str(resolved, 200))
            kasal_task_id = getattr(task, "_kasal_task_id", None)
            if kasal_task_id:
                span.set_attribute("kasal.extra.frontend_task_id", str(kasal_task_id))

        # ── Agent identification (BaseEvent fields) ──
        agent_role = getattr(event, "agent_role", None)
        if agent_role:
            span.set_attribute("kasal.extra.agent_role", str(agent_role))
        agent_id = getattr(event, "agent_id", None)
        if agent_id:
            span.set_attribute("kasal.extra.agent_id", str(agent_id))

        # Fallback: check event.agent object
        if not agent_role:
            agent = getattr(event, "agent", None)
            if agent:
                role = getattr(agent, "role", None)
                if role:
                    span.set_attribute("kasal.extra.agent_role", str(role))

        # ── Memory type from source_type (BaseEvent field) ──
        source_type = getattr(event, "source_type", None)
        if source_type:
            span.set_attribute("kasal.extra.source_type", str(source_type))
            # Derive memory_type from source_type for frontend display
            st = str(source_type).lower()
            if "short" in st:
                span.set_attribute("kasal.extra.memory_type", "short_term")
            elif "long" in st:
                span.set_attribute("kasal.extra.memory_type", "long_term")
            elif "entity" in st:
                span.set_attribute("kasal.extra.memory_type", "entity")
            elif "external" in st:
                span.set_attribute("kasal.extra.memory_type", "external")

        # ── Memory query/save fields ──
        query = getattr(event, "query", None)
        if query:
            span.set_attribute("kasal.extra.query", _safe_str(query, 500))
        value = getattr(event, "value", None)
        if value:
            span.set_attribute("kasal.extra.value", _safe_str(value, 500))
        query_time = getattr(event, "query_time_ms", None)
        if query_time is not None:
            span.set_attribute("kasal.extra.query_time_ms", float(query_time))
        save_time = getattr(event, "save_time_ms", None)
        if save_time is not None:
            span.set_attribute("kasal.extra.save_time_ms", float(save_time))
        retrieval_time = getattr(event, "retrieval_time_ms", None)
        if retrieval_time is not None:
            span.set_attribute("kasal.extra.retrieval_time_ms", float(retrieval_time))
        memory_content = getattr(event, "memory_content", None)
        if memory_content:
            span.set_attribute("kasal.extra.memory_content", str(memory_content))
        limit = getattr(event, "limit", None)
        if limit is not None:
            span.set_attribute("kasal.extra.limit", int(limit))
        score_threshold = getattr(event, "score_threshold", None)
        if score_threshold is not None:
            span.set_attribute("kasal.extra.score_threshold", float(score_threshold))

        # ── Memory results (query completed) ──
        results = getattr(event, "results", None)
        if results is not None:
            if isinstance(results, (list, tuple)):
                span.set_attribute("kasal.extra.results_count", len(results))

        # ── Tool fields ──
        tool_name = getattr(event, "tool_name", None) or getattr(event, "tool", None)
        if tool_name:
            span.set_attribute("kasal.extra.tool_name", str(tool_name))
        tool_args = getattr(event, "tool_args", None)
        if tool_args:
            span.set_attribute("kasal.extra.tool_args", str(tool_args))
        tool_class = getattr(event, "tool_class", None)
        if tool_class:
            cls_name = getattr(tool_class, "__name__", str(tool_class))
            span.set_attribute("kasal.extra.tool_class", cls_name)
        from_cache = getattr(event, "from_cache", None)
        if from_cache is not None:
            span.set_attribute("kasal.extra.from_cache", bool(from_cache))
        run_attempts = getattr(event, "run_attempts", None)
        if run_attempts is not None:
            span.set_attribute("kasal.extra.run_attempts", int(run_attempts))
        delegations = getattr(event, "delegations", None)
        if delegations is not None:
            span.set_attribute("kasal.extra.delegations", int(delegations))

        # ── Crew fields ──
        # Use event.crew_name if present, otherwise fall back to the name
        # captured from the last CrewKickoffStartedEvent in this bridge instance.
        crew_name = getattr(event, "crew_name", None) or self._current_crew_name
        if crew_name:
            span.set_attribute("kasal.extra.crew_name", str(crew_name))
        inputs = getattr(event, "inputs", None)
        if inputs:
            span.set_attribute("kasal.extra.inputs", str(inputs))
        total_tokens = getattr(event, "total_tokens", None)
        if total_tokens is not None:
            span.set_attribute("kasal.extra.total_tokens", int(total_tokens))

        # ── Agent execution fields ──
        task_prompt = getattr(event, "task_prompt", None)
        if task_prompt:
            span.set_attribute("kasal.extra.task_prompt", str(task_prompt))
        tools = getattr(event, "tools", None)
        if tools and isinstance(tools, (list, tuple)):
            tool_names = [getattr(t, "name", str(t)) for t in tools[:20]]
            span.set_attribute("kasal.extra.tools", _safe_str(tool_names, 500))

        # ── Task context ──
        context = getattr(event, "context", None)
        if context and isinstance(context, str):
            span.set_attribute("kasal.extra.context", str(context))

        # ── Knowledge fields ──
        retrieved_knowledge = getattr(event, "retrieved_knowledge", None)
        if retrieved_knowledge:
            span.set_attribute("kasal.extra.retrieved_knowledge", str(retrieved_knowledge))

        # ── Reasoning fields ──
        plan = getattr(event, "plan", None)
        if plan:
            span.set_attribute("kasal.extra.plan", str(plan))
        ready = getattr(event, "ready", None)
        if ready is not None:
            span.set_attribute("kasal.extra.ready", bool(ready))
        attempt = getattr(event, "attempt", None)
        if attempt is not None:
            span.set_attribute("kasal.extra.attempt", int(attempt))

        # ── Guardrail fields ──
        guardrail = getattr(event, "guardrail", None)
        if guardrail:
            span.set_attribute("kasal.extra.guardrail", _safe_str(guardrail, 500))
        success = getattr(event, "success", None)
        if success is not None:
            span.set_attribute("kasal.extra.success", bool(success))
        result = getattr(event, "result", None)
        if result is not None:
            span.set_attribute("kasal.extra.result", str(result))
        retry_count = getattr(event, "retry_count", None)
        if retry_count is not None:
            span.set_attribute("kasal.extra.retry_count", int(retry_count))

        # ── Flow fields ──
        flow_name = getattr(event, "flow_name", None)
        if flow_name:
            span.set_attribute("kasal.extra.flow_name", str(flow_name))
        method_name = getattr(event, "method_name", None)
        if method_name:
            span.set_attribute("kasal.extra.method_name", str(method_name))

        # ── MCP fields ──
        server_name = getattr(event, "server_name", None)
        if server_name:
            span.set_attribute("kasal.extra.server_name", str(server_name))
        server_url = getattr(event, "server_url", None)
        if server_url:
            span.set_attribute("kasal.extra.server_url", str(server_url))
        transport_type = getattr(event, "transport_type", None)
        if transport_type:
            span.set_attribute("kasal.extra.transport_type", str(transport_type))
        conn_duration = getattr(event, "connection_duration_ms", None)
        if conn_duration is not None:
            span.set_attribute("kasal.extra.connection_duration_ms", float(conn_duration))
        exec_duration = getattr(event, "execution_duration_ms", None)
        if exec_duration is not None:
            span.set_attribute("kasal.extra.execution_duration_ms", float(exec_duration))

        # ── HITL fields ──
        message = getattr(event, "message", None)
        if message and isinstance(message, str):
            span.set_attribute("kasal.extra.message", str(message))
        feedback = getattr(event, "feedback", None)
        if feedback:
            span.set_attribute("kasal.extra.feedback", str(feedback))
        outcome = getattr(event, "outcome", None)
        if outcome:
            span.set_attribute("kasal.extra.outcome", str(outcome))

        # ── LLM call fields ──
        model = getattr(event, "model", None)
        if model:
            span.set_attribute("kasal.extra.model", str(model))
        messages = getattr(event, "messages", None)
        if messages:
            # messages can be str or list[dict] — serialize for storage
            if isinstance(messages, list):
                # Extract last user message as prompt summary
                user_msgs = [m for m in messages if isinstance(m, dict) and m.get("role") == "user"]
                if user_msgs:
                    last_user = user_msgs[-1].get("content", "")
                    span.set_attribute("kasal.extra.prompt", str(last_user))
                span.set_attribute("kasal.extra.message_count", len(messages))
            else:
                span.set_attribute("kasal.extra.prompt", str(messages))
        call_type = getattr(event, "call_type", None)
        if call_type is not None:
            span.set_attribute("kasal.extra.call_type", str(call_type))
        # tools available to the LLM (tool definitions, not tool usage)
        available_functions = getattr(event, "available_functions", None)
        if available_functions and isinstance(available_functions, dict):
            span.set_attribute("kasal.extra.available_tools", _safe_str(list(available_functions.keys()), 500))

        # ── Error field (all failed events) ──
        error = getattr(event, "error", None)
        if error:
            span.set_attribute("kasal.extra.error", str(error))

        # ── Generic operation type ──
        operation = getattr(event, "operation", None)
        if operation:
            span.set_attribute("kasal.extra.operation", str(operation))
