"""Turn runtime events into MLflow spans, so exported apps still produce traces.

``mlflow.crewai.autolog()`` used to be this app's ONLY span tracing — ``otel.py``
handles logs, not spans. It hooks CrewAI internals, so removing CrewAI removed
tracing outright: the ``@mlflow.trace`` on each turn would still record the turn,
but with nothing inside it. No crew, no task, no agent, no LLM call, no tool.

Kasal solves the same problem with ``OTelEventBridge``, which cannot be reused
here: it writes through ``KasalDBSpanExporter``, which needs a database. So this
is the standalone equivalent — same idea, MLflow spans instead of Kasal's.

**How the tree is built.** Not by guessing from timestamps. The event bus stamps
every event with real causality (``event_id``, ``parent_event_id``,
``started_event_id``) computed from a scope stack, so:

- a scope-STARTING event opens a span whose parent is the span registered for
  its ``parent_event_id`` — or, at the top, whatever ``@mlflow.trace`` span is
  active, which is what attaches the whole tree to the turn;
- a scope-CLOSING event carries ``started_event_id``, naming exactly the span it
  closes. No nearest-match heuristics, no mismatched pairs.

That gives:

    turn (@mlflow.trace)
     └─ crew.kickoff
        └─ task: research
           └─ agent: Researcher
              ├─ llm.call
              └─ tool: SerperDevTool

The bar here is *a usable trace*, not byte-identical parity with a Kasal run:
correct nesting, correct names, the inputs and outputs a reviewer needs. Span
attribute keys are NOT guaranteed to match Kasal's own.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Optional

import mlflow
from agent_server.kasal_runtime.core.events import (
    AgentExecutionCompletedEvent,
    AgentExecutionStartedEvent,
    BaseEventListener,
    CrewKickoffCompletedEvent,
    CrewKickoffStartedEvent,
    LiteAgentExecutionCompletedEvent,
    LiteAgentExecutionErrorEvent,
    LiteAgentExecutionStartedEvent,
    LLMCallCompletedEvent,
    LLMCallFailedEvent,
    LLMCallStartedEvent,
    TaskCompletedEvent,
    TaskFailedEvent,
    TaskStartedEvent,
    ToolUsageErrorEvent,
    ToolUsageFinishedEvent,
    ToolUsageStartedEvent,
)

logger = logging.getLogger(__name__)

# Long payloads (prompts, tool outputs, final answers) are truncated before they
# reach a span. A trace nobody can load is not a trace, and UC trace tables are
# billed storage.
_MAX_CHARS = 8000


def _clip(value: Any, limit: int = _MAX_CHARS) -> Any:
    """Keep a payload readable and bounded."""
    if value is None or isinstance(value, (int, float, bool)):
        return value
    text = value if isinstance(value, str) else repr(value)
    if len(text) <= limit:
        return text
    return text[:limit] + f"… [{len(text) - limit} more chars]"


def _short(value: Any, limit: int = 120) -> str:
    return " ".join(str(value or "").split())[:limit]


def _agent_role(event: Any) -> str:
    """The agent's role, from wherever this event carries it."""
    role = getattr(event, "agent_role", None)
    if not role:
        agent = getattr(event, "agent", None)
        role = getattr(agent, "role", None)
    if not role:
        info = getattr(event, "agent_info", None)
        if isinstance(info, dict):
            role = info.get("role") or info.get("name")
    return str(role or "agent")


def _task_label(event: Any) -> str:
    name = getattr(event, "task_name", None)
    if not name:
        task = getattr(event, "task", None)
        name = getattr(task, "name", None) or getattr(task, "description", None)
    return _short(name or "task")


class _SpanBridge(BaseEventListener):
    """Open a span per scope-start event; close it on the paired scope-end."""

    def __init__(self) -> None:
        # event_id -> LiveSpan. Guarded because the crew runs in a worker thread
        # (``asyncio.to_thread``) while the server thread may still be emitting.
        self._spans: Dict[str, Any] = {}
        self._lock = threading.Lock()
        super().__init__()

    # ------------------------------------------------------------- span plumbing

    def _open(
        self,
        event: Any,
        name: str,
        span_type: str,
        inputs: Any = None,
        attributes: Optional[Dict[str, str]] = None,
    ) -> None:
        with self._lock:
            parent = self._spans.get(getattr(event, "parent_event_id", None) or "")
        if parent is None:
            # Top of the runtime's tree: hang it off the turn's @mlflow.trace
            # span. Without this the crew becomes a separate root trace and the
            # turn shows up empty.
            parent = mlflow.get_current_active_span()
        try:
            span = mlflow.start_span_no_context(
                name=name,
                span_type=span_type,
                parent_span=parent,
                inputs=inputs,
                attributes=attributes or None,
            )
        except Exception as exc:  # noqa: BLE001
            # Tracing must never take the run down — a trace is diagnostics.
            logger.debug(f"could not open span {name!r}: {exc}")
            return
        with self._lock:
            self._spans[event.event_id] = span

    def _close(self, event: Any, outputs: Any = None, error: Any = None) -> None:
        start_id = getattr(event, "started_event_id", None)
        if not start_id:
            return  # a close with no matching open — the bus logged it already
        with self._lock:
            span = self._spans.pop(start_id, None)
        if span is None:
            return
        try:
            if error is not None:
                span.set_attribute("error", _clip(error, 2000))
                span.set_status("ERROR")
            span.end(outputs=_clip(outputs) if outputs is not None else None)
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"could not close span: {exc}")

    def close_dangling(self) -> int:
        """End any span left open (a crash, or a cancelled turn).

        An unclosed span makes the whole trace unusable in the UI, so this is
        called at the end of every turn rather than only on error."""
        with self._lock:
            spans, self._spans = list(self._spans.values()), {}
        for span in spans:
            try:
                span.set_attribute("error", "span was never closed")
                span.set_status("ERROR")
                span.end()
            except Exception:  # noqa: BLE001
                pass
        return len(spans)

    # ---------------------------------------------------------------- listeners

    def setup_listeners(self, bus) -> None:  # noqa: ANN001 — bus is EventsBus
        from mlflow.entities import SpanType

        @bus.on(CrewKickoffStartedEvent)
        def _crew_start(source, event):  # noqa: ANN001, ARG001
            self._open(
                event,
                f"crew.{event.crew_name or 'kickoff'}",
                SpanType.CHAIN,
                inputs=event.inputs,
            )

        @bus.on(CrewKickoffCompletedEvent)
        def _crew_end(source, event):  # noqa: ANN001, ARG001
            self._close(event, outputs=event.output)

        @bus.on(TaskStartedEvent)
        def _task_start(source, event):  # noqa: ANN001, ARG001
            self._open(
                event,
                f"task: {_task_label(event)}",
                SpanType.TASK,
                inputs=_clip(event.context) if event.context else None,
            )

        @bus.on(TaskCompletedEvent)
        def _task_end(source, event):  # noqa: ANN001, ARG001
            self._close(event, outputs=getattr(event.output, "raw", event.output))

        @bus.on(TaskFailedEvent)
        def _task_failed(source, event):  # noqa: ANN001, ARG001
            self._close(event, error=event.error)

        @bus.on(AgentExecutionStartedEvent)
        def _agent_start(source, event):  # noqa: ANN001, ARG001
            self._open(
                event,
                f"agent: {_agent_role(event)}",
                SpanType.AGENT,
                inputs=_clip(getattr(event, "task_prompt", None)),
            )

        @bus.on(AgentExecutionCompletedEvent)
        def _agent_end(source, event):  # noqa: ANN001, ARG001
            self._close(event, outputs=event.output)

        @bus.on(LiteAgentExecutionStartedEvent)
        def _lite_start(source, event):  # noqa: ANN001, ARG001
            # The conversation layer's classify/gather steps are standalone
            # agents; without these the intake half of a turn traces as nothing.
            self._open(
                event,
                f"agent: {_agent_role(event)}",
                SpanType.AGENT,
                inputs=_clip(event.messages),
            )

        @bus.on(LiteAgentExecutionCompletedEvent)
        def _lite_end(source, event):  # noqa: ANN001, ARG001
            self._close(event, outputs=event.output)

        @bus.on(LiteAgentExecutionErrorEvent)
        def _lite_error(source, event):  # noqa: ANN001, ARG001
            self._close(event, error=event.error)

        @bus.on(LLMCallStartedEvent)
        def _llm_start(source, event):  # noqa: ANN001, ARG001
            attributes = {}
            if event.model:
                attributes["model"] = str(event.model)
            if event.tools:
                attributes["tool_count"] = str(len(event.tools))
            self._open(
                event,
                f"llm: {event.model or 'call'}",
                SpanType.LLM,
                inputs=_clip(event.messages),
                attributes=attributes,
            )

        @bus.on(LLMCallCompletedEvent)
        def _llm_end(source, event):  # noqa: ANN001, ARG001
            span_id = getattr(event, "started_event_id", None)
            if span_id and event.usage:
                with self._lock:
                    span = self._spans.get(span_id)
                if span is not None:
                    try:
                        for key in (
                            "prompt_tokens",
                            "completion_tokens",
                            "total_tokens",
                        ):
                            if event.usage.get(key) is not None:
                                span.set_attribute(key, str(event.usage[key]))
                    except Exception:  # noqa: BLE001
                        pass
            self._close(event, outputs=event.response)

        @bus.on(LLMCallFailedEvent)
        def _llm_failed(source, event):  # noqa: ANN001, ARG001
            self._close(event, error=event.error)

        @bus.on(ToolUsageStartedEvent)
        def _tool_start(source, event):  # noqa: ANN001, ARG001
            self._open(
                event,
                f"tool: {event.tool_name}",
                SpanType.TOOL,
                inputs=_clip(event.tool_args),
            )

        @bus.on(ToolUsageFinishedEvent)
        def _tool_end(source, event):  # noqa: ANN001, ARG001
            self._close(event, outputs=event.output)

        @bus.on(ToolUsageErrorEvent)
        def _tool_error(source, event):  # noqa: ANN001, ARG001
            self._close(event, error=event.error)


_bridge: Optional[_SpanBridge] = None


def install() -> Optional[_SpanBridge]:
    """Register the bridge once. Safe to call repeatedly."""
    global _bridge
    if _bridge is None:
        _bridge = _SpanBridge()
    return _bridge


def end_turn() -> int:
    """Close anything left open by the turn that just finished.

    Called from a ``finally`` in the conversation layer: a cancelled turn (Stop)
    unwinds through an exception that never reaches the runtime's completion
    events, leaving spans open and the trace unrenderable."""
    return _bridge.close_dangling() if _bridge is not None else 0
