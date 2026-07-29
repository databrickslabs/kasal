"""Surface a subtle, live "currently doing X" hint by tapping the event bus.

Translates a few high-signal events (crew start, task start, agent thinking, tool
use) into short human strings and writes them to ``agent_server.progress`` — the
ephemeral channel the UI polls. Nothing is persisted. ``install()`` is idempotent.

The bus is Kasal's, vendored into this app, so these events are exactly the ones
Kasal's own runtime emits. The old ``try/except ImportError`` guard around the
import is GONE on purpose: it existed because CrewAI moved its event API between
releases, and its effect was that a rename silently turned the progress feed off
rather than failing. A vendored bus cannot go missing, so a failure here is a
real bug and should look like one.
"""

from __future__ import annotations

from agent_server import progress
from agent_server.kasal_runtime.core.events import (
    AgentExecutionStartedEvent,
    BaseEventListener,
    CrewKickoffStartedEvent,
    LiteAgentExecutionStartedEvent,
    TaskStartedEvent,
    ToolUsageStartedEvent,
)

_listener = None


def _short(text: object, limit: int = 60) -> str:
    s = " ".join(str(text or "").split())
    return s if len(s) <= limit else s[: limit - 1] + "…"


class _ProgressListener(BaseEventListener):
    def setup_listeners(self, bus):  # noqa: ANN001 — bus is event_bus
        @bus.on(CrewKickoffStartedEvent)
        def _on_crew(source, event):  # noqa: ANN001, ARG001
            progress.report("Starting…")

        @bus.on(TaskStartedEvent)
        def _on_task(source, event):  # noqa: ANN001, ARG001
            name = (event.task_name or "").strip() or " ".join(
                str(
                    getattr(getattr(event, "task", None), "description", "") or ""
                ).split()
            )
            # Defensive: exported apps no longer enable planning, but if a
            # hand-edited copy turns it back on, the planner emits a task whose
            # name/description is its internal prompt — label it rather than
            # dumping the prompt into the progress feed.
            if name[:19].lower().startswith("based on these task"):
                progress.report("Planning the work…")
                return
            progress.report(f"Working on: {_short(name)}")

        @bus.on(AgentExecutionStartedEvent)
        def _on_agent(source, event):  # noqa: ANN001, ARG001
            role = _short(getattr(event, "agent_role", "") or "", 40)
            if role:
                progress.report(f"{role} is thinking…")

        @bus.on(LiteAgentExecutionStartedEvent)
        def _on_lite(source, event):  # noqa: ANN001, ARG001
            # The conversation layer's gather/classify steps use standalone
            # agents (LiteAgent) — surface a generic "thinking" hint for them.
            progress.report("Thinking…")

        @bus.on(ToolUsageStartedEvent)
        def _on_tool(source, event):  # noqa: ANN001, ARG001
            progress.report(
                f"Using tool: {_short(getattr(event, 'tool_name', '') or 'tool', 40)}"
            )


def install():
    """Register the listener once. Safe to call repeatedly."""
    global _listener
    if _listener is None:
        _listener = _ProgressListener()
    return _listener
