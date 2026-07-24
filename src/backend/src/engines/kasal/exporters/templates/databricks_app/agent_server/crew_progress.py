"""Surface a subtle, live "currently doing X" hint by tapping CrewAI's event bus.

Translates a few high-signal events (crew start, task start, agent thinking, tool
use) into short human strings and writes them to ``agent_server.progress`` — the
ephemeral channel the UI polls. Nothing is persisted. ``install()`` is idempotent
and degrades to a no-op on CrewAI builds without this event API.
"""

from __future__ import annotations

from agent_server import progress

try:
    from crewai.events import (
        crewai_event_bus,
        CrewKickoffStartedEvent,
        TaskStartedEvent,
        AgentExecutionStartedEvent,
        LiteAgentExecutionStartedEvent,
        ToolUsageStartedEvent,
    )
    from crewai.events.base_event_listener import BaseEventListener

    _AVAILABLE = True
except Exception:  # noqa: BLE001 — event API absent / renamed in this CrewAI
    _AVAILABLE = False

_listener = None


def _short(text: object, limit: int = 60) -> str:
    s = " ".join(str(text or "").split())
    return s if len(s) <= limit else s[: limit - 1] + "…"


if _AVAILABLE:

    class _ProgressListener(BaseEventListener):
        def setup_listeners(self, bus):  # noqa: ANN001 — bus is crewai_event_bus
            @bus.on(CrewKickoffStartedEvent)
            def _on_crew(source, event):  # noqa: ANN001, ARG001
                progress.report("Starting…")

            @bus.on(TaskStartedEvent)
            def _on_task(source, event):  # noqa: ANN001, ARG001
                name = (event.task_name or "").strip() or " ".join(
                    str(getattr(getattr(event, "task", None), "description", "") or "").split()
                )
                # The planner (PLANNING=True) emits a task whose name/description is
                # its internal prompt — label it rather than dumping it.
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
    """Register the listener once. Safe to call repeatedly; no-op if unavailable."""
    global _listener
    if _AVAILABLE and _listener is None:
        _listener = _ProgressListener()
    return _listener
