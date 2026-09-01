"""Cooperative stop signal for IN-PROCESS executions.

Subprocess runs (agent_builder / flow_builder) need none of this: Stop
SIGTERMs the subprocess and everything in it dies. In-process work — chat
light agents, thread-executor crews — cannot be killed that way: a worker
thread ignores ``future.cancel()``, so for years Stop was only consulted
AFTER ``kickoff()`` returned (thread_executor's own TODO said so). This
module is the signal those runs consult mid-flight.

Two views of one ``threading.Event``:

- a REGISTRY keyed by execution id, so the stop endpoint can set the event
  without holding a reference to the run;
- a CONTEXTVAR bound by the code that starts the run, so deep call sites
  (the transport round loop, the MCP follow-poll loop) can ask "was MY run
  stopped?" without threading an execution id through every layer.
  contextvars flow into ``asyncio.to_thread`` and into coroutines, which is
  exactly the path every runtime LLM/tool call takes; a thread entered via
  plain ``run_in_executor`` binds explicitly at its top instead
  (``thread_executor.crew_wrapper``).

Lives in ``core`` because both the transport loop (``core/llm``) and
capability packages (``services/tools/mcp_follow``) consult it — and core
never imports upward.
"""

import contextvars
import threading
from typing import Dict, Optional

_events: Dict[str, threading.Event] = {}
_events_lock = threading.Lock()

_current: contextvars.ContextVar[Optional[threading.Event]] = contextvars.ContextVar(
    "kasal_execution_stop_event", default=None
)


def stop_event_for(execution_id: str) -> threading.Event:
    """Get or create the registry's stop event for an execution."""
    with _events_lock:
        event = _events.get(execution_id)
        if event is None:
            event = threading.Event()
            _events[execution_id] = event
        return event


def request_stop(execution_id: str) -> bool:
    """Set an execution's stop event. True when a run had registered one."""
    with _events_lock:
        event = _events.get(execution_id)
    if event is None:
        return False
    event.set()
    return True


def discard_stop_event(execution_id: str) -> None:
    """Drop the registry entry when the run is over (the run's own finally)."""
    with _events_lock:
        _events.pop(execution_id, None)


def bind_stop_event(event: Optional[threading.Event]) -> "contextvars.Token":
    """Bind the current context's stop event; reset with the returned token."""
    return _current.set(event)


def reset_stop_event(token: "contextvars.Token") -> None:
    _current.reset(token)


def stop_requested() -> bool:
    """Whether THIS context's execution has been asked to stop.

    False wherever no event is bound — every subprocess, and any in-process
    work that is not an execution — so callers need no guard.
    """
    event = _current.get()
    return event is not None and event.is_set()
