"""Ephemeral "what is the agent doing right now" channel.

NOT persisted: a tiny in-memory, per-conversation status that the frontend polls
while a turn is in flight and discards when it finishes. It exists only to give
the user a subtle, live hint (which task / which tool) — there is no history.

Fed by ``crew_progress`` (a CrewAI event-bus listener). Correlation: we stamp the
conversation id on a ``ContextVar`` for the turn. CrewAI dispatches its event
handlers via ``contextvars.copy_context()`` taken at emit time, so the id set on
the worker thread that runs the kickoff propagates into the (separate) bus handler
thread. Each turn runs in its own copied context (``asyncio.to_thread``), so
concurrent turns stay isolated; if an event ever fires without context, ``report``
simply no-ops.
"""

from __future__ import annotations

import contextvars
import threading
import time
from typing import Dict, Optional

_lock = threading.Lock()
_store: Dict[str, dict] = {}
_current: "contextvars.ContextVar[Optional[str]]" = contextvars.ContextVar(
    "kasal_progress_cid", default=None
)


def set_current(conversation_id: Optional[str]) -> None:
    """Bind the current context's activity to a conversation for this turn."""
    _current.set(conversation_id)


def clear_current() -> None:
    """Unbind (call in a finally; harmless given per-turn copied contexts)."""
    _current.set(None)


def current() -> Optional[str]:
    return _current.get()


def report(status: str) -> None:
    """Record the latest activity for the in-flight conversation (best-effort)."""
    cid = current()
    if not cid or not status:
        return
    with _lock:
        prev = _store.get(cid)
        seq = (prev["seq"] + 1) if prev else 1
        _store[cid] = {"status": status, "seq": seq, "ts": time.time()}


def get(conversation_id: str) -> Optional[dict]:
    """The current {status, seq, ts} for a conversation, or None when idle."""
    with _lock:
        item = _store.get(conversation_id)
        return dict(item) if item else None


def clear(conversation_id: Optional[str]) -> None:
    """Drop a conversation's status once its turn is fully done (ephemeral)."""
    if not conversation_id:
        return
    with _lock:
        _store.pop(conversation_id, None)
