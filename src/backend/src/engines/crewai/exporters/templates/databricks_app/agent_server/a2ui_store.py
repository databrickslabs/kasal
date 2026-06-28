"""Out-of-band A2UI surface delivery for the exported app.

Databricks Apps proxy a request for only so long; composing the A2UI surface
inline (an extra LLM call after the crew) would hold that connection open and risk
the surface — and the whole answer — being dropped on timeout. So a turn returns
its TEXT answer immediately and composes the surface in a BACKGROUND thread; this
tiny in-memory, per-conversation stash holds the result for the frontend to poll
(``GET /a2ui/{conversation_id}``), decoupled from the answer request.

In-memory only — the live Kasal app persists surfaces in its database instead; a
hard reconnect/redeploy loses an in-flight surface, which is acceptable for a chat
UI because the text answer is already delivered. Mirrors ``agent_server.progress``.

Status lifecycle per conversation:
  ``pending`` — composing (set at turn start) · ``ready`` — surface available ·
  ``none`` — composed, but no rich surface for this turn · ``idle`` — unknown
  (never started or pruned).
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional

_lock = threading.Lock()
# conversation_id -> {"status": str, "surface": dict|None, "ts": float}
_store: Dict[str, Dict[str, Any]] = {}
# Drop entries older than this on access so the stash can't grow unbounded.
_TTL_SECONDS = 600


def _prune(now: float) -> None:
    stale = [k for k, v in _store.items() if now - v.get("ts", now) > _TTL_SECONDS]
    for k in stale:
        _store.pop(k, None)


def begin(conversation_id: Optional[str]) -> None:
    """Mark a turn's surface as composing (clears any prior turn's surface)."""
    if not conversation_id:
        return
    now = time.time()
    with _lock:
        _prune(now)
        _store[conversation_id] = {"status": "pending", "surface": None, "ts": now}


def put(conversation_id: Optional[str], surface: Optional[Dict[str, Any]]) -> None:
    """Record the composed surface (or that there is none) for a conversation."""
    if not conversation_id:
        return
    with _lock:
        _store[conversation_id] = {
            "status": "ready" if surface else "none",
            "surface": surface,
            "ts": time.time(),
        }


def get(conversation_id: str) -> Dict[str, Any]:
    """The current {status, surface} for a conversation; 'idle' when unknown."""
    now = time.time()
    with _lock:
        _prune(now)
        item = _store.get(conversation_id)
        if not item:
            return {"status": "idle", "surface": None}
        return {"status": item["status"], "surface": item.get("surface")}
