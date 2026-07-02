"""Cooperative cancellation for in-flight crew turns.

A turn runs synchronously inside a worker thread (``crew.kickoff``); Python can't
force-kill a thread, so we stop *cooperatively*: the crew's step/task callbacks
check this registry between steps and raise :class:`CrewCancelled`, unwinding the
kickoff before the next LLM call — which caps token spend. A cancel is triggered
either by the user (Stop button -> ``POST /cancel/{id}``) or by the per-turn
timeout watchdog in the agent endpoints. Keyed by conversation id.

The flag lives in two layers: a process-local set (fast path — the request
thread sets it, the worker thread reads it) and the durable ``state_store`` —
so a Stop still lands when the poll/cancel request hits a different process
than the one running the turn (restart mid-flight, future multi-worker).
Worker-side reads of the durable layer are throttled to at most one per second
per conversation so step callbacks stay cheap.
"""

import threading
import time

from agent_server import state_store

_lock = threading.Lock()
_cancelled = set()
# conversation_id -> last time the durable layer was checked (throttle).
_last_check: dict = {}
_CHECK_INTERVAL_SECONDS = 1.0
_KEY = "cancel"
# A cancel flag only matters while its turn is running; ignore stale ones.
_TTL_SECONDS = 600


class CrewCancelled(Exception):
    """Raised inside a crew callback to abort a turn that was cancelled."""


def request(conversation_id) -> None:
    """Flag a conversation's running turn to stop at the next step boundary."""
    if conversation_id:
        cid = str(conversation_id)
        with _lock:
            _cancelled.add(cid)
        state_store.set_text(cid, _KEY, "1")


def is_cancelled(conversation_id) -> bool:
    if not conversation_id:
        return False
    cid = str(conversation_id)
    now = time.time()
    with _lock:
        if cid in _cancelled:
            return True
        if now - _last_check.get(cid, 0.0) < _CHECK_INTERVAL_SECONDS:
            return False
        _last_check[cid] = now
    if state_store.get_text(cid, _KEY, max_age=_TTL_SECONDS):
        with _lock:
            _cancelled.add(cid)
        return True
    return False


def clear(conversation_id) -> None:
    """Drop any cancel flag for a conversation (turn start / completion)."""
    if conversation_id:
        cid = str(conversation_id)
        with _lock:
            _cancelled.discard(cid)
            _last_check.pop(cid, None)
        state_store.delete(cid, _KEY)
