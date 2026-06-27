"""Cooperative cancellation for in-flight crew turns.

A turn runs synchronously inside a worker thread (``crew.kickoff``); Python can't
force-kill a thread, so we stop *cooperatively*: the crew's step/task callbacks
check this registry between steps and raise :class:`CrewCancelled`, unwinding the
kickoff before the next LLM call — which caps token spend. A cancel is triggered
either by the user (Stop button -> ``POST /cancel/{id}``) or by the per-turn
timeout watchdog in the agent endpoints. Keyed by conversation id; in-process and
thread-safe (the request thread sets the flag, the worker thread reads it).
"""

import threading

_lock = threading.Lock()
_cancelled = set()


class CrewCancelled(Exception):
    """Raised inside a crew callback to abort a turn that was cancelled."""


def request(conversation_id) -> None:
    """Flag a conversation's running turn to stop at the next step boundary."""
    if conversation_id:
        with _lock:
            _cancelled.add(str(conversation_id))


def is_cancelled(conversation_id) -> bool:
    if not conversation_id:
        return False
    with _lock:
        return str(conversation_id) in _cancelled


def clear(conversation_id) -> None:
    """Drop any cancel flag for a conversation (turn start / completion)."""
    if conversation_id:
        with _lock:
            _cancelled.discard(str(conversation_id))
