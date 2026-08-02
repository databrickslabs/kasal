"""The answer a finished flow announces with its COMPLETED status.

Why the announcement carries a result at all
============================================

The subprocess announces COMPLETED as soon as the flow finishes, ahead of
teardown, so the UI hears the good news ~5s sooner. It used to announce the
STATUS alone, which made the status and the answer two separate events — and
everything downstream treats the first one as final:

- The UI finalises a job exactly once (``completeExecution`` drops duplicates),
  so it finalised on the empty announcement. One observed run persisted a
  66,857-character answer that was never displayed: the chat kept its capped
  preview and showed no composed surface, while the answer sat in the database.
- And if the subprocess dies during teardown — observed as an aiosqlite
  ``CancelledError`` in SQLAlchemy pool reset, after the announcement — the
  PARENT never reaches its own status update, which is the only write that
  carries the result. That run finished COMPLETED with an empty result and the
  answer was lost outright.

Carrying the answer makes the announcement self-contained: whatever happens
during teardown, it is already durable and already on screen. The parent still
writes afterwards and UPGRADES it with the composed A2UI surface, which only the
parent can build (composition runs after the subprocess exits).
"""

from typing import Any, Optional

#: Keys a flow's completion payload may carry its answer under, in order.
_ANSWER_KEYS = ("result", "output", "text")

#: Markers meaning "this flow stopped at a gate, it did not finish".
_PAUSE_KEYS = ("paused_for_approval", "hitl_paused")


def is_paused_for_approval(result: Any) -> bool:
    """True when this payload is a HITL pause rather than a finished run.

    A pausing flow returns ``success=True`` AND ``status="COMPLETED"`` alongside
    ``paused_for_approval`` — the flow METHOD succeeded, the RUN did not finish.
    So "completed" cannot be read from the status alone, and the pause markers
    are the only reliable signal. The parent already reads them this way
    (``flow_execution_runner``); this exists so the subprocess can too.
    """
    if not isinstance(result, dict):
        return False
    if any(result.get(key) for key in _PAUSE_KEYS):
        return True
    return result.get("status") == "WAITING_FOR_APPROVAL"


def answer_from_result(result: Any) -> Optional[Any]:
    """The answer inside a flow's completion payload, or None.

    None means "announce the status without a result" — which is the old
    behaviour, and correct when there is genuinely nothing to announce. It must
    never be a *falsy-but-present* value: passing ``""`` would write an empty
    result over whatever a retry or the parent had already stored.
    """
    if result is None:
        return None
    if is_paused_for_approval(result):
        # A pause payload is bookkeeping — approval_id, gate_node_id, a prompt
        # for the reviewer. Announcing it as the answer put raw JSON on screen
        # where the reader expected their result. Defence in depth: the caller
        # should not be announcing a paused run at all.
        return None
    if isinstance(result, dict):
        for key in _ANSWER_KEYS:
            value = result.get(key)
            if value:
                return value
        # A payload that HAS an answer key and it is empty has nothing to
        # announce. Returning the envelope instead would persist
        # `{"success": true, "result": ""}` as the run's answer — a few hundred
        # bytes of bookkeeping shown to the reader as their result, which is
        # what several observed runs stored.
        if any(key in result for key in _ANSWER_KEYS):
            return None
        # No recognised key at all: this is some other payload shape, and
        # handing it on unchanged beats dropping it. The parent's later write
        # replaces it with the composed envelope anyway.
        return result or None
    return result or None
