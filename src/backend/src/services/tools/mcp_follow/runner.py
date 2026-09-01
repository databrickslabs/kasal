"""Vendor-neutral follow-up loop for start-tool + poll-tool MCP pairs.

``follow_tool_call`` executes the wrapped tool once; when a
:class:`FollowSpec` recognises the call and its result as an in-progress
envelope, it polls the spec's poll tool until the spec says the work finished,
the poll budget runs out, or polling keeps failing. The agent then sees ONE
finished result — never an in-progress snapshot it could misread as the
answer (returning those is how agents came to fabricate values).

Blocking model: this coroutine may run for minutes, and that is fine — every
runtime path executes tools on a worker thread (``asyncio.to_thread`` in
``runtime/agent.py`` / ``runtime/crew.py``), so the wait occupies that worker,
never an event loop.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from src.core.execution_stop import stop_requested

logger = logging.getLogger(__name__)

#: Total budget for one followed call, and the pause between polls.
FOLLOW_TIMEOUT_SECONDS = 300
FOLLOW_INTERVAL_SECONDS = 3
#: Consecutive poll failures (exceptions OR error results) tolerated before
#: giving up. Transient 429/5xx blips should not end a 5-minute wait early.
FOLLOW_MAX_CONSECUTIVE_FAILURES = 3
#: Every Nth poll is logged at INFO so a long wait stays visible without
#: writing a log line every few seconds.
_LOG_EVERY = 10
#: Budget for the best-effort server-side cancel call.
_CANCEL_TIMEOUT_SECONDS = 10


@dataclass(frozen=True)
class FollowSpec:
    """How to follow one server's start-tool + poll-tool pair.

    A spec is pure description: the loop in :func:`follow_tool_call` owns all
    timing and failure behaviour.
    """

    #: Name for logs ("genie", …).
    name: str
    #: The sibling tool to poll.
    poll_tool: str
    #: Parse a status envelope out of an MCP result, or None when the result
    #: is not one (an already-final answer, or an unknown shape).
    envelope_of: Callable[[Any], Optional[dict]]
    #: Whether an envelope represents FINISHED work.
    is_final: Callable[[dict], bool]
    #: The poll-tool parameters carried by an envelope (only the ids this
    #: poll tool actually declares — the adapter does NOT trim unknown keys).
    poll_params_of: Callable[[dict], Dict[str, Any]]
    #: Whether a result with no envelope carries a substantive payload (the
    #: finished answer) versus an empty not-ready acknowledgement.
    has_content: Callable[[Any], bool]
    #: Optional sibling tool that cancels the remote job (same id parameters
    #: as the poll tool). Fired best-effort when the loop abandons the job —
    #: user stop or timeout — so the server stops computing too.
    cancel_tool: Optional[str] = None


def _describe_failure(polled: Any) -> Optional[str]:
    """A failure reason when the poll result is an MCP error, else None."""
    if not getattr(polled, "isError", False):
        return None
    for block in getattr(polled, "content", None) or []:
        text = getattr(block, "text", None)
        if isinstance(text, str) and text.strip():
            return text.strip()[:300]
    return "tool returned an error result"


async def _cancel_remote(spec: FollowSpec, adapter: Any, poll_params: dict) -> None:
    """Best-effort server-side cancel via ``spec.cancel_tool``. Never raises —
    the job is being abandoned either way; cancelling is a courtesy to the
    remote service, not a step the agent's answer depends on."""
    if not spec.cancel_tool:
        return
    try:
        await asyncio.wait_for(
            adapter.execute_tool(spec.cancel_tool, poll_params),
            timeout=_CANCEL_TIMEOUT_SECONDS,
        )
        logger.info(f"[{spec.name}] cancelled remote job via {spec.cancel_tool}")
    except Exception as e:  # noqa: BLE001 — cancel is best-effort
        logger.warning(
            f"[{spec.name}] remote cancel via {spec.cancel_tool} failed: {e}"
        )


def _not_available(spec: FollowSpec, reason: str) -> str:
    """The directive handed to the agent when no finished result exists.
    Explicit about unavailability so the model reports it instead of
    inventing plausible numbers."""
    return (
        f"The {spec.name} query did not produce a result: {reason}. "
        "The results are NOT available. Do not fabricate or estimate values — "
        f"report that the {spec.name} query did not complete."
    )


async def follow_tool_call(wrapper, params, spec_of) -> Any:
    """Execute the wrapped MCP tool; follow it to completion when recognised.

    ``spec_of(wrapper, result)`` returns a :class:`FollowSpec` when this call's
    result is an in-progress envelope of a known start-tool + poll-tool pair,
    else None — in which case the result is returned unchanged, exactly like
    any other MCP tool.
    """
    result = await wrapper.execute(params)
    spec = spec_of(wrapper, result)
    if spec is None:
        return result
    adapter = wrapper.adapter

    envelope = spec.envelope_of(result)
    if envelope is None or spec.is_final(envelope):
        return result

    # Capture the poll ids ONCE from the opening envelope: servers keep them
    # stable for the life of the job, and an in-progress poll does not always
    # echo them back.
    poll_params = spec.poll_params_of(envelope)
    if len(poll_params) < 2:
        return result  # not enough ids to poll with — surface what we got

    status = str(envelope.get("status") or "")
    deadline = time.monotonic() + FOLLOW_TIMEOUT_SECONDS
    polls = 0
    failures = 0
    while True:
        # A user Stop ends the wait immediately (the transport round loop
        # will end the whole run at the next round boundary; this check just
        # stops the polling — and the remote job — right away).
        if stop_requested():
            logger.info(
                f"[{spec.name}] execution stopped by user after {polls} polls — "
                "abandoning follow-up"
            )
            await _cancel_remote(spec, adapter, poll_params)
            return _not_available(spec, "the execution was stopped by the user")
        if time.monotonic() >= deadline:
            logger.warning(
                f"[{spec.name}] follow-up timed out after {FOLLOW_TIMEOUT_SECONDS}s "
                f"({polls} polls, last status={status!r}) via {spec.poll_tool}"
            )
            await _cancel_remote(spec, adapter, poll_params)
            return _not_available(
                spec,
                f"still {status or 'in progress'} after {FOLLOW_TIMEOUT_SECONDS} seconds",
            )
        await asyncio.sleep(FOLLOW_INTERVAL_SECONDS)
        polls += 1
        log = logger.info if polls % _LOG_EVERY == 0 else logger.debug
        log(
            f"[{spec.name}] follow-up poll #{polls} (last status={status!r}) via {spec.poll_tool}"
        )

        # The poll itself is bounded by what is LEFT of the budget, so one
        # hung request cannot sail past the deadline on retries.
        remaining = max(1.0, deadline - time.monotonic())
        failure: Optional[str] = None
        polled = None
        try:
            polled = await asyncio.wait_for(
                adapter.execute_tool(spec.poll_tool, poll_params), timeout=remaining
            )
            failure = _describe_failure(polled)
        except Exception as e:  # noqa: BLE001 — counted and bounded below
            failure = str(e) or e.__class__.__name__

        if failure is not None:
            # Never hand back an in-progress envelope on failure — that is the
            # exact shape agents misread as an answer. Retry within the budget;
            # give up only after consecutive failures.
            failures += 1
            logger.warning(
                f"[{spec.name}] follow-up poll #{polls} failed "
                f"({failures}/{FOLLOW_MAX_CONSECUTIVE_FAILURES}): {failure}"
            )
            if failures >= FOLLOW_MAX_CONSECUTIVE_FAILURES:
                return _not_available(spec, f"polling failed repeatedly ({failure})")
            continue
        failures = 0

        next_envelope = spec.envelope_of(polled)
        if next_envelope is not None:
            result = polled
            status = str(next_envelope.get("status") or "")
            # Keep any ids the poll refreshed; the spec re-trims to its schema.
            poll_params = {**poll_params, **spec.poll_params_of(next_envelope)}
            if spec.is_final(next_envelope):
                logger.info(
                    f"[{spec.name}] follow-up finished after {polls} polls "
                    f"(status={status!r})"
                )
                return result
            if not status:
                return result  # no status and not final — cannot make progress
            continue

        # No envelope: either the finished answer payload (done — surface it)
        # or an empty not-ready acknowledgement (HTTP 202 — keep polling).
        if spec.has_content(polled):
            logger.info(
                f"[{spec.name}] follow-up finished after {polls} polls (answer payload)"
            )
            return polled
