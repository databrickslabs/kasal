"""Streaming a run to an external caller, as NDJSON.

The run is walked ONCE, here, and encoded per caller: NDJSON for a client that
wants plain chunked lines, SSE for one that speaks ``text/event-stream``. Both
carry identical frames, so a caller picks a framing rather than a feature — and
a fix to what is streamed lands in both by construction.

The alternative was polling, and polling is what this exists to remove. A crew
run takes minutes; without a stream the caller either blocks blind or hammers
``get_run_status``, and neither shows what the crew is actually doing.

Protocol-neutral, like everything else in the EIL: this yields canonical
dictionaries, and each adapter decides what to put on the wire. The MCP surface
emits them as-is; the A2A surface renders each into a Task frame.
"""

import asyncio
import logging
from typing import Any, AsyncIterator, Dict, Optional

from src.services.external import artifacts as canonical_artifacts
from src.services.external import interaction
from src.services.external.identity import ExternalCaller
from src.services.external.invocation import run_status
from src.services.external.state import ExternalTaskState, is_terminal

logger = logging.getLogger(__name__)

#: How often the run is re-read. A crew emits work over minutes, so a second is
#: responsive without turning one caller into a hot loop on the database.
POLL_INTERVAL_SECONDS = 1.0

#: Hard ceiling on a single stream, independent of the run. A stream is a held
#: connection; a run that never terminates must not hold one forever. The run
#: itself is unaffected — the caller reconnects or polls.
MAX_STREAM_SECONDS = 3600.0


async def stream_run(
    caller: ExternalCaller,
    run_id: str,
    session: Any = None,
    poll_interval: float = POLL_INTERVAL_SECONDS,
    max_seconds: float = MAX_STREAM_SECONDS,
) -> AsyncIterator[Dict[str, Any]]:
    """Yield a frame per state change until the run is terminal.

    Frames are emitted on CHANGE, not on every poll: a caller reading a line per
    second for a ten-minute run learns nothing from the 599 identical ones, and
    the noise buries the transitions that matter.

    A run that pauses for a human yields ``input_required`` with the question
    inline, so a streaming caller can answer it without switching to a different
    call.

    Never raises into the stream. Once a chunked response has begun the status
    code is already sent, so an exception mid-body is an unexplained truncation
    to the caller — the last frame carries the error instead.
    """
    elapsed = 0.0
    last_signature: Optional[tuple] = None

    while elapsed < max_seconds:
        try:
            result = await run_status(caller, run_id, session=session)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[external] stream for %s ended on error: %s", run_id, exc)
            yield {
                "run_id": run_id,
                "state": ExternalTaskState.FAILED.value,
                "error": str(exc),
            }
            return

        if result is None:
            # Not visible to this caller: no such run, or another tenant's. One
            # frame, then stop — the same indistinguishable answer the
            # non-streaming reads give.
            yield {
                "run_id": run_id,
                "state": ExternalTaskState.FAILED.value,
                "error": "No such run",
            }
            return

        frame: Dict[str, Any] = {"run_id": run_id, "state": result.state.value}

        pending = await interaction.pending_for_run(caller, run_id, session=session)
        if pending:
            frame["state"] = ExternalTaskState.INPUT_REQUIRED.value
            frame["waiting_for"] = [p.as_dict() for p in pending]

        terminal = is_terminal(result.state)
        if terminal and result.output is not None:
            frame["output"] = result.output
            frame["artifact"] = canonical_artifacts.build(result.output).as_dict()
        if terminal and result.error:
            frame["error"] = result.error

        signature = (frame["state"], frame.get("waiting_for") and True)
        if signature != last_signature:
            last_signature = signature
            yield frame

        if terminal:
            return

        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    # Ceiling reached. Say so explicitly rather than closing the body silently,
    # which a caller cannot distinguish from a dropped connection.
    yield {
        "run_id": run_id,
        "state": ExternalTaskState.WORKING.value,
        "detail": (
            f"Stream ended after {int(max_seconds)}s; the run is still going. "
            "Reconnect or poll for its result."
        ),
    }


async def to_ndjson(frames: AsyncIterator[Dict[str, Any]]) -> AsyncIterator[bytes]:
    """Canonical frames -> NDJSON bytes, one object per line.

    A trailing newline per line is what makes the stream readable
    incrementally: a client can split on b"\\n" and parse each complete line
    without waiting for the body to end.
    """
    import json

    async for frame in frames:
        yield (json.dumps(frame, default=str) + "\n").encode("utf-8")


async def to_sse(
    frames: AsyncIterator[Dict[str, Any]], event_name: Optional[str] = None
) -> AsyncIterator[bytes]:
    """Canonical frames -> Server-Sent Events.

    The same frames NDJSON carries, in SSE framing: ``event:`` when the caller
    wants a named event, then ``data:`` and the blank line that terminates the
    event. A2A clients and browser EventSource both expect this shape.

    Multi-line JSON would break the framing — a bare newline inside ``data:``
    ends the event — so frames are serialised compactly, on one line.
    """
    import json

    async for frame in frames:
        payload = json.dumps(frame, default=str, separators=(",", ":"))
        prefix = f"event: {event_name}\n" if event_name else ""
        yield f"{prefix}data: {payload}\n\n".encode("utf-8")
