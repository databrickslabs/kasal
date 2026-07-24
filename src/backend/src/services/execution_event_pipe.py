"""Live child→parent event pipe for subprocess (crew/flow) executions.

The spawned execution subprocess already receives a per-execution
``multiprocessing.Queue`` (``log_queue``) whose producer side was abandoned
when trace writing moved to direct DB writes — the parent's relay loop was
reading an always-empty queue. This module fills the channel back in for the
one event class that cannot ride the DB path: live LLM token chunks.

Child side — :class:`EventPipeWriter`:
  registers on the engine event bus, coalesces ``LLMStreamChunkEvent`` text
  into ~50ms frames (the pipe carries frames, not tokens), and never blocks
  the emitting LLM worker thread: ``put_nowait``, drop on full. Traces/logs
  keep their existing DB paths — the pipe is a live-view lane, never the
  source of truth.

Parent side — :func:`relay_execution_events`:
  reads frames until the child's EOF sentinel and re-broadcasts them as SSE
  ``llm_chunk`` events with ``skip_replay=True`` — the exact wire contract the
  in-process light path uses (``{"job_id", "chunk", "seq"}``), so the chat UI
  streams subprocess runs with zero frontend changes.

Unknown frame kinds are ignored by the relay (forward compatibility: task
lifecycle frames can be added later once the trace path has deduplication).
"""

import asyncio
import logging
import threading
import time
from datetime import datetime
from queue import Empty
from typing import Any

logger = logging.getLogger(__name__)

_CHUNK_FLUSH_INTERVAL = 0.05  # seconds of tokens coalesced per pipe frame
_EOF_KIND = "__eof__"

# One execution per subprocess, so one writer per process. Lets teardown code
# close the pipe without threading the writer through deeply nested scopes.
_active_writer: "EventPipeWriter | None" = None


class EventPipeWriter:
    """Child-side bus subscriber that projects events into small queue frames."""

    def __init__(self, queue: Any, execution_id: str) -> None:
        self._queue = queue
        self._execution_id = execution_id
        self._lock = threading.Lock()
        self._chunk_buf: list[str] = []
        self._chunk_seq = 0
        self._last_flush = 0.0
        self._closed = False

    def register(self, bus: Any) -> "EventPipeWriter":
        from kasal_engine.events import (
            LLMCallCompletedEvent,
            LLMStreamChunkEvent,
            TaskCompletedEvent,
            TaskFailedEvent,
        )

        bus.register_handler(LLMStreamChunkEvent, self._on_chunk)
        # Boundary flushes: the tail of an answer must not wait for the next
        # token to arrive before it reaches the UI.
        bus.register_handler(LLMCallCompletedEvent, self._on_boundary)
        bus.register_handler(TaskCompletedEvent, self._on_boundary)
        bus.register_handler(TaskFailedEvent, self._on_boundary)

        global _active_writer
        _active_writer = self
        return self

    def _put(self, frame: dict) -> None:
        # Never block or raise on the emitting thread. A full/broken pipe just
        # drops live frames — the DB trace/result paths stay authoritative.
        try:
            self._queue.put_nowait(frame)
        except Exception:
            pass

    def _on_chunk(self, source: Any, event: Any) -> None:
        text = getattr(event, "chunk", "") or ""
        if not text or self._closed:
            return
        frame = None
        with self._lock:
            self._chunk_buf.append(text)
            now = time.monotonic()
            if now - self._last_flush >= _CHUNK_FLUSH_INTERVAL:
                frame = self._drain_locked(now)
        if frame:
            self._put(frame)

    def _drain_locked(self, now: float) -> dict | None:
        if not self._chunk_buf:
            return None
        text = "".join(self._chunk_buf)
        self._chunk_buf.clear()
        self._last_flush = now
        frame = {"kind": "chunk", "chunk": text, "seq": self._chunk_seq}
        self._chunk_seq += 1
        return frame

    def flush_chunks(self) -> None:
        with self._lock:
            frame = self._drain_locked(time.monotonic())
        if frame:
            self._put(frame)

    def _on_boundary(self, source: Any, event: Any) -> None:
        self.flush_chunks()

    def close(self) -> None:
        """Flush remaining chunks and send the EOF sentinel (idempotent)."""
        if self._closed:
            return
        self._closed = True
        self.flush_chunks()
        self._put({"kind": _EOF_KIND})


def close_active_pipe_writer() -> None:
    """Teardown hook for the subprocess: close whichever writer registered."""
    global _active_writer
    writer, _active_writer = _active_writer, None
    if writer is not None:
        try:
            writer.close()
        except Exception:
            pass


def put_parent_eof(queue: Any) -> None:
    """Parent-side EOF: called after the child exited (join returned).

    Everything the child wrote is already in the queue, so appending an EOF
    here lets the relay drain those frames and stop deterministically — even
    when the child died before its finally could send its own sentinel. A
    duplicate sentinel is harmless (the relay stops at the first one).
    """
    try:
        queue.put_nowait({"kind": _EOF_KIND})
    except Exception:
        pass


async def relay_execution_events(queue: Any, execution_id: str) -> None:
    """Parent-side reader: broadcast child frames as SSE until EOF/cancel.

    Cancellation-safe: CancelledError propagates out of the executor await, so
    ``await relay_task`` after ``relay_task.cancel()`` behaves as asyncio
    expects. Callers should first give the relay a bounded grace period to
    drain to the EOF sentinel so final chunks are not dropped.
    """
    from src.core.sse_manager import SSEEvent, sse_manager

    logger.info(f"[EventPipe] relay started for {execution_id}")
    loop = asyncio.get_running_loop()
    invalid_streak = 0
    while True:
        try:
            frame = await loop.run_in_executor(
                None, lambda: queue.get(block=True, timeout=0.5)
            )
        except Empty:
            invalid_streak = 0
            continue
        except asyncio.CancelledError:
            raise
        except Exception as get_err:
            # Closed/broken pipe, or a queue object that doesn't speak the
            # mp.Queue protocol (e.g. a test double): the relay must never
            # poison the execution result — stop streaming and let the DB
            # paths carry the run.
            logger.debug(f"[EventPipe] {execution_id}: relay stopping ({get_err})")
            break
        if not isinstance(frame, dict):
            # A real queue only yields what a writer put. A stream of non-dict
            # frames means the queue object itself is broken (or a test mock)
            # and get() is not actually blocking — bail out rather than busy-spin.
            invalid_streak += 1
            if invalid_streak >= 50:
                logger.warning(
                    f"[EventPipe] {execution_id}: queue yields non-frame objects; stopping relay"
                )
                break
            continue
        invalid_streak = 0
        kind = frame.get("kind")
        if kind == _EOF_KIND:
            break
        try:
            if kind == "chunk":
                await sse_manager.broadcast_to_job(
                    execution_id,
                    SSEEvent(
                        data={
                            "job_id": execution_id,
                            "chunk": frame.get("chunk", ""),
                            "seq": frame.get("seq"),
                        },
                        event="llm_chunk",
                        id=f"{execution_id}_chunk_{datetime.now().timestamp()}",
                    ),
                    skip_replay=True,
                )
            elif kind == "hitl_request":
                # A tool-approval gate in the subprocess needs the human NOW.
                # skip_replay: replaying old gate events after a reconnect pops
                # stale/expired dialogs; clients that missed the live event
                # discover pending gates via the approvals API instead.
                payload = {k: v for k, v in frame.items() if k != "kind"}
                payload.setdefault("job_id", execution_id)
                await sse_manager.broadcast_to_job(
                    execution_id,
                    SSEEvent(data=payload, event="hitl_request"),
                    skip_replay=True,
                )
            else:
                continue  # forward compatibility: ignore unknown frame kinds
        except Exception as sse_err:
            logger.warning(f"[EventPipe] {kind} broadcast failed for {execution_id}: {sse_err}")
    logger.info(f"[EventPipe] relay finished for {execution_id}")
