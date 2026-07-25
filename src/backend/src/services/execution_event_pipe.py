"""Live child→parent event pipe for subprocess (crew/flow) executions.

The spawned execution subprocess already receives a per-execution
``multiprocessing.Queue`` (``log_queue``) whose producer side was abandoned
when trace writing moved to direct DB writes — the parent's relay loop was
reading an always-empty queue. This module fills the channel back in for the
event classes the DB path serves too slowly: live LLM token chunks and
task/tool/LLM lifecycle traces (the DB route is child OTel batch write →
parent 1s poll → SSE, ~2s+ end to end; the pipe is instant).

Child side — :class:`EventPipeWriter`:
  registers on the engine event bus, coalesces ``LLMStreamChunkEvent`` text
  into ~50ms frames (the pipe carries frames, not tokens), projects lifecycle
  events into small ``kind:"trace"`` frames, and never blocks the emitting
  worker thread: ``put_nowait``, drop on full. Traces/logs keep their existing
  DB paths — the pipe is a live-view lane, never the source of truth.

Parent side — :func:`relay_execution_events`:
  reads frames until the child's EOF sentinel and re-broadcasts them as SSE
  events: ``llm_chunk`` with ``skip_replay=True`` — the exact wire contract
  the in-process light path uses (``{"job_id", "chunk", "seq"}``) — and
  ``trace`` frames in the same payload shape TraceBroadcastService emits from
  DB rows, so the run view updates with zero frontend changes.

Deduplication — the DB poller (``trace_broadcast_service``) would otherwise
re-broadcast the same logical lifecycle events ~2s later (piped frames carry
no DB id, so the frontend cannot collapse them). While a relay is live —
and for a short grace after it ends, covering rows the poller only sees
post-close — the poller consults :func:`suppresses_poller_broadcast` and
skips exactly the event types the pipe carries. Event types the pipe does
NOT carry (memory, knowledge, reasoning, …) keep flowing through the poller,
and the DB rows remain authoritative for refresh/history REST fetches.
"""

import asyncio
import logging
import threading
import time
from datetime import datetime, timezone
from queue import Empty
from typing import Any

logger = logging.getLogger(__name__)

_CHUNK_FLUSH_INTERVAL = 0.05  # seconds of tokens coalesced per pipe frame
_EOF_KIND = "__eof__"

# Engine event class name → the event_type STRING the frontend speaks.
# Mirrors the corresponding entries of otel_tracing.event_bridge._EVENT_SPAN_MAP
# so a piped frame and its DB row describe the same logical event identically.
# NOTE: every event class that shares an event_type with a piped class must be
# piped too (e.g. both AgentExecutionCompleted and LLMCallCompleted map to
# "llm_response") — suppression is by event_type, so a half-covered type would
# silently drop the un-piped class from the live SSE view.
_TRACE_EVENT_MAP = {
    "CrewKickoffStartedEvent": "crew_started",
    "CrewKickoffCompletedEvent": "crew_completed",
    "TaskStartedEvent": "task_started",
    "TaskCompletedEvent": "task_completed",
    "TaskFailedEvent": "task_failed",
    "AgentExecutionStartedEvent": "agent_execution",
    "AgentExecutionCompletedEvent": "llm_response",
    "ToolUsageStartedEvent": "tool_usage",
    "ToolUsageFinishedEvent": "tool_usage",
    "ToolUsageErrorEvent": "tool_error",
    "LLMCallStartedEvent": "llm_call",
    "LLMCallCompletedEvent": "llm_response",
    "LLMCallFailedEvent": "llm_call_failed",
}

# The event_type strings the pipe delivers live. trace_broadcast_service skips
# DB rows of these types for live-piped executions (see
# suppresses_poller_broadcast) so each logical event renders exactly once.
PIPED_TRACE_EVENT_TYPES = frozenset(_TRACE_EVENT_MAP.values())

# ── Live-pipe registry (parent process) ─────────────────────────────────────
# Executions whose relay is currently draining frames, plus executions whose
# relay recently finished. The grace window closes the tail race: rows the
# child committed near exit may only be SEEN by the poller after the relay
# ended — without the grace they would be re-broadcast as duplicates of
# frames the pipe already delivered. All rows are committed before the child
# exits (the OTel exporter flushes at teardown), so the grace only needs to
# outlast poll latency, not span-export latency.
_live_piped_executions: set = set()
_recently_closed_pipes: dict = {}
_CLOSED_PIPE_GRACE = 30.0  # seconds


def _mark_relay_started(execution_id: str) -> None:
    now = time.monotonic()
    for job, closed_at in list(_recently_closed_pipes.items()):
        if now - closed_at >= _CLOSED_PIPE_GRACE:
            del _recently_closed_pipes[job]
    _recently_closed_pipes.pop(execution_id, None)
    _live_piped_executions.add(execution_id)


def _mark_relay_finished(execution_id: str) -> None:
    _live_piped_executions.discard(execution_id)
    _recently_closed_pipes[execution_id] = time.monotonic()


def suppresses_poller_broadcast(execution_id: str, event_type: str) -> bool:
    """Should trace_broadcast_service SKIP broadcasting this DB row via SSE?

    True only for (a) event types the pipe carries, on (b) executions with a
    live relay or one that closed within the grace window. Everything else —
    other event types, non-subprocess runs, long-finished runs — broadcasts
    exactly as before. The poller must still ADVANCE its id cursor for
    suppressed rows; suppression is about the SSE send, not the scan.
    """
    if event_type not in PIPED_TRACE_EVENT_TYPES:
        return False
    if execution_id in _live_piped_executions:
        return True
    closed_at = _recently_closed_pipes.get(execution_id)
    if closed_at is None:
        return False
    if time.monotonic() - closed_at < _CLOSED_PIPE_GRACE:
        return True
    del _recently_closed_pipes[execution_id]
    return False

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
        from kasal_engine import events as engine_events
        from kasal_engine.events import (
            LLMCallCompletedEvent,
            LLMStreamChunkEvent,
            TaskCompletedEvent,
            TaskFailedEvent,
        )

        bus.register_handler(LLMStreamChunkEvent, self._on_chunk)
        # Boundary flushes: the tail of an answer must not wait for the next
        # token to arrive before it reaches the UI. Registered BEFORE the
        # trace handlers (the bus calls handlers in registration order) so the
        # final chunk frame precedes the completion trace frame.
        bus.register_handler(LLMCallCompletedEvent, self._on_boundary)
        bus.register_handler(TaskCompletedEvent, self._on_boundary)
        bus.register_handler(TaskFailedEvent, self._on_boundary)

        # Lifecycle events → kind:"trace" frames for the instant run view.
        for cls_name, event_type in _TRACE_EVENT_MAP.items():
            event_cls = getattr(engine_events, cls_name, None)
            if event_cls is not None:
                bus.register_handler(event_cls, self._make_trace_handler(event_type))

        global _active_writer
        _active_writer = self
        return self

    def _make_trace_handler(self, event_type: str):
        def _handler(source: Any, event: Any) -> None:
            self._on_trace_event(event_type, event)

        return _handler

    def _on_trace_event(self, event_type: str, event: Any) -> None:
        if self._closed:
            return
        try:
            frame = self._project_trace_frame(event_type, event)
        except Exception as proj_err:
            # A projection failure must never touch the emitting thread; the
            # DB trace row still records the event.
            logger.debug(f"[EventPipe] trace projection failed ({event_type}): {proj_err}")
            return
        if frame is not None:
            self._put(frame)

    def _project_trace_frame(self, event_type: str, event: Any) -> "dict | None":
        """Project an engine event into a sub-KB trace frame.

        Field extraction and truncation reuse the OTel event bridge helpers so
        the piped frame agrees with the DB row the same event produces.
        """
        from src.services.otel_tracing.event_bridge import (
            _MEMORY_WRAPPER_TOOLS,
            _get_agent_name,
            _get_output,
            _get_task_name,
            _get_tool_name,
            _safe_str,
        )

        agent_name = _get_agent_name(event)
        task_name = _get_task_name(event)
        tool_name = _get_tool_name(event)
        # The bridge absorbs memory-wrapper tool spans (the Memory* events
        # cover them); no DB row exists, so a live pill would be an orphan.
        if tool_name in _MEMORY_WRAPPER_TOOLS and event_type.startswith("tool"):
            return None
        output = _safe_str(_get_output(event), 500)

        metadata = {
            "task_name": _safe_str(task_name, 200),
            "task_id": _safe_str(getattr(event, "task_id", None), 100),
            "agent_role": _safe_str(agent_name, 200),
            "crew_name": _safe_str(getattr(event, "crew_name", None), 200),
            "tool_name": tool_name,
            "model": _safe_str(getattr(event, "model", None), 200),
            "error": _safe_str(getattr(event, "error", None), 500),
        }

        if event_type in ("crew_started", "crew_completed"):
            event_source = "crew"
        else:
            event_source = agent_name or "System"
        # Same priority as the DB exporter: task description, then tool:<name>.
        event_context = task_name or (f"tool:{tool_name}" if tool_name else event_type)

        timestamp = getattr(event, "timestamp", None)
        created_at = (
            timestamp.isoformat()
            if isinstance(timestamp, datetime)
            else datetime.now(timezone.utc).isoformat()
        )

        return {
            "kind": "trace",
            "event_type": event_type,
            "event_source": _safe_str(event_source, 200),
            "event_context": _safe_str(event_context, 500),
            "output": output or None,
            "trace_metadata": {k: v for k, v in metadata.items() if v},
            "created_at": created_at,
        }

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


async def relay_execution_events(
    queue: Any, execution_id: str, group_context: Any = None
) -> None:
    """Parent-side reader: broadcast child frames as SSE until EOF/cancel.

    Cancellation-safe: CancelledError propagates out of the executor await, so
    ``await relay_task`` after ``relay_task.cancel()`` behaves as asyncio
    expects. Callers should first give the relay a bounded grace period to
    drain to the EOF sentinel so final chunks are not dropped.

    ``group_context`` stamps group_id/group_email onto trace payloads —
    broadcast_to_job also feeds the cross-tenant ``all_groups_*`` streams,
    where clients filter by group_id, so omitting it would leak frames into
    other workspaces' stores.
    """
    from src.core.sse_manager import SSEEvent, sse_manager

    group_id = getattr(group_context, "primary_group_id", None) if group_context else None
    group_email = getattr(group_context, "group_email", None) if group_context else None

    logger.info(f"[EventPipe] relay started for {execution_id}")
    _mark_relay_started(execution_id)
    try:
        await _relay_loop(
            queue, execution_id, sse_manager, SSEEvent, group_id, group_email
        )
    finally:
        # Always hand SSE duty back to the DB poller — including on
        # cancellation — or suppression would outlive the pipe.
        _mark_relay_finished(execution_id)
    logger.info(f"[EventPipe] relay finished for {execution_id}")


async def _relay_loop(
    queue: Any,
    execution_id: str,
    sse_manager: Any,
    SSEEvent: Any,
    group_id: Any,
    group_email: Any,
) -> None:
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
            elif kind == "trace":
                # Same wire shape as TraceBroadcastService's DB-row broadcasts
                # (minus the DB id — no row exists yet). Replay stays ON,
                # unlike chunks/hitl: suppression removes these events from
                # the poller's replay-buffer writes, so the pipe frames must
                # take over reconnect catch-up or a transient SSE drop would
                # lose lifecycle events until the next full REST fetch.
                payload = {k: v for k, v in frame.items() if k != "kind"}
                payload["job_id"] = execution_id
                if group_id:
                    payload["group_id"] = group_id
                if group_email:
                    payload["group_email"] = group_email
                await sse_manager.broadcast_to_job(
                    execution_id,
                    SSEEvent(data=payload, event="trace"),
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
