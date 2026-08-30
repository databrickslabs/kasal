"""Memory read / write trace rows for the in-process light-agent (chat) run.

The crew and flow paths get these rows from the OTel event bridge
(``otel_tracing/event_bridge.py``), which turns the engine's memory events
into spans the DB exporter lands in ``execution_trace``. Chat runs in-process
with no bridge, so the same three events are mirrored here into the same row
shapes — the field names (``query``, ``results_count``, ``query_time_ms``,
``record_ids``, ``record_id``) match the bridge's ``kasal.extra.*`` keys
exactly, so the Jobs timeline and the memory pane render one row kind for
all three builders.

Split out of ``chat/service.py`` (over the file-size ceiling) as one cohesive
seam: everything that turns a memory event into a trace row.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

TraceFactory = Callable[[str, Dict[str, Any], str], Dict[str, Any]]
TraceSink = Callable[[Dict[str, Any]], None]
LogSink = Callable[[str], None]

# The bridge caps the recorded query at 500 chars; same here so the row reads
# identically whichever path produced it.
QUERY_CAP = 500


def cap_text(text: str, n: int = 8000) -> str:
    return text if len(text) <= n else text[:n] + "…[truncated]"


class MemoryTraceHandlers:
    """Bus / hook callbacks that persist a run's memory activity as traces.

    Scoped to THIS run's Memory instance (the bus ``source``), so concurrent
    in-process chat runs never see each other's memory events.
    """

    def __init__(
        self,
        *,
        agent_memory: Any,
        base_trace: TraceFactory,
        schedule_trace: TraceSink,
        log: LogSink,
    ) -> None:
        self._agent_memory = agent_memory
        self._base_trace = base_trace
        self._schedule_trace = schedule_trace
        self._log = log

    def _matches_memory(self, source: Any) -> bool:
        return self._agent_memory is not None and source is self._agent_memory

    def _emit(self, event_type: str, content: str, extra: Dict[str, Any]) -> None:
        out = {"tool_name": "Memory", "content": content, "extra_data": extra}
        td = self._base_trace(event_type, out, "Memory")
        td["trace_metadata"].update(extra)
        self._schedule_trace(td)

    # ── Memory Read: the recall query and what it matched ─────────────────
    def on_memory_query(self, source: Any, event: Any) -> None:
        try:
            if not self._matches_memory(source):
                return
            results = getattr(event, "results", None)
            count = len(results) if isinstance(results, (list, tuple)) else None
            qms = getattr(event, "query_time_ms", None)
            content = "" if results is None else cap_text(str(results))
            extra: Dict[str, Any] = {}
            # The query the recall ran against the store — shown on the row so
            # a reader can see WHAT was asked, not just how many rows came back.
            query = getattr(event, "query", None)
            if query:
                extra["query"] = cap_text(str(query), QUERY_CAP)
            distilled = getattr(event, "distilled_query", None)
            if distilled:
                extra["distilled_query"] = cap_text(str(distilled), QUERY_CAP)
            rounds = getattr(event, "exploration_rounds", None)
            if rounds:
                extra["exploration_rounds"] = int(rounds)
            if count is not None:
                extra["results_count"] = count
            if qms is not None:
                extra["query_time_ms"] = float(qms)
            # Structured ids of the retrieved records. The capped prose above
            # can truncate away the tail results, so anything reconstructing
            # "what this run recalled" (the memory pane) must NOT have to
            # parse content.
            rids = [
                str(rid)
                for rid in (getattr(r, "id", None) for r in (results or []))
                if rid
            ]
            if rids:
                extra["record_ids"] = rids
            self._log(f"Memory read: {count if count is not None else '?'} result(s)")
            self._emit("memory_retrieval", content, extra)
        except Exception as h_err:  # noqa: BLE001
            logger.debug(f"[light_agent] memory-query trace skipped: {h_err}")

    # ── Context Retrieved: the aggregated block handed to the agent ───────
    def on_memory_retrieval(self, source: Any, event: Any) -> None:
        try:
            if not self._matches_memory(source):
                return
            mc = getattr(event, "memory_content", None)
            content = str(mc).strip() if mc else ""
            if not content:
                content = "(no memories matched the query)"
            content = cap_text(content)
            rms = getattr(event, "retrieval_time_ms", None)
            extra: Dict[str, Any] = {}
            if rms is not None:
                extra["retrieval_time_ms"] = float(rms)
            self._emit("memory_retrieval_completed", content, extra)
        except Exception as h_err:  # noqa: BLE001
            logger.debug(f"[light_agent] memory-retrieval trace skipped: {h_err}")

    # ── Memory Write: one row per record that actually landed ─────────────
    def on_records_saved(self, records: Optional[list]) -> None:
        # Save-hook on THIS run's Memory instance (see Memory.add_save_hook) —
        # NOT a bus handler: the bus handlers are unregistered in kickoff's
        # finally, and the chat turn persist (remember_async) is submitted
        # AFTER that, so a bus handler structurally never saw chat-path writes
        # and the trace had no "Memory Write" row. The hook fires inside
        # Memory.remember whenever the write actually lands — mid-kickoff
        # (engine self-saves) or after completion — and the instance scoping
        # replaces _matches_memory.
        try:
            for record in records or []:
                content = cap_text(str(getattr(record, "content", "") or ""))
                extra: Dict[str, Any] = {}
                rid = getattr(record, "id", None)
                if rid:
                    # How the memory pane resolves "what this run wrote" exactly.
                    extra["record_id"] = str(rid)
                self._log("Memory write")
                self._emit("memory_write", content, extra)
        except Exception as h_err:  # noqa: BLE001
            logger.debug(f"[light_agent] memory-save trace skipped: {h_err}")
