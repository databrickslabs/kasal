"""Writing a trace row for a run that is not inside the OTel pipeline.

The pipeline covers the normal case: an engine event becomes a span, the
exporter writes the row. Two things happen OUTSIDE it and still belong to the
run's trace:

* **A2UI composition.** For crew/flow it runs in the PARENT after the subprocess
  that owns the bridge has exited, so a bus event reaches nothing. Worse, the
  engine bus is a module-global there, so the event could be picked up by
  whichever OTHER run has handlers registered — filed under a run it did not
  come from.
* Anything else post-run that has an answer to record and no span to hang it on.

This is that writer, and it lives with the rest of the trace subsystem rather
than in the engine: the engine knows WHAT happened, this knows how a row is
written, attributed and parented.
"""

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

#: Event types whose span is the run's root. Ordered: a flow owns its crews, so
#: its span is the outermost when both exist.
_ROOT_EVENT_TYPES = ("flow_started", "crew_started", "task_started")

#: Sources that are the run's scaffolding rather than an agent doing work.
_NON_AGENT_SOURCES = frozenset({"crew", "flow", "System", "A2UI"})


async def _root_span_id(session: Any, job_id: str) -> Optional[str]:
    """The run's outermost span, so a written row hangs under it rather than
    floating at the top of the trace as if it were its own run."""
    try:
        from src.repositories.execution_trace_repository import ExecutionTraceRepository

        by_type = await ExecutionTraceRepository(session).get_span_ids_by_event_type(job_id)
        for event_type in _ROOT_EVENT_TYPES:
            if by_type.get(event_type):
                return by_type[event_type]
        return rows[0][1] if rows else None
    except Exception as lookup_err:  # noqa: BLE001
        logger.debug(f"[trace-writer] root span lookup skipped for {job_id}: {lookup_err}")
        return None


async def resolve_attribution(session: Any, job_id: str) -> Dict[str, Any]:
    """Whose work a post-run row belongs to: agent role, task, task id.

    Without it a row carries its own source — "A2UI" — and the timeline groups
    it as an AGENT of its own with a task named after its outcome: the
    separate-execution look, one level down. Post-run work is not a separate
    agent; it is the last thing THIS run's agent did with its answer.
    """
    attribution: Dict[str, Any] = {}
    try:
        from src.repositories.execution_trace_repository import ExecutionTraceRepository

        rows = await ExecutionTraceRepository(session).get_attribution_candidates(job_id)
        for source, context, metadata in rows:
            if not source or source in _NON_AGENT_SOURCES:
                continue
            attribution["event_source"] = source
            if context:
                attribution["event_context"] = context
            task_id = (metadata or {}).get("task_id") if isinstance(metadata, dict) else None
            if task_id:
                attribution["task_id"] = task_id
            break
    except Exception as attribution_err:  # noqa: BLE001
        logger.debug(f"[trace-writer] attribution skipped for {job_id}: {attribution_err}")
    return attribution


async def write_rows(
    job_id: Optional[str],
    rows: list,
    *,
    fallback_source: str = "System",
    fallback_context: str = "",
    group_context: Any = None,
) -> None:
    """Write post-run rows against ``job_id``, attributed to that run.

    Each entry in ``rows`` is ``(event_type, span_name, content, metadata)``.
    They share one session, one attribution lookup and one parent span — a
    request/response pair costs the same reads as a single row.

    Never raises: the answer is already produced by the time this runs, and a
    lost trace row must not cost the user their reply.
    """
    if not job_id or not rows:
        return

    try:
        from src.db.session import get_isolated_db_session
        from src.services.trace.service import ExecutionTraceService

        async with get_isolated_db_session() as session:
            attribution = await resolve_attribution(session, job_id)
            parent_span = await _root_span_id(session, job_id)
            service = ExecutionTraceService(session)

            for event_type, span_name, content, metadata in rows:
                payload = dict(metadata or {})
                if attribution.get("task_id"):
                    # The timeline groups by task id — this is what puts the row
                    # INSIDE the task rather than in a lane of its own.
                    payload["task_id"] = attribution["task_id"]
                payload = {k: v for k, v in payload.items() if v is not None}

                trace_data: Dict[str, Any] = {
                    "job_id": job_id,
                    "event_source": attribution.get("event_source") or fallback_source,
                    "event_context": (
                        attribution.get("event_context") or fallback_context or event_type
                    ),
                    "event_type": event_type,
                    "span_name": span_name,
                    "output": {"content": content, "extra_data": payload},
                    "trace_metadata": payload,
                    "parent_span_id": parent_span,
                }
                if group_context is not None:
                    group_id = getattr(group_context, "primary_group_id", None)
                    if group_id:
                        trace_data["group_id"] = group_id
                    group_email = getattr(group_context, "group_email", None)
                    if group_email:
                        trace_data["group_email"] = group_email

                await service.create_trace(trace_data)
            await session.commit()
    except Exception as write_err:  # noqa: BLE001
        logger.debug(f"[trace-writer] rows not recorded for {job_id}: {write_err}")
