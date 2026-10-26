"""A one-off generation call, recorded as a run.

Some product actions are a single focused LLM call rather than an agent run:
drafting a skill, revising one slide of a deck. They still have to be visible
the way every other LLM call is — in the chat's run activity (whose expanded
body is the run's TRACE, read by job id), in the preview pane, on the Jobs
page. So each becomes a small run: an ``executionhistory`` row opened before
the first call, an ``llm_call`` / ``llm_response`` pair per attempt, and a
terminal status with a summary as its result.

Everything here is best-effort: a run record that could not be written must
not cost the user the answer, so failures are logged and the caller goes on
without a job id.
"""

import logging
from typing import Any, Dict, List, Optional

from src.models.execution_status import ExecutionStatus
from src.services.execution.status import ExecutionStatusService
from src.services.trace.writer import write_rows

logger = logging.getLogger(__name__)


async def open_run(
    session: Any,
    *,
    run_name: str,
    inputs: Dict[str, Any],
    trigger_type: str,
    group_context: Any,
) -> Optional[str]:
    """Create the RUNNING run record; its job id, or None when there is no
    session to write with or the write failed (the call goes on regardless)."""
    if session is None:
        return None
    try:
        # Lazy: the execution service is a heavy module, and the callers are
        # capability packages that must stay importable without it.
        from src.services.execution.service import ExecutionService

        job_id = ExecutionService.create_execution_id()
        await ExecutionService.create_run_record(
            session,
            job_id=job_id,
            run_name=run_name,
            inputs=inputs,
            execution_type="agent",
            group_id=getattr(group_context, "primary_group_id", None),
            group_email=getattr(group_context, "group_email", None),
            status=ExecutionStatus.RUNNING.value,
            trigger_type=trigger_type,
        )
        return job_id
    except Exception as exc:  # noqa: BLE001 — the answer must not depend on this
        logger.warning("[generation-run] run record not created: %s", exc)
        return None


async def record_call(
    job_id: Optional[str],
    *,
    source: str,
    context: str,
    attempt: int,
    model: Optional[str],
    prompt: str,
    response: str,
    duration_ms: float,
    group_context: Any,
) -> None:
    """One LLM call as a request row and a response row, like every other
    LLM call in a trace (one row can only report one payload). ``source`` is
    the timeline lane ("Skills", "Decks"); ``context`` its step label."""
    if not job_id:
        return
    shared: Dict[str, Any] = {"model": model, "attempt": attempt}
    key = source.lower()
    rows: List[tuple] = [
        ("llm_call", f"kasal.{key}.llm_call", prompt, {**shared, "prompt": prompt}),
        (
            "llm_response",
            f"kasal.{key}.llm_response",
            response,
            {**shared, "duration_ms": round(duration_ms, 2)},
        ),
    ]
    await write_rows(
        job_id,
        rows,
        fallback_source=source,
        fallback_context=context,
        group_context=group_context,
    )


async def close_run(
    job_id: Optional[str],
    *,
    message: str = "Completed",
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> None:
    """End the run: COMPLETED with ``result`` as its summary, or FAILED with
    the reason. The answer itself is the API response; storing a summary on
    the run only makes the Jobs page self-describing."""
    if not job_id:
        return
    try:
        if error:
            await ExecutionStatusService.update_status(
                job_id, ExecutionStatus.FAILED.value, error[:500]
            )
            return
        await ExecutionStatusService.update_status(
            job_id, ExecutionStatus.COMPLETED.value, message, result=result or {}
        )
    except Exception as exc:  # noqa: BLE001 — never fails the answer
        logger.warning("[generation-run] run %s not closed: %s", job_id, exc)
