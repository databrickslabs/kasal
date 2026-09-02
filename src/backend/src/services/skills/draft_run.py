"""A skill draft as a run — the run record and the trace rows behind it.

The chat shows drafting as run activity, and the activity's expanded body is
the run's TRACE, read from the trace API by job id (the same durable record
every run leaves; see ``useRunTimeline`` on the frontend). A draft that made
its LLM calls outside any run left nothing to open: "Run activity" with an
empty body. So a draft IS a run — a small ``executionhistory`` row opened
before the first call, one ``llm_call`` / ``llm_response`` pair per attempt,
and a terminal status with the draft as its result — and its calls show up
where every other LLM call does: the run activity, the preview pane, the
Jobs page.

Everything here is best-effort: a run record that could not be written must
not cost the user the draft, so failures are logged and the draft proceeds
without a job id.
"""

import logging
from typing import Any, Dict, List, Optional

from src.models.execution_status import ExecutionStatus
from src.services.execution.status import ExecutionStatusService
from src.services.trace.writer import write_rows

logger = logging.getLogger(__name__)

#: How the rows are attributed in the timeline (its lanes are event sources).
EVENT_SOURCE = "Skills"
EVENT_CONTEXT = "skill draft"
#: Recorded on the run so the Jobs page can tell a draft from a chat turn.
TRIGGER_TYPE = "skill_draft"
_MAX_RUN_NAME = 80


def run_name(request: str, transcript_turns: int) -> str:
    """ "Skill draft: <request>" — or the conversation, when that is the source."""
    text = " ".join((request or "").split())
    if not text:
        return f"Skill draft from conversation ({transcript_turns} turns)"
    if len(text) > _MAX_RUN_NAME:
        text = text[: _MAX_RUN_NAME - 1].rstrip() + "…"
    return f"Skill draft: {text}"


async def open_run(
    session: Any,
    *,
    request: str,
    transcript_turns: int,
    model: Optional[str],
    group_context: Any,
) -> Optional[str]:
    """Create the RUNNING run record; its job id, or None when there is no
    session to write with or the write failed (the draft goes on regardless)."""
    if session is None:
        return None
    try:
        # Lazy: the execution service is a heavy module, and the skills package
        # must stay importable without it.
        from src.services.execution.service import ExecutionService

        job_id = ExecutionService.create_execution_id()
        await ExecutionService.create_run_record(
            session,
            job_id=job_id,
            run_name=run_name(request, transcript_turns),
            inputs={
                "request": request,
                "mode": "capture" if transcript_turns else "blank",
                "transcript_turns": transcript_turns,
                "model": model,
            },
            execution_type="agent",
            group_id=getattr(group_context, "primary_group_id", None),
            group_email=getattr(group_context, "group_email", None),
            status=ExecutionStatus.RUNNING.value,
            trigger_type=TRIGGER_TYPE,
        )
        return job_id
    except Exception as exc:  # noqa: BLE001 — the draft must not depend on this
        logger.warning("[skills] draft run record not created: %s", exc)
        return None


async def record_call(
    job_id: Optional[str],
    *,
    attempt: int,
    model: Optional[str],
    prompt: str,
    response: str,
    duration_ms: float,
    group_context: Any,
) -> None:
    """One LLM call as a request row and a response row, like every other
    LLM call in a trace (one row can only report one payload)."""
    if not job_id:
        return
    shared: Dict[str, Any] = {"model": model, "attempt": attempt}
    rows: List[tuple] = [
        ("llm_call", "kasal.skills.llm_call", prompt, {**shared, "prompt": prompt}),
        (
            "llm_response",
            "kasal.skills.llm_response",
            response,
            {**shared, "duration_ms": round(duration_ms, 2)},
        ),
    ]
    await write_rows(
        job_id,
        rows,
        fallback_source=EVENT_SOURCE,
        fallback_context=EVENT_CONTEXT,
        group_context=group_context,
    )


async def close_run(
    job_id: Optional[str],
    *,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> None:
    """End the run: COMPLETED with the draft as its result, or FAILED with the
    reason. The draft itself is the API response; storing it on the run only
    makes the Jobs page self-describing."""
    if not job_id:
        return
    try:
        if error:
            await ExecutionStatusService.update_status(
                job_id, ExecutionStatus.FAILED.value, error[:500]
            )
            return
        draft = result or {}
        message = (
            "Skill drafted" if draft.get("valid") else "Draft did not pass validation"
        )
        await ExecutionStatusService.update_status(
            job_id,
            ExecutionStatus.COMPLETED.value,
            message,
            result={
                "name": draft.get("name"),
                "description": draft.get("description"),
                "valid": draft.get("valid"),
                "errors": draft.get("errors") or [],
                "model": draft.get("model"),
                "attempts": draft.get("attempts"),
            },
        )
    except Exception as exc:  # noqa: BLE001 — never fails the draft
        logger.warning("[skills] draft run %s not closed: %s", job_id, exc)
