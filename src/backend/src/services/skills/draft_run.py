"""A skill draft as a run — the run record and the trace rows behind it.

The chat shows drafting as run activity, and the activity's expanded body is
the run's TRACE, read from the trace API by job id. A draft that made its LLM
calls outside any run left nothing to open. So a draft IS a run — see
:mod:`src.services.execution.generation_run`, which owns the mechanics; this
module only says what a skill draft's run looks like.
"""

from typing import Any, Dict, Optional

from src.services.execution import generation_run

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
    """The RUNNING run record's job id, or None (no session / write failed)."""
    return await generation_run.open_run(
        session,
        run_name=run_name(request, transcript_turns),
        inputs={
            "request": request,
            "mode": "capture" if transcript_turns else "blank",
            "transcript_turns": transcript_turns,
            "model": model,
        },
        trigger_type=TRIGGER_TYPE,
        group_context=group_context,
    )


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
    """One LLM call as a request row and a response row."""
    await generation_run.record_call(
        job_id,
        source=EVENT_SOURCE,
        context=EVENT_CONTEXT,
        attempt=attempt,
        model=model,
        prompt=prompt,
        response=response,
        duration_ms=duration_ms,
        group_context=group_context,
    )


async def close_run(
    job_id: Optional[str],
    *,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> None:
    """COMPLETED with the draft's summary as the result, or FAILED with the reason."""
    if error:
        await generation_run.close_run(job_id, error=error)
        return
    draft = result or {}
    await generation_run.close_run(
        job_id,
        message=(
            "Skill drafted" if draft.get("valid") else "Draft did not pass validation"
        ),
        result={
            "name": draft.get("name"),
            "description": draft.get("description"),
            "valid": draft.get("valid"),
            "errors": draft.get("errors") or [],
            "model": draft.get("model"),
            "attempts": draft.get("attempts"),
        },
    )
