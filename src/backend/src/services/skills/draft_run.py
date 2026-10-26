"""A skill draft as a run — the run record behind its trace.

The chat shows drafting as run activity, and the activity's expanded body is
the run's TRACE, read from the trace API by job id. A draft that made its LLM
calls outside any run left nothing to open. So a draft IS a run — see
:mod:`src.services.execution.generation_run`, which owns the record's
mechanics. The trace rows themselves are NOT written here: the draft's LLM
calls reach the event bus like every other call, and the scoped
``OTelEventBridge`` (``otel_tracing.generation_scope``) files them under this
run. This module only says what a skill draft's run looks like.
"""

from typing import Any, Dict, Optional

from src.services.execution import generation_run

#: Recorded on the run so the Jobs page can tell a draft from a chat turn.
TRIGGER_TYPE = "skill_draft"
#: The key the full draft is stored under in the run's result — what a caller
#: that started the draft in the background (:mod:`draft_job`) polls for.
RESULT_KEY = "skill_draft"
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


async def close_run(
    job_id: Optional[str],
    *,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> None:
    """COMPLETED with the draft as the result, or FAILED with the reason.

    The summary fields make the Jobs page self-describing; ``skill_draft`` is
    the whole draft, which is the answer for a caller that polls the run."""
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
            RESULT_KEY: draft,
        },
    )
