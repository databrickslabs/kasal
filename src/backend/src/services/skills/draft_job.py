"""Start a skill draft in the background, so its trace is readable live.

The synchronous ``POST /skills/draft`` returns the run's job id only with the
finished draft, so the chat could open the drafting call's trace only after the
fact. This is the builder-generation shape instead
(``api/builder_generation_router``): open the run in the request, answer 202
with its job id, and let the draft run under ``generation_trace`` while the
client reads the trace by that id and polls the run for the result
(``draft_run.RESULT_KEY``).
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Set

from src.core.sse_manager import sse_manager
from src.services.skills import draft_run
from src.services.skills.generation import SkillGenerationService
from src.utils.user_context import GroupContext, UserContext

logger = logging.getLogger(__name__)

#: Strong references: a task only the loop holds can be garbage-collected mid-run.
_tasks: Set[asyncio.Task] = set()


async def start(
    request: str,
    group_context: GroupContext,
    session: Any,
    *,
    transcript: Optional[List[Dict[str, str]]] = None,
    model: Optional[str] = None,
) -> str:
    """Open the draft's run and start drafting; the run's job id.

    Raises ``RuntimeError`` when the run record could not be written: without
    it there is nothing for the caller to poll, so starting would lose the
    answer rather than merely its trace."""
    job_id = await draft_run.open_run(
        session,
        request=request,
        transcript_turns=len(transcript or []),
        model=model,
        group_context=group_context,
    )
    if not job_id:
        raise RuntimeError("Could not start the skill draft run")
    # Streams for this id are then visible to its own workspace only.
    sse_manager.register_job_owner(job_id, group_context.primary_group_id)
    task = asyncio.create_task(
        _draft(request, group_context, job_id, transcript=transcript, model=model)
    )
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return job_id


async def _draft(
    request: str,
    group_context: GroupContext,
    job_id: str,
    *,
    transcript: Optional[List[Dict[str, str]]],
    model: Optional[str],
) -> None:
    """The draft itself. It outlives the HTTP request, so it carries the
    caller's identity forward (the LLM credentials are resolved per group) and
    takes no request session; every write it makes routes its own."""
    UserContext.set_group_context(group_context)
    if group_context.access_token:
        UserContext.set_user_token(group_context.access_token)
    try:
        # draft() closes the run itself: COMPLETED with the draft, or FAILED.
        await SkillGenerationService.draft(
            request,
            group_context,
            transcript=transcript,
            model=model,
            job_id=job_id,
        )
    except asyncio.CancelledError:
        await draft_run.close_run(job_id, error="Draft cancelled")
        raise
    except Exception:  # noqa: BLE001 — already recorded on the run as FAILED
        logger.exception("[skills] background draft %s failed", job_id)
