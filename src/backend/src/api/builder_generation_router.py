"""Start builder turns before their first model call so traces are visible live."""

import asyncio
import logging

from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder

from src.core.exceptions import ForbiddenError
from src.core.permissions import check_role_in_context
from src.core.sse_manager import sse_manager
from src.dependencies.providers import GroupContextDep, SessionDep
from src.schemas.crew import CrewStreamingResponse
from src.schemas.dispatcher import DispatcherRequest
from src.schemas.flow_generation import FlowGenerationRequest
from src.services.execution import generation_run
from src.services.generation.builder import BuilderGenerationService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/builder-generations", tags=["builder-generations"])
_tasks: set[asyncio.Task] = set()


async def _generate(mode, request, group_context, job_id):
    # This background entry point owns a fresh routed session. Never pass the
    # request session to a worker that outlives the HTTP response.
    from src.db.session import routed_scoped_session
    from src.utils.user_context import UserContext

    UserContext.set_group_context(group_context)
    if group_context.access_token:
        UserContext.set_user_token(group_context.access_token)
    try:
        async with routed_scoped_session() as session:
            result = await BuilderGenerationService(session).generate(
                mode, request, group_context, job_id
            )
        await generation_run.close_run(
            job_id, result={"builder_result": jsonable_encoder(result)}
        )
    except asyncio.CancelledError:
        await generation_run.close_run(job_id, error="Generation cancelled")
        raise
    except Exception as exc:
        logger.exception("Builder generation %s failed", job_id)
        await generation_run.close_run(job_id, error=str(exc))


async def _start(mode, request, group_context, session):
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can design crews and flows")
    job_id = await BuilderGenerationService(session).open(mode, request, group_context)
    sse_manager.register_job_owner(job_id, group_context.primary_group_id)
    task = asyncio.create_task(_generate(mode, request, group_context, job_id))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return CrewStreamingResponse(generation_id=job_id)


@router.post("/crew", response_model=CrewStreamingResponse, status_code=202)
async def start_crew(
    request: DispatcherRequest, group_context: GroupContextDep, session: SessionDep
):
    # A builder turn designs a crew; it must never auto-run a Chat answer.
    return await _start(
        "crew",
        request.model_copy(update={"auto_execute": False}),
        group_context,
        session,
    )


@router.post("/flow", response_model=CrewStreamingResponse, status_code=202)
async def start_flow(
    request: FlowGenerationRequest, group_context: GroupContextDep, session: SessionDep
):
    return await _start("flow", request, group_context, session)
