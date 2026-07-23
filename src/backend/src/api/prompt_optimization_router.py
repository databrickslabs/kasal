"""
API router for prompt optimization operations.

Endpoints for running GEPA-based optimization of seeded prompt
templates: start a background run, poll its status, and apply a
completed proposal as a group-scoped template override.
"""

import logging

from fastapi import APIRouter

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import BadRequestError, NotFoundError
from src.schemas.prompt_optimization import (
    CrewOptimizationRequest,
    PromptOptimizationApplyResponse,
    PromptOptimizationRequest,
    PromptOptimizationRunList,
    PromptOptimizationRunStatus,
    PromptOptimizationStartResponse,
)
from src.services.prompt_optimization_service import PromptOptimizationService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/prompt-optimization",
    tags=["prompt optimization"],
    responses={404: {"description": "Not found"}},
)


def _publish_user_context(group_context) -> None:
    # CRITICAL: publish the request's group + user token to UserContext so
    # LLMManager resolves auth the same way the dispatcher does (OBO for
    # OpenAI-protocol models). The background task inherits this context via
    # asyncio.create_task. Mirrors task_generation_router's suggest_guardrail.
    from src.utils.user_context import UserContext

    if group_context:
        UserContext.set_group_context(group_context)
        if group_context.access_token:
            UserContext.set_user_token(group_context.access_token)


@router.post("/optimize", response_model=PromptOptimizationStartResponse)
async def start_optimization(
    request: PromptOptimizationRequest,
    group_context: GroupContextDep,
    session: SessionDep,
):
    """
    Start a prompt optimization run in the background.

    Training examples come from the request (`examples`) or are mined from the
    template's logged LLM interactions. Returns a run_id to poll. The proposed
    template is NOT applied automatically — review it via the status endpoint
    and apply explicitly.
    """
    _publish_user_context(group_context)
    service = PromptOptimizationService(session)
    try:
        result = await service.start_optimization(request, group_context)
    except ValueError as e:
        raise BadRequestError(str(e))
    return PromptOptimizationStartResponse(**result)


@router.post("/optimize-crew", response_model=PromptOptimizationStartResponse)
async def start_crew_optimization(
    request: CrewOptimizationRequest,
    group_context: GroupContextDep,
    session: SessionDep,
):
    """
    Start GEPA optimization of a saved crew's prompt fields in the background.

    EXPENSIVE: every evaluation executes the crew for real (tools included) and
    judges the final deliverable — max_metric_calls bounds the number of crew
    executions. The proposal is NOT applied automatically.
    """
    _publish_user_context(group_context)
    service = PromptOptimizationService(session)
    try:
        result = await service.start_crew_optimization(request, group_context)
    except ValueError as e:
        raise BadRequestError(str(e))
    return PromptOptimizationStartResponse(**result)


@router.get("/crew-evals/{crew_id}")
async def list_crew_evals(
    crew_id: str, group_context: GroupContextDep, session: SessionDep
):
    """List a crew's optimization-evaluation answers (local MLflow traces) so
    they can be graded in-app. Empty when local MLflow mode is not enabled."""
    service = PromptOptimizationService(session)
    return {"evals": await service.list_crew_evals(crew_id)}


@router.post("/crew-evals/{trace_id}/feedback")
async def add_eval_feedback(
    trace_id: str, body: dict, group_context: GroupContextDep, session: SessionDep
):
    """Attach a human grade (0-10, Feedback) and/or an expectation (ground
    truth of what the answer SHOULD contain) to an evaluation answer.

    Stored as MLflow assessments on the trace; the next optimization run folds
    both into the judge's rubric.
    """
    service = PromptOptimizationService(session)
    value = body.get("value")
    if value is not None:
        try:
            value = float(value)
        except (TypeError, ValueError):
            raise BadRequestError("Feedback 'value' must be a number 0-10")
    try:
        ok = await service.add_eval_feedback(
            trace_id, value, body.get("comment"), body.get("expectation")
        )
    except ValueError as e:
        raise BadRequestError(str(e))
    return {"ok": ok}


@router.get("/judges")
async def list_judges(group_context: GroupContextDep, session: SessionDep):
    """List registered LLM judges (local MLflow scorer registry)."""
    service = PromptOptimizationService(session)
    return {"judges": await service.list_judges()}


@router.post("/judges")
async def create_judge(body: dict, group_context: GroupContextDep, session: SessionDep):
    """Create + register an LLM judge from Kasal: {name, instructions, model?}.

    Registered judges automatically participate in crew-optimization scoring.
    """
    _publish_user_context(group_context)
    service = PromptOptimizationService(session)
    try:
        result = await service.create_judge(
            name=str(body.get("name") or ""),
            instructions=str(body.get("instructions") or ""),
            model=body.get("model"),
            crew_id=body.get("crew_id"),
            group_context=group_context,
        )
    except ValueError as e:
        raise BadRequestError(str(e))
    return result


@router.post("/judges/{name}/assign")
async def assign_judge(
    name: str, body: dict, group_context: GroupContextDep, session: SessionDep
):
    """Assign a shared library judge to a crew: {crew_id}. The crew's runs use
    only its assigned judges."""
    service = PromptOptimizationService(session)
    crew_id = str(body.get("crew_id") or "")
    if not crew_id:
        raise BadRequestError("crew_id is required")
    try:
        return await service.assign_judge(name, crew_id, group_context)
    except ValueError as e:
        raise BadRequestError(str(e))


@router.delete("/judges/{name}")
async def delete_judge(name: str, group_context: GroupContextDep, session: SessionDep):
    """Delete a registered LLM judge by name."""
    service = PromptOptimizationService(session)
    try:
        ok = await service.delete_judge(name)
    except ValueError as e:
        raise BadRequestError(str(e))
    return {"ok": ok}


@router.get("/runs", response_model=PromptOptimizationRunList)
async def list_runs(group_context: GroupContextDep, session: SessionDep):
    """List recent optimization runs for the caller's group."""
    service = PromptOptimizationService(session)
    return PromptOptimizationRunList(
        runs=[
            PromptOptimizationRunStatus(**r) for r in service.list_runs(group_context)
        ]
    )


@router.get("/runs/{run_id}", response_model=PromptOptimizationRunStatus)
async def get_run(run_id: str, group_context: GroupContextDep, session: SessionDep):
    """Get the status/result of an optimization run, including the proposed template."""
    service = PromptOptimizationService(session)
    run = service.get_run(run_id, group_context)
    if run is None:
        raise NotFoundError(f"Optimization run '{run_id}' not found")
    return PromptOptimizationRunStatus(**run)


@router.post("/runs/{run_id}/cancel")
async def cancel_run(run_id: str, group_context: GroupContextDep, session: SessionDep):
    """Request a running optimization to stop (honored before the next crew
    execution; an in-flight execution finishes first)."""
    service = PromptOptimizationService(session)
    try:
        return service.cancel_run(run_id, group_context)
    except ValueError as e:
        raise NotFoundError(str(e))


@router.post("/runs/{run_id}/apply", response_model=PromptOptimizationApplyResponse)
async def apply_run(run_id: str, group_context: GroupContextDep, session: SessionDep):
    """
    Apply a completed run's proposed template as a group-scoped override.

    The base template row is never mutated (the seeder overwrites base rows on
    startup); reverting is a template reset in Prompt Configuration.
    """
    service = PromptOptimizationService(session)
    try:
        result = await service.apply_run(run_id, group_context)
    except ValueError as e:
        raise NotFoundError(str(e))
    return PromptOptimizationApplyResponse(**result)
