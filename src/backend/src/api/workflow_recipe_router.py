"""Workflow recipes — read-only reuse candidates for a generation prompt.

Phase 2 of workflow reuse. This exposes retrieval ONLY: it answers "have you
built something like this before?" and returns the matching recipes. It never
creates, runs, or modifies anything, so a wrong suggestion costs a glance.

The reuse decision itself stays with the caller and, ultimately, the human at
the canvas — a retrieved plan is never executed unreviewed.
"""

from typing import Annotated, List

from fastapi import APIRouter, Depends, Query, status

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import BadRequestError, ForbiddenError, NotFoundError
from src.core.permissions import check_role_in_context
from src.schemas.workflow_recipe import (
    RecipeCurateRequest,
    RecipeSuggestRequest,
    RecipeSummary,
)
from src.services.workflow_recipe_service import WorkflowRecipeService

router = APIRouter(
    prefix="/workflow-recipes",
    tags=["workflow-recipes"],
    responses={404: {"description": "Not found"}},
)


async def get_workflow_recipe_service(session: SessionDep) -> WorkflowRecipeService:
    return WorkflowRecipeService(session)


WorkflowRecipeServiceDep = Annotated[
    WorkflowRecipeService, Depends(get_workflow_recipe_service)
]


@router.post("/suggest", response_model=List[RecipeSummary])
async def suggest_recipes(
    request: RecipeSuggestRequest,
    service: WorkflowRecipeServiceDep,
    group_context: GroupContextDep,
) -> List[RecipeSummary]:
    """Recipes from THIS workspace similar enough to the prompt to be worth reusing.

    Empty when nothing clears the similarity floor — the honest answer when the
    library holds nothing like this request, and the reason the floor lives in
    the service rather than at each caller.

    Allowed roles: admin / editor / operator — anyone who can start a run can be
    told one already exists.
    """
    if not check_role_in_context(group_context, ["admin", "editor", "operator"]):
        raise ForbiddenError("Not permitted to read workflow recipes")

    group_ids = group_context.group_ids if group_context else []
    return await service.suggest_for_prompt(
        request.prompt,
        group_ids,
        limit=request.limit,
        min_similarity=request.min_similarity,
    )


@router.patch("/{recipe_id}/curation", response_model=RecipeSummary)
async def curate_recipe(
    recipe_id: int,
    request: RecipeCurateRequest,
    service: WorkflowRecipeServiceDep,
    group_context: GroupContextDep,
) -> RecipeSummary:
    """Mark a recipe good / bad / hidden, or clear the mark with null.

    The only trustworthy quality signal in the system: everything mined is just
    "a crew that finished", which says nothing about whether its output was
    right. 'bad' and 'hidden' take effect immediately — the retrieval query
    filters them, so a rejected recipe cannot be suggested again.

    Allowed roles: admin / editor — curating changes what the workspace is
    offered, so it is not an operator-level action.
    """
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Not permitted to curate workflow recipes")

    group_ids = group_context.group_ids if group_context else []
    try:
        recipe = await service.curate(
            recipe_id,
            request.curation,
            group_ids,
            curated_by=group_context.group_email if group_context else None,
        )
    except ValueError as invalid:
        raise BadRequestError(str(invalid))

    if recipe is None:
        raise NotFoundError(f"Workflow recipe {recipe_id} not found")
    return service.to_summary(recipe)


@router.post("/{recipe_id}/reuse", response_model=RecipeSummary)
async def record_reuse(
    recipe_id: int,
    service: WorkflowRecipeServiceDep,
    group_context: GroupContextDep,
) -> RecipeSummary:
    """Record that a suggestion was actually taken up.

    The accept rate is the cheapest honest signal available — a human choosing a
    stored crew over generating a fresh one, captured without asking anyone to
    rate anything. It also tells you whether the similarity floor is sane:
    suggestions that are never accepted mean it is set too low.
    """
    if not check_role_in_context(group_context, ["admin", "editor", "operator"]):
        raise ForbiddenError("Not permitted to use workflow recipes")

    group_ids = group_context.group_ids if group_context else []
    recipe = await service.record_reuse(recipe_id, group_ids)
    if recipe is None:
        raise NotFoundError(f"Workflow recipe {recipe_id} not found")
    return service.to_summary(recipe)


@router.get("", response_model=List[RecipeSummary], status_code=status.HTTP_200_OK)
async def list_recipes(
    service: WorkflowRecipeServiceDep,
    group_context: GroupContextDep,
    limit: int = Query(50, ge=1, le=200),
) -> List[RecipeSummary]:
    """The workspace's recipe library, most recently refreshed first."""
    if not check_role_in_context(group_context, ["admin", "editor", "operator"]):
        raise ForbiddenError("Not permitted to read workflow recipes")

    group_ids = group_context.group_ids if group_context else []
    return await service.list_for_group(group_ids, limit=limit)
