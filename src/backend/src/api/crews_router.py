import json
import logging
from typing import Annotated, Any, Dict, List
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, status
from pydantic import ValidationError

from src.core.exceptions import ForbiddenError, NotFoundError, UnprocessableEntityError

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.permissions import check_role_in_context
from src.schemas.crew import CrewCreate, CrewResponse, CrewUpdate
from src.schemas.crew_feedback import (
    CrewFeedbackCreateRequest,
    CrewFeedbackResponse,
    CrewFeedbackSummaryEntry,
)
from src.services.crew_feedback_service import CrewFeedbackService
from src.services.crew_service import CrewService

router = APIRouter(
    prefix="/crews",
    tags=["crews"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)


# Dependency to get CrewService
def get_crew_service(session: SessionDep) -> CrewService:
    return CrewService(session)


def _crew_to_response(crew) -> CrewResponse:
    """Serialize a Crew model to CrewResponse including ALL execution-config fields.

    Building CrewResponse field-by-field previously listed only id/name/ids/nodes/
    edges/timestamps, silently dropping process/planning/reasoning/llms (and now
    reasoning_config) on every GET/POST/PUT — so a crew saved to the catalog
    reloaded with empty/default execution config. Centralized here so the response
    can never diverge from the model again.
    """
    return CrewResponse(
        id=crew.id,
        name=crew.name,
        agent_ids=crew.agent_ids,
        task_ids=crew.task_ids,
        nodes=crew.nodes or [],
        edges=crew.edges or [],
        created_at=crew.created_at.isoformat(),
        updated_at=crew.updated_at.isoformat(),
        # getattr defaults: real Crew models always have these columns; defensive
        # against partial stubs (and forward-compatible if a column is missing).
        process=getattr(crew, "process", None) or "sequential",
        planning=getattr(crew, "planning", False),
        planning_llm=getattr(crew, "planning_llm", None),
        reasoning=getattr(crew, "reasoning", False),
        reasoning_llm=getattr(crew, "reasoning_llm", None),
        reasoning_config=getattr(crew, "reasoning_config", None),
        manager_llm=getattr(crew, "manager_llm", None),
        tool_configs=getattr(crew, "tool_configs", None),
        memory=getattr(crew, "memory", True),
        verbose=getattr(crew, "verbose", True),
        max_rpm=getattr(crew, "max_rpm", None),
    )


@router.get("", response_model=List[CrewResponse])
async def list_crews(
    service: Annotated[CrewService, Depends(get_crew_service)],
    group_context: GroupContextDep,
):
    """
    Retrieve all crews for the current group.

    Args:
        service: Crew service injected by dependency
        group_context: Group context from headers

    Returns:
        List of crews for current group
    """
    crews = await service.find_by_group(group_context)
    return [_crew_to_response(crew) for crew in crews]


def get_crew_feedback_service(session: SessionDep) -> CrewFeedbackService:
    return CrewFeedbackService(session)


@router.get("/feedback-summary", response_model=List[CrewFeedbackSummaryEntry])
async def crew_feedback_summary(
    group_context: GroupContextDep,
    service: Annotated[CrewFeedbackService, Depends(get_crew_feedback_service)],
):
    """Per-crew thumbs up/down counts for this workspace's catalog view.

    NOTE: registered BEFORE /{crew_id} — a literal segment would otherwise be
    parsed (and rejected) as a UUID path param.
    """
    return [CrewFeedbackSummaryEntry(**row) for row in await service.summary(group_context)]


@router.post("/{crew_id}/feedback", response_model=CrewFeedbackResponse, status_code=status.HTTP_201_CREATED)
async def add_crew_feedback(
    crew_id: Annotated[UUID, Path(title="The ID of the crew")],
    request: CrewFeedbackCreateRequest,
    group_context: GroupContextDep,
    service: Annotated[CrewFeedbackService, Depends(get_crew_feedback_service)],
):
    """Record a thumbs up/down on a cataloged crew (down requires a comment)."""
    try:
        record = await service.add_feedback(
            crew_id=str(crew_id),
            rating=request.rating,
            comment=request.comment,
            group_context=group_context,
        )
    except ValueError as e:
        raise UnprocessableEntityError(str(e))
    return CrewFeedbackResponse.model_validate(record)


@router.get("/{crew_id}/feedback", response_model=List[CrewFeedbackResponse])
async def list_crew_feedback(
    crew_id: Annotated[UUID, Path(title="The ID of the crew")],
    group_context: GroupContextDep,
    service: Annotated[CrewFeedbackService, Depends(get_crew_feedback_service)],
):
    """All feedback entries for a crew (newest first) — incl. down-vote comments."""
    records = await service.list_for_crew(str(crew_id), group_context)
    return [CrewFeedbackResponse.model_validate(r) for r in records]


@router.get("/{crew_id}", response_model=CrewResponse)
async def get_crew(
    crew_id: Annotated[UUID, Path(title="The ID of the crew to get")],
    service: Annotated[CrewService, Depends(get_crew_service)],
    group_context: GroupContextDep,
):
    """
    Get a specific crew by ID for the current group.

    Args:
        crew_id: ID of the crew to get
        service: Crew service injected by dependency
        group_context: Group context from headers

    Returns:
        Crew if found and belongs to group

    Raises:
        HTTPException: If crew not found or doesn't belong to group
    """
    crew = await service.get_by_group(crew_id, group_context)
    if not crew:
        raise NotFoundError("Crew not found")
    return _crew_to_response(crew)


@router.post("", response_model=CrewResponse, status_code=status.HTTP_201_CREATED)
async def create_crew(
    crew_in: CrewCreate,
    service: Annotated[CrewService, Depends(get_crew_service)],
    group_context: GroupContextDep,
    overwrite: bool = Query(False, description="Replace an existing crew of the same name instead of failing with 409."),
):
    """
    Create a new crew for the current group.
    Only Editors and Admins can create crews.

    Args:
        crew_in: Crew data for creation
        service: Crew service injected by dependency
        group_context: Group context from headers
        overwrite: When true, replace an existing same-named crew instead of 409.

    Returns:
        Created crew
    """
    # Check permissions - only editors and admins can create crews
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can create crews")

    try:
        # Use the group-aware create method
        crew = await service.create_with_group(crew_in, group_context, overwrite=overwrite)

        # Format the response
        return _crew_to_response(crew)
    except ValidationError as e:
        logger.error(f"Validation error: {e.errors()}")
        raise UnprocessableEntityError(str(e))


@router.post("/debug")
async def debug_crew_data(
    crew_in: CrewCreate,
    group_context: GroupContextDep,
):
    """
    Debug endpoint to validate crew data structure without saving.

    Args:
        crew_in: Crew data to validate
        group_context: Group context from headers

    Returns:
        Validation result
    """
    try:
        # Convert to dict and back to ensure it's valid
        data_dict = crew_in.model_dump()
        logger.info("Data validation successful")
        logger.info(f"Crew name: {data_dict['name']}")
        logger.info(f"Agent IDs: {data_dict['agent_ids']}")
        logger.info(f"Task IDs: {data_dict['task_ids']}")
        logger.info(f"Number of nodes: {len(data_dict['nodes'])}")
        logger.info(f"Number of edges: {len(data_dict['edges'])}")
        return {
            "status": "success",
            "message": "Data validation successful",
            "data": {
                "name": data_dict["name"],
                "agent_ids": data_dict["agent_ids"],
                "task_ids": data_dict["task_ids"],
                "node_count": len(data_dict["nodes"]),
                "edge_count": len(data_dict["edges"]),
            },
        }
    except ValidationError as e:
        logger.error(f"Validation error: {e.json()}")
        return {
            "status": "error",
            "message": "Validation failed",
            "errors": json.loads(e.json()),
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {"status": "error", "message": f"Unexpected error: {str(e)}"}


@router.put("/{crew_id}", response_model=CrewResponse)
async def update_crew(
    crew_id: Annotated[UUID, Path(title="The ID of the crew to update")],
    crew_update: CrewUpdate,
    service: Annotated[CrewService, Depends(get_crew_service)],
    group_context: GroupContextDep,
):
    """
    Update a crew for the current group.
    Only Editors and Admins can update crews.

    Args:
        crew_id: ID of the crew to update
        crew_update: Crew data for update (only provided fields will be updated)
        service: Crew service injected by dependency
        group_context: Group context from headers

    Returns:
        Updated crew

    Raises:
        HTTPException: If crew not found or doesn't belong to group
    """
    # Check permissions - only editors and admins can update crews
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can update crews")

    try:
        updated_crew = await service.update_with_partial_data_by_group(
            crew_id, crew_update, group_context
        )
        if not updated_crew:
            raise NotFoundError("Crew not found")
        return _crew_to_response(updated_crew)
    except ValidationError as e:
        logger.error(f"Validation error: {e.errors()}")
        raise UnprocessableEntityError(str(e))


@router.delete("/{crew_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_crew(
    crew_id: Annotated[UUID, Path(title="The ID of the crew to delete")],
    service: Annotated[CrewService, Depends(get_crew_service)],
    group_context: GroupContextDep,
):
    """
    Delete a crew for the current group.
    Only Editors and Admins can delete crews.

    Args:
        crew_id: ID of the crew to delete
        service: Crew service injected by dependency
        group_context: Group context from headers

    Raises:
        HTTPException: If crew not found or doesn't belong to group
    """
    # Check permissions - only editors and admins can delete crews
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise ForbiddenError("Only editors and admins can delete crews")

    deleted = await service.delete_by_group(crew_id, group_context)
    if not deleted:
        raise NotFoundError("Crew not found")


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
async def delete_all_crews(
    service: Annotated[CrewService, Depends(get_crew_service)],
    group_context: GroupContextDep,
):
    """
    Delete all crews for the current group.
    Only Admins can delete all crews.

    Args:
        service: Crew service injected by dependency
        group_context: Group context from headers
    """
    # Check permissions - only admins can delete all crews
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can delete all crews")

    await service.delete_all_by_group(group_context)
