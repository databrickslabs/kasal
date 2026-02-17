"""
API router for crew generation operations.

This module provides API endpoints for generating crew setups
with agents and tasks in the CrewAI ecosystem.
"""

import asyncio
import logging
import uuid

from fastapi import APIRouter

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import BadRequestError
from src.schemas.crew import (
    CrewCreationResponse,
    CrewGenerationRequest,
    CrewGenerationResponse,
    CrewStreamingRequest,
    CrewStreamingResponse,
)
from src.services.crew_generation_service import CrewGenerationService

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/crew",
    tags=["crew"],
    responses={404: {"description": "Not found"}},
)


@router.post("/create-crew", response_model=CrewCreationResponse)
async def create_crew(
    request: CrewGenerationRequest, group_context: GroupContextDep, session: SessionDep
):
    """
    Generate and create a crew setup with agents and tasks in the database.

    This endpoint generates a crew plan and creates all entities in the database.
    """
    # Create service with injected session
    crew_service = CrewGenerationService(session)

    # Generate and create the crew - all DB handling is inside the service
    logger.info(f"Creating crew from prompt: {request.prompt[:50]}...")
    result = await crew_service.create_crew_complete(request, group_context)

    # Log success
    created_agents = result.get("agents", [])
    created_tasks = result.get("tasks", [])
    logger.info(
        f"Created crew with {len(created_agents)} agents and {len(created_tasks)} tasks"
    )

    # Return the created objects
    return CrewCreationResponse(agents=created_agents, tasks=created_tasks)


@router.post("/create-crew-streaming", response_model=CrewStreamingResponse)
async def create_crew_streaming(
    request: CrewStreamingRequest, group_context: GroupContextDep, session: SessionDep
):
    """
    Start progressive crew generation with SSE streaming.

    Returns a generation_id immediately. Connect to
    GET /sse/generations/{generation_id}/stream to receive progressive updates.
    """
    if not request.prompt or not request.prompt.strip():
        raise BadRequestError("Prompt is required and cannot be empty")

    generation_id = str(uuid.uuid4())

    # NOTE: The request-scoped session will be closed once this response is sent.
    # create_crew_progressive() creates its own independent session for DB work.
    crew_service = CrewGenerationService(session)

    logger.info(
        f"Starting progressive crew generation {generation_id} "
        f"from prompt: {request.prompt[:50]}..."
    )

    # Spawn background task — it manages its own DB session internally
    asyncio.create_task(
        crew_service.create_crew_progressive(request, group_context, generation_id)
    )

    return CrewStreamingResponse(generation_id=generation_id)
