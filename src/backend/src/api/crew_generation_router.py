"""
API router for crew generation operations.

This module provides API endpoints for generating crew setups
with agents and tasks in the CrewAI ecosystem.
"""

import logging
import traceback

from fastapi import APIRouter

from src.core.dependencies import GroupContextDep, SessionDep
from src.schemas.crew import (
    CrewCreationResponse,
    CrewGenerationRequest,
    CrewGenerationResponse,
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
