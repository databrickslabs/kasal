"""
API router for crew generation operations.

This module provides API endpoints for generating crew setups
with agents and tasks in the CrewAI ecosystem.
"""

import logging
import traceback
from fastapi import APIRouter, HTTPException

from src.schemas.crew import CrewGenerationRequest, CrewGenerationResponse, CrewCreationResponse
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
    request: CrewGenerationRequest
):
    """
    Generate and create a crew setup with agents and tasks in the database.
    
    This endpoint generates a crew plan and creates all entities in the database.
    """
    try:
        # Create service
        crew_service = CrewGenerationService.create()
        
        # Generate and create the crew - all DB handling is inside the service
        logger.info(f"Creating crew from prompt: {request.prompt[:50]}...")
        result = await crew_service.create_crew_complete(request)
        
        # Log success
        created_agents = result.get('agents', [])
        created_tasks = result.get('tasks', [])
        logger.info(f"Created crew with {len(created_agents)} agents and {len(created_tasks)} tasks")
        
        # Return the created objects
        return CrewCreationResponse(
            agents=created_agents,
            tasks=created_tasks
        )
        
    except ValueError as e:
        # Handle validation errors
        error_msg = str(e)
        logger.error(f"Validation error: {error_msg}")
        raise HTTPException(status_code=400, detail=error_msg)
        
    except Exception as e:
        # Handle other errors
        error_msg = f"Error creating crew: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=error_msg) 