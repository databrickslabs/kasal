"""
API router for connections between agents and tasks.

This module provides API endpoints for generating and testing connections
between agents and tasks in the CrewAI ecosystem.
"""

import logging
from typing import Annotated
from fastapi import APIRouter, HTTPException, Depends

from src.core.dependencies import GroupContextDep, SessionDep
from src.schemas.connection import ConnectionRequest, ConnectionResponse, ApiKeyTestResponse
from src.services.connection_service import ConnectionService

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/connections",
    tags=["connections"],
    responses={404: {"description": "Not found"}},
)

async def get_connection_service(session: SessionDep) -> ConnectionService:
    """
    Dependency provider for ConnectionService.

    Creates service with session for accessing TemplateService properly.

    Args:
        session: Database session from FastAPI DI

    Returns:
        ConnectionService instance
    """
    return ConnectionService(session)

# Type alias for cleaner function signatures
ConnectionServiceDep = Annotated[ConnectionService, Depends(get_connection_service)]

@router.post("/generate-connections", response_model=ConnectionResponse)
async def generate_connections(
    request: ConnectionRequest,
    service: ConnectionServiceDep,
    group_context: GroupContextDep
):
    """
    Generate connections between agents and tasks.
    
    This endpoint analyzes agents and tasks and determines the optimal
    assignments and dependencies between tasks.
    """
    try:
        # Use injected service
        logger.info(f"Generating connections for {len(request.agents)} agents and {len(request.tasks)} tasks")
        connections = await service.generate_connections(request)
        
        # Log the number of assignments and dependencies
        logger.info(f"Generated {len(connections.assignments)} assignments and {len(connections.dependencies)} dependencies")
        
        return connections
        
    except ValueError as e:
        # Handle validation errors with a 400 response
        error_msg = str(e)
        logger.error(f"Validation error: {error_msg}")
        raise HTTPException(status_code=400, detail=error_msg)
        
    except Exception as e:
        # Handle other errors with a 500 response
        error_msg = f"Error generating connections: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)

@router.get("/test-api-key", response_model=ApiKeyTestResponse)
async def test_api_key(
    service: ConnectionServiceDep,
    group_context: GroupContextDep
):
    """
    Test API keys and configuration.
    
    This endpoint validates API keys for different providers
    and returns information about their status.
    """
    try:
        # Use injected service
        logger.info("Testing API keys")
        results = await service.test_api_keys()
        
        return results
        
    except Exception as e:
        # Handle errors with a 500 response
        error_msg = f"Error testing API keys: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg) 