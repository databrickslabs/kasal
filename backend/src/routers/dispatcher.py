"""
Router for dispatching natural language requests to appropriate generation services.

This module provides endpoints for analyzing user messages and determining
whether they want to generate an agent, task, or crew, then calling the appropriate service.
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any

from src.schemas.dispatcher import DispatcherRequest, DispatcherResponse
from src.services.dispatcher_service import DispatcherService
from src.services.log_service import LLMLogService

router = APIRouter(
    prefix="/api/dispatcher",
    tags=["dispatcher"]
)

@router.post("/dispatch", response_model=Dict[str, Any])
async def dispatch_request(request: DispatcherRequest) -> Dict[str, Any]:
    """
    Dispatch a natural language request to the appropriate generation service.
    
    Args:
        request: Dispatcher request with user message and options
        
    Returns:
        Dictionary containing the intent detection result and generation response
    """
    try:
        # Create service instance
        dispatcher_service = DispatcherService.create()
        
        # Process request
        result = await dispatcher_service.dispatch(request)
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing request: {str(e)}"
        ) 