"""
Router for dispatching natural language requests to appropriate generation services.

This module provides endpoints for analyzing user messages and determining
whether they want to generate an agent, task, or crew, then calling the appropriate service.
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any

from src.schemas.dispatcher import DispatcherRequest, DispatcherResponse
from src.services.dispatcher_service import DispatcherService
from src.services.log_service import LLMLogService

router = APIRouter(
    prefix="/dispatcher",
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


@router.post("/detect-intent", response_model=DispatcherResponse)
async def detect_intent_only(
    request: DispatcherRequest
) -> DispatcherResponse:
    """
    Detect intent from a natural language message without executing generation.
    
    This endpoint only performs intent detection without calling the generation services.
    Useful for previewing what action would be taken.
    
    Args:
        request: The dispatcher request containing the user's message
        
    Returns:
        DispatcherResponse with intent detection results
        
    Raises:
        HTTPException: If there's an error in processing
    """
    try:
        # Create service instance
        dispatcher_service = DispatcherService.create()
        
        # Only detect intent without dispatching
        intent_result = await dispatcher_service._detect_intent(request.message, request.model or "databricks-llama-4-maverick")
        
        # Create response
        response = DispatcherResponse(
            intent=intent_result["intent"],
            confidence=intent_result["confidence"],
            extracted_info=intent_result["extracted_info"],
            suggested_prompt=intent_result["suggested_prompt"]
        )
        
        return response
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error in intent detection: {str(e)}"
        ) 