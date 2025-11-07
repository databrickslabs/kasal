"""
Router for dispatching natural language requests to appropriate generation services.

This module provides endpoints for analyzing user messages and determining
whether they want to generate an agent, task, or crew, then calling the appropriate service.
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any

from src.schemas.dispatcher import DispatcherRequest, DispatcherResponse
from src.services.dispatcher_service import DispatcherService
from src.core.dependencies import GroupContextDep, SessionDep

router = APIRouter(
    prefix="/dispatcher",
    tags=["dispatcher"]
)

@router.post("/dispatch", response_model=Dict[str, Any])
async def dispatch_request(
    request: DispatcherRequest,
    group_context: GroupContextDep,
    session: SessionDep
) -> Dict[str, Any]:
    """
    Dispatch a natural language request to the appropriate generation service.

    Args:
        request: Dispatcher request with user message and options
        group_context: Group context from headers
        session: Database session from FastAPI DI

    Returns:
        Dictionary containing the intent detection result and generation response
    """
    try:
        # CRITICAL: Set UserContext so LLMManager can access group_id
        # This is needed for multi-tenant isolation in API key operations
        from src.utils.user_context import UserContext
        if group_context:
            UserContext.set_group_context(group_context)
            # Also set user token if available for OBO authentication
            if group_context.access_token:
                UserContext.set_user_token(group_context.access_token)

        # Create service instance with injected session
        dispatcher_service = DispatcherService.create(session)

        # Process request with tenant context
        result = await dispatcher_service.dispatch(request, group_context)

        return result

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing request: {str(e)}"
        )


@router.post("/detect-intent", response_model=DispatcherResponse)
async def detect_intent_only(
    request: DispatcherRequest,
    group_context: GroupContextDep,
    session: SessionDep
) -> DispatcherResponse:
    """
    Detect intent from a natural language message without executing generation.

    This endpoint only performs intent detection without calling the generation services.
    Useful for previewing what action would be taken.

    Args:
        request: The dispatcher request containing the user's message
        group_context: Group context from headers
        session: Database session from FastAPI DI

    Returns:
        DispatcherResponse with intent detection results

    Raises:
        HTTPException: If there's an error in processing
    """
    try:
        # CRITICAL: Set UserContext so LLMManager can access group_id
        from src.utils.user_context import UserContext
        if group_context:
            UserContext.set_group_context(group_context)
            if group_context.access_token:
                UserContext.set_user_token(group_context.access_token)

        # Create service instance with injected session
        dispatcher_service = DispatcherService.create(session)

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