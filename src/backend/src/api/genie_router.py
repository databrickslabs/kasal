"""
Genie API Router

Handles Genie-related API endpoints using proper service/repository architecture.
"""

from fastapi import APIRouter, Request, HTTPException, Depends
from typing import Optional
import logging

from src.services.genie_service import GenieService
from src.schemas.genie import (
    GenieSpace,
    GenieSpacesRequest,
    GenieSpacesResponse,
    GenieSendMessageRequest,
    GenieSendMessageResponse,
    GenieExecutionRequest,
    GenieExecutionResponse,
    GenieAuthConfig
)
from src.utils.databricks_auth import extract_user_token_from_request

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/genie", tags=["genie"])


@router.get("/spaces", response_model=GenieSpacesResponse)
async def get_genie_spaces(
    request: Request,
    page_token: Optional[str] = None,
    page_size: int = 50
) -> GenieSpacesResponse:
    """
    Fetch available Genie spaces from Databricks with pagination.
    
    Args:
        request: FastAPI request object
        page_token: Token for fetching next page
        page_size: Number of items per page (default 50, max 200)
    
    Returns:
        GenieSpacesResponse: List of available Genie spaces with pagination info
    """
    try:
        # Extract user token for OBO authentication if available
        user_token = extract_user_token_from_request(request)
        
        # Create auth config with user token for OBO
        auth_config = GenieAuthConfig(
            use_obo=True,
            user_token=user_token
        )
        
        # Create service with auth config
        service = GenieService(auth_config)
        
        # Create request with pagination parameters
        spaces_request = GenieSpacesRequest(
            page_token=page_token,
            page_size=min(page_size, 200)  # Cap at 200
        )
        
        # Get spaces with pagination
        spaces_response = await service.get_spaces(spaces_request)
        
        if not spaces_response.spaces and not page_token:
            logger.warning("No Genie spaces found. User may not have access to any spaces.")
        
        return spaces_response
        
    except Exception as e:
        logger.error(f"Error fetching Genie spaces: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch Genie spaces: {str(e)}")


@router.post("/spaces/search", response_model=GenieSpacesResponse)
async def search_genie_spaces(
    request: Request,
    spaces_request: GenieSpacesRequest
) -> GenieSpacesResponse:
    """
    Search and filter Genie spaces from Databricks with pagination.
    
    Args:
        request: FastAPI request object
        spaces_request: Request with search, filter, and pagination parameters
    
    Returns:
        GenieSpacesResponse: List of filtered Genie spaces with pagination info
    """
    try:
        # Extract user token for OBO authentication if available
        user_token = extract_user_token_from_request(request)
        
        # Create auth config with user token for OBO
        auth_config = GenieAuthConfig(
            use_obo=True,
            user_token=user_token
        )
        
        # Create service with auth config
        service = GenieService(auth_config)
        
        # Get spaces through service layer with all parameters including pagination
        spaces_response = await service.get_spaces(spaces_request)
        
        if not spaces_response.spaces and not spaces_request.page_token:
            logger.warning("No Genie spaces found matching criteria.")
        
        return spaces_response
        
    except Exception as e:
        logger.error(f"Error searching Genie spaces: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to search Genie spaces: {str(e)}")


@router.get("/spaces/{space_id}", response_model=GenieSpace)
async def get_genie_space_details(space_id: str, request: Request) -> GenieSpace:
    """
    Get details for a specific Genie space.
    
    Args:
        space_id: The ID of the Genie space
        request: FastAPI request object
        
    Returns:
        GenieSpace object with space details
    """
    try:
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(request)
        
        # Create auth config
        auth_config = GenieAuthConfig(
            use_obo=True,
            user_token=user_token
        )
        
        # Create service with auth config
        service = GenieService(auth_config)
        
        # Get space details through service layer
        space = await service.get_space_details(space_id)
        
        if not space:
            raise HTTPException(status_code=404, detail=f"Space {space_id} not found")
        
        return space
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching space details: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch space details: {str(e)}")


@router.post("/execute", response_model=GenieExecutionResponse)
async def execute_genie_query(
    request: Request,
    execution_request: GenieExecutionRequest
) -> GenieExecutionResponse:
    """
    Execute a Genie query in a specific space.
    
    Args:
        request: FastAPI request object
        execution_request: Query execution request
        
    Returns:
        GenieExecutionResponse with query result
    """
    try:
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(request)
        
        # Create auth config
        auth_config = GenieAuthConfig(
            use_obo=True,
            user_token=user_token
        )
        
        # Create service with auth config
        service = GenieService(auth_config)
        
        # Execute query through service layer
        response = await service.execute_query(
            space_id=execution_request.space_id,
            question=execution_request.question,
            conversation_id=execution_request.conversation_id,
            timeout=execution_request.timeout or 120
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error executing Genie query: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to execute query: {str(e)}")


@router.post("/send-message", response_model=GenieSendMessageResponse)
async def send_genie_message(
    request: Request,
    message_request: GenieSendMessageRequest
) -> GenieSendMessageResponse:
    """
    Send a message to Genie.
    
    Args:
        request: FastAPI request object
        message_request: Message request
        
    Returns:
        GenieSendMessageResponse with message details
    """
    try:
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(request)
        
        # Create auth config
        auth_config = GenieAuthConfig(
            use_obo=True,
            user_token=user_token
        )
        
        # Create service with auth config
        service = GenieService(auth_config)
        
        # Send message through service layer
        response = await service.send_message(
            space_id=message_request.space_id,
            message=message_request.message,
            conversation_id=message_request.conversation_id
        )
        
        if not response:
            raise HTTPException(status_code=500, detail="Failed to send message")
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send message: {str(e)}")