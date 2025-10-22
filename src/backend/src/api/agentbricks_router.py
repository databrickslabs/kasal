"""
AgentBricks API Router

Handles AgentBricks-related API endpoints using proper service/repository architecture.
"""

from fastapi import APIRouter, Request, HTTPException, Depends
from src.core.dependencies import GroupContextDep
from typing import Optional
import logging

from src.services.agentbricks_service import AgentBricksService
from src.schemas.agentbricks import (
    AgentBricksEndpoint,
    AgentBricksEndpointsRequest,
    AgentBricksEndpointsResponse,
    AgentBricksQueryRequest,
    AgentBricksQueryResponse,
    AgentBricksExecutionRequest,
    AgentBricksExecutionResponse,
    AgentBricksAuthConfig
)
from src.utils.databricks_auth import extract_user_token_from_request
from src.utils.user_context import UserContext

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/agentbricks", tags=["agentbricks"])


@router.get("/endpoints", response_model=AgentBricksEndpointsResponse)
async def get_agentbricks_endpoints(
    request: Request,
    ready_only: bool = True,
    search_query: Optional[str] = None,
    group_context: GroupContextDep = None
) -> AgentBricksEndpointsResponse:
    """
    Fetch available AgentBricks endpoints from Databricks.

    Args:
        request: FastAPI request object
        ready_only: Only return ready endpoints (default True)
        search_query: Optional search query to filter endpoints
        group_context: Group context from dependency injection

    Returns:
        AgentBricksEndpointsResponse: List of available AgentBricks endpoints
    """
    try:
        # Set group context for this request so get_auth_context() can access it
        if group_context:
            UserContext.set_group_context(group_context)

        # Extract user token for OBO authentication if available
        user_token = extract_user_token_from_request(request)

        # Create auth config with user token for OBO
        auth_config = AgentBricksAuthConfig(
            use_obo=True,
            user_token=user_token
        )

        # Create service with auth config
        service = AgentBricksService(auth_config)

        # Create request
        endpoints_request = AgentBricksEndpointsRequest(
            search_query=search_query,
            ready_only=ready_only
        )

        # Get endpoints
        endpoints_response = await service.get_endpoints(endpoints_request)

        if not endpoints_response.endpoints:
            logger.warning("No AgentBricks endpoints found. User may not have access to any endpoints.")

        return endpoints_response

    except Exception as e:
        logger.error(f"Error fetching AgentBricks endpoints: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch AgentBricks endpoints: {str(e)}")


@router.post("/endpoints/search", response_model=AgentBricksEndpointsResponse)
async def search_agentbricks_endpoints(
    request: Request,
    endpoints_request: AgentBricksEndpointsRequest,
    group_context: GroupContextDep = None
) -> AgentBricksEndpointsResponse:
    """
    Search and filter AgentBricks endpoints from Databricks.

    Args:
        request: FastAPI request object
        endpoints_request: Request with search and filter parameters
        group_context: Group context from dependency injection

    Returns:
        AgentBricksEndpointsResponse: List of filtered AgentBricks endpoints
    """
    try:
        # Set group context for this request so get_auth_context() can access it
        if group_context:
            UserContext.set_group_context(group_context)

        # Extract user token for OBO authentication if available
        user_token = extract_user_token_from_request(request)

        # Create auth config with user token for OBO
        auth_config = AgentBricksAuthConfig(
            use_obo=True,
            user_token=user_token
        )

        # Create service with auth config
        service = AgentBricksService(auth_config)

        # Get endpoints through service layer
        endpoints_response = await service.get_endpoints(endpoints_request)

        if not endpoints_response.endpoints:
            logger.warning("No AgentBricks endpoints found matching criteria.")

        return endpoints_response

    except Exception as e:
        logger.error(f"Error searching AgentBricks endpoints: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to search AgentBricks endpoints: {str(e)}")


@router.get("/endpoints/{endpoint_name}", response_model=AgentBricksEndpoint)
async def get_agentbricks_endpoint_details(
    endpoint_name: str,
    request: Request,
    group_context: GroupContextDep = None
) -> AgentBricksEndpoint:
    """
    Get details for a specific AgentBricks endpoint.

    Args:
        endpoint_name: The name of the AgentBricks endpoint
        request: FastAPI request object
        group_context: Group context from dependency injection

    Returns:
        AgentBricksEndpoint object with endpoint details
    """
    try:
        # Set group context for this request
        if group_context:
            UserContext.set_group_context(group_context)

        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(request)

        # Create auth config
        auth_config = AgentBricksAuthConfig(
            use_obo=True,
            user_token=user_token
        )

        # Create service with auth config
        service = AgentBricksService(auth_config)

        # Get endpoint details through service layer
        endpoint = await service.get_endpoint_by_name(endpoint_name)

        if not endpoint:
            raise HTTPException(status_code=404, detail=f"Endpoint {endpoint_name} not found")

        return endpoint

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching endpoint details: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch endpoint details: {str(e)}")


@router.post("/query", response_model=AgentBricksQueryResponse)
async def query_agentbricks_endpoint(
    request: Request,
    query_request: AgentBricksQueryRequest,
    group_context: GroupContextDep = None
) -> AgentBricksQueryResponse:
    """
    Query an AgentBricks endpoint with messages.

    Args:
        request: FastAPI request object
        query_request: Query request with endpoint name and messages
        group_context: Group context from dependency injection

    Returns:
        AgentBricksQueryResponse with the result
    """
    try:
        # Set group context for this request
        if group_context:
            UserContext.set_group_context(group_context)

        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(request)

        # Create auth config
        auth_config = AgentBricksAuthConfig(
            use_obo=True,
            user_token=user_token
        )

        # Create service with auth config
        service = AgentBricksService(auth_config)

        # Query endpoint through service layer
        response = await service.query_endpoint(
            endpoint_name=query_request.endpoint_name,
            messages=query_request.messages,
            custom_inputs=query_request.custom_inputs,
            return_trace=query_request.return_trace
        )

        return response

    except Exception as e:
        logger.error(f"Error querying AgentBricks endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to query endpoint: {str(e)}")


@router.post("/execute", response_model=AgentBricksExecutionResponse)
async def execute_agentbricks_query(
    request: Request,
    execution_request: AgentBricksExecutionRequest,
    group_context: GroupContextDep = None
) -> AgentBricksExecutionResponse:
    """
    Execute a simplified query to an AgentBricks endpoint.

    Args:
        request: FastAPI request object
        execution_request: Execution request with endpoint name and question
        group_context: Group context from dependency injection

    Returns:
        AgentBricksExecutionResponse with query result
    """
    try:
        # Set group context for this request
        if group_context:
            UserContext.set_group_context(group_context)

        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(request)

        # Create auth config
        auth_config = AgentBricksAuthConfig(
            use_obo=True,
            user_token=user_token
        )

        # Create service with auth config
        service = AgentBricksService(auth_config)

        # Execute query through service layer
        response = await service.execute_query(
            endpoint_name=execution_request.endpoint_name,
            question=execution_request.question,
            custom_inputs=execution_request.custom_inputs,
            return_trace=execution_request.return_trace,
            timeout=execution_request.timeout or 120
        )

        return response

    except Exception as e:
        logger.error(f"Error executing AgentBricks query: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to execute query: {str(e)}")
