"""
AgentBricks Service Layer

Business logic layer for AgentBricks operations.
Coordinates between router and repository layers.
"""

import logging
from typing import Optional, List

from src.repositories.agentbricks_repository import AgentBricksRepository
from src.schemas.agentbricks import (
    AgentBricksEndpoint,
    AgentBricksEndpointsRequest,
    AgentBricksEndpointsResponse,
    AgentBricksQueryRequest,
    AgentBricksQueryResponse,
    AgentBricksExecutionRequest,
    AgentBricksExecutionResponse,
    AgentBricksAuthConfig,
    AgentBricksMessage,
    AgentBricksQueryStatus
)

logger = logging.getLogger(__name__)


class AgentBricksService:
    """
    Service layer for AgentBricks operations.
    Handles business logic and coordination.
    """

    def __init__(self, auth_config: Optional[AgentBricksAuthConfig] = None):
        """
        Initialize AgentBricks Service.

        Args:
            auth_config: Optional authentication configuration
        """
        self.auth_config = auth_config
        self.repository = AgentBricksRepository(auth_config)

    async def get_endpoints(
        self,
        request: Optional[AgentBricksEndpointsRequest] = None
    ) -> AgentBricksEndpointsResponse:
        """
        Get available AgentBricks endpoints with optional filtering.

        Args:
            request: Optional request with search and filter parameters

        Returns:
            AgentBricksEndpointsResponse containing list of endpoints
        """
        try:
            # Use default request if none provided
            if request is None:
                request = AgentBricksEndpointsRequest()

            logger.info(
                f"Fetching AgentBricks endpoints (search: {request.search_query}, "
                f"ready_only: {request.ready_only})"
            )

            # Call repository
            endpoints_response = await self.repository.get_endpoints(request)

            logger.info(
                f"Retrieved {len(endpoints_response.endpoints)} AgentBricks endpoints"
            )
            return endpoints_response

        except Exception as e:
            logger.error(f"Error in get_endpoints service: {e}")
            # Return empty response on error
            return AgentBricksEndpointsResponse(endpoints=[])

    async def search_endpoints(
        self,
        query: Optional[str] = None,
        ready_only: bool = True
    ) -> AgentBricksEndpointsResponse:
        """
        Search for AgentBricks endpoints by query.

        Args:
            query: Search query string
            ready_only: Only return ready endpoints

        Returns:
            AgentBricksEndpointsResponse with matching endpoints
        """
        try:
            logger.info(f"Searching AgentBricks endpoints with query: {query}")
            request = AgentBricksEndpointsRequest(
                search_query=query,
                ready_only=ready_only
            )
            return await self.get_endpoints(request)

        except Exception as e:
            logger.error(f"Error searching endpoints: {e}")
            return AgentBricksEndpointsResponse(endpoints=[])

    async def get_endpoint_by_name(
        self,
        endpoint_name: str
    ) -> Optional[AgentBricksEndpoint]:
        """
        Get a specific AgentBricks endpoint by name.

        Args:
            endpoint_name: The endpoint name

        Returns:
            AgentBricksEndpoint object or None if not found
        """
        try:
            logger.info(f"Fetching endpoint: {endpoint_name}")

            # Search for specific endpoint
            request = AgentBricksEndpointsRequest(
                search_query=endpoint_name,
                ready_only=False  # Include all states for specific lookup
            )
            response = await self.repository.get_endpoints(request)

            # Find exact match
            for endpoint in response.endpoints:
                if endpoint.name == endpoint_name:
                    logger.info(f"Found endpoint: {endpoint.name}")
                    return endpoint

            logger.warning(f"Endpoint not found: {endpoint_name}")
            return None

        except Exception as e:
            logger.error(f"Error getting endpoint by name: {e}")
            return None

    async def query_endpoint(
        self,
        endpoint_name: str,
        messages: List[AgentBricksMessage],
        custom_inputs: Optional[dict] = None,
        return_trace: bool = False
    ) -> AgentBricksQueryResponse:
        """
        Query an AgentBricks endpoint with messages.

        Args:
            endpoint_name: The endpoint name
            messages: List of messages (conversation history)
            custom_inputs: Optional custom inputs for the agent
            return_trace: Whether to return execution trace

        Returns:
            AgentBricksQueryResponse with the result
        """
        try:
            logger.info(f"Querying AgentBricks endpoint: {endpoint_name}")

            request = AgentBricksQueryRequest(
                endpoint_name=endpoint_name,
                messages=messages,
                custom_inputs=custom_inputs,
                return_trace=return_trace
            )

            response = await self.repository.query_endpoint(request)

            if response.status == AgentBricksQueryStatus.SUCCESS:
                logger.info(f"Query executed successfully on {endpoint_name}")
            else:
                logger.error(f"Query failed on {endpoint_name}: {response.error}")

            return response

        except Exception as e:
            logger.error(f"Error querying endpoint: {e}")
            return AgentBricksQueryResponse(
                response="",
                status=AgentBricksQueryStatus.FAILED,
                error=str(e)
            )

    async def execute_query(
        self,
        endpoint_name: str,
        question: str,
        custom_inputs: Optional[dict] = None,
        return_trace: bool = False,
        timeout: int = 120
    ) -> AgentBricksExecutionResponse:
        """
        Execute a simplified query to an AgentBricks endpoint.

        Args:
            endpoint_name: The endpoint name
            question: The question to ask the agent
            custom_inputs: Optional custom inputs
            return_trace: Whether to include execution trace
            timeout: Timeout in seconds

        Returns:
            AgentBricksExecutionResponse with the result
        """
        try:
            logger.info(f"Executing query on {endpoint_name}: {question[:50]}...")

            request = AgentBricksExecutionRequest(
                endpoint_name=endpoint_name,
                question=question,
                custom_inputs=custom_inputs,
                return_trace=return_trace,
                timeout=timeout
            )

            response = await self.repository.execute_query(request)

            if response.status == AgentBricksQueryStatus.SUCCESS:
                logger.info(f"Query executed successfully on {endpoint_name}")
            else:
                logger.error(f"Query failed on {endpoint_name}: {response.error}")

            return response

        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return AgentBricksExecutionResponse(
                endpoint_name=endpoint_name,
                status=AgentBricksQueryStatus.FAILED,
                error=str(e)
            )

    async def validate_endpoint_access(
        self,
        endpoint_name: str,
        auth_config: Optional[AgentBricksAuthConfig] = None
    ) -> bool:
        """
        Validate that the current authentication has access to an endpoint.

        Args:
            endpoint_name: The endpoint name to validate
            auth_config: Optional auth config to use

        Returns:
            True if access is valid, False otherwise
        """
        try:
            # Try to get endpoint details as a validation check
            if auth_config:
                self.repository.auth_config = auth_config

            endpoint = await self.get_endpoint_by_name(endpoint_name)
            return endpoint is not None

        except Exception as e:
            logger.error(f"Error validating endpoint access: {e}")
            return False

    async def list_ready_endpoints(self) -> List[AgentBricksEndpoint]:
        """
        Get a simple list of all ready AgentBricks endpoints.

        Returns:
            List of ready AgentBricksEndpoint objects
        """
        try:
            request = AgentBricksEndpointsRequest(ready_only=True)
            response = await self.repository.get_endpoints(request)
            return response.endpoints

        except Exception as e:
            logger.error(f"Error listing ready endpoints: {e}")
            return []
