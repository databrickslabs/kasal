"""
AgentBricks Repository Layer

Handles all communication with Databricks AgentBricks (Mosaic AI Agent Bricks) API.
Uses unified authentication from get_auth_context() which implements:
  1. OBO (On-Behalf-Of) with user token
  2. PAT from database with group_id filtering
  3. Service Principal OAuth (SPN)
"""

import asyncio
import logging
import time
from typing import Optional, Dict, Any, List, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.schemas.agentbricks import (
    AgentBricksEndpoint,
    AgentBricksEndpointsRequest,
    AgentBricksEndpointsResponse,
    AgentBricksQueryRequest,
    AgentBricksQueryResponse,
    AgentBricksQueryStatus,
    AgentBricksExecutionRequest,
    AgentBricksExecutionResponse,
    AgentBricksAuthConfig,
    AgentBricksMessage
)
from src.utils.databricks_auth import get_auth_context

logger = logging.getLogger(__name__)


class AgentBricksRepository:
    """
    Repository for interacting with Databricks AgentBricks API.
    Follows the same authentication pattern as GenieRepository.
    """

    def __init__(self, auth_config: Optional[AgentBricksAuthConfig] = None):
        """
        Initialize AgentBricks Repository.

        Args:
            auth_config: Optional authentication configuration
        """
        self.auth_config = auth_config if auth_config is not None else None
        self._host = None
        self._session = None
        self._setup_session()

    def _setup_session(self):
        """Setup requests session with retry logic."""
        self._session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    async def _get_host(self) -> str:
        """
        Get Databricks host with auto-detection.
        Priority: config -> environment -> SDK Config -> databricks_auth
        """
        if self._host:
            return self._host

        # Check from config
        if self.auth_config and self.auth_config.host:
            self._host = self.auth_config.host
            logger.info(f"Using host from config: {self._host}")
            return self._host

        # Use unified auth to get host
        auth = await get_auth_context()
        databricks_host = auth.workspace_url if auth else None

        # If not available from auth context, try SDK Config
        if not databricks_host:
            try:
                from databricks.sdk.config import Config
                sdk_config = Config()
                if sdk_config.host:
                    databricks_host = sdk_config.host
                    logger.info(f"Auto-detected host from SDK Config: {databricks_host}")
            except Exception as e:
                logger.debug(f"Could not auto-detect host from SDK: {e}")

        # If still no host, try databricks_auth
        if not databricks_host:
            try:
                from src.utils.databricks_auth import _databricks_auth
                await _databricks_auth._load_config()
                databricks_host = _databricks_auth.get_workspace_host()
                logger.info(f"Got host from databricks_auth: {databricks_host}")
            except Exception as e:
                logger.debug(f"Could not get host from databricks_auth: {e}")

        if not databricks_host:
            databricks_host = "your-workspace.cloud.databricks.com"
            logger.warning(f"Using default host: {databricks_host}")

        # Normalize host format
        if databricks_host.startswith('https://'):
            databricks_host = databricks_host[8:]
        if databricks_host.endswith('/'):
            databricks_host = databricks_host[:-1]

        self._host = databricks_host
        return self._host

    async def _get_auth_headers(self) -> Tuple[Optional[Dict[str, str]], Optional[str]]:
        """
        Get authentication headers using unified authentication.
        Delegates all authentication logic to get_auth_context().

        Returns:
            Tuple of (headers dict, error message)
        """
        try:
            # Extract user token if available (for OBO)
            user_token = None
            if self.auth_config and self.auth_config.use_obo and self.auth_config.user_token:
                user_token = self.auth_config.user_token

            # Get unified auth context (handles OBO → PAT with group_id → SPN)
            auth = await get_auth_context(user_token=user_token)

            if not auth:
                return None, "No authentication method available"

            # Return headers from auth context
            return auth.get_headers(), None

        except Exception as e:
            logger.error(f"Error getting auth headers: {e}")
            return None, str(e)

    async def _make_url(self, path: str) -> str:
        """Construct full URL from path."""
        host = await self._get_host()
        if not host.startswith("https://"):
            host = f"https://{host}"
        return f"{host}{path}"

    def _is_agentbricks_endpoint(self, endpoint_data: Dict[str, Any]) -> bool:
        """
        Determine if an endpoint is an AgentBricks endpoint.

        AgentBricks endpoints typically have:
        - task: "llm/v1/chat" or similar
        - Specific naming patterns or tags

        Args:
            endpoint_data: Endpoint data from API

        Returns:
            True if this is an AgentBricks endpoint
        """
        # Check for AgentBricks-specific indicators
        config = endpoint_data.get("config", {})

        # AgentBricks endpoints often use served_entities with foundation_model_name
        # or have specific task types
        served_entities = config.get("served_entities", [])
        for entity in served_entities:
            # AgentBricks endpoints may have external models or specific patterns
            if entity.get("external_model"):
                return True
            if entity.get("foundation_model_name"):
                # Check if it's a custom agent configuration
                workload_size = entity.get("workload_size")
                if workload_size:  # AgentBricks typically have workload sizing
                    return True

        # Check tags for AgentBricks indicators
        tags = endpoint_data.get("tags", [])
        for tag in tags:
            if isinstance(tag, dict):
                key = tag.get("key", "").lower()
                value = tag.get("value", "").lower()
                if "agentbricks" in key or "agentbricks" in value:
                    return True
                if "agent" in key or "mosaic" in key:
                    return True

        # Check endpoint name patterns (AgentBricks often start with specific prefixes)
        name = endpoint_data.get("name", "")
        if name.startswith("mas-") or "agent" in name.lower():
            return True

        return False

    async def get_endpoints(
        self,
        request: AgentBricksEndpointsRequest
    ) -> AgentBricksEndpointsResponse:
        """
        Fetch available AgentBricks endpoints with optional filtering.

        Args:
            request: Request with filtering parameters

        Returns:
            AgentBricksEndpointsResponse containing list of endpoints
        """
        try:
            headers, error = await self._get_auth_headers()
            if error:
                logger.error(f"Authentication failed: {error}")
                return AgentBricksEndpointsResponse(endpoints=[])

            # Query Databricks serving endpoints API
            url = await self._make_url("/api/2.0/serving-endpoints")
            logger.info(f"Fetching serving endpoints from: {url}")

            response = self._session.get(url, headers=headers, timeout=30)

            if response.status_code == 403:
                logger.error(f"Permission denied: {response.text}")
                return AgentBricksEndpointsResponse(endpoints=[])

            response.raise_for_status()
            data = response.json()

            # Parse endpoints
            all_endpoints = []
            endpoints_list = data.get("endpoints", [])

            for endpoint_data in endpoints_list:
                # Filter for AgentBricks endpoints only
                if not self._is_agentbricks_endpoint(endpoint_data):
                    continue

                # Extract endpoint information
                endpoint_id = endpoint_data.get("id", "")
                endpoint_name = endpoint_data.get("name", "")

                # Parse state
                state_data = endpoint_data.get("state", {})
                state_str = state_data.get("ready") if isinstance(state_data, dict) else None
                if state_str == "READY":
                    state_str = "READY"
                elif state_str == "NOT_READY":
                    state_str = "NOT_UPDATING"
                else:
                    state_str = state_data.get("config_update") or "NOT_UPDATING"

                endpoint = AgentBricksEndpoint(
                    id=endpoint_id,
                    name=endpoint_name,
                    creator=endpoint_data.get("creator"),
                    creation_timestamp=endpoint_data.get("creation_timestamp"),
                    last_updated_timestamp=endpoint_data.get("last_updated_timestamp"),
                    state=state_str,
                    config=endpoint_data.get("config"),
                    tags=endpoint_data.get("tags", []),
                    task=endpoint_data.get("task"),
                    endpoint_type=endpoint_data.get("endpoint_type")
                )
                all_endpoints.append(endpoint)

            # Apply filtering
            filtered_endpoints = all_endpoints
            filtered = False

            # Filter by ready status
            if request.ready_only:
                filtered_endpoints = [
                    ep for ep in filtered_endpoints
                    if ep.state == "READY"
                ]
                filtered = True

            # Filter by specific endpoint IDs
            if request.endpoint_ids:
                filtered_endpoints = [
                    ep for ep in filtered_endpoints
                    if ep.id in request.endpoint_ids
                ]
                filtered = True

            # Filter by search query
            if request.search_query:
                search_lower = request.search_query.lower()
                filtered_endpoints = [
                    ep for ep in filtered_endpoints
                    if search_lower in ep.name.lower() or
                       (ep.creator and search_lower in ep.creator.lower())
                ]
                filtered = True

            # Filter by creator
            if request.creator_filter:
                filtered_endpoints = [
                    ep for ep in filtered_endpoints
                    if ep.creator and request.creator_filter.lower() in ep.creator.lower()
                ]
                filtered = True

            logger.info(f"Found {len(filtered_endpoints)} AgentBricks endpoints{' (filtered)' if filtered else ''}")

            return AgentBricksEndpointsResponse(
                endpoints=filtered_endpoints,
                total_count=len(filtered_endpoints),
                filtered=filtered
            )

        except Exception as e:
            logger.error(f"Error fetching AgentBricks endpoints: {e}")
            return AgentBricksEndpointsResponse(endpoints=[])

    async def query_endpoint(
        self,
        request: AgentBricksQueryRequest
    ) -> AgentBricksQueryResponse:
        """
        Query an AgentBricks endpoint with messages.

        AgentBricks uses a different API format than standard LLMs:
        - Uses 'input' field instead of 'messages'
        - Input format: [{"role": "user", "content": "query"}]

        Args:
            request: Query request with endpoint name and messages

        Returns:
            AgentBricksQueryResponse with the result
        """
        try:
            headers, error = await self._get_auth_headers()
            if error:
                logger.error(f"Authentication failed: {error}")
                return AgentBricksQueryResponse(
                    response="",
                    status=AgentBricksQueryStatus.FAILED,
                    error=f"Authentication failed: {error}"
                )

            # Build endpoint URL
            url = await self._make_url(f"/serving-endpoints/{request.endpoint_name}/invocations")
            logger.info(f"Querying AgentBricks endpoint: {url}")

            # Convert messages to AgentBricks input format
            input_messages = [
                {"role": msg.role, "content": msg.content}
                for msg in request.messages
            ]

            # Build request payload
            payload = {
                "input": input_messages
            }

            # Add custom inputs if provided
            if request.custom_inputs:
                payload.update(request.custom_inputs)

            # Add streaming flag if requested
            if request.stream:
                payload["stream"] = True

            # Send request
            logger.debug(f"AgentBricks request payload: {payload}")
            response = self._session.post(url, headers=headers, json=payload, timeout=120)

            if response.status_code != 200:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                logger.error(f"AgentBricks query failed: {error_msg}")
                return AgentBricksQueryResponse(
                    response="",
                    status=AgentBricksQueryStatus.FAILED,
                    error=error_msg
                )

            response.raise_for_status()
            result = response.json()

            # Extract response content
            # AgentBricks typically returns: {"choices": [{"message": {"content": "..."}}]}
            # or similar OpenAI-compatible format
            response_text = ""
            usage_info = None

            if isinstance(result, dict):
                # Try OpenAI-compatible format first
                if "choices" in result:
                    choices = result.get("choices", [])
                    if choices and len(choices) > 0:
                        first_choice = choices[0]
                        message = first_choice.get("message", {})
                        response_text = message.get("content", "")

                # Try direct response field
                if not response_text and "response" in result:
                    response_text = result.get("response", "")

                # Try predictions format
                if not response_text and "predictions" in result:
                    predictions = result.get("predictions", [])
                    if predictions and len(predictions) > 0:
                        response_text = predictions[0]

                # Extract usage information
                if "usage" in result:
                    usage_info = result.get("usage")

            if not response_text and isinstance(result, str):
                response_text = result

            # Get trace if requested
            trace_info = None
            if request.return_trace and isinstance(result, dict):
                trace_info = result.get("trace") or result.get("metadata")

            logger.info(f"AgentBricks query completed successfully")

            return AgentBricksQueryResponse(
                response=response_text or str(result),
                status=AgentBricksQueryStatus.SUCCESS,
                trace=trace_info,
                usage=usage_info
            )

        except Exception as e:
            logger.error(f"Error querying AgentBricks endpoint: {e}")
            return AgentBricksQueryResponse(
                response="",
                status=AgentBricksQueryStatus.FAILED,
                error=str(e)
            )

    async def execute_query(
        self,
        request: AgentBricksExecutionRequest
    ) -> AgentBricksExecutionResponse:
        """
        Execute a simplified query to an AgentBricks endpoint.

        Args:
            request: Execution request with question

        Returns:
            AgentBricksExecutionResponse with the result
        """
        try:
            # Convert question to message format
            messages = [
                AgentBricksMessage(role="user", content=request.question)
            ]

            # Create query request
            query_request = AgentBricksQueryRequest(
                endpoint_name=request.endpoint_name,
                messages=messages,
                custom_inputs=request.custom_inputs,
                return_trace=request.return_trace
            )

            # Execute query
            query_response = await self.query_endpoint(query_request)

            # Convert to execution response
            return AgentBricksExecutionResponse(
                endpoint_name=request.endpoint_name,
                status=query_response.status,
                result=query_response.response if query_response.status == AgentBricksQueryStatus.SUCCESS else None,
                error=query_response.error,
                trace=query_response.trace
            )

        except Exception as e:
            logger.error(f"Error executing AgentBricks query: {e}")
            return AgentBricksExecutionResponse(
                endpoint_name=request.endpoint_name,
                status=AgentBricksQueryStatus.FAILED,
                error=str(e)
            )

    def __del__(self):
        """Cleanup session on deletion."""
        if self._session:
            self._session.close()
