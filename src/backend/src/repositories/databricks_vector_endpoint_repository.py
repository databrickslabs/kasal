"""
Repository for Databricks Vector Search Endpoint operations.

This repository handles all interactions with Databricks Vector Search endpoints,
following the clean architecture pattern.
"""
from typing import Optional, List, Dict, Any
import aiohttp
# No longer using VectorSearchClient - using REST API directly
from src.core.logger import LoggerManager
from src.schemas.databricks_vector_endpoint import (
    EndpointCreate,
    EndpointInfo,
    EndpointResponse,
    EndpointListResponse,
    EndpointState,
    EndpointType
)
from src.repositories.databricks_auth_helper import DatabricksAuthHelper

logger = LoggerManager.get_instance().system


class DatabricksVectorEndpointRepository:
    """Repository for managing Databricks Vector Search endpoints."""
    
    def __init__(self, workspace_url: str):
        """
        Initialize the repository.
        
        Args:
            workspace_url: Databricks workspace URL
        """
        self.workspace_url = workspace_url
    
    async def _get_auth_token(self, user_token: Optional[str] = None) -> str:
        """
        Get authentication token for REST API calls.
        
        Follows authentication priority:
        1. OBO (On-Behalf-Of) with user token
        2. PAT from database (encrypted storage)
        3. PAT from environment variables
        
        Args:
            user_token: Optional user token for OBO authentication
            
        Returns:
            Authentication token
            
        Raises:
            Exception: If no authentication token can be obtained
        """
        return await DatabricksAuthHelper.get_auth_token(
            workspace_url=self.workspace_url,
            user_token=user_token
        )
    
    async def create_endpoint(
        self,
        endpoint_data: EndpointCreate,
        user_token: Optional[str] = None
    ) -> EndpointResponse:
        """
        Create a new vector search endpoint using REST API.
        
        Args:
            endpoint_data: Endpoint creation parameters
            user_token: Optional user token for OBO authentication
            
        Returns:
            EndpointResponse with creation result
        """
        try:
            logger.info(f"[DEBUG] Creating endpoint: {endpoint_data.name}")
            logger.info(f"[DEBUG] Endpoint type: {endpoint_data.endpoint_type}")
            logger.info(f"[DEBUG] User token present: {bool(user_token)}")
            logger.info(f"[DEBUG] Workspace URL: {self.workspace_url}")
            
            # Get authentication token
            auth_token = await self._get_auth_token(user_token)
            
            # Prepare the REST API endpoint
            url = f"{self.workspace_url}/api/2.0/vector-search/endpoints"
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
            
            # Prepare the payload
            payload = {
                "name": endpoint_data.name,
                "endpoint_type": endpoint_data.endpoint_type.value if hasattr(endpoint_data.endpoint_type, 'value') else endpoint_data.endpoint_type
            }
            
            logger.info(f"Creating endpoint {endpoint_data.name} via REST API")
            
            # Make the REST API call
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    response_text = await response.text()
                    
                    if response.status in [200, 201]:
                        logger.info(f"Successfully created endpoint: {endpoint_data.name}")
                        
                        # Get endpoint info to return
                        endpoint_info = await self.get_endpoint(endpoint_data.name, user_token)
                        
                        return EndpointResponse(
                            success=True,
                            endpoint=endpoint_info.endpoint,
                            message=f"Endpoint {endpoint_data.name} created successfully"
                        )
                    elif response.status == 409 or "already exists" in response_text.lower():
                        logger.info(f"Endpoint {endpoint_data.name} already exists")
                        endpoint_info = await self.get_endpoint(endpoint_data.name, user_token)
                        return EndpointResponse(
                            success=True,
                            endpoint=endpoint_info.endpoint,
                            message=f"Endpoint {endpoint_data.name} already exists"
                        )
                    else:
                        error_msg = f"Failed to create endpoint. Status: {response.status}, Response: {response_text}"
                        logger.error(error_msg)
                        return EndpointResponse(
                            success=False,
                            error=error_msg,
                            message=f"Failed to create endpoint: {error_msg}"
                        )
            
        except Exception as e:
            logger.error(f"Failed to create endpoint {endpoint_data.name}: {e}")
            return EndpointResponse(
                success=False,
                error=str(e),
                message=f"Failed to create endpoint: {str(e)}"
            )
    
    async def get_endpoint(
        self,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> EndpointResponse:
        """
        Get information about a specific endpoint using REST API.
        
        Args:
            endpoint_name: Name of the endpoint
            user_token: Optional user token for OBO authentication
            
        Returns:
            EndpointResponse with endpoint information
        """
        try:
            # Get authentication token
            auth_token = await self._get_auth_token(user_token)
            
            # Prepare the REST API endpoint
            url = f"{self.workspace_url}/api/2.0/vector-search/endpoints/{endpoint_name}"
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
            
            logger.info(f"Getting endpoint {endpoint_name} via REST API")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        endpoint_status = data.get('endpoint_status', {})
                        
                        endpoint_info = EndpointInfo(
                            name=endpoint_name,
                            endpoint_type=EndpointType(data.get('endpoint_type', 'STANDARD')),
                            state=EndpointState(endpoint_status.get('state', 'UNKNOWN')),
                            ready=endpoint_status.get('state') == 'ONLINE'
                        )
                        
                        return EndpointResponse(
                            success=True,
                            endpoint=endpoint_info,
                            message=f"Endpoint {endpoint_name} retrieved successfully"
                        )
                    elif response.status == 404:
                        return EndpointResponse(
                            success=False,
                            endpoint=EndpointInfo(
                                name=endpoint_name,
                                state=EndpointState.NOT_FOUND,
                                ready=False
                            ),
                            message=f"Endpoint {endpoint_name} not found"
                        )
                    else:
                        error_text = await response.text()
                        error_msg = f"API returned status {response.status}: {error_text}"
                        logger.error(error_msg)
                        return EndpointResponse(
                            success=False,
                            error=error_msg,
                            message=f"Failed to get endpoint: {error_msg}"
                        )
                        
        except Exception as e:
            logger.error(f"Failed to get endpoint {endpoint_name}: {e}")
            return EndpointResponse(
                success=False,
                error=str(e),
                message=f"Failed to get endpoint: {str(e)}"
            )
    
    async def list_endpoints(
        self,
        user_token: Optional[str] = None
    ) -> EndpointListResponse:
        """
        List all vector search endpoints using REST API.
        
        Args:
            user_token: Optional user token for OBO authentication
            
        Returns:
            EndpointListResponse with list of endpoints
        """
        try:
            # Get authentication token
            auth_token = await self._get_auth_token(user_token)
            
            # Prepare the REST API endpoint
            url = f"{self.workspace_url}/api/2.0/vector-search/endpoints"
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
            
            logger.info("Listing all endpoints via REST API")
            
            # Make the REST API call
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        endpoints_data = data.get("endpoints", [])
                        
                        # Convert to EndpointInfo objects
                        endpoint_infos = []
                        for ep in endpoints_data:
                            endpoint_status = ep.get('endpoint_status', {})
                            endpoint_infos.append(EndpointInfo(
                                name=ep.get("name", ""),
                                endpoint_type=EndpointType(ep.get("endpoint_type", "STANDARD")),
                                state=EndpointState(endpoint_status.get("state", "UNKNOWN")),
                                ready=endpoint_status.get("state") == "ONLINE"
                            ))
                        
                        return EndpointListResponse(
                            success=True,
                            endpoints=endpoint_infos,
                            message=f"Found {len(endpoint_infos)} endpoints"
                        )
                    else:
                        error_text = await response.text()
                        error_msg = f"API returned status {response.status}: {error_text}"
                        logger.error(error_msg)
                        return EndpointListResponse(
                            success=False,
                            endpoints=[],
                            message=f"Failed to list endpoints: {error_msg}"
                        )
            
        except Exception as e:
            logger.error(f"Failed to list endpoints: {e}")
            return EndpointListResponse(
                success=False,
                endpoints=[],
                message=f"Failed to list endpoints: {str(e)}"
            )
    
    async def delete_endpoint(
        self,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> EndpointResponse:
        """
        Delete a vector search endpoint using REST API.
        
        Args:
            endpoint_name: Name of the endpoint to delete
            user_token: Optional user token for OBO authentication
            
        Returns:
            EndpointResponse with deletion result
        """
        try:
            # Check if endpoint has any indexes first
            from src.repositories.databricks_vector_index_repository import DatabricksVectorIndexRepository
            index_repo = DatabricksVectorIndexRepository(self.workspace_url)
            indexes_response = await index_repo.list_indexes(endpoint_name, user_token)
            
            if indexes_response.success and indexes_response.indexes:
                return EndpointResponse(
                    success=False,
                    error="Endpoint has indexes",
                    message=f"Cannot delete endpoint {endpoint_name}: it has {len(indexes_response.indexes)} indexes"
                )
            
            # Get authentication token
            auth_token = await self._get_auth_token(user_token)
            
            # Prepare the REST API endpoint
            url = f"{self.workspace_url}/api/2.0/vector-search/endpoints/{endpoint_name}"
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
            
            logger.info(f"Deleting endpoint {endpoint_name} via REST API")
            
            # Make the REST API call
            async with aiohttp.ClientSession() as session:
                async with session.delete(url, headers=headers) as response:
                    if response.status in [200, 204]:
                        logger.info(f"Successfully deleted endpoint: {endpoint_name}")
                        return EndpointResponse(
                            success=True,
                            message=f"Endpoint {endpoint_name} deleted successfully"
                        )
                    elif response.status == 404:
                        return EndpointResponse(
                            success=False,
                            error="Endpoint not found",
                            message=f"Endpoint {endpoint_name} not found"
                        )
                    else:
                        error_text = await response.text()
                        error_msg = f"Failed to delete endpoint. Status: {response.status}, Response: {error_text}"
                        logger.error(error_msg)
                        return EndpointResponse(
                            success=False,
                            error=error_msg,
                            message=f"Failed to delete endpoint: {error_msg}"
                        )
            
        except Exception as e:
            logger.error(f"Failed to delete endpoint {endpoint_name}: {e}")
            return EndpointResponse(
                success=False,
                error=str(e),
                message=f"Failed to delete endpoint: {str(e)}"
            )
    
    async def wait_for_endpoint_ready(
        self,
        endpoint_name: str,
        max_wait_seconds: int = 300,
        user_token: Optional[str] = None
    ) -> EndpointResponse:
        """
        Wait for an endpoint to become ready.
        
        Args:
            endpoint_name: Name of the endpoint
            max_wait_seconds: Maximum time to wait
            user_token: Optional user token for OBO authentication
            
        Returns:
            EndpointResponse with final endpoint state
        """
        import asyncio
        
        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < max_wait_seconds:
            response = await self.get_endpoint(endpoint_name, user_token)
            
            if response.success and response.endpoint:
                if response.endpoint.state == EndpointState.ONLINE:
                    return response
                elif response.endpoint.state == EndpointState.FAILED:
                    return EndpointResponse(
                        success=False,
                        endpoint=response.endpoint,
                        error="Endpoint provisioning failed",
                        message=f"Endpoint {endpoint_name} failed to provision"
                    )
            
            # Wait before checking again
            await asyncio.sleep(5)
        
        # Timeout
        return EndpointResponse(
            success=False,
            error="Timeout",
            message=f"Endpoint {endpoint_name} did not become ready within {max_wait_seconds} seconds"
        )
    
    async def get_endpoint_status(
        self,
        endpoint_name: str,
        user_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get endpoint status using REST API.
        
        Args:
            endpoint_name: Name of the endpoint
            user_token: Optional user token for OBO authentication
            
        Returns:
            Dictionary with endpoint information
        """
        try:
            # Use get_endpoint which already uses REST API
            response = await self.get_endpoint(endpoint_name, user_token)
            
            if response.success and response.endpoint:
                return {
                    "success": True,
                    "endpoint": {
                        "name": response.endpoint.name,
                        "endpoint_type": response.endpoint.endpoint_type.value,
                        "endpoint_status": {
                            "state": response.endpoint.state.value,
                            "ready": response.endpoint.ready
                        }
                    },
                    "status": response.endpoint.state.value.lower(),
                    "message": "Endpoint status retrieved successfully"
                }
            else:
                if response.endpoint and response.endpoint.state == EndpointState.NOT_FOUND:
                    return {
                        "success": False,
                        "message": f"Endpoint {endpoint_name} not found",
                        "status": "not_found",
                        "error": response.error or "Endpoint not found"
                    }
                else:
                    return {
                        "success": False,
                        "message": response.message,
                        "error": response.error
                    }
            
        except Exception as e:
            logger.error(f"Failed to get endpoint status for {endpoint_name}: {e}")
            return {
                "success": False,
                "message": f"Failed to get endpoint status: {str(e)}",
                "error": str(e)
            }