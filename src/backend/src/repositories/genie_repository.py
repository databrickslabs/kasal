"""
Genie Repository Layer

Handles all communication with Databricks Genie API.
Implements authentication hierarchy: OBO -> PAT from DB -> PAT from environment.
"""

import asyncio
import logging
import os
import time
from typing import Optional, Dict, Any, List, Tuple
import httpx
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.schemas.genie import (
    GenieSpace,
    GenieSpacesResponse,
    GenieConversation,
    GenieMessage,
    GenieMessageStatus,
    GenieQueryResult,
    GenieQueryStatus,
    GenieStartConversationRequest,
    GenieStartConversationResponse,
    GenieSendMessageRequest,
    GenieSendMessageResponse,
    GenieGetMessageStatusRequest,
    GenieGetQueryResultRequest,
    GenieExecutionRequest,
    GenieExecutionResponse,
    GenieAuthConfig
)
from src.utils.databricks_auth import (
    get_databricks_auth_headers,
    is_databricks_apps_environment,
    extract_user_token_from_request
)
from src.core.unit_of_work import UnitOfWork
from src.repositories.api_key_repository import ApiKeyRepository
from src.utils.encryption_utils import EncryptionUtils

logger = logging.getLogger(__name__)


class GenieRepository:
    """
    Repository for interacting with Databricks Genie API.
    Follows the same authentication pattern as GenieTool.
    """
    
    def __init__(self, auth_config: Optional[GenieAuthConfig] = None):
        """
        Initialize Genie Repository.
        
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
    
    @property
    def base_url(self) -> str:
        """Get the base URL for Genie API."""
        if not self.auth_config or not self.auth_config.host:
            return ""
        host = self.auth_config.host
        if host.startswith('https://'):
            host = host[8:]
        if host.endswith('/'):
            host = host[:-1]
        return f"https://{host}/api/2.0/genie"
    
    def _build_headers(self) -> Dict[str, str]:
        """Build authentication headers."""
        headers = {"Content-Type": "application/json"}
        
        if self.auth_config:
            if self.auth_config.pat_token:
                headers["Authorization"] = f"Bearer {self.auth_config.pat_token}"
            if self.auth_config.user_token:
                headers["X-Databricks-Genie-User-Token"] = self.auth_config.user_token
        
        return headers
    
    async def _get_host(self) -> str:
        """
        Get Databricks host with auto-detection.
        Priority: config -> environment -> SDK Config -> databricks_auth
        """
        if self._host:
            return self._host
        
        # Check from config
        if self.auth_config.host:
            self._host = self.auth_config.host
            logger.info(f"Using host from config: {self._host}")
            return self._host
        
        # Check environment
        databricks_host = os.getenv("DATABRICKS_HOST")
        
        # If not in environment and in Databricks Apps, try SDK Config
        if not databricks_host and is_databricks_apps_environment():
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
        Get authentication headers following the hierarchy:
        1. OBO (On-Behalf-Of) authentication using user token
        2. PAT from database
        3. PAT from environment variables
        
        Returns:
            Tuple of (headers dict, error message)
        """
        headers = {"Content-Type": "application/json"}
        
        try:
            # Priority 1: OBO authentication with user token
            if self.auth_config.use_obo and self.auth_config.user_token:
                logger.info("Attempting OBO authentication with user token")
                auth_headers, error = await get_databricks_auth_headers(
                    user_token=self.auth_config.user_token
                )
                if auth_headers and not error:
                    headers.update(auth_headers)
                    logger.info("Successfully using OBO authentication")
                    return headers, None
                else:
                    logger.warning(f"OBO authentication failed: {error}")
            
            # Priority 2: PAT from database
            try:
                async with UnitOfWork() as uow:
                    api_key_repo = ApiKeyRepository(uow.session)
                    databricks_key = await api_key_repo.get_api_key_by_service("DATABRICKS")
                    
                    if databricks_key and databricks_key.api_key:
                        decrypted_key = EncryptionUtils.decrypt_if_needed(databricks_key.api_key)
                        if decrypted_key:
                            headers["Authorization"] = f"Bearer {decrypted_key}"
                            logger.info("Using PAT from database")
                            return headers, None
            except Exception as e:
                logger.debug(f"Could not get PAT from database: {e}")
            
            # Priority 3: PAT from environment variables
            pat_token = (
                self.auth_config.pat_token or
                os.getenv("DATABRICKS_TOKEN") or
                os.getenv("DATABRICKS_API_KEY")
            )
            
            if pat_token:
                headers["Authorization"] = f"Bearer {pat_token}"
                logger.info("Using PAT from environment")
                return headers, None
            
            # Fallback: Try generic databricks_auth which has its own hierarchy
            auth_headers, error = await get_databricks_auth_headers()
            if auth_headers and not error:
                headers.update(auth_headers)
                logger.info("Using authentication from databricks_auth utility")
                return headers, None
            
            return None, "No authentication method available"
            
        except Exception as e:
            logger.error(f"Error getting auth headers: {e}")
            return None, str(e)
    
    async def _make_url(self, path: str) -> str:
        """Construct full URL from path."""
        host = await self._get_host()
        if not host.startswith("https://"):
            host = f"https://{host}"
        return f"{host}{path}"
    
    async def get_spaces(
        self, 
        search_query: Optional[str] = None, 
        space_ids: Optional[List[str]] = None,
        enabled_only: bool = True,
        page_token: Optional[str] = None,
        page_size: int = 50,
        fetch_all: bool = False
    ) -> GenieSpacesResponse:
        """
        Fetch available Genie spaces with optional filtering and pagination.
        
        Args:
            search_query: Optional search string to filter spaces by name or description
            space_ids: Optional list of specific space IDs to fetch
            enabled_only: Only return enabled spaces
            page_token: Token for fetching next page
            page_size: Number of items per page
            fetch_all: If True, fetch all pages and return complete list
        
        Returns:
            GenieSpacesResponse containing list of spaces with pagination info
        """
        try:
            headers, error = await self._get_auth_headers()
            if error:
                logger.error(f"Authentication failed: {error}")
                return GenieSpacesResponse(spaces=[])
            
            all_spaces = []
            current_token = page_token
            has_more = True
            total_fetched = 0
            max_pages_for_search = 5  # Limit pages fetched for search
            pages_fetched = 0
            
            # Only fetch all if explicitly requested, not for search
            should_fetch_all = fetch_all
            # For search, fetch limited pages to avoid timeout
            should_fetch_limited = search_query or space_ids
            
            if search_query:
                logger.info(f"Searching for spaces with query: '{search_query}' (will fetch up to {max_pages_for_search} pages)")
            
            while has_more:
                # Build URL with pagination parameters
                url = await self._make_url("/api/2.0/genie/spaces")
                params = {"page_size": page_size}
                if current_token:
                    params["page_token"] = current_token
                
                logger.info(f"Fetching Genie spaces from: {url} with params: {params}")
                
                response = self._session.get(url, headers=headers, params=params, timeout=30)
                
                if response.status_code == 403:
                    logger.error(f"Permission denied: {response.text}")
                    return GenieSpacesResponse(spaces=[])
                
                response.raise_for_status()
                data = response.json()
                
                # Parse the response - API returns {"spaces": [...], "next_page_token": "..."}
                spaces_list = []
                next_token = None
                
                if isinstance(data, dict):
                    spaces_list = data.get("spaces", [])
                    next_token = data.get("next_page_token")
                elif isinstance(data, list):
                    # Fallback for non-paginated response
                    spaces_list = data
                
                # Convert to GenieSpace objects
                for space_data in spaces_list:
                    if isinstance(space_data, dict):
                        # Extract ID - it might be under different keys
                        space_id = (
                            space_data.get("id") or 
                            space_data.get("space_id") or 
                            space_data.get("spaceId") or 
                            ""
                        )
                        
                        space = GenieSpace(
                            id=space_id,
                            name=space_data.get("name", space_data.get("title", f"Space {space_id or 'Unknown'}")),
                            description=space_data.get("description", ""),
                            type=space_data.get("type", ""),
                            enabled=space_data.get("enabled", True),
                            owner=space_data.get("owner"),
                            workspace_id=space_data.get("workspace_id")
                        )
                        all_spaces.append(space)
                
                total_fetched += len(spaces_list)
                pages_fetched += 1
                
                # Check if we should continue fetching
                if should_fetch_all:
                    # Fetch all pages if explicitly requested
                    has_more = bool(next_token)
                elif should_fetch_limited:
                    # For search, limit pages to avoid timeout
                    has_more = bool(next_token) and pages_fetched < max_pages_for_search
                else:
                    # Normal pagination - just return first page
                    has_more = False
                
                current_token = next_token
                
                # Break if we shouldn't continue
                if not has_more:
                    break
            
            # Apply filtering on complete list
            filtered_spaces = all_spaces
            filtered = False
            
            # Filter by enabled status
            if enabled_only:
                filtered_spaces = [
                    space for space in filtered_spaces 
                    if space.enabled
                ]
                filtered = True
            
            # Filter by specific space IDs if provided
            if space_ids:
                filtered_spaces = [
                    space for space in filtered_spaces 
                    if space.id in space_ids
                ]
                filtered = True
            
            # Filter by search query if provided
            if search_query:
                search_lower = search_query.lower()
                filtered_spaces = [
                    space for space in filtered_spaces
                    if (search_lower in space.name.lower() or 
                        (space.description and search_lower in space.description.lower()))
                ]
                filtered = True
            
            # Determine what token to return
            if should_fetch_all:
                # If we fetched all, no more pages
                return_token = None
            elif should_fetch_limited and pages_fetched >= max_pages_for_search:
                # If we hit the limit for search, indicate there might be more
                return_token = current_token
            else:
                # Normal pagination
                return_token = current_token if current_token else None
            
            logger.info(f"Found {len(filtered_spaces)} Genie spaces{' (filtered)' if filtered else ''}, total fetched: {total_fetched}")
            
            return GenieSpacesResponse(
                spaces=filtered_spaces,
                next_page_token=return_token,
                page_size=page_size,
                has_more=bool(return_token),
                filtered=filtered,
                total_fetched=total_fetched
            )
            
        except Exception as e:
            logger.error(f"Error fetching Genie spaces: {e}")
            return GenieSpacesResponse(spaces=[])
    
    async def get_space_details(self, space_id: str) -> Optional[GenieSpace]:
        """
        Get details for a specific Genie space.
        
        Args:
            space_id: The space ID
            
        Returns:
            GenieSpace object or None if not found
        """
        try:
            headers, error = await self._get_auth_headers()
            if error:
                logger.error(f"Authentication failed: {error}")
                return None
            
            url = await self._make_url(f"/api/2.0/genie/spaces/{space_id}")
            response = self._session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return GenieSpace(
                id=data.get("id", space_id),
                name=data.get("name", ""),
                description=data.get("description", ""),
                type=data.get("type", ""),
                enabled=data.get("enabled", True),
                owner=data.get("owner"),
                workspace_id=data.get("workspace_id")
            )
            
        except Exception as e:
            logger.error(f"Error fetching space details: {e}")
            return None
    
    async def start_conversation(
        self, 
        request: GenieStartConversationRequest
    ) -> Optional[GenieStartConversationResponse]:
        """
        Start a new Genie conversation.
        
        Args:
            request: Start conversation request
            
        Returns:
            GenieStartConversationResponse or None if failed
        """
        try:
            headers, error = await self._get_auth_headers()
            if error:
                logger.error(f"Authentication failed: {error}")
                return None
            
            url = await self._make_url(f"/api/2.0/genie/spaces/{request.space_id}/start-conversation")
            
            payload = {}
            if request.initial_message:
                payload["content"] = request.initial_message
            if request.title:
                payload["title"] = request.title
            
            response = self._session.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return GenieStartConversationResponse(
                conversation_id=data.get("conversation_id", ""),
                message_id=data.get("message_id"),
                space_id=request.space_id,
                created_at=data.get("created_at")
            )
            
        except Exception as e:
            logger.error(f"Error starting conversation: {e}")
            return None
    
    async def send_message(
        self,
        request: GenieSendMessageRequest
    ) -> Optional[GenieSendMessageResponse]:
        """
        Send a message to Genie.
        
        Args:
            request: Send message request
            
        Returns:
            GenieSendMessageResponse or None if failed
        """
        try:
            headers, error = await self._get_auth_headers()
            if error:
                logger.error(f"Authentication failed: {error}")
                return None
            
            # Start new conversation if needed
            conversation_id = request.conversation_id
            if not conversation_id:
                start_response = await self.start_conversation(
                    GenieStartConversationRequest(
                        space_id=request.space_id,
                        initial_message=request.message
                    )
                )
                if not start_response:
                    return None
                return GenieSendMessageResponse(
                    conversation_id=start_response.conversation_id,
                    message_id=start_response.message_id or "",
                    status=GenieMessageStatus.RUNNING
                )
            
            # Send message to existing conversation
            url = await self._make_url(
                f"/api/2.0/genie/spaces/{request.space_id}/conversations/{conversation_id}/messages"
            )
            
            payload = {
                "content": request.message
            }
            if request.attachments:
                payload["attachments"] = request.attachments
            
            response = self._session.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return GenieSendMessageResponse(
                conversation_id=conversation_id,
                message_id=data.get("id", ""),
                status=GenieMessageStatus(data.get("status", "RUNNING")),
                response=data.get("content")
            )
            
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return None
    
    async def get_message_status(
        self,
        request: GenieGetMessageStatusRequest
    ) -> Optional[GenieMessageStatus]:
        """
        Get the status of a message.
        
        Args:
            request: Get message status request
            
        Returns:
            GenieMessageStatus or None if failed
        """
        try:
            headers, error = await self._get_auth_headers()
            if error:
                logger.error(f"Authentication failed: {error}")
                return None
            
            url = await self._make_url(
                f"/api/2.0/genie/spaces/{request.space_id}/conversations/"
                f"{request.conversation_id}/messages/{request.message_id}"
            )
            
            response = self._session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            status_str = data.get("status", "RUNNING")
            return GenieMessageStatus(status_str)
            
        except Exception as e:
            logger.error(f"Error getting message status: {e}")
            return None
    
    async def get_query_result(
        self,
        request: GenieGetQueryResultRequest
    ) -> Optional[GenieQueryResult]:
        """
        Get the query result for a message.
        
        Args:
            request: Get query result request
            
        Returns:
            GenieQueryResult or None if failed
        """
        try:
            headers, error = await self._get_auth_headers()
            if error:
                logger.error(f"Authentication failed: {error}")
                return None
            
            url = await self._make_url(
                f"/api/2.0/genie/spaces/{request.space_id}/conversations/"
                f"{request.conversation_id}/messages/{request.message_id}/query-result"
            )
            
            response = self._session.get(url, headers=headers, timeout=30)
            
            if response.status_code == 404:
                logger.debug("Query result not ready yet")
                return GenieQueryResult(status=GenieQueryStatus.PENDING)
            
            response.raise_for_status()
            data = response.json()
            
            # Parse the query result
            result = GenieQueryResult(
                query_id=data.get("query_id"),
                status=GenieQueryStatus(data.get("status", "RUNNING")),
                sql=data.get("sql_query") or data.get("query"),
                error=data.get("error_message") or data.get("error")
            )
            
            # Extract result data
            if "result" in data:
                result.result = data["result"]
            
            if "data" in data:
                result.data = data["data"]
                result.row_count = len(data["data"])
            
            if "columns" in data:
                result.columns = data["columns"]
            
            if "execution_time" in data:
                result.execution_time = data["execution_time"]
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting query result: {e}")
            return None
    
    async def execute_query(
        self,
        request: GenieExecutionRequest
    ) -> GenieExecutionResponse:
        """
        Execute a complete Genie query workflow.
        Sends a message and waits for the result.
        
        Args:
            request: Execution request
            
        Returns:
            GenieExecutionResponse with the result
        """
        try:
            # Send the message
            send_response = await self.send_message(
                GenieSendMessageRequest(
                    space_id=request.space_id,
                    conversation_id=request.conversation_id,
                    message=request.question
                )
            )
            
            if not send_response:
                return GenieExecutionResponse(
                    conversation_id="",
                    message_id="",
                    status=GenieQueryStatus.FAILED,
                    error="Failed to send message to Genie"
                )
            
            # Wait for the message to complete
            start_time = time.time()
            timeout = request.timeout or 120
            retry_count = 0
            max_retries = request.max_retries or 3
            
            while (time.time() - start_time) < timeout:
                # Check message status
                status = await self.get_message_status(
                    GenieGetMessageStatusRequest(
                        space_id=request.space_id,
                        conversation_id=send_response.conversation_id,
                        message_id=send_response.message_id
                    )
                )
                
                if status == GenieMessageStatus.COMPLETED:
                    # Get the query result
                    query_result = await self.get_query_result(
                        GenieGetQueryResultRequest(
                            space_id=request.space_id,
                            conversation_id=send_response.conversation_id,
                            message_id=send_response.message_id
                        )
                    )
                    
                    if query_result and query_result.status == GenieQueryStatus.SUCCESS:
                        # Extract response text
                        result_text = self._extract_response_text(query_result)
                        
                        return GenieExecutionResponse(
                            conversation_id=send_response.conversation_id,
                            message_id=send_response.message_id,
                            status=GenieQueryStatus.SUCCESS,
                            result=result_text,
                            query_result=query_result
                        )
                    elif query_result and query_result.status == GenieQueryStatus.FAILED:
                        return GenieExecutionResponse(
                            conversation_id=send_response.conversation_id,
                            message_id=send_response.message_id,
                            status=GenieQueryStatus.FAILED,
                            error=query_result.error or "Query failed"
                        )
                
                elif status == GenieMessageStatus.FAILED:
                    retry_count += 1
                    if retry_count >= max_retries:
                        return GenieExecutionResponse(
                            conversation_id=send_response.conversation_id,
                            message_id=send_response.message_id,
                            status=GenieQueryStatus.FAILED,
                            error="Message processing failed"
                        )
                    logger.warning(f"Message failed, retry {retry_count}/{max_retries}")
                
                # Wait before next check
                await asyncio.sleep(2)
            
            # Timeout
            return GenieExecutionResponse(
                conversation_id=send_response.conversation_id,
                message_id=send_response.message_id,
                status=GenieQueryStatus.FAILED,
                error=f"Query timed out after {timeout} seconds"
            )
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return GenieExecutionResponse(
                conversation_id=request.conversation_id or "",
                message_id="",
                status=GenieQueryStatus.FAILED,
                error=str(e)
            )
    
    def _extract_response_text(self, query_result: GenieQueryResult) -> str:
        """
        Extract readable response text from query result.
        
        Args:
            query_result: The query result
            
        Returns:
            Formatted response text
        """
        response_parts = []
        
        # Add main result if available
        if query_result.result:
            if isinstance(query_result.result, str):
                response_parts.append(query_result.result)
            elif isinstance(query_result.result, dict):
                response_parts.append(str(query_result.result))
        
        # Add SQL query if available
        if query_result.sql:
            response_parts.append(f"SQL Query:\n{query_result.sql}")
        
        # Add data summary if available
        if query_result.data and query_result.columns:
            response_parts.append(f"Results: {query_result.row_count} rows")
            # Add first few rows as preview
            if len(query_result.data) > 0:
                preview_rows = query_result.data[:5]
                response_parts.append("Preview:")
                for row in preview_rows:
                    response_parts.append(str(row))
        
        return "\n\n".join(response_parts) if response_parts else "No response content found"
    
    def __del__(self):
        """Cleanup session on deletion."""
        if self._session:
            self._session.close()