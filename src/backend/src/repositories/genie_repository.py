"""
Genie Repository Layer

Handles all communication with Databricks Genie API.
Uses unified authentication from get_auth_context() which implements:
  1. OBO (On-Behalf-Of) with user token
  2. PAT from database with group_id filtering
  3. Service Principal OAuth (SPN)
"""

import asyncio
import logging
import os
import time
from typing import Optional, Dict, Any, List, Tuple
import httpx

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
from src.utils.databricks_auth import get_auth_context
from src.utils.telemetry import get_user_agent_header, KasalProduct

logger = logging.getLogger(__name__)


# The Genie spaces API has NO server-side name search, so filtering by name needs
# the full list. Walking all ~50 pages on every keystroke (and concurrently) is
# what made search slow and triggered HTTP 429. Instead we fetch the full list
# ONCE per workspace host, cache it briefly, and filter in memory. Keyed by host
# (the visible set is workspace-level for this app's PAT/SP auth); short TTL keeps
# it fresh. The per-host lock serializes the (one-time) full fetch so concurrent
# searches don't each scan.
_SPACES_CACHE: Dict[str, Dict[str, Any]] = {}
_SPACES_LOCKS: Dict[str, asyncio.Lock] = {}
_SPACES_CACHE_TTL = 300          # seconds — full list cached this long
_SPACES_PARTIAL_TTL = 30         # seconds — shorter when the walk didn't finish (retry sooner)
_FULL_FETCH_PAGE_SIZE = 100      # bigger pages on the one-time full walk = far fewer round-trips
_SPACES_MAX_RETRIES = 5          # 429 backoff attempts per page


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
        self._client: Optional[httpx.AsyncClient] = None
        self._setup_client()

    def _setup_client(self):
        """Setup async HTTP client with retry logic."""
        transport = httpx.AsyncHTTPTransport(retries=3)
        self._client = httpx.AsyncClient(transport=transport, timeout=30.0)
    
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
        
        # Check from config (auth_config is optional — may be None when the
        # repository relies on get_auth_context() for auth/host instead).
        if self.auth_config and self.auth_config.host:
            host = self.auth_config.host
            if host.startswith('https://'):
                host = host[8:]
            if host.endswith('/'):
                host = host[:-1]
            self._host = host
            logger.info(f"Using host from config: {self._host}")
            return self._host
        
        # Use unified auth to get host
        from src.utils.databricks_auth import get_auth_context
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
            from src.utils.databricks_auth import get_auth_context

            # Extract user token if available (for OBO)
            user_token = None
            if self.auth_config and self.auth_config.use_obo and self.auth_config.user_token:
                user_token = self.auth_config.user_token

            # Get unified auth context (handles OBO → PAT with group_id → SPN)
            auth = await get_auth_context(user_token=user_token)

            if not auth:
                return None, "No authentication method available"

            # Return headers from auth context with telemetry
            headers = auth.get_headers()
            headers.update(get_user_agent_header(KasalProduct.GENIE))  # Kasal_genie User-Agent
            return headers, None

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
            
            # Determine fetching strategy:
            # - search/filter (or fetch_all): need the FULL list — served from the
            #   per-host cache (fetched once), then filtered in memory.
            # - default: fetch one page and return next_page_token for pagination.
            should_fetch_all = fetch_all or bool(search_query) or bool(space_ids)

            if should_fetch_all:
                if search_query:
                    logger.info(f"Searching spaces for '{search_query}' via cached full list (in-memory filter)")
                all_spaces = await self._get_all_spaces_cached(headers)
                current_token = None
                total_fetched = len(all_spaces)
            else:
                all_spaces, current_token = await self._fetch_spaces_page(
                    headers, page_size, page_token
                )
                total_fetched = len(all_spaces)

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
                # If we fetched all pages, no more to paginate
                return_token = None
            else:
                # Normal pagination - pass through the next_page_token
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

    def _space_from_data(self, space_data: dict) -> GenieSpace:
        """Convert one API space dict into a GenieSpace (with deep link)."""
        space_id = (
            space_data.get("id")
            or space_data.get("space_id")
            or space_data.get("spaceId")
            or ""
        )
        space_url = (
            f"https://{self._host}/genie/rooms/{space_id}"
            if self._host and space_id else None
        )
        return GenieSpace(
            id=space_id,
            name=space_data.get("name", space_data.get("title", f"Space {space_id or 'Unknown'}")),
            description=space_data.get("description", ""),
            type=space_data.get("type", ""),
            enabled=space_data.get("enabled", True),
            owner=space_data.get("owner"),
            workspace_id=space_data.get("workspace_id"),
            url=space_url,
        )

    async def _fetch_spaces_page(
        self, headers: dict, page_size: int, token: Optional[str]
    ) -> Tuple[List[GenieSpace], Optional[str]]:
        """Fetch ONE page of spaces -> (spaces, next_page_token).

        Retries on HTTP 429 (rate limit) with backoff — that's what previously
        killed the full walk mid-way so only a partial list got cached. Falls
        back to a conservative page_size if the larger one is rejected (400)."""
        url = await self._make_url("/api/2.0/genie/spaces")
        size = page_size
        attempt = 0
        while True:
            params: Dict[str, Any] = {"page_size": size}
            if token:
                params["page_token"] = token
            response = await self._client.get(url, headers=headers, params=params)

            if response.status_code == 403:
                logger.error(f"Permission denied: {response.text}")
                return [], None
            if response.status_code == 429 and attempt < _SPACES_MAX_RETRIES:
                retry_after = (response.headers.get("Retry-After") or "").strip()
                delay = float(retry_after) if retry_after.isdigit() else min(2 ** attempt, 8)
                logger.warning(
                    f"Genie spaces rate-limited (429); backing off {delay}s "
                    f"(attempt {attempt + 1}/{_SPACES_MAX_RETRIES})"
                )
                await asyncio.sleep(delay)
                attempt += 1
                continue
            if response.status_code == 400 and size != 50:
                # Larger page_size not accepted — retry this page at the safe size.
                logger.warning(f"Genie spaces 400 at page_size={size}; retrying at 50")
                size = 50
                continue

            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict):
                spaces_list = data.get("spaces", []) or []
                next_token = data.get("next_page_token")
            elif isinstance(data, list):
                spaces_list, next_token = data, None
            else:
                spaces_list, next_token = [], None
            spaces = [self._space_from_data(s) for s in spaces_list if isinstance(s, dict)]
            return spaces, next_token

    async def _fetch_all_spaces_uncached(
        self, headers: dict
    ) -> Tuple[List[GenieSpace], bool]:
        """Walk every page of spaces sequentially -> (spaces, complete). On an
        unrecoverable error mid-walk, returns what was fetched with complete=False
        so the caller can cache the partial list (and re-attempt soon) instead of
        discarding everything and re-walking from scratch on the next search."""
        all_spaces: List[GenieSpace] = []
        token: Optional[str] = None
        while True:
            try:
                page, token = await self._fetch_spaces_page(headers, _FULL_FETCH_PAGE_SIZE, token)
            except Exception as e:  # noqa: BLE001 — keep partial results
                logger.warning(
                    f"Genie spaces walk interrupted after {len(all_spaces)} spaces: {e}"
                )
                return all_spaces, False
            all_spaces.extend(page)
            if not token:
                return all_spaces, True

    async def _get_all_spaces_cached(self, headers: dict) -> List[GenieSpace]:
        """Full spaces list for this host, fetched once and cached. A per-host lock
        serializes the one-time full walk so concurrent searches don't each scan
        (which caused 429). A complete list caches for the full TTL; a partial one
        (walk interrupted) caches briefly so the next search re-attempts."""
        host = await self._get_host()
        cached = _SPACES_CACHE.get(host)
        if cached and (time.time() - cached["ts"]) < cached.get("ttl", _SPACES_CACHE_TTL):
            return cached["spaces"]
        lock = _SPACES_LOCKS.setdefault(host, asyncio.Lock())
        async with lock:
            # Re-check inside the lock: another search may have just populated it.
            cached = _SPACES_CACHE.get(host)
            if cached and (time.time() - cached["ts"]) < cached.get("ttl", _SPACES_CACHE_TTL):
                return cached["spaces"]
            spaces, complete = await self._fetch_all_spaces_uncached(headers)
            ttl = _SPACES_CACHE_TTL if complete else _SPACES_PARTIAL_TTL
            _SPACES_CACHE[host] = {"spaces": spaces, "ts": time.time(), "ttl": ttl}
            logger.info(
                f"[GenieSpacesCache] cached {len(spaces)} spaces "
                f"(complete={complete}, ttl={ttl}s) for host {host}"
            )
            return spaces

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
            response = await self._client.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            # Deep link to the Genie space UI (self._host resolved by _make_url
            # above; it has no scheme). Lets the chat open the space to validate it.
            resolved_id = data.get("id", space_id)
            space_url = (
                f"https://{self._host}/genie/rooms/{resolved_id}"
                if self._host and resolved_id else None
            )
            return GenieSpace(
                id=resolved_id,
                name=data.get("name", ""),
                description=data.get("description", ""),
                type=data.get("type", ""),
                enabled=data.get("enabled", True),
                owner=data.get("owner"),
                workspace_id=data.get("workspace_id"),
                url=space_url,
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
            
            response = await self._client.post(url, headers=headers, json=payload)
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
            
            response = await self._client.post(url, headers=headers, json=payload)
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
            
            response = await self._client.get(url, headers=headers)
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
            
            response = await self._client.get(url, headers=headers)
            
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
    
    async def aclose(self):
        """Close the async HTTP client."""
        if self._client:
            await self._client.aclose()

    def __del__(self):
        """Cleanup client on deletion."""
        client = getattr(self, "_client", None)
        if client and not client.is_closed:
            try:
                asyncio.get_running_loop().create_task(client.aclose())
            except RuntimeError:
                pass