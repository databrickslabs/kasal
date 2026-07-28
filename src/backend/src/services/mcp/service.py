from typing import List, Optional, Dict, Any
import logging
import asyncio

from src.core.exceptions import (
    KasalError,
    NotFoundError,
    ConflictError,
    BadRequestError,
)

from src.repositories.mcp_repository import MCPServerRepository, MCPSettingsRepository
from src.schemas.mcp import (
    MCPServerCreate,
    MCPServerUpdate,
    MCPServerResponse,
    MCPServerListResponse,
    MCPToggleResponse,
    MCPTestConnectionRequest,
    MCPTestConnectionResponse,
    MCPSettingsResponse,
    MCPSettingsUpdate
)
from src.utils.encryption_utils import EncryptionUtils

logger = logging.getLogger(__name__)

class MCPService:
    """
    Service for MCP business logic and error handling.
    Acts as an intermediary between the API routers and the repository.
    """

    def __init__(self, session):
        """
        Initialize service with database session.

        Args:
            session: SQLAlchemy async session from dependency injection
        """
        self.server_repository = MCPServerRepository(session)
        self.settings_repository = MCPSettingsRepository(session)


    async def get_all_servers(self) -> MCPServerListResponse:
        """
        Get all MCP servers.

        Returns:
            MCPServerListResponse with list of all servers and count
        """
        servers = await self.server_repository.list()
        server_responses = []

        for server in servers:
            server_response = MCPServerResponse.model_validate(server)
            # Don't include API key in list response
            server_response.api_key = ""
            server_responses.append(server_response)

        return MCPServerListResponse(
            servers=server_responses,
            count=len(servers)
        )

    async def get_all_servers_effective(
        self, group_id: Optional[str], enabled_only: bool = False
    ) -> MCPServerListResponse:
        """
        Get MCP servers effective for a workspace (group): prefer group-specific
        entries over base entries for the same name. Return both enabled and
        disabled so admins can manage state.

        ``enabled_only`` restricts the result to enabled servers — used for
        non-admin callers, who must only see the servers a workspace admin has
        enabled (the curated allow-list), never the disabled ones.

        OPT-IN model: a globally-available base server (``group_id IS NULL``)
        that THIS workspace has not yet opted into (no override row) is reported
        with ``enabled=False`` so the workspace toggle starts off and the server
        is not usable (picker/execution) until a workspace admin enables it,
        which creates an enabled override. The base ``enabled`` flag only means
        "published / available to workspaces", not "active for this workspace".
        """
        servers = await self.server_repository.list_for_group_scope(group_id)
        # Deduplicate by name, preferring group-specific
        dedup: Dict[str, Any] = {}
        for s in servers:
            key = s.name
            if key not in dedup:
                dedup[key] = s
            else:
                # Prefer group-specific over base
                if getattr(s, 'group_id', None) and not getattr(dedup[key], 'group_id', None):
                    dedup[key] = s
        server_responses: List[MCPServerResponse] = []
        for server in dedup.values():
            # Inherited base row with no workspace override → not opted in yet.
            is_inherited_base = (
                bool(group_id) and getattr(server, "group_id", None) is None
            )
            effective_enabled = False if is_inherited_base else bool(
                getattr(server, "enabled", False)
            )
            if enabled_only and not effective_enabled:
                continue
            resp = MCPServerResponse.model_validate(server)
            resp.enabled = effective_enabled
            resp.api_key = ""  # do not include in list
            server_responses.append(resp)
        return MCPServerListResponse(servers=server_responses, count=len(server_responses))

    async def get_enabled_servers(self) -> MCPServerListResponse:
        """
        Get all enabled MCP servers.

        Returns:
            MCPServerListResponse with list of enabled servers and count
        """
        servers = await self.server_repository.find_enabled()
        server_responses = []

        for server in servers:
            server_response = MCPServerResponse.model_validate(server)
            # Don't include API key in list response
            server_response.api_key = ""
            server_responses.append(server_response)

        return MCPServerListResponse(
            servers=server_responses,
            count=len(servers)
        )

    async def get_global_servers(self) -> MCPServerListResponse:
        """
        Get all globally enabled MCP servers.

        Returns:
            MCPServerListResponse with list of globally enabled servers and count
        """
        servers = await self.server_repository.find_global_enabled()
        server_responses = []

        for server in servers:
            server_response = MCPServerResponse.model_validate(server)
            # Don't include API key in list response
            server_response.api_key = ""
            server_responses.append(server_response)

        return MCPServerListResponse(
            servers=server_responses,
            count=len(servers)
        )

    async def get_servers_by_names(self, names: List[str]) -> List[MCPServerResponse]:
        """
        Get MCP servers by a list of names.

        Args:
            names: List of server names to retrieve

        Returns:
            List of MCPServerResponse objects
        """
        if not names:
            return []
        servers = await self.server_repository.find_by_names(names)
        server_responses: List[MCPServerResponse] = []
        for server in servers:
            server_response = MCPServerResponse.model_validate(server)
            decrypted = self._decrypt_server_api_key(server)
            if decrypted is not None:
                server_response.api_key = decrypted
            server_responses.append(server_response)
        return server_responses

    async def get_servers_by_names_group_aware(self, names: List[str], group_id: Optional[str]) -> List[MCPServerResponse]:
        """
        Get MCP servers by names with group scoping (prefer group-specific over base).
        """
        if not names:
            return []
        servers = await self.server_repository.find_by_names_group_scope(names, group_id)
        # Deduplicate by name preferring group-specific
        best_by_name: Dict[str, Any] = {}
        for s in servers:
            key = s.name
            if key not in best_by_name:
                best_by_name[key] = s
            else:
                if getattr(s, 'group_id', None) and not getattr(best_by_name[key], 'group_id', None):
                    best_by_name[key] = s
        responses: List[MCPServerResponse] = []
        for server in best_by_name.values():
            resp = MCPServerResponse.model_validate(server)
            decrypted = self._decrypt_server_api_key(server)
            if decrypted is not None:
                resp.api_key = decrypted
            responses.append(resp)
        return responses

    @staticmethod
    def _decrypt_server_api_key(server) -> Optional[str]:
        """Decrypt a server's stored API key — only when its auth mode uses one.

        OBO/SPN servers authenticate with tokens, not the stored key. A row can
        still carry a leftover ``encrypted_api_key`` blob (saved before the auth
        type changed, possibly under an older encryption key); decrypting it on
        every run produced anonymous "Error decrypting value with SSH key:
        Decryption failed" errors for servers that never use the value. Skip
        those outright, and when an api_key-mode server's stored key genuinely
        cannot be decrypted, say WHICH server and how to fix it.

        Returns the decrypted key, "" when decryption failed, or None when the
        server has no usable stored key (no blob, or a token-auth server).
        """
        auth_type = getattr(server, "auth_type", None) or "api_key"
        if auth_type != "api_key" or not server.encrypted_api_key:
            return None
        try:
            decrypted = EncryptionUtils.decrypt_value(server.encrypted_api_key)
        except Exception as e:  # decrypt_value normally returns "" — belt and braces
            logger.warning(f"MCP server '{server.name}': error decrypting stored API key: {e}")
            return ""
        if not decrypted:
            logger.warning(
                f"MCP server '{server.name}': stored API key cannot be decrypted "
                "(it was encrypted under a previous encryption key) — re-save the "
                "server's API key to clear this warning."
            )
        return decrypted

    def _response_with_key(self, server) -> MCPServerResponse:
        """Build a response, decrypting the API key (best-effort)."""
        resp = MCPServerResponse.model_validate(server)
        decrypted = self._decrypt_server_api_key(server)
        if decrypted is not None:
            resp.api_key = decrypted
        return resp

    def _group_override_payload(
        self, base, group_id: str, enabled: bool
    ) -> Dict[str, Any]:
        """Clone a base server's config into a workspace-scoped override row."""
        return {
            "name": base.name,
            "server_url": base.server_url,
            "server_type": base.server_type,
            "auth_type": base.auth_type,
            "enabled": enabled,
            "global_enabled": False,
            "timeout_seconds": base.timeout_seconds,
            "max_retries": base.max_retries,
            "model_mapping_enabled": base.model_mapping_enabled,
            "rate_limit": base.rate_limit,
            "additional_config": base.additional_config or {},
            "encrypted_api_key": base.encrypted_api_key,
            "group_id": group_id,
        }

    async def set_server_enabled_for_group(
        self, server_id: int, group_id: str, enabled: bool
    ) -> MCPServerResponse:
        """
        Set the enabled state of a server FOR A SPECIFIC WORKSPACE (group),
        mirroring how Models are toggled per workspace.

        - If the target is this group's own row → just flip its ``enabled``.
        - If the target is a base/global row → create or update a workspace-scoped
          override clone with the requested ``enabled`` (this is how a workspace
          admin disables a globally-available MCP for their workspace). The base
          row is NEVER mutated, so other workspaces are unaffected.
        - If the target is another group's row → not found for this caller.
        """
        if not group_id:
            raise BadRequestError(detail="No workspace/group selected")
        target = await self.server_repository.get(server_id)
        if not target:
            raise NotFoundError(detail=f"MCP server with ID {server_id} not found")

        target_group = getattr(target, "group_id", None)
        # This group's own row → flip directly.
        if target_group == group_id:
            updated = await self.server_repository.update(
                server_id, {"enabled": enabled}
            )
            return self._response_with_key(updated)
        # Another group's row → not visible to this caller.
        if target_group is not None:
            raise NotFoundError(detail=f"MCP server with ID {server_id} not found")
        # Base/global row → create/update this group's override (never touch base).
        existing = await self.server_repository.find_by_name_and_group(
            target.name, group_id
        )
        payload = self._group_override_payload(target, group_id, enabled)
        if existing:
            updated = await self.server_repository.update(existing.id, payload)
            return self._response_with_key(updated)
        created = await self.server_repository.create(payload)
        return self._response_with_key(created)

    async def enable_server_for_group(self, server_id: int, group_id: str) -> MCPServerResponse:
        """
        Enable a server for a workspace (opt-in / re-enable an override).
        Thin wrapper over set_server_enabled_for_group; never disables the base.
        """
        return await self.set_server_enabled_for_group(server_id, group_id, True)

    async def get_base_servers(self) -> MCPServerListResponse:
        """
        List base/global MCP servers (group_id IS NULL) — the system-admin
        catalog. A base server is "available to all workspaces" when enabled.
        """
        servers = await self.server_repository.find_all_base()
        server_responses: List[MCPServerResponse] = []
        for server in servers:
            resp = MCPServerResponse.model_validate(server)
            resp.api_key = ""  # never include in list responses
            server_responses.append(resp)
        return MCPServerListResponse(
            servers=server_responses, count=len(server_responses)
        )

    async def create_global_server(
        self, server_data: MCPServerCreate
    ) -> MCPServerResponse:
        """
        Create a base/global MCP server (group_id IS NULL) — available to all
        workspaces. System-admin only (enforced at the router).
        """
        return await self.create_server(server_data, group_id=None)

    async def set_global_availability(
        self, server_id: int, enabled: bool
    ) -> MCPServerResponse:
        """
        System admin: set whether a base/global server is available to all
        workspaces (its ``enabled`` flag). Validates the target IS a base row.
        """
        server = await self.server_repository.get(server_id)
        if not server:
            raise NotFoundError(detail=f"MCP server with ID {server_id} not found")
        if getattr(server, "group_id", None) is not None:
            raise BadRequestError(detail="Not a global MCP server")
        updated = await self.server_repository.update(server_id, {"enabled": enabled})
        return self._response_with_key(updated)

    async def get_effective_servers(self, explicit_servers: List[str]) -> List[MCPServerResponse]:
        """
        Get effective MCP servers combining global and explicit selections.

        Args:
            explicit_servers: List of explicitly selected server names

        Returns:
            List of effective MCPServerResponse objects (global + explicit, deduplicated)
        """
        # Get global servers
        global_response = await self.get_global_servers()
        global_names = {server.name for server in global_response.servers}

        # Combine global and explicit server names (deduplicated)
        all_server_names = list(global_names.union(set(explicit_servers)))

        # Get all servers by names
        return await self.get_servers_by_names(all_server_names)

    async def get_server_by_id(self, server_id: int) -> MCPServerResponse:
        """
        Get a MCP server by ID.

        Args:
            server_id: ID of the server to retrieve

        Returns:
            MCPServerResponse if found

        Raises:
            HTTPException: If server not found
        """
        server = await self.server_repository.get(server_id)
        if not server:
            logger.warning(f"MCP server with ID {server_id} not found")
            raise NotFoundError(
                detail=f"MCP server with ID {server_id} not found"
            )

        server_response = MCPServerResponse.model_validate(server)

        decrypted = self._decrypt_server_api_key(server)
        if decrypted is not None:
            server_response.api_key = decrypted

        return server_response

    async def create_server(self, server_data: MCPServerCreate, group_id: Optional[str] = None) -> MCPServerResponse:
        """
        Create a new MCP server.

        Args:
            server_data: Server data for creation
            group_id: Optional workspace group ID to scope this server to. When provided,
                      the server will be created as workspace-specific (not global).

        Returns:
            MCPServerResponse of the created server

        Raises:
            HTTPException: If server creation fails
        """
        try:
            # Check if a server with the same name already exists
            existing_server = await self.server_repository.find_by_name(server_data.name)
            if existing_server:
                logger.warning(f"MCP server with name '{server_data.name}' already exists")
                raise ConflictError(
                    detail=f"MCP server with name '{server_data.name}' already exists"
                )

            # Encrypt the API key
            encrypted_api_key = EncryptionUtils.encrypt_value(server_data.api_key)

            # Convert server_data to dictionary excluding api_key
            server_dict = server_data.model_dump(exclude={"api_key"})

            # Add encrypted API key
            server_dict["encrypted_api_key"] = encrypted_api_key

            # Scope to workspace if group_id provided
            if group_id:
                server_dict["group_id"] = group_id
                # Default new workspace-scoped servers to not global
                server_dict["global_enabled"] = False

            # Create server
            server = await self.server_repository.create(server_dict)

            # Prepare response
            server_response = MCPServerResponse.model_validate(server)
            server_response.api_key = server_data.api_key  # Include the original API key in the response

            return server_response
        except KasalError:
            raise
        except Exception as e:
            logger.error(f"Failed to create MCP server: {str(e)}")
            raise KasalError(
                detail=f"Failed to create MCP server: {str(e)}"
            )

    async def update_server(self, server_id: int, server_data: MCPServerUpdate) -> MCPServerResponse:
        """
        Update an existing MCP server.

        Args:
            server_id: ID of server to update
            server_data: Server data for update

        Returns:
            MCPServerResponse of the updated server

        Raises:
            HTTPException: If server not found or update fails
        """
        # Check if server exists
        server = await self.server_repository.get(server_id)
        if not server:
            logger.warning(f"MCP server with ID {server_id} not found for update")
            raise NotFoundError(
                detail=f"MCP server with ID {server_id} not found"
            )

        try:
            # Prepare update data
            update_data = server_data.model_dump(exclude_unset=True, exclude={"api_key"})

            # If API key is provided and not empty, encrypt it
            # Only update encrypted_api_key if a new API key is actually provided
            if server_data.api_key and server_data.api_key.strip():
                update_data["encrypted_api_key"] = EncryptionUtils.encrypt_value(server_data.api_key)

            # Update server
            updated_server = await self.server_repository.update(server_id, update_data)

            # Prepare response
            server_response = MCPServerResponse.model_validate(updated_server)

            decrypted = self._decrypt_server_api_key(updated_server)
            if decrypted is not None:
                server_response.api_key = decrypted

            return server_response
        except Exception as e:
            logger.error(f"Failed to update MCP server: {str(e)}")
            raise KasalError(
                detail=f"Failed to update MCP server: {str(e)}"
            )

    async def delete_server(self, server_id: int) -> bool:
        """
        Delete a MCP server by ID.

        Args:
            server_id: ID of server to delete

        Returns:
            True if deleted successfully

        Raises:
            HTTPException: If server not found or deletion fails
        """
        # Check if server exists
        server = await self.server_repository.get(server_id)
        if not server:
            logger.warning(f"MCP server with ID {server_id} not found for deletion")
            raise NotFoundError(
                detail=f"MCP server with ID {server_id} not found"
            )

        # A GLOBAL (base) server has no group_id. Deleting it must cascade to every
        # workspace that opted in — otherwise their override rows are orphaned and
        # those workspaces keep the server. Workspace-only rows (group_id set) delete
        # just themselves.
        is_base = getattr(server, "group_id", None) is None
        server_name = server.name

        try:
            # Delete server
            await self.server_repository.delete(server_id)
            if is_base:
                removed = await self.server_repository.delete_overrides_by_name(server_name)
                if removed:
                    logger.info(
                        f"Cascade-deleted {removed} workspace override(s) for global MCP server '{server_name}'"
                    )
            return True
        except Exception as e:
            logger.error(f"Failed to delete MCP server: {str(e)}")
            raise KasalError(
                detail=f"Failed to delete MCP server: {str(e)}"
            )

    async def toggle_server_enabled(self, server_id: int) -> MCPToggleResponse:
        """
        Toggle the enabled status of a MCP server.

        Args:
            server_id: ID of server to toggle

        Returns:
            MCPToggleResponse with message and current enabled state

        Raises:
            HTTPException: If server not found or toggle fails
        """
        try:
            # Toggle server enabled status using repository
            server = await self.server_repository.toggle_enabled(server_id)
            if not server:
                logger.warning(f"MCP server with ID {server_id} not found for toggle")
                raise NotFoundError(
                    detail=f"MCP server with ID {server_id} not found"
                )

            status_text = "enabled" if server.enabled else "disabled"
            return MCPToggleResponse(
                message=f"MCP server {status_text} successfully",
                enabled=server.enabled
            )
        except KasalError:
            raise
        except Exception as e:
            logger.error(f"Failed to toggle MCP server: {str(e)}")
            raise KasalError(
                detail=f"Failed to toggle MCP server: {str(e)}"
            )

    async def toggle_server_global_enabled(self, server_id: int) -> MCPToggleResponse:
        """
        Toggle the global enabled status of a MCP server.

        Args:
            server_id: ID of server to toggle global enablement

        Returns:
            MCPToggleResponse with message and current global enabled state

        Raises:
            HTTPException: If server not found or toggle fails
        """
        try:
            # Toggle server global enabled status using repository
            server = await self.server_repository.toggle_global_enabled(server_id)
            if not server:
                logger.warning(f"MCP server with ID {server_id} not found for global toggle")
                raise NotFoundError(
                    detail=f"MCP server with ID {server_id} not found"
                )

            status_text = "globally enabled" if server.global_enabled else "globally disabled"
            return MCPToggleResponse(
                message=f"MCP server {status_text} successfully",
                enabled=server.global_enabled
            )
        except KasalError:
            raise
        except Exception as e:
            logger.error(f"Failed to toggle MCP server global status: {str(e)}")
            raise KasalError(
                detail=f"Failed to toggle MCP server global status: {str(e)}"
            )

    async def test_connection(self, test_data: MCPTestConnectionRequest) -> MCPTestConnectionResponse:
        """
        Test connection to an MCP server.

        Args:
            test_data: Connection test data

        Returns:
            MCPTestConnectionResponse with success status and message
        """
        logger.info(f"Testing MCP connection - Type: {test_data.server_type}, URL: {test_data.server_url}")
        logger.debug(f"Connection test parameters - Timeout: {test_data.timeout_seconds}s, Has API key: {bool(test_data.api_key)}")

        if test_data.server_type.lower() == "sse":
            return await self._test_sse_connection(test_data)
        elif test_data.server_type.lower() == "streamable":
            return await self._test_streamable_connection(test_data)
        else:
            logger.warning(f"Unsupported MCP server type requested: {test_data.server_type}")
            return MCPTestConnectionResponse(
                success=False,
                message=f"Unsupported server type: {test_data.server_type}"
            )

    async def _test_sse_connection(self, test_data: MCPTestConnectionRequest) -> MCPTestConnectionResponse:
        """
        Test connection to an SSE MCP server using the official MCP SSE client.

        Args:
            test_data: Connection test data

        Returns:
            MCPTestConnectionResponse with success status and message
        """
        logger.info(f"Starting SSE connection test to: {test_data.server_url}")

        headers = {}
        if test_data.api_key:
            headers["Authorization"] = f"Bearer {test_data.api_key}"
            logger.debug("Added API key authentication to headers")
        elif test_data.auth_type in ("databricks_obo", "databricks_spn"):
            try:
                from src.utils.databricks_auth import get_auth_context
                # For SPN, skip OBO (user_token=None); for OBO, would pass user_token
                auth_context = await get_auth_context(user_token=None)
                if auth_context and auth_context.token:
                    headers = {
                        "Authorization": f"Bearer {auth_context.token}",
                        "Content-Type": "application/json",
                        "Accept": "text/event-stream",
                        "Cache-Control": "no-cache",
                        "Connection": "keep-alive",
                    }
                    logger.debug(f"Using {auth_context.auth_method} authentication for MCP test")
                else:
                    logger.warning("No Databricks auth context available for MCP test")
            except Exception as e:
                logger.warning(f"Failed to get Databricks auth headers: {e}")

        try:
            from mcp.client.sse import sse_client
            from mcp import ClientSession

            timeout = test_data.timeout_seconds

            async with sse_client(
                test_data.server_url,
                headers=headers if headers else None,
                timeout=timeout
            ) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    tools_result = await session.list_tools()

                    tool_count = 0
                    if hasattr(tools_result, 'tools') and tools_result.tools:
                        tool_count = len(tools_result.tools)

                    logger.info(f"SSE connection test successful - found {tool_count} tools")
                    return MCPTestConnectionResponse(
                        success=True,
                        message=f"Successfully connected to MCP SSE server ({tool_count} tools available)"
                    )

        except Exception as e:
            error_msg = str(e)
            logger.error(f"SSE connection test failed: {type(e).__name__}: {error_msg}", exc_info=True)

            if "401" in error_msg or "403" in error_msg or "Unauthorized" in error_msg.lower():
                return MCPTestConnectionResponse(
                    success=False,
                    message="Authentication failed - please check your API key"
                )
            elif "timeout" in error_msg.lower() or isinstance(e, asyncio.TimeoutError):
                return MCPTestConnectionResponse(
                    success=False,
                    message=f"Connection timed out after {test_data.timeout_seconds} seconds"
                )
            elif "connect" in error_msg.lower() or "refused" in error_msg.lower():
                return MCPTestConnectionResponse(
                    success=False,
                    message=f"Failed to connect: {error_msg}"
                )
            else:
                return MCPTestConnectionResponse(
                    success=False,
                    message=f"Error testing connection: {error_msg}"
                )

    async def _test_streamable_connection(self, test_data: MCPTestConnectionRequest) -> MCPTestConnectionResponse:
        """
        Test connection to a Streamable HTTP MCP server using the official MCP client.

        Args:
            test_data: Connection test data

        Returns:
            MCPTestConnectionResponse with success status and message
        """
        logger.info(f"Starting Streamable HTTP connection test to: {test_data.server_url}")

        headers = {}
        if test_data.api_key:
            headers["Authorization"] = f"Bearer {test_data.api_key}"
            logger.debug("Added API key authentication to headers")
        elif test_data.auth_type in ("databricks_obo", "databricks_spn"):
            try:
                from src.utils.databricks_auth import get_auth_context
                auth_context = await get_auth_context(user_token=None)
                if auth_context and auth_context.token:
                    headers = {"Authorization": f"Bearer {auth_context.token}"}
                    logger.debug(f"Using {auth_context.auth_method} authentication for MCP streamable test")
                else:
                    logger.warning("No Databricks auth context available for MCP streamable test")
            except Exception as e:
                logger.warning(f"Failed to get Databricks auth headers: {e}")

        try:
            from mcp.client.streamable_http import streamablehttp_client
            from mcp import ClientSession

            timeout = test_data.timeout_seconds

            async with streamablehttp_client(
                test_data.server_url,
                headers=headers if headers else None,
                timeout=timeout
            ) as (read_stream, write_stream, _):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    tools_result = await session.list_tools()

                    tool_count = 0
                    if hasattr(tools_result, 'tools') and tools_result.tools:
                        tool_count = len(tools_result.tools)

                    logger.info(f"Streamable HTTP connection test successful - found {tool_count} tools")
                    return MCPTestConnectionResponse(
                        success=True,
                        message=f"Successfully connected to MCP Streamable HTTP server ({tool_count} tools available)"
                    )

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Streamable HTTP connection test failed: {type(e).__name__}: {error_msg}", exc_info=True)

            if "401" in error_msg or "403" in error_msg or "Unauthorized" in error_msg.lower():
                return MCPTestConnectionResponse(
                    success=False,
                    message="Authentication failed - please check your API key"
                )
            elif "timeout" in error_msg.lower() or isinstance(e, asyncio.TimeoutError):
                return MCPTestConnectionResponse(
                    success=False,
                    message=f"Connection timed out after {test_data.timeout_seconds} seconds"
                )
            elif "connect" in error_msg.lower() or "refused" in error_msg.lower():
                return MCPTestConnectionResponse(
                    success=False,
                    message=f"Failed to connect: {error_msg}"
                )
            else:
                return MCPTestConnectionResponse(
                    success=False,
                    message=f"Error testing connection: {error_msg}"
                )

    async def get_settings(self) -> MCPSettingsResponse:
        """
        Get global MCP settings.

        Returns:
            MCPSettingsResponse with global settings
        """
        try:
            settings = await self.settings_repository.get_settings()
            return MCPSettingsResponse.model_validate(settings)
        except Exception as e:
            logger.error(f"Error getting MCP settings: {str(e)}")
            raise KasalError(
                detail=f"Error getting MCP settings: {str(e)}"
            )

    async def update_settings(self, settings_data: MCPSettingsUpdate) -> MCPSettingsResponse:
        """
        Update global MCP settings.

        Args:
            settings_data: Settings data for update

        Returns:
            MCPSettingsResponse with updated settings
        """
        try:
            # Get current settings
            settings = await self.settings_repository.get_settings()

            # Update settings
            update_data = settings_data.model_dump()
            updated_settings = await self.settings_repository.update(settings.id, update_data)

            return MCPSettingsResponse.model_validate(updated_settings)
        except Exception as e:
            logger.error(f"Error updating MCP settings: {str(e)}")
            raise KasalError(
                detail=f"Error updating MCP settings: {str(e)}"
            )