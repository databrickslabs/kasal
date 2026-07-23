"""
API router for MCP server operations.

This module provides endpoints for managing MCP (Model Context Protocol) servers.
"""
import logging
from typing import Annotated, Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Request, status

from src.core.exceptions import BadRequestError, ForbiddenError

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.permissions import check_role_in_context
from src.schemas.mcp import (
    MCPServerCreate,
    MCPServerListResponse,
    MCPServerResponse,
    MCPServerUpdate,
    MCPSettingsResponse,
    MCPSettingsUpdate,
    MCPTestConnectionRequest,
    MCPTestConnectionResponse,
    MCPToggleResponse,
)
from src.services.mcp_service import MCPService

# Create router instance
router = APIRouter(
    prefix="/mcp",
    tags=["mcp"],
    responses={404: {"description": "Not found"}},
)

# Set up logger
logger = logging.getLogger(__name__)


async def get_mcp_service(session: SessionDep) -> MCPService:
    """
    Dependency provider for MCPService.

    Creates service with properly injected session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI

    Returns:
        MCPService instance with injected session
    """
    return MCPService(session=session)


# Type alias for cleaner function signatures
MCPServiceDep = Annotated[MCPService, Depends(get_mcp_service)]


def _is_global_admin(group_context) -> bool:
    """
    Whether the caller may manage GLOBAL (base) MCP servers.

    Mirrors models_router's global gate: an effective admin role OR a system
    admin. Global MCP servers are available to all workspaces, so changing them
    is a system-administration action.
    """
    try:
        from src.core.permissions import get_effective_role

        role = get_effective_role(group_context) if group_context else None
        if role and role.lower() == "admin":
            return True
    except Exception:
        pass
    return bool(
        group_context is not None
        and getattr(
            getattr(group_context, "current_user", None), "is_system_admin", False
        )
    )


def _require_enabled_flag(payload: Dict[str, Any]) -> bool:
    """Validate and extract a boolean ``enabled`` from a PATCH body."""
    if "enabled" not in payload or not isinstance(payload["enabled"], bool):
        raise BadRequestError("'enabled' boolean is required")
    return bool(payload["enabled"])


@router.get("/servers", response_model=MCPServerListResponse)
async def get_mcp_servers(
    service: MCPServiceDep, group_context: GroupContextDep = None
) -> MCPServerListResponse:
    """
    Get MCP servers effective for the current workspace (group).
    Deduplicated by name, preferring workspace overrides over base.

    Only workspace admins see the full list (including disabled servers, which
    they manage in Configuration → MCP). Everyone else sees only the servers an
    admin has ENABLED — the curated allow-list — so regular users can't pick
    servers the workspace hasn't sanctioned.
    """
    group_id = (
        getattr(group_context, "primary_group_id", None) if group_context else None
    )
    is_admin = check_role_in_context(group_context, ["admin"])
    return await service.get_all_servers_effective(group_id, enabled_only=not is_admin)


async def _list_external_mcp_options(
    workspace_url: str, user_token: Optional[str]
) -> List[Dict[str, Any]]:
    """
    External MCP servers registered IN DATABRICKS that the caller can use.

    These are Unity Catalog HTTP connections flagged as MCP connections
    (workspace UI: AI Gateway → MCPs), proxied by the workspace at
    ``/api/2.0/mcp/external/{connection_name}``. Listing uses the CALLER's
    credentials (OBO when available), so the result honors their UC
    permissions — users only see the servers they have access to.
    """
    import aiohttp

    from src.utils.databricks_auth import get_auth_context
    from src.utils.telemetry import KasalProduct, get_user_agent_header

    auth = await get_auth_context(user_token=user_token)
    if not auth:
        return []
    headers = auth.get_headers()
    headers.update(get_user_agent_header(KasalProduct.MCP))

    url = f"{workspace_url}/api/2.1/unity-catalog/connections"
    async with aiohttp.ClientSession() as http:
        async with http.get(url, headers=headers) as resp:
            if resp.status != 200:
                logger.warning(
                    f"Could not list UC connections for external MCPs: HTTP {resp.status}"
                )
                return []
            payload = await resp.json()

    options: List[Dict[str, Any]] = []
    for conn in payload.get("connections") or []:
        if str(conn.get("connection_type", "")).upper() != "HTTP":
            continue
        name = str(conn.get("name", ""))
        if not name:
            continue
        # The MCP flag is an option on the HTTP connection ("Is MCP connection"
        # in the UI). Match any mcp-ish option key so naming variants
        # (is_mcp / is_mcp_connection) all register.
        # Databricks' system-managed AI-agent connections (system_ai_agent_*)
        # are deliberately NOT special-cased: the REST-backed ones (gmail,
        # calendar, drive, sharepoint) answer an MCP handshake with the
        # vendor's 404 ("Session terminated"), and even the MCP-backed ones
        # (slack/atlassian) are AgentBricks-internal — they stay out of the
        # picker. Per-user services like Gmail are exposed as dedicated Kasal
        # tools instead (see gmail_tool.py).
        conn_options = conn.get("options") or {}
        is_mcp = any(
            "mcp" in str(key).lower() and str(value).strip().lower() in ("true", "1", "yes")
            for key, value in conn_options.items()
        )
        if not is_mcp:
            continue
        options.append(
            {
                "id": f"external:{name}",
                "kind": "external",
                "name": name,
                "description": conn.get("comment"),
                "server_url": f"{workspace_url}/api/2.0/mcp/external/{name}",
            }
        )
    return options


@router.get("/databricks/available")
async def get_databricks_mcp_options(
    request: Request, session: SessionDep, group_context: GroupContextDep = None
) -> Dict[str, Any]:
    """
    The Databricks MCP catalog for this workspace, grouped for the chat's
    two-step picker:

    - ``external``: MCP servers registered IN DATABRICKS as UC HTTP
      connections (proxied at ``/api/2.0/mcp/external/{name}``), listed with
      the caller's credentials so only the ones they have permission on appear.
    - ``managed``: the workspace's managed MCP server TYPES. Leaf types carry
      a ``server_url`` and are selectable directly:
        * Databricks SQL  → ``/api/2.0/mcp/sql``
        * Unity Catalog Functions → ``/api/2.0/mcp/functions/{catalog}/{schema}``
          using the catalog/schema from the workspace's Databricks
          configuration (only offered when configured).
      Expandable types (``expandable: true``) have a second step — Genie
      spaces can number in the thousands, so they are NOT enumerated here:
        * Genie     → drill into /mcp/databricks/genie-spaces (search+paging)
        * AI Search → drill into /mcp/databricks/ai-search-indexes

    Selecting any leaf registers it as a Kasal MCP server with
    ``auth_type=databricks_spn`` and ``server_type=streamable``.

    Browsing/registering Databricks MCP servers is a workspace-admin action, so
    this catalog is admin-only (enforced here, not just hidden in the UI).
    """
    if not (
        check_role_in_context(group_context, ["admin"])
        or _is_global_admin(group_context)
    ):
        raise ForbiddenError("Only admins can browse Databricks MCP servers")

    from src.utils.databricks_auth import (
        extract_user_token_from_request,
        get_auth_context,
    )
    from src.utils.user_context import UserContext

    if group_context:
        UserContext.set_group_context(group_context)
    user_token = extract_user_token_from_request(request)

    workspace_url = ""
    try:
        auth = await get_auth_context(user_token=user_token)
        workspace_url = (auth.workspace_url or "").rstrip("/") if auth else ""
    except Exception as e:
        logger.warning(f"Could not resolve workspace URL for Databricks MCPs: {e}")

    external: List[Dict[str, Any]] = []
    managed: List[Dict[str, Any]] = []
    if workspace_url:
        # External (connection-based) MCP servers, permission-filtered by
        # listing with the caller's own credentials.
        try:
            external = await _list_external_mcp_options(workspace_url, user_token)
        except Exception as e:
            logger.warning(f"Could not enumerate external Databricks MCP servers: {e}")

        managed.append(
            {
                "id": "sql",
                "kind": "sql",
                "name": "Databricks SQL",
                "description": "Execute SQL against the workspace (managed MCP)",
                "server_url": f"{workspace_url}/api/2.0/mcp/sql",
                "expandable": False,
            }
        )

        # Unity Catalog Functions — schema-level server built from the
        # catalog/schema in the workspace's Databricks configuration.
        try:
            from src.repositories.databricks_config_repository import (
                DatabricksConfigRepository,
            )

            group_id = (
                getattr(group_context, "primary_group_id", None) if group_context else None
            )
            config = await DatabricksConfigRepository(session).get_active_config(
                group_id=group_id
            )
            catalog = getattr(config, "catalog", None) if config else None
            schema = getattr(config, "schema", None) if config else None
            if catalog and schema:
                managed.append(
                    {
                        "id": f"functions:{catalog}.{schema}",
                        "kind": "functions",
                        "name": f"Unity Catalog Functions ({catalog}.{schema})",
                        "description": "Run the UC functions in the configured schema",
                        "server_url": f"{workspace_url}/api/2.0/mcp/functions/{catalog}/{schema}",
                        "expandable": False,
                    }
                )
        except Exception as e:
            logger.warning(f"Could not derive UC Functions MCP from config: {e}")

        # Built-in system.ai functions (python_exec, etc.) are always offered;
        # skipped only when the configured schema already IS system.ai.
        if all(o["id"] != "functions:system.ai" for o in managed):
            managed.append(
                {
                    "id": "functions:system.ai",
                    "kind": "functions",
                    "name": "Unity Catalog Functions (system.ai)",
                    "description": "Built-in functions such as python_exec",
                    "server_url": f"{workspace_url}/api/2.0/mcp/functions/system/ai",
                    "expandable": False,
                }
            )

        # Two-step types — instances are listed on drill-in.
        managed.append(
            {
                "id": "genie",
                "kind": "genie",
                "name": "Genie",
                "description": "Pick a Genie space",
                "expandable": True,
            }
        )
        managed.append(
            {
                "id": "ai-search",
                "kind": "ai-search",
                "name": "AI Search",
                "description": "Pick an AI Search index",
                "expandable": True,
            }
        )

    return {"workspace_url": workspace_url, "external": external, "managed": managed}


@router.get("/databricks/genie-spaces")
async def list_genie_mcp_spaces(
    request: Request,
    search: Optional[str] = None,
    page_token: Optional[str] = None,
    group_context: GroupContextDep = None,
) -> Dict[str, Any]:
    """
    Second step of the Genie managed-MCP picker: the caller's Genie spaces as
    selectable MCP servers (``/api/2.0/mcp/genie/{space_id}``), searchable and
    paginated — workspaces can have thousands of spaces, so the first step
    never enumerates them.

    Admin-only: registering a Genie space as an MCP server is a workspace-admin
    action (enforced here, not just hidden in the UI).
    """
    if not (
        check_role_in_context(group_context, ["admin"])
        or _is_global_admin(group_context)
    ):
        raise ForbiddenError("Only admins can browse Databricks MCP servers")

    from src.schemas.genie import GenieAuthConfig, GenieSpacesRequest
    from src.services.genie_service import GenieService
    from src.utils.databricks_auth import (
        extract_user_token_from_request,
        get_auth_context,
    )
    from src.utils.user_context import UserContext

    if group_context:
        UserContext.set_group_context(group_context)
    user_token = extract_user_token_from_request(request)

    auth = await get_auth_context(user_token=user_token)
    workspace_url = (auth.workspace_url or "").rstrip("/") if auth else ""
    if not workspace_url:
        return {"options": [], "next_page_token": None}

    genie = GenieService(GenieAuthConfig(use_obo=True, user_token=user_token))
    spaces = await genie.get_spaces(
        GenieSpacesRequest(search_query=search, page_token=page_token, page_size=50)
    )
    return {
        "options": [
            {
                "id": f"genie:{space.id}",
                "kind": "genie",
                "name": space.name,
                "description": space.description,
                "server_url": f"{workspace_url}/api/2.0/mcp/genie/{space.id}",
            }
            for space in spaces.spaces
        ],
        "next_page_token": spaces.next_page_token,
    }


@router.get("/databricks/ai-search-indexes")
async def list_ai_search_mcp_indexes(
    request: Request, group_context: GroupContextDep = None
) -> Dict[str, Any]:
    """
    Second step of the AI Search managed-MCP picker: the workspace's vector
    search indexes as selectable MCP servers
    (``/api/2.0/mcp/ai-search/{catalog}/{schema}/{index}``), listed with the
    caller's credentials.

    Admin-only: registering an AI Search index as an MCP server is a
    workspace-admin action (enforced here, not just hidden in the UI).
    """
    if not (
        check_role_in_context(group_context, ["admin"])
        or _is_global_admin(group_context)
    ):
        raise ForbiddenError("Only admins can browse Databricks MCP servers")

    import aiohttp

    from src.utils.databricks_auth import (
        extract_user_token_from_request,
        get_auth_context,
    )
    from src.utils.telemetry import KasalProduct, get_user_agent_header
    from src.utils.user_context import UserContext

    if group_context:
        UserContext.set_group_context(group_context)
    user_token = extract_user_token_from_request(request)

    auth = await get_auth_context(user_token=user_token)
    workspace_url = (auth.workspace_url or "").rstrip("/") if auth else ""
    if not workspace_url:
        return {"options": []}

    headers = auth.get_headers()
    headers.update(get_user_agent_header(KasalProduct.MCP))

    options: List[Dict[str, Any]] = []
    try:
        async with aiohttp.ClientSession() as http:
            async with http.get(
                f"{workspace_url}/api/2.0/vector-search/endpoints", headers=headers
            ) as resp:
                if resp.status != 200:
                    logger.warning(
                        f"Could not list vector search endpoints: HTTP {resp.status}"
                    )
                    return {"options": []}
                endpoints = (await resp.json()).get("endpoints") or []

            for endpoint in endpoints:
                endpoint_name = endpoint.get("name")
                if not endpoint_name:
                    continue
                async with http.get(
                    f"{workspace_url}/api/2.0/vector-search/indexes",
                    headers=headers,
                    params={"endpoint_name": endpoint_name},
                ) as resp:
                    if resp.status != 200:
                        continue
                    indexes = (await resp.json()).get("vector_indexes") or []
                for index in indexes:
                    full_name = str(index.get("name", ""))
                    parts = full_name.split(".")
                    if len(parts) != 3:
                        continue
                    catalog, schema, index_name = parts
                    options.append(
                        {
                            "id": f"ai-search:{full_name}",
                            "kind": "ai-search",
                            "name": full_name,
                            "description": f"Endpoint: {endpoint_name}",
                            "server_url": (
                                f"{workspace_url}/api/2.0/mcp/ai-search/"
                                f"{catalog}/{schema}/{index_name}"
                            ),
                        }
                    )
    except Exception as e:
        logger.warning(f"Could not enumerate AI Search indexes: {e}")

    return {"options": options}


@router.get("/servers/enabled", response_model=MCPServerListResponse)
async def get_enabled_mcp_servers(
    service: MCPServiceDep, group_context: GroupContextDep = None
) -> MCPServerListResponse:
    """
    Get all enabled MCP servers.
    """
    logger.info("Getting enabled MCP servers")
    servers_response = await service.get_enabled_servers()
    logger.info(f"Found {servers_response.count} enabled MCP servers")
    return servers_response


@router.get("/servers/global", response_model=MCPServerListResponse)
async def get_global_mcp_servers(
    service: MCPServiceDep, group_context: GroupContextDep = None
) -> MCPServerListResponse:
    """
    Get all globally enabled MCP servers.
    """
    logger.info("Getting globally enabled MCP servers")
    servers_response = await service.get_global_servers()
    logger.info(f"Found {servers_response.count} globally enabled MCP servers")
    return servers_response


@router.get("/servers/base", response_model=MCPServerListResponse)
async def get_base_mcp_servers(
    service: MCPServiceDep, group_context: GroupContextDep = None
) -> MCPServerListResponse:
    """
    Get the base/global MCP servers (group_id IS NULL) — the system-admin
    catalog. A base server is "available to all workspaces" when enabled.
    Only system admins manage this list.
    """
    if not _is_global_admin(group_context):
        raise ForbiddenError("Only system admins can view global MCP servers")
    return await service.get_base_servers()


@router.post(
    "/servers/global",
    response_model=MCPServerResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_global_mcp_server(
    server_data: MCPServerCreate,
    service: MCPServiceDep,
    group_context: GroupContextDep = None,
) -> MCPServerResponse:
    """
    Create a base/global MCP server (group_id IS NULL), available to all
    workspaces. Only system admins can create global MCP servers.
    """
    if not _is_global_admin(group_context):
        raise ForbiddenError("Only system admins can create global MCP servers")
    logger.info(f"Creating global MCP server with name '{server_data.name}'")
    server = await service.create_global_server(server_data)
    logger.info(f"Created global MCP server with ID {server.id}")
    return server


@router.get("/servers/{server_id}", response_model=MCPServerResponse)
async def get_mcp_server(
    server_id: int, service: MCPServiceDep, group_context: GroupContextDep = None
) -> MCPServerResponse:
    """
    Get an MCP server by ID.
    """
    logger.info(f"Getting MCP server with ID {server_id}")
    server = await service.get_server_by_id(server_id)
    logger.info(f"Found MCP server with ID {server_id}")
    return server


@router.post(
    "/servers", response_model=MCPServerResponse, status_code=status.HTTP_201_CREATED
)
async def create_mcp_server(
    server_data: MCPServerCreate,
    service: MCPServiceDep,
    group_context: GroupContextDep = None,
) -> MCPServerResponse:
    """
    Create a new MCP server.
    Only Admins can create MCP servers.

    Behavior:
    - If a workspace context (group_id) is present, the server is created scoped to that workspace
      to prevent cross-workspace visibility.
    - Otherwise, it is created as a base (global) server.
    """
    # Check permissions - only admins can create MCP servers
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can create MCP servers")

    # Determine workspace scoping
    group_id = (
        getattr(group_context, "primary_group_id", None) if group_context else None
    )

    logger.info(
        f"Creating MCP server with name '{server_data.name}' scoped to group_id={group_id}"
    )
    server = await service.create_server(server_data, group_id=group_id)
    logger.info(f"Created MCP server with ID {server.id}")
    return server


@router.put("/servers/{server_id}", response_model=MCPServerResponse)
async def update_mcp_server(
    server_id: int,
    server_data: MCPServerUpdate,
    service: MCPServiceDep,
    group_context: GroupContextDep = None,
) -> MCPServerResponse:
    """
    Update an existing MCP server.
    Only Admins can update MCP servers.
    """
    # Check permissions - workspace admins (own rows) or system admins (base rows)
    if not (
        check_role_in_context(group_context, ["admin"])
        or _is_global_admin(group_context)
    ):
        raise ForbiddenError("Only admins can update MCP servers")

    logger.info(f"Updating MCP server with ID {server_id}")
    server = await service.update_server(server_id, server_data)
    logger.info(f"Updated MCP server with ID {server_id}")
    return server


@router.delete("/servers/{server_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_mcp_server(
    server_id: int, service: MCPServiceDep, group_context: GroupContextDep = None
) -> None:
    """
    Delete an MCP server.
    Only Admins can delete MCP servers.
    """
    # Check permissions - workspace admins (own rows) or system admins (base rows)
    if not (
        check_role_in_context(group_context, ["admin"])
        or _is_global_admin(group_context)
    ):
        raise ForbiddenError("Only admins can delete MCP servers")

    logger.info(f"Deleting MCP server with ID {server_id}")
    await service.delete_server(server_id)
    logger.info(f"Deleted MCP server with ID {server_id}")


@router.patch("/servers/{server_id}/toggle-enabled", response_model=MCPToggleResponse)
async def toggle_mcp_server_enabled(
    server_id: int, service: MCPServiceDep, group_context: GroupContextDep = None
) -> MCPToggleResponse:
    """
    Toggle the enabled status of an MCP server.
    Only Admins can toggle MCP server status.
    """
    # Check permissions - only admins can toggle MCP servers
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can toggle MCP server status")

    logger.info(f"Toggling enabled status for MCP server with ID {server_id}")
    response = await service.toggle_server_enabled(server_id)
    status_text = "enabled" if response.enabled else "disabled"
    logger.info(f"MCP server with ID {server_id} {status_text}")
    return response


@router.patch(
    "/servers/{server_id}/toggle-global-enabled", response_model=MCPToggleResponse
)
async def toggle_mcp_server_global_enabled(
    server_id: int, service: MCPServiceDep, group_context: GroupContextDep = None
) -> MCPToggleResponse:
    """
    Toggle the global enabled status of an MCP server.
    Only Admins can toggle global MCP server status.
    """
    # Check permissions - only admins can toggle global MCP server status
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can toggle global MCP server status")

    logger.info(f"Toggling global enabled status for MCP server with ID {server_id}")
    response = await service.toggle_server_global_enabled(server_id)
    status_text = "globally enabled" if response.enabled else "globally disabled"
    logger.info(f"MCP server with ID {server_id} {status_text}")
    return response


@router.post(
    "/servers/{server_id}/enable-for-workspace", response_model=MCPServerResponse
)
async def enable_mcp_server_for_workspace(
    server_id: int, service: MCPServiceDep, group_context: GroupContextDep = None
) -> MCPServerResponse:
    """
    Create or update a workspace-scoped override for this server and enable it.
    Only Admins can perform this action.
    """
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can enable MCP server for a workspace")
    group_id = getattr(group_context, "primary_group_id", None)
    if not group_id:
        raise BadRequestError("No workspace/group selected")
    return await service.enable_server_for_group(server_id, group_id)


@router.patch(
    "/servers/{server_id}/global-availability", response_model=MCPServerResponse
)
async def set_mcp_server_global_availability(
    server_id: int,
    payload: Dict[str, Any],
    service: MCPServiceDep,
    group_context: GroupContextDep = None,
) -> MCPServerResponse:
    """
    System admin: set whether a base/global MCP server is available to all
    workspaces (its ``enabled`` flag). Mirrors Tools' global-availability toggle.
    """
    if not _is_global_admin(group_context):
        raise ForbiddenError("Only system admins can change global MCP availability")
    enabled = _require_enabled_flag(payload)
    logger.info(f"Setting global availability for MCP server {server_id} to {enabled}")
    return await service.set_global_availability(server_id, enabled)


@router.patch(
    "/servers/{server_id}/workspace-enabled", response_model=MCPServerResponse
)
async def set_mcp_server_workspace_enabled(
    server_id: int,
    payload: Dict[str, Any],
    service: MCPServiceDep,
    group_context: GroupContextDep = None,
) -> MCPServerResponse:
    """
    Workspace admin: enable/disable a server FOR THIS WORKSPACE only.

    Disabling a globally-available (base) server creates a workspace-scoped
    override (enabled=false) that hides it from this workspace's users without
    affecting other workspaces. Toggling the workspace's own row flips it in place.
    """
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can change MCP server state for a workspace")
    group_id = getattr(group_context, "primary_group_id", None)
    if not group_id:
        raise BadRequestError("No workspace/group selected")
    enabled = _require_enabled_flag(payload)
    logger.info(
        f"Setting workspace-enabled for MCP server {server_id} to {enabled} "
        f"(group={group_id})"
    )
    return await service.set_server_enabled_for_group(server_id, group_id, enabled)


@router.post("/test-connection", response_model=MCPTestConnectionResponse)
async def test_mcp_connection(
    test_data: MCPTestConnectionRequest,
    service: MCPServiceDep,
    group_context: GroupContextDep = None,
) -> MCPTestConnectionResponse:
    """
    Test connection to an MCP server.
    Only Admins can test MCP server connections.
    """
    # Check permissions - only admins can test MCP connections
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can test MCP server connections")

    logger.info(f"Testing connection to MCP server at {test_data.server_url}")
    try:
        response = await service.test_connection(test_data)
        success_text = "successful" if response.success else "failed"
        logger.info(f"Connection test {success_text}: {response.message}")
        return response
    except Exception as e:
        logger.error(f"Error testing MCP server connection: {str(e)}")
        return MCPTestConnectionResponse(
            success=False, message=f"Error testing connection: {str(e)}"
        )


@router.get("/settings", response_model=MCPSettingsResponse)
async def get_mcp_settings(
    service: MCPServiceDep, group_context: GroupContextDep = None
) -> MCPSettingsResponse:
    """
    Get global MCP settings.
    """
    logger.info("Getting global MCP settings")
    settings = await service.get_settings()
    logger.info(f"Retrieved global MCP settings (enabled: {settings.global_enabled})")
    return settings


@router.put("/settings", response_model=MCPSettingsResponse)
async def update_mcp_settings(
    settings_data: MCPSettingsUpdate,
    service: MCPServiceDep,
    group_context: GroupContextDep = None,
) -> MCPSettingsResponse:
    """
    Update global MCP settings.
    Only Admins can update global MCP settings.
    """
    # Check permissions - only admins can update global MCP settings
    if not check_role_in_context(group_context, ["admin"]):
        raise ForbiddenError("Only admins can update global MCP settings")

    logger.info(
        f"Updating global MCP settings (enabled: {settings_data.global_enabled})"
    )
    settings = await service.update_settings(settings_data)
    logger.info(f"Updated global MCP settings (enabled: {settings.global_enabled})")
    return settings
