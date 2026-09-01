"""Tests for the Databricks MCP catalog endpoints behind the chat's two-step
"+" picker: /mcp/databricks/available (grouped external + managed types),
/mcp/databricks/genie-spaces and /mcp/databricks/ai-search-indexes."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.api.mcp_router import (
    _list_external_mcp_options,
    get_databricks_mcp_options,
    list_ai_search_mcp_indexes,
    list_function_mcp_schemas,
    list_genie_mcp_spaces,
    list_schema_functions,
)
from src.core.exceptions import ForbiddenError


def _request():
    req = MagicMock()
    req.headers = {}
    return req


def _admin_ctx():
    """A group context that resolves to the 'admin' role.

    Browsing the Databricks catalog is an admin action (workspace or system
    admin), enforced in the endpoints. With current_user falsy, get_effective_role
    falls through to user_role, so role='admin' satisfies the gate.
    """
    return SimpleNamespace(user_role="admin", current_user=None, primary_group_id=None)


def _non_admin_ctx():
    """A group context with a non-admin role (gate must reject)."""
    return SimpleNamespace(
        user_role="operator", current_user=None, primary_group_id=None
    )


def _auth(url="https://ws.example.com"):
    return SimpleNamespace(
        workspace_url=url, get_headers=lambda: {"Authorization": "Bearer t"}
    )


def _space(space_id, name, description=None):
    return SimpleNamespace(id=space_id, name=name, description=description)


def _aiohttp_session(responses):
    """Fake aiohttp.ClientSession returning queued (status, payload) per GET."""
    get_cms = []
    for status, payload in responses:
        response = MagicMock()
        response.status = status
        response.json = AsyncMock(return_value=payload)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=response)
        cm.__aexit__ = AsyncMock(return_value=False)
        get_cms.append(cm)

    session = MagicMock()
    session.get = MagicMock(side_effect=get_cms)
    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=False)
    return session_cm, session


# ---------------------------------------------------------------------------
# /mcp/databricks/available — grouped catalog (step one)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_catalog_groups_external_and_managed_types():
    config = SimpleNamespace(catalog="main", schema="gold")
    config_repo = MagicMock()
    config_repo.get_active_config = AsyncMock(return_value=config)

    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth("https://ws.example.com/")),
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value="tok",
        ),
        patch(
            "src.api.mcp_router._list_external_mcp_options",
            AsyncMock(
                return_value=[
                    {
                        "id": "external:jira",
                        "kind": "external",
                        "name": "jira",
                        "description": "Jira MCP",
                        "server_url": "https://ws.example.com/api/2.0/mcp/external/jira",
                    },
                ]
            ),
        ),
        patch(
            "src.repositories.databricks_config_repository.DatabricksConfigRepository",
            MagicMock(return_value=config_repo),
        ),
    ):
        result = await get_databricks_mcp_options(
            _request(), session=AsyncMock(), group_context=_admin_ctx()
        )

    assert result["workspace_url"] == "https://ws.example.com"
    # External (connection-based) servers come grouped on their own.
    assert [o["id"] for o in result["external"]] == ["external:jira"]

    managed = {o["id"]: o for o in result["managed"]}
    # Leaves are directly selectable.
    assert managed["sql"]["server_url"] == "https://ws.example.com/api/2.0/mcp/sql"
    assert managed["sql"]["expandable"] is False
    # Genie One — the workspace-wide managed Genie MCP (no space id) is a
    # directly-selectable leaf, distinct from the per-space drill-in below.
    assert (
        managed["genie-one"]["server_url"] == "https://ws.example.com/api/2.0/mcp/genie"
    )
    assert managed["genie-one"]["kind"] == "genie"
    assert managed["genie-one"]["expandable"] is False
    # The server's start+poll convention ships as preset DATA (the engine
    # The catalog ships NO tool-convention config: the follow presets are
    # applied at tool-creation time from the managed URL
    # (mcp_integration._follow_config_for), so the option carries none.
    assert "additional_config" not in managed["genie-one"]
    # Two-step types carry NO instance list on drill-in step one.
    # Functions is schema-scoped (a server per catalog.schema), so it drills in
    # like Genie/AI Search rather than shipping a leaf.
    assert managed["functions"]["expandable"] is True
    assert "server_url" not in managed["functions"]
    # Genie can have 1000s of spaces — also two-step.
    assert managed["genie"]["expandable"] is True
    assert "server_url" not in managed["genie"]
    assert managed["ai-search"]["expandable"] is True


@pytest.mark.asyncio
async def test_catalog_managed_ids_are_config_independent():
    """Step one no longer reads the Databricks config: Functions is an
    expandable two-step type (schemas listed on drill-in), so the managed list
    is the same regardless of the configured catalog.schema."""
    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value=None,
        ),
        patch(
            "src.api.mcp_router._list_external_mcp_options", AsyncMock(return_value=[])
        ),
    ):
        result = await get_databricks_mcp_options(
            _request(), session=AsyncMock(), group_context=_admin_ctx()
        )

    ids = [o["id"] for o in result["managed"]]
    assert ids == ["sql", "functions", "genie-one", "genie", "ai-search"]


@pytest.mark.asyncio
async def test_catalog_empty_without_workspace_url():
    with (
        patch(
            "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value=None,
        ),
    ):
        result = await get_databricks_mcp_options(
            _request(), session=AsyncMock(), group_context=_admin_ctx()
        )

    assert result == {"workspace_url": "", "external": [], "managed": []}


@pytest.mark.asyncio
async def test_catalog_survives_external_and_config_failures():
    config_repo = MagicMock()
    config_repo.get_active_config = AsyncMock(side_effect=Exception("db down"))

    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value="tok",
        ),
        patch(
            "src.api.mcp_router._list_external_mcp_options",
            AsyncMock(side_effect=Exception("uc down")),
        ),
        patch(
            "src.repositories.databricks_config_repository.DatabricksConfigRepository",
            MagicMock(return_value=config_repo),
        ),
    ):
        result = await get_databricks_mcp_options(
            _request(), session=AsyncMock(), group_context=_admin_ctx()
        )

    assert result["external"] == []
    assert [o["id"] for o in result["managed"]] == [
        "sql",
        "functions",
        "genie-one",
        "genie",
        "ai-search",
    ]


@pytest.mark.asyncio
async def test_catalog_forbidden_for_non_admin():
    """Browsing the Databricks catalog is admin-only (workspace or system)."""
    with pytest.raises(ForbiddenError):
        await get_databricks_mcp_options(
            _request(), session=AsyncMock(), group_context=_non_admin_ctx()
        )


# ---------------------------------------------------------------------------
# /mcp/databricks/genie-spaces — step two (searchable + paginated)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_genie_spaces_step_returns_mcp_urls_and_page_token():
    genie_service = MagicMock()
    genie_service.get_spaces = AsyncMock(
        return_value=SimpleNamespace(
            spaces=[_space("s1", "Sales Space", "sales data")],
            next_page_token="tok-2",
        )
    )

    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value="tok",
        ),
        patch(
            "src.services.databricks.genie.service.GenieService",
            MagicMock(return_value=genie_service),
        ),
    ):
        result = await list_genie_mcp_spaces(
            _request(), search="sales", page_token=None, group_context=_admin_ctx()
        )

    assert result["next_page_token"] == "tok-2"
    assert result["options"] == [
        {
            "id": "genie:s1",
            "kind": "genie",
            "name": "Sales Space",
            "description": "sales data",
            "server_url": "https://ws.example.com/api/2.0/mcp/genie/s1",
        }
    ]
    # The search query and page token ride into the Genie request.
    spaces_request = genie_service.get_spaces.call_args.args[0]
    assert spaces_request.search_query == "sales"
    assert spaces_request.page_token is None


@pytest.mark.asyncio
async def test_genie_spaces_step_empty_without_workspace():
    with (
        patch(
            "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value=None,
        ),
    ):
        result = await list_genie_mcp_spaces(_request(), group_context=_admin_ctx())
    assert result == {"options": [], "next_page_token": None}


# ---------------------------------------------------------------------------
# /mcp/databricks/function-schemas — step two (schema-scoped functions servers)
# ---------------------------------------------------------------------------


def _databricks_service(catalogs, schemas_by_catalog):
    svc = MagicMock()
    svc.list_catalogs = AsyncMock(return_value=catalogs)
    svc.list_schemas = AsyncMock(
        side_effect=lambda catalog, host=None: schemas_by_catalog.get(catalog, [])
    )
    return svc


@pytest.mark.asyncio
async def test_function_schemas_step_pins_system_ai_and_config_then_lists_catalog():
    config = SimpleNamespace(catalog="main", schema="gold")
    config_repo = MagicMock()
    config_repo.get_active_config = AsyncMock(return_value=config)
    svc = _databricks_service(
        catalogs=["main", "other"],
        schemas_by_catalog={"main": ["gold", "bronze"]},
    )

    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value="tok",
        ),
        patch(
            "src.repositories.databricks_config_repository.DatabricksConfigRepository",
            MagicMock(return_value=config_repo),
        ),
        patch(
            "src.services.databricks.workspace.service.DatabricksService",
            MagicMock(return_value=svc),
        ),
    ):
        result = await list_function_mcp_schemas(
            _request(), session=AsyncMock(), group_context=_admin_ctx()
        )

    assert result["catalogs"] == ["main", "other"]
    assert result["selected_catalog"] == "main"  # defaulted to the configured catalog
    # system.ai and the configured main.gold are pinned first; main.gold is not
    # duplicated when it also appears in the browsed catalog's schemas.
    assert [o["id"] for o in result["options"]] == [
        "functions:system.ai",
        "functions:main.gold",
        "functions:main.bronze",
    ]
    system_ai = result["options"][0]
    assert (
        system_ai["server_url"]
        == "https://ws.example.com/api/2.0/mcp/functions/system/ai"
    )
    assert system_ai["kind"] == "functions"
    assert (
        result["options"][2]["server_url"]
        == "https://ws.example.com/api/2.0/mcp/functions/main/bronze"
    )


@pytest.mark.asyncio
async def test_function_schemas_step_browses_requested_catalog_and_filters_search():
    config_repo = MagicMock()
    config_repo.get_active_config = AsyncMock(return_value=None)
    svc = _databricks_service(
        catalogs=["main", "sales"],
        schemas_by_catalog={"sales": ["gold", "silver"]},
    )

    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value="tok",
        ),
        patch(
            "src.repositories.databricks_config_repository.DatabricksConfigRepository",
            MagicMock(return_value=config_repo),
        ),
        patch(
            "src.services.databricks.workspace.service.DatabricksService",
            MagicMock(return_value=svc),
        ),
    ):
        result = await list_function_mcp_schemas(
            _request(),
            session=AsyncMock(),
            catalog="sales",
            search="silver",
            group_context=_admin_ctx(),
        )

    svc.list_schemas.assert_awaited_with("sales")
    assert result["selected_catalog"] == "sales"
    # No config schema to pin; system.ai is filtered out by the search, leaving
    # only the matching sales.silver.
    assert [o["id"] for o in result["options"]] == ["functions:sales.silver"]


@pytest.mark.asyncio
async def test_function_schemas_step_empty_without_workspace():
    with (
        patch(
            "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value=None,
        ),
    ):
        result = await list_function_mcp_schemas(
            _request(), session=AsyncMock(), group_context=_admin_ctx()
        )
    assert result == {"options": [], "catalogs": [], "selected_catalog": None}


@pytest.mark.asyncio
async def test_function_schemas_step_forbidden_for_non_admin():
    with pytest.raises(ForbiddenError):
        await list_function_mcp_schemas(
            _request(), session=AsyncMock(), group_context=_non_admin_ctx()
        )


@pytest.mark.asyncio
async def test_schema_functions_lists_and_filters_by_search():
    svc = MagicMock()
    svc.list_functions = AsyncMock(
        return_value=[
            {"name": "ai_query", "comment": "Call a model"},
            {"name": "ai_forecast", "comment": None},
            {"name": "python_exec", "comment": "Run python"},
        ]
    )

    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value="tok",
        ),
        patch(
            "src.services.databricks.workspace.service.DatabricksService",
            MagicMock(return_value=svc),
        ),
    ):
        result = await list_schema_functions(
            _request(),
            session=AsyncMock(),
            catalog="system",
            schema="ai",
            search="ai_",
            group_context=_admin_ctx(),
        )

    svc.list_functions.assert_awaited_with("system", "ai")
    # search matches on the function name (ai_query, ai_forecast) — python_exec drops out.
    assert [f["name"] for f in result["functions"]] == ["ai_query", "ai_forecast"]


@pytest.mark.asyncio
async def test_schema_functions_empty_without_workspace():
    with (
        patch(
            "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value=None,
        ),
    ):
        result = await list_schema_functions(
            _request(),
            session=AsyncMock(),
            catalog="system",
            schema="ai",
            group_context=_admin_ctx(),
        )
    assert result == {"functions": []}


@pytest.mark.asyncio
async def test_schema_functions_forbidden_for_non_admin():
    with pytest.raises(ForbiddenError):
        await list_schema_functions(
            _request(),
            session=AsyncMock(),
            catalog="system",
            schema="ai",
            group_context=_non_admin_ctx(),
        )


# ---------------------------------------------------------------------------
# /mcp/databricks/ai-search-indexes — step two
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ai_search_step_lists_indexes_across_endpoints():
    session_cm, session = _aiohttp_session(
        [
            (200, {"endpoints": [{"name": "ep1"}, {"name": "ep2"}, {}]}),
            (
                200,
                {
                    "vector_indexes": [
                        {"name": "main.gold.docs_idx"},
                        {"name": "bad-name"},
                    ]
                },
            ),
            (403, {}),
        ]
    )

    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value="tok",
        ),
        patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)),
    ):
        result = await list_ai_search_mcp_indexes(
            _request(), group_context=_admin_ctx()
        )

    assert result["options"] == [
        {
            "id": "ai-search:main.gold.docs_idx",
            "kind": "ai-search",
            "name": "main.gold.docs_idx",
            "description": "Endpoint: ep1",
            "server_url": "https://ws.example.com/api/2.0/mcp/ai-search/main/gold/docs_idx",
        }
    ]
    assert session.get.call_count == 3  # endpoints + 2 named endpoints' indexes


@pytest.mark.asyncio
async def test_ai_search_step_empty_on_endpoint_listing_error():
    session_cm, _ = _aiohttp_session([(500, {})])
    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value="tok",
        ),
        patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)),
    ):
        result = await list_ai_search_mcp_indexes(
            _request(), group_context=_admin_ctx()
        )
    assert result == {"options": []}


@pytest.mark.asyncio
async def test_ai_search_step_empty_without_workspace():
    with (
        patch(
            "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)
        ),
        patch(
            "src.utils.databricks_auth.extract_user_token_from_request",
            return_value=None,
        ),
    ):
        result = await list_ai_search_mcp_indexes(
            _request(), group_context=_admin_ctx()
        )
    assert result == {"options": []}


# ---------------------------------------------------------------------------
# External (connection-based) MCP listing — unchanged contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_external_options_only_include_mcp_flagged_http_connections():
    payload = {
        "connections": [
            {
                "name": "jira",
                "connection_type": "HTTP",
                "comment": "Jira MCP",
                "options": {"is_mcp": "true", "host": "https://jira.example.com"},
            },
            {"name": "plain-http", "connection_type": "HTTP", "options": {"host": "x"}},
            {
                "name": "warehouse",
                "connection_type": "SNOWFLAKE",
                "options": {"is_mcp": "true"},
            },
            {
                "name": "",
                "connection_type": "HTTP",
                "options": {"is_mcp_connection": "TRUE"},
            },
            # System AI-agent connections are AgentBricks-internal and stay
            # out of the picker — even the MCP-backed ones (slack/atlassian).
            {
                "name": "system_ai_agent_slack_mcp",
                "connection_type": "HTTP",
                "comment": "System-managed connection for AI agents.",
                "options": {
                    "is_mcp_connection": "false",
                    "host": "https://mcp.slack.com",
                    "base_path": "/mcp",
                },
            },
            {
                "name": "system_ai_agent_gmail",
                "connection_type": "HTTP",
                "comment": "System-managed connection for AI agents.",
                "options": {
                    "is_mcp_connection": "false",
                    "host": "https://www.googleapis.com",
                    "base_path": "/",
                },
            },
        ]
    }
    session_cm, session = _aiohttp_session([(200, payload)])

    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)),
    ):
        options = await _list_external_mcp_options("https://ws.example.com", "tok")

    assert options == [
        {
            "id": "external:jira",
            "kind": "external",
            "name": "jira",
            "description": "Jira MCP",
            "server_url": "https://ws.example.com/api/2.0/mcp/external/jira",
        }
    ]
    called_url = session.get.call_args.args[0]
    assert called_url == "https://ws.example.com/api/2.1/unity-catalog/connections"


@pytest.mark.asyncio
async def test_external_options_empty_on_http_error_or_missing_auth():
    session_cm, _ = _aiohttp_session([(403, {})])
    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)),
    ):
        assert await _list_external_mcp_options("https://ws.example.com", "tok") == []

    with patch(
        "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)
    ):
        assert await _list_external_mcp_options("https://ws.example.com", None) == []
