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
    list_genie_mcp_spaces,
)


def _request():
    req = MagicMock()
    req.headers = {}
    return req


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
        patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=_auth("https://ws.example.com/"))),
        patch("src.utils.databricks_auth.extract_user_token_from_request", return_value="tok"),
        patch(
            "src.api.mcp_router._list_external_mcp_options",
            AsyncMock(return_value=[
                {
                    "id": "external:jira",
                    "kind": "external",
                    "name": "jira",
                    "description": "Jira MCP",
                    "server_url": "https://ws.example.com/api/2.0/mcp/external/jira",
                },
            ]),
        ),
        patch(
            "src.repositories.databricks_config_repository.DatabricksConfigRepository",
            MagicMock(return_value=config_repo),
        ),
    ):
        result = await get_databricks_mcp_options(
            _request(), session=AsyncMock(), group_context=None
        )

    assert result["workspace_url"] == "https://ws.example.com"
    # External (connection-based) servers come grouped on their own.
    assert [o["id"] for o in result["external"]] == ["external:jira"]

    managed = {o["id"]: o for o in result["managed"]}
    # Leaves are directly selectable.
    assert managed["sql"]["server_url"] == "https://ws.example.com/api/2.0/mcp/sql"
    assert managed["sql"]["expandable"] is False
    assert (
        managed["functions:main.gold"]["server_url"]
        == "https://ws.example.com/api/2.0/mcp/functions/main/gold"
    )
    assert managed["functions:main.gold"]["name"] == "Unity Catalog Functions (main.gold)"
    # The built-in system.ai functions (python_exec, …) are always offered.
    assert (
        managed["functions:system.ai"]["server_url"]
        == "https://ws.example.com/api/2.0/mcp/functions/system/ai"
    )
    assert managed["functions:system.ai"]["expandable"] is False
    # Two-step types carry NO instance list (Genie can have 1000s of spaces).
    assert managed["genie"]["expandable"] is True
    assert "server_url" not in managed["genie"]
    assert managed["ai-search"]["expandable"] is True


@pytest.mark.asyncio
async def test_catalog_omits_functions_leaf_without_configured_catalog_schema():
    config_repo = MagicMock()
    config_repo.get_active_config = AsyncMock(return_value=None)

    with (
        patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=_auth())),
        patch("src.utils.databricks_auth.extract_user_token_from_request", return_value=None),
        patch("src.api.mcp_router._list_external_mcp_options", AsyncMock(return_value=[])),
        patch(
            "src.repositories.databricks_config_repository.DatabricksConfigRepository",
            MagicMock(return_value=config_repo),
        ),
    ):
        result = await get_databricks_mcp_options(
            _request(), session=AsyncMock(), group_context=None
        )

    ids = [o["id"] for o in result["managed"]]
    # No config-derived schema leaf — but the built-in system.ai one stays.
    assert ids == ["sql", "functions:system.ai", "genie", "ai-search"]


@pytest.mark.asyncio
async def test_catalog_does_not_duplicate_functions_leaf_when_config_is_system_ai():
    config = SimpleNamespace(catalog="system", schema="ai")
    config_repo = MagicMock()
    config_repo.get_active_config = AsyncMock(return_value=config)

    with (
        patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=_auth())),
        patch("src.utils.databricks_auth.extract_user_token_from_request", return_value="tok"),
        patch("src.api.mcp_router._list_external_mcp_options", AsyncMock(return_value=[])),
        patch(
            "src.repositories.databricks_config_repository.DatabricksConfigRepository",
            MagicMock(return_value=config_repo),
        ),
    ):
        result = await get_databricks_mcp_options(
            _request(), session=AsyncMock(), group_context=None
        )

    ids = [o["id"] for o in result["managed"]]
    assert ids.count("functions:system.ai") == 1


@pytest.mark.asyncio
async def test_catalog_empty_without_workspace_url():
    with (
        patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)),
        patch("src.utils.databricks_auth.extract_user_token_from_request", return_value=None),
    ):
        result = await get_databricks_mcp_options(
            _request(), session=AsyncMock(), group_context=None
        )

    assert result == {"workspace_url": "", "external": [], "managed": []}


@pytest.mark.asyncio
async def test_catalog_survives_external_and_config_failures():
    config_repo = MagicMock()
    config_repo.get_active_config = AsyncMock(side_effect=Exception("db down"))

    with (
        patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=_auth())),
        patch("src.utils.databricks_auth.extract_user_token_from_request", return_value="tok"),
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
            _request(), session=AsyncMock(), group_context=None
        )

    assert result["external"] == []
    assert [o["id"] for o in result["managed"]] == [
        "sql",
        "functions:system.ai",
        "genie",
        "ai-search",
    ]


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
        patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=_auth())),
        patch("src.utils.databricks_auth.extract_user_token_from_request", return_value="tok"),
        patch("src.services.genie_service.GenieService", MagicMock(return_value=genie_service)),
    ):
        result = await list_genie_mcp_spaces(
            _request(), search="sales", page_token=None, group_context=None
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
        patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)),
        patch("src.utils.databricks_auth.extract_user_token_from_request", return_value=None),
    ):
        result = await list_genie_mcp_spaces(_request(), group_context=None)
    assert result == {"options": [], "next_page_token": None}


# ---------------------------------------------------------------------------
# /mcp/databricks/ai-search-indexes — step two
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ai_search_step_lists_indexes_across_endpoints():
    session_cm, session = _aiohttp_session([
        (200, {"endpoints": [{"name": "ep1"}, {"name": "ep2"}, {}]}),
        (200, {"vector_indexes": [{"name": "main.gold.docs_idx"}, {"name": "bad-name"}]}),
        (403, {}),
    ])

    with (
        patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=_auth())),
        patch("src.utils.databricks_auth.extract_user_token_from_request", return_value="tok"),
        patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)),
    ):
        result = await list_ai_search_mcp_indexes(_request(), group_context=None)

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
        patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=_auth())),
        patch("src.utils.databricks_auth.extract_user_token_from_request", return_value="tok"),
        patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)),
    ):
        result = await list_ai_search_mcp_indexes(_request(), group_context=None)
    assert result == {"options": []}


@pytest.mark.asyncio
async def test_ai_search_step_empty_without_workspace():
    with (
        patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)),
        patch("src.utils.databricks_auth.extract_user_token_from_request", return_value=None),
    ):
        result = await list_ai_search_mcp_indexes(_request(), group_context=None)
    assert result == {"options": []}


# ---------------------------------------------------------------------------
# External (connection-based) MCP listing — unchanged contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_external_options_only_include_mcp_flagged_http_connections():
    payload = {
        "connections": [
            {"name": "jira", "connection_type": "HTTP", "comment": "Jira MCP",
             "options": {"is_mcp": "true", "host": "https://jira.example.com"}},
            {"name": "plain-http", "connection_type": "HTTP", "options": {"host": "x"}},
            {"name": "warehouse", "connection_type": "SNOWFLAKE", "options": {"is_mcp": "true"}},
            {"name": "", "connection_type": "HTTP", "options": {"is_mcp_connection": "TRUE"}},
            # System AI-agent connections are AgentBricks-internal and stay
            # out of the picker — even the MCP-backed ones (slack/atlassian).
            {"name": "system_ai_agent_slack_mcp", "connection_type": "HTTP",
             "comment": "System-managed connection for AI agents.",
             "options": {"is_mcp_connection": "false",
                         "host": "https://mcp.slack.com", "base_path": "/mcp"}},
            {"name": "system_ai_agent_gmail", "connection_type": "HTTP",
             "comment": "System-managed connection for AI agents.",
             "options": {"is_mcp_connection": "false",
                         "host": "https://www.googleapis.com", "base_path": "/"}},
        ]
    }
    session_cm, session = _aiohttp_session([(200, payload)])

    with (
        patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=_auth())),
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
        patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=_auth())),
        patch("aiohttp.ClientSession", MagicMock(return_value=session_cm)),
    ):
        assert await _list_external_mcp_options("https://ws.example.com", "tok") == []

    with patch("src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=None)):
        assert await _list_external_mcp_options("https://ws.example.com", None) == []
