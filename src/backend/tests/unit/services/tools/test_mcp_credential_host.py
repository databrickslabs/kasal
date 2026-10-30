"""MCP servers get workspace credentials only on the credential's own workspace (V3-2).

The MCP server URL is tenant-configured. Credential forwarding used to trust any
``*.databricks.com`` / ``*.azuredatabricks.net`` / ``*.databricksapps.com`` host,
which includes workspaces and apps an attacker owns. Now only the credentialed
workspace host, or a Databricks App of the same workspace, receives the OBO, PAT
or SPN token; every other host gets no Databricks credential.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.exceptions import ForbiddenError
from src.schemas.mcp import MCPTestConnectionRequest
from src.services.databricks.workspace.host_guard import (
    assert_mcp_credential_host,
    is_workspace_app_host,
)
from src.services.mcp.mcp_client.service import MCPService
from src.services.tools.mcp_integration import MCPIntegration

OWN = "https://ws-own.cloud.databricks.com"
WSID = "1234567890"
OWN_APP = f"https://mcp-tools-{WSID}.aws.databricksapps.com/mcp"
FOREIGN = [
    "https://ws-other.cloud.databricks.com/api/2.0/mcp/genie/x",
    "https://adb-999.9.azuredatabricks.net/api/2.0/mcp/functions/a/b",
    "https://mcp-tools-999.aws.databricksapps.com/mcp",
    # Another workspace's app named to look like ours.
    f"https://mcp-tools-{WSID}-999.aws.databricksapps.com/mcp",
    f"https://mcp-tools-9{WSID}.aws.databricksapps.com/mcp",
    "https://attacker.example.com/mcp",
    f"https://mcp-tools-{WSID}.aws.databricksapps.com:8443/mcp",
]


@pytest.fixture
def workspace_id(monkeypatch):
    for var in ("DATABRICKS_APP_NAME", "DATABRICKS_APP_PORT"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATABRICKS_WORKSPACE_ID", WSID)


# ── host_guard ───────────────────────────────────────────────────────────────


def test_app_host_must_carry_this_workspace_id():
    assert is_workspace_app_host(OWN_APP, WSID)
    assert is_workspace_app_host(f"mcp-{WSID}.azure.databricksapps.com", WSID)
    for url in FOREIGN:
        assert not is_workspace_app_host(url, WSID), url
    assert not is_workspace_app_host(OWN_APP.replace("https", "http"), WSID)
    # No known workspace id: no app can be verified.
    assert not is_workspace_app_host(OWN_APP, "")


def test_app_host_needs_a_known_workspace_id(monkeypatch):
    for var in ("DATABRICKS_APP_NAME", "DATABRICKS_WORKSPACE_ID"):
        monkeypatch.delenv(var, raising=False)
    assert not is_workspace_app_host(OWN_APP)


@pytest.mark.usefixtures("workspace_id")
def test_credential_host_allows_own_workspace_and_own_apps_only():
    assert assert_mcp_credential_host(f"{OWN}/api/2.0/mcp/genie/x", OWN)
    assert assert_mcp_credential_host(OWN_APP, OWN)
    for url in FOREIGN:
        with pytest.raises(ForbiddenError):
            assert_mcp_credential_host(url, OWN)
    with pytest.raises(ForbiddenError):
        assert_mcp_credential_host(f"{OWN}/mcp".replace("https", "http"), OWN)
    with pytest.raises(ForbiddenError):  # no credentialed host at all
        assert_mcp_credential_host(OWN_APP, "")


# ── A run: MCPIntegration._create_tools_for_server ───────────────────────────


def _server(url, auth_type="databricks_obo"):
    return {"id": 1, "name": "srv", "server_url": url, "auth_type": auth_type}


async def _create(server):
    auth = SimpleNamespace(token="dapi-secret", workspace_url=OWN, auth_method="obo")
    adapter = MagicMock(tools=[], initialization_error=None)
    create_adapter = AsyncMock(return_value=adapter)
    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=auth),
        ),
        patch(
            "src.services.tools.mcp_handler.get_or_create_mcp_adapter", create_adapter
        ),
    ):
        await MCPIntegration._create_tools_for_server(
            server, "agent", MagicMock(), user_token="u", group_id="g"
        )
    return create_adapter


@pytest.mark.asyncio
@pytest.mark.usefixtures("workspace_id")
@pytest.mark.parametrize("url", [f"{OWN}/api/2.0/mcp/genie/x", OWN_APP])
async def test_run_sends_credential_to_own_workspace_and_apps(url):
    MCPIntegration.reset_warnings()
    create_adapter = await _create(_server(url))
    params = create_adapter.await_args.args[0]
    assert params["headers"]["Authorization"] == "Bearer dapi-secret"


@pytest.mark.asyncio
@pytest.mark.usefixtures("workspace_id")
@pytest.mark.parametrize("url", FOREIGN[:6])
async def test_run_refuses_credential_for_other_hosts(url):
    MCPIntegration.reset_warnings()
    create_adapter = await _create(_server(url))
    create_adapter.assert_not_awaited()
    assert any("refusing to send" in w for w in MCPIntegration.get_warnings())


@pytest.mark.asyncio
@pytest.mark.usefixtures("workspace_id")
async def test_api_key_server_is_unaffected():
    server = {**_server("https://mcp.example.com/sse", "api_key"), "api_key": "k"}
    create_adapter = await _create(server)
    params = create_adapter.await_args.args[0]
    assert params["headers"]["Authorization"] == "Bearer k"


# ── Test connection (admin UI) ───────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.usefixtures("workspace_id")
@pytest.mark.parametrize("server_type", ["sse", "streamable"])
async def test_connection_test_refuses_foreign_host(server_type):
    auth = SimpleNamespace(token="dapi-secret", workspace_url=OWN, auth_method="spn")
    request = MCPTestConnectionRequest(
        server_url=FOREIGN[0],
        api_key="",
        server_type=server_type,
        auth_type="databricks_spn",
        timeout_seconds=5,
    )
    with patch(
        "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=auth)
    ):
        result = await MCPService(MagicMock()).test_connection(request)
    assert result.success is False
    assert "only sent to this workspace" in result.message
