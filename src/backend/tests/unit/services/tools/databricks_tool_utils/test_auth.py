"""A tool's ``databricks_host`` never receives a credential (audit N1 / F3a / M4).

``databricks_host`` is in the LLM-facing args schema of the Databricks tools, and
in the Jobs tool's configuration. The credential resolved for the tool (OBO,
the group's PAT or the app's SPN token) is only valid on the auth context's
workspace, so the host may re-spell that workspace and nothing else.
"""

import contextvars
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.exceptions import ForbiddenError
from src.services.tools.databricks_dashboard_creator_tool import (
    DatabricksDashboardCreatorTool,
)
from src.services.tools.databricks_jobs_tool import DatabricksJobsTool
from src.services.tools.databricks_tool_utils import (
    apply_host_override,
    assert_tool_host,
    resolve_tool_auth,
)
from src.services.tools.genie_space_generator_tool import GenieSpaceGeneratorTool
from src.services.tools.metric_view_deployer_tool import MetricViewDeployerTool
from src.services.tools.metric_view_utils import uc_query
from src.services.tools.pbi_visual_ucmv_mapper_tool import PBIVisualUCMVMapperTool
from src.services.tools.ucmv_genie_config_generator_tool import (
    UCMVGenieConfigGeneratorTool,
)

WORKSPACE = "https://ws.cloud.databricks.com"
FOREIGN = [
    "https://attacker.example.net",
    "attacker.cloud.databricks.com",  # another Databricks workspace
    "http://ws.cloud.databricks.com",  # clear text
    "https://ws.cloud.databricks.com:8443",
]


def _auth(workspace_url=WORKSPACE):
    auth = MagicMock()
    auth.workspace_url = workspace_url
    auth.get_headers.return_value = {"Authorization": "Bearer SECRET"}
    return auth


def _patch_auth(auth):
    return patch(
        "src.utils.databricks_auth.get_auth_context", AsyncMock(return_value=auth)
    )


# ------------------------------------------------------------ shared helper


@pytest.mark.parametrize("host", FOREIGN)
def test_foreign_override_is_refused_and_auth_untouched(host):
    auth = _auth()
    with pytest.raises(ForbiddenError):
        apply_host_override(auth, host)
    assert auth.workspace_url == WORKSPACE


def test_override_may_respell_the_credentialed_workspace():
    auth = apply_host_override(_auth(), "WS.cloud.databricks.com/some/path")
    assert auth.workspace_url == WORKSPACE


def test_no_override_or_no_auth_is_a_no_op():
    auth = _auth()
    assert apply_host_override(auth, None) is auth
    assert apply_host_override(None, "https://attacker.example.net") is None


def test_resolve_tool_auth_refuses_foreign_host():
    with _patch_auth(_auth()), pytest.raises(ForbiddenError):
        resolve_tool_auth("https://attacker.example.net")


_GROUP = contextvars.ContextVar("group", default=None)


@pytest.mark.asyncio
async def test_resolve_tool_auth_keeps_context_inside_a_running_loop():
    """The old copies ran in a bare thread and lost the group's ContextVars."""
    seen = {}

    async def fake_auth(*args, **kwargs):
        seen["group"] = _GROUP.get()
        return _auth()

    _GROUP.set("g1")
    with patch("src.utils.databricks_auth.get_auth_context", fake_auth):
        auth = resolve_tool_auth(None)
    assert auth.workspace_url == WORKSPACE
    assert seen["group"] == "g1"


# ------------------------------------------------------- the five LLM tools


@pytest.mark.parametrize(
    "tool_cls",
    [
        GenieSpaceGeneratorTool,
        PBIVisualUCMVMapperTool,
        DatabricksDashboardCreatorTool,
        UCMVGenieConfigGeneratorTool,
        MetricViewDeployerTool,
    ],
)
def test_every_tool_authenticate_refuses_a_foreign_host(tool_cls):
    tool = tool_cls()
    with _patch_auth(_auth()):
        with pytest.raises(ForbiddenError):
            tool._authenticate(host_override="https://attacker.example.net")
        assert tool._authenticate(host_override=None).workspace_url == WORKSPACE


def test_genie_run_with_foreign_host_sends_nothing():
    requests_mock = MagicMock()
    with (
        _patch_auth(_auth()),
        patch.dict("sys.modules", {"requests": requests_mock}),
    ):
        result = GenieSpaceGeneratorTool()._run(
            warehouse_id="wh-123",
            space_title="Test",
            catalog="main",
            schema_name="m",
            databricks_host="https://attacker.example.net",
        )
    assert "configured Databricks workspace" in result
    requests_mock.post.assert_not_called()
    requests_mock.patch.assert_not_called()


@pytest.mark.asyncio
async def test_uc_query_refuses_foreign_override():
    with _patch_auth(_auth()), pytest.raises(uc_query.UCQueryError):
        await uc_query._auth_headers("https://attacker.example.net")


@pytest.mark.asyncio
async def test_uc_query_refuses_foreign_warehouse_endpoint_url():
    with _patch_auth(_auth()), pytest.raises(uc_query.UCQueryError):
        await uc_query.resolve_workspace_and_warehouse(
            "https://attacker.example.net/sql/1.0/warehouses/abc123"
        )


# ------------------------------------------------------------ jobs tool (M4)


def _pin_credentialed(host=WORKSPACE):
    return patch(
        "src.services.databricks.workspace.host_guard.credentialed_workspace_host",
        AsyncMock(return_value=host),
    )


@pytest.mark.asyncio
async def test_assert_tool_host_accepts_only_the_credentialed_workspace():
    with _pin_credentialed():
        assert await assert_tool_host("WS.cloud.databricks.com") == (
            "ws.cloud.databricks.com"
        )
        with pytest.raises(ForbiddenError):
            await assert_tool_host("attacker.cloud.databricks.com")


@pytest.mark.asyncio
async def test_assert_tool_host_refuses_when_nothing_is_configured():
    with _pin_credentialed(""), pytest.raises(ForbiddenError):
        await assert_tool_host("ws.cloud.databricks.com")


def _jobs_tool(host):
    return DatabricksJobsTool(
        tool_config={"DATABRICKS_API_KEY": "dapi-secret", "DATABRICKS_HOST": host},
        group_id="g1",
    )


@pytest.mark.asyncio
async def test_jobs_tool_never_sends_the_token_to_a_configured_foreign_host():
    tool = _jobs_tool("https://attacker.example.net")
    with (
        _pin_credentialed(),
        patch("src.services.tools.databricks_jobs_tool.aiohttp.ClientSession") as cs,
    ):
        with pytest.raises(ForbiddenError):
            await tool._make_api_call("GET", "/api/2.2/jobs/list")
    cs.assert_not_called()


@pytest.mark.asyncio
async def test_jobs_tool_calls_the_credentialed_workspace():
    tool = _jobs_tool("https://WS.cloud.databricks.com/")
    response = MagicMock(status=200)
    response.text = AsyncMock(return_value="{}")
    response.json = AsyncMock(return_value={"jobs": []})
    request_cm = MagicMock()
    request_cm.__aenter__ = AsyncMock(return_value=response)
    request_cm.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.request = MagicMock(return_value=request_cm)
    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=False)
    with (
        _pin_credentialed(),
        patch(
            "src.services.tools.databricks_jobs_tool.aiohttp.ClientSession",
            return_value=session_cm,
        ),
    ):
        assert await tool._make_api_call("GET", "/api/2.2/jobs/list") == {"jobs": []}
    assert session.request.call_args.kwargs["url"].startswith(
        "https://ws.cloud.databricks.com/api/2.2/jobs/list"
    )


def test_jobs_tool_does_not_log_token_fragments(caplog):
    caplog.set_level("DEBUG")
    _jobs_tool(WORKSPACE)
    assert "dapi" not in caplog.text
    assert "cret" not in caplog.text
