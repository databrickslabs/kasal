"""The ``?host=`` override on the workspace listing endpoints (audit C2).

The listing calls attach the server's Databricks credential (OBO / PAT / SPN)
to the outbound request. These tests pin that the credential never goes to a
host other than the configured workspace, that the upstream body is not
reflected to the caller, and that only admins/editors may use the override.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.api.databricks_router import list_catalogs, list_schemas, list_warehouses
from src.core.exceptions import ForbiddenError, KasalError
from src.services.databricks.workspace.host_guard import (
    assert_host_is_configured_workspace,
    normalize_workspace_host,
)
from src.services.databricks.workspace.service import DatabricksService
from src.utils.user_context import GroupContext

CONFIGURED = "https://ws.example.com"


def _service(configured=CONFIGURED):
    with patch("src.services.databricks.workspace.service.DatabricksConfigRepository"):
        svc = DatabricksService(session=MagicMock(), group_id="g1")
    cfg = MagicMock()
    cfg.workspace_url = configured
    svc.repository = AsyncMock()
    svc.repository.get_active_config = AsyncMock(return_value=cfg)
    return svc


def _auth(workspace_url=CONFIGURED):
    auth = MagicMock()
    auth.workspace_url = workspace_url
    auth.get_headers.return_value = {"Authorization": "Bearer SECRET"}
    return auth


def _gc(role):
    return GroupContext(
        group_ids=["g1"],
        group_email=f"{role}@x.com",
        email_domain="x.com",
        user_role=role,
    )


# --------------------------------------------------------------------- guard


@pytest.mark.parametrize(
    "host",
    [
        "ws.example.com",
        "https://ws.example.com",
        "https://WS.example.com/",
        "https://ws.example.com:443/some/path",
        "  ws.example.com.  ",
    ],
)
def test_guard_accepts_configured_host_in_any_spelling(host):
    assert_host_is_configured_workspace(host, [CONFIGURED])


@pytest.mark.parametrize(
    "host",
    [
        "https://attacker.example.net",
        "https://ws.example.com.attacker.net",
        "https://attacker.cloud.databricks.com",  # a Databricks host is not enough
        "http://ws.example.com",  # no clear-text credential
        "https://ws.example.com:8443",
        "https://169.254.169.254",
        "",
    ],
)
def test_guard_refuses_any_other_host(host):
    with pytest.raises(ForbiddenError):
        assert_host_is_configured_workspace(host, [CONFIGURED])


def test_guard_refuses_when_nothing_is_configured():
    with pytest.raises(ForbiddenError):
        assert_host_is_configured_workspace("ws.example.com", [None, ""])


def test_normalize_workspace_host():
    assert normalize_workspace_host("https://A.b.com/x") == "a.b.com"
    assert normalize_workspace_host("a.b.com:8443") == "a.b.com:8443"
    assert normalize_workspace_host(None) == ""


# ------------------------------------------------------------------- service


@pytest.mark.asyncio
async def test_foreign_host_never_receives_the_credential():
    svc = _service()
    client = MagicMock()
    client.get = AsyncMock()
    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch("httpx.AsyncClient") as mock_client_cls,
    ):
        mock_client_cls.return_value.__aenter__.return_value = client
        with pytest.raises(ForbiddenError):
            await svc.list_warehouses(host="https://attacker.example.net")
        with pytest.raises(ForbiddenError):
            await svc.list_catalogs(host="attacker.example.net")
        with pytest.raises(ForbiddenError):
            await svc.list_schemas("main", host="https://169.254.169.254")
    client.get.assert_not_called()


@pytest.mark.asyncio
async def test_configured_host_override_is_used_normalised():
    svc = _service()
    with patch(
        "src.utils.databricks_auth.get_auth_context",
        AsyncMock(return_value=_auth()),
    ):
        headers, url = await svc._resolve_workspace_url_and_headers(
            "https://WS.example.com/ignored/path"
        )
    assert url == "https://ws.example.com"
    assert headers["Authorization"] == "Bearer SECRET"


@pytest.mark.asyncio
async def test_host_matching_auth_workspace_is_accepted_without_db_config():
    svc = _service(configured=None)
    with (
        patch.dict("os.environ", {}, clear=False) as env,
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth("https://auth-ws.example.com")),
        ),
    ):
        env.pop("DATABRICKS_HOST", None)
        _, url = await svc._resolve_workspace_url_and_headers("auth-ws.example.com")
    assert url == "https://auth-ws.example.com"


@pytest.mark.asyncio
async def test_no_host_uses_configured_workspace():
    svc = _service()
    with patch(
        "src.utils.databricks_auth.get_auth_context",
        AsyncMock(return_value=_auth()),
    ):
        _, url = await svc._resolve_workspace_url_and_headers(None)
    assert url == CONFIGURED


@pytest.mark.asyncio
async def test_upstream_error_body_is_not_reflected():
    svc = _service()
    response = MagicMock(status_code=500, text="internal-secret-body")
    client = MagicMock()
    client.get = AsyncMock(return_value=response)
    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch("httpx.AsyncClient") as mock_client_cls,
    ):
        mock_client_cls.return_value.__aenter__.return_value = client
        with pytest.raises(KasalError) as exc:
            await svc.list_catalogs()
    assert "internal-secret-body" not in str(exc.value.detail)
    assert "500" in str(exc.value.detail)


# -------------------------------------------------------------------- router


@pytest.mark.asyncio
async def test_operator_cannot_use_host_override():
    svc = AsyncMock()
    with pytest.raises(ForbiddenError):
        await list_warehouses(
            service=svc, group_context=_gc("operator"), host=CONFIGURED
        )
    with pytest.raises(ForbiddenError):
        await list_catalogs(service=svc, group_context=_gc("operator"), host=CONFIGURED)
    with pytest.raises(ForbiddenError):
        await list_schemas(
            service=svc, group_context=_gc("operator"), catalog="c", host=CONFIGURED
        )
    svc.list_warehouses.assert_not_called()
    svc.list_catalogs.assert_not_called()
    svc.list_schemas.assert_not_called()


@pytest.mark.asyncio
async def test_operator_can_still_list_without_override():
    svc = AsyncMock()
    svc.list_catalogs.return_value = ["main"]
    result = await list_catalogs(service=svc, group_context=_gc("operator"), host=None)
    assert result == ["main"]
    svc.list_catalogs.assert_awaited_once_with(host=None)


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["admin", "editor"])
async def test_admin_and_editor_can_pass_host(role):
    svc = AsyncMock()
    svc.list_schemas.return_value = ["s"]
    result = await list_schemas(
        service=svc, group_context=_gc(role), catalog="c", host=CONFIGURED
    )
    assert result == ["s"]
    svc.list_schemas.assert_awaited_once_with(catalog="c", host=CONFIGURED)
