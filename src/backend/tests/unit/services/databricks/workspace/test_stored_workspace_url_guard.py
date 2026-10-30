"""A team's stored ``workspace_url`` never receives a credential (audit N1).

``POST /databricks/config`` is workspace-admin only, but whatever it stores is
where every team member's OBO token (and, failing that, the group PAT or the
app's SPN token) is sent by the warehouse/catalog/schema pickers. These tests
pin that only the workspace the credentials belong to can be stored, and that a
row which somehow names another host is refused at send time too.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.databricks_app import DatabricksAppInstallation
from src.core.exceptions import ForbiddenError
from src.services.databricks.workspace import host_guard
from src.services.databricks.workspace.host_guard import (
    credentialed_workspace_host,
    validate_stored_workspace_url,
)
from src.services.databricks.workspace.service import DatabricksService

TRUSTED = "https://ws.cloud.databricks.com"
FOREIGN = "https://attacker.cloud.databricks.com"


def _pin_trusted(host=TRUSTED):
    return patch.object(
        host_guard, "credentialed_workspace_host", AsyncMock(return_value=host)
    )


def _service(stored=TRUSTED):
    with patch("src.services.databricks.workspace.service.DatabricksConfigRepository"):
        svc = DatabricksService(session=MagicMock(), group_id="g1")
    cfg = MagicMock()
    cfg.workspace_url = stored
    cfg.is_enabled = True
    cfg.warehouse_id, cfg.catalog, cfg.schema = "wh", "main", "default"
    svc.repository = AsyncMock()
    svc.repository.get_active_config = AsyncMock(return_value=cfg)
    return svc


def _auth(workspace_url=TRUSTED):
    auth = MagicMock()
    auth.workspace_url = workspace_url
    auth.get_headers.return_value = {"Authorization": "Bearer SECRET"}
    return auth


def _not_hosted():
    return patch(
        "src.services.databricks.workspace.service.DatabricksAppInstallation.from_env",
        return_value=DatabricksAppInstallation(hosted=False),
    )


# ------------------------------------------------------------ write validation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value", ["ws.cloud.databricks.com", "https://WS.cloud.databricks.com/x/"]
)
async def test_credentialed_workspace_is_stored_normalised(value):
    with _pin_trusted():
        assert await validate_stored_workspace_url(value) == TRUSTED


@pytest.mark.asyncio
async def test_empty_workspace_url_is_allowed():
    with _pin_trusted():
        assert await validate_stored_workspace_url("  ") == ""


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [
        FOREIGN,  # a Databricks domain is not enough
        "https://attacker.example.net",
        "http://ws.cloud.databricks.com",  # clear text
        "https://ws.cloud.databricks.com:8443",
        "https://ws.cloud.databricks.com.attacker.net",
    ],
)
async def test_foreign_workspace_url_is_refused(value):
    with _pin_trusted(), pytest.raises(ForbiddenError):
        await validate_stored_workspace_url(value)


@pytest.mark.asyncio
async def test_fresh_install_accepts_only_a_databricks_domain():
    with _pin_trusted(""):
        assert await validate_stored_workspace_url(FOREIGN) == FOREIGN
        with pytest.raises(ForbiddenError):
            await validate_stored_workspace_url("https://attacker.example.net")


@pytest.mark.asyncio
async def test_hosted_installation_host_is_the_credentialed_host():
    installation = DatabricksAppInstallation(hosted=True, host=TRUSTED)
    with patch.object(
        host_guard.DatabricksAppInstallation, "from_env", return_value=installation
    ):
        assert await credentialed_workspace_host() == TRUSTED


@pytest.mark.asyncio
async def test_local_uses_auth_context_host():
    with (
        patch.object(
            host_guard.DatabricksAppInstallation,
            "from_env",
            return_value=DatabricksAppInstallation(hosted=False),
        ),
        patch(
            "src.utils.databricks_auth._databricks_auth.get_workspace_url",
            AsyncMock(return_value=TRUSTED),
        ),
    ):
        assert await credentialed_workspace_host() == TRUSTED


@pytest.mark.asyncio
async def test_set_config_refuses_foreign_url_before_writing():
    svc = _service()
    svc.repository.create_config = AsyncMock()
    config_in = MagicMock(spec=[])
    config_in.workspace_url = FOREIGN
    with _pin_trusted(), pytest.raises(ForbiddenError):
        await svc.set_databricks_config(config_in)
    svc.repository.create_config.assert_not_called()


# ------------------------------------------------------------- send-time check


@pytest.mark.asyncio
async def test_stored_foreign_url_never_receives_the_credential():
    svc = _service(stored=FOREIGN)
    client = MagicMock()
    client.get = AsyncMock()
    with (
        _not_hosted(),
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch("httpx.AsyncClient") as mock_client_cls,
    ):
        mock_client_cls.return_value.__aenter__.return_value = client
        with pytest.raises(ForbiddenError):
            await svc.list_warehouses()
        with pytest.raises(ForbiddenError):
            await svc.get_workspace_auth()
    client.get.assert_not_called()


@pytest.mark.asyncio
async def test_host_override_matching_only_the_stored_row_is_refused():
    """The stored row is no longer an allowed ``?host=`` on its own."""
    svc = _service(stored=FOREIGN)
    with (
        _not_hosted(),
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        pytest.raises(ForbiddenError),
    ):
        await svc._resolve_workspace_url_and_headers(FOREIGN)


@pytest.mark.asyncio
async def test_hosted_ignores_the_stored_row():
    svc = _service(stored=FOREIGN)
    installation = DatabricksAppInstallation(hosted=True, host=TRUSTED)
    with (
        patch(
            "src.services.databricks.workspace.service.DatabricksAppInstallation.from_env",
            return_value=installation,
        ),
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
    ):
        headers, url = await svc._resolve_workspace_url_and_headers()
    assert url == TRUSTED
    svc.repository.get_active_config.assert_not_called()


@pytest.mark.asyncio
async def test_connection_check_refuses_foreign_stored_url():
    svc = _service(stored=FOREIGN)
    client = MagicMock()
    client.get = AsyncMock()
    with (
        _not_hosted(),
        patch(
            "src.utils.databricks_auth.get_auth_context",
            AsyncMock(return_value=_auth()),
        ),
        patch(
            "src.services.databricks.workspace.service.httpx.AsyncClient"
        ) as mock_client_cls,
    ):
        mock_client_cls.return_value.__aenter__.return_value = client
        result = await svc.check_databricks_connection()
    assert result["connected"] is False
    client.get.assert_not_called()
