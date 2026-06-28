"""Tests for LakebaseSessionFactory._refresh_token and get_connection_string autoscaling fallback."""
import sys
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import uuid



@pytest.fixture
def factory():
    from src.db.lakebase_session import LakebaseSessionFactory
    f = LakebaseSessionFactory(instance_name="test-inst", user_email="u@example.com")
    return f


# ---- _refresh_token ----

@pytest.mark.asyncio
async def test_refresh_token_provisioned(factory):
    """_refresh_token succeeds via provisioned DatabaseAPI."""
    mock_w = MagicMock()
    cred = MagicMock()
    cred.token = "prov-tok"
    mock_w.database.generate_database_credential.return_value = cred
    factory._get_workspace_client = AsyncMock(return_value=mock_w)

    token = await factory._refresh_token()
    assert token == "prov-tok"
    assert factory._token_holder["token"] == "prov-tok"
    assert factory._token_holder["refreshed_at"] > 0


@pytest.mark.asyncio
async def test_refresh_token_fallback_autoscaling(factory):
    """_refresh_token falls back to PostgresAPI when provisioned not found."""
    mock_w = MagicMock()
    mock_w.database.generate_database_credential.side_effect = Exception("not found")

    ep = MagicMock()
    ep.name = "projects/test-inst/branches/production/endpoints/ep1"
    mock_w.postgres.list_endpoints.return_value = [ep]

    auto_cred = MagicMock()
    auto_cred.token = "auto-tok"
    mock_w.postgres.generate_database_credential.return_value = auto_cred

    factory._get_workspace_client = AsyncMock(return_value=mock_w)

    token = await factory._refresh_token()
    assert token == "auto-tok"
    assert factory._token_holder["token"] == "auto-tok"
    mock_w.postgres.generate_database_credential.assert_called_once_with(
        endpoint="projects/test-inst/branches/production/endpoints/ep1"
    )


@pytest.mark.asyncio
async def test_refresh_token_provisioned_non_not_found_error(factory):
    """_refresh_token raises if provisioned error is not 'not found'."""
    mock_w = MagicMock()
    mock_w.database.generate_database_credential.side_effect = RuntimeError("internal error")
    factory._get_workspace_client = AsyncMock(return_value=mock_w)

    with pytest.raises(RuntimeError, match="internal error"):
        await factory._refresh_token()


@pytest.mark.asyncio
async def test_refresh_token_autoscaling_no_endpoints(factory):
    """_refresh_token raises when autoscaling has no endpoints."""
    mock_w = MagicMock()
    mock_w.database.generate_database_credential.side_effect = Exception("not found")
    mock_w.postgres.list_endpoints.return_value = []
    factory._get_workspace_client = AsyncMock(return_value=mock_w)

    with pytest.raises(ValueError, match="No endpoints found"):
        await factory._refresh_token()


# ---- get_connection_string ----

@pytest.mark.asyncio
async def test_get_connection_string_provisioned(factory):
    """get_connection_string resolves DNS from provisioned instance."""
    mock_w = MagicMock()
    inst = MagicMock()
    inst.state = "AVAILABLE"
    inst.read_write_dns = "prov-dns.example.com"
    mock_w.database.get_database_instance.return_value = inst

    cred = MagicMock()
    cred.token = "some-token"
    mock_w.database.generate_database_credential.return_value = cred

    factory._get_workspace_client = AsyncMock(return_value=mock_w)

    with patch.dict(os.environ, {}, clear=False):
        # Ensure DATABRICKS_CLIENT_ID is set so _get_username uses it
        with patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "spn-id"}):
            url = await factory.get_connection_string()

    assert "prov-dns.example.com" in url
    assert "spn-id" in url
    assert "postgresql+asyncpg" in url


@pytest.mark.asyncio
async def test_get_connection_string_fallback_autoscaling(factory):
    """get_connection_string falls back to autoscaling endpoint when provisioned not found."""
    mock_w = MagicMock()
    # Provisioned get_instance fails
    mock_w.database.get_database_instance.side_effect = Exception("not found")

    # Autoscaling endpoint
    ep = MagicMock()
    ep.status = MagicMock()
    ep.status.hosts = MagicMock()
    ep.status.hosts.host = "auto-dns.example.com"
    ep.name = "projects/test-inst/branches/production/endpoints/ep1"
    mock_w.postgres.list_endpoints.return_value = [ep]

    # Token refresh: provisioned fails, autoscaling succeeds
    mock_w.database.generate_database_credential.side_effect = Exception("not found")
    auto_cred = MagicMock()
    auto_cred.token = "auto-token"
    mock_w.postgres.generate_database_credential.return_value = auto_cred

    factory._get_workspace_client = AsyncMock(return_value=mock_w)

    with patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "spn-id"}):
        url = await factory.get_connection_string()

    assert "auto-dns.example.com" in url


@pytest.mark.asyncio
async def test_get_connection_string_no_dns_anywhere(factory):
    """get_connection_string raises when no DNS can be found."""
    mock_w = MagicMock()
    mock_w.database.get_database_instance.side_effect = Exception("not found")
    # Autoscaling endpoints with no DNS
    ep = MagicMock()
    ep.status = None
    mock_w.postgres.list_endpoints.return_value = [ep]

    factory._get_workspace_client = AsyncMock(return_value=mock_w)

    with patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "spn-id"}):
        with pytest.raises(ValueError, match="No endpoint found"):
            await factory.get_connection_string()


@pytest.mark.asyncio
async def test_get_connection_string_instance_not_ready(factory):
    """get_connection_string raises when instance is not in ready state."""
    mock_w = MagicMock()
    inst = MagicMock()
    inst.state = "STOPPED"
    inst.read_write_dns = "dns"
    mock_w.database.get_database_instance.return_value = inst

    factory._get_workspace_client = AsyncMock(return_value=mock_w)

    with patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "spn-id"}):
        with pytest.raises(ValueError, match="not ready"):
            await factory.get_connection_string()


# ---- legible error when the instance exists in NEITHER API ----


@pytest.mark.asyncio
async def test_get_connection_string_instance_unavailable_everywhere(factory):
    """When both APIs report 'not found', a legible LakebaseInstanceUnavailableError
    is raised (not the raw SDK NotFound that surfaces inside the memory drain)."""
    from src.core.exceptions import LakebaseInstanceUnavailableError

    mock_w = MagicMock()
    mock_w.config.host = "https://my-workspace.example.com"
    # Provisioned API: not found
    mock_w.database.get_database_instance.side_effect = Exception("instance not found")
    # Autoscaling API: project does not exist either
    mock_w.postgres.list_endpoints.side_effect = Exception(
        "Project with name 'projects/test-inst' not found"
    )
    factory._get_workspace_client = AsyncMock(return_value=mock_w)

    with patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "spn-id"}):
        with pytest.raises(LakebaseInstanceUnavailableError) as exc_info:
            await factory.get_connection_string()

    msg = str(exc_info.value)
    # Message must name the instance, the workspace, and how to fix it.
    assert "test-inst" in msg
    assert "my-workspace.example.com" in msg
    assert "not provisioned" in msg
    # Original SDK error is chained for debugging.
    assert exc_info.value.__cause__ is not None
    # Wired into the Kasal exception hierarchy so the global handler maps it to 503.
    from src.core.exceptions import KasalError, LakebaseUnavailableError

    assert isinstance(exc_info.value, KasalError)
    assert isinstance(exc_info.value, LakebaseUnavailableError)
    assert exc_info.value.status_code == 503


@pytest.mark.asyncio
async def test_get_connection_string_autoscaling_non_not_found_propagates(factory):
    """A non-'not found' error from list_endpoints is NOT wrapped — it propagates raw."""
    from src.core.exceptions import LakebaseInstanceUnavailableError

    mock_w = MagicMock()
    mock_w.database.get_database_instance.side_effect = Exception("not found")
    mock_w.postgres.list_endpoints.side_effect = RuntimeError("permission denied")
    factory._get_workspace_client = AsyncMock(return_value=mock_w)

    with patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "spn-id"}):
        with pytest.raises(RuntimeError, match="permission denied") as exc_info:
            await factory.get_connection_string()
    assert not isinstance(exc_info.value, LakebaseInstanceUnavailableError)


@pytest.mark.asyncio
async def test_refresh_token_instance_unavailable_everywhere(factory):
    """_refresh_token raises a legible LakebaseInstanceUnavailableError when neither
    the provisioned nor the autoscaling API can resolve the instance."""
    from src.core.exceptions import LakebaseInstanceUnavailableError

    mock_w = MagicMock()
    mock_w.config.host = "https://my-workspace.example.com"
    mock_w.database.generate_database_credential.side_effect = Exception("not found")
    mock_w.postgres.list_endpoints.side_effect = Exception(
        "Project with name 'projects/test-inst' not found"
    )
    factory._get_workspace_client = AsyncMock(return_value=mock_w)

    with pytest.raises(LakebaseInstanceUnavailableError) as exc_info:
        await factory._refresh_token()

    msg = str(exc_info.value)
    assert "test-inst" in msg
    assert exc_info.value.__cause__ is not None
