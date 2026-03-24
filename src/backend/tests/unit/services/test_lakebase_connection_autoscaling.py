"""Tests for LakebaseConnectionService.generate_credentials - provisioned + autoscaling fallback."""
import sys
import os
import uuid
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, "/Users/nehme.tohme/workspace/kasal/src/backend")


@pytest.fixture
def service():
    from src.services.lakebase_connection_service import LakebaseConnectionService
    svc = LakebaseConnectionService(user_token="tok", user_email="u@example.com")
    return svc


# ---- generate_credentials ----

@pytest.mark.asyncio
async def test_generate_credentials_provisioned_success(service):
    """generate_credentials returns credential from provisioned DatabaseAPI."""
    mock_w = MagicMock()
    cred = MagicMock()
    cred.token = "prov-token-abc"
    mock_w.database.generate_database_credential.return_value = cred
    service.get_workspace_client = AsyncMock(return_value=mock_w)

    result = await service.generate_credentials("my-instance")
    assert result.token == "prov-token-abc"
    mock_w.database.generate_database_credential.assert_called_once()


@pytest.mark.asyncio
async def test_generate_credentials_fallback_to_autoscaling(service):
    """generate_credentials falls back to PostgresAPI when provisioned not found."""
    mock_w = MagicMock()
    # Provisioned raises 'not found'
    mock_w.database.generate_database_credential.side_effect = Exception("instance not found")

    # Autoscaling endpoint and credential
    ep = MagicMock()
    ep.name = "projects/proj/branches/production/endpoints/ep1"
    mock_w.postgres.list_endpoints.return_value = [ep]

    auto_cred = MagicMock()
    auto_cred.token = "auto-token-xyz"
    mock_w.postgres.generate_database_credential.return_value = auto_cred

    service.get_workspace_client = AsyncMock(return_value=mock_w)

    result = await service.generate_credentials("proj")
    assert result.token == "auto-token-xyz"
    mock_w.postgres.generate_database_credential.assert_called_once_with(
        endpoint="projects/proj/branches/production/endpoints/ep1"
    )


@pytest.mark.asyncio
async def test_generate_credentials_provisioned_non_not_found_error(service):
    """generate_credentials raises if provisioned error is not 'not found'."""
    mock_w = MagicMock()
    mock_w.database.generate_database_credential.side_effect = RuntimeError("server error 500")
    service.get_workspace_client = AsyncMock(return_value=mock_w)

    with pytest.raises(RuntimeError, match="server error 500"):
        await service.generate_credentials("inst")


@pytest.mark.asyncio
async def test_generate_credentials_autoscaling_no_endpoints(service):
    """generate_credentials raises when autoscaling has no endpoints."""
    mock_w = MagicMock()
    mock_w.database.generate_database_credential.side_effect = Exception("not found")
    mock_w.postgres.list_endpoints.return_value = []
    service.get_workspace_client = AsyncMock(return_value=mock_w)

    with pytest.raises(ValueError, match="No endpoints found"):
        await service.generate_credentials("proj-no-ep")


@pytest.mark.asyncio
async def test_generate_credentials_autoscaling_failure(service):
    """generate_credentials raises when autoscaling cred generation fails."""
    mock_w = MagicMock()
    mock_w.database.generate_database_credential.side_effect = Exception("not found")

    ep = MagicMock()
    ep.name = "projects/proj/branches/production/endpoints/ep1"
    mock_w.postgres.list_endpoints.return_value = [ep]
    mock_w.postgres.generate_database_credential.side_effect = RuntimeError("auth failure")

    service.get_workspace_client = AsyncMock(return_value=mock_w)

    with pytest.raises(RuntimeError, match="auth failure"):
        await service.generate_credentials("proj")


@pytest.mark.asyncio
async def test_generate_credentials_not_found_case_insensitive(service):
    """generate_credentials treats 'NOT_FOUND' (uppercase) as fallback trigger."""
    mock_w = MagicMock()
    mock_w.database.generate_database_credential.side_effect = Exception("NOT_FOUND: instance xyz")

    ep = MagicMock()
    ep.name = "projects/xyz/branches/production/endpoints/ep1"
    mock_w.postgres.list_endpoints.return_value = [ep]

    auto_cred = MagicMock()
    auto_cred.token = "token-from-not-found"
    mock_w.postgres.generate_database_credential.return_value = auto_cred

    service.get_workspace_client = AsyncMock(return_value=mock_w)

    result = await service.generate_credentials("xyz")
    assert result.token == "token-from-not-found"
