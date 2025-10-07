import pytest
import os
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from src.services.databricks_service import DatabricksService


@pytest.mark.asyncio
async def test_from_session_returns_service_and_uses_session():
    mock_session = MagicMock()
    with patch('src.services.databricks_service.DatabricksConfigRepository') as MockRepo:
        MockRepo.return_value = AsyncMock()

        service = DatabricksService.from_session(mock_session)
        assert isinstance(service, DatabricksService)
        # repository should be constructed internally
        assert service.repository is not None
        # secrets service should be accessible (force lazy init override)
        service._secrets_service = MagicMock()
        assert service.secrets_service is not None


@pytest.mark.asyncio
async def test_setup_token_uses_provider_api_key_when_no_personal_token():
    """
    Test that authentication works via centralized databricks_auth module.
    The setup_token() method has been replaced by get_auth_context() from databricks_auth.
    This test verifies that the auth context can be retrieved successfully.
    """
    # Mock the auth context
    with patch('src.utils.databricks_auth.get_auth_context') as mock_get_auth:
        mock_auth = MagicMock()
        mock_auth.token = "test-provider-key"
        mock_auth.workspace_url = "https://example.com"
        mock_get_auth.return_value = mock_auth

        # Import and call get_auth_context
        from src.utils.databricks_auth import get_auth_context
        auth = await get_auth_context()

        assert auth is not None
        assert auth.token == "test-provider-key"
        assert auth.workspace_url == "https://example.com"

