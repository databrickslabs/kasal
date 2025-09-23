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
    # Patch session factory and repository
    with patch('src.db.session.async_session_factory') as mock_factory, \
         patch('src.repositories.databricks_config_repository.DatabricksConfigRepository') as MockRepo, \
         patch.object(DatabricksService, 'setup_endpoint', return_value=True), \
         patch.object(DatabricksService, 'secrets_service', new_callable=PropertyMock) as mock_prop:
        # Mock session context manager
        mock_session = AsyncMock()
        mock_factory.return_value.__aenter__.return_value = mock_session

        # Mock repository and config
        mock_repo = AsyncMock()
        MockRepo.return_value = mock_repo
        mock_config = MagicMock()
        mock_config.workspace_url = "https://example.com"
        mock_repo.get_active_config.return_value = mock_config

        # Mock secrets service to return provider api key
        mock_secrets = MagicMock()
        mock_secrets.get_provider_api_key = AsyncMock(return_value="test-provider-key")
        mock_prop.return_value = mock_secrets

        # Ensure env is clean
        with patch.dict(os.environ, {}, clear=True):
            ok = await DatabricksService.setup_token()
            assert ok is True
            assert os.environ.get("DATABRICKS_TOKEN") == "test-provider-key"
            assert os.environ.get("DATABRICKS_API_KEY") == "test-provider-key"

