"""
Unit tests for DatabricksConnectionService.

Tests connection testing and authentication for Databricks Vector Search.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from aiohttp import ClientSession

from src.services.databricks_connection_service import DatabricksConnectionService
from src.schemas.memory_backend import DatabricksMemoryConfig
from src.core.unit_of_work import UnitOfWork


@pytest.fixture
def mock_uow():
    """Create a mock Unit of Work."""
    uow = AsyncMock(spec=UnitOfWork)
    return uow


@pytest.fixture
def service(mock_uow):
    """Create a DatabricksConnectionService instance."""
    return DatabricksConnectionService(mock_uow)


@pytest.fixture
def databricks_config():
    """Create a sample Databricks configuration."""
    return DatabricksMemoryConfig(
        endpoint_name="test-endpoint",
        short_term_index="ml.agents.short_term",
        long_term_index="ml.agents.long_term",
        entity_index="ml.agents.entity",
        workspace_url="https://test.databricks.com",
        embedding_dimension=768,
        auth_type="pat",
        personal_access_token="test-token"
    )


class TestDatabricksConnectionService:
    """Test cases for DatabricksConnectionService."""
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    @patch('src.utils.databricks_auth.get_databricks_auth_headers')
    async def test_test_connection_obo_auth_success(
        self, mock_get_headers, mock_client_class, service, databricks_config
    ):
        """Test successful connection with OBO authentication."""
        # Arrange
        user_token = "user-token-123"
        mock_get_headers.return_value = ({"Authorization": "Bearer obo-token"}, None)
        
        mock_client = MagicMock()
        mock_client.get_endpoint.return_value = {
            "endpoint_status": {"state": "ONLINE"}
        }
        mock_client.get_index.side_effect = [
            {"status": {"state": "READY"}},  # short_term_index
            {"status": {"state": "READY"}},  # long_term_index
            Exception("Index not found")      # entity_index
        ]
        mock_client_class.return_value = mock_client
        
        # Act
        result = await service.test_databricks_connection(databricks_config, user_token)
        
        # Assert
        assert result["success"] is True
        assert "Successfully connected" in result["message"]
        assert result["details"]["auth_method"] == "OBO"
        assert len(result["details"]["indexes_found"]) == 2
        assert len(result["details"]["indexes_missing"]) == 1
        mock_get_headers.assert_called_once_with(user_token=user_token)
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    @patch('src.utils.databricks_auth.get_databricks_auth_headers')
    @patch('src.services.api_keys_service.ApiKeysService')
    @patch('src.core.unit_of_work.UnitOfWork')
    @patch('src.utils.encryption_utils.EncryptionUtils')
    async def test_test_connection_api_key_fallback(
        self, mock_encryption, mock_uow_class, mock_api_service_class, 
        mock_get_headers, mock_client_class, service
    ):
        """Test connection falls back to API key when OBO fails."""
        # Arrange
        # Create config without PAT to test API key fallback
        config_without_pat = DatabricksMemoryConfig(
            endpoint_name="test-endpoint",
            short_term_index="ml.agents.short_term",
            workspace_url="https://test.databricks.com"
        )
        
        mock_get_headers.return_value = (None, "OBO failed")
        
        # Mock API key service
        mock_api_service = AsyncMock()
        mock_api_key = MagicMock(encrypted_value="encrypted-token")
        mock_api_service.find_by_name.side_effect = [mock_api_key, None]
        # from_unit_of_work is a classmethod that returns a coroutine
        mock_api_service_class.from_unit_of_work = AsyncMock(return_value=mock_api_service)
        
        # Mock encryption
        mock_encryption.decrypt_value.return_value = "decrypted-token"
        
        # Mock UnitOfWork context manager
        mock_uow_instance = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow_instance
        
        # Mock client
        mock_client = MagicMock()
        mock_client.get_endpoint.return_value = {
            "endpoint_status": {"state": "PROVISIONING"}
        }
        mock_client_class.return_value = mock_client
        
        # Act
        result = await service.test_databricks_connection(config_without_pat, None)
        
        # Assert
        assert result["success"] is True
        assert result["details"]["auth_method"] == "API Key Service"
        assert result["details"]["endpoint_status"] == "PROVISIONING"
    
    @pytest.mark.asyncio
    @patch('src.services.api_keys_service.ApiKeysService')
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_test_connection_config_auth(
        self, mock_client_class, mock_api_service_class, service, databricks_config
    ):
        """Test connection using configuration credentials."""
        # Arrange
        mock_client = MagicMock()
        mock_client.get_endpoint.return_value = {
            "endpoint_status": {"state": "ONLINE"}
        }
        mock_client_class.return_value = mock_client
        
        # Mock API service to fail, forcing fallback to config auth
        mock_api_service_class.from_unit_of_work.side_effect = Exception("No API service")
        
        # Act
        result = await service.test_databricks_connection(databricks_config, None)
        
        # Assert
        assert result["success"] is True
        assert result["details"]["auth_method"] == "PAT (from config)"
        mock_client_class.assert_called_with(
            workspace_url="https://test.databricks.com",
            personal_access_token="test-token"
        )
    
    @pytest.mark.asyncio
    @patch('src.services.api_keys_service.ApiKeysService')
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_test_connection_service_principal_auth(
        self, mock_client_class, mock_api_service_class, service
    ):
        """Test connection with service principal authentication."""
        # Arrange
        sp_config = DatabricksMemoryConfig(
            endpoint_name="test-endpoint",
            short_term_index="ml.agents.short_term",
            workspace_url="https://test.databricks.com",
            auth_type="service_principal",
            service_principal_client_id="client-id",
            service_principal_client_secret="client-secret"
        )
        
        mock_client = MagicMock()
        mock_client.get_endpoint.return_value = {
            "endpoint_status": {"state": "ONLINE"}
        }
        mock_client_class.return_value = mock_client
        
        # Mock API service to fail, forcing fallback to service principal auth
        mock_api_service_class.from_unit_of_work.side_effect = Exception("No API service")
        
        # Act
        result = await service.test_databricks_connection(sp_config, None)
        
        # Assert
        assert result["success"] is True
        assert result["details"]["auth_method"] == "Service Principal"
        mock_client_class.assert_called_with(
            workspace_url="https://test.databricks.com",
            service_principal_client_id="client-id",
            service_principal_client_secret="client-secret"
        )
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    @patch('src.utils.databricks_auth.is_databricks_apps_environment')
    @patch('src.services.api_keys_service.ApiKeysService')
    @patch('src.core.unit_of_work.UnitOfWork')
    async def test_test_connection_databricks_apps_env(
        self, mock_uow_class, mock_api_service_class, mock_is_apps_env, 
        mock_client_class, service
    ):
        """Test connection in Databricks Apps environment."""
        # Arrange
        # Create config without auth for Databricks Apps environment
        apps_config = DatabricksMemoryConfig(
            endpoint_name="test-endpoint",
            short_term_index="ml.agents.short_term",
            workspace_url="https://test.databricks.com",
            auth_type="default"  # No PAT or service principal
        )
        mock_is_apps_env.return_value = True
        
        # Mock API key service to return None (no keys found)
        mock_api_service = AsyncMock()
        mock_api_service.find_by_name.return_value = None
        mock_api_service_class.from_unit_of_work = AsyncMock(return_value=mock_api_service)
        
        # Mock UnitOfWork
        mock_uow_instance = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow_instance
        
        mock_client = MagicMock()
        mock_client.get_endpoint.return_value = {
            "endpoint_status": {"state": "ONLINE"}
        }
        mock_client_class.return_value = mock_client
        
        # Act
        result = await service.test_databricks_connection(apps_config, None)
        
        # Assert
        assert result["success"] is True
        assert result["details"]["auth_method"] == "Databricks Apps Environment"
        mock_client_class.assert_called_with()
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_test_connection_endpoint_error(
        self, mock_client_class, service, databricks_config
    ):
        """Test connection when endpoint info fails."""
        # Arrange
        mock_client = MagicMock()
        mock_client.get_endpoint.side_effect = Exception("Endpoint not found")
        mock_client_class.return_value = mock_client
        
        # Act
        result = await service.test_databricks_connection(databricks_config, None)
        
        # Assert
        assert result["success"] is False
        assert "Failed to get endpoint info" in result["message"]
        assert "Endpoint not found" in result["details"]["error"]
    
    @pytest.mark.asyncio
    async def test_test_connection_import_error(self, service, databricks_config):
        """Test connection when databricks package not installed."""
        # Arrange
        with patch('databricks.vector_search.client.VectorSearchClient', 
                   side_effect=ImportError("No module")):
            # Act
            result = await service.test_databricks_connection(databricks_config, None)
        
        # Assert
        assert result["success"] is False
        assert "databricks-vectorsearch package not installed" in result["message"]
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    async def test_get_client_with_auth_all_methods_fail(
        self, mock_client_class, service
    ):
        """Test get_databricks_client_with_auth when all auth methods fail."""
        # Arrange
        mock_client_class.side_effect = Exception("Auth failed")
        
        # Act & Assert
        with pytest.raises(ValueError, match="All authentication methods failed"):
            await service.get_databricks_client_with_auth("https://test.databricks.com")
    
    @pytest.mark.asyncio
    @patch('databricks.vector_search.client.VectorSearchClient')
    @patch('src.services.api_keys_service.ApiKeysService')
    @patch('src.core.unit_of_work.UnitOfWork')
    @patch('src.utils.encryption_utils.EncryptionUtils')
    @patch('databricks.sdk.WorkspaceClient')
    @patch('databricks.sdk.config.Config')
    async def test_get_client_with_auth_oauth_success(
        self, mock_config_class, mock_workspace_class, mock_encryption,
        mock_uow_class, mock_api_service_class, mock_client_class, service
    ):
        """Test OAuth client credentials authentication success."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        
        # Mock VectorSearchClient creation attempts
        # No user token, so OBO is skipped
        # OAuth succeeds
        mock_oauth_client = MagicMock()
        mock_oauth_client.list_endpoints.return_value = []
        mock_client_class.return_value = mock_oauth_client
        
        # Mock API service for OAuth credentials
        mock_api_service = AsyncMock()
        mock_client_id_key = MagicMock(encrypted_value="encrypted-client-id")
        mock_client_secret_key = MagicMock(encrypted_value="encrypted-client-secret")
        # OAuth credentials lookup
        mock_api_service.find_by_name.side_effect = [
            mock_client_id_key,  # DATABRICKS_CLIENT_ID found
            mock_client_secret_key  # DATABRICKS_CLIENT_SECRET found
        ]
        # from_unit_of_work is a classmethod that returns a coroutine
        mock_api_service_class.from_unit_of_work = AsyncMock(return_value=mock_api_service)
        
        # Mock encryption
        mock_encryption.decrypt_value.side_effect = ["client-id", "client-secret"]
        
        # Mock OAuth config and auth
        mock_config = MagicMock()
        mock_auth_result = MagicMock(access_token="oauth-token")
        mock_config.authenticate.return_value = mock_auth_result
        mock_config_class.return_value = mock_config
        
        # Mock UnitOfWork
        mock_uow_instance = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow_instance
        
        # Act
        client, auth_method = await service.get_databricks_client_with_auth(workspace_url, None)
        
        # Assert
        assert client == mock_oauth_client
        assert auth_method == "OAuth"
        mock_config_class.assert_called_with(
            client_id="client-id",
            client_secret="client-secret",
            host=workspace_url
        )
    
    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession.get')
    @patch('src.utils.databricks_auth.get_databricks_auth_headers')
    async def test_get_endpoint_status_success(
        self, mock_get_headers, mock_get, service
    ):
        """Test getting endpoint status successfully."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        endpoint_name = "test-endpoint"
        user_token = "user-token"
        
        mock_get_headers.return_value = (
            {"Authorization": "Bearer token", "Content-Type": "application/json"},
            None
        )
        
        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "endpoint_status": {
                "state": "ONLINE",
                "message": "Endpoint is ready"
            },
            "endpoint_type": "STANDARD"
        })
        mock_get.return_value.__aenter__.return_value = mock_response
        
        # Act
        result = await service.get_databricks_endpoint_status(
            workspace_url, endpoint_name, user_token
        )
        
        # Assert
        assert result["success"] is True
        assert result["endpoint_name"] == endpoint_name
        assert result["state"] == "ONLINE"
        assert result["ready"] is True
        assert result["can_delete_indexes"] is True
    
    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession.get')
    @patch('src.utils.databricks_auth.get_databricks_auth_headers')
    async def test_get_endpoint_status_not_found(
        self, mock_get_headers, mock_get, service
    ):
        """Test getting endpoint status when endpoint not found."""
        # Arrange
        workspace_url = "https://test.databricks.com"
        endpoint_name = "missing-endpoint"
        
        mock_get_headers.return_value = (
            {"Authorization": "Bearer token", "Content-Type": "application/json"},
            None
        )
        
        # Mock 404 response
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_get.return_value.__aenter__.return_value = mock_response
        
        # Act
        result = await service.get_databricks_endpoint_status(
            workspace_url, endpoint_name, None
        )
        
        # Assert
        assert result["success"] is False
        assert result["state"] == "NOT_FOUND"
        assert result["ready"] is False
        assert result["can_delete_indexes"] is False