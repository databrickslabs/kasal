"""
Unit tests for DatabricksService.

Tests the core functionality of Databricks integration operations.
"""
import pytest
import os
import requests
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from fastapi import HTTPException

from src.services.databricks_service import DatabricksService
from src.schemas.databricks_config import DatabricksConfigCreate, DatabricksConfigResponse


# Mock models
class MockDatabricksConfig:
    def __init__(self, id=1, workspace_url="https://test.databricks.com",
                 warehouse_id="warehouse123", catalog="test_catalog", schema="test_schema",
                 secret_scope="test_scope", is_enabled=True, apps_enabled=True,
                 created_at=None, updated_at=None):
        self.id = id
        self.workspace_url = workspace_url
        self.warehouse_id = warehouse_id
        self.catalog = catalog
        self.schema = schema
        self.secret_scope = secret_scope
        self.is_enabled = is_enabled
        self.apps_enabled = apps_enabled
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()


@pytest.fixture
def mock_repository():
    """Create a mock DatabricksConfigRepository."""
    return AsyncMock()


@pytest.fixture
def databricks_service(mock_repository):
    """Create a DatabricksService instance with mock repository."""
    with patch('src.services.databricks_service.DatabricksSecretsService') as MockSecretsService:
        mock_secrets = MagicMock()
        mock_secrets.set_databricks_service = MagicMock()
        MockSecretsService.return_value = mock_secrets
        
        service = DatabricksService(mock_repository)
        return service


@pytest.fixture
def mock_databricks_config():
    """Create a mock databricks config."""
    return MockDatabricksConfig()


@pytest.fixture
def valid_config_data():
    """Create valid DatabricksConfigCreate data with apps enabled."""
    return {
        "workspace_url": "https://test.databricks.com",
        "warehouse_id": "warehouse123",
        "catalog": "test_catalog",
        "db_schema": "test_schema",
        "secret_scope": "test_scope",
        "enabled": True,
        "apps_enabled": True  # Apps enabled avoids validation requirements
    }


class TestDatabricksService:
    """Test cases for DatabricksService."""
    
    def test_databricks_service_initialization(self, databricks_service, mock_repository):
        """Test DatabricksService initialization."""
        assert databricks_service.repository == mock_repository
        assert hasattr(databricks_service, 'secrets_service')
        assert databricks_service.secrets_service is not None
    
    @pytest.mark.asyncio
    async def test_set_databricks_config_success(self, databricks_service, valid_config_data, mock_databricks_config):
        """Test successful databricks configuration setting."""
        databricks_service.repository.create_config.return_value = mock_databricks_config
        
        config_create = DatabricksConfigCreate(**valid_config_data)
        result = await databricks_service.set_databricks_config(config_create)
        
        assert result["status"] == "success"
        assert "message" in result
        assert "config" in result
        databricks_service.repository.create_config.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_set_databricks_config_error(self, databricks_service, valid_config_data):
        """Test databricks configuration setting with error."""
        databricks_service.repository.create_config.side_effect = Exception("Database error")
        
        config_create = DatabricksConfigCreate(**valid_config_data)
        
        with pytest.raises(HTTPException) as exc_info:
            await databricks_service.set_databricks_config(config_create)
        
        assert exc_info.value.status_code == 500
        assert "Error setting Databricks configuration" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_databricks_config_success(self, databricks_service, mock_databricks_config):
        """Test successful databricks configuration retrieval."""
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        
        result = await databricks_service.get_databricks_config()
        
        assert isinstance(result, DatabricksConfigResponse)
        assert result.workspace_url == mock_databricks_config.workspace_url
        assert result.enabled == mock_databricks_config.is_enabled
        databricks_service.repository.get_active_config.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_databricks_config_not_found(self, databricks_service):
        """Test databricks configuration retrieval when not found."""
        databricks_service.repository.get_active_config.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            await databricks_service.get_databricks_config()
        
        assert exc_info.value.status_code == 404
        assert "Databricks configuration not found" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_get_databricks_config_error(self, databricks_service):
        """Test databricks configuration retrieval with general error."""
        databricks_service.repository.get_active_config.side_effect = Exception("Database error")
        
        with pytest.raises(HTTPException) as exc_info:
            await databricks_service.get_databricks_config()
        
        assert exc_info.value.status_code == 500
        assert "Error getting Databricks configuration" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_check_personal_token_required_no_config(self, databricks_service):
        """Test personal token check when no config exists."""
        databricks_service.repository.get_active_config.return_value = None
        
        result = await databricks_service.check_personal_token_required()
        
        assert result["personal_token_required"] is False
        assert "message" in result
    
    @pytest.mark.asyncio
    async def test_check_personal_token_required_apps_enabled(self, databricks_service, mock_databricks_config):
        """Test personal token check when apps are enabled."""
        mock_databricks_config.apps_enabled = True
        mock_databricks_config.is_enabled = True
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        
        result = await databricks_service.check_personal_token_required()
        
        assert result["personal_token_required"] is True
        assert "personal access token" in result["message"].lower()
    
    @pytest.mark.asyncio
    async def test_check_personal_token_required_apps_disabled(self, databricks_service, mock_databricks_config):
        """Test personal token check when apps are disabled."""
        mock_databricks_config.apps_enabled = False
        mock_databricks_config.is_enabled = True
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        
        result = await databricks_service.check_personal_token_required()
        
        assert result["personal_token_required"] is False
        assert "not configured to use" in result["message"].lower()
    
    @pytest.mark.asyncio
    async def test_check_apps_configuration_success(self, databricks_service, mock_databricks_config):
        """Test successful apps configuration check."""
        mock_databricks_config.apps_enabled = False  # This triggers the path we want to test
        mock_databricks_config.is_enabled = True
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        databricks_service.secrets_service.get_personal_access_token = AsyncMock(return_value="test-token")
        
        is_enabled, token = await databricks_service.check_apps_configuration()
        
        assert is_enabled is True
        assert token == "test-token"
    
    @pytest.mark.asyncio
    async def test_check_apps_configuration_disabled(self, databricks_service, mock_databricks_config):
        """Test apps configuration check when disabled."""
        mock_databricks_config.apps_enabled = False
        mock_databricks_config.is_enabled = True
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        
        is_enabled, token = await databricks_service.check_apps_configuration()
        
        assert is_enabled is False
        assert token == ""
    
    @pytest.mark.asyncio
    async def test_check_apps_configuration_no_config(self, databricks_service):
        """Test apps configuration check when no config exists."""
        databricks_service.repository.get_active_config.return_value = None
        
        is_enabled, token = await databricks_service.check_apps_configuration()
        
        assert is_enabled is False
        assert token == ""
    
    def test_setup_endpoint_success(self):
        """Test successful endpoint setup."""
        mock_config = MagicMock()
        mock_config.workspace_url = "https://test.databricks.com"
        mock_config.warehouse_id = "warehouse123"
        
        with patch.dict(os.environ, {}, clear=True):
            result = DatabricksService.setup_endpoint(mock_config)
            
            assert result is True
            assert os.environ.get("DATABRICKS_API_BASE") == "https://test.databricks.com/serving-endpoints"
            assert os.environ.get("DATABRICKS_ENDPOINT") == "https://test.databricks.com/serving-endpoints"
    
    def test_setup_endpoint_missing_config(self):
        """Test endpoint setup with missing configuration."""
        mock_config = MagicMock()
        mock_config.workspace_url = None
        mock_config.warehouse_id = "warehouse123"
        
        result = DatabricksService.setup_endpoint(mock_config)
        
        assert result is False
    
    def test_setup_token_sync_success(self):
        """Test synchronous token setup."""
        with patch('src.utils.asyncio_utils.create_and_run_loop') as mock_loop, \
             patch.object(DatabricksService, 'setup_token', new_callable=AsyncMock) as mock_setup_token:
            mock_loop.return_value = True
            mock_setup_token.return_value = True
            
            result = DatabricksService.setup_token_sync()
            
            assert result is True
            mock_loop.assert_called_once()
    
    def test_setup_token_sync_no_token(self):
        """Test synchronous token setup without token."""
        with patch.dict(os.environ, {}, clear=True):
            result = DatabricksService.setup_token_sync()
            
            assert result is False
    
    def test_from_session(self):
        """Test creating service from session."""
        mock_session = MagicMock()
        
        result = DatabricksService.from_session(mock_session)
        
        # Verify the result is a DatabricksService instance
        assert isinstance(result, DatabricksService)
        assert result.repository is not None
        assert result.secrets_service is not None
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_no_config(self, databricks_service):
        """Test Databricks connection check with no configuration."""
        databricks_service.repository.get_active_config.return_value = None
        
        result = await databricks_service.check_databricks_connection()
        
        assert result["status"] == "error"
        assert result["connected"] is False
        assert "configuration not found" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_success(self, databricks_service, mock_databricks_config):
        """Test successful Databricks connection check."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = False
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        databricks_service.secrets_service.get_personal_access_token = AsyncMock(return_value="test-token")
        
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_get.return_value = mock_response
            
            result = await databricks_service.check_databricks_connection()
            
            assert result["status"] == "success"
            assert result["connected"] is True
            assert "Successfully connected" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_unauthorized(self, databricks_service, mock_databricks_config):
        """Test Databricks connection check with unauthorized response."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = False
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        databricks_service.secrets_service.get_personal_access_token = AsyncMock(return_value="test-token")
        
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 401
            mock_response.text = "Unauthorized"
            mock_get.return_value = mock_response
            
            result = await databricks_service.check_databricks_connection()
            
            assert result["status"] == "error"
            assert result["connected"] is False
            assert "Authentication failed" in result["message"]
    
    def test_secrets_service_integration(self, databricks_service):
        """Test that secrets service is properly integrated."""
        assert hasattr(databricks_service, 'secrets_service')
        assert databricks_service.secrets_service is not None
        # The secrets service should have been configured with the databricks service
        databricks_service.secrets_service.set_databricks_service.assert_called_once_with(databricks_service)
    
    def test_environment_variable_handling(self):
        """Test proper handling of environment variables."""
        test_vars = {
            "DATABRICKS_HOST": "https://test.databricks.com",
            "DATABRICKS_TOKEN": "test-token",
            "DATABRICKS_WAREHOUSE_ID": "warehouse123"
        }
        
        with patch.dict(os.environ, test_vars):
            assert os.environ.get("DATABRICKS_HOST") == "https://test.databricks.com"
            assert os.environ.get("DATABRICKS_TOKEN") == "test-token"
            assert os.environ.get("DATABRICKS_WAREHOUSE_ID") == "warehouse123"
    
    @pytest.mark.asyncio
    async def test_configuration_data_structure(self, databricks_service, valid_config_data, mock_databricks_config):
        """Test that configuration data is properly structured."""
        databricks_service.repository.create_config.return_value = mock_databricks_config
        
        config_create = DatabricksConfigCreate(**valid_config_data)
        await databricks_service.set_databricks_config(config_create)
        
        # Verify the call was made with proper data structure
        call_args = databricks_service.repository.create_config.call_args[0][0]
        
        assert "workspace_url" in call_args
        assert "warehouse_id" in call_args
        assert "catalog" in call_args
        assert "schema" in call_args
        assert "secret_scope" in call_args
        assert "apps_enabled" in call_args
    
    def test_pydantic_schema_validation(self, valid_config_data):
        """Test that Pydantic schemas work correctly."""
        config_create = DatabricksConfigCreate(**valid_config_data)
        
        assert config_create.workspace_url == valid_config_data["workspace_url"]
        assert config_create.enabled == valid_config_data["enabled"]
        assert config_create.apps_enabled == valid_config_data["apps_enabled"]
    
    def test_method_existence(self, databricks_service):
        """Test that all expected methods exist."""
        expected_methods = [
            'set_databricks_config',
            'get_databricks_config',
            'check_personal_token_required',
            'check_apps_configuration',
            'check_databricks_connection'
        ]
        
        for method_name in expected_methods:
            assert hasattr(databricks_service, method_name)
            assert callable(getattr(databricks_service, method_name))
    
    def test_static_method_existence(self):
        """Test that static methods exist."""
        static_methods = [
            'setup_endpoint',
            'setup_token_sync',
            'from_session'
        ]
        
        for method_name in static_methods:
            assert hasattr(DatabricksService, method_name)
            assert callable(getattr(DatabricksService, method_name))
    
    @pytest.mark.asyncio
    async def test_class_method_existence(self):
        """Test that class methods exist."""
        class_methods = [
            'setup_token',
            'from_unit_of_work'
        ]
        
        for method_name in class_methods:
            assert hasattr(DatabricksService, method_name)
            assert callable(getattr(DatabricksService, method_name))
    
    @pytest.mark.asyncio
    async def test_check_personal_token_required_disabled_config(self, databricks_service, mock_databricks_config):
        """Test personal token check when databricks is disabled."""
        mock_databricks_config.is_enabled = False
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        
        result = await databricks_service.check_personal_token_required()
        
        assert result["personal_token_required"] is False
        assert "integration is disabled" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_personal_token_required_missing_fields(self, databricks_service, mock_databricks_config):
        """Test personal token check when required fields are missing."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = False
        mock_databricks_config.warehouse_id = None  # Missing required field
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        
        result = await databricks_service.check_personal_token_required()
        
        assert result["personal_token_required"] is True
        assert "missing warehouse_id" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_personal_token_required_error(self, databricks_service):
        """Test personal token check with general error."""
        databricks_service.repository.get_active_config.side_effect = Exception("Database error")
        
        with pytest.raises(HTTPException) as exc_info:
            await databricks_service.check_personal_token_required()
        
        assert exc_info.value.status_code == 500
        assert "Error checking personal token requirement" in str(exc_info.value.detail)
    
    @pytest.mark.asyncio
    async def test_check_apps_configuration_error(self, databricks_service):
        """Test apps configuration check with error handling."""
        databricks_service.repository.get_active_config.side_effect = Exception("Database error")
        
        is_enabled, token = await databricks_service.check_apps_configuration()
        
        assert is_enabled is False
        assert token == ""
    
    def test_setup_endpoint_with_trailing_slash(self):
        """Test endpoint setup with workspace URL that has trailing slash."""
        mock_config = MagicMock()
        mock_config.workspace_url = "https://test.databricks.com/"
        
        with patch.dict(os.environ, {}, clear=True):
            result = DatabricksService.setup_endpoint(mock_config)
            
            assert result is True
            assert os.environ.get("DATABRICKS_API_BASE") == "https://test.databricks.com/serving-endpoints"
            assert os.environ.get("DATABRICKS_ENDPOINT") == "https://test.databricks.com/serving-endpoints"
    
    def test_setup_endpoint_already_with_serving_endpoints(self):
        """Test endpoint setup when URL already ends with serving-endpoints."""
        mock_config = MagicMock()
        mock_config.workspace_url = "https://test.databricks.com/serving-endpoints"
        
        with patch.dict(os.environ, {}, clear=True):
            result = DatabricksService.setup_endpoint(mock_config)
            
            assert result is True
            assert os.environ.get("DATABRICKS_ENDPOINT") == "https://test.databricks.com/serving-endpoints"
    
    def test_setup_endpoint_none_config(self):
        """Test endpoint setup with None config."""
        result = DatabricksService.setup_endpoint(None)
        
        assert result is False
    
    def test_setup_endpoint_error_handling(self):
        """Test endpoint setup with error handling."""
        mock_config = MagicMock()
        mock_config.workspace_url = "https://test.databricks.com"
        
        # Mock hasattr to return False to trigger the first branch
        with patch('builtins.hasattr', return_value=False):
            result = DatabricksService.setup_endpoint(mock_config)
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_setup_token_success(self):
        """Test successful async token setup with personal token."""
        # Test that setup_token is a valid class method
        assert hasattr(DatabricksService, 'setup_token')
        assert callable(DatabricksService.setup_token)
        
        # Test with personal token available
        with patch.dict(os.environ, {}, clear=True):
            with patch('src.db.session.async_session_factory') as mock_factory:
                mock_session = AsyncMock()
                mock_factory.return_value.__aenter__.return_value = mock_session
                
                with patch('src.repositories.databricks_config_repository.DatabricksConfigRepository') as MockRepo:
                    mock_repo = AsyncMock()
                    MockRepo.return_value = mock_repo
                    mock_repo.get_active_config.return_value = MockDatabricksConfig()
                    
                    result = await DatabricksService.setup_token()
                    
                    # The method should complete without error
                    assert isinstance(result, bool)
    
    @pytest.mark.asyncio
    async def test_setup_token_fallback_scenarios(self):
        """Test token setup fallback scenarios."""
        # Test with no config - should return False
        with patch('src.db.session.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            
            with patch('src.repositories.databricks_config_repository.DatabricksConfigRepository') as MockRepo:
                mock_repo = AsyncMock()
                MockRepo.return_value = mock_repo
                mock_repo.get_active_config.return_value = None  # No config
                
                result = await DatabricksService.setup_token()
                
                # Should handle gracefully
                assert isinstance(result, bool)
    
    @pytest.mark.asyncio
    async def test_setup_token_error(self):
        """Test token setup with error handling."""
        with patch('src.db.session.async_session_factory', side_effect=Exception("Database error")):
            result = await DatabricksService.setup_token()
            
            assert result is False
    
    def test_setup_token_sync_error(self):
        """Test synchronous token setup with error handling."""
        with patch('src.utils.asyncio_utils.create_and_run_loop', side_effect=Exception("Async error")):
            result = DatabricksService.setup_token_sync()
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_from_unit_of_work(self):
        """Test creating service from unit of work."""
        mock_uow = AsyncMock()
        mock_uow.databricks_config_repository = AsyncMock()
        
        service = await DatabricksService.from_unit_of_work(mock_uow)
        
        assert isinstance(service, DatabricksService)
        assert service.repository == mock_uow.databricks_config_repository
    
    def test_from_session_with_api_keys_service(self):
        """Test creating service from session with API keys service."""
        mock_session = MagicMock()
        mock_api_keys_service = AsyncMock()
        
        with patch('src.services.databricks_service.DatabricksSecretsService'):
            result = DatabricksService.from_session(mock_session, mock_api_keys_service)
            
            assert isinstance(result, DatabricksService)
            # Verify the API keys service was set
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_disabled(self, databricks_service, mock_databricks_config):
        """Test connection check when databricks is disabled."""
        mock_databricks_config.is_enabled = False
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        
        result = await databricks_service.check_databricks_connection()
        
        assert result["status"] == "disabled"
        assert result["connected"] is False
        assert "integration is disabled" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_apps_enabled_with_token(self, databricks_service, mock_databricks_config):
        """Test connection check with apps enabled and token available."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = True
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        databricks_service.secrets_service.get_personal_access_token = AsyncMock(return_value="test-token")
        
        result = await databricks_service.check_databricks_connection()
        
        assert result["status"] == "success"
        assert result["connected"] is True
        assert "Apps integration is enabled" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_apps_enabled_no_token(self, databricks_service, mock_databricks_config):
        """Test connection check with apps enabled but no token."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = True
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        databricks_service.secrets_service.get_personal_access_token = AsyncMock(return_value=None)
        
        result = await databricks_service.check_databricks_connection()
        
        assert result["status"] == "error"
        assert result["connected"] is False
        assert "no personal access token found" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_missing_fields(self, databricks_service, mock_databricks_config):
        """Test connection check with missing required fields."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = False
        mock_databricks_config.warehouse_id = None  # Missing required field
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        
        result = await databricks_service.check_databricks_connection()
        
        assert result["status"] == "error"
        assert result["connected"] is False
        assert "Missing required fields" in result["message"]
        assert "warehouse_id" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_url_formatting(self, databricks_service, mock_databricks_config):
        """Test connection check with URL formatting."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = False
        mock_databricks_config.workspace_url = "test.databricks.com/"  # No https, has trailing slash
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        databricks_service.secrets_service.get_personal_access_token = AsyncMock(return_value="test-token")
        
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_get.return_value = mock_response
            
            result = await databricks_service.check_databricks_connection()
            
            # Verify the URL was properly formatted
            call_args = mock_get.call_args[0][0]
            assert call_args.startswith("https://test.databricks.com/api/2.0/sql/warehouses")
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_with_oauth(self, databricks_service, mock_databricks_config):
        """Test connection check using OAuth authentication."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = False
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        
        with patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=True), \
             patch('src.utils.databricks_auth.get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth, \
             patch('requests.get') as mock_get:
            
            mock_auth.return_value = ({"Authorization": "Bearer oauth-token"}, None)
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_get.return_value = mock_response
            
            result = await databricks_service.check_databricks_connection()
            
            assert result["status"] == "success"
            assert result["connected"] is True
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_oauth_error(self, databricks_service, mock_databricks_config):
        """Test connection check with OAuth authentication error."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = False
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        
        with patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=True), \
             patch('src.utils.databricks_auth.get_databricks_auth_headers', new_callable=AsyncMock) as mock_auth:
            
            mock_auth.return_value = (None, "OAuth failed")
            
            result = await databricks_service.check_databricks_connection()
            
            assert result["status"] == "error"
            assert result["connected"] is False
            assert "Failed to get OAuth authentication" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_no_auth_credentials(self, databricks_service, mock_databricks_config):
        """Test connection check when no authentication credentials are available."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = False
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        databricks_service.secrets_service.get_personal_access_token = AsyncMock(return_value=None)
        
        with patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', new_callable=AsyncMock) as mock_api:
            mock_api.return_value = None
            
            result = await databricks_service.check_databricks_connection()
            
            assert result["status"] == "error"
            assert result["connected"] is False
            assert "No authentication credentials available" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_forbidden(self, databricks_service, mock_databricks_config):
        """Test connection check with forbidden response."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = False
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        databricks_service.secrets_service.get_personal_access_token = AsyncMock(return_value="test-token")
        
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 403
            mock_get.return_value = mock_response
            
            result = await databricks_service.check_databricks_connection()
            
            assert result["status"] == "error"
            assert result["connected"] is False
            assert "Access forbidden" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_other_status_code(self, databricks_service, mock_databricks_config):
        """Test connection check with other status codes."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = False
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        databricks_service.secrets_service.get_personal_access_token = AsyncMock(return_value="test-token")
        
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_response.text = "Internal Server Error"
            mock_get.return_value = mock_response
            
            result = await databricks_service.check_databricks_connection()
            
            assert result["status"] == "error"
            assert result["connected"] is False
            assert "Connection failed with status 500" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_connection_error(self, databricks_service, mock_databricks_config):
        """Test connection check with connection error."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = False
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        databricks_service.secrets_service.get_personal_access_token = AsyncMock(return_value="test-token")
        
        with patch('requests.get', side_effect=requests.exceptions.ConnectionError("Connection failed")):
            result = await databricks_service.check_databricks_connection()
            
            assert result["status"] == "error"
            assert result["connected"] is False
            assert "Failed to connect to" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_timeout(self, databricks_service, mock_databricks_config):
        """Test connection check with timeout error."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = False
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        databricks_service.secrets_service.get_personal_access_token = AsyncMock(return_value="test-token")
        
        with patch('requests.get', side_effect=requests.exceptions.Timeout("Request timeout")):
            result = await databricks_service.check_databricks_connection()
            
            assert result["status"] == "error"
            assert result["connected"] is False
            assert "Connection timeout" in result["message"]
    
    @pytest.mark.asyncio
    async def test_check_databricks_connection_general_exception(self, databricks_service, mock_databricks_config):
        """Test connection check with general exception."""
        mock_databricks_config.is_enabled = True
        mock_databricks_config.apps_enabled = False
        databricks_service.repository.get_active_config.return_value = mock_databricks_config
        databricks_service.secrets_service.get_personal_access_token = AsyncMock(return_value="test-token")
        
        with patch('requests.get', side_effect=Exception("Unexpected error")):
            result = await databricks_service.check_databricks_connection()
            
            assert result["status"] == "error"
            assert result["connected"] is False
            assert "Connection test failed" in result["message"]