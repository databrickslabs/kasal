import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
import json
import os
import base64
from typing import Dict, Any, List, Optional

from src.services.databricks_secrets_service import DatabricksSecretsService
from src.repositories.databricks_config_repository import DatabricksConfigRepository


# Global fixtures for all test classes
@pytest.fixture
def mock_databricks_repository():
    """Mock Databricks Config Repository"""
    repo = AsyncMock(spec=DatabricksConfigRepository)
    return repo

@pytest.fixture
def databricks_secrets_service(mock_databricks_repository):
    """Create DatabricksSecretsService instance"""
    service = DatabricksSecretsService(mock_databricks_repository)
    # Set the session attribute that BaseService and the service methods expect
    service.session = AsyncMock()
    return service

@pytest.fixture
def mock_databricks_config():
    """Mock Databricks configuration"""
    config = MagicMock()
    config.is_enabled = True
    config.workspace_url = "https://test.databricks.com"
    config.secret_scope = "test_scope"
    return config

@pytest.fixture
def sample_secrets_response():
    """Sample Databricks secrets response"""
    return {
        "secrets": [
            {
                "key": "secret1",
                "last_updated_timestamp": 1234567890
            },
            {
                "key": "secret2",
                "last_updated_timestamp": 1234567891
            }
        ]
    }


class TestDatabricksSecretsService:
    """Test the DatabricksSecretsService class"""


class TestInit:
    """Test DatabricksSecretsService initialization"""
    
    def test_init_with_repository(self, mock_databricks_repository):
        """Test initialization with databricks repository"""
        service = DatabricksSecretsService(mock_databricks_repository)
        assert service.databricks_repository == mock_databricks_repository
        assert service.api_keys_service is None
        assert service.databricks_service is None
    
    def test_set_databricks_service(self, databricks_secrets_service):
        """Test setting databricks service"""
        mock_service = MagicMock()
        databricks_secrets_service.set_databricks_service(mock_service)
        assert databricks_secrets_service.databricks_service == mock_service
    
    def test_set_databricks_service_already_set(self, databricks_secrets_service):
        """Test setting databricks service when already set"""
        first_service = MagicMock()
        second_service = MagicMock()
        databricks_secrets_service.set_databricks_service(first_service)
        databricks_secrets_service.set_databricks_service(second_service)
        # Should not override if already set
        assert databricks_secrets_service.databricks_service == first_service
    
    def test_set_api_keys_service(self, databricks_secrets_service):
        """Test setting API keys service"""
        mock_service = MagicMock()
        databricks_secrets_service.set_api_keys_service(mock_service)
        assert databricks_secrets_service.api_keys_service == mock_service


class TestValidateDatabricksConfig:
    """Test the validate_databricks_config method"""
    
    @pytest.mark.asyncio
    async def test_validate_databricks_config_success(self, databricks_secrets_service, mock_databricks_config):
        """Test successful validation of Databricks config"""
        mock_databricks_service = AsyncMock()
        mock_databricks_service.get_databricks_config.return_value = mock_databricks_config
        databricks_secrets_service.set_databricks_service(mock_databricks_service)
        
        workspace_url, secret_scope = await databricks_secrets_service.validate_databricks_config()
        
        assert workspace_url == "https://test.databricks.com"
        assert secret_scope == "test_scope"
    
    @pytest.mark.asyncio
    async def test_validate_databricks_config_no_service_set(self, databricks_secrets_service, mock_databricks_config):
        """Test validation when databricks service is not set"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            # Set session attribute on service since BaseService expects it
            databricks_secrets_service.session = AsyncMock()
            
            workspace_url, secret_scope = await databricks_secrets_service.validate_databricks_config()
            
            assert workspace_url == "https://test.databricks.com"
            assert secret_scope == "test_scope"
            assert databricks_secrets_service.databricks_service == mock_service
    
    @pytest.mark.asyncio
    async def test_validate_databricks_config_no_config(self, databricks_secrets_service):
        """Test validation when no config found"""
        mock_databricks_service = AsyncMock()
        mock_databricks_service.get_databricks_config.return_value = None
        databricks_secrets_service.set_databricks_service(mock_databricks_service)
        
        with pytest.raises(ValueError, match="Databricks configuration not found"):
            await databricks_secrets_service.validate_databricks_config()
    
    @pytest.mark.asyncio
    async def test_validate_databricks_config_disabled(self, databricks_secrets_service):
        """Test validation when Databricks is disabled"""
        mock_config = MagicMock()
        mock_config.is_enabled = False
        
        mock_databricks_service = AsyncMock()
        mock_databricks_service.get_databricks_config.return_value = mock_config
        databricks_secrets_service.set_databricks_service(mock_databricks_service)
        
        with pytest.raises(ValueError, match="Databricks integration is disabled"):
            await databricks_secrets_service.validate_databricks_config()
    
    @pytest.mark.asyncio
    async def test_validate_databricks_config_no_workspace_url(self, databricks_secrets_service):
        """Test validation when workspace URL is None"""
        mock_config = MagicMock()
        mock_config.is_enabled = True
        mock_config.workspace_url = None
        mock_config.secret_scope = "test_scope"
        
        mock_databricks_service = AsyncMock()
        mock_databricks_service.get_databricks_config.return_value = mock_config
        databricks_secrets_service.set_databricks_service(mock_databricks_service)
        
        workspace_url, secret_scope = await databricks_secrets_service.validate_databricks_config()
        
        assert workspace_url == ""
        assert secret_scope == "test_scope"


class TestGetDatabricksSecrets:
    """Test the get_databricks_secrets method"""
    
    @pytest.mark.asyncio
    async def test_get_databricks_secrets_success(self, databricks_secrets_service, mock_databricks_config, sample_secrets_response):
        """Test successful retrieval of Databricks secrets"""
        scope = "test_scope"
        
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}), \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            # Mock databricks service
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            # Mock HTTP response for listing secrets
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = sample_secrets_response
            
            # Create mock session with proper async context manager
            mock_session = MagicMock()
            mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
            
            # Mock get_databricks_secret_value calls
            with patch.object(databricks_secrets_service, 'get_databricks_secret_value', new_callable=AsyncMock) as mock_get_value:
                mock_get_value.side_effect = ["value1", "value2"]
                
                result = await databricks_secrets_service.get_databricks_secrets(scope)
                
                assert len(result) == 2
                assert result[0]["name"] == "secret1"
                assert result[0]["value"] == "value1"
                assert result[0]["scope"] == scope
                assert result[0]["source"] == "databricks"
                assert result[1]["name"] == "secret2"
                assert result[1]["value"] == "value2"
    
    @pytest.mark.asyncio
    async def test_get_databricks_secrets_no_config(self, databricks_secrets_service):
        """Test get_databricks_secrets when config is not found"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = None
            mock_service_class.return_value = mock_service
            
            result = await databricks_secrets_service.get_databricks_secrets("test_scope")
            
            assert result == []
    
    @pytest.mark.asyncio
    async def test_get_databricks_secrets_disabled(self, databricks_secrets_service):
        """Test get_databricks_secrets when Databricks is disabled"""
        mock_config = MagicMock()
        mock_config.is_enabled = False
        
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_config
            mock_service_class.return_value = mock_service
            
            result = await databricks_secrets_service.get_databricks_secrets("test_scope")
            
            assert result == []
    
    @pytest.mark.asyncio
    async def test_get_databricks_secrets_no_workspace_url(self, databricks_secrets_service):
        """Test get_databricks_secrets when workspace URL is missing"""
        mock_config = MagicMock()
        mock_config.is_enabled = True
        mock_config.workspace_url = None
        
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_config
            mock_service_class.return_value = mock_service
            
            result = await databricks_secrets_service.get_databricks_secrets("test_scope")
            
            assert result == []
    
    @pytest.mark.asyncio
    async def test_get_databricks_secrets_no_token(self, databricks_secrets_service, mock_databricks_config):
        """Test get_databricks_secrets when token is not set"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch.dict(os.environ, {}, clear=True):
            
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            result = await databricks_secrets_service.get_databricks_secrets("test_scope")
            
            assert result == []
    
    @pytest.mark.asyncio
    async def test_get_databricks_secrets_http_error(self, databricks_secrets_service, mock_databricks_config):
        """Test get_databricks_secrets with HTTP error"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}), \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            # Mock HTTP error response
            mock_response = AsyncMock()
            mock_response.status = 404
            mock_response.text = AsyncMock(return_value="Not found")
            
            mock_session = MagicMock()
            mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
            
            result = await databricks_secrets_service.get_databricks_secrets("test_scope")
            
            assert result == []
    
    @pytest.mark.asyncio
    async def test_get_databricks_secrets_exception(self, databricks_secrets_service):
        """Test get_databricks_secrets with general exception"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service_class.side_effect = Exception("Unexpected error")
            
            result = await databricks_secrets_service.get_databricks_secrets("test_scope")
            
            assert result == []


class TestGetDatabricksSecretValue:
    """Test the get_databricks_secret_value method"""
    
    @pytest.mark.asyncio
    async def test_get_databricks_secret_value_success(self, databricks_secrets_service, mock_databricks_config):
        """Test successful retrieval of Databricks secret value"""
        scope = "test_scope"
        key = "test_key"
        secret_value = "test_secret_value"
        encoded_value = base64.b64encode(secret_value.encode('utf-8')).decode('utf-8')
        
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}), \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            # Mock HTTP response
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = {"value": encoded_value}
            
            mock_session = MagicMock()
            mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
            
            result = await databricks_secrets_service.get_databricks_secret_value(scope, key)
            
            assert result == secret_value
    
    @pytest.mark.asyncio
    async def test_get_databricks_secret_value_non_base64(self, databricks_secrets_service, mock_databricks_config):
        """Test retrieval when value is not base64 encoded"""
        scope = "test_scope"
        key = "test_key"
        secret_value = "plain_text_value"
        
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}), \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            # Mock HTTP response with non-base64 value
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = {"value": secret_value}
            
            mock_session = MagicMock()
            mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
            
            result = await databricks_secrets_service.get_databricks_secret_value(scope, key)
            
            assert result == secret_value
    
    @pytest.mark.asyncio
    async def test_get_databricks_secret_value_no_config(self, databricks_secrets_service):
        """Test get_databricks_secret_value when config is not found"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = None
            mock_service_class.return_value = mock_service
            
            result = await databricks_secrets_service.get_databricks_secret_value("scope", "key")
            
            assert result == ""
    
    @pytest.mark.asyncio
    async def test_get_databricks_secret_value_no_token(self, databricks_secrets_service, mock_databricks_config):
        """Test get_databricks_secret_value when token is not set"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch.dict(os.environ, {}, clear=True):
            
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            result = await databricks_secrets_service.get_databricks_secret_value("scope", "key")
            
            assert result == ""
    
    @pytest.mark.asyncio
    async def test_get_databricks_secret_value_http_error(self, databricks_secrets_service, mock_databricks_config):
        """Test get_databricks_secret_value with HTTP error"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}), \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            # Mock HTTP error response
            mock_response = AsyncMock()
            mock_response.status = 404
            mock_response.text = AsyncMock(return_value="Not found")
            
            mock_session = MagicMock()
            mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
            
            result = await databricks_secrets_service.get_databricks_secret_value("scope", "key")
            
            assert result == ""
    
    @pytest.mark.asyncio
    async def test_get_databricks_secret_value_exception(self, databricks_secrets_service):
        """Test get_databricks_secret_value with general exception"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service_class.side_effect = Exception("Unexpected error")
            
            result = await databricks_secrets_service.get_databricks_secret_value("scope", "key")
            
            assert result == ""


class TestSetDatabricksSecretValue:
    """Test the set_databricks_secret_value method"""
    
    @pytest.mark.asyncio
    async def test_set_databricks_secret_value_success(self, databricks_secrets_service, mock_databricks_config):
        """Test successful setting of Databricks secret value"""
        scope = "test_scope"
        key = "test_key"
        value = "test_value"
        
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}), \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            # Mock create_databricks_secret_scope
            with patch.object(databricks_secrets_service, 'create_databricks_secret_scope', new_callable=AsyncMock) as mock_create_scope:
                mock_create_scope.return_value = True
                
                # Mock HTTP response
                mock_response = AsyncMock()
                mock_response.status = 200
                
                mock_session = MagicMock()
                mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
                mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
                
                mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
                
                result = await databricks_secrets_service.set_databricks_secret_value(scope, key, value)
                
                assert result is True
                mock_create_scope.assert_called_once_with(
                    mock_databricks_config.workspace_url, 'test_token', scope
                )
    
    @pytest.mark.asyncio
    async def test_set_databricks_secret_value_no_config(self, databricks_secrets_service):
        """Test set_databricks_secret_value when config is not found"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = None
            mock_service_class.return_value = mock_service
            
            result = await databricks_secrets_service.set_databricks_secret_value("scope", "key", "value")
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_set_databricks_secret_value_no_token(self, databricks_secrets_service, mock_databricks_config):
        """Test set_databricks_secret_value when token is not set"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch.dict(os.environ, {}, clear=True):
            
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            result = await databricks_secrets_service.set_databricks_secret_value("scope", "key", "value")
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_set_databricks_secret_value_http_error(self, databricks_secrets_service, mock_databricks_config):
        """Test set_databricks_secret_value with HTTP error"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}), \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            with patch.object(databricks_secrets_service, 'create_databricks_secret_scope', new_callable=AsyncMock) as mock_create_scope:
                mock_create_scope.return_value = True
                
                # Mock HTTP error response
                mock_response = AsyncMock()
                mock_response.status = 400
                mock_response.text = AsyncMock(return_value="Bad request")
                
                mock_session = MagicMock()
                mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
                mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
                
                mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
                mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
                
                result = await databricks_secrets_service.set_databricks_secret_value("scope", "key", "value")
                
                assert result is False
    
    @pytest.mark.asyncio
    async def test_set_databricks_secret_value_exception(self, databricks_secrets_service):
        """Test set_databricks_secret_value with general exception"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service_class.side_effect = Exception("Unexpected error")
            
            result = await databricks_secrets_service.set_databricks_secret_value("scope", "key", "value")
            
            assert result is False


class TestDeleteDatabricksSecret:
    """Test the delete_databricks_secret method"""
    
    @pytest.mark.asyncio
    async def test_delete_databricks_secret_success(self, databricks_secrets_service, mock_databricks_config):
        """Test successful deletion of Databricks secret"""
        scope = "test_scope"
        key = "test_key"
        
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}), \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            mock_response = AsyncMock()
            mock_response.status = 200
            
            mock_session = MagicMock()
            mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
            
            result = await databricks_secrets_service.delete_databricks_secret(scope, key)
            
            assert result is True
            mock_session.post.assert_called_once()
            
            # Verify request data
            call_args = mock_session.post.call_args
            request_data = call_args[1]["json"]
            assert request_data["scope"] == scope
            assert request_data["key"] == key
    
    @pytest.mark.asyncio
    async def test_delete_databricks_secret_no_config(self, databricks_secrets_service):
        """Test delete_databricks_secret when config is not found"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = None
            mock_service_class.return_value = mock_service
            
            result = await databricks_secrets_service.delete_databricks_secret("scope", "key")
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_databricks_secret_no_token(self, databricks_secrets_service, mock_databricks_config):
        """Test delete_databricks_secret when token is not set"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch.dict(os.environ, {}, clear=True):
            
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            result = await databricks_secrets_service.delete_databricks_secret("scope", "key")
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_databricks_secret_http_error(self, databricks_secrets_service, mock_databricks_config):
        """Test delete_databricks_secret with HTTP error"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class, \
             patch.dict(os.environ, {'DATABRICKS_TOKEN': 'test_token'}), \
             patch('aiohttp.ClientSession') as mock_session_class:
            
            mock_service = AsyncMock()
            mock_service.get_databricks_config.return_value = mock_databricks_config
            mock_service_class.return_value = mock_service
            
            mock_response = AsyncMock()
            mock_response.status = 404
            mock_response.text = AsyncMock(return_value="Secret not found")
            
            mock_session = MagicMock()
            mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
            
            result = await databricks_secrets_service.delete_databricks_secret("scope", "key")
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_databricks_secret_exception(self, databricks_secrets_service):
        """Test delete_databricks_secret with general exception"""
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service_class.side_effect = Exception("Unexpected error")
            
            result = await databricks_secrets_service.delete_databricks_secret("scope", "key")
            
            assert result is False


class TestCreateDatabricksSecretScope:
    """Test the create_databricks_secret_scope method"""
    
    @pytest.mark.asyncio
    async def test_create_databricks_secret_scope_success(self, databricks_secrets_service):
        """Test successful creation of Databricks secret scope"""
        workspace_url = "https://test.databricks.com"
        token = "test_token"
        scope = "test_scope"
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 200
            
            mock_session = MagicMock()
            mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
            
            result = await databricks_secrets_service.create_databricks_secret_scope(
                workspace_url, token, scope
            )
            
            assert result is True
            mock_session.post.assert_called_once()
            
            # Verify request data
            call_args = mock_session.post.call_args
            request_data = call_args[1]["json"]
            assert request_data["scope"] == scope
            assert request_data["initial_manage_principal"] == "users"
    
    @pytest.mark.asyncio
    async def test_create_databricks_secret_scope_already_exists(self, databricks_secrets_service):
        """Test creating scope that already exists"""
        workspace_url = "https://test.databricks.com"
        token = "test_token"
        scope = "existing_scope"
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 400
            mock_response.text = AsyncMock(return_value="Scope already exists")
            
            mock_session = MagicMock()
            mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
            
            result = await databricks_secrets_service.create_databricks_secret_scope(
                workspace_url, token, scope
            )
            
            assert result is True  # Should return True when scope already exists
    
    @pytest.mark.asyncio
    async def test_create_databricks_secret_scope_resource_already_exists(self, databricks_secrets_service):
        """Test creating scope with RESOURCE_ALREADY_EXISTS error"""
        workspace_url = "https://test.databricks.com"
        token = "test_token"
        scope = "existing_scope"
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 400
            mock_response.text = AsyncMock(return_value="RESOURCE_ALREADY_EXISTS: Scope exists")
            
            mock_session = MagicMock()
            mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
            
            result = await databricks_secrets_service.create_databricks_secret_scope(
                workspace_url, token, scope
            )
            
            assert result is True  # Should return True when scope already exists
    
    @pytest.mark.asyncio
    async def test_create_databricks_secret_scope_bad_request(self, databricks_secrets_service):
        """Test creating scope with other 400 error"""
        workspace_url = "https://test.databricks.com"
        token = "test_token"
        scope = "invalid_scope"
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 400
            mock_response.text = AsyncMock(return_value="Invalid scope name")
            
            mock_session = MagicMock()
            mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
            
            result = await databricks_secrets_service.create_databricks_secret_scope(
                workspace_url, token, scope
            )
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_create_databricks_secret_scope_http_error(self, databricks_secrets_service):
        """Test creating scope with HTTP error"""
        workspace_url = "https://test.databricks.com"
        token = "test_token"
        scope = "test_scope"
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_response = AsyncMock()
            mock_response.status = 500
            mock_response.text = AsyncMock(return_value="Internal server error")
            
            mock_session = MagicMock()
            mock_session.post.return_value.__aenter__ = AsyncMock(return_value=mock_response)
            mock_session.post.return_value.__aexit__ = AsyncMock(return_value=False)
            
            mock_session_class.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_class.return_value.__aexit__ = AsyncMock(return_value=False)
            
            result = await databricks_secrets_service.create_databricks_secret_scope(
                workspace_url, token, scope
            )
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_create_databricks_secret_scope_exception(self, databricks_secrets_service):
        """Test creating scope with general exception"""
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session_class.side_effect = Exception("Connection error")
            
            result = await databricks_secrets_service.create_databricks_secret_scope(
                "workspace_url", "token", "scope"
            )
            
            assert result is False


class TestSetDatabricksToken:
    """Test the set_databricks_token method"""
    
    @pytest.mark.asyncio
    async def test_set_databricks_token_success(self, databricks_secrets_service):
        """Test successful setting of Databricks token"""
        scope = "test_scope"
        token = "test_token_value"
        
        with patch.object(databricks_secrets_service, 'set_databricks_secret_value') as mock_set_secret:
            mock_set_secret.return_value = True
            
            result = await databricks_secrets_service.set_databricks_token(scope, token)
            
            assert result is True
            mock_set_secret.assert_called_once_with(scope, "DATABRICKS_TOKEN", token)
    
    @pytest.mark.asyncio
    async def test_set_databricks_token_failure(self, databricks_secrets_service):
        """Test failed setting of Databricks token"""
        scope = "test_scope"
        token = "test_token_value"
        
        with patch.object(databricks_secrets_service, 'set_databricks_secret_value') as mock_set_secret:
            mock_set_secret.return_value = False
            
            result = await databricks_secrets_service.set_databricks_token(scope, token)
            
            assert result is False
            mock_set_secret.assert_called_once_with(scope, "DATABRICKS_TOKEN", token)


class TestSetupProviderApiKey:
    """Test the setup_provider_api_key method"""
    
    @pytest.mark.asyncio
    async def test_setup_provider_api_key_from_api_keys_service(self, databricks_secrets_service):
        """Test successful setup from API keys service"""
        key_name = "OPENAI_API_KEY"
        key_value = "test_api_key_value"
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service_class, \
             patch.dict(os.environ, {}, clear=True):
            
            mock_api_service_class.get_api_key_value = AsyncMock(return_value=key_value)
            
            result = await DatabricksSecretsService.setup_provider_api_key(None, key_name)
            
            assert result is True
            assert os.environ.get(key_name) == key_value
    
    @pytest.mark.asyncio
    async def test_setup_provider_api_key_from_databricks_secrets(self, databricks_secrets_service):
        """Test setup from Databricks secrets when not found in API keys"""
        key_name = "OPENAI_API_KEY"
        key_value = "test_secret_value"
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service_class, \
             patch('src.services.databricks_service.DatabricksService') as mock_databricks_service_class, \
             patch.dict(os.environ, {}, clear=True):
            
            # API keys service returns None
            mock_api_service_class.get_api_key_value = AsyncMock(return_value=None)
            
            # Mock databricks service and config
            mock_databricks_service = AsyncMock()
            mock_config = MagicMock()
            mock_config.is_enabled = True
            mock_config.workspace_url = "https://test.databricks.com"
            mock_config.secret_scope = "test_scope"
            mock_databricks_service.get_databricks_config.return_value = mock_config
            mock_databricks_service_class.return_value = mock_databricks_service
            
            # Create a service instance and mock get_databricks_secret_value
            with patch.object(DatabricksSecretsService, 'get_databricks_secret_value') as mock_get_secret:
                mock_get_secret.return_value = key_value
                
                result = await DatabricksSecretsService.setup_provider_api_key(None, key_name)
                
                assert result is True
                assert os.environ.get(key_name) == key_value
    
    @pytest.mark.asyncio
    async def test_setup_provider_api_key_not_found(self, databricks_secrets_service):
        """Test setup when key is not found anywhere"""
        key_name = "NONEXISTENT_API_KEY"
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service_class, \
             patch('src.services.databricks_service.DatabricksService') as mock_databricks_service_class, \
             patch.dict(os.environ, {}, clear=True):
            
            # API keys service returns None
            mock_api_service_class.get_api_key_value = AsyncMock(return_value=None)
            
            # Mock databricks service and config
            mock_databricks_service = AsyncMock()
            mock_config = MagicMock()
            mock_config.is_enabled = True
            mock_config.workspace_url = "https://test.databricks.com"
            mock_config.secret_scope = "test_scope"
            mock_databricks_service.get_databricks_config.return_value = mock_config
            mock_databricks_service_class.return_value = mock_databricks_service
            
            # Mock get_databricks_secret_value returns empty
            with patch.object(DatabricksSecretsService, 'get_databricks_secret_value') as mock_get_secret:
                mock_get_secret.return_value = ""
                
                result = await DatabricksSecretsService.setup_provider_api_key(None, key_name)
                
                assert result is False
                assert key_name not in os.environ
    
    @pytest.mark.asyncio
    async def test_setup_provider_api_key_databricks_config_error(self, databricks_secrets_service):
        """Test setup when databricks config validation fails"""
        key_name = "OPENAI_API_KEY"
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service_class, \
             patch('src.services.databricks_service.DatabricksService') as mock_databricks_service_class, \
             patch.dict(os.environ, {}, clear=True):
            
            # API keys service returns None
            mock_api_service_class.get_api_key_value = AsyncMock(return_value=None)
            
            # Mock databricks service throws exception
            mock_databricks_service = AsyncMock()
            mock_databricks_service.get_databricks_config.side_effect = Exception("Config error")
            mock_databricks_service_class.return_value = mock_databricks_service
            
            result = await DatabricksSecretsService.setup_provider_api_key(None, key_name)
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_setup_provider_api_key_general_exception(self, databricks_secrets_service):
        """Test setup with general exception"""
        key_name = "OPENAI_API_KEY"
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service_class:
            mock_api_service_class.get_api_key_value = AsyncMock(side_effect=Exception("General error"))
            
            result = await DatabricksSecretsService.setup_provider_api_key(None, key_name)
            
            assert result is False


class TestSetupProviderApiKeySync:
    """Test the _setup_provider_api_key_sync method"""
    
    def test_setup_provider_api_key_sync_success(self, databricks_secrets_service):
        """Test successful sync setup of provider API key"""
        key_name = "OPENAI_API_KEY"
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service_class:
            mock_api_service_class.setup_provider_api_key_sync.return_value = True
            
            result = DatabricksSecretsService._setup_provider_api_key_sync(None, key_name)
            
            assert result is True
            mock_api_service_class.setup_provider_api_key_sync.assert_called_once_with(None, key_name)
    
    def test_setup_provider_api_key_sync_failure(self, databricks_secrets_service):
        """Test failed sync setup of provider API key"""
        key_name = "NONEXISTENT_API_KEY"
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service_class:
            mock_api_service_class.setup_provider_api_key_sync.return_value = False
            
            result = DatabricksSecretsService._setup_provider_api_key_sync(None, key_name)
            
            assert result is False
    
    def test_setup_provider_api_key_sync_exception(self, databricks_secrets_service):
        """Test sync setup with exception"""
        key_name = "OPENAI_API_KEY"
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service_class:
            mock_api_service_class.setup_provider_api_key_sync.side_effect = Exception("API error")
            
            result = DatabricksSecretsService._setup_provider_api_key_sync(None, key_name)
            
            assert result is False


class TestGetPersonalAccessToken:
    """Test the get_personal_access_token method"""
    
    @pytest.mark.asyncio
    async def test_get_personal_access_token_success(self, databricks_secrets_service):
        """Test successful retrieval of personal access token"""
        token_value = "test_personal_access_token"
        
        mock_api_keys_service = AsyncMock()
        mock_api_keys_service.get_api_key_value.return_value = token_value
        databricks_secrets_service.set_api_keys_service(mock_api_keys_service)
        
        result = await databricks_secrets_service.get_personal_access_token()
        
        assert result == token_value
        mock_api_keys_service.get_api_key_value.assert_called_once_with("DATABRICKS_PERSONAL_ACCESS_TOKEN")
    
    @pytest.mark.asyncio
    async def test_get_personal_access_token_not_found(self, databricks_secrets_service):
        """Test retrieval when token not found"""
        mock_api_keys_service = AsyncMock()
        mock_api_keys_service.get_api_key_value.return_value = None
        databricks_secrets_service.set_api_keys_service(mock_api_keys_service)
        
        result = await databricks_secrets_service.get_personal_access_token()
        
        assert result == ""
    
    @pytest.mark.asyncio
    async def test_get_personal_access_token_no_service(self, databricks_secrets_service):
        """Test retrieval when API keys service is not set"""
        result = await databricks_secrets_service.get_personal_access_token()
        
        assert result == ""
    
    @pytest.mark.asyncio
    async def test_get_personal_access_token_exception(self, databricks_secrets_service):
        """Test retrieval with exception"""
        mock_api_keys_service = AsyncMock()
        mock_api_keys_service.get_api_key_value.side_effect = Exception("API error")
        databricks_secrets_service.set_api_keys_service(mock_api_keys_service)
        
        result = await databricks_secrets_service.get_personal_access_token()
        
        assert result == ""


class TestGetProviderApiKey:
    """Test the get_provider_api_key method"""
    
    @pytest.mark.asyncio
    async def test_get_provider_api_key_success(self, databricks_secrets_service):
        """Test successful retrieval of provider API key"""
        provider = "openai"
        key_value = "test_openai_key"
        
        mock_api_keys_service = AsyncMock()
        databricks_secrets_service.set_api_keys_service(mock_api_keys_service)
        
        with patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', new_callable=AsyncMock) as mock_get_provider_key:
            mock_get_provider_key.return_value = key_value
            
            result = await databricks_secrets_service.get_provider_api_key(provider)
            
            assert result == key_value
            mock_get_provider_key.assert_called_once_with(provider)
    
    @pytest.mark.asyncio
    async def test_get_provider_api_key_not_found(self, databricks_secrets_service):
        """Test retrieval when key not found"""
        provider = "nonexistent"
        
        mock_api_keys_service = AsyncMock()
        databricks_secrets_service.set_api_keys_service(mock_api_keys_service)
        
        with patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', new_callable=AsyncMock) as mock_get_provider_key:
            mock_get_provider_key.return_value = None
            
            result = await databricks_secrets_service.get_provider_api_key(provider)
            
            assert result == ""
    
    @pytest.mark.asyncio
    async def test_get_provider_api_key_no_service(self, databricks_secrets_service):
        """Test retrieval when API keys service is not set"""
        result = await databricks_secrets_service.get_provider_api_key("openai")
        
        assert result == ""
    
    @pytest.mark.asyncio
    async def test_get_provider_api_key_exception(self, databricks_secrets_service):
        """Test retrieval with exception"""
        provider = "openai"
        
        mock_api_keys_service = AsyncMock()
        databricks_secrets_service.set_api_keys_service(mock_api_keys_service)
        
        with patch('src.services.api_keys_service.ApiKeysService.get_provider_api_key', new_callable=AsyncMock) as mock_get_provider_key:
            mock_get_provider_key.side_effect = Exception("API error")
            
            result = await databricks_secrets_service.get_provider_api_key(provider)
            
            assert result == ""


class TestGetAllDatabricksTokens:
    """Test the get_all_databricks_tokens method"""
    
    @pytest.mark.asyncio
    async def test_get_all_databricks_tokens_success(self, databricks_secrets_service):
        """Test successful retrieval of all Databricks tokens"""
        token_values = {
            "DATABRICKS_TOKEN": "token1",
            "DATABRICKS_API_KEY": "token2",
            "DATABRICKS_PERSONAL_ACCESS_TOKEN": "token3"
        }
        
        mock_api_keys_service = AsyncMock()
        mock_api_keys_service.get_api_key_value.side_effect = lambda key: token_values.get(key)
        databricks_secrets_service.set_api_keys_service(mock_api_keys_service)
        
        result = await databricks_secrets_service.get_all_databricks_tokens()
        
        assert len(result) == 3
        assert "token1" in result
        assert "token2" in result
        assert "token3" in result
    
    @pytest.mark.asyncio
    async def test_get_all_databricks_tokens_partial(self, databricks_secrets_service):
        """Test retrieval when only some tokens are found"""
        token_values = {
            "DATABRICKS_TOKEN": "token1",
            "DATABRICKS_API_KEY": None,
            "DATABRICKS_PERSONAL_ACCESS_TOKEN": "token3"
        }
        
        mock_api_keys_service = AsyncMock()
        mock_api_keys_service.get_api_key_value.side_effect = lambda key: token_values.get(key)
        databricks_secrets_service.set_api_keys_service(mock_api_keys_service)
        
        result = await databricks_secrets_service.get_all_databricks_tokens()
        
        assert len(result) == 2
        assert "token1" in result
        assert "token3" in result
    
    @pytest.mark.asyncio
    async def test_get_all_databricks_tokens_no_service(self, databricks_secrets_service):
        """Test retrieval when API keys service is not set"""
        result = await databricks_secrets_service.get_all_databricks_tokens()
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_all_databricks_tokens_exception(self, databricks_secrets_service):
        """Test retrieval with exception"""
        mock_api_keys_service = AsyncMock()
        mock_api_keys_service.get_api_key_value.side_effect = Exception("API error")
        databricks_secrets_service.set_api_keys_service(mock_api_keys_service)
        
        result = await databricks_secrets_service.get_all_databricks_tokens()
        
        assert result == []