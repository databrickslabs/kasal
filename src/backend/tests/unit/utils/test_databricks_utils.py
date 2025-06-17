"""
Unit tests for databricks_utils module.
"""

import os
import pytest
from unittest.mock import Mock, patch, AsyncMock
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from src.utils.databricks_utils import (
    setup_databricks_token,
    setup_databricks_token_async,
    setup_databricks_token_sync,
    get_databricks_config
)


class TestSetupDatabricksToken:
    """Test setup_databricks_token function."""
    
    @pytest.mark.asyncio
    async def test_setup_databricks_token_from_api_keys_success(self):
        """Test successful token setup from API keys."""
        mock_db = Mock(spec=AsyncSession)
        expected_token = "dapi-test-token"
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service, \
             patch.dict(os.environ, {}, clear=True):
            
            mock_api_service.get_api_key_value = AsyncMock(return_value=expected_token)
            
            result = await setup_databricks_token(mock_db)
            
            assert result is True
            assert os.environ.get("DATABRICKS_TOKEN") == expected_token
            mock_api_service.get_api_key_value.assert_called_once_with(mock_db, "DATABRICKS_TOKEN")
    
    @pytest.mark.asyncio
    async def test_setup_databricks_token_from_databricks_secrets(self):
        """Test token setup from Databricks secrets when API keys fail."""
        mock_db = Mock(spec=AsyncSession)
        expected_token = "dapi-secret-token"
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service, \
             patch('src.services.databricks_secrets_service.DatabricksSecretsService') as mock_secrets_service, \
             patch.dict(os.environ, {}, clear=True):
            
            # API keys returns None
            mock_api_service.get_api_key_value = AsyncMock(return_value=None)
            
            # Databricks secrets service mocking
            mock_secrets_instance = Mock()
            mock_secrets_instance.validate_databricks_config = AsyncMock(return_value=("https://test.databricks.com", "test-scope"))
            mock_secrets_instance.get_databricks_secret_value = AsyncMock(return_value=expected_token)
            mock_secrets_service.return_value = mock_secrets_instance
            
            result = await setup_databricks_token(mock_db)
            
            assert result is True
            assert os.environ.get("DATABRICKS_TOKEN") == expected_token
            mock_secrets_instance.validate_databricks_config.assert_called_once()
            mock_secrets_instance.get_databricks_secret_value.assert_called_once_with("test-scope", "DATABRICKS_TOKEN")
    
    @pytest.mark.asyncio
    async def test_setup_databricks_token_no_token_found(self):
        """Test token setup when no token is found."""
        mock_db = Mock(spec=AsyncSession)
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service, \
             patch('src.services.databricks_secrets_service.DatabricksSecretsService') as mock_secrets_service:
            
            # API keys returns None
            mock_api_service.get_api_key_value = AsyncMock(return_value=None)
            
            # Databricks secrets service returns None for token
            mock_secrets_instance = Mock()
            mock_secrets_instance.validate_databricks_config = AsyncMock(return_value=("https://test.databricks.com", "test-scope"))
            mock_secrets_instance.get_databricks_secret_value = AsyncMock(return_value=None)
            mock_secrets_service.return_value = mock_secrets_instance
            
            result = await setup_databricks_token(mock_db)
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_setup_databricks_token_databricks_config_exception(self):
        """Test token setup when Databricks config validation fails."""
        mock_db = Mock(spec=AsyncSession)
        expected_token = "dapi-api-key-token"
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service, \
             patch('src.services.databricks_secrets_service.DatabricksSecretsService') as mock_secrets_service, \
             patch.dict(os.environ, {}, clear=True):
            
            # API keys returns None initially
            mock_api_service.get_api_key_value = AsyncMock(return_value=None)
            
            # Databricks secrets service raises exception
            mock_secrets_instance = Mock()
            mock_secrets_instance.validate_databricks_config = AsyncMock(side_effect=Exception("Config error"))
            mock_secrets_service.return_value = mock_secrets_instance
            
            # But we should still return False if no token is found
            result = await setup_databricks_token(mock_db)
            
            assert result is False
    
    @pytest.mark.asyncio
    async def test_setup_databricks_token_general_exception(self):
        """Test token setup when a general exception occurs."""
        mock_db = Mock(spec=AsyncSession)
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service:
            mock_api_service.get_api_key_value = AsyncMock(side_effect=Exception("Database error"))
            
            result = await setup_databricks_token(mock_db)
            
            assert result is False


class TestSetupDatabricksTokenAsync:
    """Test setup_databricks_token_async function (alias)."""
    
    @pytest.mark.asyncio
    async def test_setup_databricks_token_async_calls_main_function(self):
        """Test that async alias calls the main function."""
        mock_db = Mock(spec=AsyncSession)
        
        with patch('src.utils.databricks_utils.setup_databricks_token') as mock_setup:
            mock_setup.return_value = True
            
            result = await setup_databricks_token_async(mock_db)
            
            assert result is True
            mock_setup.assert_called_once_with(mock_db)


class TestSetupDatabricksTokenSync:
    """Test setup_databricks_token_sync function."""
    
    def test_setup_databricks_token_sync_success(self):
        """Test successful sync token setup."""
        mock_db = Mock(spec=Session)
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service:
            mock_api_service.setup_provider_api_key_sync = Mock(return_value=True)
            
            result = setup_databricks_token_sync(mock_db)
            
            assert result is True
            mock_api_service.setup_provider_api_key_sync.assert_called_once_with(mock_db, "DATABRICKS_TOKEN")
    
    def test_setup_databricks_token_sync_failure(self):
        """Test failed sync token setup."""
        mock_db = Mock(spec=Session)
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service:
            mock_api_service.setup_provider_api_key_sync = Mock(return_value=False)
            
            result = setup_databricks_token_sync(mock_db)
            
            assert result is False
            mock_api_service.setup_provider_api_key_sync.assert_called_once_with(mock_db, "DATABRICKS_TOKEN")
    
    def test_setup_databricks_token_sync_exception(self):
        """Test sync token setup with exception."""
        mock_db = Mock(spec=Session)
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service:
            mock_api_service.setup_provider_api_key_sync = Mock(side_effect=Exception("Service error"))
            
            result = setup_databricks_token_sync(mock_db)
            
            assert result is False


class TestGetDatabricksConfig:
    """Test get_databricks_config function."""
    
    @pytest.mark.asyncio
    async def test_get_databricks_config_success(self):
        """Test successful Databricks config retrieval."""
        mock_db = Mock(spec=AsyncSession)
        expected_config = Mock()
        expected_config.workspace_url = "https://test.databricks.com"
        expected_config.apps_enabled = True
        
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_databricks_config = AsyncMock(return_value=expected_config)
            mock_service_class.return_value = mock_service
            
            result = await get_databricks_config(mock_db)
            
            assert result == expected_config
            mock_service_class.assert_called_once_with(mock_db)
            mock_service.get_databricks_config.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_databricks_config_service_exception(self):
        """Test Databricks config retrieval with service exception."""
        mock_db = Mock(spec=AsyncSession)
        
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service_class.side_effect = Exception("Service creation error")
            
            result = await get_databricks_config(mock_db)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_get_databricks_config_method_exception(self):
        """Test Databricks config retrieval with method exception."""
        mock_db = Mock(spec=AsyncSession)
        
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_databricks_config = AsyncMock(side_effect=Exception("Method error"))
            mock_service_class.return_value = mock_service
            
            result = await get_databricks_config(mock_db)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_get_databricks_config_returns_none(self):
        """Test Databricks config retrieval when service returns None."""
        mock_db = Mock(spec=AsyncSession)
        
        with patch('src.services.databricks_service.DatabricksService') as mock_service_class:
            mock_service = Mock()
            mock_service.get_databricks_config = AsyncMock(return_value=None)
            mock_service_class.return_value = mock_service
            
            result = await get_databricks_config(mock_db)
            
            assert result is None


class TestIntegrationScenarios:
    """Test integration scenarios for databricks_utils."""
    
    @pytest.mark.asyncio
    async def test_complete_token_setup_workflow(self):
        """Test complete token setup workflow from API keys to environment."""
        mock_db = Mock(spec=AsyncSession)
        test_token = "dapi-integration-test-token"
        
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service, \
             patch.dict(os.environ, {}, clear=True):
            
            mock_api_service.get_api_key_value = AsyncMock(return_value=test_token)
            
            # Test the async version
            async_result = await setup_databricks_token(mock_db)
            assert async_result is True
            assert os.environ.get("DATABRICKS_TOKEN") == test_token
            
            # Test the alias
            alias_result = await setup_databricks_token_async(mock_db)
            assert alias_result is True
    
    def test_sync_vs_async_token_setup(self):
        """Test that sync and async versions work similarly."""
        mock_async_db = Mock(spec=AsyncSession)
        mock_sync_db = Mock(spec=Session)
        
        # Test sync version
        with patch('src.services.api_keys_service.ApiKeysService') as mock_api_service:
            mock_api_service.setup_provider_api_key_sync = Mock(return_value=True)
            
            sync_result = setup_databricks_token_sync(mock_sync_db)
            assert sync_result is True
            
            # Verify the correct sync method was called
            mock_api_service.setup_provider_api_key_sync.assert_called_with(
                mock_sync_db, "DATABRICKS_TOKEN"
            )