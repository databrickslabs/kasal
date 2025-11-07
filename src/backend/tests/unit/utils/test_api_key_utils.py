"""
Unit tests for api_key_utils module.
"""

import pytest
import os
from unittest.mock import Mock, patch, AsyncMock
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from src.utils.api_key_utils import (
    async_setup_provider_api_key,
    setup_provider_api_key,
    async_setup_openai_api_key,
    async_setup_anthropic_api_key,
    async_setup_deepseek_api_key,
    async_setup_all_api_keys,
    setup_openai_api_key,
    setup_anthropic_api_key,
    setup_deepseek_api_key,
    setup_all_api_keys,
    async_get_databricks_personal_access_token,
    get_databricks_personal_access_token
)


class TestAsyncApiKeyUtils:
    """Test async API key utility functions."""
    
    @pytest.mark.asyncio
    async def test_async_setup_provider_api_key_success(self):
        """Test successful async provider API key setup."""
        mock_db = Mock(spec=AsyncSession)
        key_name = "TEST_API_KEY"
        
        with patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key', new_callable=AsyncMock) as mock_service:
            mock_service.return_value = True
            
            result = await async_setup_provider_api_key(mock_db, key_name)
            
            assert result is True
            mock_service.assert_called_once_with(mock_db, key_name)
    
    @pytest.mark.asyncio
    async def test_async_setup_provider_api_key_failure(self):
        """Test failed async provider API key setup."""
        mock_db = Mock(spec=AsyncSession)
        key_name = "TEST_API_KEY"
        
        with patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key', new_callable=AsyncMock) as mock_service:
            mock_service.return_value = False
            
            result = await async_setup_provider_api_key(mock_db, key_name)
            
            assert result is False
            mock_service.assert_called_once_with(mock_db, key_name)
    
    @pytest.mark.asyncio
    async def test_async_setup_openai_api_key(self):
        """Test async OpenAI API key setup."""
        mock_db = Mock(spec=AsyncSession)
        
        with patch('src.utils.api_key_utils.ApiKeysService.setup_openai_api_key', new_callable=AsyncMock) as mock_service:
            mock_service.return_value = True
            
            result = await async_setup_openai_api_key(mock_db)
            
            assert result is True
            mock_service.assert_called_once_with(mock_db)
    
    @pytest.mark.asyncio
    async def test_async_setup_anthropic_api_key(self):
        """Test async Anthropic API key setup."""
        mock_db = Mock(spec=AsyncSession)
        
        with patch('src.utils.api_key_utils.ApiKeysService.setup_anthropic_api_key', new_callable=AsyncMock) as mock_service:
            mock_service.return_value = True
            
            result = await async_setup_anthropic_api_key(mock_db)
            
            assert result is True
            mock_service.assert_called_once_with(mock_db)
    
    @pytest.mark.asyncio
    async def test_async_setup_deepseek_api_key(self):
        """Test async DeepSeek API key setup."""
        mock_db = Mock(spec=AsyncSession)
        
        with patch('src.utils.api_key_utils.ApiKeysService.setup_deepseek_api_key', new_callable=AsyncMock) as mock_service:
            mock_service.return_value = True
            
            result = await async_setup_deepseek_api_key(mock_db)
            
            assert result is True
            mock_service.assert_called_once_with(mock_db)
    
    @pytest.mark.asyncio
    async def test_async_setup_all_api_keys(self):
        """Test async setup of all API keys."""
        mock_db = Mock(spec=AsyncSession)
        
        with patch('src.utils.api_key_utils.ApiKeysService.setup_all_api_keys', new_callable=AsyncMock) as mock_service:
            await async_setup_all_api_keys(mock_db)
            
            mock_service.assert_called_once_with(mock_db)
    
    @pytest.mark.asyncio
    async def test_async_get_databricks_personal_access_token(self):
        """Test async Databricks token retrieval."""
        mock_db = Mock(spec=AsyncSession)
        expected_token = "dapi-test-token"
        
        with patch('src.utils.api_key_utils.ApiKeysService.get_api_key_value', new_callable=AsyncMock) as mock_service:
            mock_service.return_value = expected_token
            
            result = await async_get_databricks_personal_access_token(mock_db)
            
            assert result == expected_token
            mock_service.assert_called_once_with(mock_db, "DATABRICKS_TOKEN")
    
    @pytest.mark.asyncio
    async def test_async_get_databricks_personal_access_token_empty(self):
        """Test async Databricks token retrieval when token is None."""
        mock_db = Mock(spec=AsyncSession)
        
        with patch('src.utils.api_key_utils.ApiKeysService.get_api_key_value', new_callable=AsyncMock) as mock_service:
            mock_service.return_value = None
            
            result = await async_get_databricks_personal_access_token(mock_db)
            
            assert result == ""
            mock_service.assert_called_once_with(mock_db, "DATABRICKS_TOKEN")


class TestSyncApiKeyUtils:
    """Test synchronous API key utility functions."""
    
    def test_setup_provider_api_key_success(self):
        """Test successful sync provider API key setup."""
        mock_db = Mock(spec=Session)
        key_name = "TEST_API_KEY"
        
        with patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync') as mock_service:
            mock_service.return_value = True
            
            result = setup_provider_api_key(mock_db, key_name)
            
            assert result is True
            mock_service.assert_called_once_with(mock_db, key_name)
    
    def test_setup_provider_api_key_failure(self):
        """Test failed sync provider API key setup."""
        mock_db = Mock(spec=Session)
        key_name = "TEST_API_KEY"
        
        with patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync') as mock_service:
            mock_service.return_value = False
            
            result = setup_provider_api_key(mock_db, key_name)
            
            assert result is False
            mock_service.assert_called_once_with(mock_db, key_name)
    
    def test_setup_openai_api_key(self):
        """Test sync OpenAI API key setup."""
        mock_db = Mock(spec=Session)
        
        with patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync') as mock_service:
            mock_service.return_value = True
            
            result = setup_openai_api_key(mock_db)
            
            assert result is True
            mock_service.assert_called_once_with(mock_db, "OPENAI_API_KEY")
    
    def test_setup_anthropic_api_key(self):
        """Test sync Anthropic API key setup."""
        mock_db = Mock(spec=Session)
        
        with patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync') as mock_service:
            mock_service.return_value = True
            
            result = setup_anthropic_api_key(mock_db)
            
            assert result is True
            mock_service.assert_called_once_with(mock_db, "ANTHROPIC_API_KEY")
    
    def test_setup_deepseek_api_key(self):
        """Test sync DeepSeek API key setup."""
        mock_db = Mock(spec=Session)
        
        with patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync') as mock_service:
            mock_service.return_value = True
            
            result = setup_deepseek_api_key(mock_db)
            
            assert result is True
            mock_service.assert_called_once_with(mock_db, "DEEPSEEK_API_KEY")
    
    def test_setup_all_api_keys(self):
        """Test sync setup of all API keys."""
        mock_db = Mock(spec=Session)
        
        with patch('src.utils.api_key_utils.setup_openai_api_key') as mock_openai, \
             patch('src.utils.api_key_utils.setup_anthropic_api_key') as mock_anthropic, \
             patch('src.utils.api_key_utils.setup_deepseek_api_key') as mock_deepseek:
            
            setup_all_api_keys(mock_db)
            
            mock_openai.assert_called_once_with(mock_db)
            mock_anthropic.assert_called_once_with(mock_db)
            mock_deepseek.assert_called_once_with(mock_db)
    
    def test_get_databricks_personal_access_token_with_token(self):
        """Test sync Databricks token retrieval with environment variable set."""
        mock_db = Mock(spec=Session)
        expected_token = "dapi-test-token"

        # Mock the auth context to return the token
        mock_auth = Mock()
        mock_auth.token = expected_token

        with patch('src.utils.databricks_auth.get_auth_context') as mock_get_auth:
            # Mock asyncio.run to return the mock auth
            mock_get_auth.return_value = mock_auth

            # Need to mock asyncio.run since it's called inside the function
            with patch('asyncio.run', return_value=mock_auth):
                result = get_databricks_personal_access_token(mock_db)

                assert result == expected_token
    
    def test_get_databricks_personal_access_token_empty(self):
        """Test sync Databricks token retrieval with no environment variable."""
        mock_db = Mock(spec=Session)

        # Mock the auth context to return None (no token)
        with patch('src.utils.databricks_auth.get_auth_context') as mock_get_auth, \
             patch('asyncio.run') as mock_run, \
             patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync') as mock_service, \
             patch.dict(os.environ, {}, clear=True):
            mock_run.return_value = None  # No auth context
            mock_service.return_value = False  # No token in DB

            result = get_databricks_personal_access_token(mock_db)

            assert result == ""