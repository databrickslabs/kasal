"""
Comprehensive unit tests for API key utilities.

Tests all wrapper functions for API key management.
"""
import pytest
import asyncio
from unittest.mock import patch, Mock, AsyncMock
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


class TestAsyncSetupProviderApiKey:
    """Test async_setup_provider_api_key function."""

    @pytest.mark.asyncio
    @patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key')
    async def test_async_setup_provider_api_key_success(self, mock_setup):
        """Test async_setup_provider_api_key with successful setup."""
        mock_setup.return_value = True
        mock_db = Mock(spec=AsyncSession)
        
        result = await async_setup_provider_api_key(mock_db, "TEST_API_KEY")
        
        assert result is True
        mock_setup.assert_called_once_with(mock_db, "TEST_API_KEY")

    @pytest.mark.asyncio
    @patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key')
    async def test_async_setup_provider_api_key_failure(self, mock_setup):
        """Test async_setup_provider_api_key with failed setup."""
        mock_setup.return_value = False
        mock_db = Mock(spec=AsyncSession)
        
        result = await async_setup_provider_api_key(mock_db, "TEST_API_KEY")
        
        assert result is False
        mock_setup.assert_called_once_with(mock_db, "TEST_API_KEY")


class TestSetupProviderApiKey:
    """Test setup_provider_api_key function."""

    @patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync')
    def test_setup_provider_api_key_success(self, mock_setup):
        """Test setup_provider_api_key with successful setup."""
        mock_setup.return_value = True
        mock_db = Mock(spec=Session)
        
        result = setup_provider_api_key(mock_db, "TEST_API_KEY")
        
        assert result is True
        mock_setup.assert_called_once_with(mock_db, "TEST_API_KEY")

    @patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync')
    def test_setup_provider_api_key_failure(self, mock_setup):
        """Test setup_provider_api_key with failed setup."""
        mock_setup.return_value = False
        mock_db = Mock(spec=Session)
        
        result = setup_provider_api_key(mock_db, "TEST_API_KEY")
        
        assert result is False
        mock_setup.assert_called_once_with(mock_db, "TEST_API_KEY")


class TestAsyncSpecificProviderSetup:
    """Test async setup functions for specific providers."""

    @pytest.mark.asyncio
    @patch('src.utils.api_key_utils.ApiKeysService.setup_openai_api_key')
    async def test_async_setup_openai_api_key(self, mock_setup):
        """Test async_setup_openai_api_key."""
        mock_setup.return_value = True
        mock_db = Mock(spec=AsyncSession)
        
        result = await async_setup_openai_api_key(mock_db)
        
        assert result is True
        mock_setup.assert_called_once_with(mock_db)

    @pytest.mark.asyncio
    @patch('src.utils.api_key_utils.ApiKeysService.setup_anthropic_api_key')
    async def test_async_setup_anthropic_api_key(self, mock_setup):
        """Test async_setup_anthropic_api_key."""
        mock_setup.return_value = True
        mock_db = Mock(spec=AsyncSession)
        
        result = await async_setup_anthropic_api_key(mock_db)
        
        assert result is True
        mock_setup.assert_called_once_with(mock_db)

    @pytest.mark.asyncio
    @patch('src.utils.api_key_utils.ApiKeysService.setup_deepseek_api_key')
    async def test_async_setup_deepseek_api_key(self, mock_setup):
        """Test async_setup_deepseek_api_key."""
        mock_setup.return_value = True
        mock_db = Mock(spec=AsyncSession)
        
        result = await async_setup_deepseek_api_key(mock_db)
        
        assert result is True
        mock_setup.assert_called_once_with(mock_db)


class TestAsyncSetupAllApiKeys:
    """Test async_setup_all_api_keys function."""

    @pytest.mark.asyncio
    @patch('src.utils.api_key_utils.ApiKeysService.setup_all_api_keys')
    async def test_async_setup_all_api_keys(self, mock_setup):
        """Test async_setup_all_api_keys."""
        mock_setup.return_value = None
        mock_db = Mock(spec=AsyncSession)
        
        result = await async_setup_all_api_keys(mock_db)
        
        assert result is None
        mock_setup.assert_called_once_with(mock_db)


class TestSyncSpecificProviderSetup:
    """Test sync setup functions for specific providers."""

    @patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync')
    def test_setup_openai_api_key(self, mock_setup):
        """Test setup_openai_api_key."""
        mock_setup.return_value = True
        mock_db = Mock(spec=Session)
        
        result = setup_openai_api_key(mock_db)
        
        assert result is True
        mock_setup.assert_called_once_with(mock_db, "OPENAI_API_KEY")

    @patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync')
    def test_setup_anthropic_api_key(self, mock_setup):
        """Test setup_anthropic_api_key."""
        mock_setup.return_value = True
        mock_db = Mock(spec=Session)
        
        result = setup_anthropic_api_key(mock_db)
        
        assert result is True
        mock_setup.assert_called_once_with(mock_db, "ANTHROPIC_API_KEY")

    @patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync')
    def test_setup_deepseek_api_key(self, mock_setup):
        """Test setup_deepseek_api_key."""
        mock_setup.return_value = True
        mock_db = Mock(spec=Session)
        
        result = setup_deepseek_api_key(mock_db)
        
        assert result is True
        mock_setup.assert_called_once_with(mock_db, "DEEPSEEK_API_KEY")


class TestSetupAllApiKeys:
    """Test setup_all_api_keys function."""

    @patch('src.utils.api_key_utils.setup_openai_api_key')
    @patch('src.utils.api_key_utils.setup_anthropic_api_key')
    @patch('src.utils.api_key_utils.setup_deepseek_api_key')
    def test_setup_all_api_keys(self, mock_deepseek, mock_anthropic, mock_openai):
        """Test setup_all_api_keys calls all provider setup functions."""
        mock_openai.return_value = True
        mock_anthropic.return_value = True
        mock_deepseek.return_value = True
        mock_db = Mock(spec=Session)
        
        result = setup_all_api_keys(mock_db)
        
        assert result is None
        mock_openai.assert_called_once_with(mock_db)
        mock_anthropic.assert_called_once_with(mock_db)
        mock_deepseek.assert_called_once_with(mock_db)

    @patch('src.utils.api_key_utils.setup_openai_api_key')
    @patch('src.utils.api_key_utils.setup_anthropic_api_key')
    @patch('src.utils.api_key_utils.setup_deepseek_api_key')
    def test_setup_all_api_keys_with_failures(self, mock_deepseek, mock_anthropic, mock_openai):
        """Test setup_all_api_keys continues even if some providers fail."""
        mock_openai.return_value = False
        mock_anthropic.return_value = True
        mock_deepseek.return_value = False
        mock_db = Mock(spec=Session)
        
        result = setup_all_api_keys(mock_db)
        
        assert result is None
        mock_openai.assert_called_once_with(mock_db)
        mock_anthropic.assert_called_once_with(mock_db)
        mock_deepseek.assert_called_once_with(mock_db)


class TestAsyncGetDatabricksPersonalAccessToken:
    """Test async_get_databricks_personal_access_token function."""

    @pytest.mark.asyncio
    @patch('src.utils.api_key_utils.ApiKeysService.get_api_key_value')
    async def test_async_get_databricks_personal_access_token_success(self, mock_get_value):
        """Test async_get_databricks_personal_access_token with token found."""
        mock_get_value.return_value = "test-token-123"
        mock_db = Mock(spec=AsyncSession)
        
        result = await async_get_databricks_personal_access_token(mock_db)
        
        assert result == "test-token-123"
        mock_get_value.assert_called_once_with(mock_db, "DATABRICKS_TOKEN")

    @pytest.mark.asyncio
    @patch('src.utils.api_key_utils.ApiKeysService.get_api_key_value')
    async def test_async_get_databricks_personal_access_token_none(self, mock_get_value):
        """Test async_get_databricks_personal_access_token with no token found."""
        mock_get_value.return_value = None
        mock_db = Mock(spec=AsyncSession)
        
        result = await async_get_databricks_personal_access_token(mock_db)
        
        assert result == ""
        mock_get_value.assert_called_once_with(mock_db, "DATABRICKS_TOKEN")


class TestGetDatabricksPersonalAccessToken:
    """Test get_databricks_personal_access_token function."""

    @patch('asyncio.run')
    def test_get_databricks_personal_access_token_unified_auth_success(self, mock_run):
        """Test get_databricks_personal_access_token with unified auth success."""
        mock_auth = Mock()
        mock_auth.token = "unified-token-123"
        mock_run.return_value = mock_auth
        mock_db = Mock(spec=Session)

        result = get_databricks_personal_access_token(mock_db)

        assert result == "unified-token-123"
        mock_run.assert_called_once()

    @patch('asyncio.run')
    @patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync')
    def test_get_databricks_personal_access_token_unified_auth_no_token(self, mock_setup, mock_run):
        """Test get_databricks_personal_access_token with unified auth but no token."""
        mock_auth = Mock()
        mock_auth.token = None
        mock_run.return_value = mock_auth
        mock_setup.return_value = "fallback-token-456"
        mock_db = Mock(spec=Session)

        result = get_databricks_personal_access_token(mock_db)

        assert result == "fallback-token-456"
        mock_setup.assert_called_once_with(mock_db, "DATABRICKS_TOKEN")

    @patch('asyncio.run')
    @patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync')
    def test_get_databricks_personal_access_token_unified_auth_exception(self, mock_setup, mock_run):
        """Test get_databricks_personal_access_token with unified auth exception."""
        mock_run.side_effect = Exception("Auth failed")
        mock_setup.return_value = "fallback-token-789"
        mock_db = Mock(spec=Session)

        result = get_databricks_personal_access_token(mock_db)

        assert result == "fallback-token-789"
        mock_setup.assert_called_once_with(mock_db, "DATABRICKS_TOKEN")

    @patch('asyncio.run')
    @patch('src.utils.api_key_utils.ApiKeysService.setup_provider_api_key_sync')
    def test_get_databricks_personal_access_token_fallback_none(self, mock_setup, mock_run):
        """Test get_databricks_personal_access_token with fallback returning None."""
        mock_run.side_effect = Exception("Auth failed")
        mock_setup.return_value = None
        mock_db = Mock(spec=Session)

        result = get_databricks_personal_access_token(mock_db)

        assert result == ""
        mock_setup.assert_called_once_with(mock_db, "DATABRICKS_TOKEN")


class TestFunctionExistence:
    """Test that all expected functions exist and are callable."""

    def test_all_functions_exist(self):
        """Test all expected functions exist."""
        functions = [
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
        ]
        
        for func in functions:
            assert callable(func)

    def test_async_functions_are_coroutines(self):
        """Test async functions are coroutine functions."""
        async_functions = [
            async_setup_provider_api_key,
            async_setup_openai_api_key,
            async_setup_anthropic_api_key,
            async_setup_deepseek_api_key,
            async_setup_all_api_keys,
            async_get_databricks_personal_access_token
        ]
        
        for func in async_functions:
            assert asyncio.iscoroutinefunction(func)

    def test_sync_functions_are_not_coroutines(self):
        """Test sync functions are not coroutine functions."""
        sync_functions = [
            setup_provider_api_key,
            setup_openai_api_key,
            setup_anthropic_api_key,
            setup_deepseek_api_key,
            setup_all_api_keys,
            get_databricks_personal_access_token
        ]
        
        for func in sync_functions:
            assert not asyncio.iscoroutinefunction(func)
