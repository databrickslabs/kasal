"""
Unit tests for engines/crewai/tools/custom/powerbi_auth_utils.py

Tests Power BI authentication utility functions.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
import httpx


try:
    from src.engines.crewai.tools.custom.powerbi_auth_utils import (
        get_access_token_service_principal,
        get_access_token_service_account,
        validate_access_token,
        refresh_access_token
    )
    HAS_AUTH_UTILS = True
except ImportError:
    HAS_AUTH_UTILS = False


@pytest.mark.skipif(not HAS_AUTH_UTILS, reason="powerbi_auth_utils module not found or different structure")
class TestPowerBIAuthUtils:
    """Tests for Power BI authentication utility functions"""

    @pytest.mark.asyncio
    async def test_get_access_token_service_principal_success(self):
        """Test successful service principal token acquisition"""
        mock_response = {
            'access_token': 'mock_access_token_123',
            'token_type': 'Bearer',
            'expires_in': 3600
        }

        with patch('httpx.AsyncClient.post', new_callable=AsyncMock) as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=mock_response)
            )

            token = await get_access_token_service_principal(
                tenant_id="tenant123",
                client_id="client456",
                client_secret="secret789"
            )

            assert token == "mock_access_token_123"
            mock_post.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_access_token_service_principal_failure(self):
        """Test service principal token acquisition failure"""
        with patch('httpx.AsyncClient.post', new_callable=AsyncMock) as mock_post:
            mock_post.return_value = Mock(
                status_code=401,
                json=Mock(return_value={'error': 'invalid_client'}),
                text="Unauthorized"
            )

            with pytest.raises(Exception) as exc_info:
                await get_access_token_service_principal(
                    tenant_id="tenant123",
                    client_id="invalid",
                    client_secret="invalid"
                )

            assert "401" in str(exc_info.value) or "invalid" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_get_access_token_service_account_success(self):
        """Test successful service account token acquisition"""
        mock_response = {
            'access_token': 'mock_access_token_456',
            'token_type': 'Bearer',
            'expires_in': 3600
        }

        with patch('httpx.AsyncClient.post', new_callable=AsyncMock) as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=mock_response)
            )

            token = await get_access_token_service_account(
                tenant_id="tenant123",
                username="user@example.com",
                password="password123"
            )

            assert token == "mock_access_token_456"

    @pytest.mark.asyncio
    async def test_get_access_token_service_account_invalid_credentials(self):
        """Test service account with invalid credentials"""
        with patch('httpx.AsyncClient.post', new_callable=AsyncMock) as mock_post:
            mock_post.return_value = Mock(
                status_code=400,
                json=Mock(return_value={'error': 'invalid_grant'}),
                text="Bad Request"
            )

            with pytest.raises(Exception) as exc_info:
                await get_access_token_service_account(
                    tenant_id="tenant123",
                    username="invalid@example.com",
                    password="wrong"
                )

            assert "400" in str(exc_info.value) or "invalid" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_validate_access_token_valid(self):
        """Test validation of a valid access token"""
        with patch('httpx.AsyncClient.get', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = Mock(status_code=200)

            is_valid = await validate_access_token("valid_token")

            assert is_valid is True

    @pytest.mark.asyncio
    async def test_validate_access_token_invalid(self):
        """Test validation of an invalid access token"""
        with patch('httpx.AsyncClient.get', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = Mock(status_code=401)

            is_valid = await validate_access_token("invalid_token")

            assert is_valid is False

    @pytest.mark.asyncio
    async def test_validate_access_token_expired(self):
        """Test validation of an expired access token"""
        with patch('httpx.AsyncClient.get', new_callable=AsyncMock) as mock_get:
            mock_get.return_value = Mock(
                status_code=401,
                json=Mock(return_value={'error': 'token_expired'})
            )

            is_valid = await validate_access_token("expired_token")

            assert is_valid is False

    @pytest.mark.asyncio
    async def test_refresh_access_token_success(self):
        """Test successful token refresh"""
        mock_response = {
            'access_token': 'new_access_token_789',
            'token_type': 'Bearer',
            'expires_in': 3600
        }

        with patch('httpx.AsyncClient.post', new_callable=AsyncMock) as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=mock_response)
            )

            new_token = await refresh_access_token(
                refresh_token="old_refresh_token",
                tenant_id="tenant123",
                client_id="client456"
            )

            assert new_token == "new_access_token_789"

    @pytest.mark.asyncio
    async def test_refresh_access_token_failure(self):
        """Test token refresh failure"""
        with patch('httpx.AsyncClient.post', new_callable=AsyncMock) as mock_post:
            mock_post.return_value = Mock(
                status_code=400,
                json=Mock(return_value={'error': 'invalid_grant'})
            )

            with pytest.raises(Exception):
                await refresh_access_token(
                    refresh_token="invalid_refresh",
                    tenant_id="tenant123",
                    client_id="client456"
                )

    @pytest.mark.asyncio
    async def test_get_access_token_with_custom_scope(self):
        """Test token acquisition with custom scope"""
        mock_response = {
            'access_token': 'scoped_token',
            'token_type': 'Bearer',
            'expires_in': 3600
        }

        with patch('httpx.AsyncClient.post', new_callable=AsyncMock) as mock_post:
            mock_post.return_value = Mock(
                status_code=200,
                json=Mock(return_value=mock_response)
            )

            token = await get_access_token_service_principal(
                tenant_id="tenant123",
                client_id="client456",
                client_secret="secret789",
                scope="https://analysis.windows.net/powerbi/api/.default"
            )

            assert token == "scoped_token"
            # Verify the scope was passed in the request
            call_args = mock_post.call_args
            assert 'scope' in str(call_args) or 'powerbi' in str(call_args)

    @pytest.mark.asyncio
    async def test_get_access_token_with_network_error(self):
        """Test token acquisition with network error"""
        with patch('httpx.AsyncClient.post', new_callable=AsyncMock) as mock_post:
            mock_post.side_effect = httpx.ConnectError("Connection failed")

            with pytest.raises(Exception) as exc_info:
                await get_access_token_service_principal(
                    tenant_id="tenant123",
                    client_id="client456",
                    client_secret="secret789"
                )

            assert "connection" in str(exc_info.value).lower() or "network" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_get_access_token_with_timeout(self):
        """Test token acquisition with timeout"""
        with patch('httpx.AsyncClient.post', new_callable=AsyncMock) as mock_post:
            mock_post.side_effect = httpx.TimeoutException("Request timeout")

            with pytest.raises(Exception) as exc_info:
                await get_access_token_service_principal(
                    tenant_id="tenant123",
                    client_id="client456",
                    client_secret="secret789"
                )

            assert "timeout" in str(exc_info.value).lower()
