"""
Extended unit tests for converters/services/uc_metrics/authentication.py

Targets uncovered lines: 123-126, 246-263
"""

import pytest
from unittest.mock import patch, Mock, MagicMock
from src.converters.services.uc_metrics.authentication import DatabricksAuthService


class TestDatabricksAuthServiceBasics:
    """Initialization and basic property tests"""

    def test_init_stores_workspace_url(self):
        service = DatabricksAuthService(workspace_url="https://dbc-123.cloud.databricks.com")
        assert service.workspace_url == "https://dbc-123.cloud.databricks.com"

    def test_init_strips_trailing_slash(self):
        service = DatabricksAuthService(workspace_url="https://dbc-123.cloud.databricks.com/")
        assert not service.workspace_url.endswith("/")

    def test_init_with_api_key(self):
        service = DatabricksAuthService(
            workspace_url="https://example.com",
            api_key="dapiABCDEF"
        )
        assert service.api_key == "dapiABCDEF"

    def test_init_with_access_token(self):
        service = DatabricksAuthService(
            workspace_url="https://example.com",
            access_token="eyJ.test.token"
        )
        assert service._access_token == "eyJ.test.token"

    def test_init_with_service_principal(self):
        service = DatabricksAuthService(
            workspace_url="https://example.com",
            client_id="client123",
            client_secret="secret456"
        )
        assert service.client_id == "client123"
        assert service.client_secret == "secret456"

    def test_init_creates_default_logger(self):
        service = DatabricksAuthService(workspace_url="https://example.com")
        assert service.logger is not None

    def test_init_with_custom_logger(self):
        import logging
        mock_logger = Mock()
        service = DatabricksAuthService(
            workspace_url="https://example.com",
            logger=mock_logger
        )
        assert service.logger == mock_logger


class TestGetAccessToken:
    """Tests for get_access_token priority chain"""

    def test_priority_access_token_first(self):
        """Priority 1: pre-obtained OAuth token"""
        service = DatabricksAuthService(
            workspace_url="https://example.com",
            access_token="oauth_token",
            api_key="dapiXYZ"  # should be ignored
        )
        token = service.get_access_token()
        assert token == "oauth_token"

    def test_priority_api_key_second(self):
        """Priority 2: PAT when no OAuth token"""
        service = DatabricksAuthService(
            workspace_url="https://example.com",
            api_key="dapiPAT123"
        )
        token = service.get_access_token()
        assert token == "dapiPAT123"

    @patch('requests.post')
    def test_priority_service_principal_third(self, mock_post):
        """Priority 3: service principal via OAuth"""
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "sp_token_xyz"}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        service = DatabricksAuthService(
            workspace_url="https://example.com",
            client_id="sp_client",
            client_secret="sp_secret"
        )
        token = service.get_access_token()
        assert token == "sp_token_xyz"

    def test_no_credentials_raises_value_error(self):
        """Priority 5: no credentials -> ValueError"""
        service = DatabricksAuthService(workspace_url="https://example.com")
        with pytest.raises(ValueError, match="No credentials available"):
            service.get_access_token()

    def test_database_raises_not_implemented(self):
        """Priority 4: use_database -> NotImplementedError"""
        service = DatabricksAuthService(
            workspace_url="https://example.com",
            use_database=True
        )
        with pytest.raises(NotImplementedError):
            service.get_access_token()


class TestAcquireTokenWithServicePrincipal:
    """Lines 140-201: _acquire_token_with_service_principal"""

    @patch('requests.post')
    def test_success(self, mock_post):
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "the_token"}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        service = DatabricksAuthService(
            workspace_url="https://example.com",
            client_id="client",
            client_secret="secret"
        )
        token = service._acquire_token_with_service_principal()
        assert token == "the_token"

    @patch('requests.post')
    def test_missing_access_token_in_response_raises(self, mock_post):
        mock_response = Mock()
        mock_response.json.return_value = {"error": "invalid_client"}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        service = DatabricksAuthService(
            workspace_url="https://example.com",
            client_id="client",
            client_secret="secret"
        )
        with pytest.raises(Exception, match="Failed to acquire access token"):
            service._acquire_token_with_service_principal()

    def test_incomplete_credentials_raises(self):
        service = DatabricksAuthService(
            workspace_url="https://example.com",
            client_id="only_client"
            # Missing client_secret
        )
        with pytest.raises(ValueError, match="Incomplete Service Principal credentials"):
            service._acquire_token_with_service_principal()

    @patch('requests.post')
    def test_request_exception_raises(self, mock_post):
        """Lines 199-201: requests.exceptions.RequestException"""
        import requests as rq
        mock_post.side_effect = rq.exceptions.RequestException("Connection error")

        service = DatabricksAuthService(
            workspace_url="https://example.com",
            client_id="client",
            client_secret="secret"
        )
        with pytest.raises(Exception, match="Error retrieving access token"):
            service._acquire_token_with_service_principal()

    @patch('requests.post')
    def test_custom_client_id_and_secret(self, mock_post):
        """Line 163-164: explicit client_id and client_secret override instance vars"""
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "custom_token"}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        service = DatabricksAuthService(
            workspace_url="https://example.com",
            client_id="default_client",
            client_secret="default_secret"
        )
        token = service._acquire_token_with_service_principal(
            client_id="override_client",
            client_secret="override_secret"
        )
        assert token == "custom_token"


class TestGetCredentialsFromDatabase:
    """Line 203-231: _get_credentials_from_database"""

    def test_raises_not_implemented(self):
        service = DatabricksAuthService(workspace_url="https://example.com")
        with pytest.raises(NotImplementedError, match="Database credential storage not yet implemented"):
            service._get_credentials_from_database()


class TestValidateToken:
    """Lines 233-263: validate_token"""

    def test_valid_dapi_token(self):
        """Lines 254-256: PAT starting with 'dapi' is valid if length > 10"""
        service = DatabricksAuthService(
            workspace_url="https://example.com",
            api_key="dapiABCDEFGHIJKLMNOP"
        )
        assert service.validate_token("dapiABCDEFGHIJKLMNOP") is True

    def test_dapi_token_too_short(self):
        """Length check for PAT tokens"""
        service = DatabricksAuthService(workspace_url="https://example.com")
        assert service.validate_token("dapiABC") is False

    def test_valid_jwt_token(self):
        """Lines 259-261: JWT token with 3 parts"""
        service = DatabricksAuthService(workspace_url="https://example.com")
        assert service.validate_token("header.payload.signature") is True

    def test_invalid_non_jwt_non_pat(self):
        """Lines 262-263: not a PAT and not a JWT"""
        service = DatabricksAuthService(workspace_url="https://example.com")
        assert service.validate_token("not_a_valid_token") is False

    def test_no_token_returns_false(self):
        """Line 248-249: no token returns False"""
        service = DatabricksAuthService(workspace_url="https://example.com")
        assert service.validate_token() is False

    def test_uses_internal_token_when_no_arg(self):
        """Line 246: uses _access_token or api_key when no arg"""
        service = DatabricksAuthService(
            workspace_url="https://example.com",
            api_key="dapiABCDEFGHIJKLMNOP"
        )
        # validate_token() with no arg uses self.api_key
        assert service.validate_token() is True

    def test_none_token_returns_false(self):
        service = DatabricksAuthService(workspace_url="https://example.com")
        assert service.validate_token(None) is False


class TestGetHeaders:
    """Lines 265-276: get_headers"""

    def test_get_headers_returns_authorization(self):
        service = DatabricksAuthService(
            workspace_url="https://example.com",
            api_key="dapiTEST12345678"
        )
        headers = service.get_headers()
        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer dapiTEST12345678"
        assert headers["Content-Type"] == "application/json"

    def test_get_headers_with_access_token(self):
        service = DatabricksAuthService(
            workspace_url="https://example.com",
            access_token="eyJ.oauth.token"
        )
        headers = service.get_headers()
        assert "Bearer eyJ.oauth.token" == headers["Authorization"]
