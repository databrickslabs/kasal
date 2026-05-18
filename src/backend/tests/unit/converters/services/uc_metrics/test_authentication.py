"""
Unit tests for converters/services/uc_metrics/authentication.py

Comprehensive tests for DatabricksAuthService class.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.converters.services.uc_metrics.authentication import DatabricksAuthService


class TestDatabricksAuthServiceInit:
    """Tests for DatabricksAuthService initialization."""

    def test_init_with_api_key(self):
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            api_key="dapi_abc123",
        )
        assert svc.workspace_url == "https://example.databricks.com"
        assert svc.api_key == "dapi_abc123"

    def test_init_strips_trailing_slash(self):
        svc = DatabricksAuthService(workspace_url="https://example.databricks.com/")
        assert svc.workspace_url == "https://example.databricks.com"

    def test_init_with_access_token(self):
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            access_token="eyJoaWdoLnRva2Vu",
        )
        assert svc._access_token == "eyJoaWdoLnRva2Vu"

    def test_init_with_service_principal(self):
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            client_id="client_123",
            client_secret="secret_abc",
        )
        assert svc.client_id == "client_123"
        assert svc.client_secret == "secret_abc"

    def test_init_uses_provided_logger(self):
        import logging
        custom_logger = logging.getLogger("custom")
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            logger=custom_logger,
        )
        assert svc.logger is custom_logger

    def test_init_creates_default_logger(self):
        svc = DatabricksAuthService(workspace_url="https://example.databricks.com")
        assert svc.logger is not None


class TestGetAccessToken:
    """Tests for DatabricksAuthService.get_access_token."""

    def test_returns_access_token_first(self):
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            access_token="my_oauth_token",
            api_key="dapi_should_not_be_used",
        )
        assert svc.get_access_token() == "my_oauth_token"

    def test_returns_api_key_when_no_access_token(self):
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            api_key="dapi_key_123",
        )
        assert svc.get_access_token() == "dapi_key_123"

    def test_uses_service_principal_when_no_other_creds(self):
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            client_id="cid",
            client_secret="csecret",
        )
        with patch.object(svc, "_acquire_token_with_service_principal", return_value="sp_token") as mock_sp:
            result = svc.get_access_token()
        assert result == "sp_token"
        mock_sp.assert_called_once()

    def test_raises_when_no_credentials(self):
        svc = DatabricksAuthService(workspace_url="https://example.databricks.com")
        with pytest.raises(ValueError, match="No credentials available"):
            svc.get_access_token()

    def test_raises_when_use_database_not_implemented(self):
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            use_database=True,
        )
        with pytest.raises(NotImplementedError):
            svc.get_access_token()


class TestAcquireTokenWithServicePrincipal:
    """Tests for DatabricksAuthService._acquire_token_with_service_principal."""

    def test_raises_when_credentials_incomplete(self):
        svc = DatabricksAuthService(workspace_url="https://example.databricks.com")
        with pytest.raises(ValueError, match="Incomplete Service Principal"):
            svc._acquire_token_with_service_principal(client_id=None, client_secret=None)

    def test_returns_token_on_success(self):
        import requests
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            client_id="cid",
            client_secret="csecret",
        )
        mock_response = MagicMock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {"access_token": "sp_access_token"}

        with patch("requests.post", return_value=mock_response):
            result = svc._acquire_token_with_service_principal()
        assert result == "sp_access_token"

    def test_raises_on_missing_access_token_in_response(self):
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            client_id="cid",
            client_secret="csecret",
        )
        mock_response = MagicMock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {"error": "bad_credentials"}

        with patch("requests.post", return_value=mock_response):
            with pytest.raises(Exception, match="Failed to acquire access token"):
                svc._acquire_token_with_service_principal()

    def test_raises_on_request_exception(self):
        import requests
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            client_id="cid",
            client_secret="csecret",
        )
        with patch("requests.post", side_effect=requests.exceptions.ConnectionError("conn refused")):
            with pytest.raises(Exception, match="Error retrieving access token"):
                svc._acquire_token_with_service_principal()


class TestValidateToken:
    """Tests for DatabricksAuthService.validate_token."""

    def test_returns_false_when_no_token(self):
        svc = DatabricksAuthService(workspace_url="https://example.databricks.com")
        assert svc.validate_token() is False

    def test_validates_pat_token(self):
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            api_key="dapi_my_long_token_abc",
        )
        assert svc.validate_token() is True

    def test_rejects_short_pat(self):
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            api_key="dapi",
        )
        assert svc.validate_token() is False

    def test_validates_jwt_format(self):
        svc = DatabricksAuthService(workspace_url="https://example.databricks.com")
        token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.abc123"
        assert svc.validate_token(token) is True

    def test_rejects_invalid_token(self):
        svc = DatabricksAuthService(workspace_url="https://example.databricks.com")
        assert svc.validate_token("not_valid") is False

    def test_uses_provided_token_override(self):
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            api_key="dapi_default_key_xxxxx",
        )
        # Explicitly providing a bad token should return False
        assert svc.validate_token("bad") is False


class TestGetHeaders:
    """Tests for DatabricksAuthService.get_headers."""

    def test_returns_bearer_headers(self):
        svc = DatabricksAuthService(
            workspace_url="https://example.databricks.com",
            api_key="dapi_key_xyz",
        )
        headers = svc.get_headers()
        assert headers["Authorization"] == "Bearer dapi_key_xyz"
        assert headers["Content-Type"] == "application/json"

    def test_raises_when_no_credentials(self):
        svc = DatabricksAuthService(workspace_url="https://example.databricks.com")
        with pytest.raises(ValueError):
            svc.get_headers()

    def test_get_access_token_invalid_input(self):
        """Test get_access_token handles invalid input"""
        # TODO: Implement test
        pass


class TestValidateToken:
    """Tests for validate_token function"""

    def test_validate_token_success(self):
        """Test validate_token succeeds with valid input"""
        # TODO: Implement test
        pass

    def test_validate_token_invalid_input(self):
        """Test validate_token handles invalid input"""
        # TODO: Implement test
        pass


class TestGetHeaders:
    """Tests for get_headers function"""

    def test_get_headers_success(self):
        """Test get_headers succeeds with valid input"""
        # TODO: Implement test
        pass

    def test_get_headers_invalid_input(self):
        """Test get_headers handles invalid input"""
        # TODO: Implement test
        pass



# TODO: Add more comprehensive tests
# TODO: Test edge cases and error handling
# TODO: Achieve 80%+ code coverage
