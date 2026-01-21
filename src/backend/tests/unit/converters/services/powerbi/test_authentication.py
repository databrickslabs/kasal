"""
Unit tests for converters/services/powerbi/authentication.py

Tests Azure AD authentication service for Power BI API including Service Principal
authentication, token validation, and credential management.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.converters.services.powerbi.authentication import AadService


class TestAadService:
    """Tests for AadService class"""

    @pytest.fixture
    def mock_logger(self):
        """Create mock logger"""
        return Mock()

    @pytest.fixture
    def service_with_token(self, mock_logger):
        """Create AadService with pre-obtained token"""
        return AadService(
            access_token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U",
            logger=mock_logger
        )

    @pytest.fixture
    def service_with_credentials(self, mock_logger):
        """Create AadService with direct credentials"""
        return AadService(
            client_id="test_client_id",
            client_secret="test_client_secret",
            tenant_id="test_tenant_id",
            logger=mock_logger
        )

    @pytest.fixture
    def service_empty(self, mock_logger):
        """Create AadService without credentials"""
        return AadService(logger=mock_logger)

    # ========== Initialization Tests ==========

    def test_initialization_with_token(self, service_with_token):
        """Test AadService initializes with access token"""
        assert service_with_token._access_token is not None
        assert service_with_token.client_id is None
        assert service_with_token.client_secret is None
        assert service_with_token.tenant_id is None

    def test_initialization_with_credentials(self, service_with_credentials):
        """Test AadService initializes with credentials"""
        assert service_with_credentials.client_id == "test_client_id"
        assert service_with_credentials.client_secret == "test_client_secret"
        assert service_with_credentials.tenant_id == "test_tenant_id"
        assert service_with_credentials._access_token is None

    def test_initialization_with_all_parameters(self, mock_logger):
        """Test AadService initializes with all parameters"""
        service = AadService(
            client_id="client",
            client_secret="secret",
            tenant_id="tenant",
            access_token="token",
            project_id="proj123",
            use_database=True,
            logger=mock_logger
        )

        assert service.client_id == "client"
        assert service.client_secret == "secret"
        assert service.tenant_id == "tenant"
        assert service._access_token == "token"
        assert service.project_id == "proj123"
        assert service.use_database is True
        assert service.logger == mock_logger

    def test_initialization_creates_default_logger(self):
        """Test AadService creates default logger if not provided"""
        service = AadService()
        assert service.logger is not None

    def test_authority_base_constant(self):
        """Test AUTHORITY_BASE constant is set"""
        assert AadService.AUTHORITY_BASE == "https://login.microsoftonline.com"

    def test_powerbi_scope_constant(self):
        """Test POWERBI_SCOPE constant is set"""
        assert AadService.POWERBI_SCOPE == "https://analysis.windows.net/powerbi/api/.default"

    # ========== Get Access Token Tests ==========

    def test_get_access_token_with_preobained_token(self, service_with_token):
        """Test getting access token when pre-obtained token exists"""
        token = service_with_token.get_access_token()

        assert token == service_with_token._access_token
        assert "eyJ" in token  # JWT format

    @patch('src.converters.services.powerbi.authentication.MSAL_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.msal')
    def test_get_access_token_with_credentials(self, mock_msal, service_with_credentials):
        """Test getting access token with direct credentials"""
        # Mock MSAL app
        mock_app = Mock()
        mock_app.acquire_token_for_client.return_value = {
            "access_token": "new_token_12345"
        }
        mock_msal.ConfidentialClientApplication.return_value = mock_app

        token = service_with_credentials.get_access_token()

        assert token == "new_token_12345"
        mock_msal.ConfidentialClientApplication.assert_called_once()
        mock_app.acquire_token_for_client.assert_called_once_with(
            scopes=[AadService.POWERBI_SCOPE]
        )

    @patch('src.converters.services.powerbi.authentication.MSAL_AVAILABLE', False)
    def test_get_access_token_msal_not_available(self, service_with_credentials):
        """Test error when MSAL library not available"""
        with pytest.raises(RuntimeError, match="MSAL library required"):
            service_with_credentials.get_access_token()

    def test_get_access_token_no_credentials(self, service_empty):
        """Test error when no credentials provided"""
        with pytest.raises(ValueError, match="No credentials available"):
            service_empty.get_access_token()

    @patch('src.converters.services.powerbi.authentication.MSAL_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.msal')
    def test_get_access_token_msal_error(self, mock_msal, service_with_credentials):
        """Test handling MSAL token acquisition error"""
        mock_app = Mock()
        mock_app.acquire_token_for_client.return_value = {
            "error": "invalid_client",
            "error_description": "Invalid client credentials"
        }
        mock_msal.ConfidentialClientApplication.return_value = mock_app

        with pytest.raises(Exception, match="Failed to acquire access token"):
            service_with_credentials.get_access_token()

    @patch('src.converters.services.powerbi.authentication.MSAL_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.msal')
    def test_get_access_token_msal_exception(self, mock_msal, service_with_credentials):
        """Test handling MSAL exception"""
        mock_msal.ConfidentialClientApplication.side_effect = Exception("MSAL error")

        with pytest.raises(Exception, match="Error retrieving access token"):
            service_with_credentials.get_access_token()

    def test_get_access_token_database_not_implemented(self, mock_logger):
        """Test database credential lookup raises NotImplementedError"""
        service = AadService(use_database=True, logger=mock_logger)

        with pytest.raises(NotImplementedError, match="Database credential storage not yet implemented"):
            service.get_access_token()

    # ========== Acquire Token with MSAL Tests ==========

    @patch('src.converters.services.powerbi.authentication.MSAL_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.msal')
    def test_acquire_token_builds_correct_authority(self, mock_msal, service_with_credentials):
        """Test _acquire_token_with_msal builds correct authority URL"""
        mock_app = Mock()
        mock_app.acquire_token_for_client.return_value = {"access_token": "token"}
        mock_msal.ConfidentialClientApplication.return_value = mock_app

        credentials = {
            "client_id": "client123",
            "client_secret": "secret456",
            "tenant_id": "tenant789"
        }

        service_with_credentials._acquire_token_with_msal(credentials)

        # Verify authority URL construction
        call_args = mock_msal.ConfidentialClientApplication.call_args
        assert call_args[1]['authority'] == f"{AadService.AUTHORITY_BASE}/tenant789"

    def test_acquire_token_incomplete_credentials(self, service_with_credentials):
        """Test _acquire_token_with_msal with incomplete credentials"""
        incomplete_credentials = {
            "client_id": "client123",
            # Missing client_secret and tenant_id
        }

        with pytest.raises(ValueError, match="Incomplete credentials"):
            service_with_credentials._acquire_token_with_msal(incomplete_credentials)

    # ========== Validate Token Tests ==========

    def test_validate_token_valid_jwt(self, service_with_token):
        """Test validating valid JWT token"""
        is_valid = service_with_token.validate_token()
        assert is_valid is True

    def test_validate_token_with_parameter(self, service_empty):
        """Test validating token passed as parameter"""
        token = "eyJ.test.token"
        is_valid = service_empty.validate_token(token)
        assert is_valid is True

    def test_validate_token_no_token(self, service_empty):
        """Test validating when no token exists"""
        is_valid = service_empty.validate_token()
        assert is_valid is False

    def test_validate_token_invalid_format(self, service_empty):
        """Test validating token with invalid format"""
        invalid_token = "not_a_jwt_token"
        is_valid = service_empty.validate_token(invalid_token)
        assert is_valid is False

    def test_validate_token_two_parts_only(self, service_empty):
        """Test validating token with only two parts"""
        invalid_token = "part1.part2"
        is_valid = service_empty.validate_token(invalid_token)
        assert is_valid is False

    def test_validate_token_empty_string(self, service_empty):
        """Test validating empty string token"""
        is_valid = service_empty.validate_token("")
        assert is_valid is False

    # ========== Database Credentials Tests ==========

    def test_get_credentials_from_database_not_implemented(self, service_empty):
        """Test _get_credentials_from_database raises NotImplementedError"""
        with pytest.raises(NotImplementedError, match="Database credential storage not yet implemented"):
            service_empty._get_credentials_from_database()

    # ========== Integration Tests ==========

    @patch('src.converters.services.powerbi.authentication.MSAL_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.msal')
    def test_full_authentication_flow(self, mock_msal, mock_logger):
        """Test complete authentication flow from initialization to token"""
        # Mock MSAL
        mock_app = Mock()
        mock_app.acquire_token_for_client.return_value = {
            "access_token": "eyJ.integration.token"  # Valid JWT format (3 parts)
        }
        mock_msal.ConfidentialClientApplication.return_value = mock_app

        # Create service and get token
        service = AadService(
            client_id="test_client",
            client_secret="test_secret",
            tenant_id="test_tenant",
            logger=mock_logger
        )

        token = service.get_access_token()

        # Verify token
        assert token == "eyJ.integration.token"
        assert service.validate_token(token) is True

    def test_priority_preobained_token_over_credentials(self, mock_logger):
        """Test pre-obtained token has priority over credentials"""
        service = AadService(
            access_token="preobained_token",
            client_id="client",
            client_secret="secret",
            tenant_id="tenant",
            logger=mock_logger
        )

        # Should return pre-obtained token without calling MSAL
        token = service.get_access_token()
        assert token == "preobained_token"

    # ========== Edge Cases ==========

    @patch('src.converters.services.powerbi.authentication.MSAL_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.msal')
    def test_get_access_token_with_partial_credentials(self, mock_msal, mock_logger):
        """Test getting token with only some credentials fails properly"""
        service = AadService(
            client_id="client",
            client_secret="secret",
            # Missing tenant_id
            logger=mock_logger
        )

        with pytest.raises(ValueError, match="No credentials available"):
            service.get_access_token()

    def test_validate_token_four_parts(self, service_empty):
        """Test validating token with four parts (invalid JWT)"""
        invalid_token = "part1.part2.part3.part4"
        is_valid = service_empty.validate_token(invalid_token)
        assert is_valid is False

    def test_validate_token_none(self, service_empty):
        """Test validating None token"""
        is_valid = service_empty.validate_token(None)
        assert is_valid is False

    @patch('src.converters.services.powerbi.authentication.MSAL_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.msal')
    def test_acquire_token_logs_tenant_id(self, mock_msal, service_with_credentials, mock_logger):
        """Test _acquire_token_with_msal logs tenant ID"""
        mock_app = Mock()
        mock_app.acquire_token_for_client.return_value = {"access_token": "token"}
        mock_msal.ConfidentialClientApplication.return_value = mock_app

        credentials = {
            "client_id": "client",
            "client_secret": "secret",
            "tenant_id": "tenant123"
        }

        service_with_credentials._acquire_token_with_msal(credentials)

        # Verify logging was called with tenant info
        assert any("tenant123" in str(call) for call in mock_logger.info.call_args_list)

    def test_constants_are_strings(self):
        """Test that constants are properly defined as strings"""
        assert isinstance(AadService.AUTHORITY_BASE, str)
        assert isinstance(AadService.POWERBI_SCOPE, str)
        assert "https://" in AadService.AUTHORITY_BASE
        assert "powerbi" in AadService.POWERBI_SCOPE.lower()
