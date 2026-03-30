"""
Unit tests for converters/services/powerbi/authentication.py

Tests Azure AD authentication service for Power BI API including Service Principal
authentication, Service Account authentication, token validation, and credential management.
"""

import os
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
            access_token="dummy_access_token_for_testing",  # gitleaks:allow
            logger=mock_logger
        )

    @pytest.fixture
    def service_with_credentials(self, mock_logger):
        """Create AadService with direct credentials (service principal)"""
        return AadService(
            client_id="test_client_id",
            client_secret="test_client_secret",
            tenant_id="test_tenant_id",
            logger=mock_logger
        )

    @pytest.fixture
    def service_with_service_account(self, mock_logger):
        """Create AadService with service account credentials"""
        return AadService(
            client_id="test_client_id",
            tenant_id="test_tenant_id",
            username="test_user@domain.com",
            password="test_password",
            auth_method="service_account",
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

    def test_initialization_with_service_account(self, service_with_service_account):
        """Test AadService initializes with service account credentials"""
        assert service_with_service_account.client_id == "test_client_id"
        assert service_with_service_account.tenant_id == "test_tenant_id"
        assert service_with_service_account.username == "test_user@domain.com"
        assert service_with_service_account.password == "test_password"
        assert service_with_service_account.auth_method == "service_account"

    def test_initialization_with_all_parameters(self, mock_logger):
        """Test AadService initializes with all parameters"""
        service = AadService(
            client_id="client",
            client_secret="secret",
            tenant_id="tenant",
            access_token="token",
            username="user",
            password="pass",
            username_env="USER_ENV",
            password_env="PASS_ENV",
            auth_method="service_account",
            project_id="proj123",
            use_database=True,
            logger=mock_logger
        )

        assert service.client_id == "client"
        assert service.client_secret == "secret"
        assert service.tenant_id == "tenant"
        assert service._access_token == "token"
        assert service.username == "user"
        assert service.password == "pass"
        assert service.username_env == "USER_ENV"
        assert service.password_env == "PASS_ENV"
        assert service.auth_method == "service_account"
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

    def test_auth_method_constants(self):
        """Test authentication method constants"""
        assert AadService.AUTH_METHOD_SERVICE_PRINCIPAL == "service_principal"
        assert AadService.AUTH_METHOD_SERVICE_ACCOUNT == "service_account"
        assert AadService.AUTH_METHOD_TOKEN == "token"

    # ========== Get Access Token Tests ==========

    def test_get_access_token_with_preobained_token(self, service_with_token):
        """Test getting access token when pre-obtained token exists"""
        token = service_with_token.get_access_token()

        assert token == service_with_token._access_token
        assert "eyJ" in token  # JWT format

    @patch('src.converters.services.powerbi.authentication.AZURE_IDENTITY_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.ClientSecretCredential')
    def test_get_access_token_with_credentials(self, mock_credential_class, service_with_credentials):
        """Test getting access token with direct credentials"""
        # Mock ClientSecretCredential
        mock_credential = Mock()
        mock_token = Mock()
        mock_token.token = "new_token_12345"
        mock_credential.get_token.return_value = mock_token
        mock_credential_class.return_value = mock_credential

        token = service_with_credentials.get_access_token()

        assert token == "new_token_12345"
        mock_credential_class.assert_called_once_with(
            tenant_id="test_tenant_id",
            client_id="test_client_id",
            client_secret="test_client_secret"
        )
        mock_credential.get_token.assert_called_once_with(AadService.POWERBI_SCOPE)

    @patch('src.converters.services.powerbi.authentication.AZURE_IDENTITY_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.UsernamePasswordCredential')
    def test_get_access_token_with_service_account(self, mock_credential_class, service_with_service_account):
        """Test getting access token with service account credentials"""
        # Mock UsernamePasswordCredential
        mock_credential = Mock()
        mock_token = Mock()
        mock_token.token = "service_account_token_12345"
        mock_credential.get_token.return_value = mock_token
        mock_credential_class.return_value = mock_credential

        token = service_with_service_account.get_access_token()

        assert token == "service_account_token_12345"
        mock_credential_class.assert_called_once_with(
            client_id="test_client_id",
            username="test_user@domain.com",
            password="test_password",
            tenant_id="test_tenant_id",
            client_secret=None
        )
        mock_credential.get_token.assert_called_once_with(AadService.POWERBI_SCOPE)

    @patch('src.converters.services.powerbi.authentication.AZURE_IDENTITY_AVAILABLE', False)
    def test_get_access_token_azure_identity_not_available(self, service_with_credentials):
        """Test error when azure-identity library not available"""
        with pytest.raises(RuntimeError, match="azure-identity library required"):
            service_with_credentials.get_access_token()

    def test_get_access_token_no_credentials(self, service_empty):
        """Test error when no credentials provided"""
        with pytest.raises(ValueError, match="No credentials available"):
            service_empty.get_access_token()

    @patch('src.converters.services.powerbi.authentication.AZURE_IDENTITY_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.ClientSecretCredential')
    def test_get_access_token_credential_exception(self, mock_credential_class, service_with_credentials):
        """Test handling credential exception"""
        mock_credential_class.side_effect = Exception("Azure identity error")

        with pytest.raises(Exception, match="Error retrieving access token"):
            service_with_credentials.get_access_token()

    def test_get_access_token_database_not_implemented(self, mock_logger):
        """Test database credential lookup raises NotImplementedError"""
        service = AadService(use_database=True, logger=mock_logger)

        with pytest.raises(NotImplementedError, match="Database credential storage not yet implemented"):
            service.get_access_token()

    # ========== Auth Method Detection Tests ==========

    def test_determine_auth_method_explicit_service_principal(self, mock_logger):
        """Test explicit service principal auth method"""
        service = AadService(
            client_id="client",
            tenant_id="tenant",
            username="user",
            password="pass",
            auth_method="service_principal",  # Explicit override
            logger=mock_logger
        )
        assert service._determine_auth_method() == "service_principal"

    def test_determine_auth_method_explicit_service_account(self, mock_logger):
        """Test explicit service account auth method"""
        service = AadService(
            client_id="client",
            client_secret="secret",
            tenant_id="tenant",
            auth_method="service_account",  # Explicit override
            logger=mock_logger
        )
        assert service._determine_auth_method() == "service_account"

    def test_determine_auth_method_auto_service_principal(self, service_with_credentials):
        """Test auto-detection of service principal"""
        assert service_with_credentials._determine_auth_method() == "service_principal"

    def test_determine_auth_method_auto_service_account(self, service_with_service_account):
        """Test auto-detection of service account"""
        # Override auth_method to None to test auto-detection
        service_with_service_account.auth_method = None
        service_with_service_account.client_secret = None  # Remove secret to prefer service account
        assert service_with_service_account._determine_auth_method() == "service_account"

    def test_determine_auth_method_no_credentials(self, service_empty):
        """Test no auth method when no credentials"""
        assert service_empty._determine_auth_method() is None

    # ========== Username/Password Resolution Tests ==========

    def test_resolve_username_direct(self, mock_logger):
        """Test resolving username from direct value"""
        service = AadService(username="direct_user@domain.com", logger=mock_logger)
        assert service._resolve_username() == "direct_user@domain.com"

    @patch.dict(os.environ, {"TEST_USERNAME_ENV": "env_user@domain.com"})
    def test_resolve_username_from_env(self, mock_logger):
        """Test resolving username from environment variable"""
        service = AadService(username_env="TEST_USERNAME_ENV", logger=mock_logger)
        assert service._resolve_username() == "env_user@domain.com"

    def test_resolve_username_none(self, service_empty):
        """Test resolving username when not set"""
        assert service_empty._resolve_username() is None

    def test_resolve_password_direct(self, mock_logger):
        """Test resolving password from direct value"""
        service = AadService(password="direct_password", logger=mock_logger)
        assert service._resolve_password() == "direct_password"

    @patch.dict(os.environ, {"TEST_PASSWORD_ENV": "env_password"})
    def test_resolve_password_from_env(self, mock_logger):
        """Test resolving password from environment variable"""
        service = AadService(password_env="TEST_PASSWORD_ENV", logger=mock_logger)
        assert service._resolve_password() == "env_password"

    def test_resolve_password_none(self, service_empty):
        """Test resolving password when not set"""
        assert service_empty._resolve_password() is None

    # ========== Service Principal Credentials Tests ==========

    def test_get_service_principal_credentials_complete(self, service_with_credentials):
        """Test getting complete service principal credentials"""
        credentials = service_with_credentials._get_service_principal_credentials()
        assert credentials["client_id"] == "test_client_id"
        assert credentials["client_secret"] == "test_client_secret"
        assert credentials["tenant_id"] == "test_tenant_id"

    def test_get_service_principal_credentials_incomplete(self, mock_logger):
        """Test error with incomplete service principal credentials"""
        service = AadService(client_id="client", logger=mock_logger)
        with pytest.raises(ValueError, match="Incomplete Service Principal credentials"):
            service._get_service_principal_credentials()

    # ========== Service Account Token Acquisition Tests ==========

    @patch('src.converters.services.powerbi.authentication.AZURE_IDENTITY_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.UsernamePasswordCredential')
    def test_acquire_token_with_username_password_complete(self, mock_credential_class, service_with_service_account):
        """Test successful service account token acquisition"""
        mock_credential = Mock()
        mock_token = Mock()
        mock_token.token = "test_token"
        mock_credential.get_token.return_value = mock_token
        mock_credential_class.return_value = mock_credential

        token = service_with_service_account._acquire_token_with_username_password()
        assert token == "test_token"

    def test_acquire_token_with_username_password_incomplete(self, mock_logger):
        """Test error with incomplete service account credentials"""
        service = AadService(
            client_id="client",
            tenant_id="tenant",
            # Missing username and password
            auth_method="service_account",
            logger=mock_logger
        )
        with pytest.raises(ValueError, match="Incomplete Service Account credentials"):
            service._acquire_token_with_username_password()

    @patch('src.converters.services.powerbi.authentication.AZURE_IDENTITY_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.UsernamePasswordCredential')
    def test_acquire_token_with_username_password_exception(self, mock_credential_class, service_with_service_account):
        """Test handling exception during service account token acquisition"""
        mock_credential_class.side_effect = Exception("Authentication failed")

        with pytest.raises(Exception, match="Error retrieving access token with service account"):
            service_with_service_account._acquire_token_with_username_password()

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

    def test_validate_token_four_parts(self, service_empty):
        """Test validating token with four parts (invalid JWT)"""
        invalid_token = "part1.part2.part3.part4"
        is_valid = service_empty.validate_token(invalid_token)
        assert is_valid is False

    def test_validate_token_none(self, service_empty):
        """Test validating None token"""
        is_valid = service_empty.validate_token(None)
        assert is_valid is False

    # ========== Database Credentials Tests ==========

    def test_get_credentials_from_database_not_implemented(self, service_empty):
        """Test _get_credentials_from_database raises NotImplementedError"""
        with pytest.raises(NotImplementedError, match="Database credential storage not yet implemented"):
            service_empty._get_credentials_from_database()

    # ========== Integration Tests ==========

    @patch('src.converters.services.powerbi.authentication.AZURE_IDENTITY_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.ClientSecretCredential')
    def test_full_authentication_flow_service_principal(self, mock_credential_class, mock_logger):
        """Test complete authentication flow with service principal"""
        mock_credential = Mock()
        mock_token = Mock()
        mock_token.token = "eyJ.integration.token"
        mock_credential.get_token.return_value = mock_token
        mock_credential_class.return_value = mock_credential

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

    @patch('src.converters.services.powerbi.authentication.AZURE_IDENTITY_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.UsernamePasswordCredential')
    def test_full_authentication_flow_service_account(self, mock_credential_class, mock_logger):
        """Test complete authentication flow with service account"""
        mock_credential = Mock()
        mock_token = Mock()
        mock_token.token = "eyJ.service_account.token"
        mock_credential.get_token.return_value = mock_token
        mock_credential_class.return_value = mock_credential

        # Create service and get token
        service = AadService(
            client_id="test_client",
            tenant_id="test_tenant",
            username="user@domain.com",
            password="password123",
            auth_method="service_account",
            logger=mock_logger
        )

        token = service.get_access_token()

        # Verify token
        assert token == "eyJ.service_account.token"
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

        # Should return pre-obtained token without calling azure-identity
        token = service.get_access_token()
        assert token == "preobained_token"

    # ========== Edge Cases ==========

    @patch('src.converters.services.powerbi.authentication.AZURE_IDENTITY_AVAILABLE', True)
    def test_get_access_token_with_partial_credentials(self, mock_logger):
        """Test getting token with only some credentials fails properly"""
        service = AadService(
            client_id="client",
            client_secret="secret",
            # Missing tenant_id
            logger=mock_logger
        )

        with pytest.raises(ValueError, match="No credentials available"):
            service.get_access_token()

    @patch('src.converters.services.powerbi.authentication.AZURE_IDENTITY_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.ClientSecretCredential')
    def test_acquire_token_logs_tenant_id(self, mock_credential_class, service_with_credentials, mock_logger):
        """Test _acquire_token_with_client_credential logs tenant ID"""
        mock_credential = Mock()
        mock_token = Mock()
        mock_token.token = "token"
        mock_credential.get_token.return_value = mock_token
        mock_credential_class.return_value = mock_credential

        credentials = {
            "client_id": "client",
            "client_secret": "secret",
            "tenant_id": "tenant123"
        }

        service_with_credentials._acquire_token_with_client_credential(credentials)

        # Verify logging was called with tenant info
        assert any("tenant123" in str(call) for call in mock_logger.info.call_args_list)

    def test_constants_are_strings(self):
        """Test that constants are properly defined as strings"""
        assert isinstance(AadService.AUTHORITY_BASE, str)
        assert isinstance(AadService.POWERBI_SCOPE, str)
        assert "https://" in AadService.AUTHORITY_BASE
        assert "powerbi" in AadService.POWERBI_SCOPE.lower()

    @patch.dict(os.environ, {
        "CUSTOM_USERNAME": "env_user@domain.com",
        "CUSTOM_PASSWORD": "env_password"
    })
    @patch('src.converters.services.powerbi.authentication.AZURE_IDENTITY_AVAILABLE', True)
    @patch('src.converters.services.powerbi.authentication.UsernamePasswordCredential')
    def test_service_account_with_env_vars(self, mock_credential_class, mock_logger):
        """Test service account authentication using environment variables"""
        mock_credential = Mock()
        mock_token = Mock()
        mock_token.token = "env_token"
        mock_credential.get_token.return_value = mock_token
        mock_credential_class.return_value = mock_credential

        service = AadService(
            client_id="test_client",
            tenant_id="test_tenant",
            username_env="CUSTOM_USERNAME",
            password_env="CUSTOM_PASSWORD",
            auth_method="service_account",
            logger=mock_logger
        )

        token = service.get_access_token()

        assert token == "env_token"
        mock_credential_class.assert_called_once_with(
            client_id="test_client",
            username="env_user@domain.com",
            password="env_password",
            tenant_id="test_tenant",
            client_secret=None
        )
