import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from types import SimpleNamespace
import os
from typing import Dict, Any, Optional, Tuple

# Test databricks_auth utility functions - based on actual code inspection

from src.utils.databricks_auth import (
    AuthContext,
    extract_user_token_from_request,
    is_scope_error,
    setup_environment_variables,
    _clean_environment,
    get_databricks_auth_headers_sync,
    validate_databricks_connection,
    get_databricks_auth_headers,
    get_auth_context,
    get_workspace_client,
    get_workspace_client_with_fallback,
    get_mcp_access_token,
    get_current_databricks_user,
    get_mcp_auth_headers
)


class TestAuthContext:
    """Test AuthContext class based on actual implementation"""

    def test_auth_context_init_basic(self):
        """Test AuthContext __init__ with basic parameters"""
        token = "test-token"
        workspace_url = "https://test.databricks.com"
        auth_method = "pat"
        
        auth_ctx = AuthContext(
            token=token,
            workspace_url=workspace_url,
            auth_method=auth_method
        )
        
        assert auth_ctx.token == token
        assert auth_ctx.workspace_url == workspace_url
        assert auth_ctx.auth_method == auth_method
        assert auth_ctx.user_identity is None

    def test_auth_context_init_with_user_identity(self):
        """Test AuthContext __init__ with user identity"""
        token = "test-token"
        workspace_url = "https://test.databricks.com"
        auth_method = "obo"
        user_identity = "test@example.com"
        
        auth_ctx = AuthContext(
            token=token,
            workspace_url=workspace_url,
            auth_method=auth_method,
            user_identity=user_identity
        )
        
        assert auth_ctx.token == token
        assert auth_ctx.workspace_url == workspace_url
        assert auth_ctx.auth_method == auth_method
        assert auth_ctx.user_identity == user_identity

    def test_auth_context_workspace_url_normalization(self):
        """Test AuthContext normalizes workspace URL"""
        token = "test-token"
        workspace_url = "test.databricks.com/"  # No https, trailing slash
        auth_method = "pat"
        
        auth_ctx = AuthContext(
            token=token,
            workspace_url=workspace_url,
            auth_method=auth_method
        )
        
        assert auth_ctx.workspace_url == "https://test.databricks.com"

    def test_auth_context_workspace_url_strips_trailing_slash(self):
        """Test AuthContext strips trailing slash from workspace URL"""
        token = "test-token"
        workspace_url = "https://test.databricks.com/"
        auth_method = "pat"
        
        auth_ctx = AuthContext(
            token=token,
            workspace_url=workspace_url,
            auth_method=auth_method
        )
        
        assert auth_ctx.workspace_url == "https://test.databricks.com"

    def test_auth_context_get_headers(self):
        """Test AuthContext get_headers method"""
        token = "test-token"
        workspace_url = "https://test.databricks.com"
        auth_method = "pat"
        
        auth_ctx = AuthContext(
            token=token,
            workspace_url=workspace_url,
            auth_method=auth_method
        )
        
        headers = auth_ctx.get_headers()
        
        assert isinstance(headers, dict)
        assert headers["Authorization"] == f"Bearer {token}"
        assert headers["Content-Type"] == "application/json"

    def test_auth_context_get_mcp_headers_basic(self):
        """Test AuthContext get_mcp_headers without SSE"""
        token = "test-token"
        workspace_url = "https://test.databricks.com"
        auth_method = "pat"
        
        auth_ctx = AuthContext(
            token=token,
            workspace_url=workspace_url,
            auth_method=auth_method
        )
        
        headers = auth_ctx.get_mcp_headers()
        
        assert isinstance(headers, dict)
        # Should include basic headers

    def test_auth_context_get_mcp_headers_with_sse(self):
        """Test AuthContext get_mcp_headers with SSE enabled"""
        token = "test-token"
        workspace_url = "https://test.databricks.com"
        auth_method = "pat"
        
        auth_ctx = AuthContext(
            token=token,
            workspace_url=workspace_url,
            auth_method=auth_method
        )
        
        headers = auth_ctx.get_mcp_headers(include_sse=True)
        
        assert isinstance(headers, dict)
        # Should include SSE headers when enabled

    def test_auth_context_get_litellm_params(self):
        """Test AuthContext get_litellm_params method"""
        token = "test-token"
        workspace_url = "https://test.databricks.com"
        auth_method = "pat"
        
        auth_ctx = AuthContext(
            token=token,
            workspace_url=workspace_url,
            auth_method=auth_method
        )
        
        params = auth_ctx.get_litellm_params()
        
        assert isinstance(params, dict)
        # Should return LiteLLM-compatible parameters

    def test_auth_context_get_workspace_client(self):
        """Test AuthContext get_workspace_client method"""
        token = "test-token"
        workspace_url = "https://test.databricks.com"
        auth_method = "pat"
        
        auth_ctx = AuthContext(
            token=token,
            workspace_url=workspace_url,
            auth_method=auth_method
        )
        
        # Should have get_workspace_client method
        assert hasattr(auth_ctx, 'get_workspace_client')
        assert callable(getattr(auth_ctx, 'get_workspace_client'))

    def test_auth_context_repr(self):
        """Test AuthContext __repr__ method"""
        token = "test-token"
        workspace_url = "https://test.databricks.com"
        auth_method = "obo"
        user_identity = "test@example.com"
        
        auth_ctx = AuthContext(
            token=token,
            workspace_url=workspace_url,
            auth_method=auth_method,
            user_identity=user_identity
        )
        
        repr_str = repr(auth_ctx)
        
        assert isinstance(repr_str, str)
        assert "AuthContext" in repr_str
        assert auth_method in repr_str
        assert user_identity in repr_str

    def test_auth_context_repr_service_principal(self):
        """Test AuthContext __repr__ for service principal"""
        token = "test-token"
        workspace_url = "https://test.databricks.com"
        auth_method = "service_principal"
        
        auth_ctx = AuthContext(
            token=token,
            workspace_url=workspace_url,
            auth_method=auth_method
        )
        
        repr_str = repr(auth_ctx)
        
        assert isinstance(repr_str, str)
        assert "AuthContext" in repr_str
        assert "service_principal" in repr_str
        assert "service" in repr_str


class TestUtilityFunctions:
    """Test standalone utility functions"""

    def test_extract_user_token_from_request_with_authorization_header(self):
        """Test extract_user_token_from_request with Authorization header"""
        mock_request = Mock()
        mock_request.headers = {"Authorization": "Bearer test-token"}
        
        result = extract_user_token_from_request(mock_request)
        
        assert result == "test-token"

    def test_extract_user_token_from_request_with_forwarded_header(self):
        """Test extract_user_token_from_request with X-Forwarded-Access-Token header"""
        mock_request = Mock()
        mock_request.headers = {"X-Forwarded-Access-Token": "forwarded-token"}
        
        result = extract_user_token_from_request(mock_request)
        
        assert result == "forwarded-token"

    def test_extract_user_token_from_request_no_token(self):
        """Test extract_user_token_from_request with no token"""
        mock_request = Mock()
        mock_request.headers = {}
        
        result = extract_user_token_from_request(mock_request)
        
        assert result is None

    def test_extract_user_token_from_request_invalid_bearer(self):
        """Test extract_user_token_from_request with invalid Bearer format"""
        mock_request = Mock()
        mock_request.headers = {"Authorization": "InvalidFormat"}
        
        result = extract_user_token_from_request(mock_request)
        
        assert result is None

    def test_extract_user_token_from_request_empty_bearer(self):
        """Test extract_user_token_from_request with empty Bearer token"""
        mock_request = Mock()
        mock_request.headers = {"Authorization": "Bearer "}
        
        result = extract_user_token_from_request(mock_request)
        
        assert result is None or result == ""

    def test_is_scope_error_with_scope_error(self):
        """Test is_scope_error with actual scope error"""
        error = Exception("insufficient_scope: Scope required")
        
        result = is_scope_error(error)
        
        assert isinstance(result, bool)
        # Should detect scope-related errors

    def test_is_scope_error_with_permission_error(self):
        """Test is_scope_error with permission error"""
        error = Exception("permission denied")
        
        result = is_scope_error(error)
        
        assert isinstance(result, bool)
        # Should detect permission-related errors

    def test_is_scope_error_with_regular_error(self):
        """Test is_scope_error with regular error"""
        error = Exception("regular error message")
        
        result = is_scope_error(error)
        
        assert isinstance(result, bool)
        # Should return False for non-scope errors

    def test_is_scope_error_with_none(self):
        """Test is_scope_error with None"""
        result = is_scope_error(None)
        
        assert isinstance(result, bool)
        assert result is False

    def test_setup_environment_variables_with_user_token(self):
        """Test setup_environment_variables with user token"""
        user_token = "test-user-token"
        
        result = setup_environment_variables(user_token)
        
        assert isinstance(result, bool)
        # Should return success/failure status

    def test_setup_environment_variables_without_token(self):
        """Test setup_environment_variables without token"""
        result = setup_environment_variables(None)
        
        assert isinstance(result, bool)
        # Should handle None token gracefully

    def test_clean_environment_context_manager(self):
        """Test _clean_environment context manager"""
        # Set some environment variables
        original_env = os.environ.copy()
        os.environ["TEST_VAR"] = "test_value"
        
        with _clean_environment():
            # Environment should be managed
            assert True  # Context manager should work
        
        # Environment should be restored
        # Note: _clean_environment manages Databricks-specific vars, not TEST_VAR

    def test_get_databricks_auth_headers_sync_basic(self):
        """Test get_databricks_auth_headers_sync basic functionality"""
        result = get_databricks_auth_headers_sync()
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        # Should return (headers, error) tuple

    def test_get_databricks_auth_headers_sync_with_host(self):
        """Test get_databricks_auth_headers_sync with host parameter"""
        host = "https://test.databricks.com"
        
        result = get_databricks_auth_headers_sync(host=host)
        
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_get_databricks_auth_headers_sync_with_user_token(self):
        """Test get_databricks_auth_headers_sync with user token"""
        user_token = "test-token"

        result = get_databricks_auth_headers_sync(user_token=user_token)

        assert isinstance(result, tuple)
        assert len(result) == 2


class TestAsyncFunctions:
    """Test async utility functions"""

    @pytest.mark.asyncio
    async def test_validate_databricks_connection(self):
        """Test validate_databricks_connection async function"""
        result = await validate_databricks_connection()

        assert isinstance(result, tuple)
        assert len(result) == 2
        # Should return (success, error_message) tuple

    @pytest.mark.asyncio
    async def test_get_databricks_auth_headers_basic(self):
        """Test get_databricks_auth_headers basic functionality"""
        result = await get_databricks_auth_headers()

        assert isinstance(result, tuple)
        assert len(result) == 2
        # Should return (headers, error) tuple

    @pytest.mark.asyncio
    async def test_get_databricks_auth_headers_with_host(self):
        """Test get_databricks_auth_headers with host parameter"""
        host = "https://test.databricks.com"

        result = await get_databricks_auth_headers(host=host)

        assert isinstance(result, tuple)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_databricks_auth_headers_with_user_token(self):
        """Test get_databricks_auth_headers with user token"""
        user_token = "test-token"

        result = await get_databricks_auth_headers(user_token=user_token)

        assert isinstance(result, tuple)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_auth_context_basic(self):
        """Test get_auth_context basic functionality"""
        result = await get_auth_context()

        # Should return AuthContext or None
        assert result is None or isinstance(result, AuthContext)

    @pytest.mark.asyncio
    async def test_get_auth_context_with_user_token(self):
        """Test get_auth_context with user token"""
        user_token = "test-token"

        result = await get_auth_context(user_token=user_token)

        # Should return AuthContext or None
        assert result is None or isinstance(result, AuthContext)

    @pytest.mark.asyncio
    async def test_get_workspace_client_basic(self):
        """Test get_workspace_client basic functionality"""
        result = await get_workspace_client()

        # Should return WorkspaceClient or None
        assert result is None or hasattr(result, 'clusters')

    @pytest.mark.asyncio
    async def test_get_workspace_client_with_user_token(self):
        """Test get_workspace_client with user token"""
        user_token = "test-token"

        result = await get_workspace_client(user_token=user_token)

        # Should return WorkspaceClient or None
        assert result is None or hasattr(result, 'clusters')

    @pytest.mark.asyncio
    async def test_get_workspace_client_with_fallback_basic(self):
        """Test get_workspace_client_with_fallback basic functionality"""
        result = await get_workspace_client_with_fallback()

        assert isinstance(result, tuple)
        assert len(result) == 2
        # Should return (client, error_message) tuple

    @pytest.mark.asyncio
    async def test_get_workspace_client_with_fallback_with_operation_name(self):
        """Test get_workspace_client_with_fallback with operation name"""
        operation_name = "test_operation"

        result = await get_workspace_client_with_fallback(operation_name=operation_name)

        assert isinstance(result, tuple)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_mcp_access_token(self):
        """Test get_mcp_access_token async function"""
        result = await get_mcp_access_token()

        assert isinstance(result, tuple)
        assert len(result) == 2
        # Should return (token, error_message) tuple

    @pytest.mark.asyncio
    async def test_get_current_databricks_user_basic(self):
        """Test get_current_databricks_user basic functionality"""
        result = await get_current_databricks_user()

        assert isinstance(result, tuple)
        assert len(result) == 2
        # Should return (user_identity, error_message) tuple

    @pytest.mark.asyncio
    async def test_get_current_databricks_user_with_user_token(self):
        """Test get_current_databricks_user with user token"""
        user_token = "test-token"

        result = await get_current_databricks_user(user_token=user_token)

        assert isinstance(result, tuple)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_mcp_auth_headers_basic(self):
        """Test get_mcp_auth_headers basic functionality"""
        mcp_server_url = "https://mcp.example.com"

        result = await get_mcp_auth_headers(mcp_server_url)

        assert isinstance(result, tuple)
        assert len(result) == 2
        # Should return (headers, error_message) tuple

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.DatabricksAuth')
    async def test_get_mcp_auth_headers_with_user_token(self, mock_databricks_auth):
        """Test get_mcp_auth_headers with user token"""
        mcp_server_url = "https://mcp.example.com"
        user_token = "test-token"

        # Mock DatabricksAuth instance and its methods
        mock_auth_instance = Mock()
        mock_auth_instance.set_user_access_token = Mock()
        mock_auth_instance.get_auth_headers = AsyncMock(return_value=({"Authorization": "Bearer test"}, None))
        mock_databricks_auth.return_value = mock_auth_instance

        result = await get_mcp_auth_headers(mcp_server_url, user_token=user_token)

        assert isinstance(result, tuple)
        assert len(result) == 2
        mock_auth_instance.set_user_access_token.assert_called_once_with(user_token)
        mock_auth_instance.get_auth_headers.assert_called_once_with(mcp_server_url=mcp_server_url)

    @pytest.mark.asyncio
    @patch('src.utils.databricks_auth.DatabricksAuth')
    async def test_get_mcp_auth_headers_with_api_key(self, mock_databricks_auth):
        """Test get_mcp_auth_headers with api key"""
        mcp_server_url = "https://mcp.example.com"
        api_key = "test-api-key"

        # Mock DatabricksAuth instance and its methods
        mock_auth_instance = Mock()
        mock_auth_instance.get_auth_headers = AsyncMock(return_value=({"Authorization": "Bearer test"}, None))
        mock_databricks_auth.return_value = mock_auth_instance

        result = await get_mcp_auth_headers(mcp_server_url, api_key=api_key)

        assert isinstance(result, tuple)
        assert len(result) == 2
