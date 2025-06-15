"""
Unit tests for user context utilities.

Tests the functionality of user and group context management including
token extraction, group isolation, context variables, and middleware.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional, Dict, Any

from fastapi import Request

from src.utils.user_context import (
    GroupContext, UserContext, 
    extract_user_token_from_request, extract_group_context_from_request, 
    extract_user_context_from_request, user_context_middleware,
    is_databricks_app_context, _is_databricks_apps_enabled
)


class TestGroupContext:
    """Test cases for GroupContext class."""
    
    def test_group_context_initialization(self):
        """Test GroupContext initialization with all parameters."""
        group_context = GroupContext(
            group_ids=["group-1", "group-2"],
            group_email="user@company.com",
            email_domain="company.com",
            user_id="user-123",
            access_token="token-abc"
        )
        
        assert group_context.group_ids == ["group-1", "group-2"]
        assert group_context.group_email == "user@company.com"
        assert group_context.email_domain == "company.com"
        assert group_context.user_id == "user-123"
        assert group_context.access_token == "token-abc"
    
    def test_group_context_default_initialization(self):
        """Test GroupContext initialization with default parameters."""
        group_context = GroupContext()
        
        assert group_context.group_ids is None
        assert group_context.group_email is None
        assert group_context.email_domain is None
        assert group_context.user_id is None
        assert group_context.access_token is None
    
    def test_primary_group_id_with_groups(self):
        """Test primary_group_id property when groups exist."""
        group_context = GroupContext(group_ids=["group-1", "group-2", "group-3"])
        
        assert group_context.primary_group_id == "group-1"
    
    def test_primary_group_id_single_group(self):
        """Test primary_group_id property with single group."""
        group_context = GroupContext(group_ids=["only-group"])
        
        assert group_context.primary_group_id == "only-group"
    
    def test_primary_group_id_no_groups(self):
        """Test primary_group_id property when no groups exist."""
        group_context = GroupContext(group_ids=[])
        
        assert group_context.primary_group_id is None
    
    def test_primary_group_id_none_groups(self):
        """Test primary_group_id property when groups is None."""
        group_context = GroupContext(group_ids=None)
        
        assert group_context.primary_group_id is None
    
    def test_generate_group_id_basic(self):
        """Test generate_group_id with basic domain."""
        result = GroupContext.generate_group_id("acme-corp.com")
        
        assert result == "acme_corp_com"
    
    def test_generate_group_id_complex(self):
        """Test generate_group_id with complex domain."""
        result = GroupContext.generate_group_id("tech.startup.io")
        
        assert result == "tech_startup_io"
    
    def test_generate_group_id_special_chars(self):
        """Test generate_group_id with special characters."""
        result = GroupContext.generate_group_id("test-domain.co.uk")
        
        assert result == "test_domain_co_uk"
    
    def test_generate_individual_group_id_basic(self):
        """Test generate_individual_group_id with basic email."""
        result = GroupContext.generate_individual_group_id("alice@company.com")
        
        assert result == "user_alice_company_com"
    
    def test_generate_individual_group_id_complex(self):
        """Test generate_individual_group_id with complex email."""
        result = GroupContext.generate_individual_group_id("bob.smith@startup.io")
        
        assert result == "user_bob_smith_startup_io"
    
    def test_generate_individual_group_id_special_chars(self):
        """Test generate_individual_group_id with special characters."""
        result = GroupContext.generate_individual_group_id("user+test@co-domain.com")
        
        assert result == "user_user_test_co_domain_com"
    
    def test_is_valid_with_valid_context(self):
        """Test is_valid with valid group context."""
        group_context = GroupContext(
            group_ids=["group-1"],
            email_domain="company.com"
        )
        
        assert group_context.is_valid() is True
    
    def test_is_valid_no_groups(self):
        """Test is_valid with no groups."""
        group_context = GroupContext(
            group_ids=[],
            email_domain="company.com"
        )
        
        assert group_context.is_valid() is False
    
    def test_is_valid_no_domain(self):
        """Test is_valid with no email domain."""
        group_context = GroupContext(
            group_ids=["group-1"],
            email_domain=None
        )
        
        assert group_context.is_valid() is False
    
    def test_is_valid_none_groups(self):
        """Test is_valid with None groups."""
        group_context = GroupContext(
            group_ids=None,
            email_domain="company.com"
        )
        
        assert group_context.is_valid() is False
    
    @pytest.mark.asyncio
    @patch('src.utils.user_context.GroupContext._get_user_group_memberships')
    async def test_from_email_with_groups(self, mock_get_memberships):
        """Test from_email when user has group memberships."""
        mock_get_memberships.return_value = ["group-1", "group-2"]
        
        result = await GroupContext.from_email(
            "user@company.com", 
            access_token="token-123",
            user_id="user-456"
        )
        
        assert result.group_ids == ["group-1", "group-2"]
        assert result.group_email == "user@company.com"
        assert result.email_domain == "company.com"
        assert result.user_id == "user-456"
        assert result.access_token == "token-123"
        mock_get_memberships.assert_called_once_with("user@company.com")
    
    @pytest.mark.asyncio
    @patch('src.utils.user_context.GroupContext._get_user_group_memberships')
    async def test_from_email_no_groups(self, mock_get_memberships):
        """Test from_email when user has no group memberships."""
        mock_get_memberships.return_value = []
        
        result = await GroupContext.from_email("user@company.com")
        
        assert result.group_ids == ["user_user_company_com"]  # Individual group
        assert result.group_email == "user@company.com"
        assert result.email_domain == "company.com"
        mock_get_memberships.assert_called_once_with("user@company.com")
    
    @pytest.mark.asyncio
    @patch('src.utils.user_context.GroupContext._get_user_group_memberships')
    async def test_from_email_lookup_error(self, mock_get_memberships):
        """Test from_email when group lookup fails."""
        mock_get_memberships.side_effect = Exception("Database error")
        
        result = await GroupContext.from_email("user@company.com")
        
        assert result.group_ids == ["user_user_company_com"]  # Fallback to individual
        assert result.group_email == "user@company.com"
        assert result.email_domain == "company.com"
    
    @pytest.mark.asyncio
    async def test_from_email_invalid_email(self):
        """Test from_email with invalid email."""
        result = await GroupContext.from_email("invalid-email")
        
        assert result.group_ids is None
        assert result.group_email is None
        assert result.email_domain is None
    
    @pytest.mark.asyncio
    async def test_from_email_empty_email(self):
        """Test from_email with empty email."""
        result = await GroupContext.from_email("")
        
        assert result.group_ids is None
        assert result.group_email is None
        assert result.email_domain is None
    
    @pytest.mark.asyncio
    @patch('src.db.session.async_session_factory')
    async def test_get_user_group_memberships_success(self, mock_session_factory):
        """Test _get_user_group_memberships with successful lookup."""
        # Mock session and service
        mock_session = AsyncMock()
        mock_session_factory.return_value.__aenter__.return_value = mock_session
        mock_session_factory.return_value.__aexit__.return_value = None
        
        mock_groups = [MagicMock(id="group-1"), MagicMock(id="group-2")]
        
        with patch('src.services.group_service.GroupService') as mock_service_class:
            mock_service = AsyncMock()
            mock_service.get_user_group_memberships.return_value = mock_groups
            mock_service_class.return_value = mock_service
            
            result = await GroupContext._get_user_group_memberships("user@test.com")
            
            assert result == ["group-1", "group-2"]
            mock_service.get_user_group_memberships.assert_called_once_with("user@test.com")
    
    @pytest.mark.asyncio
    @patch('src.db.session.async_session_factory')
    async def test_get_user_group_memberships_error(self, mock_session_factory):
        """Test _get_user_group_memberships with error."""
        mock_session_factory.side_effect = Exception("Connection error")
        
        result = await GroupContext._get_user_group_memberships("user@test.com")
        
        assert result == []


class TestUserContext:
    """Test cases for UserContext class."""
    
    def test_set_and_get_user_token(self):
        """Test setting and getting user token."""
        token = "test-token-123"
        
        UserContext.set_user_token(token)
        result = UserContext.get_user_token()
        
        assert result == token
    
    def test_get_user_token_not_set(self):
        """Test getting user token when not set."""
        UserContext.clear_context()
        
        result = UserContext.get_user_token()
        
        assert result is None
    
    def test_set_and_get_user_context(self):
        """Test setting and getting user context."""
        context = {
            "user_id": "123",
            "email": "user@test.com",
            "permissions": ["read", "write"]
        }
        
        UserContext.set_user_context(context)
        result = UserContext.get_user_context()
        
        assert result == context
        assert result["user_id"] == "123"
        assert result["email"] == "user@test.com"
    
    def test_get_user_context_not_set(self):
        """Test getting user context when not set."""
        UserContext.clear_context()
        
        result = UserContext.get_user_context()
        
        assert result is None
    
    def test_set_and_get_group_context(self):
        """Test setting and getting group context."""
        group_context = GroupContext(
            group_ids=["group-1"],
            group_email="user@company.com",
            email_domain="company.com"
        )
        
        UserContext.set_group_context(group_context)
        result = UserContext.get_group_context()
        
        assert result == group_context
        assert result.group_ids == ["group-1"]
        assert result.group_email == "user@company.com"
    
    def test_get_group_context_not_set(self):
        """Test getting group context when not set."""
        UserContext.clear_context()
        
        result = UserContext.get_group_context()
        
        assert result is None
    
    def test_clear_context(self):
        """Test clearing all context."""
        # Set all contexts
        UserContext.set_user_token("token")
        UserContext.set_user_context({"user_id": "123"})
        UserContext.set_group_context(GroupContext(group_ids=["group-1"]))
        
        # Clear all
        UserContext.clear_context()
        
        # Verify all are cleared
        assert UserContext.get_user_token() is None
        assert UserContext.get_user_context() is None
        assert UserContext.get_group_context() is None


class TestExtractUserTokenFromRequest:
    """Test cases for extract_user_token_from_request function."""
    
    def test_extract_forwarded_access_token(self):
        """Test extracting X-Forwarded-Access-Token header."""
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            'X-Forwarded-Access-Token': 'forwarded-token-123'
        }
        
        result = extract_user_token_from_request(mock_request)
        
        assert result == 'forwarded-token-123'
    
    def test_extract_authorization_bearer_token(self):
        """Test extracting Authorization Bearer token."""
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            'Authorization': 'Bearer bearer-token-456'
        }
        
        result = extract_user_token_from_request(mock_request)
        
        assert result == 'bearer-token-456'
    
    def test_extract_forwarded_token_priority(self):
        """Test that X-Forwarded-Access-Token takes priority over Authorization."""
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            'X-Forwarded-Access-Token': 'forwarded-token',
            'Authorization': 'Bearer bearer-token'
        }
        
        result = extract_user_token_from_request(mock_request)
        
        assert result == 'forwarded-token'
    
    def test_extract_no_token_headers(self):
        """Test when no token headers are present."""
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            'Content-Type': 'application/json'
        }
        
        result = extract_user_token_from_request(mock_request)
        
        assert result is None
    
    def test_extract_invalid_authorization_header(self):
        """Test with invalid Authorization header format."""
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            'Authorization': 'Invalid format'
        }
        
        result = extract_user_token_from_request(mock_request)
        
        assert result is None
    
    def test_extract_token_exception_handling(self):
        """Test exception handling in token extraction."""
        mock_request = MagicMock(spec=Request)
        mock_request.headers.get.side_effect = Exception("Header error")
        
        result = extract_user_token_from_request(mock_request)
        
        assert result is None


class TestExtractGroupContextFromRequest:
    """Test cases for extract_group_context_from_request function."""
    
    @pytest.mark.asyncio
    @patch('src.utils.user_context.GroupContext.from_email')
    @patch('src.utils.user_context.extract_user_token_from_request')
    async def test_extract_group_context_success(self, mock_extract_token, mock_from_email):
        """Test successful group context extraction."""
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            'X-Forwarded-Email': 'user@company.com'
        }
        mock_extract_token.return_value = "token-123"
        
        mock_group_context = GroupContext(
            group_ids=["group-1"],
            group_email="user@company.com",
            email_domain="company.com"
        )
        mock_from_email.return_value = mock_group_context
        
        result = await extract_group_context_from_request(mock_request)
        
        assert result == mock_group_context
        mock_from_email.assert_called_once_with("user@company.com", "token-123")
    
    @pytest.mark.asyncio
    async def test_extract_group_context_no_email(self):
        """Test group context extraction with no email header."""
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {}
        
        result = await extract_group_context_from_request(mock_request)
        
        assert result is None
    
    @pytest.mark.asyncio
    @patch('src.utils.user_context.GroupContext.from_email')
    async def test_extract_group_context_invalid_context(self, mock_from_email):
        """Test group context extraction with invalid context."""
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            'X-Forwarded-Email': 'user@company.com'
        }
        
        mock_invalid_context = GroupContext()  # Invalid context
        mock_from_email.return_value = mock_invalid_context
        
        result = await extract_group_context_from_request(mock_request)
        
        assert result is None
    
    @pytest.mark.asyncio
    @patch('src.utils.user_context.GroupContext.from_email')
    async def test_extract_group_context_exception(self, mock_from_email):
        """Test group context extraction with exception."""
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            'X-Forwarded-Email': 'user@company.com'
        }
        mock_from_email.side_effect = Exception("Context error")
        
        result = await extract_group_context_from_request(mock_request)
        
        assert result is None


class TestExtractUserContextFromRequest:
    """Test cases for extract_user_context_from_request function."""
    
    @patch('src.utils.user_context.extract_user_token_from_request')
    def test_extract_user_context_complete(self, mock_extract_token):
        """Test extracting complete user context."""
        mock_extract_token.return_value = "token-123"
        
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            'X-Forwarded-Email': 'user@company.com',
            'User-Agent': 'TestAgent/1.0',
            'X-Databricks-Workspace': 'workspace-123',
            'X-Forwarded-For': '192.168.1.1'
        }
        mock_request.client = MagicMock()
        mock_request.client.host = '127.0.0.1'
        mock_request.method = 'POST'
        mock_request.url = 'https://api.example.com/test'
        
        result = extract_user_context_from_request(mock_request)
        
        assert result['access_token'] == 'token-123'
        assert result['email'] == 'user@company.com'
        assert result['user_agent'] == 'TestAgent/1.0'
        assert result['client_host'] == '127.0.0.1'
        assert result['method'] == 'POST'
        assert result['url'] == 'https://api.example.com/test'
        assert 'databricks_headers' in result
        assert 'X-Databricks-Workspace' in result['databricks_headers']
        assert 'X-Forwarded-For' in result['databricks_headers']
    
    def test_extract_user_context_minimal(self):
        """Test extracting minimal user context."""
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {}
        mock_request.client = None
        mock_request.method = 'GET'
        mock_request.url = 'https://api.example.com'
        
        with patch('src.utils.user_context.extract_user_token_from_request') as mock_extract_token:
            mock_extract_token.return_value = None
            
            result = extract_user_context_from_request(mock_request)
        
        assert result['client_host'] is None
        assert result['method'] == 'GET'
        assert result['url'] == 'https://api.example.com'
        assert 'access_token' not in result
        assert 'email' not in result
        assert 'user_agent' not in result
    
    def test_extract_user_context_exception(self):
        """Test user context extraction with exception."""
        mock_request = MagicMock(spec=Request)
        mock_request.headers.items.side_effect = Exception("Headers error")
        
        result = extract_user_context_from_request(mock_request)
        
        assert result == {}


class TestUserContextMiddleware:
    """Test cases for user_context_middleware function."""
    
    @pytest.mark.asyncio
    @patch('src.utils.user_context._is_databricks_apps_enabled')
    @patch('src.utils.user_context.extract_group_context_from_request')
    @patch('src.utils.user_context.extract_user_context_from_request')
    @patch('src.utils.user_context.UserContext')
    async def test_middleware_apps_enabled(self, mock_user_context_class, mock_extract_user_context, 
                                         mock_extract_group_context, mock_is_apps_enabled):
        """Test middleware when Databricks Apps is enabled."""
        mock_is_apps_enabled.return_value = True
        
        mock_group_context = GroupContext(group_ids=["group-1"])
        mock_extract_group_context.return_value = mock_group_context
        
        mock_user_context = {"user_id": "123", "access_token": "token"}
        mock_extract_user_context.return_value = mock_user_context
        
        mock_request = MagicMock(spec=Request)
        mock_call_next = AsyncMock()
        mock_response = MagicMock()
        mock_call_next.return_value = mock_response
        
        result = await user_context_middleware(mock_request, mock_call_next)
        
        assert result == mock_response
        mock_user_context_class.set_group_context.assert_called_once_with(mock_group_context)
        mock_user_context_class.set_user_context.assert_called_once_with(mock_user_context)
        mock_user_context_class.set_user_token.assert_called_once_with("token")
        mock_user_context_class.clear_context.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.utils.user_context._is_databricks_apps_enabled')
    @patch('src.utils.user_context.extract_group_context_from_request')
    @patch('src.utils.user_context.UserContext')
    async def test_middleware_apps_disabled(self, mock_user_context_class, mock_extract_group_context,
                                          mock_is_apps_enabled):
        """Test middleware when Databricks Apps is disabled."""
        mock_is_apps_enabled.return_value = False
        
        mock_group_context = GroupContext(group_ids=["group-1"])
        mock_extract_group_context.return_value = mock_group_context
        
        mock_request = MagicMock(spec=Request)
        mock_call_next = AsyncMock()
        mock_response = MagicMock()
        mock_call_next.return_value = mock_response
        
        result = await user_context_middleware(mock_request, mock_call_next)
        
        assert result == mock_response
        mock_user_context_class.set_group_context.assert_called_once_with(mock_group_context)
        mock_user_context_class.set_user_context.assert_not_called()
        mock_user_context_class.set_user_token.assert_not_called()
        mock_user_context_class.clear_context.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.utils.user_context._is_databricks_apps_enabled')
    @patch('src.utils.user_context.extract_group_context_from_request')
    @patch('src.utils.user_context.UserContext')
    async def test_middleware_no_group_context(self, mock_user_context_class, mock_extract_group_context,
                                             mock_is_apps_enabled):
        """Test middleware when no group context is extracted."""
        mock_is_apps_enabled.return_value = True
        mock_extract_group_context.return_value = None
        
        mock_request = MagicMock(spec=Request)
        mock_call_next = AsyncMock()
        mock_response = MagicMock()
        mock_call_next.return_value = mock_response
        
        result = await user_context_middleware(mock_request, mock_call_next)
        
        assert result == mock_response
        mock_user_context_class.set_group_context.assert_not_called()
    
    @pytest.mark.asyncio
    @patch('src.utils.user_context._is_databricks_apps_enabled')
    @patch('src.utils.user_context.UserContext')
    async def test_middleware_exception_handling(self, mock_user_context_class, mock_is_apps_enabled):
        """Test middleware exception handling."""
        mock_is_apps_enabled.side_effect = Exception("Apps check error")
        
        mock_request = MagicMock(spec=Request)
        mock_call_next = AsyncMock()
        mock_response = MagicMock()
        mock_call_next.return_value = mock_response
        
        result = await user_context_middleware(mock_request, mock_call_next)
        
        assert result == mock_response
        mock_user_context_class.clear_context.assert_called()


class TestIsDatabricksAppsEnabled:
    """Test cases for _is_databricks_apps_enabled function."""
    
    @pytest.mark.asyncio
    @patch('src.core.unit_of_work.UnitOfWork')
    @patch('src.services.databricks_service.DatabricksService')
    async def test_is_databricks_apps_enabled_true(self, mock_service_class, mock_uow_class):
        """Test _is_databricks_apps_enabled when enabled."""
        mock_uow = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        mock_service = AsyncMock()
        mock_service_class.from_unit_of_work = AsyncMock(return_value=mock_service)
        
        mock_config = MagicMock()
        mock_config.apps_enabled = True
        mock_service.get_databricks_config.return_value = mock_config
        
        result = await _is_databricks_apps_enabled()
        
        assert result is True
    
    @pytest.mark.asyncio
    @patch('src.core.unit_of_work.UnitOfWork')
    @patch('src.services.databricks_service.DatabricksService')
    async def test_is_databricks_apps_enabled_false(self, mock_service_class, mock_uow_class):
        """Test _is_databricks_apps_enabled when disabled."""
        mock_uow = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        mock_service = AsyncMock()
        mock_service_class.from_unit_of_work = AsyncMock(return_value=mock_service)
        
        mock_config = MagicMock()
        mock_config.apps_enabled = False
        mock_service.get_databricks_config.return_value = mock_config
        
        result = await _is_databricks_apps_enabled()
        
        assert result is False
    
    @pytest.mark.asyncio
    @patch('src.core.unit_of_work.UnitOfWork')
    @patch('src.services.databricks_service.DatabricksService')
    async def test_is_databricks_apps_enabled_no_config(self, mock_service_class, mock_uow_class):
        """Test _is_databricks_apps_enabled with no config."""
        mock_uow = AsyncMock()
        mock_uow_class.return_value.__aenter__.return_value = mock_uow
        mock_uow_class.return_value.__aexit__.return_value = None
        
        mock_service = AsyncMock()
        mock_service_class.from_unit_of_work = AsyncMock(return_value=mock_service)
        mock_service.get_databricks_config.return_value = None
        
        result = await _is_databricks_apps_enabled()
        
        assert result is False
    
    @pytest.mark.asyncio
    @patch('src.core.unit_of_work.UnitOfWork')
    async def test_is_databricks_apps_enabled_exception(self, mock_uow_class):
        """Test _is_databricks_apps_enabled with exception."""
        mock_uow_class.side_effect = Exception("Service error")
        
        result = await _is_databricks_apps_enabled()
        
        assert result is False


class TestIsDatabricksAppContext:
    """Test cases for is_databricks_app_context function."""
    
    @patch('src.utils.user_context.UserContext.get_user_context')
    def test_is_databricks_app_context_true(self, mock_get_context):
        """Test is_databricks_app_context when in Databricks context."""
        mock_get_context.return_value = {
            'access_token': 'token-123',
            'databricks_headers': {
                'X-Databricks-Workspace': 'workspace-123'
            }
        }
        
        result = is_databricks_app_context()
        
        assert result is True
    
    @patch('src.utils.user_context.UserContext.get_user_context')
    def test_is_databricks_app_context_with_databricks_key(self, mock_get_context):
        """Test is_databricks_app_context with databricks key in context."""
        mock_get_context.return_value = {
            'access_token': 'token-123',
            'databricks_user_id': 'user-123'
        }
        
        result = is_databricks_app_context()
        
        assert result is True
    
    @patch('src.utils.user_context.UserContext.get_user_context')
    def test_is_databricks_app_context_no_token(self, mock_get_context):
        """Test is_databricks_app_context without access token."""
        mock_get_context.return_value = {
            'databricks_headers': {
                'X-Databricks-Workspace': 'workspace-123'
            }
        }
        
        result = is_databricks_app_context()
        
        assert result is False
    
    @patch('src.utils.user_context.UserContext.get_user_context')
    def test_is_databricks_app_context_no_databricks_info(self, mock_get_context):
        """Test is_databricks_app_context without Databricks info."""
        mock_get_context.return_value = {
            'access_token': 'token-123',
            'user_id': 'user-123'
        }
        
        result = is_databricks_app_context()
        
        assert result is False
    
    @patch('src.utils.user_context.UserContext.get_user_context')
    def test_is_databricks_app_context_no_context(self, mock_get_context):
        """Test is_databricks_app_context without user context."""
        mock_get_context.return_value = None
        
        result = is_databricks_app_context()
        
        assert result is False


class TestUserContextIntegration:
    """Test cases for integration scenarios."""
    
    @pytest.mark.asyncio
    async def test_complete_user_context_workflow(self):
        """Test complete user context workflow."""
        # Setup mock request
        mock_request = MagicMock(spec=Request)
        mock_request.headers = {
            'X-Forwarded-Email': 'user@company.com',
            'X-Forwarded-Access-Token': 'token-123'
        }
        
        # Test token extraction
        token = extract_user_token_from_request(mock_request)
        assert token == 'token-123'
        
        # Test user context extraction
        user_context = extract_user_context_from_request(mock_request)
        assert user_context['access_token'] == 'token-123'
        assert user_context['email'] == 'user@company.com'
        
        # Test setting context
        UserContext.set_user_token(token)
        UserContext.set_user_context(user_context)
        
        # Verify context is set
        assert UserContext.get_user_token() == 'token-123'
        assert UserContext.get_user_context()['email'] == 'user@company.com'
        
        # Test clearing context
        UserContext.clear_context()
        assert UserContext.get_user_token() is None
        assert UserContext.get_user_context() is None
    
    @pytest.mark.asyncio
    @patch('src.utils.user_context.GroupContext._get_user_group_memberships')
    async def test_group_context_isolation_workflow(self, mock_get_memberships):
        """Test group context isolation workflow."""
        # Setup different users with different groups
        mock_get_memberships.side_effect = [
            ["group-1", "group-2"],  # User 1 groups
            ["group-3"],             # User 2 groups
            []                       # User 3 no groups
        ]
        
        # Test user 1 (multiple groups)
        context_1 = await GroupContext.from_email("user1@company.com")
        assert context_1.group_ids == ["group-1", "group-2"]
        assert context_1.primary_group_id == "group-1"
        assert context_1.is_valid() is True
        
        # Test user 2 (single group)
        context_2 = await GroupContext.from_email("user2@company.com")
        assert context_2.group_ids == ["group-3"]
        assert context_2.primary_group_id == "group-3"
        assert context_2.is_valid() is True
        
        # Test user 3 (individual group)
        context_3 = await GroupContext.from_email("user3@company.com")
        assert context_3.group_ids == ["user_user3_company_com"]
        assert context_3.primary_group_id == "user_user3_company_com"
        assert context_3.is_valid() is True
    
    def test_context_variable_isolation(self):
        """Test that context variables are properly isolated."""
        import asyncio
        
        async def set_context_1():
            UserContext.set_user_token("token-1")
            UserContext.set_user_context({"user_id": "user-1"})
            await asyncio.sleep(0.1)
            # Context should remain isolated
            assert UserContext.get_user_token() == "token-1"
            assert UserContext.get_user_context()["user_id"] == "user-1"
        
        async def set_context_2():
            UserContext.set_user_token("token-2")
            UserContext.set_user_context({"user_id": "user-2"})
            await asyncio.sleep(0.1)
            # Context should remain isolated
            assert UserContext.get_user_token() == "token-2"
            assert UserContext.get_user_context()["user_id"] == "user-2"
        
        async def run_concurrent_contexts():
            await asyncio.gather(set_context_1(), set_context_2())
        
        # Run concurrent context setting
        asyncio.run(run_concurrent_contexts())


class TestUserContextEdgeCases:
    """Test cases for edge cases and error conditions."""
    
    def test_header_case_insensitivity(self):
        """Test that header extraction handles case variations."""
        from starlette.datastructures import Headers
        
        # Test different case variations
        test_cases = [
            {'x-forwarded-access-token': 'token-123'},
            {'X-FORWARDED-ACCESS-TOKEN': 'token-123'},
            {'authorization': 'Bearer token-456'},
            {'AUTHORIZATION': 'Bearer token-456'}
        ]
        
        for headers in test_cases:
            mock_request = MagicMock(spec=Request)
            # Use Starlette Headers which are case-insensitive
            mock_request.headers = Headers(headers)
            result = extract_user_token_from_request(mock_request)
            assert result is not None
    
    def test_special_characters_in_email(self):
        """Test handling of special characters in email addresses."""
        special_emails = [
            "user+tag@company.com",
            "user.name@company.com", 
            "user-name@company.com",
            "user_name@company.com"
        ]
        
        for email in special_emails:
            individual_group = GroupContext.generate_individual_group_id(email)
            assert isinstance(individual_group, str)
            assert individual_group.startswith("user_")
            assert "_" in individual_group  # Should handle special chars
    
    def test_very_long_email_addresses(self):
        """Test handling of very long email addresses."""
        long_email = f"{'a' * 100}@{'b' * 100}.com"
        
        individual_group = GroupContext.generate_individual_group_id(long_email)
        assert isinstance(individual_group, str)
        assert individual_group.startswith("user_")
    
    @pytest.mark.asyncio
    async def test_concurrent_group_lookups(self):
        """Test concurrent group membership lookups."""
        import asyncio
        
        async def lookup_groups(email):
            return await GroupContext._get_user_group_memberships(email)
        
        emails = [f"user{i}@company.com" for i in range(10)]
        
        with patch('src.db.session.async_session_factory') as mock_factory:
            mock_session = AsyncMock()
            mock_factory.return_value.__aenter__.return_value = mock_session
            mock_factory.return_value.__aexit__.return_value = None
            
            with patch('src.services.group_service.GroupService') as mock_service_class:
                mock_service = AsyncMock()
                mock_service.get_user_group_memberships.return_value = []
                mock_service_class.return_value = mock_service
                
                # Should handle concurrent lookups without issues
                results = await asyncio.gather(*[lookup_groups(email) for email in emails])
                
                assert len(results) == 10
                assert all(result == [] for result in results)
    
    def test_malformed_request_objects(self):
        """Test handling of malformed request objects."""
        # Request with missing attributes
        malformed_request = MagicMock()
        del malformed_request.headers
        
        result = extract_user_token_from_request(malformed_request)
        assert result is None
        
        result = extract_user_context_from_request(malformed_request)
        assert result == {}
    
    @pytest.mark.asyncio
    async def test_memory_cleanup_after_context_clearing(self):
        """Test that context clearing properly releases memory."""
        # Set large context data
        large_context = {f"key_{i}": f"value_{i}" * 1000 for i in range(100)}
        large_group_context = GroupContext(
            group_ids=[f"group-{i}" for i in range(100)],
            group_email="user@company.com"
        )
        
        UserContext.set_user_context(large_context)
        UserContext.set_group_context(large_group_context)
        UserContext.set_user_token("token" * 1000)
        
        # Clear context
        UserContext.clear_context()
        
        # Verify all context is cleared
        assert UserContext.get_user_context() is None
        assert UserContext.get_group_context() is None
        assert UserContext.get_user_token() is None