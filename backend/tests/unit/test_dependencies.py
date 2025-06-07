"""
Unit tests for core dependencies.

Tests the functionality of dependency injection and context extraction
including tenant and group context handling.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import Request

from src.core.dependencies import (
    get_tenant_context, get_group_context, get_repository, get_service, get_log_service
)
from src.core.base_repository import BaseRepository
from src.core.base_service import BaseService
from src.db.base import Base
from src.utils.user_context import TenantContext, GroupContext


@pytest.fixture
def mock_request():
    """Create a mock FastAPI request."""
    request = MagicMock(spec=Request)
    return request


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    session = AsyncMock()
    return session


# Mock model for testing
class MockModel(Base):
    __tablename__ = "mock_model"


# Mock repository for testing
class MockRepository(BaseRepository):
    def __init__(self, model, session):
        super().__init__(model, session)


# Mock service for testing
class MockService(BaseService):
    def __init__(self, session, repository_class=None, model_class=None):
        self.session = session
        self.repository_class = repository_class
        self.model_class = model_class


class TestDependencies:
    """Test cases for core dependencies."""
    
    @pytest.mark.asyncio
    async def test_get_tenant_context_with_oauth2_proxy_headers(self, mock_request):
        """Test tenant context extraction with OAuth2-Proxy headers."""
        with patch("src.core.dependencies.TenantContext") as mock_tenant_context:
            mock_context = MagicMock()
            mock_context.primary_tenant_id = "tenant_123"
            mock_context.tenant_ids = ["tenant_123"]
            mock_context.tenant_email = "user@example.com"
            mock_tenant_context.from_email.return_value = mock_context
            
            result = await get_tenant_context(
                request=mock_request,
                x_forwarded_email=None,
                x_forwarded_access_token=None,
                x_auth_request_email="user@example.com",
                x_auth_request_user="user",
                x_auth_request_access_token="token123"
            )
            
            assert result == mock_context
            mock_tenant_context.from_email.assert_called_once_with(
                email="user@example.com",
                access_token="token123"
            )
    
    @pytest.mark.asyncio
    async def test_get_tenant_context_with_forwarded_headers(self, mock_request):
        """Test tenant context extraction with forwarded headers."""
        with patch("src.core.dependencies.TenantContext") as mock_tenant_context:
            mock_context = MagicMock()
            mock_tenant_context.from_email.return_value = mock_context
            
            result = await get_tenant_context(
                request=mock_request,
                x_forwarded_email="user@example.com",
                x_forwarded_access_token="token123",
                x_auth_request_email=None,
                x_auth_request_user=None,
                x_auth_request_access_token=None
            )
            
            assert result == mock_context
            mock_tenant_context.from_email.assert_called_once_with(
                email="user@example.com",
                access_token="token123"
            )
    
    @pytest.mark.asyncio
    async def test_get_tenant_context_oauth2_proxy_preferred(self, mock_request):
        """Test that OAuth2-Proxy headers are preferred over forwarded headers."""
        with patch("src.core.dependencies.TenantContext") as mock_tenant_context:
            mock_context = MagicMock()
            mock_tenant_context.from_email.return_value = mock_context
            
            result = await get_tenant_context(
                request=mock_request,
                x_forwarded_email="forwarded@example.com",
                x_forwarded_access_token="forwarded_token",
                x_auth_request_email="oauth2@example.com",
                x_auth_request_user="oauth2_user",
                x_auth_request_access_token="oauth2_token"
            )
            
            # Should use OAuth2-Proxy headers
            mock_tenant_context.from_email.assert_called_once_with(
                email="oauth2@example.com",
                access_token="oauth2_token"
            )
    
    @pytest.mark.asyncio
    async def test_get_tenant_context_no_headers(self, mock_request):
        """Test tenant context extraction with no headers."""
        with patch("src.core.dependencies.TenantContext") as mock_tenant_context:
            mock_context = MagicMock()
            mock_tenant_context.return_value = mock_context
            
            result = await get_tenant_context(
                request=mock_request,
                x_forwarded_email=None,
                x_forwarded_access_token=None,
                x_auth_request_email=None,
                x_auth_request_user=None,
                x_auth_request_access_token=None
            )
            
            assert result == mock_context
            mock_tenant_context.assert_called_once_with()
    
    @pytest.mark.asyncio
    async def test_get_group_context_with_oauth2_proxy_headers(self, mock_request):
        """Test group context extraction with OAuth2-Proxy headers."""
        with patch("src.core.dependencies.GroupContext") as mock_group_context:
            mock_context = MagicMock()
            mock_context.primary_group_id = "group_123"
            mock_context.group_ids = ["group_123"]
            mock_context.group_email = "user@example.com"
            mock_group_context.from_email.return_value = mock_context
            
            result = await get_group_context(
                request=mock_request,
                x_forwarded_email=None,
                x_forwarded_access_token=None,
                x_auth_request_email="user@example.com",
                x_auth_request_user="user",
                x_auth_request_access_token="token123"
            )
            
            assert result == mock_context
            mock_group_context.from_email.assert_called_once_with(
                email="user@example.com",
                access_token="token123"
            )
    
    @pytest.mark.asyncio
    async def test_get_group_context_with_forwarded_headers(self, mock_request):
        """Test group context extraction with forwarded headers."""
        with patch("src.core.dependencies.GroupContext") as mock_group_context:
            mock_context = MagicMock()
            mock_group_context.from_email.return_value = mock_context
            
            result = await get_group_context(
                request=mock_request,
                x_forwarded_email="user@example.com",
                x_forwarded_access_token="token123",
                x_auth_request_email=None,
                x_auth_request_user=None,
                x_auth_request_access_token=None
            )
            
            assert result == mock_context
            mock_group_context.from_email.assert_called_once_with(
                email="user@example.com",
                access_token="token123"
            )
    
    @pytest.mark.asyncio
    async def test_get_group_context_no_headers(self, mock_request):
        """Test group context extraction with no headers."""
        with patch("src.core.dependencies.GroupContext") as mock_group_context:
            mock_context = MagicMock()
            mock_group_context.return_value = mock_context
            
            result = await get_group_context(
                request=mock_request,
                x_forwarded_email=None,
                x_forwarded_access_token=None,
                x_auth_request_email=None,
                x_auth_request_user=None,
                x_auth_request_access_token=None
            )
            
            assert result == mock_context
            mock_group_context.assert_called_once_with()
    
    def test_get_repository_factory(self, mock_session):
        """Test repository factory function."""
        repo_factory = get_repository(MockRepository, MockModel)
        
        # Should return a callable
        assert callable(repo_factory)
        
        # Call the factory function
        repository = repo_factory(mock_session)
        
        # Should return a repository instance
        assert isinstance(repository, MockRepository)
        assert repository.model == MockModel
        assert repository.session == mock_session
    
    def test_get_service_factory_simple(self, mock_session):
        """Test service factory function with simple service."""
        service_factory = get_service(MockService, MockRepository, MockModel)
        
        # Should return a callable
        assert callable(service_factory)
        
        # Call the factory function
        service = service_factory(mock_session)
        
        # Should return a service instance
        assert isinstance(service, MockService)
        assert service.session == mock_session
    
    def test_get_service_factory_with_parameters(self, mock_session):
        """Test service factory function with service that requires repository parameters."""
        # Mock a service that requires additional parameters
        class ParameterizedService(BaseService):
            def __init__(self, session, repository_class=None, model_class=None):
                if repository_class is None or model_class is None:
                    raise TypeError("Missing required parameters")
                self.session = session
                self.repository_class = repository_class
                self.model_class = model_class
        
        service_factory = get_service(ParameterizedService, MockRepository, MockModel)
        
        # Call the factory function
        service = service_factory(mock_session)
        
        # Should return a service instance with parameters
        assert isinstance(service, ParameterizedService)
        assert service.session == mock_session
        assert service.repository_class == MockRepository
        assert service.model_class == MockModel
    
    def test_get_service_factory_error_handling(self, mock_session):
        """Test service factory function error handling."""
        # Mock a service that always fails to initialize
        class FailingService(BaseService):
            def __init__(self, *args, **kwargs):
                raise Exception("Always fails")
        
        service_factory = get_service(FailingService, MockRepository, MockModel)
        
        # Should raise the exception
        with pytest.raises(Exception, match="Always fails"):
            service_factory(mock_session)
    
    def test_get_log_service(self):
        """Test log service factory function."""
        with patch("src.core.dependencies.LLMLogRepository") as mock_repo_class, \
             patch("src.core.dependencies.LLMLogService") as mock_service_class:
            
            mock_repository = MagicMock()
            mock_service = MagicMock()
            mock_repo_class.return_value = mock_repository
            mock_service_class.return_value = mock_service
            
            result = get_log_service()
            
            assert result == mock_service
            mock_repo_class.assert_called_once_with()
            mock_service_class.assert_called_once_with(mock_repository)
    
    def test_session_dependency_type(self):
        """Test that SessionDep is properly typed."""
        from src.core.dependencies import SessionDep
        from typing import get_origin, get_args
        
        # SessionDep should be an Annotated type
        assert get_origin(SessionDep) is not None
        args = get_args(SessionDep)
        assert len(args) >= 2
    
    def test_tenant_context_dependency_type(self):
        """Test that TenantContextDep is properly typed."""
        from src.core.dependencies import TenantContextDep
        from typing import get_origin, get_args
        
        # TenantContextDep should be an Annotated type
        assert get_origin(TenantContextDep) is not None
        args = get_args(TenantContextDep)
        assert len(args) >= 2
    
    def test_group_context_dependency_type(self):
        """Test that GroupContextDep is properly typed."""
        from src.core.dependencies import GroupContextDep
        from typing import get_origin, get_args
        
        # GroupContextDep should be an Annotated type
        assert get_origin(GroupContextDep) is not None
        args = get_args(GroupContextDep)
        assert len(args) >= 2
    
    @pytest.mark.asyncio
    async def test_debug_output_tenant_context(self, mock_request, capsys):
        """Test that debug output is printed for tenant context."""
        with patch("src.core.dependencies.TenantContext") as mock_tenant_context:
            mock_context = MagicMock()
            mock_tenant_context.from_email.return_value = mock_context
            mock_tenant_context.return_value = mock_context
            
            await get_tenant_context(
                request=mock_request,
                x_forwarded_email=None,
                x_forwarded_access_token=None,
                x_auth_request_email="user@example.com",
                x_auth_request_user=None,
                x_auth_request_access_token=None
            )
            
            # Check that debug output was printed
            captured = capsys.readouterr()
            assert "DEBUG: get_tenant_context called with:" in captured.out
            assert "X-Auth-Request-Email: user@example.com" in captured.out
    
    @pytest.mark.asyncio
    async def test_debug_output_group_context(self, mock_request, capsys):
        """Test that debug output is printed for group context."""
        with patch("src.core.dependencies.GroupContext") as mock_group_context:
            mock_context = MagicMock()
            mock_group_context.from_email.return_value = mock_context
            mock_group_context.return_value = mock_context
            
            await get_group_context(
                request=mock_request,
                x_forwarded_email=None,
                x_forwarded_access_token=None,
                x_auth_request_email="user@example.com",
                x_auth_request_user=None,
                x_auth_request_access_token=None
            )
            
            # Check that debug output was printed
            captured = capsys.readouterr()
            assert "DEBUG: get_group_context called with:" in captured.out
            assert "X-Auth-Request-Email: user@example.com" in captured.out