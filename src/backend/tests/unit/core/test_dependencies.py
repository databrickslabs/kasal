"""
Unit tests for dependencies module.

Tests the functionality of dependency injection factories and
group context extraction.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from typing import Optional

from fastapi import Request, Header
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.dependencies import (
    get_group_context,
    get_repository,
    get_service,
    get_log_service,
    GroupContextDep,
    SessionDep
)
from src.core.base_repository import BaseRepository
from src.core.base_service import BaseService
from src.db.base import Base
from src.utils.user_context import GroupContext
from src.services.log_service import LLMLogService
from src.repositories.log_repository import LLMLogRepository


# Mock model class
class MockModel:
    """Mock model that doesn't inherit from Base to avoid SQLAlchemy issues."""
    __tablename__ = "mock_model"
    __name__ = "MockModel"


# Mock repository class
class MockRepository(BaseRepository):
    def __init__(self, model_class, session):
        self.model_class = model_class
        self.session = session


# Mock service class
class MockService(BaseService):
    model_class = MockModel
    repository_class = MockRepository
    
    def __init__(self, session, repository_class=None, model_class=None):
        self.session = session
        self.repository_class = repository_class or self.repository_class
        self.model_class = model_class or self.model_class


# Mock service that only accepts session
class SimpleService(BaseService):
    def __init__(self, session):
        self.session = session


# Mock service that raises exception
class BrokenService(BaseService):
    def __init__(self, *args, **kwargs):
        raise ValueError("Service initialization failed")


@pytest.fixture
def mock_request():
    """Create a mock FastAPI request."""
    request = MagicMock(spec=Request)
    return request


@pytest.fixture
def mock_session():
    """Create a mock async session."""
    return AsyncMock(spec=AsyncSession)


class TestGetGroupContext:
    """Test cases for get_group_context dependency."""
    
    @pytest.mark.asyncio
    async def test_get_group_context_with_auth_request_email(self, mock_request):
        """Test group context extraction with OAuth2-Proxy headers."""
        # Mock GroupContext.from_email
        mock_group_context = MagicMock(spec=GroupContext)
        mock_group_context.primary_group_id = "group-123"
        mock_group_context.group_ids = ["group-123", "group-456"]
        mock_group_context.group_email = "test@example.com"
        
        with patch('src.utils.user_context.GroupContext.from_email', 
                   return_value=mock_group_context) as mock_from_email:
            result = await get_group_context(
                request=mock_request,
                x_forwarded_email="old@example.com",
                x_forwarded_access_token="old-token",
                x_auth_request_email="test@example.com",
                x_auth_request_user="testuser",
                x_auth_request_access_token="auth-token"
            )
            
            # Verify OAuth2-Proxy headers are preferred
            mock_from_email.assert_called_once_with(
                email="test@example.com",
                access_token="auth-token"
            )
            assert result == mock_group_context
    
    @pytest.mark.asyncio
    async def test_get_group_context_with_forwarded_email_fallback(self, mock_request):
        """Test group context extraction with Databricks Apps headers as fallback."""
        # Mock GroupContext.from_email
        mock_group_context = MagicMock(spec=GroupContext)
        mock_group_context.primary_group_id = "group-789"
        mock_group_context.group_ids = ["group-789"]
        mock_group_context.group_email = "fallback@example.com"
        
        with patch('src.utils.user_context.GroupContext.from_email', 
                   return_value=mock_group_context) as mock_from_email:
            result = await get_group_context(
                request=mock_request,
                x_forwarded_email="fallback@example.com",
                x_forwarded_access_token="fallback-token",
                x_auth_request_email=None,
                x_auth_request_user=None,
                x_auth_request_access_token=None
            )
            
            # Verify fallback headers are used
            mock_from_email.assert_called_once_with(
                email="fallback@example.com",
                access_token="fallback-token"
            )
            assert result == mock_group_context
    
    @pytest.mark.asyncio
    async def test_get_group_context_no_email(self, mock_request):
        """Test group context when no email headers are provided."""
        with patch('src.core.dependencies.GroupContext') as mock_group_context_class:
            mock_empty_context = MagicMock(spec=GroupContext)
            mock_group_context_class.return_value = mock_empty_context
            
            result = await get_group_context(
                request=mock_request,
                x_forwarded_email=None,
                x_forwarded_access_token=None,
                x_auth_request_email=None,
                x_auth_request_user=None,
                x_auth_request_access_token=None
            )
            
            # Verify empty context is returned
            mock_group_context_class.assert_called_once_with()
            assert result == mock_empty_context
    
    @pytest.mark.asyncio
    async def test_get_group_context_with_partial_headers(self, mock_request):
        """Test group context with email but no access token."""
        # Mock GroupContext.from_email
        mock_group_context = MagicMock(spec=GroupContext)
        mock_group_context.primary_group_id = "group-partial"
        mock_group_context.group_ids = ["group-partial"]
        mock_group_context.group_email = "partial@example.com"
        
        with patch('src.utils.user_context.GroupContext.from_email', 
                   return_value=mock_group_context) as mock_from_email:
            result = await get_group_context(
                request=mock_request,
                x_forwarded_email=None,
                x_forwarded_access_token=None,
                x_auth_request_email="partial@example.com",
                x_auth_request_user=None,
                x_auth_request_access_token=None
            )
            
            # Verify called with email only
            mock_from_email.assert_called_once_with(
                email="partial@example.com",
                access_token=None
            )
            assert result == mock_group_context


class TestGetRepository:
    """Test cases for get_repository factory."""
    
    def test_get_repository_returns_callable(self):
        """Test that get_repository returns a callable dependency."""
        repo_factory = get_repository(MockRepository, MockModel)
        
        assert callable(repo_factory)
    
    def test_get_repository_creates_instance(self, mock_session):
        """Test that the factory creates a repository instance."""
        repo_factory = get_repository(MockRepository, MockModel)
        
        # Call the factory with a session
        repository = repo_factory(mock_session)
        
        assert isinstance(repository, MockRepository)
        assert repository.model_class == MockModel
        assert repository.session == mock_session
    
    def test_get_repository_with_different_classes(self, mock_session):
        """Test factory with different repository and model classes."""
        # Create another mock model
        class AnotherModel:
            __tablename__ = "another_model"
            __name__ = "AnotherModel"
        
        repo_factory = get_repository(MockRepository, AnotherModel)
        repository = repo_factory(mock_session)
        
        assert repository.model_class == AnotherModel


class TestGetService:
    """Test cases for get_service factory."""
    
    def test_get_service_returns_callable(self):
        """Test that get_service returns a callable dependency."""
        service_factory = get_service(MockService, MockRepository, MockModel)
        
        assert callable(service_factory)
    
    def test_get_service_creates_simple_instance(self, mock_session):
        """Test service creation with session-only constructor."""
        service_factory = get_service(SimpleService, MockRepository, MockModel)
        
        # Call the factory with a session
        service = service_factory(mock_session)
        
        assert isinstance(service, SimpleService)
        assert service.session == mock_session
    
    def test_get_service_creates_complex_instance(self, mock_session):
        """Test service creation that falls back to complex constructor."""
        # Mock a service that raises exception on simple init
        class ComplexService(BaseService):
            def __init__(self, session, repository_class=None, model_class=None):
                if repository_class is None or model_class is None:
                    raise TypeError("Repository and model required")
                self.session = session
                self.repository_class = repository_class
                self.model_class = model_class
        
        service_factory = get_service(ComplexService, MockRepository, MockModel)
        
        # Patch the first service creation to fail
        with patch.object(ComplexService, '__init__', side_effect=[
            TypeError("Repository and model required"),
            None  # Second call succeeds
        ]) as mock_init:
            service = service_factory(mock_session)
            
            # Verify it was called twice - once simple, once with full params
            assert mock_init.call_count == 2
    
    def test_get_service_handles_complete_failure(self, mock_session):
        """Test service creation when both attempts fail."""
        service_factory = get_service(BrokenService, MockRepository, MockModel)
        
        with pytest.raises(ValueError, match="Service initialization failed"):
            service_factory(mock_session)
    
    def test_get_service_logs_error(self, mock_session, caplog):
        """Test that service creation errors are logged."""
        # Create a service that fails on all attempts
        class FailingService(BaseService):
            attempt = 0
            
            def __init__(self, *args, **kwargs):
                FailingService.attempt += 1
                if FailingService.attempt == 1:
                    raise TypeError("First attempt failed")
                else:
                    raise RuntimeError("Second attempt failed")
        
        service_factory = get_service(FailingService, MockRepository, MockModel)
        
        with pytest.raises(RuntimeError, match="Second attempt failed"):
            service_factory(mock_session)
        
        # Check that error was logged
        assert "Error creating service: Second attempt failed" in caplog.text


class TestGetLogService:
    """Test cases for get_log_service factory."""
    
    def test_get_log_service_creates_singleton(self):
        """Test that get_log_service creates a service instance."""
        with patch('src.core.dependencies.LLMLogRepository') as mock_repo_class:
            with patch('src.core.dependencies.LLMLogService') as mock_service_class:
                mock_repo_instance = MagicMock()
                mock_service_instance = MagicMock()
                
                mock_repo_class.return_value = mock_repo_instance
                mock_service_class.return_value = mock_service_instance
                
                result = get_log_service()
                
                # Verify repository was created
                mock_repo_class.assert_called_once_with()
                
                # Verify service was created with repository
                mock_service_class.assert_called_once_with(mock_repo_instance)
                
                assert result == mock_service_instance
    
    def test_get_log_service_returns_same_type(self):
        """Test that get_log_service returns LLMLogService instance."""
        # Mock the actual imports to avoid dependencies
        mock_repo = MagicMock(spec=LLMLogRepository)
        mock_service = MagicMock(spec=LLMLogService)
        
        with patch('src.core.dependencies.LLMLogRepository', return_value=mock_repo):
            with patch('src.core.dependencies.LLMLogService', return_value=mock_service):
                result = get_log_service()
                
                # Verify it returns the service instance
                assert result == mock_service