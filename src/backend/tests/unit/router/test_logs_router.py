"""
Unit tests for LogsRouter.

Tests the functionality of LLM log management endpoints.
"""
import pytest
from unittest.mock import AsyncMock, patch
from datetime import datetime
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)
from src.utils.user_context import GroupContext
from fastapi.testclient import TestClient


@pytest.fixture
def mock_log_service():
    """Create a mock log service."""
    return AsyncMock()


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    return GroupContext(
        group_ids=["test-group"],
        group_email="test@example.com",
        email_domain="example.com",
        user_id="user-123"
    )


@pytest.fixture
def mock_current_user():
    """Create a mock authenticated user."""
    from src.models.enums import UserRole, UserStatus
    from datetime import datetime
    
    class MockUser:
        def __init__(self):
            self.id = "current-user-123"
            self.username = "testuser"
            self.email = "test@example.com"
            self.role = UserRole.REGULAR
            self.status = UserStatus.ACTIVE
            self.created_at = datetime.utcnow()
            self.updated_at = datetime.utcnow()
    
    return MockUser()


@pytest.fixture
def app(mock_log_service, mock_group_context, mock_current_user):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.logs_router import router
    from src.core.dependencies import get_log_service, get_group_context
    
    app = FastAPI()
    app.include_router(router)
    
    # Override dependencies
    app.dependency_overrides[get_log_service] = lambda: mock_log_service
    app.dependency_overrides[get_group_context] = lambda: mock_group_context
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user
    
    return app


@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)


class TestLogsRouter:
    """Test cases for LLM log endpoints."""
    
    def test_get_llm_logs_success(self, client, mock_log_service):
        """Test successful LLM logs retrieval."""
        # Mock the logs data
        logs_data = [
            {
                "id": 1,
                "endpoint": "chat/completions",
                "prompt": "Test prompt",
                "response": "Test response", 
                "model": "gpt-3.5-turbo",
                "status": "success",
                "tokens_used": 100,
                "duration_ms": 500,
                "error_message": None,
                "extra_data": {},
                "created_at": datetime(2023, 1, 1)
            }
        ]
        mock_log_service.get_logs_paginated_by_group.return_value = logs_data
        
        response = client.get("/llm-logs")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == 1
    
    def test_count_llm_logs_success(self, client, mock_log_service):
        """Test successful LLM logs count."""
        mock_log_service.count_logs_by_group.return_value = 42
        
        response = client.get("/llm-logs/count")
        
        assert response.status_code == 200
        data = response.json()
        assert data == 42
    
    def test_get_unique_endpoints_success(self, client, mock_log_service):
        """Test successful unique endpoints retrieval."""
        mock_log_service.get_unique_endpoints_by_group.return_value = [
            "/endpoint1",
            "/endpoint2",
            "/endpoint3"
        ]
        
        response = client.get("/llm-logs/endpoints")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        assert "/endpoint1" in data
    
    def test_get_log_stats_success(self, client, mock_log_service):
        """Test successful log statistics retrieval."""
        mock_log_service.get_log_stats_by_group.return_value = {
            "total_requests": 100,
            "successful_requests": 95,
            "failed_requests": 5,
            "average_response_time": 150.5,
            "endpoints": {
                "/endpoint1": 60,
                "/endpoint2": 40
            }
        }
        
        response = client.get("/llm-logs/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_requests"] == 100
        assert data["successful_requests"] == 95
        assert "/endpoint1" in data["endpoints"]
    
    def test_get_llm_logs_exception(self, client, mock_log_service):
        """Test exception handling in get_llm_logs endpoint."""
        # Mock service to raise an exception
        mock_log_service.get_logs_paginated_by_group.side_effect = Exception("Service error")
        
        with pytest.raises(Exception) as exc_info:
            response = client.get("/llm-logs")
        
        assert str(exc_info.value) == "Service error"
        mock_log_service.get_logs_paginated_by_group.assert_called_once()
    
    def test_count_llm_logs_exception(self, client, mock_log_service):
        """Test exception handling in count_llm_logs endpoint."""
        # Mock service to raise an exception
        mock_log_service.count_logs_by_group.side_effect = Exception("Count error")
        
        with pytest.raises(Exception) as exc_info:
            response = client.get("/llm-logs/count")
        
        assert str(exc_info.value) == "Count error"
        mock_log_service.count_logs_by_group.assert_called_once()
    
    def test_get_unique_endpoints_exception(self, client, mock_log_service):
        """Test exception handling in get_unique_endpoints endpoint."""
        # Mock service to raise an exception
        mock_log_service.get_unique_endpoints_by_group.side_effect = Exception("Endpoints error")
        
        with pytest.raises(Exception) as exc_info:
            response = client.get("/llm-logs/endpoints")
        
        assert str(exc_info.value) == "Endpoints error"
        mock_log_service.get_unique_endpoints_by_group.assert_called_once()
    
    def test_get_log_stats_exception(self, client, mock_log_service):
        """Test exception handling in get_log_stats endpoint."""
        # Mock service to raise an exception
        mock_log_service.get_log_stats_by_group.side_effect = Exception("Stats error")
        
        with pytest.raises(Exception) as exc_info:
            response = client.get("/llm-logs/stats")
        
        assert str(exc_info.value) == "Stats error"
        mock_log_service.get_log_stats_by_group.assert_called_once()
    
    def test_get_llm_logs_with_params(self, client, mock_log_service, mock_group_context):
        """Test get_llm_logs with query parameters."""
        # Mock the logs data
        logs_data = [
            {
                "id": 1,
                "endpoint": "chat/completions",
                "prompt": "Test prompt",
                "response": "Test response",
                "model": "gpt-3.5-turbo",
                "status": "success",
                "tokens_used": 100,
                "duration_ms": 500,
                "error_message": None,
                "extra_data": {},
                "created_at": datetime(2023, 1, 1)
            }
        ]
        mock_log_service.get_logs_paginated_by_group.return_value = logs_data
        
        # Test with various query parameters
        response = client.get("/llm-logs?page=1&per_page=20&endpoint=chat/completions")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["endpoint"] == "chat/completions"
        
        # Verify service was called with correct parameters
        mock_log_service.get_logs_paginated_by_group.assert_called_once_with(
            1, 20, "chat/completions", mock_group_context
        )
    
    def test_count_llm_logs_with_endpoint_filter(self, client, mock_log_service, mock_group_context):
        """Test count_llm_logs with endpoint filter."""
        mock_log_service.count_logs_by_group.return_value = 15
        
        response = client.get("/llm-logs/count?endpoint=embeddings")
        
        assert response.status_code == 200
        data = response.json()
        assert data == 15
        
        # Verify service was called with correct parameters
        mock_log_service.count_logs_by_group.assert_called_once_with(
            "embeddings", mock_group_context
        )
    
    def test_get_log_stats_with_custom_days(self, client, mock_log_service, mock_group_context):
        """Test get_log_stats with custom days parameter."""
        mock_log_service.get_log_stats_by_group.return_value = {
            "total_requests": 250,
            "successful_requests": 240,
            "failed_requests": 10,
            "average_response_time": 200.0
        }
        
        response = client.get("/llm-logs/stats?days=7")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_requests"] == 250
        assert data["failed_requests"] == 10
        
        # Verify service was called with correct parameters
        mock_log_service.get_log_stats_by_group.assert_called_once_with(
            7, mock_group_context
        )
    
    def test_get_llm_logs_edge_cases(self, client, mock_log_service):
        """Test get_llm_logs with edge case parameters."""
        mock_log_service.get_logs_paginated_by_group.return_value = []
        
        # Test with maximum per_page
        response = client.get("/llm-logs?page=0&per_page=100")
        assert response.status_code == 200
        
        # Test with 'all' endpoint filter
        response = client.get("/llm-logs?endpoint=all")
        assert response.status_code == 200
        
        # Test with minimum page and per_page
        response = client.get("/llm-logs?page=0&per_page=1")
        assert response.status_code == 200
    
    def test_get_log_stats_edge_cases(self, client, mock_log_service):
        """Test get_log_stats with edge case parameters."""
        mock_log_service.get_log_stats_by_group.return_value = {"total_requests": 0}
        
        # Test with minimum days
        response = client.get("/llm-logs/stats?days=1")
        assert response.status_code == 200
        
        # Test with maximum days
        response = client.get("/llm-logs/stats?days=365")
        assert response.status_code == 200