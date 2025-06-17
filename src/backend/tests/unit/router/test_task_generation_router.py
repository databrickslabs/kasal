"""
Unit tests for TaskGenerationRouter.

Tests the functionality of AI-powered task generation endpoints.
"""
import pytest
from unittest.mock import AsyncMock, patch
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi.testclient import TestClient

from src.utils.user_context import GroupContext


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    return GroupContext(
        group_ids=["group-123"],
        group_email="test@example.com", 
        email_domain="example.com",
        user_id="user-123"
    )


@pytest.fixture
def app(mock_group_context):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.task_generation_router import router
    from src.core.dependencies import get_group_context
    
    app = FastAPI()
    app.include_router(router)
    
    async def override_get_group_context():
        return mock_group_context
    
    app.dependency_overrides[get_group_context] = override_get_group_context
    
    return app



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
def client(app):
    """Create a test client."""
    # Override authentication dependencies for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user


    return TestClient(app)


class TestTaskGenerationRouter:
    """Test cases for task generation endpoints."""
    
    @patch('src.api.task_generation_router.TaskGenerationService')
    def test_generate_task_success(self, mock_service_class, client, mock_group_context):
        """Test successful task generation."""
        mock_service = AsyncMock()
        mock_service_class.create.return_value = mock_service
        from src.schemas.task_generation import TaskGenerationResponse, AdvancedConfig
        mock_response = TaskGenerationResponse(
            name="Data Analysis Task",
            description="Analyze customer data",
            expected_output="Analysis report",
            tools=[],
            advanced_config=AdvancedConfig()
        )
        mock_service.generate_task.return_value = mock_response
        
        request_data = {
            "text": "Create a task to analyze customer data"
        }
        
        response = client.post("/task-generation/generate-task", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Data Analysis Task"
        assert "description" in data
    
    @patch('src.api.task_generation_router.TaskGenerationService')
    def test_generate_task_error(self, mock_service_class, client, mock_group_context):
        """Test task generation with error."""
        mock_service = AsyncMock()
        mock_service_class.create.return_value = mock_service
        mock_service.generate_task.side_effect = Exception("Generation failed")
        
        request_data = {"text": "Invalid prompt"}
        
        response = client.post("/task-generation/generate-task", json=request_data)
        
        assert response.status_code == 500