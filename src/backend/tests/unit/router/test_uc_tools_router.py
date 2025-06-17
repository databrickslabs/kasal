"""
Unit tests for UCToolsRouter.

Tests the functionality of Unity Catalog tools management endpoints.
"""
import pytest
from unittest.mock import AsyncMock, patch
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi.testclient import TestClient


@pytest.fixture
def app():
    """Create a FastAPI app."""
    from fastapi import FastAPI
    from src.api.uc_tools_router import router
    
    app = FastAPI()
    app.include_router(router)
    
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


class TestUCToolsRouter:
    """Test cases for Unity Catalog tools endpoints."""
    
    @patch('src.api.uc_tools_router.UCToolService')
    def test_get_uc_tools_success(self, mock_service_class, client):
        """Test successful UC tools retrieval."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_all_uc_tools.return_value = {
            "tools": [
                {
                    "name": "table_reader",
                    "full_name": "catalog1.schema1.table_reader",
                    "catalog": "catalog1",
                    "db_schema": "schema1",
                    "comment": "Read data from Unity Catalog tables",
                    "return_type": "string",
                    "input_params": []
                },
                {
                    "name": "function_executor",
                    "full_name": "catalog2.schema2.function_executor",
                    "catalog": "catalog2",
                    "db_schema": "schema2",
                    "comment": "Execute Unity Catalog functions",
                    "return_type": "int",
                    "input_params": []
                }
            ],
            "count": 2
        }
        
        response = client.get("/uc-tools/")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["tools"]) == 2
        assert data["tools"][0]["name"] == "table_reader"
        assert data["tools"][0]["catalog"] == "catalog1"
    
    @patch('src.api.uc_tools_router.UCToolService')
    def test_get_uc_tools_error(self, mock_service_class, client):
        """Test UC tools retrieval with error."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_all_uc_tools.side_effect = Exception("Service error")
        
        response = client.get("/uc-tools/")
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]
    
    @patch('src.api.uc_tools_router.UCToolService')
    def test_get_uc_tools_empty_response(self, mock_service_class, client):
        """Test UC tools retrieval with empty response."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_all_uc_tools.return_value = {
            "tools": [],
            "count": 0
        }
        
        response = client.get("/uc-tools/")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert len(data["tools"]) == 0