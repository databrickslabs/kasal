"""
Unit tests for UCFunctionsRouter.

Tests the functionality of Unity Catalog function management endpoints.
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
    from src.api.uc_functions_router import router
    
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


class TestUCFunctionsRouter:
    """Test cases for Unity Catalog functions endpoints."""
    
    @patch('src.api.uc_functions_router.UCFunctionService')
    def test_list_functions_success(self, mock_service_class, client):
        """Test successful function listing."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.list_functions.return_value = {
            "functions": [
                {
                    "name": "function1", 
                    "comment": "Test function 1",
                    "return_type": "string",
                    "input_params": [],
                    "catalog_name": "test_catalog",
                    "schema_name": "test_schema"
                },
                {
                    "name": "function2", 
                    "comment": "Test function 2",
                    "return_type": "int",
                    "input_params": [],
                    "catalog_name": "test_catalog", 
                    "schema_name": "test_schema"
                }
            ],
            "count": 2,
            "catalog_name": "test_catalog",
            "schema_name": "test_schema"
        }
        
        response = client.get("/uc-functions/list/test_catalog/test_schema")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["functions"]) == 2
        assert data["catalog_name"] == "test_catalog"
    
    @patch('src.api.uc_functions_router.UCFunctionService')
    def test_list_functions_error(self, mock_service_class, client):
        """Test function listing with error."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.list_functions.side_effect = Exception("Database error")
        
        response = client.get("/uc-functions/list/test_catalog/test_schema")
        
        assert response.status_code == 500
        assert "Error listing functions" in response.json()["detail"]
    
    @patch('src.api.uc_functions_router.UCFunctionService')
    def test_get_function_details_success(self, mock_service_class, client):
        """Test successful function details retrieval."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_function.return_value = {
            "function": {
                "name": "test_function",
                "comment": "Test function",
                "return_type": "string",
                "input_params": [],
                "catalog_name": "test_catalog",
                "schema_name": "test_schema"
            },
            "catalog_name": "test_catalog",
            "schema_name": "test_schema"
        }
        
        response = client.get("/uc-functions/details/test_catalog/test_schema/test_function")
        
        assert response.status_code == 200
        data = response.json()
        assert data["function"]["name"] == "test_function"
        assert data["catalog_name"] == "test_catalog"
    
    @patch('src.api.uc_functions_router.UCFunctionService')
    def test_get_function_details_not_found(self, mock_service_class, client):
        """Test function details with function not found."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_function.side_effect = ValueError("Function not found")
        
        response = client.get("/uc-functions/details/test_catalog/test_schema/nonexistent")
        
        assert response.status_code == 404
        assert "Function not found" in response.json()["detail"]
    
    @patch('src.api.uc_functions_router.UCFunctionService')
    def test_get_function_details_error(self, mock_service_class, client):
        """Test function details with general error."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.get_function.side_effect = Exception("Database error")
        
        response = client.get("/uc-functions/details/test_catalog/test_schema/test_function")
        
        assert response.status_code == 500
        assert "Error getting function details" in response.json()["detail"]
    
    @patch('src.api.uc_functions_router.UCFunctionService')
    def test_list_functions_post_success(self, mock_service_class, client):
        """Test successful function listing via POST."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.list_functions.return_value = {
            "functions": [
                {
                    "name": "function1", 
                    "comment": "Test function 1",
                    "return_type": "string",
                    "input_params": [],
                    "catalog_name": "test_catalog",
                    "schema_name": "test_schema"
                }
            ],
            "count": 1,
            "catalog_name": "test_catalog",
            "schema_name": "test_schema"
        }
        
        request_data = {
            "catalog_name": "test_catalog",
            "schema_name": "test_schema"
        }
        
        response = client.post("/uc-functions/list", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["catalog_name"] == "test_catalog"
    
    @patch('src.api.uc_functions_router.UCFunctionService')
    def test_list_functions_post_error(self, mock_service_class, client):
        """Test function listing via POST with error."""
        mock_service = AsyncMock()
        mock_service_class.return_value = mock_service
        mock_service.list_functions.side_effect = Exception("Service error")
        
        request_data = {
            "catalog_name": "test_catalog",
            "schema_name": "test_schema"
        }
        
        response = client.post("/uc-functions/list", json=request_data)
        
        assert response.status_code == 500
        assert "Error listing functions" in response.json()["detail"]