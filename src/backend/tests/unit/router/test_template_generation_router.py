"""
Unit tests for TemplateGenerationRouter.

Tests the functionality of AI-powered template generation endpoints.
"""
import pytest
import json
from unittest.mock import AsyncMock, patch, Mock
from fastapi.testclient import TestClient
from fastapi import HTTPException

from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)
from src.utils.user_context import GroupContext


@pytest.fixture
def app():
    """Create a FastAPI app."""
    from fastapi import FastAPI
    from src.api.template_generation_router import router
    
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
def client(app, mock_current_user):
    """Create a test client."""
    # Override authentication dependencies for testing
    app.dependency_overrides[require_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_authenticated_user] = lambda: mock_current_user
    app.dependency_overrides[get_admin_user] = lambda: mock_current_user

    return TestClient(app)


class TestTemplateGenerationRouter:
    """Test cases for template generation endpoints."""
    
    @patch('src.api.template_generation_router.TemplateGenerationService')
    @patch('src.api.template_generation_router.logger')
    def test_generate_templates_success(self, mock_logger, mock_service_class, client):
        """Test successful template generation."""
        mock_service = AsyncMock()
        mock_service_class.create.return_value = mock_service
        
        expected_response = {
            "system_template": "You are a helpful customer service assistant.",
            "prompt_template": "Please help the customer with their inquiry: {query}",
            "response_template": "Thank you for contacting us. {response}"
        }
        mock_service.generate_templates.return_value = expected_response
        
        request_data = {
            "role": "Customer Service Agent",
            "goal": "Help customers with their inquiries",
            "backstory": "You are an experienced customer service representative",
            "model": "databricks-llama-4-maverick"
        }
        
        response = client.post("/template-generation/generate-templates", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data == expected_response
        
        # Verify service was called correctly
        mock_service_class.create.assert_called_once()
        mock_service.generate_templates.assert_called_once()
        
        # Verify logging
        mock_logger.info.assert_any_call(f"Generating templates for agent role: {request_data['role']}")
        mock_logger.info.assert_any_call(f"Successfully generated templates for agent role: {request_data['role']}")
    
    @patch('src.api.template_generation_router.TemplateGenerationService')
    @patch('src.api.template_generation_router.logger')
    def test_generate_templates_value_error_not_found(self, mock_logger, mock_service_class, client):
        """Test template generation with ValueError containing 'not found in database'."""
        mock_service = AsyncMock()
        mock_service_class.create.return_value = mock_service
        mock_service.generate_templates.side_effect = ValueError("Template not found in database")
        
        request_data = {
            "role": "Test Role",
            "goal": "Test Goal",
            "backstory": "Test Backstory"
        }
        
        response = client.post("/template-generation/generate-templates", json=request_data)
        
        assert response.status_code == 500
        data = response.json()
        assert "Server configuration error" in data["detail"]
        assert "Template not found in database" in data["detail"]
        
        # Verify logging
        mock_logger.error.assert_called_once()
        error_call = mock_logger.error.call_args[0][0]
        assert "Server configuration error" in error_call
    
    @patch('src.api.template_generation_router.TemplateGenerationService')
    @patch('src.api.template_generation_router.logger')
    def test_generate_templates_value_error_other(self, mock_logger, mock_service_class, client):
        """Test template generation with other ValueError."""
        mock_service = AsyncMock()
        mock_service_class.create.return_value = mock_service
        mock_service.generate_templates.side_effect = ValueError("Invalid request data")
        
        request_data = {
            "role": "Test Role",
            "goal": "Test Goal",
            "backstory": "Test Backstory"
        }
        
        response = client.post("/template-generation/generate-templates", json=request_data)
        
        assert response.status_code == 400
        data = response.json()
        assert "Invalid request or response" in data["detail"]
        assert "Invalid request data" in data["detail"]
        
        # Verify logging
        mock_logger.error.assert_called_once()
        error_call = mock_logger.error.call_args[0][0]
        assert "Invalid request or response" in error_call
    
    @patch('src.api.template_generation_router.TemplateGenerationService')
    @patch('src.api.template_generation_router.logger')
    def test_generate_templates_json_decode_error(self, mock_logger, mock_service_class, client):
        """Test template generation with JSONDecodeError (caught as ValueError)."""
        mock_service = AsyncMock()
        mock_service_class.create.return_value = mock_service
        mock_service.generate_templates.side_effect = json.JSONDecodeError("Invalid JSON", "test", 0)
        
        request_data = {
            "role": "Test Role",
            "goal": "Test Goal",
            "backstory": "Test Backstory"
        }
        
        response = client.post("/template-generation/generate-templates", json=request_data)
        
        # JSONDecodeError is handled as a ValueError since it inherits from ValueError
        assert response.status_code == 400
        data = response.json()
        assert "Invalid request or response" in data["detail"]
        
        # Verify logging
        mock_logger.error.assert_called_once()
        error_call = mock_logger.error.call_args[0][0]
        assert "Invalid request or response" in error_call

    @patch('src.api.template_generation_router.TemplateGenerationService')
    @patch('src.api.template_generation_router.logger')
    @patch('src.api.template_generation_router.json')
    def test_generate_templates_direct_json_decode_error(self, mock_json, mock_logger, mock_service_class, client):
        """Test template generation with direct JSONDecodeError handling."""
        mock_service = AsyncMock()
        mock_service_class.create.return_value = mock_service
        
        # Create a custom exception that is JSONDecodeError but not ValueError
        class CustomJSONDecodeError(Exception):
            pass
        
        # Make json.JSONDecodeError point to our custom exception
        mock_json.JSONDecodeError = CustomJSONDecodeError
        mock_service.generate_templates.side_effect = CustomJSONDecodeError("Invalid JSON")
        
        request_data = {
            "role": "Test Role",
            "goal": "Test Goal",
            "backstory": "Test Backstory"
        }
        
        response = client.post("/template-generation/generate-templates", json=request_data)
        
        # Should be handled by the specific JSONDecodeError handler
        assert response.status_code == 500
        data = response.json()
        assert data["detail"] == "Failed to parse AI response as JSON"
        
        # Verify logging
        mock_logger.error.assert_called_once_with("Failed to parse AI response as JSON")
    
    @patch('src.api.template_generation_router.TemplateGenerationService')
    @patch('src.api.template_generation_router.logger')
    def test_generate_templates_generic_exception(self, mock_logger, mock_service_class, client):
        """Test template generation with generic exception."""
        mock_service = AsyncMock()
        mock_service_class.create.return_value = mock_service
        mock_service.generate_templates.side_effect = Exception("Unexpected error")
        
        request_data = {
            "role": "Test Role",
            "goal": "Test Goal",
            "backstory": "Test Backstory"
        }
        
        response = client.post("/template-generation/generate-templates", json=request_data)
        
        assert response.status_code == 500
        data = response.json()
        assert "Error generating templates" in data["detail"]
        assert "Unexpected error" in data["detail"]
        
        # Verify logging
        mock_logger.error.assert_called_once()
        error_call = mock_logger.error.call_args[0][0]
        assert "Error generating templates" in error_call
    
    def test_generate_templates_invalid_request_data(self, client):
        """Test template generation with invalid request data."""
        # Missing required fields
        request_data = {
            "role": "Test Role"
            # Missing goal and backstory
        }
        
        response = client.post("/template-generation/generate-templates", json=request_data)
        
        assert response.status_code == 422  # Validation error
        data = response.json()
        assert "detail" in data
        assert isinstance(data["detail"], list)
    
    def test_generate_templates_empty_request(self, client):
        """Test template generation with empty request."""
        response = client.post("/template-generation/generate-templates", json={})
        
        assert response.status_code == 422  # Validation error
        data = response.json()
        assert "detail" in data
        assert isinstance(data["detail"], list)
    
    @patch('src.api.template_generation_router.TemplateGenerationService')
    def test_generate_templates_with_default_model(self, mock_service_class, client):
        """Test template generation with default model when not specified."""
        mock_service = AsyncMock()
        mock_service_class.create.return_value = mock_service
        
        expected_response = {
            "system_template": "System template",
            "prompt_template": "Prompt template", 
            "response_template": "Response template"
        }
        mock_service.generate_templates.return_value = expected_response
        
        request_data = {
            "role": "Test Role",
            "goal": "Test Goal",
            "backstory": "Test Backstory"
            # model field omitted to test default
        }
        
        response = client.post("/template-generation/generate-templates", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data == expected_response
        
        # Verify the service was called with the request (which should have default model)
        mock_service.generate_templates.assert_called_once()
        call_args = mock_service.generate_templates.call_args[0][0]
        assert call_args.model == "databricks-llama-4-maverick"  # Default from schema
    
    @patch('src.api.template_generation_router.TemplateGenerationService')
    def test_generate_templates_with_custom_model(self, mock_service_class, client):
        """Test template generation with custom model."""
        mock_service = AsyncMock()
        mock_service_class.create.return_value = mock_service
        
        expected_response = {
            "system_template": "System template",
            "prompt_template": "Prompt template",
            "response_template": "Response template"
        }
        mock_service.generate_templates.return_value = expected_response
        
        custom_model = "custom-model-name"
        request_data = {
            "role": "Test Role",
            "goal": "Test Goal", 
            "backstory": "Test Backstory",
            "model": custom_model
        }
        
        response = client.post("/template-generation/generate-templates", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data == expected_response
        
        # Verify the service was called with the custom model
        mock_service.generate_templates.assert_called_once()
        call_args = mock_service.generate_templates.call_args[0][0]
        assert call_args.model == custom_model