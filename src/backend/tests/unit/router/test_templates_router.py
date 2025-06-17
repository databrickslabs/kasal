"""
Unit tests for TemplatesRouter.

Tests the functionality of prompt template management endpoints including
CRUD operations, bulk operations, and template reset functionality.
"""
import pytest
from unittest.mock import AsyncMock, patch
from datetime import datetime
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi.testclient import TestClient

from src.schemas.template import PromptTemplateCreate, PromptTemplateUpdate


# Mock prompt template model
class MockPromptTemplate:
    def __init__(self, id=1, name="default_agent", description="Default agent template",
                 template="You are a helpful assistant.", is_active=True):
        self.id = id
        self.name = name
        self.description = description
        self.template = template
        self.is_active = is_active
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
    def model_dump(self):
        """Mock model_dump for Pydantic compatibility."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "template": self.template,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@pytest.fixture
def app():
    """Create a FastAPI app with mocked template service."""
    from fastapi import FastAPI
    from src.api.templates_router import router
    
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


@pytest.fixture
def sample_template_create():
    """Create a sample template creation request."""
    return PromptTemplateCreate(
        name="test_template",
        description="Test template for unit tests",
        template="You are a test assistant. {input}",
        is_active=True
    )


@pytest.fixture
def sample_template_update():
    """Create a sample template update request."""
    return PromptTemplateUpdate(
        name="updated_template",
        description="Updated test template",
        template="You are an updated test assistant. {input}",
        is_active=False
    )


class TestHealthCheck:
    """Test cases for health check endpoint."""
    
    def test_health_check_success(self, client):
        """Test successful health check."""
        response = client.get("/templates/health")
        
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}


class TestListTemplates:
    """Test cases for list templates endpoint."""
    
    @patch('src.api.templates_router.TemplateService.find_all_templates')
    def test_list_templates_success(self, mock_find_all, client):
        """Test successful templates listing."""
        templates = [
            MockPromptTemplate(id=1, name="template1"),
            MockPromptTemplate(id=2, name="template2")
        ]
        mock_find_all.return_value = templates
        
        response = client.get("/templates")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["name"] == "template1"
        assert data[1]["name"] == "template2"
    
    @patch('src.api.templates_router.TemplateService.find_all_templates')
    def test_list_templates_empty(self, mock_find_all, client):
        """Test listing templates when none exist."""
        mock_find_all.return_value = []
        
        response = client.get("/templates")
        
        assert response.status_code == 200
        assert response.json() == []
    
    @patch('src.api.templates_router.TemplateService.find_all_templates')
    def test_list_templates_service_error(self, mock_find_all, client):
        """Test listing templates with service error."""
        mock_find_all.side_effect = Exception("Database error")
        
        response = client.get("/templates")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestGetTemplate:
    """Test cases for get template by ID endpoint."""
    
    @patch('src.api.templates_router.TemplateService.get_template_by_id')
    def test_get_template_success(self, mock_get_template, client):
        """Test successful template retrieval by ID."""
        template = MockPromptTemplate()
        mock_get_template.return_value = template
        
        response = client.get("/templates/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 1
        assert data["name"] == "default_agent"
        mock_get_template.assert_called_once_with(1)
    
    @patch('src.api.templates_router.TemplateService.get_template_by_id')
    def test_get_template_not_found(self, mock_get_template, client):
        """Test getting non-existent template."""
        mock_get_template.return_value = None
        
        response = client.get("/templates/999")
        
        assert response.status_code == 404
        assert "Prompt template not found" in response.json()["detail"]
    
    @patch('src.api.templates_router.TemplateService.get_template_by_id')
    def test_get_template_service_error(self, mock_get_template, client):
        """Test getting template with service error."""
        mock_get_template.side_effect = Exception("Database error")
        
        response = client.get("/templates/1")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestGetTemplateByName:
    """Test cases for get template by name endpoint."""
    
    @patch('src.api.templates_router.TemplateService.find_template_by_name')
    def test_get_template_by_name_success(self, mock_find_by_name, client):
        """Test successful template retrieval by name."""
        template = MockPromptTemplate(name="test_template")
        mock_find_by_name.return_value = template
        
        response = client.get("/templates/by-name/test_template")
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "test_template"
        mock_find_by_name.assert_called_once_with("test_template")
    
    @patch('src.api.templates_router.TemplateService.find_template_by_name')
    def test_get_template_by_name_not_found(self, mock_find_by_name, client):
        """Test getting non-existent template by name."""
        mock_find_by_name.return_value = None
        
        response = client.get("/templates/by-name/nonexistent")
        
        assert response.status_code == 404
        assert "Prompt template with name 'nonexistent' not found" in response.json()["detail"]
    
    @patch('src.api.templates_router.TemplateService.find_template_by_name')
    def test_get_template_by_name_service_error(self, mock_find_by_name, client):
        """Test getting template by name with service error."""
        mock_find_by_name.side_effect = Exception("Database error")
        
        response = client.get("/templates/by-name/test_template")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestCreateTemplate:
    """Test cases for create template endpoint."""
    
    @patch('src.api.templates_router.TemplateService.create_new_template')
    def test_create_template_success(self, mock_create, client, sample_template_create):
        """Test successful template creation."""
        created_template = MockPromptTemplate(name="test_template")
        mock_create.return_value = created_template
        
        response = client.post("/templates", json=sample_template_create.model_dump())
        
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "test_template"
        mock_create.assert_called_once()
    
    @patch('src.api.templates_router.TemplateService.create_new_template')
    def test_create_template_name_exists(self, mock_create, client, sample_template_create):
        """Test creating template with existing name."""
        mock_create.side_effect = ValueError("Template name already exists")
        
        response = client.post("/templates", json=sample_template_create.model_dump())
        
        assert response.status_code == 400
        assert "Template name already exists" in response.json()["detail"]
    
    @patch('src.api.templates_router.TemplateService.create_new_template')
    def test_create_template_service_error(self, mock_create, client, sample_template_create):
        """Test creating template with service error."""
        mock_create.side_effect = Exception("Database error")
        
        response = client.post("/templates", json=sample_template_create.model_dump())
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestUpdateTemplate:
    """Test cases for update template endpoint."""
    
    @patch('src.api.templates_router.TemplateService.update_existing_template')
    def test_update_template_success(self, mock_update, client, sample_template_update):
        """Test successful template update."""
        updated_template = MockPromptTemplate(name="updated_template")
        mock_update.return_value = updated_template
        
        response = client.put("/templates/1", json=sample_template_update.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "updated_template"
        mock_update.assert_called_once_with(1, sample_template_update)
    
    @patch('src.api.templates_router.TemplateService.update_existing_template')
    def test_update_template_not_found(self, mock_update, client, sample_template_update):
        """Test updating non-existent template."""
        mock_update.return_value = None
        
        response = client.put("/templates/999", json=sample_template_update.model_dump())
        
        assert response.status_code == 404
        assert "Prompt template not found" in response.json()["detail"]
    
    @patch('src.api.templates_router.TemplateService.update_existing_template')
    def test_update_template_name_conflict(self, mock_update, client, sample_template_update):
        """Test updating template with name conflict."""
        mock_update.side_effect = ValueError("Template name already exists")
        
        response = client.put("/templates/1", json=sample_template_update.model_dump())
        
        assert response.status_code == 400
        assert "Template name already exists" in response.json()["detail"]
    
    @patch('src.api.templates_router.TemplateService.update_existing_template')
    def test_update_template_service_error(self, mock_update, client, sample_template_update):
        """Test updating template with service error."""
        mock_update.side_effect = Exception("Database error")
        
        response = client.put("/templates/1", json=sample_template_update.model_dump())
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestDeleteTemplate:
    """Test cases for delete template endpoint."""
    
    @patch('src.api.templates_router.TemplateService.delete_template_by_id')
    def test_delete_template_success(self, mock_delete, client):
        """Test successful template deletion."""
        mock_delete.return_value = True
        
        response = client.delete("/templates/1")
        
        assert response.status_code == 200
        data = response.json()
        assert "deleted successfully" in data["message"]
        mock_delete.assert_called_once_with(1)
    
    @patch('src.api.templates_router.TemplateService.delete_template_by_id')
    def test_delete_template_not_found(self, mock_delete, client):
        """Test deleting non-existent template."""
        mock_delete.return_value = False
        
        response = client.delete("/templates/999")
        
        assert response.status_code == 404
        assert "Prompt template not found" in response.json()["detail"]
    
    @patch('src.api.templates_router.TemplateService.delete_template_by_id')
    def test_delete_template_service_error(self, mock_delete, client):
        """Test deleting template with service error."""
        mock_delete.side_effect = Exception("Database error")
        
        response = client.delete("/templates/1")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestDeleteAllTemplates:
    """Test cases for delete all templates endpoint."""
    
    @patch('src.api.templates_router.TemplateService.delete_all_templates_service')
    def test_delete_all_templates_success(self, mock_delete_all, client):
        """Test successful deletion of all templates."""
        mock_delete_all.return_value = 5
        
        response = client.delete("/templates")
        
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "All prompt templates deleted successfully"
        assert data["deleted_count"] == 5
    
    @patch('src.api.templates_router.TemplateService.delete_all_templates_service')
    def test_delete_all_templates_none_exist(self, mock_delete_all, client):
        """Test deleting all templates when none exist."""
        mock_delete_all.return_value = 0
        
        response = client.delete("/templates")
        
        assert response.status_code == 200
        data = response.json()
        assert data["deleted_count"] == 0
    
    @patch('src.api.templates_router.TemplateService.delete_all_templates_service')
    def test_delete_all_templates_service_error(self, mock_delete_all, client):
        """Test deleting all templates with service error."""
        mock_delete_all.side_effect = Exception("Database error")
        
        response = client.delete("/templates")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestResetTemplates:
    """Test cases for reset templates endpoint."""
    
    @patch('src.api.templates_router.TemplateService.reset_templates_service')
    def test_reset_templates_success(self, mock_reset, client):
        """Test successful template reset."""
        mock_reset.return_value = 3
        
        response = client.post("/templates/reset")
        
        assert response.status_code == 200
        data = response.json()
        assert "Reset 3 prompt templates" in data["message"]
        assert data["reset_count"] == 3
    
    @patch('src.api.templates_router.TemplateService.reset_templates_service')
    def test_reset_templates_none_to_reset(self, mock_reset, client):
        """Test resetting templates when none need reset."""
        mock_reset.return_value = 0
        
        response = client.post("/templates/reset")
        
        assert response.status_code == 200
        data = response.json()
        assert data["reset_count"] == 0
    
    @patch('src.api.templates_router.TemplateService.reset_templates_service')
    def test_reset_templates_service_error(self, mock_reset, client):
        """Test resetting templates with service error."""
        mock_reset.side_effect = Exception("Database error")
        
        response = client.post("/templates/reset")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]