"""
Unit tests for ApiKeysRouter.

Tests the functionality of API key management endpoints including
CRUD operations for API keys.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import logging
from datetime import datetime
from src.dependencies.admin_auth import (
    require_authenticated_user, get_authenticated_user, get_admin_user
)

from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.schemas.api_key import ApiKeyCreate, ApiKeyUpdate


# Mock API key model
class MockApiKey:
    def __init__(self, id=123, name="test-key", description="Test API Key"):
        self.id = id
        self.name = name
        self.description = description
        self.value = ""  # Empty for security
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
    def model_dump(self):
        """Mock model_dump for Pydantic compatibility."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "value": self.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@pytest.fixture
def mock_api_key_service():
    """Create a mock API key service."""
    service = AsyncMock()
    return service


@pytest.fixture
def app(mock_api_key_service):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.api_keys_router import router, get_api_key_service
    
    app = FastAPI()
    app.include_router(router)
    
    # Override dependency
    app.dependency_overrides[get_api_key_service] = lambda: mock_api_key_service
    
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


@pytest.fixture
def sample_api_key_create():
    """Create a sample API key creation request."""
    return ApiKeyCreate(
        name="new-api-key",
        description="New API Key for testing",
        value="sk-test-1234567890"
    )


@pytest.fixture
def sample_api_key_update():
    """Create a sample API key update request."""
    return ApiKeyUpdate(
        description="Updated API Key description",
        value="sk-test-updated-1234567890"
    )


class TestGetApiKeysMetadata:
    """Test cases for get API keys metadata endpoint."""
    
    def test_get_api_keys_metadata_success(self, client, mock_api_key_service):
        """Test successful retrieval of API keys metadata."""
        api_keys = [
            MockApiKey(id=1, name="openai-key"),
            MockApiKey(id=2, name="anthropic-key")
        ]
        mock_api_key_service.get_api_keys_metadata.return_value = api_keys
        
        response = client.get("/api-keys")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["name"] == "openai-key"
        assert data[1]["name"] == "anthropic-key"
        assert all(key["value"] == "" for key in data)  # Values should be empty
    
    def test_get_api_keys_metadata_empty_list(self, client, mock_api_key_service):
        """Test getting API keys metadata when no keys exist."""
        mock_api_key_service.get_api_keys_metadata.return_value = []
        
        response = client.get("/api-keys")
        
        assert response.status_code == 200
        assert response.json() == []
    
    def test_get_api_keys_metadata_service_error(self, client, mock_api_key_service):
        """Test getting API keys metadata with service error."""
        mock_api_key_service.get_api_keys_metadata.side_effect = Exception("Service error")
        
        response = client.get("/api-keys")
        
        assert response.status_code == 500
        assert "Service error" in response.json()["detail"]
    
    def test_get_api_keys_metadata_logging(self, client, mock_api_key_service, caplog):
        """Test that getting API keys metadata logs errors properly."""
        mock_api_key_service.get_api_keys_metadata.side_effect = Exception("Database error")
        
        with caplog.at_level(logging.ERROR):
            response = client.get("/api-keys")
        
        assert response.status_code == 500
        assert "Error getting API keys metadata: Database error" in caplog.text


class TestCreateApiKey:
    """Test cases for create API key endpoint."""
    
    def test_create_api_key_success(self, client, mock_api_key_service, sample_api_key_create):
        """Test successful API key creation."""
        mock_api_key_service.find_by_name.return_value = None  # No existing key
        created_key = MockApiKey(name=sample_api_key_create.name)
        mock_api_key_service.create_api_key.return_value = created_key
        
        response = client.post("/api-keys", json=sample_api_key_create.model_dump())
        
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "new-api-key"
        mock_api_key_service.create_api_key.assert_called_once()
    
    def test_create_api_key_already_exists(self, client, mock_api_key_service, sample_api_key_create):
        """Test creating API key that already exists."""
        existing_key = MockApiKey(name=sample_api_key_create.name)
        mock_api_key_service.find_by_name.return_value = existing_key
        
        response = client.post("/api-keys", json=sample_api_key_create.model_dump())
        
        assert response.status_code == 400
        assert f"API key with name '{sample_api_key_create.name}' already exists" in response.json()["detail"]
        mock_api_key_service.create_api_key.assert_not_called()
    
    def test_create_api_key_service_error(self, client, mock_api_key_service, sample_api_key_create):
        """Test creating API key with service error."""
        mock_api_key_service.find_by_name.return_value = None
        mock_api_key_service.create_api_key.side_effect = Exception("Database error")
        
        response = client.post("/api-keys", json=sample_api_key_create.model_dump())
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_create_api_key_find_error(self, client, mock_api_key_service, sample_api_key_create):
        """Test creating API key with find operation error."""
        mock_api_key_service.find_by_name.side_effect = Exception("Find error")
        
        response = client.post("/api-keys", json=sample_api_key_create.model_dump())
        
        assert response.status_code == 500
        assert "Find error" in response.json()["detail"]
    
    def test_create_api_key_http_exception_reraise(self, client, mock_api_key_service, sample_api_key_create):
        """Test that HTTPExceptions are re-raised in create endpoint."""
        mock_api_key_service.find_by_name.return_value = None
        mock_api_key_service.create_api_key.side_effect = HTTPException(
            status_code=422, detail="Validation error"
        )
        
        response = client.post("/api-keys", json=sample_api_key_create.model_dump())
        
        assert response.status_code == 422
        assert "Validation error" in response.json()["detail"]
    
    def test_create_api_key_logging_error(self, client, mock_api_key_service, sample_api_key_create, caplog):
        """Test that create API key logs errors properly."""
        mock_api_key_service.find_by_name.return_value = None
        mock_api_key_service.create_api_key.side_effect = Exception("Create error")
        
        with caplog.at_level(logging.ERROR):
            response = client.post("/api-keys", json=sample_api_key_create.model_dump())
        
        assert response.status_code == 500
        assert "Error creating API key: Create error" in caplog.text


class TestUpdateApiKey:
    """Test cases for update API key endpoint."""
    
    def test_update_api_key_success(self, client, mock_api_key_service, sample_api_key_update):
        """Test successful API key update."""
        existing_key = MockApiKey(name="test-key")
        mock_api_key_service.find_by_name.return_value = existing_key
        
        updated_key = MockApiKey(name="test-key", description=sample_api_key_update.description)
        mock_api_key_service.update_api_key.return_value = updated_key
        
        response = client.put("/api-keys/test-key", json=sample_api_key_update.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "test-key"
        assert data["description"] == sample_api_key_update.description
    
    def test_update_api_key_not_found(self, client, mock_api_key_service, sample_api_key_update):
        """Test updating non-existent API key."""
        mock_api_key_service.find_by_name.return_value = None
        
        response = client.put("/api-keys/nonexistent", json=sample_api_key_update.model_dump())
        
        assert response.status_code == 404
        assert "API key 'nonexistent' not found" in response.json()["detail"]
    
    def test_update_api_key_update_failed(self, client, mock_api_key_service, sample_api_key_update):
        """Test API key update when update operation returns None."""
        existing_key = MockApiKey(name="test-key")
        mock_api_key_service.find_by_name.return_value = existing_key
        mock_api_key_service.update_api_key.return_value = None
        
        response = client.put("/api-keys/test-key", json=sample_api_key_update.model_dump())
        
        assert response.status_code == 404
        assert "API key 'test-key' update failed" in response.json()["detail"]
    
    def test_update_api_key_service_error(self, client, mock_api_key_service, sample_api_key_update):
        """Test updating API key with service error."""
        existing_key = MockApiKey(name="test-key")
        mock_api_key_service.find_by_name.return_value = existing_key
        mock_api_key_service.update_api_key.side_effect = Exception("Update error")
        
        response = client.put("/api-keys/test-key", json=sample_api_key_update.model_dump())
        
        assert response.status_code == 500
        assert "Update error" in response.json()["detail"]
    
    def test_update_api_key_logging(self, client, mock_api_key_service, sample_api_key_update, caplog):
        """Test that API key update logs properly."""
        existing_key = MockApiKey(name="test-key")
        mock_api_key_service.find_by_name.return_value = existing_key
        updated_key = MockApiKey(name="test-key")
        mock_api_key_service.update_api_key.return_value = updated_key
        
        with caplog.at_level(logging.INFO):
            response = client.put("/api-keys/test-key", json=sample_api_key_update.model_dump())
        
        assert response.status_code == 200
        assert "Attempting to update API key: test-key" in caplog.text
        assert "API key updated successfully: test-key" in caplog.text
    
    def test_update_api_key_http_exception_reraise(self, client, mock_api_key_service, sample_api_key_update):
        """Test that HTTPExceptions are re-raised in update endpoint."""
        existing_key = MockApiKey(name="test-key")
        mock_api_key_service.find_by_name.return_value = existing_key
        mock_api_key_service.update_api_key.side_effect = HTTPException(
            status_code=422, detail="Validation error"
        )
        
        response = client.put("/api-keys/test-key", json=sample_api_key_update.model_dump())
        
        assert response.status_code == 422
        assert "Validation error" in response.json()["detail"]
    
    def test_update_api_key_error_logging_not_found(self, client, mock_api_key_service, sample_api_key_update, caplog):
        """Test error logging when API key is not found."""
        mock_api_key_service.find_by_name.return_value = None
        
        with caplog.at_level(logging.ERROR):
            response = client.put("/api-keys/nonexistent", json=sample_api_key_update.model_dump())
        
        assert response.status_code == 404
        assert "API key 'nonexistent' not found" in caplog.text
    
    def test_update_api_key_error_logging_update_failed(self, client, mock_api_key_service, sample_api_key_update, caplog):
        """Test error logging when update operation fails."""
        existing_key = MockApiKey(name="test-key")
        mock_api_key_service.find_by_name.return_value = existing_key
        mock_api_key_service.update_api_key.return_value = None
        
        with caplog.at_level(logging.ERROR):
            response = client.put("/api-keys/test-key", json=sample_api_key_update.model_dump())
        
        assert response.status_code == 404
        assert "API key 'test-key' update failed" in caplog.text
    
    def test_update_api_key_find_error(self, client, mock_api_key_service, sample_api_key_update):
        """Test updating API key with find operation error."""
        mock_api_key_service.find_by_name.side_effect = Exception("Find error")
        
        response = client.put("/api-keys/test-key", json=sample_api_key_update.model_dump())
        
        assert response.status_code == 500
        assert "Find error" in response.json()["detail"]


class TestDeleteApiKey:
    """Test cases for delete API key endpoint."""
    
    def test_delete_api_key_success(self, client, mock_api_key_service):
        """Test successful API key deletion."""
        existing_key = MockApiKey(name="test-key")
        mock_api_key_service.find_by_name.return_value = existing_key
        mock_api_key_service.delete_api_key.return_value = True
        
        response = client.delete("/api-keys/test-key")
        
        assert response.status_code == 204
    
    def test_delete_api_key_not_found(self, client, mock_api_key_service):
        """Test deleting non-existent API key."""
        mock_api_key_service.find_by_name.return_value = None
        
        response = client.delete("/api-keys/nonexistent")
        
        assert response.status_code == 404
        assert "API key 'nonexistent' not found" in response.json()["detail"]
    
    def test_delete_api_key_delete_failed(self, client, mock_api_key_service):
        """Test API key deletion when delete operation returns False."""
        existing_key = MockApiKey(name="test-key")
        mock_api_key_service.find_by_name.return_value = existing_key
        mock_api_key_service.delete_api_key.return_value = False
        
        response = client.delete("/api-keys/test-key")
        
        assert response.status_code == 404
        assert "API key 'test-key' not found" in response.json()["detail"]
    
    def test_delete_api_key_service_error(self, client, mock_api_key_service):
        """Test deleting API key with service error."""
        existing_key = MockApiKey(name="test-key")
        mock_api_key_service.find_by_name.return_value = existing_key
        mock_api_key_service.delete_api_key.side_effect = Exception("Delete error")
        
        response = client.delete("/api-keys/test-key")
        
        assert response.status_code == 500
        assert "Delete error" in response.json()["detail"]
    
    def test_delete_api_key_find_error(self, client, mock_api_key_service):
        """Test deleting API key with find operation error."""
        mock_api_key_service.find_by_name.side_effect = Exception("Find error")
        
        response = client.delete("/api-keys/test-key")
        
        assert response.status_code == 500
        assert "Find error" in response.json()["detail"]
    
    def test_delete_api_key_logging(self, client, mock_api_key_service, caplog):
        """Test that API key deletion logs errors properly."""
        existing_key = MockApiKey(name="test-key")
        mock_api_key_service.find_by_name.return_value = existing_key
        mock_api_key_service.delete_api_key.side_effect = Exception("Delete failed")
        
        with caplog.at_level(logging.ERROR):
            response = client.delete("/api-keys/test-key")
        
        assert response.status_code == 500
        assert "Error deleting API key: Delete failed" in caplog.text
    
    def test_delete_api_key_http_exception_reraise(self, client, mock_api_key_service):
        """Test that HTTPExceptions are re-raised in delete endpoint."""
        existing_key = MockApiKey(name="test-key")
        mock_api_key_service.find_by_name.return_value = existing_key
        mock_api_key_service.delete_api_key.side_effect = HTTPException(
            status_code=422, detail="Validation error"
        )
        
        response = client.delete("/api-keys/test-key")
        
        assert response.status_code == 422
        assert "Validation error" in response.json()["detail"]


class TestGetApiKeyService:
    """Test cases for the get_api_key_service dependency function."""
    
    def test_get_api_key_service_returns_service_instance(self):
        """Test that get_api_key_service returns ApiKeysService instance."""
        from src.api.api_keys_router import get_api_key_service
        from src.services.api_keys_service import ApiKeysService
        
        # Mock the session dependency
        mock_session = MagicMock()
        
        # Call the dependency function
        service = get_api_key_service(mock_session)
        
        # Verify it returns an ApiKeysService instance
        assert isinstance(service, ApiKeysService)
        assert service.session == mock_session
    
    @patch('src.api.api_keys_router.ApiKeysService')
    def test_get_api_key_service_dependency_injection(self, mock_service_class):
        """Test that get_api_key_service properly injects session dependency."""
        from src.api.api_keys_router import get_api_key_service
        
        mock_session = MagicMock()
        mock_service_instance = MagicMock()
        mock_service_class.return_value = mock_service_instance
        
        # Call the dependency function
        result = get_api_key_service(mock_session)
        
        # Verify ApiKeysService was called with the session
        mock_service_class.assert_called_once_with(mock_session)
        assert result == mock_service_instance


class TestApiKeysRouterConfiguration:
    """Test cases for router configuration and setup."""
    
    def test_router_configuration(self):
        """Test that the router is configured correctly."""
        from src.api.api_keys_router import router
        
        assert router.prefix == "/api-keys"
        assert "api-keys" in router.tags
        assert 404 in router.responses
        assert router.responses[404]["description"] == "Not found"
    
    def test_logger_setup(self):
        """Test that logger is properly configured."""
        from src.api.api_keys_router import logger
        
        assert logger.name == "src.api.api_keys_router"
        assert isinstance(logger, logging.Logger)
    
    def test_module_imports(self):
        """Test that all necessary imports are accessible."""
        from src.api.api_keys_router import (
            router, logger, get_api_key_service, 
            get_api_keys_metadata, create_api_key, 
            update_api_key, delete_api_key
        )
        
        # Verify all imports are callable/accessible
        assert callable(get_api_key_service)
        assert callable(get_api_keys_metadata)
        assert callable(create_api_key)
        assert callable(update_api_key)
        assert callable(delete_api_key)


class TestErrorHandlingEdgeCases:
    """Test cases for various error handling edge cases."""
    
    def test_get_api_keys_metadata_http_exception_converted_to_500(self, client, mock_api_key_service):
        """Test that HTTPExceptions are converted to 500 errors in get metadata endpoint."""
        mock_api_key_service.get_api_keys_metadata.side_effect = HTTPException(
            status_code=403, detail="Forbidden"
        )
        
        response = client.get("/api-keys")
        
        assert response.status_code == 500
        assert "403: Forbidden" in response.json()["detail"]
    
    def test_create_api_key_existing_key_http_exception(self, client, mock_api_key_service, sample_api_key_create):
        """Test HTTPException when creating API key that already exists (covers line coverage)."""
        existing_key = MockApiKey(name=sample_api_key_create.name)
        mock_api_key_service.find_by_name.return_value = existing_key
        
        response = client.post("/api-keys", json=sample_api_key_create.model_dump())
        
        assert response.status_code == 400
        detail = response.json()["detail"]
        assert f"API key with name '{sample_api_key_create.name}' already exists" in detail
        
        # Verify the specific HTTPException was raised (this covers the HTTPException path)
        mock_api_key_service.create_api_key.assert_not_called()
    
    def test_update_api_key_not_found_http_exception(self, client, mock_api_key_service, sample_api_key_update):
        """Test HTTPException when updating non-existent API key (covers line coverage)."""
        mock_api_key_service.find_by_name.return_value = None
        
        response = client.put("/api-keys/nonexistent", json=sample_api_key_update.model_dump())
        
        assert response.status_code == 404
        detail = response.json()["detail"]
        assert "API key 'nonexistent' not found" in detail
    
    def test_update_api_key_update_failed_http_exception(self, client, mock_api_key_service, sample_api_key_update):
        """Test HTTPException when update operation fails (covers line coverage)."""
        existing_key = MockApiKey(name="test-key")
        mock_api_key_service.find_by_name.return_value = existing_key
        mock_api_key_service.update_api_key.return_value = None
        
        response = client.put("/api-keys/test-key", json=sample_api_key_update.model_dump())
        
        assert response.status_code == 404
        detail = response.json()["detail"]
        assert "API key 'test-key' update failed" in detail
    
    def test_delete_api_key_not_found_http_exception(self, client, mock_api_key_service):
        """Test HTTPException when deleting non-existent API key (covers line coverage)."""
        mock_api_key_service.find_by_name.return_value = None
        
        response = client.delete("/api-keys/nonexistent")
        
        assert response.status_code == 404
        detail = response.json()["detail"]
        assert "API key 'nonexistent' not found" in detail
    
    def test_delete_api_key_delete_failed_http_exception(self, client, mock_api_key_service):
        """Test HTTPException when delete operation returns False (covers line coverage)."""
        existing_key = MockApiKey(name="test-key")
        mock_api_key_service.find_by_name.return_value = existing_key
        mock_api_key_service.delete_api_key.return_value = False
        
        response = client.delete("/api-keys/test-key")
        
        assert response.status_code == 404
        detail = response.json()["detail"]
        assert "API key 'test-key' not found" in detail