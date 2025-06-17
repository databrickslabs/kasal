"""
Comprehensive unit tests for models_router.py to achieve 100% coverage.

Tests all functionality of model configuration management endpoints including
CRUD operations, bulk enable/disable operations, and dependency functions.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
from fastapi import HTTPException, status
from fastapi.testclient import TestClient

from src.schemas.model_config import ModelConfigCreate, ModelConfigUpdate, ModelToggleUpdate


# Mock model config model
class MockModelConfig:
    def __init__(self, id=1, key="gpt-4", name="GPT-4", provider="openai", 
                 temperature=0.7, context_window=8192, max_output_tokens=4096,
                 extended_thinking=False, enabled=True):
        self.id = id
        self.key = key
        self.name = name
        self.provider = provider
        self.temperature = temperature
        self.context_window = context_window
        self.max_output_tokens = max_output_tokens
        self.extended_thinking = extended_thinking
        self.enabled = enabled
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
    def model_dump(self):
        """Mock model_dump for Pydantic compatibility."""
        return {
            "id": self.id,
            "key": self.key,
            "name": self.name,
            "provider": self.provider,
            "temperature": self.temperature,
            "context_window": self.context_window,
            "max_output_tokens": self.max_output_tokens,
            "extended_thinking": self.extended_thinking,
            "enabled": self.enabled,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@pytest.fixture
def mock_model_config_service():
    """Create a mock model config service."""
    service = AsyncMock()
    return service


@pytest.fixture
def app(mock_model_config_service):
    """Create a FastAPI app with mocked dependencies."""
    from fastapi import FastAPI
    from src.api.models_router import router, get_model_config_service
    
    app = FastAPI()
    app.include_router(router)
    
    # Create override function
    async def override_get_model_config_service():
        return mock_model_config_service
    
    # Override dependency
    app.dependency_overrides[get_model_config_service] = override_get_model_config_service
    
    return app


@pytest.fixture
def client(app):
    """Create a test client."""
    return TestClient(app)


@pytest.fixture
def sample_model_create():
    """Create a sample model creation request."""
    return ModelConfigCreate(
        key="gpt-4-turbo",
        name="GPT-4 Turbo",
        provider="openai",
        temperature=0.7,
        context_window=16384,
        max_output_tokens=4096,
        extended_thinking=False,
        enabled=True
    )


@pytest.fixture
def sample_model_update():
    """Create a sample model update request."""
    return ModelConfigUpdate(
        key="gpt-4-turbo",
        name="GPT-4 Turbo Updated",
        provider="openai",
        temperature=0.8,
        context_window=32768,
        max_output_tokens=8192,
        extended_thinking=True,
        enabled=True
    )


@pytest.fixture
def sample_toggle_update():
    """Create a sample toggle update request."""
    return ModelToggleUpdate(enabled=False)


@pytest.fixture
def mock_unit_of_work():
    """Create a mock unit of work."""
    uow = AsyncMock()
    uow.model_config_repository = AsyncMock()
    return uow


@pytest.fixture
def mock_model_config_service_class():
    """Create a mock ModelConfigService class."""
    service_class = AsyncMock()
    service_instance = AsyncMock()
    service_class.from_unit_of_work.return_value = service_instance
    return service_class, service_instance


class TestGetModelConfigService:
    """Test cases for the get_model_config_service dependency function."""
    
    @pytest.mark.asyncio
    async def test_get_model_config_service_success(self, mock_unit_of_work, mock_model_config_service_class):
        """Test successful creation of ModelConfigService from UnitOfWork."""
        service_class, service_instance = mock_model_config_service_class
        
        with patch('src.api.models_router.UnitOfWork') as mock_uow_class, \
             patch('src.api.models_router.ModelConfigService', service_class):
            
            # Configure UnitOfWork as async context manager
            mock_uow_instance = AsyncMock()
            mock_uow_instance.__aenter__.return_value = mock_unit_of_work
            mock_uow_instance.__aexit__.return_value = None
            mock_uow_class.return_value = mock_uow_instance
            
            from src.api.models_router import get_model_config_service
            
            result = await get_model_config_service()
            
            # Verify behavior
            mock_uow_class.assert_called_once()
            mock_uow_instance.__aenter__.assert_called_once()
            mock_uow_instance.__aexit__.assert_called_once()
            service_class.from_unit_of_work.assert_called_once_with(mock_unit_of_work)
            assert result == service_instance
    
    @pytest.mark.asyncio
    async def test_get_model_config_service_exception_handling(self, mock_model_config_service_class):
        """Test exception handling in get_model_config_service."""
        service_class, _ = mock_model_config_service_class
        
        with patch('src.api.models_router.UnitOfWork') as mock_uow_class, \
             patch('src.api.models_router.ModelConfigService', service_class):
            
            # Configure UnitOfWork to raise an exception
            mock_uow_class.side_effect = Exception("Database connection failed")
            
            from src.api.models_router import get_model_config_service
            
            with pytest.raises(Exception, match="Database connection failed"):
                await get_model_config_service()


class TestGetModels:
    """Test cases for get all models endpoint."""
    
    def test_get_models_success(self, client, mock_model_config_service):
        """Test successful models retrieval."""
        models = [
            MockModelConfig(id=1, key="gpt-4", name="GPT-4"),
            MockModelConfig(id=2, key="claude-3", name="Claude-3")
        ]
        mock_model_config_service.find_all.return_value = models
        
        response = client.get("/models")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["models"]) == 2
        assert data["models"][0]["key"] == "gpt-4"
        assert data["models"][1]["key"] == "claude-3"
    
    def test_get_models_empty(self, client, mock_model_config_service):
        """Test getting models when none exist."""
        mock_model_config_service.find_all.return_value = []
        
        response = client.get("/models")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert len(data["models"]) == 0
    
    def test_get_models_debug_logging_three_models(self, client, mock_model_config_service):
        """Test debug logging for exactly 3 models (lines 53-54)."""
        models = [
            MockModelConfig(id=1, key="model-1", name="Model 1"),
            MockModelConfig(id=2, key="model-2", name="Model 2"),
            MockModelConfig(id=3, key="model-3", name="Model 3")
        ]
        mock_model_config_service.find_all.return_value = models
        
        response = client.get("/models")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 3
        assert len(data["models"]) == 3
    
    def test_get_models_debug_logging_more_than_three(self, client, mock_model_config_service):
        """Test debug logging for more than 3 models (lines 53-54)."""
        models = [
            MockModelConfig(id=i, key=f"model-{i}", name=f"Model {i}") 
            for i in range(1, 6)
        ]
        mock_model_config_service.find_all.return_value = models
        
        response = client.get("/models")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 5
        assert len(data["models"]) == 5
    
    def test_get_models_service_error(self, client, mock_model_config_service):
        """Test getting models with service error."""
        mock_model_config_service.find_all.side_effect = Exception("Database error")
        
        response = client.get("/models")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestGetEnabledModels:
    """Test cases for get enabled models endpoint."""
    
    def test_get_enabled_models_success(self, client, mock_model_config_service):
        """Test successful enabled models retrieval."""
        enabled_models = [
            MockModelConfig(id=1, key="gpt-4", name="GPT-4", enabled=True)
        ]
        mock_model_config_service.find_enabled_models.return_value = enabled_models
        
        response = client.get("/models/enabled")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert len(data["models"]) == 1
        assert data["models"][0]["enabled"] is True
    
    def test_get_enabled_models_none_enabled(self, client, mock_model_config_service):
        """Test getting enabled models when none are enabled."""
        mock_model_config_service.find_enabled_models.return_value = []
        
        response = client.get("/models/enabled")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert len(data["models"]) == 0
    
    def test_get_enabled_models_service_error(self, client, mock_model_config_service):
        """Test getting enabled models with service error."""
        mock_model_config_service.find_enabled_models.side_effect = Exception("Database error")
        
        response = client.get("/models/enabled")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestGetModel:
    """Test cases for get single model endpoint."""
    
    def test_get_model_success(self, client, mock_model_config_service):
        """Test successful single model retrieval."""
        model = MockModelConfig()
        mock_model_config_service.find_by_key.return_value = model
        
        response = client.get("/models/gpt-4")
        
        assert response.status_code == 200
        data = response.json()
        assert data["key"] == "gpt-4"
        assert data["name"] == "GPT-4"
        mock_model_config_service.find_by_key.assert_called_once_with("gpt-4")
    
    def test_get_model_not_found(self, client, mock_model_config_service):
        """Test getting non-existent model."""
        mock_model_config_service.find_by_key.return_value = None
        
        response = client.get("/models/nonexistent")
        
        assert response.status_code == 404
        assert "Model with key nonexistent not found" in response.json()["detail"]
    
    def test_get_model_http_exception_reraise(self, client, mock_model_config_service):
        """Test that HTTPExceptions are re-raised in get_model endpoint."""
        mock_model_config_service.find_by_key.side_effect = HTTPException(
            status_code=403, detail="Access forbidden"
        )
        
        response = client.get("/models/test-key")
        
        assert response.status_code == 403
        assert response.json()["detail"] == "Access forbidden"
    
    def test_get_model_service_error(self, client, mock_model_config_service):
        """Test getting model with service error."""
        mock_model_config_service.find_by_key.side_effect = Exception("Database error")
        
        response = client.get("/models/gpt-4")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestCreateModel:
    """Test cases for create model endpoint."""
    
    def test_create_model_success(self, client, mock_model_config_service, sample_model_create):
        """Test successful model creation."""
        created_model = MockModelConfig(key="gpt-4-turbo", name="GPT-4 Turbo")
        mock_model_config_service.create_model_config.return_value = created_model
        
        response = client.post("/models", json=sample_model_create.model_dump())
        
        assert response.status_code == 201
        data = response.json()
        assert data["key"] == "gpt-4-turbo"
        assert data["name"] == "GPT-4 Turbo"
        mock_model_config_service.create_model_config.assert_called_once()
    
    def test_create_model_value_error(self, client, mock_model_config_service, sample_model_create):
        """Test creating model that already exists (ValueError handling)."""
        mock_model_config_service.create_model_config.side_effect = ValueError("Model already exists")
        
        response = client.post("/models", json=sample_model_create.model_dump())
        
        assert response.status_code == 400
        assert "Model already exists" in response.json()["detail"]
    
    def test_create_model_service_error(self, client, mock_model_config_service, sample_model_create):
        """Test creating model with service error."""
        mock_model_config_service.create_model_config.side_effect = Exception("Database error")
        
        response = client.post("/models", json=sample_model_create.model_dump())
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestUpdateModel:
    """Test cases for update model endpoint."""
    
    def test_update_model_success(self, client, mock_model_config_service, sample_model_update):
        """Test successful model update."""
        updated_model = MockModelConfig(key="gpt-4-turbo", name="GPT-4 Turbo Updated")
        mock_model_config_service.update_model_config.return_value = updated_model
        
        response = client.put("/models/gpt-4-turbo", json=sample_model_update.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["key"] == "gpt-4-turbo"
        assert data["name"] == "GPT-4 Turbo Updated"
        mock_model_config_service.update_model_config.assert_called_once_with("gpt-4-turbo", sample_model_update)
    
    def test_update_model_not_found(self, client, mock_model_config_service, sample_model_update):
        """Test updating non-existent model."""
        mock_model_config_service.update_model_config.return_value = None
        
        response = client.put("/models/nonexistent", json=sample_model_update.model_dump())
        
        assert response.status_code == 404
        assert "Model with key nonexistent not found" in response.json()["detail"]
    
    def test_update_model_http_exception_reraise(self, client, mock_model_config_service, sample_model_update):
        """Test that HTTPExceptions are re-raised in update_model endpoint."""
        mock_model_config_service.update_model_config.side_effect = HTTPException(
            status_code=403, detail="Update forbidden"
        )
        
        response = client.put("/models/test-key", json=sample_model_update.model_dump())
        
        assert response.status_code == 403
        assert response.json()["detail"] == "Update forbidden"
    
    def test_update_model_service_error(self, client, mock_model_config_service, sample_model_update):
        """Test updating model with service error."""
        mock_model_config_service.update_model_config.side_effect = Exception("Database error")
        
        response = client.put("/models/gpt-4-turbo", json=sample_model_update.model_dump())
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestToggleModel:
    """Test cases for toggle model endpoint."""
    
    def test_toggle_model_success(self, client, mock_model_config_service, sample_toggle_update):
        """Test successful model toggle."""
        toggled_model = MockModelConfig(enabled=False)
        mock_model_config_service.toggle_model_enabled.return_value = toggled_model
        
        response = client.patch("/models/gpt-4/toggle", json=sample_toggle_update.model_dump())
        
        assert response.status_code == 200
        data = response.json()
        assert data["enabled"] is False
        mock_model_config_service.toggle_model_enabled.assert_called_once_with("gpt-4", False)
    
    def test_toggle_model_not_found(self, client, mock_model_config_service, sample_toggle_update):
        """Test toggling non-existent model."""
        mock_model_config_service.toggle_model_enabled.return_value = None
        
        response = client.patch("/models/nonexistent/toggle", json=sample_toggle_update.model_dump())
        
        assert response.status_code == 404
        assert "Model with key nonexistent not found" in response.json()["detail"]
    
    def test_toggle_model_http_exception_reraise(self, client, mock_model_config_service, sample_toggle_update):
        """Test that HTTPExceptions are re-raised in toggle_model endpoint."""
        mock_model_config_service.toggle_model_enabled.side_effect = HTTPException(
            status_code=403, detail="Toggle forbidden"
        )
        
        response = client.patch("/models/test-key/toggle", json=sample_toggle_update.model_dump())
        
        assert response.status_code == 403
        assert response.json()["detail"] == "Toggle forbidden"
    
    def test_toggle_model_service_error(self, client, mock_model_config_service, sample_toggle_update):
        """Test toggling model with service error."""
        mock_model_config_service.toggle_model_enabled.side_effect = Exception("Database error")
        
        response = client.patch("/models/gpt-4/toggle", json=sample_toggle_update.model_dump())
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestDeleteModel:
    """Test cases for delete model endpoint."""
    
    def test_delete_model_success(self, client, mock_model_config_service):
        """Test successful model deletion."""
        mock_model_config_service.delete_model_config.return_value = True
        
        response = client.delete("/models/gpt-4")
        
        assert response.status_code == 204
        mock_model_config_service.delete_model_config.assert_called_once_with("gpt-4")
    
    def test_delete_model_not_found(self, client, mock_model_config_service):
        """Test deleting non-existent model."""
        mock_model_config_service.delete_model_config.return_value = False
        
        response = client.delete("/models/nonexistent")
        
        assert response.status_code == 404
        assert "Model with key nonexistent not found" in response.json()["detail"]
    
    def test_delete_model_http_exception_reraise(self, client, mock_model_config_service):
        """Test that HTTPExceptions are re-raised in delete_model endpoint."""
        mock_model_config_service.delete_model_config.side_effect = HTTPException(
            status_code=403, detail="Delete forbidden"
        )
        
        response = client.delete("/models/test-key")
        
        assert response.status_code == 403
        assert response.json()["detail"] == "Delete forbidden"
    
    def test_delete_model_service_error(self, client, mock_model_config_service):
        """Test deleting model with service error."""
        mock_model_config_service.delete_model_config.side_effect = Exception("Database error")
        
        response = client.delete("/models/gpt-4")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestEnableAllModels:
    """Test cases for enable all models endpoint."""
    
    def test_enable_all_models_success(self, client, mock_model_config_service):
        """Test successful enable all models."""
        enabled_models = [
            MockModelConfig(id=1, key="gpt-4", enabled=True),
            MockModelConfig(id=2, key="claude-3", enabled=True)
        ]
        mock_model_config_service.enable_all_models.return_value = enabled_models
        
        response = client.post("/models/enable-all")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert all(model["enabled"] for model in data["models"])
    
    def test_enable_all_models_service_error(self, client, mock_model_config_service):
        """Test enable all models with service error."""
        mock_model_config_service.enable_all_models.side_effect = Exception("Database error")
        
        response = client.post("/models/enable-all")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestDisableAllModels:
    """Test cases for disable all models endpoint."""
    
    def test_disable_all_models_success(self, client, mock_model_config_service):
        """Test successful disable all models."""
        disabled_models = [
            MockModelConfig(id=1, key="gpt-4", enabled=False),
            MockModelConfig(id=2, key="claude-3", enabled=False)
        ]
        mock_model_config_service.disable_all_models.return_value = disabled_models
        
        response = client.post("/models/disable-all")
        
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert all(not model["enabled"] for model in data["models"])
    
    def test_disable_all_models_service_error(self, client, mock_model_config_service):
        """Test disable all models with service error."""
        mock_model_config_service.disable_all_models.side_effect = Exception("Database error")
        
        response = client.post("/models/disable-all")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestRouterConfiguration:
    """Test router configuration and metadata."""
    
    def test_router_metadata_coverage(self):
        """Test router configuration (lines 18-22)."""
        from src.api.models_router import router
        
        # Verify router configuration
        assert router.prefix == "/models"
        assert "models" in router.tags
        assert 404 in router.responses
        assert router.responses[404]["description"] == "Not found"
    
    def test_logger_initialization_coverage(self):
        """Test logger setup (lines 24-25)."""
        import logging
        from src.api.models_router import logger
        
        # Verify logger is properly configured
        assert isinstance(logger, logging.Logger)
        assert logger.name == "src.api.models_router"