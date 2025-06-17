"""
Unit tests for EngineConfigRouter.

Tests the functionality of engine configuration management endpoints.
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastapi import HTTPException, status

from src.api.engine_config_router import router, get_engine_config_service
from src.schemas.engine_config import (
    EngineConfigCreate,
    EngineConfigUpdate,
    EngineConfigToggleUpdate,
    EngineConfigValueUpdate,
    CrewAIFlowConfigUpdate
)


@pytest.fixture
def mock_engine_config():
    """Create mock engine config data."""
    from datetime import datetime
    return {
        "id": 123,
        "engine_name": "crewai",
        "engine_type": "ai_engine", 
        "config_key": "flow_enabled",
        "config_value": '{"enabled": true}',
        "description": "CrewAI flow configuration",
        "enabled": True,
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }


@pytest.fixture
def mock_service():
    """Create a mock EngineConfigService."""
    service = AsyncMock()
    service.find_all = AsyncMock()
    service.find_enabled_configs = AsyncMock()
    service.find_by_engine_name = AsyncMock()
    service.find_by_engine_and_key = AsyncMock()
    service.find_by_engine_type = AsyncMock()
    service.create_engine_config = AsyncMock()
    service.update_engine_config = AsyncMock()
    service.toggle_engine_enabled = AsyncMock()
    service.update_config_value = AsyncMock()
    service.get_crewai_flow_enabled = AsyncMock()
    service.set_crewai_flow_enabled = AsyncMock()
    service.delete_engine_config = AsyncMock()
    return service


@pytest.fixture
def client(mock_service):
    """Create a test client with mocked dependencies.""" 
    app = FastAPI()
    app.include_router(router)
    
    # Override the dependency to return our mock service
    app.dependency_overrides[get_engine_config_service] = lambda: mock_service
    
    return TestClient(app)


class TestEngineConfigRouter:
    """Test cases for engine config endpoints."""
    
    def test_get_engine_configs_success(self, client, mock_service, mock_engine_config):
        """Test successful retrieval of all engine configs."""
        mock_service.find_all.return_value = [mock_engine_config]
        
        response = client.get("/engine-config")
        
        assert response.status_code == 200
        data = response.json()
        assert "configs" in data
        assert "count" in data
        assert data["count"] == 1
        assert len(data["configs"]) == 1
        mock_service.find_all.assert_called_once()
    
    def test_get_engine_configs_exception(self, client, mock_service):
        """Test exception handling in get_engine_configs."""
        mock_service.find_all.side_effect = Exception("Database error")
        
        response = client.get("/engine-config")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_get_enabled_engine_configs_success(self, client, mock_service, mock_engine_config):
        """Test successful retrieval of enabled engine configs."""
        mock_service.find_enabled_configs.return_value = [mock_engine_config]
        
        response = client.get("/engine-config/enabled")
        
        assert response.status_code == 200
        data = response.json()
        assert "configs" in data
        assert "count" in data
        assert data["count"] == 1
        mock_service.find_enabled_configs.assert_called_once()
    
    def test_get_enabled_engine_configs_exception(self, client, mock_service):
        """Test exception handling in get_enabled_engine_configs."""
        mock_service.find_enabled_configs.side_effect = Exception("Database error")
        
        response = client.get("/engine-config/enabled")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_get_engine_config_success(self, client, mock_service, mock_engine_config):
        """Test successful retrieval of specific engine config."""
        mock_service.find_by_engine_name.return_value = mock_engine_config
        
        response = client.get("/engine-config/engine/crewai")
        
        assert response.status_code == 200
        data = response.json()
        assert data["engine_name"] == "crewai"
        mock_service.find_by_engine_name.assert_called_once_with("crewai")
    
    def test_get_engine_config_not_found(self, client, mock_service):
        """Test engine config not found."""
        mock_service.find_by_engine_name.return_value = None
        
        response = client.get("/engine-config/engine/nonexistent")
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"]
    
    def test_get_engine_config_exception(self, client, mock_service):
        """Test exception handling in get_engine_config."""
        mock_service.find_by_engine_name.side_effect = Exception("Database error")
        
        response = client.get("/engine-config/engine/crewai")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_get_engine_config_http_exception_reraise(self, client, mock_service):
        """Test that HTTPException is re-raised properly."""
        mock_service.find_by_engine_name.side_effect = HTTPException(status_code=403, detail="Forbidden")
        
        response = client.get("/engine-config/engine/crewai")
        
        assert response.status_code == 403
        assert "Forbidden" in response.json()["detail"]
    
    def test_get_engine_config_by_key_success(self, client, mock_service, mock_engine_config):
        """Test successful retrieval of engine config by key."""
        mock_service.find_by_engine_and_key.return_value = mock_engine_config
        
        response = client.get("/engine-config/engine/crewai/config/flow_enabled")
        
        assert response.status_code == 200
        data = response.json()
        assert data["engine_name"] == "crewai"
        mock_service.find_by_engine_and_key.assert_called_once_with("crewai", "flow_enabled")
    
    def test_get_engine_config_by_key_not_found(self, client, mock_service):
        """Test engine config by key not found."""
        mock_service.find_by_engine_and_key.return_value = None
        
        response = client.get("/engine-config/engine/crewai/config/nonexistent")
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"]
    
    def test_get_engine_config_by_key_exception(self, client, mock_service):
        """Test exception handling in get_engine_config_by_key."""
        mock_service.find_by_engine_and_key.side_effect = Exception("Database error")
        
        response = client.get("/engine-config/engine/crewai/config/flow_enabled")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_get_engine_config_by_key_http_exception_reraise(self, client, mock_service):
        """Test that HTTPException is re-raised properly in get_engine_config_by_key."""
        mock_service.find_by_engine_and_key.side_effect = HTTPException(status_code=403, detail="Forbidden")
        
        response = client.get("/engine-config/engine/crewai/config/flow_enabled")
        
        assert response.status_code == 403
        assert "Forbidden" in response.json()["detail"]
    
    def test_get_engine_configs_by_type_success(self, client, mock_service, mock_engine_config):
        """Test successful retrieval of engine configs by type."""
        mock_service.find_by_engine_type.return_value = [mock_engine_config]
        
        response = client.get("/engine-config/type/ai_engine")
        
        assert response.status_code == 200
        data = response.json()
        assert "configs" in data
        assert "count" in data
        assert data["count"] == 1
        mock_service.find_by_engine_type.assert_called_once_with("ai_engine")
    
    def test_get_engine_configs_by_type_exception(self, client, mock_service):
        """Test exception handling in get_engine_configs_by_type."""
        mock_service.find_by_engine_type.side_effect = Exception("Database error")
        
        response = client.get("/engine-config/type/ai_engine")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_create_engine_config_success(self, client, mock_service, mock_engine_config):
        """Test successful creation of engine config."""
        mock_service.create_engine_config.return_value = mock_engine_config
        
        config_data = {
            "engine_name": "crewai",
            "engine_type": "ai_engine",
            "config_key": "flow_enabled",
            "config_value": '{"enabled": true}',
            "description": "CrewAI flow configuration",
            "enabled": True
        }
        
        response = client.post("/engine-config", json=config_data)
        
        assert response.status_code == 201
        data = response.json()
        assert data["engine_name"] == "crewai"
        mock_service.create_engine_config.assert_called_once()
    
    def test_create_engine_config_already_exists(self, client, mock_service):
        """Test creation of engine config that already exists."""
        mock_service.create_engine_config.side_effect = ValueError("Engine config already exists")
        
        config_data = {
            "engine_name": "crewai",
            "engine_type": "ai_engine",
            "config_key": "flow_enabled",
            "config_value": '{"enabled": true}'
        }
        
        response = client.post("/engine-config", json=config_data)
        
        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]
    
    def test_create_engine_config_exception(self, client, mock_service):
        """Test exception handling in create_engine_config."""
        mock_service.create_engine_config.side_effect = Exception("Database error")
        
        config_data = {
            "engine_name": "crewai",
            "engine_type": "ai_engine",
            "config_key": "flow_enabled",
            "config_value": '{"enabled": true}'
        }
        
        response = client.post("/engine-config", json=config_data)
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_update_engine_config_success(self, client, mock_service, mock_engine_config):
        """Test successful update of engine config."""
        mock_service.update_engine_config.return_value = mock_engine_config
        
        update_data = {
            "description": "Updated description",
            "enabled": False
        }
        
        response = client.put("/engine-config/engine/crewai", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["engine_name"] == "crewai"
        mock_service.update_engine_config.assert_called_once()
    
    def test_update_engine_config_not_found(self, client, mock_service):
        """Test update of non-existent engine config."""
        mock_service.update_engine_config.return_value = None
        
        update_data = {"description": "Updated description"}
        
        response = client.put("/engine-config/engine/nonexistent", json=update_data)
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"]
    
    def test_update_engine_config_exception(self, client, mock_service):
        """Test exception handling in update_engine_config."""
        mock_service.update_engine_config.side_effect = Exception("Database error")
        
        update_data = {"description": "Updated description"}
        
        response = client.put("/engine-config/engine/crewai", json=update_data)
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_update_engine_config_http_exception_reraise(self, client, mock_service):
        """Test that HTTPException is re-raised properly in update_engine_config."""
        mock_service.update_engine_config.side_effect = HTTPException(status_code=403, detail="Forbidden")
        
        update_data = {"description": "Updated description"}
        
        response = client.put("/engine-config/engine/crewai", json=update_data)
        
        assert response.status_code == 403
        assert "Forbidden" in response.json()["detail"]
    
    def test_toggle_engine_config_success(self, client, mock_service, mock_engine_config):
        """Test successful toggle of engine config."""
        mock_service.toggle_engine_enabled.return_value = mock_engine_config
        
        toggle_data = {"enabled": False}
        
        response = client.patch("/engine-config/engine/crewai/toggle", json=toggle_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["engine_name"] == "crewai"
        mock_service.toggle_engine_enabled.assert_called_once_with("crewai", False)
    
    def test_toggle_engine_config_not_found(self, client, mock_service):
        """Test toggle of non-existent engine config."""
        mock_service.toggle_engine_enabled.return_value = None
        
        toggle_data = {"enabled": False}
        
        response = client.patch("/engine-config/engine/nonexistent/toggle", json=toggle_data)
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"]
    
    def test_toggle_engine_config_exception(self, client, mock_service):
        """Test exception handling in toggle_engine_config."""
        mock_service.toggle_engine_enabled.side_effect = Exception("Database error")
        
        toggle_data = {"enabled": False}
        
        response = client.patch("/engine-config/engine/crewai/toggle", json=toggle_data)
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_toggle_engine_config_http_exception_reraise(self, client, mock_service):
        """Test that HTTPException is re-raised properly in toggle_engine_config."""
        mock_service.toggle_engine_enabled.side_effect = HTTPException(status_code=403, detail="Forbidden")
        
        toggle_data = {"enabled": False}
        
        response = client.patch("/engine-config/engine/crewai/toggle", json=toggle_data)
        
        assert response.status_code == 403
        assert "Forbidden" in response.json()["detail"]
    
    def test_update_config_value_success(self, client, mock_service, mock_engine_config):
        """Test successful update of config value."""
        mock_service.update_config_value.return_value = mock_engine_config
        
        value_data = {"config_value": '{"enabled": false}'}
        
        response = client.patch("/engine-config/engine/crewai/config/flow_enabled/value", json=value_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["engine_name"] == "crewai"
        mock_service.update_config_value.assert_called_once_with("crewai", "flow_enabled", '{"enabled": false}')
    
    def test_update_config_value_not_found(self, client, mock_service):
        """Test update of non-existent config value."""
        mock_service.update_config_value.return_value = None
        
        value_data = {"config_value": '{"enabled": false}'}
        
        response = client.patch("/engine-config/engine/nonexistent/config/flow_enabled/value", json=value_data)
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"]
    
    def test_update_config_value_exception(self, client, mock_service):
        """Test exception handling in update_config_value."""
        mock_service.update_config_value.side_effect = Exception("Database error")
        
        value_data = {"config_value": '{"enabled": false}'}
        
        response = client.patch("/engine-config/engine/crewai/config/flow_enabled/value", json=value_data)
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_update_config_value_http_exception_reraise(self, client, mock_service):
        """Test that HTTPException is re-raised properly in update_config_value."""
        mock_service.update_config_value.side_effect = HTTPException(status_code=403, detail="Forbidden")
        
        value_data = {"config_value": '{"enabled": false}'}
        
        response = client.patch("/engine-config/engine/crewai/config/flow_enabled/value", json=value_data)
        
        assert response.status_code == 403
        assert "Forbidden" in response.json()["detail"]
    
    def test_get_crewai_flow_enabled_success(self, client, mock_service):
        """Test successful retrieval of CrewAI flow enabled status."""
        mock_service.get_crewai_flow_enabled.return_value = True
        
        response = client.get("/engine-config/crewai/flow-enabled")
        
        assert response.status_code == 200
        data = response.json()
        assert data["flow_enabled"] is True
        mock_service.get_crewai_flow_enabled.assert_called_once()
    
    def test_get_crewai_flow_enabled_exception(self, client, mock_service):
        """Test exception handling in get_crewai_flow_enabled."""
        mock_service.get_crewai_flow_enabled.side_effect = Exception("Database error")
        
        response = client.get("/engine-config/crewai/flow-enabled")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_set_crewai_flow_enabled_success(self, client, mock_service):
        """Test successful setting of CrewAI flow enabled status."""
        mock_service.set_crewai_flow_enabled.return_value = True
        
        config_data = {"flow_enabled": False}
        
        response = client.patch("/engine-config/crewai/flow-enabled", json=config_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["flow_enabled"] is False
        mock_service.set_crewai_flow_enabled.assert_called_once_with(False)
    
    def test_set_crewai_flow_enabled_failure(self, client, mock_service):
        """Test failure in setting CrewAI flow enabled status."""
        mock_service.set_crewai_flow_enabled.return_value = False
        
        config_data = {"flow_enabled": False}
        
        response = client.patch("/engine-config/crewai/flow-enabled", json=config_data)
        
        assert response.status_code == 500
        assert "Failed to update" in response.json()["detail"]
    
    def test_set_crewai_flow_enabled_exception(self, client, mock_service):
        """Test exception handling in set_crewai_flow_enabled."""
        mock_service.set_crewai_flow_enabled.side_effect = Exception("Database error")
        
        config_data = {"flow_enabled": False}
        
        response = client.patch("/engine-config/crewai/flow-enabled", json=config_data)
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_set_crewai_flow_enabled_http_exception_reraise(self, client, mock_service):
        """Test that HTTPException is re-raised properly in set_crewai_flow_enabled."""
        mock_service.set_crewai_flow_enabled.side_effect = HTTPException(status_code=403, detail="Forbidden")
        
        config_data = {"flow_enabled": False}
        
        response = client.patch("/engine-config/crewai/flow-enabled", json=config_data)
        
        assert response.status_code == 403
        assert "Forbidden" in response.json()["detail"]
    
    def test_delete_engine_config_success(self, client, mock_service):
        """Test successful deletion of engine config."""
        mock_service.delete_engine_config.return_value = True
        
        response = client.delete("/engine-config/engine/crewai")
        
        assert response.status_code == 204
        mock_service.delete_engine_config.assert_called_once_with("crewai")
    
    def test_delete_engine_config_not_found(self, client, mock_service):
        """Test deletion of non-existent engine config."""
        mock_service.delete_engine_config.return_value = False
        
        response = client.delete("/engine-config/engine/nonexistent")
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"]
    
    def test_delete_engine_config_exception(self, client, mock_service):
        """Test exception handling in delete_engine_config."""
        mock_service.delete_engine_config.side_effect = Exception("Database error")
        
        response = client.delete("/engine-config/engine/crewai")
        
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]
    
    def test_delete_engine_config_http_exception_reraise(self, client, mock_service):
        """Test that HTTPException is re-raised properly in delete_engine_config."""
        mock_service.delete_engine_config.side_effect = HTTPException(status_code=403, detail="Forbidden")
        
        response = client.delete("/engine-config/engine/crewai")
        
        assert response.status_code == 403
        assert "Forbidden" in response.json()["detail"]


class TestEngineConfigDependency:
    """Test cases for engine config service dependency."""
    
    @pytest.mark.asyncio
    async def test_get_engine_config_service(self):
        """Test the get_engine_config_service dependency."""
        with patch('src.api.engine_config_router.UnitOfWork') as mock_uow_class, \
             patch('src.api.engine_config_router.EngineConfigService') as mock_service_class:
            
            mock_uow = AsyncMock()
            mock_uow_class.return_value.__aenter__.return_value = mock_uow
            mock_uow_class.return_value.__aexit__.return_value = None
            mock_service = AsyncMock()
            mock_service_class.from_unit_of_work = AsyncMock(return_value=mock_service)
            
            result = await get_engine_config_service()
            
            assert result == mock_service
            mock_service_class.from_unit_of_work.assert_called_once_with(mock_uow)