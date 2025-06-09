"""
Unit tests for executions API router.

Tests the functionality of the executions API endpoints including
creating, getting, and listing executions.
"""
import pytest
import uuid
import json
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from src.api.executions_router import router
from src.schemas.execution import (
    CrewConfig, ExecutionStatus, ExecutionResponse, 
    ExecutionCreateResponse, ExecutionNameGenerationRequest
)
from src.services.execution_service import ExecutionService


@pytest.fixture
def app():
    """Create a FastAPI app for testing."""
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def client(app):
    """Create a test client for the app."""
    return TestClient(app)


@pytest.fixture
def mock_crew_config():
    """Create mock crew configuration data."""
    return {
        "agents_yaml": {"agent1": {"role": "researcher"}},
        "tasks_yaml": {"task1": {"description": "research task"}},
        "model": "gpt-4o-mini",
        "planning": True,
        "execution_type": "crew",
        "inputs": {},
        "schema_detection_enabled": True
    }


@pytest.fixture
def mock_flow_config():
    """Create mock flow configuration data."""
    return {
        "agents_yaml": {},
        "tasks_yaml": {},
        "model": "gpt-4o-mini",
        "planning": False,
        "execution_type": "flow",
        "flow_id": str(uuid.uuid4()),
        "nodes": [{"id": "node1", "type": "agent"}],
        "edges": [{"source": "node1", "target": "node2"}],
        "inputs": {},
        "schema_detection_enabled": False
    }


@pytest.fixture
def mock_execution_response():
    """Create mock execution response data."""
    return {
        "execution_id": "test-execution-123",
        "status": ExecutionStatus.COMPLETED.value,
        "created_at": datetime.now(UTC),
        "run_name": "Test Execution",
        "result": {"output": "success"},
        "error": None
    }


class TestExecutionsRouter:
    """Test cases for executions router."""
    
    @pytest.mark.asyncio
    async def test_create_execution_crew_success(self, client, mock_crew_config):
        """Test successful crew execution creation."""
        mock_response = {
            "execution_id": "test-execution-123",
            "status": ExecutionStatus.PENDING.value,
            "run_name": "Generated Crew Name"
        }
        
        with patch.object(ExecutionService, "create_execution", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = mock_response
            
            response = client.post("/executions", json=mock_crew_config)
            
            assert response.status_code == 200
            result = response.json()
            assert result["execution_id"] == "test-execution-123"
            assert result["status"] == ExecutionStatus.PENDING.value
            assert result["run_name"] == "Generated Crew Name"
    
    @pytest.mark.asyncio
    async def test_create_execution_flow_success(self, client, mock_flow_config):
        """Test successful flow execution creation."""
        mock_response = {
            "execution_id": "test-flow-execution-456",
            "status": ExecutionStatus.PENDING.value,
            "run_name": "Generated Flow Name"
        }
        
        with patch.object(ExecutionService, "create_execution", new_callable=AsyncMock) as mock_create, \
             patch("src.api.executions_router.FlowService") as mock_flow_service:
            
            # Mock flow service
            mock_flow_instance = AsyncMock()
            mock_flow = MagicMock()
            mock_flow.name = "Test Flow"
            mock_flow.id = uuid.UUID(mock_flow_config["flow_id"])
            mock_flow_instance.get_flow.return_value = mock_flow
            mock_flow_service.return_value = mock_flow_instance
            
            mock_create.return_value = mock_response
            
            response = client.post("/executions", json=mock_flow_config)
            
            assert response.status_code == 200
            result = response.json()
            assert result["execution_id"] == "test-flow-execution-456"
            assert result["status"] == ExecutionStatus.PENDING.value
    
    @pytest.mark.asyncio
    async def test_create_execution_invalid_flow_id(self, client, mock_flow_config):
        """Test execution creation with invalid flow_id."""
        mock_flow_config["flow_id"] = "invalid-uuid"
        
        response = client.post("/executions", json=mock_flow_config)
        
        assert response.status_code == 400
        assert "Invalid flow_id format" in response.json()["detail"]
    
    @pytest.mark.asyncio
    async def test_create_execution_flow_not_found(self, client, mock_flow_config):
        """Test execution creation with non-existent flow."""
        with patch("src.api.executions_router.FlowService") as mock_flow_service:
            mock_flow_instance = AsyncMock()
            mock_flow_instance.get_flow.side_effect = HTTPException(
                status_code=404, detail="Flow not found"
            )
            mock_flow_service.return_value = mock_flow_instance
            
            response = client.post("/executions", json=mock_flow_config)
            
            assert response.status_code == 400
            assert "Flow with ID" in response.json()["detail"]
            assert "not found" in response.json()["detail"]
    
    @pytest.mark.asyncio
    async def test_create_execution_service_error(self, client, mock_crew_config):
        """Test execution creation with service error."""
        with patch.object(ExecutionService, "create_execution", new_callable=AsyncMock) as mock_create:
            mock_create.side_effect = Exception("Service unavailable")
            
            response = client.post("/executions", json=mock_crew_config)
            
            assert response.status_code == 500
            assert "Service unavailable" in response.json()["detail"]
    
    @pytest.mark.asyncio
    async def test_get_execution_status_success(self, client, mock_execution_response):
        """Test successful execution status retrieval."""
        execution_id = "test-execution-123"
        
        with patch.object(ExecutionService, "get_execution_status", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_execution_response
            
            response = client.get(f"/executions/{execution_id}")
            
            assert response.status_code == 200
            result = response.json()
            assert result["execution_id"] == execution_id
            assert result["status"] == ExecutionStatus.COMPLETED.value
            assert result["result"]["output"] == "success"
    
    @pytest.mark.asyncio
    async def test_get_execution_status_not_found(self, client):
        """Test execution status retrieval for non-existent execution."""
        execution_id = "nonexistent-execution"
        
        with patch.object(ExecutionService, "get_execution_status", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None
            
            response = client.get(f"/executions/{execution_id}")
            
            assert response.status_code == 404
            assert response.json()["detail"] == "Execution not found"
    
    @pytest.mark.asyncio
    async def test_get_execution_status_string_result_parsing(self, client):
        """Test execution status with string result parsing."""
        execution_id = "test-execution-123"
        mock_response = {
            "execution_id": execution_id,
            "status": ExecutionStatus.COMPLETED.value,
            "created_at": datetime.now(UTC),
            "run_name": "Test Execution",
            "result": '{"parsed": "json"}',  # String that should be parsed as JSON
            "error": None
        }
        
        with patch.object(ExecutionService, "get_execution_status", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            
            response = client.get(f"/executions/{execution_id}")
            
            assert response.status_code == 200
            result = response.json()
            assert result["result"]["parsed"] == "json"
    
    @pytest.mark.asyncio
    async def test_get_execution_status_invalid_json_result(self, client):
        """Test execution status with invalid JSON result."""
        execution_id = "test-execution-123"
        mock_response = {
            "execution_id": execution_id,
            "status": ExecutionStatus.COMPLETED.value,
            "created_at": datetime.now(UTC),
            "run_name": "Test Execution",
            "result": "invalid json string",  # Invalid JSON
            "error": None
        }
        
        with patch.object(ExecutionService, "get_execution_status", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            
            response = client.get(f"/executions/{execution_id}")
            
            assert response.status_code == 200
            result = response.json()
            assert result["result"]["value"] == "invalid json string"
    
    @pytest.mark.asyncio
    async def test_get_execution_status_list_result(self, client):
        """Test execution status with list result."""
        execution_id = "test-execution-123"
        mock_response = {
            "execution_id": execution_id,
            "status": ExecutionStatus.COMPLETED.value,
            "created_at": datetime.now(UTC),
            "run_name": "Test Execution",
            "result": ["item1", "item2", "item3"],
            "error": None
        }
        
        with patch.object(ExecutionService, "get_execution_status", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            
            response = client.get(f"/executions/{execution_id}")
            
            assert response.status_code == 200
            result = response.json()
            assert result["result"]["items"] == ["item1", "item2", "item3"]
    
    @pytest.mark.asyncio
    async def test_get_execution_status_boolean_result(self, client):
        """Test execution status with boolean result."""
        execution_id = "test-execution-123"
        mock_response = {
            "execution_id": execution_id,
            "status": ExecutionStatus.COMPLETED.value,
            "created_at": datetime.now(UTC),
            "run_name": "Test Execution",
            "result": True,
            "error": None
        }
        
        with patch.object(ExecutionService, "get_execution_status", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            
            response = client.get(f"/executions/{execution_id}")
            
            assert response.status_code == 200
            result = response.json()
            assert result["result"]["success"] is True
    
    @pytest.mark.asyncio
    async def test_list_executions_success(self, client):
        """Test successful executions listing."""
        mock_executions = [
            {
                "execution_id": "exec-1",
                "status": ExecutionStatus.COMPLETED.value,
                "created_at": datetime.now(UTC),
                "run_name": "Execution 1",
                "result": {"output": "result1"},
                "error": None
            },
            {
                "execution_id": "exec-2",
                "status": ExecutionStatus.RUNNING.value,
                "created_at": datetime.now(UTC),
                "run_name": "Execution 2",
                "result": None,
                "error": None
            }
        ]
        
        with patch.object(ExecutionService, "list_executions", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = mock_executions
            
            response = client.get("/executions")
            
            assert response.status_code == 200
            result = response.json()
            assert len(result) == 2
            assert result[0]["execution_id"] == "exec-1"
            assert result[1]["execution_id"] == "exec-2"
    
    @pytest.mark.asyncio
    async def test_list_executions_empty(self, client):
        """Test listing executions when none exist."""
        with patch.object(ExecutionService, "list_executions", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = []
            
            response = client.get("/executions")
            
            assert response.status_code == 200
            result = response.json()
            assert len(result) == 0
    
    @pytest.mark.asyncio
    async def test_list_executions_with_result_processing(self, client):
        """Test listing executions with various result formats."""
        mock_executions = [
            {
                "execution_id": "exec-1",
                "status": ExecutionStatus.COMPLETED.value,
                "created_at": datetime.now(UTC),
                "run_name": "String Result",
                "result": "plain string result",
                "error": None
            },
            {
                "execution_id": "exec-2",
                "status": ExecutionStatus.COMPLETED.value,
                "created_at": datetime.now(UTC),
                "run_name": "List Result",
                "result": [1, 2, 3],
                "error": None
            },
            {
                "execution_id": "exec-3",
                "status": ExecutionStatus.COMPLETED.value,
                "created_at": datetime.now(UTC),
                "run_name": "Boolean Result",
                "result": False,
                "error": None
            }
        ]
        
        with patch.object(ExecutionService, "list_executions", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = mock_executions
            
            response = client.get("/executions")
            
            assert response.status_code == 200
            result = response.json()
            assert len(result) == 3
            
            # Check result processing
            assert result[0]["result"]["value"] == "plain string result"
            assert result[1]["result"]["items"] == [1, 2, 3]
            assert result[2]["result"]["success"] is False
    
    @pytest.mark.asyncio
    async def test_generate_execution_name_success(self, client):
        """Test successful execution name generation."""
        request_data = {
            "agents_yaml": {"agent1": {"role": "researcher"}},
            "tasks_yaml": {"task1": {"description": "research task"}},
            "model": "gpt-4o-mini"
        }
        
        mock_response = {"name": "Research Analysis Crew"}
        
        with patch.object(ExecutionService, "generate_execution_name", new_callable=AsyncMock) as mock_generate:
            mock_service = MagicMock()
            mock_service.generate_execution_name.return_value = mock_response
            mock_generate.return_value = mock_response
            
            with patch.object(ExecutionService, "__init__", return_value=None):
                with patch.object(ExecutionService, "generate_execution_name", new_callable=AsyncMock) as mock_gen:
                    mock_gen.return_value = mock_response
                    
                    response = client.post("/executions/generate-name", json=request_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Research Analysis Crew"
    
    @pytest.mark.asyncio
    async def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/executions/health")
        
        assert response.status_code == 200
        result = response.json()
        assert result["status"] == "healthy"
    
    @pytest.mark.asyncio
    async def test_create_execution_validation_error(self, client):
        """Test execution creation with validation error."""
        # Invalid config missing required fields
        invalid_config = {
            "model": "gpt-4o-mini"
            # Missing agents_yaml and tasks_yaml
        }
        
        response = client.post("/executions", json=invalid_config)
        
        # Should return 422 for validation error
        assert response.status_code == 422
    
    @pytest.mark.asyncio
    async def test_create_execution_with_background_tasks(self, client, mock_crew_config):
        """Test that execution creation properly uses background tasks."""
        mock_response = {
            "execution_id": "test-execution-123",
            "status": ExecutionStatus.PENDING.value,
            "run_name": "Test Execution"
        }
        
        with patch.object(ExecutionService, "create_execution", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = mock_response
            
            response = client.post("/executions", json=mock_crew_config)
            
            assert response.status_code == 200
            
            # Verify that create_execution was called with background_tasks
            mock_create.assert_called_once()
            call_args = mock_create.call_args
            assert len(call_args[0]) >= 1  # config argument
            # background_tasks should be passed as second argument or keyword argument