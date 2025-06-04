"""
Unit tests for flows API router.

Tests the functionality of the flows API endpoints including
creating, getting, updating, and deleting flows.
"""
import pytest
import uuid
import json
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from src.api.flows_router import router
from src.schemas.flow import FlowCreate, FlowUpdate, FlowResponse
from src.services.flow_service import FlowService


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
def mock_flow_data():
    """Create mock flow data."""
    return {
        "name": "Test Flow",
        "description": "A test flow for unit testing",
        "nodes": [
            {"id": "node1", "type": "agent", "data": {"name": "Agent 1"}},
            {"id": "node2", "type": "task", "data": {"name": "Task 1"}}
        ],
        "edges": [
            {"source": "node1", "target": "node2", "data": {}}
        ],
        "flow_config": {"timeout": 300, "retries": 3}
    }


@pytest.fixture
def mock_flow_response():
    """Create mock flow response data."""
    return {
        "id": str(uuid.uuid4()),
        "name": "Test Flow",
        "description": "A test flow for unit testing",
        "nodes": [
            {"id": "node1", "type": "agent", "data": {"name": "Agent 1"}},
            {"id": "node2", "type": "task", "data": {"name": "Task 1"}}
        ],
        "edges": [
            {"source": "node1", "target": "node2", "data": {}}
        ],
        "flow_config": {"timeout": 300, "retries": 3},
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
        "is_active": True
    }


class TestFlowsRouter:
    """Test cases for flows router."""
    
    @pytest.mark.asyncio
    async def test_create_flow_success(self, client, mock_flow_data, mock_flow_response):
        """Test successful flow creation."""
        with patch.object(FlowService, "create", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = MagicMock(**mock_flow_response)
            
            response = client.post("/flows", json=mock_flow_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Test Flow"
            assert result["description"] == "A test flow for unit testing"
            assert len(result["nodes"]) == 2
            assert len(result["edges"]) == 1
    
    @pytest.mark.asyncio
    async def test_create_flow_validation_error(self, client):
        """Test flow creation with validation error."""
        # Invalid flow data (missing required fields)
        invalid_data = {
            "name": "Test Flow"
            # Missing description, nodes, edges
        }
        
        response = client.post("/flows", json=invalid_data)
        
        # Should return 422 for validation error
        assert response.status_code == 422
    
    @pytest.mark.asyncio
    async def test_create_flow_service_error(self, client, mock_flow_data):
        """Test flow creation with service error."""
        with patch.object(FlowService, "create", new_callable=AsyncMock) as mock_create:
            mock_create.side_effect = ValueError("Invalid flow configuration")
            
            response = client.post("/flows", json=mock_flow_data)
            
            assert response.status_code == 400
            assert "Invalid flow configuration" in response.json()["detail"]
    
    @pytest.mark.asyncio
    async def test_get_flow_success(self, client, mock_flow_response):
        """Test successful flow retrieval."""
        flow_id = str(uuid.uuid4())
        
        with patch.object(FlowService, "get_flow", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = MagicMock(**mock_flow_response)
            
            response = client.get(f"/flows/{flow_id}")
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Test Flow"
    
    @pytest.mark.asyncio
    async def test_get_flow_not_found(self, client):
        """Test flow retrieval for non-existent flow."""
        flow_id = str(uuid.uuid4())
        
        with patch.object(FlowService, "get_flow", new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = HTTPException(status_code=404, detail="Flow not found")
            
            response = client.get(f"/flows/{flow_id}")
            
            assert response.status_code == 404
            assert response.json()["detail"] == "Flow not found"
    
    @pytest.mark.asyncio
    async def test_get_flow_invalid_uuid(self, client):
        """Test flow retrieval with invalid UUID."""
        invalid_id = "invalid-uuid"
        
        response = client.get(f"/flows/{invalid_id}")
        
        assert response.status_code == 422  # Validation error
    
    @pytest.mark.asyncio
    async def test_update_flow_success(self, client, mock_flow_response):
        """Test successful flow update."""
        flow_id = str(uuid.uuid4())
        update_data = {
            "name": "Updated Flow",
            "description": "Updated description"
        }
        
        with patch.object(FlowService, "update", new_callable=AsyncMock) as mock_update:
            updated_response = mock_flow_response.copy()
            updated_response.update(update_data)
            mock_update.return_value = MagicMock(**updated_response)
            
            response = client.put(f"/flows/{flow_id}", json=update_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Updated Flow"
            assert result["description"] == "Updated description"
    
    @pytest.mark.asyncio
    async def test_update_flow_not_found(self, client):
        """Test flow update for non-existent flow."""
        flow_id = str(uuid.uuid4())
        update_data = {"name": "Updated Flow"}
        
        with patch.object(FlowService, "update", new_callable=AsyncMock) as mock_update:
            mock_update.return_value = None
            
            response = client.put(f"/flows/{flow_id}", json=update_data)
            
            assert response.status_code == 404
            assert response.json()["detail"] == "Flow not found"
    
    @pytest.mark.asyncio
    async def test_delete_flow_success(self, client):
        """Test successful flow deletion."""
        flow_id = str(uuid.uuid4())
        
        with patch.object(FlowService, "delete", new_callable=AsyncMock) as mock_delete:
            mock_delete.return_value = True
            
            response = client.delete(f"/flows/{flow_id}")
            
            assert response.status_code == 200
            result = response.json()
            assert result["message"] == "Flow deleted successfully"
    
    @pytest.mark.asyncio
    async def test_delete_flow_not_found(self, client):
        """Test flow deletion for non-existent flow."""
        flow_id = str(uuid.uuid4())
        
        with patch.object(FlowService, "delete", new_callable=AsyncMock) as mock_delete:
            mock_delete.return_value = False
            
            response = client.delete(f"/flows/{flow_id}")
            
            assert response.status_code == 404
            assert response.json()["detail"] == "Flow not found"
    
    @pytest.mark.asyncio
    async def test_list_flows_success(self, client, mock_flow_response):
        """Test successful flows listing."""
        mock_flows = [
            MagicMock(**mock_flow_response),
            MagicMock(**{**mock_flow_response, "name": "Flow 2", "id": str(uuid.uuid4())})
        ]
        
        with patch.object(FlowService, "list", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = mock_flows
            
            response = client.get("/flows")
            
            assert response.status_code == 200
            result = response.json()
            assert len(result) == 2
            assert result[0]["name"] == "Test Flow"
            assert result[1]["name"] == "Flow 2"
    
    @pytest.mark.asyncio
    async def test_list_flows_empty(self, client):
        """Test flows listing when no flows exist."""
        with patch.object(FlowService, "list", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = []
            
            response = client.get("/flows")
            
            assert response.status_code == 200
            result = response.json()
            assert len(result) == 0
    
    @pytest.mark.asyncio
    async def test_search_flows_success(self, client, mock_flow_response):
        """Test successful flow search."""
        search_query = "test"
        mock_flows = [MagicMock(**mock_flow_response)]
        
        with patch.object(FlowService, "search", new_callable=AsyncMock) as mock_search:
            mock_search.return_value = mock_flows
            
            response = client.get(f"/flows/search?q={search_query}")
            
            assert response.status_code == 200
            result = response.json()
            assert len(result) == 1
            assert result[0]["name"] == "Test Flow"
            mock_search.assert_called_once_with(search_query)
    
    @pytest.mark.asyncio
    async def test_search_flows_no_results(self, client):
        """Test flow search with no results."""
        search_query = "nonexistent"
        
        with patch.object(FlowService, "search", new_callable=AsyncMock) as mock_search:
            mock_search.return_value = []
            
            response = client.get(f"/flows/search?q={search_query}")
            
            assert response.status_code == 200
            result = response.json()
            assert len(result) == 0
    
    @pytest.mark.asyncio
    async def test_activate_flow_success(self, client, mock_flow_response):
        """Test successful flow activation."""
        flow_id = str(uuid.uuid4())
        
        with patch.object(FlowService, "activate", new_callable=AsyncMock) as mock_activate:
            activated_response = mock_flow_response.copy()
            activated_response["is_active"] = True
            mock_activate.return_value = MagicMock(**activated_response)
            
            response = client.post(f"/flows/{flow_id}/activate")
            
            assert response.status_code == 200
            result = response.json()
            assert result["is_active"] is True
    
    @pytest.mark.asyncio
    async def test_deactivate_flow_success(self, client, mock_flow_response):
        """Test successful flow deactivation."""
        flow_id = str(uuid.uuid4())
        
        with patch.object(FlowService, "deactivate", new_callable=AsyncMock) as mock_deactivate:
            deactivated_response = mock_flow_response.copy()
            deactivated_response["is_active"] = False
            mock_deactivate.return_value = MagicMock(**deactivated_response)
            
            response = client.post(f"/flows/{flow_id}/deactivate")
            
            assert response.status_code == 200
            result = response.json()
            assert result["is_active"] is False
    
    @pytest.mark.asyncio
    async def test_clone_flow_success(self, client, mock_flow_response):
        """Test successful flow cloning."""
        flow_id = str(uuid.uuid4())
        clone_data = {"new_name": "Cloned Flow"}
        
        with patch.object(FlowService, "clone_flow", new_callable=AsyncMock) as mock_clone:
            cloned_response = mock_flow_response.copy()
            cloned_response["name"] = "Cloned Flow"
            cloned_response["id"] = str(uuid.uuid4())
            mock_clone.return_value = MagicMock(**cloned_response)
            
            response = client.post(f"/flows/{flow_id}/clone", json=clone_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Cloned Flow"
            assert result["id"] != mock_flow_response["id"]
    
    @pytest.mark.asyncio
    async def test_export_flow_success(self, client):
        """Test successful flow export."""
        flow_id = str(uuid.uuid4())
        export_data = {
            "name": "Test Flow",
            "description": "A test flow",
            "nodes": [],
            "edges": [],
            "flow_config": {}
        }
        
        with patch.object(FlowService, "export_flow", new_callable=AsyncMock) as mock_export:
            mock_export.return_value = export_data
            
            response = client.get(f"/flows/{flow_id}/export")
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Test Flow"
            assert "nodes" in result
            assert "edges" in result
    
    @pytest.mark.asyncio
    async def test_import_flow_success(self, client, mock_flow_response):
        """Test successful flow import."""
        import_data = {
            "name": "Imported Flow",
            "description": "An imported flow",
            "nodes": [{"id": "node1", "type": "agent", "data": {}}],
            "edges": [],
            "flow_config": {}
        }
        
        with patch.object(FlowService, "import_flow", new_callable=AsyncMock) as mock_import:
            imported_response = mock_flow_response.copy()
            imported_response["name"] = "Imported Flow"
            mock_import.return_value = MagicMock(**imported_response)
            
            response = client.post("/flows/import", json=import_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Imported Flow"
    
    @pytest.mark.asyncio
    async def test_validate_flow_success(self, client):
        """Test successful flow validation."""
        flow_data = {
            "nodes": [
                {"id": "node1", "type": "agent", "data": {"name": "Agent 1"}},
                {"id": "node2", "type": "task", "data": {"name": "Task 1"}}
            ],
            "edges": [
                {"source": "node1", "target": "node2", "data": {}}
            ]
        }
        
        with patch.object(FlowService, "validate_flow", new_callable=AsyncMock) as mock_validate:
            mock_validate.return_value = {"valid": True, "errors": []}
            
            response = client.post("/flows/validate", json=flow_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["valid"] is True
            assert len(result["errors"]) == 0
    
    @pytest.mark.asyncio
    async def test_validate_flow_with_errors(self, client):
        """Test flow validation with errors."""
        invalid_flow_data = {
            "nodes": [
                {"id": "node1", "type": "agent", "data": {"name": "Agent 1"}}
            ],
            "edges": [
                {"source": "node1", "target": "node2", "data": {}}  # node2 doesn't exist
            ]
        }
        
        with patch.object(FlowService, "validate_flow", new_callable=AsyncMock) as mock_validate:
            mock_validate.return_value = {
                "valid": False, 
                "errors": ["Edge references non-existent node: node2"]
            }
            
            response = client.post("/flows/validate", json=invalid_flow_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["valid"] is False
            assert len(result["errors"]) == 1
            assert "node2" in result["errors"][0]
    
    @pytest.mark.asyncio
    async def test_get_flow_metrics_success(self, client):
        """Test successful flow metrics retrieval."""
        metrics_data = {
            "total_flows": 10,
            "active_flows": 8,
            "flows_by_type": {"agent": 5, "task": 3, "condition": 2},
            "average_execution_time": 125.5
        }
        
        with patch.object(FlowService, "get_flow_metrics", new_callable=AsyncMock) as mock_metrics:
            mock_metrics.return_value = metrics_data
            
            response = client.get("/flows/metrics")
            
            assert response.status_code == 200
            result = response.json()
            assert result["total_flows"] == 10
            assert result["active_flows"] == 8
            assert "flows_by_type" in result
    
    @pytest.mark.asyncio
    async def test_duplicate_flow_name_error(self, client, mock_flow_data):
        """Test flow creation with duplicate name."""
        with patch.object(FlowService, "create", new_callable=AsyncMock) as mock_create:
            mock_create.side_effect = ValueError("Flow name already exists")
            
            response = client.post("/flows", json=mock_flow_data)
            
            assert response.status_code == 400
            assert "Flow name already exists" in response.json()["detail"]
    
    @pytest.mark.asyncio
    async def test_flow_with_complex_nodes(self, client, mock_flow_response):
        """Test flow creation with complex node structure."""
        complex_flow_data = {
            "name": "Complex Flow",
            "description": "A flow with complex nodes",
            "nodes": [
                {
                    "id": "start",
                    "type": "agent",
                    "data": {
                        "name": "Start Agent",
                        "role": "initiator",
                        "config": {"timeout": 30}
                    }
                },
                {
                    "id": "condition",
                    "type": "condition",
                    "data": {
                        "condition": "x > 0",
                        "true_path": "success_task",
                        "false_path": "failure_task"
                    }
                },
                {
                    "id": "success_task",
                    "type": "task",
                    "data": {
                        "name": "Success Task",
                        "description": "Task for successful condition"
                    }
                },
                {
                    "id": "failure_task",
                    "type": "task",
                    "data": {
                        "name": "Failure Task",
                        "description": "Task for failed condition"
                    }
                }
            ],
            "edges": [
                {"source": "start", "target": "condition", "data": {}},
                {"source": "condition", "target": "success_task", "data": {"condition": "true"}},
                {"source": "condition", "target": "failure_task", "data": {"condition": "false"}}
            ],
            "flow_config": {
                "timeout": 600,
                "retries": 2,
                "parallel_execution": True
            }
        }
        
        with patch.object(FlowService, "create", new_callable=AsyncMock) as mock_create:
            complex_response = mock_flow_response.copy()
            complex_response.update({
                "name": "Complex Flow",
                "nodes": complex_flow_data["nodes"],
                "edges": complex_flow_data["edges"]
            })
            mock_create.return_value = MagicMock(**complex_response)
            
            response = client.post("/flows", json=complex_flow_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Complex Flow"
            assert len(result["nodes"]) == 4
            assert len(result["edges"]) == 3