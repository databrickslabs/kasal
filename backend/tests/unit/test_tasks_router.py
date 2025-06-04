"""
Unit tests for tasks API router.

Tests the functionality of the tasks API endpoints including
creating, getting, updating, and deleting tasks.
"""
import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from src.api.tasks_router import router
from src.schemas.task import TaskCreate, TaskUpdate, TaskResponse
from src.services.task_service import TaskService


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
def mock_task_data():
    """Create mock task data."""
    return {
        "name": "Research Task",
        "description": "Conduct thorough research on the given topic",
        "agent": "researcher",
        "expected_output": "A comprehensive research report with findings",
        "context": ["previous_research"],
        "tools": ["web_search", "document_analyzer"],
        "async_execution": False,
        "guardrails": ["data_quality_check"]
    }


@pytest.fixture
def mock_task_response():
    """Create mock task response data."""
    return {
        "id": str(uuid.uuid4()),
        "name": "Research Task",
        "description": "Conduct thorough research on the given topic",
        "agent": "researcher",
        "expected_output": "A comprehensive research report with findings",
        "context": ["previous_research"],
        "tools": ["web_search", "document_analyzer"],
        "async_execution": False,
        "guardrails": ["data_quality_check"],
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
        "is_active": True
    }


class TestTasksRouter:
    """Test cases for tasks router."""
    
    @pytest.mark.asyncio
    async def test_create_task_success(self, client, mock_task_data, mock_task_response):
        """Test successful task creation."""
        with patch.object(TaskService, "create", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = MagicMock(**mock_task_response)
            
            response = client.post("/tasks", json=mock_task_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Research Task"
            assert result["agent"] == "researcher"
            assert result["expected_output"] == "A comprehensive research report with findings"
    
    @pytest.mark.asyncio
    async def test_create_task_validation_error(self, client):
        """Test task creation with validation error."""
        # Invalid task data (missing required fields)
        invalid_data = {
            "name": "Incomplete Task"
            # Missing description, agent, expected_output
        }
        
        response = client.post("/tasks", json=invalid_data)
        
        # Should return 422 for validation error
        assert response.status_code == 422
    
    @pytest.mark.asyncio
    async def test_create_task_service_error(self, client, mock_task_data):
        """Test task creation with service error."""
        with patch.object(TaskService, "create", new_callable=AsyncMock) as mock_create:
            mock_create.side_effect = ValueError("Invalid task configuration")
            
            response = client.post("/tasks", json=mock_task_data)
            
            assert response.status_code == 400
            assert "Invalid task configuration" in response.json()["detail"]
    
    @pytest.mark.asyncio
    async def test_get_task_success(self, client, mock_task_response):
        """Test successful task retrieval."""
        task_id = str(uuid.uuid4())
        
        with patch.object(TaskService, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = MagicMock(**mock_task_response)
            
            response = client.get(f"/tasks/{task_id}")
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Research Task"
            assert result["agent"] == "researcher"
    
    @pytest.mark.asyncio
    async def test_get_task_not_found(self, client):
        """Test task retrieval for non-existent task."""
        task_id = str(uuid.uuid4())
        
        with patch.object(TaskService, "get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = None
            
            response = client.get(f"/tasks/{task_id}")
            
            assert response.status_code == 404
            assert response.json()["detail"] == "Task not found"
    
    @pytest.mark.asyncio
    async def test_get_task_invalid_uuid(self, client):
        """Test task retrieval with invalid UUID."""
        invalid_id = "invalid-uuid"
        
        response = client.get(f"/tasks/{invalid_id}")
        
        assert response.status_code == 422  # Validation error
    
    @pytest.mark.asyncio
    async def test_update_task_success(self, client, mock_task_response):
        """Test successful task update."""
        task_id = str(uuid.uuid4())
        update_data = {
            "name": "Updated Research Task",
            "description": "Updated task description"
        }
        
        with patch.object(TaskService, "update", new_callable=AsyncMock) as mock_update:
            updated_response = mock_task_response.copy()
            updated_response.update(update_data)
            mock_update.return_value = MagicMock(**updated_response)
            
            response = client.put(f"/tasks/{task_id}", json=update_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Updated Research Task"
            assert result["description"] == "Updated task description"
    
    @pytest.mark.asyncio
    async def test_update_task_not_found(self, client):
        """Test task update for non-existent task."""
        task_id = str(uuid.uuid4())
        update_data = {"name": "Updated Task"}
        
        with patch.object(TaskService, "update", new_callable=AsyncMock) as mock_update:
            mock_update.return_value = None
            
            response = client.put(f"/tasks/{task_id}", json=update_data)
            
            assert response.status_code == 404
            assert response.json()["detail"] == "Task not found"
    
    @pytest.mark.asyncio
    async def test_delete_task_success(self, client):
        """Test successful task deletion."""
        task_id = str(uuid.uuid4())
        
        with patch.object(TaskService, "delete", new_callable=AsyncMock) as mock_delete:
            mock_delete.return_value = True
            
            response = client.delete(f"/tasks/{task_id}")
            
            assert response.status_code == 200
            result = response.json()
            assert result["message"] == "Task deleted successfully"
    
    @pytest.mark.asyncio
    async def test_delete_task_not_found(self, client):
        """Test task deletion for non-existent task."""
        task_id = str(uuid.uuid4())
        
        with patch.object(TaskService, "delete", new_callable=AsyncMock) as mock_delete:
            mock_delete.return_value = False
            
            response = client.delete(f"/tasks/{task_id}")
            
            assert response.status_code == 404
            assert response.json()["detail"] == "Task not found"
    
    @pytest.mark.asyncio
    async def test_list_tasks_success(self, client, mock_task_response):
        """Test successful tasks listing."""
        mock_tasks = [
            MagicMock(**mock_task_response),
            MagicMock(**{**mock_task_response, "name": "Task 2", "id": str(uuid.uuid4())})
        ]
        
        with patch.object(TaskService, "list", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = mock_tasks
            
            response = client.get("/tasks")
            
            assert response.status_code == 200
            result = response.json()
            assert len(result) == 2
            assert result[0]["name"] == "Research Task"
            assert result[1]["name"] == "Task 2"
    
    @pytest.mark.asyncio
    async def test_list_tasks_empty(self, client):
        """Test tasks listing when no tasks exist."""
        with patch.object(TaskService, "list", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = []
            
            response = client.get("/tasks")
            
            assert response.status_code == 200
            result = response.json()
            assert len(result) == 0
    
    @pytest.mark.asyncio
    async def test_search_tasks_success(self, client, mock_task_response):
        """Test successful task search."""
        search_query = "research"
        mock_tasks = [MagicMock(**mock_task_response)]
        
        with patch.object(TaskService, "search", new_callable=AsyncMock) as mock_search:
            mock_search.return_value = mock_tasks
            
            response = client.get(f"/tasks/search?q={search_query}")
            
            assert response.status_code == 200
            result = response.json()
            assert len(result) == 1
            assert result[0]["name"] == "Research Task"
            mock_search.assert_called_once_with(search_query)
    
    @pytest.mark.asyncio
    async def test_search_tasks_no_results(self, client):
        """Test task search with no results."""
        search_query = "nonexistent"
        
        with patch.object(TaskService, "search", new_callable=AsyncMock) as mock_search:
            mock_search.return_value = []
            
            response = client.get(f"/tasks/search?q={search_query}")
            
            assert response.status_code == 200
            result = response.json()
            assert len(result) == 0
    
    @pytest.mark.asyncio
    async def test_validate_task_dependencies_success(self, client):
        """Test successful task dependency validation."""
        task_data = {
            "name": "Dependent Task",
            "description": "A task with dependencies",
            "agent": "worker",
            "expected_output": "Task output",
            "context": ["task1", "task2"]
        }
        
        with patch.object(TaskService, "validate_dependencies", new_callable=AsyncMock) as mock_validate:
            mock_validate.return_value = {"valid": True, "errors": []}
            
            response = client.post("/tasks/validate-dependencies", json=task_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["valid"] is True
            assert len(result["errors"]) == 0
    
    @pytest.mark.asyncio
    async def test_validate_task_dependencies_with_errors(self, client):
        """Test task dependency validation with errors."""
        task_data = {
            "name": "Invalid Dependent Task",
            "description": "A task with invalid dependencies",
            "agent": "worker",
            "expected_output": "Task output",
            "context": ["nonexistent_task"]
        }
        
        with patch.object(TaskService, "validate_dependencies", new_callable=AsyncMock) as mock_validate:
            mock_validate.return_value = {
                "valid": False,
                "errors": ["Task dependency not found: nonexistent_task"]
            }
            
            response = client.post("/tasks/validate-dependencies", json=task_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["valid"] is False
            assert len(result["errors"]) == 1
            assert "nonexistent_task" in result["errors"][0]
    
    @pytest.mark.asyncio
    async def test_clone_task_success(self, client, mock_task_response):
        """Test successful task cloning."""
        task_id = str(uuid.uuid4())
        clone_data = {"new_name": "Cloned Task"}
        
        with patch.object(TaskService, "clone_task", new_callable=AsyncMock) as mock_clone:
            cloned_response = mock_task_response.copy()
            cloned_response["name"] = "Cloned Task"
            cloned_response["id"] = str(uuid.uuid4())
            mock_clone.return_value = MagicMock(**cloned_response)
            
            response = client.post(f"/tasks/{task_id}/clone", json=clone_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Cloned Task"
            assert result["id"] != mock_task_response["id"]
    
    @pytest.mark.asyncio
    async def test_get_task_execution_order(self, client):
        """Test getting task execution order."""
        tasks_data = [
            {"id": "task1", "name": "Task 1", "context": []},
            {"id": "task2", "name": "Task 2", "context": ["task1"]},
            {"id": "task3", "name": "Task 3", "context": ["task1", "task2"]}
        ]
        
        with patch.object(TaskService, "calculate_execution_order", new_callable=AsyncMock) as mock_order:
            mock_order.return_value = [
                {"id": "task1", "name": "Task 1", "order": 1},
                {"id": "task2", "name": "Task 2", "order": 2},
                {"id": "task3", "name": "Task 3", "order": 3}
            ]
            
            response = client.post("/tasks/execution-order", json={"tasks": tasks_data})
            
            assert response.status_code == 200
            result = response.json()
            assert len(result) == 3
            assert result[0]["order"] == 1
            assert result[1]["order"] == 2
            assert result[2]["order"] == 3
    
    @pytest.mark.asyncio
    async def test_get_task_metrics_success(self, client):
        """Test successful task metrics retrieval."""
        metrics_data = {
            "total_tasks": 25,
            "active_tasks": 20,
            "tasks_by_agent": {"researcher": 10, "writer": 8, "analyst": 7},
            "average_execution_time": 180.5,
            "success_rate": 0.95
        }
        
        with patch.object(TaskService, "get_task_metrics", new_callable=AsyncMock) as mock_metrics:
            mock_metrics.return_value = metrics_data
            
            response = client.get("/tasks/metrics")
            
            assert response.status_code == 200
            result = response.json()
            assert result["total_tasks"] == 25
            assert result["active_tasks"] == 20
            assert "tasks_by_agent" in result
            assert result["success_rate"] == 0.95
    
    @pytest.mark.asyncio
    async def test_update_task_execution_status(self, client):
        """Test updating task execution status."""
        task_id = str(uuid.uuid4())
        status_data = {
            "status": "completed",
            "execution_time": 120.5,
            "output": "Task completed successfully"
        }
        
        with patch.object(TaskService, "update_execution_status", new_callable=AsyncMock) as mock_update:
            mock_update.return_value = {"status": "updated"}
            
            response = client.put(f"/tasks/{task_id}/execution-status", json=status_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["status"] == "updated"
    
    @pytest.mark.asyncio
    async def test_create_task_template(self, client):
        """Test creating task template."""
        task_id = str(uuid.uuid4())
        
        template_data = {
            "name": "Research Template",
            "description": "Template for research tasks",
            "agent": "researcher",
            "expected_output": "Research findings",
            "tools": ["web_search"],
            "guardrails": ["quality_check"]
        }
        
        with patch.object(TaskService, "create_template", new_callable=AsyncMock) as mock_template:
            mock_template.return_value = template_data
            
            response = client.post(f"/tasks/{task_id}/template")
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Research Template"
            assert "tools" in result
            assert "guardrails" in result
    
    @pytest.mark.asyncio
    async def test_apply_task_template(self, client, mock_task_response):
        """Test applying task template."""
        template_id = str(uuid.uuid4())
        apply_data = {
            "name": "Task from Template",
            "description": "Task created from template"
        }
        
        with patch.object(TaskService, "apply_template", new_callable=AsyncMock) as mock_apply:
            template_response = mock_task_response.copy()
            template_response.update(apply_data)
            mock_apply.return_value = MagicMock(**template_response)
            
            response = client.post(f"/tasks/templates/{template_id}/apply", json=apply_data)
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Task from Template"
    
    @pytest.mark.asyncio
    async def test_task_with_guardrails(self, client, mock_task_response):
        """Test task creation with guardrails."""
        task_with_guardrails = {
            "name": "Guarded Task",
            "description": "A task with guardrails",
            "agent": "worker",
            "expected_output": "Safe output",
            "guardrails": [
                "data_quality_check",
                "output_validation",
                "safety_check"
            ]
        }
        
        with patch.object(TaskService, "create", new_callable=AsyncMock) as mock_create:
            guarded_response = mock_task_response.copy()
            guarded_response.update(task_with_guardrails)
            mock_create.return_value = MagicMock(**guarded_response)
            
            response = client.post("/tasks", json=task_with_guardrails)
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Guarded Task"
            assert len(result["guardrails"]) == 3
            assert "data_quality_check" in result["guardrails"]
    
    @pytest.mark.asyncio
    async def test_task_with_complex_context(self, client, mock_task_response):
        """Test task creation with complex context dependencies."""
        complex_task = {
            "name": "Complex Dependent Task",
            "description": "A task with multiple dependencies",
            "agent": "coordinator",
            "expected_output": "Coordinated result",
            "context": [
                "research_task",
                "analysis_task",
                "validation_task"
            ],
            "tools": ["aggregator", "validator"],
            "async_execution": True
        }
        
        with patch.object(TaskService, "create", new_callable=AsyncMock) as mock_create:
            complex_response = mock_task_response.copy()
            complex_response.update(complex_task)
            mock_create.return_value = MagicMock(**complex_response)
            
            response = client.post("/tasks", json=complex_task)
            
            assert response.status_code == 200
            result = response.json()
            assert result["name"] == "Complex Dependent Task"
            assert len(result["context"]) == 3
            assert result["async_execution"] is True