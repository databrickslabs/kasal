"""
Integration tests for execution workflows.

Tests end-to-end execution workflows including
crew and flow execution scenarios.
"""
import pytest
import uuid
import asyncio
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

from src.main import app
from src.schemas.execution import ExecutionStatus
from src.services.execution_service import ExecutionService


@pytest.fixture
def client():
    """Create test client for integration tests."""
    return TestClient(app)


@pytest.fixture
def sample_crew_execution_config():
    """Sample crew execution configuration."""
    return {
        "agents_yaml": {
            "researcher": {
                "role": "Senior Research Analyst",
                "goal": "Find and analyze relevant information",
                "backstory": "You are an expert research analyst with years of experience",
                "tools": ["web_search", "document_analyzer"]
            }
        },
        "tasks_yaml": {
            "research_task": {
                "description": "Research the latest trends in AI",
                "agent": "researcher",
                "expected_output": "A comprehensive report on AI trends"
            }
        },
        "model": "gpt-4o-mini",
        "planning": True,
        "execution_type": "crew",
        "inputs": {"topic": "artificial intelligence"},
        "schema_detection_enabled": True
    }


@pytest.fixture
def sample_flow_execution_config():
    """Sample flow execution configuration."""
    return {
        "agents_yaml": {},
        "tasks_yaml": {},
        "model": "gpt-4o-mini",
        "planning": False,
        "execution_type": "flow",
        "flow_id": str(uuid.uuid4()),
        "nodes": [
            {"id": "start", "type": "agent", "data": {"name": "Start Agent"}},
            {"id": "task1", "type": "task", "data": {"name": "Process Data"}},
            {"id": "end", "type": "task", "data": {"name": "Generate Report"}}
        ],
        "edges": [
            {"source": "start", "target": "task1", "data": {}},
            {"source": "task1", "target": "end", "data": {}}
        ],
        "inputs": {},
        "schema_detection_enabled": False
    }


class TestExecutionWorkflow:
    """Integration tests for execution workflows."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_crew_execution_end_to_end(self, client, sample_crew_execution_config):
        """Test complete crew execution workflow."""
        # Mock the dependencies
        with patch("src.services.execution_service.ExecutionStatusService") as mock_status_service, \
             patch("src.services.execution_service.CrewAIExecutionService") as mock_crew_service:
            
            # Setup mocks
            mock_status_service.create_execution.return_value = True
            mock_status_service.update_status.return_value = True
            
            mock_crew_instance = AsyncMock()
            mock_crew_instance.run_crew_execution.return_value = {
                "status": "completed",
                "result": {"output": "AI trends analysis completed"}
            }
            mock_crew_service.return_value = mock_crew_instance
            
            # Step 1: Create execution
            response = client.post("/api/v1/executions", json=sample_crew_execution_config)
            assert response.status_code == 200
            
            execution_data = response.json()
            execution_id = execution_data["execution_id"]
            assert execution_data["status"] == ExecutionStatus.PENDING.value
            assert "run_name" in execution_data
            
            # Step 2: Check execution status
            # Allow some time for background task to start
            await asyncio.sleep(0.1)
            
            # Mock the status check
            mock_execution_status = {
                "execution_id": execution_id,
                "status": ExecutionStatus.RUNNING.value,
                "created_at": datetime.now(UTC),
                "run_name": execution_data["run_name"],
                "result": None,
                "error": None
            }
            
            with patch.object(ExecutionService, "get_execution_status") as mock_get_status:
                mock_get_status.return_value = mock_execution_status
                
                response = client.get(f"/api/v1/executions/{execution_id}")
                assert response.status_code == 200
                
                status_data = response.json()
                assert status_data["execution_id"] == execution_id
                assert status_data["status"] == ExecutionStatus.RUNNING.value
            
            # Step 3: Simulate completion and check final status
            completed_status = {
                "execution_id": execution_id,
                "status": ExecutionStatus.COMPLETED.value,
                "created_at": datetime.now(UTC),
                "run_name": execution_data["run_name"],
                "result": {"output": "AI trends analysis completed"},
                "error": None
            }
            
            with patch.object(ExecutionService, "get_execution_status") as mock_get_status:
                mock_get_status.return_value = completed_status
                
                response = client.get(f"/api/v1/executions/{execution_id}")
                assert response.status_code == 200
                
                final_data = response.json()
                assert final_data["status"] == ExecutionStatus.COMPLETED.value
                assert final_data["result"]["output"] == "AI trends analysis completed"
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_flow_execution_end_to_end(self, client, sample_flow_execution_config):
        """Test complete flow execution workflow."""
        # Mock flow service and dependencies
        with patch("src.api.executions_router.FlowService") as mock_flow_service, \
             patch("src.services.execution_service.ExecutionStatusService") as mock_status_service, \
             patch("src.services.execution_service.CrewAIExecutionService") as mock_crew_service:
            
            # Setup flow service mock
            mock_flow_instance = AsyncMock()
            mock_flow = MagicMock()
            mock_flow.name = "Test Flow"
            mock_flow.id = uuid.UUID(sample_flow_execution_config["flow_id"])
            mock_flow_instance.get_flow.return_value = mock_flow
            mock_flow_service.return_value = mock_flow_instance
            
            # Setup execution service mocks
            mock_status_service.create_execution.return_value = True
            mock_status_service.update_status.return_value = True
            
            mock_crew_instance = AsyncMock()
            mock_crew_instance.run_flow_execution.return_value = {
                "status": "completed",
                "result": {"flow_output": "Flow execution successful"}
            }
            mock_crew_service.return_value = mock_crew_instance
            
            # Step 1: Create flow execution
            response = client.post("/api/v1/executions", json=sample_flow_execution_config)
            assert response.status_code == 200
            
            execution_data = response.json()
            execution_id = execution_data["execution_id"]
            assert execution_data["status"] == ExecutionStatus.PENDING.value
            
            # Step 2: Verify flow was validated
            mock_flow_instance.get_flow.assert_called_once_with(uuid.UUID(sample_flow_execution_config["flow_id"]))
            
            # Step 3: Check execution progression
            running_status = {
                "execution_id": execution_id,
                "status": ExecutionStatus.RUNNING.value,
                "created_at": datetime.now(UTC),
                "run_name": execution_data["run_name"],
                "result": None,
                "error": None
            }
            
            with patch.object(ExecutionService, "get_execution_status") as mock_get_status:
                mock_get_status.return_value = running_status
                
                response = client.get(f"/api/v1/executions/{execution_id}")
                assert response.status_code == 200
                assert response.json()["status"] == ExecutionStatus.RUNNING.value
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_execution_error_handling(self, client, sample_crew_execution_config):
        """Test execution error handling workflow."""
        with patch("src.services.execution_service.ExecutionStatusService") as mock_status_service, \
             patch("src.services.execution_service.CrewAIExecutionService") as mock_crew_service:
            
            # Setup mocks to simulate error
            mock_status_service.create_execution.return_value = True
            mock_status_service.update_status.return_value = True
            
            mock_crew_instance = AsyncMock()
            mock_crew_instance.run_crew_execution.side_effect = Exception("Execution failed")
            mock_crew_service.return_value = mock_crew_instance
            
            # Step 1: Create execution
            response = client.post("/api/v1/executions", json=sample_crew_execution_config)
            assert response.status_code == 200
            
            execution_id = response.json()["execution_id"]
            
            # Step 2: Check that error is handled properly
            failed_status = {
                "execution_id": execution_id,
                "status": ExecutionStatus.FAILED.value,
                "created_at": datetime.now(UTC),
                "run_name": "Test Execution",
                "result": None,
                "error": "Execution failed"
            }
            
            with patch.object(ExecutionService, "get_execution_status") as mock_get_status:
                mock_get_status.return_value = failed_status
                
                response = client.get(f"/api/v1/executions/{execution_id}")
                assert response.status_code == 200
                
                status_data = response.json()
                assert status_data["status"] == ExecutionStatus.FAILED.value
                assert status_data["error"] == "Execution failed"
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_execution_list_workflow(self, client):
        """Test execution listing workflow."""
        # Mock multiple executions
        mock_executions = [
            {
                "execution_id": "exec-1",
                "status": ExecutionStatus.COMPLETED.value,
                "created_at": datetime.now(UTC),
                "run_name": "Completed Execution",
                "result": {"output": "success"},
                "error": None
            },
            {
                "execution_id": "exec-2", 
                "status": ExecutionStatus.RUNNING.value,
                "created_at": datetime.now(UTC),
                "run_name": "Running Execution",
                "result": None,
                "error": None
            },
            {
                "execution_id": "exec-3",
                "status": ExecutionStatus.FAILED.value,
                "created_at": datetime.now(UTC),
                "run_name": "Failed Execution",
                "result": None,
                "error": "Something went wrong"
            }
        ]
        
        with patch.object(ExecutionService, "list_executions") as mock_list:
            mock_list.return_value = mock_executions
            
            response = client.get("/api/v1/executions")
            assert response.status_code == 200
            
            executions_data = response.json()
            assert len(executions_data) == 3
            
            # Verify different statuses are represented
            statuses = [exec_data["status"] for exec_data in executions_data]
            assert ExecutionStatus.COMPLETED.value in statuses
            assert ExecutionStatus.RUNNING.value in statuses
            assert ExecutionStatus.FAILED.value in statuses
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_execution_name_generation_workflow(self, client):
        """Test execution name generation workflow."""
        request_data = {
            "agents_yaml": {
                "researcher": {
                    "role": "Research Analyst",
                    "goal": "Research market trends"
                }
            },
            "tasks_yaml": {
                "analysis": {
                    "description": "Analyze market data and trends"
                }
            },
            "model": "gpt-4o-mini"
        }
        
        mock_response = {"name": "Market Research Analysis"}
        
        with patch("src.services.execution_name_service.ExecutionNameService") as mock_name_service:
            mock_instance = AsyncMock()
            mock_instance.generate_execution_name.return_value = mock_response
            mock_name_service.create.return_value = mock_instance
            
            response = client.post("/api/v1/executions/generate-name", json=request_data)
            assert response.status_code == 200
            
            result = response.json()
            assert result["name"] == "Market Research Analysis"
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_invalid_flow_id_workflow(self, client, sample_flow_execution_config):
        """Test workflow with invalid flow ID."""
        # Use invalid flow ID
        sample_flow_execution_config["flow_id"] = "invalid-uuid-format"
        
        response = client.post("/api/v1/executions", json=sample_flow_execution_config)
        assert response.status_code == 400
        assert "Invalid flow_id format" in response.json()["detail"]
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_nonexistent_flow_workflow(self, client, sample_flow_execution_config):
        """Test workflow with non-existent flow."""
        with patch("src.api.executions_router.FlowService") as mock_flow_service:
            mock_flow_instance = AsyncMock()
            mock_flow_instance.get_flow.side_effect = Exception("Flow not found")
            mock_flow_service.return_value = mock_flow_instance
            
            response = client.post("/api/v1/executions", json=sample_flow_execution_config)
            assert response.status_code == 400
    
    @pytest.mark.integration
    @pytest.mark.slow
    async def test_concurrent_executions(self, client, sample_crew_execution_config):
        """Test handling of concurrent executions."""
        with patch("src.services.execution_service.ExecutionStatusService") as mock_status_service, \
             patch("src.services.execution_service.CrewAIExecutionService") as mock_crew_service:
            
            # Setup mocks
            mock_status_service.create_execution.return_value = True
            mock_crew_instance = AsyncMock()
            mock_crew_instance.run_crew_execution.return_value = {"status": "completed"}
            mock_crew_service.return_value = mock_crew_instance
            
            # Create multiple executions concurrently
            tasks = []
            for i in range(5):
                config = sample_crew_execution_config.copy()
                config["inputs"]["iteration"] = i
                tasks.append(asyncio.create_task(
                    asyncio.to_thread(client.post, "/api/v1/executions", json=config)
                ))
            
            # Wait for all executions to be created
            responses = await asyncio.gather(*tasks)
            
            # Verify all executions were created successfully
            execution_ids = []
            for response in responses:
                assert response.status_code == 200
                execution_ids.append(response.json()["execution_id"])
            
            # Verify all execution IDs are unique
            assert len(set(execution_ids)) == 5
    
    @pytest.mark.integration
    async def test_health_check_workflow(self, client):
        """Test health check workflow."""
        response = client.get("/api/v1/executions/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"