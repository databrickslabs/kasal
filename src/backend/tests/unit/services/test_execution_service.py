"""
Unit tests for ExecutionService.

Tests the functionality of execution operations including
flow execution, crew execution, status tracking, and execution management.
"""
import pytest
import uuid
import json
import concurrent.futures
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from datetime import datetime, UTC

from fastapi import HTTPException, status

from src.services.execution_service import ExecutionService
from src.schemas.execution import ExecutionStatus, CrewConfig, ExecutionNameGenerationRequest, ExecutionCreateResponse
from src.utils.user_context import GroupContext


# Mock models
class MockExecutionHistory:
    def __init__(self, id=1, execution_id="exec-123", status="completed", 
                 run_name="test_run", output="test output", created_at=None):
        self.id = id
        self.execution_id = execution_id
        self.status = status
        self.run_name = run_name
        self.output = output
        self.created_at = created_at or datetime.now()


class MockCrewConfig:
    def __init__(self, agents_yaml=None, tasks_yaml=None, inputs=None, 
                 planning=False, model="gpt-4o-mini"):
        self.agents_yaml = agents_yaml or {"agent1": {"role": "researcher"}}
        self.tasks_yaml = tasks_yaml or {"task1": {"description": "research"}}
        self.inputs = inputs or {"query": "test"}
        self.planning = planning
        self.model = model


@pytest.fixture
def execution_service():
    """Create an ExecutionService instance with mocked dependencies."""
    with patch('src.services.execution_service.ExecutionNameService') as mock_name_service, \
         patch('src.services.execution_service.CrewAIExecutionService') as mock_crew_service:
        
        mock_name_service.create.return_value = AsyncMock()
        mock_crew_instance = AsyncMock()
        
        # Set up specific async methods to return proper coroutines
        mock_crew_instance.get_flow_execution = AsyncMock()
        mock_crew_instance.run_flow_execution = AsyncMock()
        mock_crew_instance.get_flow_executions_by_flow = AsyncMock()
        
        mock_crew_service.return_value = mock_crew_instance
        
        service = ExecutionService()
        # Ensure the service's crewai_execution_service is properly mocked
        service.crewai_execution_service = mock_crew_instance
        return service


@pytest.fixture
def mock_crew_config():
    """Create a mock CrewConfig."""
    return MockCrewConfig()


@pytest.fixture
def group_context():
    """Create a mock group context."""
    return GroupContext(
        group_ids=["group-123"],
        group_email="test@example.com",
        email_domain="example.com"
    )


class TestExecutionService:
    """Test cases for ExecutionService."""
    
    @pytest.mark.asyncio
    async def test_execute_flow_with_flow_id(self, execution_service):
        """Test flow execution with flow ID."""
        flow_id = uuid.uuid4()
        job_id = "job-123"
        config = {"param": "value"}
        
        mock_result = {"execution_id": "exec-123", "status": "running"}
        execution_service.crewai_execution_service.run_flow_execution.return_value = mock_result
        
        result = await execution_service.execute_flow(
            flow_id=flow_id,
            job_id=job_id,
            config=config
        )
        
        assert result == mock_result
        execution_service.crewai_execution_service.run_flow_execution.assert_called_once_with(
            flow_id=str(flow_id),
            nodes=None,
            edges=None,
            job_id=job_id,
            config=config
        )
    
    @pytest.mark.asyncio
    async def test_execute_flow_with_nodes_edges(self, execution_service):
        """Test flow execution with nodes and edges."""
        nodes = [{"id": "1", "type": "agent"}]
        edges = [{"source": "1", "target": "2"}]
        
        mock_result = {"execution_id": "exec-456", "status": "running"}
        execution_service.crewai_execution_service.run_flow_execution.return_value = mock_result
        
        result = await execution_service.execute_flow(
            nodes=nodes,
            edges=edges
        )
        
        assert result == mock_result
        # Verify a job_id was generated
        call_args = execution_service.crewai_execution_service.run_flow_execution.call_args
        assert call_args[1]["job_id"] is not None
        assert call_args[1]["nodes"] == nodes
        assert call_args[1]["edges"] == edges
    
    @pytest.mark.asyncio
    async def test_execute_flow_generates_job_id(self, execution_service):
        """Test that flow execution generates job_id when not provided."""
        execution_service.crewai_execution_service.run_flow_execution.return_value = {"status": "running"}
        
        await execution_service.execute_flow()
        
        call_args = execution_service.crewai_execution_service.run_flow_execution.call_args
        job_id = call_args[1]["job_id"]
        assert job_id is not None
        # Verify it's a valid UUID string
        uuid.UUID(job_id)
    
    @pytest.mark.asyncio
    async def test_execute_flow_http_exception_propagation(self, execution_service):
        """Test that HTTPExceptions are propagated."""
        http_error = HTTPException(status_code=400, detail="Bad request")
        execution_service.crewai_execution_service.run_flow_execution.side_effect = http_error
        
        with pytest.raises(HTTPException) as exc_info:
            await execution_service.execute_flow()
        
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "Bad request"
    
    @pytest.mark.asyncio
    async def test_execute_flow_general_exception_handling(self, execution_service):
        """Test general exception handling in flow execution."""
        execution_service.crewai_execution_service.run_flow_execution.side_effect = Exception("Database error")
        
        with pytest.raises(HTTPException) as exc_info:
            await execution_service.execute_flow()
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Unexpected error in execute_flow" in str(exc_info.value.detail)
    
    def test_get_execution_method_exists(self, execution_service):
        """Test that get_execution method exists and is callable."""
        assert hasattr(execution_service, 'get_execution')
        assert callable(execution_service.get_execution)
    
    def test_get_executions_by_flow_method_exists(self, execution_service):
        """Test that get_executions_by_flow method exists and is callable."""
        assert hasattr(execution_service, 'get_executions_by_flow')
        assert callable(execution_service.get_executions_by_flow)
    
    @pytest.mark.asyncio
    async def test_get_executions_by_flow_success(self, execution_service):
        """Test successful retrieval of executions by flow."""
        flow_id = uuid.uuid4()
        mock_executions = [{"id": 1, "status": "completed"}, {"id": 2, "status": "running"}]
        execution_service.crewai_execution_service.get_flow_executions_by_flow.return_value = mock_executions
        
        result = await execution_service.get_executions_by_flow(flow_id)
        
        assert result == mock_executions
        execution_service.crewai_execution_service.get_flow_executions_by_flow.assert_called_once_with(str(flow_id))
    
    @pytest.mark.asyncio
    async def test_get_executions_by_flow_error_handling(self, execution_service):
        """Test error handling in get_executions_by_flow."""
        flow_id = uuid.uuid4()
        execution_service.crewai_execution_service.get_flow_executions_by_flow.side_effect = Exception("Database error")
        
        with pytest.raises(HTTPException) as exc_info:
            await execution_service.get_executions_by_flow(flow_id)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Error getting executions" in str(exc_info.value.detail)
    
    def test_create_execution_id(self):
        """Test execution ID creation."""
        execution_id = ExecutionService.create_execution_id()
        
        assert execution_id is not None
        # Verify it's a valid UUID string
        uuid.UUID(execution_id)
    
    def test_add_execution_to_memory_default_time(self):
        """Test adding execution to memory with default timestamp."""
        execution_id = "test-exec-789"
        status = "completed"
        run_name = "test_run_2"
        
        # Clear executions
        ExecutionService.executions.clear()
        
        ExecutionService.add_execution_to_memory(execution_id, status, run_name)
        
        stored_execution = ExecutionService.executions[execution_id]
        assert stored_execution["execution_id"] == execution_id
        assert stored_execution["status"] == status
        assert stored_execution["run_name"] == run_name
        assert isinstance(stored_execution["created_at"], datetime)
        
        # Clean up
        ExecutionService.executions.clear()
    
    def test_sanitize_for_database_simple_values(self):
        """Test sanitizing simple values for database."""
        data = {
            "string": "test",
            "number": 42,
            "boolean": True,
            "none": None
        }
        
        result = ExecutionService.sanitize_for_database(data)
        assert result == data
    
    def test_sanitize_for_database_uuid(self):
        """Test sanitizing UUID values."""
        test_uuid = uuid.uuid4()
        data = {"id": test_uuid}
        
        result = ExecutionService.sanitize_for_database(data)
        assert result["id"] == str(test_uuid)
    
    def test_sanitize_for_database_nested_dict(self):
        """Test sanitizing nested dictionaries."""
        test_uuid = uuid.uuid4()
        data = {
            "nested": {
                "id": test_uuid,
                "name": "test"
            }
        }
        
        result = ExecutionService.sanitize_for_database(data)
        assert result["nested"]["id"] == str(test_uuid)
        assert result["nested"]["name"] == "test"
    
    def test_sanitize_for_database_list(self):
        """Test sanitizing lists."""
        test_uuid = uuid.uuid4()
        data = {
            "items": [
                {"id": test_uuid, "name": "item1"},
                "simple_string",
                42
            ]
        }
        
        result = ExecutionService.sanitize_for_database(data)
        assert result["items"][0]["id"] == str(test_uuid)
        assert result["items"][1] == "simple_string"
        assert result["items"][2] == 42
    
    def test_sanitize_for_database_non_serializable(self):
        """Test sanitizing non-JSON-serializable values."""
        class NonSerializable:
            def __str__(self):
                return "custom_string"
        
        data = {"custom": NonSerializable()}
        result = ExecutionService.sanitize_for_database(data)
        assert result["custom"] == "custom_string"
    
    def test_static_get_execution_method(self):
        """Test static get_execution method from in-memory storage."""
        execution_id = "test-exec-123"
        test_data = {"status": "running", "name": "test"}
        
        # Clear executions and add test data
        ExecutionService.executions.clear()
        ExecutionService.executions[execution_id] = test_data
        
        result = ExecutionService.get_execution(execution_id)
        assert result == test_data
        
        # Test non-existent execution
        result = ExecutionService.get_execution("non-existent")
        assert result is None
        
        # Clean up
        ExecutionService.executions.clear()
    
    def test_memory_execution_management(self):
        """Test adding and retrieving executions from memory."""
        execution_id = "test-exec-456"
        status = "running"
        run_name = "test_run"
        created_at = datetime.now()
        
        # Clear executions
        ExecutionService.executions.clear()
        
        # Add execution
        ExecutionService.add_execution_to_memory(execution_id, status, run_name, created_at)
        
        # Retrieve execution
        stored_execution = ExecutionService.get_execution(execution_id)
        assert stored_execution["execution_id"] == execution_id
        assert stored_execution["status"] == status
        assert stored_execution["run_name"] == run_name
        assert stored_execution["created_at"] == created_at
        assert stored_execution["output"] == ""
        
        # Clean up
        ExecutionService.executions.clear()
    
    def test_execution_service_initialization(self, execution_service):
        """Test ExecutionService initialization."""
        assert execution_service.execution_name_service is not None
        assert execution_service.crewai_execution_service is not None
        assert hasattr(ExecutionService, 'executions')
        assert hasattr(ExecutionService, '_thread_pool')
    
    def test_sanitize_for_database_comprehensive(self):
        """Test comprehensive data sanitization for database storage."""
        test_uuid = uuid.uuid4()
        
        class NonSerializable:
            def __str__(self):
                return "custom_object"
        
        complex_data = {
            "simple_string": "test",
            "number": 42,
            "boolean": True,
            "null_value": None,
            "uuid_value": test_uuid,
            "nested_dict": {
                "inner_uuid": test_uuid,
                "inner_string": "nested"
            },
            "list_with_dicts": [
                {"list_uuid": test_uuid, "list_item": "item1"},
                "simple_string",
                123
            ],
            "simple_list": ["a", "b", "c"],
            "non_serializable": NonSerializable()
        }
        
        result = ExecutionService.sanitize_for_database(complex_data)
        
        # Test simple values
        assert result["simple_string"] == "test"
        assert result["number"] == 42
        assert result["boolean"] is True
        assert result["null_value"] is None
        
        # Test UUID conversion
        assert result["uuid_value"] == str(test_uuid)
        assert result["nested_dict"]["inner_uuid"] == str(test_uuid)
        assert result["list_with_dicts"][0]["list_uuid"] == str(test_uuid)
        
        # Test nested structures preserved
        assert result["nested_dict"]["inner_string"] == "nested"
        assert result["list_with_dicts"][1] == "simple_string"
        assert result["list_with_dicts"][2] == 123
        assert result["simple_list"] == ["a", "b", "c"]
        
        # Test non-serializable conversion
        assert result["non_serializable"] == "custom_object"
    
    @pytest.mark.asyncio 
    async def test_execute_flow_delegation(self, execution_service):
        """Test that execute_flow properly delegates to CrewAI service."""
        flow_id = uuid.uuid4()
        
        # Configure the mock method
        execution_service.crewai_execution_service.run_flow_execution.return_value = {"status": "running"}
        
        # Test execute_flow delegation
        result = await execution_service.execute_flow(flow_id=flow_id)
        assert result["status"] == "running"
        execution_service.crewai_execution_service.run_flow_execution.assert_called_once()
        
        # Verify call arguments contain the flow_id
        call_args = execution_service.crewai_execution_service.run_flow_execution.call_args
        assert str(flow_id) in call_args[1]["flow_id"]
    
    def test_execution_id_generation_uniqueness(self):
        """Test that execution ID generation produces unique values."""
        ids = set()
        for _ in range(100):
            execution_id = ExecutionService.create_execution_id()
            assert execution_id not in ids  # Should be unique
            ids.add(execution_id)
            # Verify it's a valid UUID
            uuid.UUID(execution_id)
    
    def test_class_attributes_persistence(self):
        """Test that class attributes persist across instances."""
        # Clear executions
        ExecutionService.executions.clear()
        
        # Create first instance and add data
        service1 = ExecutionService()
        ExecutionService.executions["test1"] = {"data": "value1"}
        
        # Create second instance and verify data persists
        service2 = ExecutionService()
        assert ExecutionService.executions["test1"]["data"] == "value1"
        
        # Add data from second instance
        ExecutionService.executions["test2"] = {"data": "value2"}
        
        # Verify both instances see both entries
        assert len(ExecutionService.executions) == 2
        assert ExecutionService.executions["test1"]["data"] == "value1"
        assert ExecutionService.executions["test2"]["data"] == "value2"
        
        # Clean up
        ExecutionService.executions.clear()
    
    def test_thread_pool_initialization(self, execution_service):
        """Test that thread pool is properly initialized."""
        assert isinstance(ExecutionService._thread_pool, concurrent.futures.ThreadPoolExecutor)
        assert ExecutionService._thread_pool._max_workers == 10
    
    def test_executions_class_attribute(self):
        """Test that executions dictionary is a class attribute."""
        # Clear and test
        ExecutionService.executions.clear()
        assert ExecutionService.executions == {}
        
        # Add data and verify it persists across instances
        ExecutionService.executions["test"] = {"data": "value"}
        service1 = ExecutionService()
        service2 = ExecutionService()
        
        # Both instances should share the same executions dict
        assert ExecutionService.executions["test"]["data"] == "value"
        
        # Clean up
        ExecutionService.executions.clear()

