"""
Unit tests for flow execution schemas.

Tests the functionality of Pydantic schemas for flow execution operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from uuid import uuid4, UUID
from pydantic import ValidationError

from src.schemas.flow_execution import (
    FlowExecutionStatus, FlowExecutionBase, FlowExecutionCreate, 
    FlowExecutionUpdate, FlowExecutionResponse, FlowNodeExecutionBase,
    FlowNodeExecutionCreate, FlowNodeExecutionUpdate, FlowNodeExecutionResponse,
    FlowExecutionDetailResponse
)


class TestFlowExecutionStatus:
    """Test cases for FlowExecutionStatus enum."""
    
    def test_flow_execution_status_values(self):
        """Test FlowExecutionStatus enum values."""
        assert FlowExecutionStatus.PENDING == "pending"
        assert FlowExecutionStatus.PREPARING == "preparing"
        assert FlowExecutionStatus.RUNNING == "running"
        assert FlowExecutionStatus.COMPLETED == "completed"
        assert FlowExecutionStatus.FAILED == "failed"
    
    def test_flow_execution_status_all_values(self):
        """Test that all expected FlowExecutionStatus values are present."""
        expected_values = {"pending", "preparing", "running", "completed", "failed"}
        actual_values = {status.value for status in FlowExecutionStatus}
        assert actual_values == expected_values
    
    def test_flow_execution_status_iteration(self):
        """Test iterating over FlowExecutionStatus."""
        statuses = list(FlowExecutionStatus)
        assert len(statuses) == 5
        assert FlowExecutionStatus.PENDING in statuses
        assert FlowExecutionStatus.PREPARING in statuses
        assert FlowExecutionStatus.RUNNING in statuses
        assert FlowExecutionStatus.COMPLETED in statuses
        assert FlowExecutionStatus.FAILED in statuses
    
    def test_flow_execution_status_string_inheritance(self):
        """Test that FlowExecutionStatus inherits from str."""
        assert isinstance(FlowExecutionStatus.PENDING, str)
        assert isinstance(FlowExecutionStatus.RUNNING, str)
        assert isinstance(FlowExecutionStatus.COMPLETED, str)


class TestFlowExecutionBase:
    """Test cases for FlowExecutionBase schema."""
    
    def test_valid_flow_execution_base_minimal(self):
        """Test FlowExecutionBase with minimal required fields."""
        flow_id = uuid4()
        execution_data = {
            "flow_id": flow_id,
            "job_id": "job_123"
        }
        execution = FlowExecutionBase(**execution_data)
        assert execution.flow_id == flow_id
        assert execution.job_id == "job_123"
        assert execution.status == FlowExecutionStatus.PENDING  # Default
        assert execution.config == {}  # Default factory
    
    def test_valid_flow_execution_base_complete(self):
        """Test FlowExecutionBase with all fields."""
        flow_id = uuid4()
        config_data = {
            "timeout": 3600,
            "retry_count": 3,
            "variables": {"input_path": "/data/input"}
        }
        execution_data = {
            "flow_id": flow_id,
            "job_id": "job_456",
            "status": FlowExecutionStatus.RUNNING,
            "config": config_data
        }
        execution = FlowExecutionBase(**execution_data)
        assert execution.flow_id == flow_id
        assert execution.job_id == "job_456"
        assert execution.status == FlowExecutionStatus.RUNNING
        assert execution.config == config_data
    
    def test_flow_execution_base_missing_required_fields(self):
        """Test FlowExecutionBase validation with missing required fields."""
        # Missing flow_id
        with pytest.raises(ValidationError) as exc_info:
            FlowExecutionBase(job_id="test_job")
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "flow_id" in missing_fields
        
        # Missing job_id
        with pytest.raises(ValidationError) as exc_info:
            FlowExecutionBase(flow_id=uuid4())
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "job_id" in missing_fields
    
    def test_flow_execution_base_string_flow_id(self):
        """Test FlowExecutionBase with string flow_id."""
        execution = FlowExecutionBase(
            flow_id="string-flow-id-123",
            job_id="job_string"
        )
        assert execution.flow_id == "string-flow-id-123"
        assert execution.job_id == "job_string"
    
    def test_flow_execution_base_various_statuses(self):
        """Test FlowExecutionBase with various status values."""
        flow_id = uuid4()
        for status in FlowExecutionStatus:
            execution = FlowExecutionBase(
                flow_id=flow_id,
                job_id=f"job_{status.value}",
                status=status
            )
            assert execution.status == status
    
    def test_flow_execution_base_complex_config(self):
        """Test FlowExecutionBase with complex configuration."""
        complex_config = {
            "execution_parameters": {
                "max_parallel_nodes": 4,
                "timeout_seconds": 7200,
                "retry_policy": {
                    "max_retries": 3,
                    "backoff_factor": 2.0,
                    "max_backoff": 300
                }
            },
            "environment": {
                "variables": {
                    "API_KEY": "secret_key",
                    "DATABASE_URL": "postgresql://localhost/db",
                    "DEBUG": True
                },
                "resources": {
                    "cpu_limit": "2",
                    "memory_limit": "4Gi"
                }
            },
            "monitoring": {
                "metrics_enabled": True,
                "log_level": "INFO",
                "tracing": {
                    "enabled": True,
                    "sampling_rate": 0.1
                }
            }
        }
        
        execution = FlowExecutionBase(
            flow_id=uuid4(),
            job_id="complex_job",
            config=complex_config
        )
        assert execution.config["execution_parameters"]["max_parallel_nodes"] == 4
        assert execution.config["environment"]["variables"]["DEBUG"] is True
        assert execution.config["monitoring"]["tracing"]["sampling_rate"] == 0.1


class TestFlowExecutionCreate:
    """Test cases for FlowExecutionCreate schema."""
    
    def test_flow_execution_create_inheritance(self):
        """Test that FlowExecutionCreate inherits from FlowExecutionBase."""
        flow_id = uuid4()
        create_data = {
            "flow_id": flow_id,
            "job_id": "create_job_001",
            "status": FlowExecutionStatus.PREPARING,
            "config": {"test": True}
        }
        create_execution = FlowExecutionCreate(**create_data)
        
        # Should have all base class attributes
        assert hasattr(create_execution, 'flow_id')
        assert hasattr(create_execution, 'job_id')
        assert hasattr(create_execution, 'status')
        assert hasattr(create_execution, 'config')
        
        # Values should match
        assert create_execution.flow_id == flow_id
        assert create_execution.job_id == "create_job_001"
        assert create_execution.status == FlowExecutionStatus.PREPARING
        assert create_execution.config == {"test": True}
    
    def test_flow_execution_create_same_validation(self):
        """Test that FlowExecutionCreate has same validation as base."""
        # Should fail with missing required fields
        with pytest.raises(ValidationError):
            FlowExecutionCreate(flow_id=uuid4())
        
        # Should succeed with required fields
        create_execution = FlowExecutionCreate(
            flow_id=uuid4(),
            job_id="valid_job"
        )
        assert create_execution.status == FlowExecutionStatus.PENDING  # Default


class TestFlowExecutionUpdate:
    """Test cases for FlowExecutionUpdate schema."""
    
    def test_valid_flow_execution_update_minimal(self):
        """Test FlowExecutionUpdate with minimal fields."""
        update = FlowExecutionUpdate()
        assert update.status is None
        assert update.result is None
        assert update.error is None
    
    def test_valid_flow_execution_update_status_only(self):
        """Test FlowExecutionUpdate with status only."""
        update = FlowExecutionUpdate(status=FlowExecutionStatus.COMPLETED)
        assert update.status == FlowExecutionStatus.COMPLETED
        assert update.result is None
        assert update.error is None
    
    def test_valid_flow_execution_update_result_only(self):
        """Test FlowExecutionUpdate with result only."""
        result_data = {"output": "processing complete", "metrics": {"duration": 120}}
        update = FlowExecutionUpdate(result=result_data)
        assert update.status is None
        assert update.result == result_data
        assert update.error is None
    
    def test_valid_flow_execution_update_error_only(self):
        """Test FlowExecutionUpdate with error only."""
        error_msg = "Flow execution failed: timeout after 3600 seconds"
        update = FlowExecutionUpdate(error=error_msg)
        assert update.status is None
        assert update.result is None
        assert update.error == error_msg
    
    def test_valid_flow_execution_update_complete(self):
        """Test FlowExecutionUpdate with all fields."""
        result_data = {"success": True, "data": [1, 2, 3]}
        update = FlowExecutionUpdate(
            status=FlowExecutionStatus.FAILED,
            result=result_data,
            error="Partial failure occurred"
        )
        assert update.status == FlowExecutionStatus.FAILED
        assert update.result == result_data
        assert update.error == "Partial failure occurred"
    
    def test_flow_execution_update_various_statuses(self):
        """Test FlowExecutionUpdate with various status values."""
        for status in FlowExecutionStatus:
            update = FlowExecutionUpdate(status=status)
            assert update.status == status


class TestFlowExecutionResponse:
    """Test cases for FlowExecutionResponse schema."""
    
    def test_valid_flow_execution_response_minimal(self):
        """Test FlowExecutionResponse with minimal required fields."""
        flow_id = uuid4()
        now = datetime.now()
        response_data = {
            "flow_id": flow_id,
            "job_id": "response_job_123",
            "id": 456,
            "created_at": now
        }
        response = FlowExecutionResponse(**response_data)
        assert response.flow_id == flow_id
        assert response.job_id == "response_job_123"
        assert response.id == 456
        assert response.created_at == now
        assert response.status == FlowExecutionStatus.PENDING  # Default
        assert response.config == {}  # Default
        assert response.result is None
        assert response.error is None
        assert response.updated_at is None
        assert response.completed_at is None
    
    def test_valid_flow_execution_response_complete(self):
        """Test FlowExecutionResponse with all fields."""
        flow_id = uuid4()
        now = datetime.now()
        config_data = {"timeout": 1800}
        result_data = {"output": "success", "nodes_executed": 5}
        
        response_data = {
            "flow_id": flow_id,
            "job_id": "complete_job_789",
            "status": FlowExecutionStatus.COMPLETED,
            "config": config_data,
            "id": 789,
            "result": result_data,
            "error": None,
            "created_at": now,
            "updated_at": now,
            "completed_at": now
        }
        response = FlowExecutionResponse(**response_data)
        assert response.flow_id == flow_id
        assert response.job_id == "complete_job_789"
        assert response.status == FlowExecutionStatus.COMPLETED
        assert response.config == config_data
        assert response.id == 789
        assert response.result == result_data
        assert response.error is None
        assert response.created_at == now
        assert response.updated_at == now
        assert response.completed_at == now
    
    def test_flow_execution_response_missing_response_fields(self):
        """Test FlowExecutionResponse validation with missing response fields."""
        base_data = {
            "flow_id": uuid4(),
            "job_id": "test_job"
        }
        
        # Missing id
        with pytest.raises(ValidationError) as exc_info:
            FlowExecutionResponse(**base_data, created_at=datetime.now())
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        
        # Missing created_at
        with pytest.raises(ValidationError) as exc_info:
            FlowExecutionResponse(**base_data, id=1)
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "created_at" in missing_fields
    
    def test_flow_execution_response_model_config(self):
        """Test FlowExecutionResponse model configuration."""
        assert hasattr(FlowExecutionResponse, 'model_config')
        assert FlowExecutionResponse.model_config.get('from_attributes') is True
    
    def test_flow_execution_response_error_scenarios(self):
        """Test FlowExecutionResponse with error scenarios."""
        flow_id = uuid4()
        now = datetime.now()
        
        # Execution with error
        error_response = FlowExecutionResponse(
            flow_id=flow_id,
            job_id="error_job",
            status=FlowExecutionStatus.FAILED,
            id=999,
            error="Node 'data_processor' failed: connection timeout",
            created_at=now,
            updated_at=now,
            completed_at=now
        )
        assert error_response.status == FlowExecutionStatus.FAILED
        assert "connection timeout" in error_response.error
        assert error_response.result is None


class TestFlowNodeExecutionBase:
    """Test cases for FlowNodeExecutionBase schema."""
    
    def test_valid_flow_node_execution_base_minimal(self):
        """Test FlowNodeExecutionBase with minimal required fields."""
        node_execution_data = {
            "flow_execution_id": 123,
            "node_id": "node_001"
        }
        node_execution = FlowNodeExecutionBase(**node_execution_data)
        assert node_execution.flow_execution_id == 123
        assert node_execution.node_id == "node_001"
        assert node_execution.status == FlowExecutionStatus.PENDING  # Default
        assert node_execution.agent_id is None
        assert node_execution.task_id is None
    
    def test_valid_flow_node_execution_base_complete(self):
        """Test FlowNodeExecutionBase with all fields."""
        node_execution_data = {
            "flow_execution_id": 456,
            "node_id": "processing_node",
            "status": FlowExecutionStatus.RUNNING,
            "agent_id": 789,
            "task_id": 101
        }
        node_execution = FlowNodeExecutionBase(**node_execution_data)
        assert node_execution.flow_execution_id == 456
        assert node_execution.node_id == "processing_node"
        assert node_execution.status == FlowExecutionStatus.RUNNING
        assert node_execution.agent_id == 789
        assert node_execution.task_id == 101
    
    def test_flow_node_execution_base_missing_required_fields(self):
        """Test FlowNodeExecutionBase validation with missing required fields."""
        # Missing flow_execution_id
        with pytest.raises(ValidationError) as exc_info:
            FlowNodeExecutionBase(node_id="test_node")
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "flow_execution_id" in missing_fields
        
        # Missing node_id
        with pytest.raises(ValidationError) as exc_info:
            FlowNodeExecutionBase(flow_execution_id=1)
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "node_id" in missing_fields
    
    def test_flow_node_execution_base_various_statuses(self):
        """Test FlowNodeExecutionBase with various status values."""
        for status in FlowExecutionStatus:
            node_execution = FlowNodeExecutionBase(
                flow_execution_id=1,
                node_id=f"node_{status.value}",
                status=status
            )
            assert node_execution.status == status


class TestFlowNodeExecutionCreate:
    """Test cases for FlowNodeExecutionCreate schema."""
    
    def test_flow_node_execution_create_inheritance(self):
        """Test that FlowNodeExecutionCreate inherits from FlowNodeExecutionBase."""
        create_data = {
            "flow_execution_id": 999,
            "node_id": "create_node",
            "status": FlowExecutionStatus.PREPARING,
            "agent_id": 555
        }
        create_node_execution = FlowNodeExecutionCreate(**create_data)
        
        # Should have all base class attributes
        assert hasattr(create_node_execution, 'flow_execution_id')
        assert hasattr(create_node_execution, 'node_id')
        assert hasattr(create_node_execution, 'status')
        assert hasattr(create_node_execution, 'agent_id')
        assert hasattr(create_node_execution, 'task_id')
        
        # Values should match
        assert create_node_execution.flow_execution_id == 999
        assert create_node_execution.node_id == "create_node"
        assert create_node_execution.status == FlowExecutionStatus.PREPARING
        assert create_node_execution.agent_id == 555


class TestFlowNodeExecutionUpdate:
    """Test cases for FlowNodeExecutionUpdate schema."""
    
    def test_valid_flow_node_execution_update_minimal(self):
        """Test FlowNodeExecutionUpdate with minimal fields."""
        update = FlowNodeExecutionUpdate()
        assert update.status is None
        assert update.result is None
        assert update.error is None
    
    def test_valid_flow_node_execution_update_status_only(self):
        """Test FlowNodeExecutionUpdate with status only."""
        update = FlowNodeExecutionUpdate(status=FlowExecutionStatus.COMPLETED)
        assert update.status == FlowExecutionStatus.COMPLETED
        assert update.result is None
        assert update.error is None
    
    def test_valid_flow_node_execution_update_complete(self):
        """Test FlowNodeExecutionUpdate with all fields."""
        result_data = {"node_output": "processed successfully", "metrics": {"duration": 45}}
        update = FlowNodeExecutionUpdate(
            status=FlowExecutionStatus.COMPLETED,
            result=result_data,
            error=None
        )
        assert update.status == FlowExecutionStatus.COMPLETED
        assert update.result == result_data
        assert update.error is None
    
    def test_flow_node_execution_update_error_scenario(self):
        """Test FlowNodeExecutionUpdate with error scenario."""
        update = FlowNodeExecutionUpdate(
            status=FlowExecutionStatus.FAILED,
            error="Node execution failed: invalid input data"
        )
        assert update.status == FlowExecutionStatus.FAILED
        assert update.error == "Node execution failed: invalid input data"
        assert update.result is None


class TestFlowNodeExecutionResponse:
    """Test cases for FlowNodeExecutionResponse schema."""
    
    def test_valid_flow_node_execution_response_minimal(self):
        """Test FlowNodeExecutionResponse with minimal required fields."""
        now = datetime.now()
        response_data = {
            "flow_execution_id": 123,
            "node_id": "response_node",
            "id": 456,
            "created_at": now
        }
        response = FlowNodeExecutionResponse(**response_data)
        assert response.flow_execution_id == 123
        assert response.node_id == "response_node"
        assert response.id == 456
        assert response.created_at == now
        assert response.status == FlowExecutionStatus.PENDING  # Default
        assert response.agent_id is None
        assert response.task_id is None
        assert response.result is None
        assert response.error is None
        assert response.updated_at is None
        assert response.completed_at is None
    
    def test_valid_flow_node_execution_response_complete(self):
        """Test FlowNodeExecutionResponse with all fields."""
        now = datetime.now()
        result_data = {"node_result": "success", "output_data": {"count": 100}}
        
        response_data = {
            "flow_execution_id": 789,
            "node_id": "complete_node",
            "status": FlowExecutionStatus.COMPLETED,
            "agent_id": 555,
            "task_id": 777,
            "id": 999,
            "result": result_data,
            "error": None,
            "created_at": now,
            "updated_at": now,
            "completed_at": now
        }
        response = FlowNodeExecutionResponse(**response_data)
        assert response.flow_execution_id == 789
        assert response.node_id == "complete_node"
        assert response.status == FlowExecutionStatus.COMPLETED
        assert response.agent_id == 555
        assert response.task_id == 777
        assert response.id == 999
        assert response.result == result_data
        assert response.error is None
        assert response.created_at == now
        assert response.updated_at == now
        assert response.completed_at == now
    
    def test_flow_node_execution_response_missing_response_fields(self):
        """Test FlowNodeExecutionResponse validation with missing response fields."""
        base_data = {
            "flow_execution_id": 1,
            "node_id": "test_node"
        }
        
        # Missing id
        with pytest.raises(ValidationError) as exc_info:
            FlowNodeExecutionResponse(**base_data, created_at=datetime.now())
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        
        # Missing created_at
        with pytest.raises(ValidationError) as exc_info:
            FlowNodeExecutionResponse(**base_data, id=1)
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "created_at" in missing_fields
    
    def test_flow_node_execution_response_model_config(self):
        """Test FlowNodeExecutionResponse model configuration."""
        assert hasattr(FlowNodeExecutionResponse, 'model_config')
        assert FlowNodeExecutionResponse.model_config.get('from_attributes') is True


class TestFlowExecutionDetailResponse:
    """Test cases for FlowExecutionDetailResponse schema."""
    
    def test_valid_flow_execution_detail_response_empty_nodes(self):
        """Test FlowExecutionDetailResponse with empty nodes list."""
        flow_id = uuid4()
        now = datetime.now()
        detail_data = {
            "flow_id": flow_id,
            "job_id": "detail_job",
            "id": 123,
            "created_at": now,
            "nodes": []
        }
        detail_response = FlowExecutionDetailResponse(**detail_data)
        assert detail_response.flow_id == flow_id
        assert detail_response.job_id == "detail_job"
        assert detail_response.id == 123
        assert detail_response.nodes == []
    
    def test_valid_flow_execution_detail_response_with_nodes(self):
        """Test FlowExecutionDetailResponse with node executions."""
        flow_id = uuid4()
        now = datetime.now()
        
        # Create node executions
        node_executions = [
            FlowNodeExecutionResponse(
                flow_execution_id=123,
                node_id="start_node",
                status=FlowExecutionStatus.COMPLETED,
                id=1,
                created_at=now,
                completed_at=now
            ),
            FlowNodeExecutionResponse(
                flow_execution_id=123,
                node_id="process_node",
                status=FlowExecutionStatus.RUNNING,
                id=2,
                created_at=now
            ),
            FlowNodeExecutionResponse(
                flow_execution_id=123,
                node_id="end_node",
                status=FlowExecutionStatus.PENDING,
                id=3,
                created_at=now
            )
        ]
        
        detail_data = {
            "flow_id": flow_id,
            "job_id": "detailed_job_456",
            "status": FlowExecutionStatus.RUNNING,
            "id": 123,
            "created_at": now,
            "nodes": node_executions
        }
        detail_response = FlowExecutionDetailResponse(**detail_data)
        assert detail_response.flow_id == flow_id
        assert detail_response.job_id == "detailed_job_456"
        assert detail_response.status == FlowExecutionStatus.RUNNING
        assert len(detail_response.nodes) == 3
        assert detail_response.nodes[0].node_id == "start_node"
        assert detail_response.nodes[0].status == FlowExecutionStatus.COMPLETED
        assert detail_response.nodes[1].node_id == "process_node"
        assert detail_response.nodes[1].status == FlowExecutionStatus.RUNNING
        assert detail_response.nodes[2].node_id == "end_node"
        assert detail_response.nodes[2].status == FlowExecutionStatus.PENDING
    
    def test_flow_execution_detail_response_inheritance(self):
        """Test that FlowExecutionDetailResponse inherits from FlowExecutionResponse."""
        flow_id = uuid4()
        now = datetime.now()
        
        detail_response = FlowExecutionDetailResponse(
            flow_id=flow_id,
            job_id="inheritance_test",
            id=456,
            created_at=now
        )
        
        # Should have all inherited attributes
        assert hasattr(detail_response, 'flow_id')
        assert hasattr(detail_response, 'job_id')
        assert hasattr(detail_response, 'status')
        assert hasattr(detail_response, 'config')
        assert hasattr(detail_response, 'id')
        assert hasattr(detail_response, 'result')
        assert hasattr(detail_response, 'error')
        assert hasattr(detail_response, 'created_at')
        assert hasattr(detail_response, 'updated_at')
        assert hasattr(detail_response, 'completed_at')
        assert hasattr(detail_response, 'nodes')  # New attribute
    
    def test_flow_execution_detail_response_model_config(self):
        """Test FlowExecutionDetailResponse model configuration."""
        assert hasattr(FlowExecutionDetailResponse, 'model_config')
        assert FlowExecutionDetailResponse.model_config.get('from_attributes') is True


class TestFlowExecutionSchemaIntegration:
    """Integration tests for flow execution schema interactions."""
    
    def test_complete_flow_execution_lifecycle(self):
        """Test complete flow execution lifecycle."""
        flow_id = uuid4()
        now = datetime.now()
        
        # Create flow execution
        create_execution = FlowExecutionCreate(
            flow_id=flow_id,
            job_id="lifecycle_job_001",
            status=FlowExecutionStatus.PREPARING,
            config={
                "timeout": 3600,
                "max_retries": 3,
                "variables": {"data_source": "database"}
            }
        )
        
        # Simulate execution creation response
        execution_response = FlowExecutionResponse(
            **create_execution.model_dump(),
            id=100,
            created_at=now
        )
        
        # Update execution to running
        update_to_running = FlowExecutionUpdate(
            status=FlowExecutionStatus.RUNNING
        )
        
        # Update execution to completed
        update_to_completed = FlowExecutionUpdate(
            status=FlowExecutionStatus.COMPLETED,
            result={
                "execution_time": 1200,
                "nodes_processed": 5,
                "output": "Flow completed successfully"
            }
        )
        
        # Verify lifecycle
        assert create_execution.flow_id == execution_response.flow_id
        assert create_execution.job_id == execution_response.job_id
        assert execution_response.id == 100
        assert update_to_running.status == FlowExecutionStatus.RUNNING
        assert update_to_completed.status == FlowExecutionStatus.COMPLETED
        assert update_to_completed.result["nodes_processed"] == 5
    
    def test_flow_node_execution_workflow(self):
        """Test flow node execution workflow."""
        now = datetime.now()
        
        # Create node executions for a flow
        node_executions = []
        for i, node_id in enumerate(["input", "process", "output"]):
            create_node = FlowNodeExecutionCreate(
                flow_execution_id=200,
                node_id=node_id,
                status=FlowExecutionStatus.PENDING,
                agent_id=i + 1 if node_id == "process" else None,
                task_id=i + 10 if node_id == "process" else None
            )
            
            node_response = FlowNodeExecutionResponse(
                **create_node.model_dump(),
                id=i + 1,
                created_at=now
            )
            node_executions.append(node_response)
        
        # Create detail response
        detail_response = FlowExecutionDetailResponse(
            flow_id=uuid4(),
            job_id="node_workflow_job",
            status=FlowExecutionStatus.RUNNING,
            id=200,
            created_at=now,
            nodes=node_executions
        )
        
        # Verify workflow
        assert len(detail_response.nodes) == 3
        assert detail_response.nodes[0].node_id == "input"
        assert detail_response.nodes[1].node_id == "process"
        assert detail_response.nodes[1].agent_id == 2
        assert detail_response.nodes[1].task_id == 11
        assert detail_response.nodes[2].node_id == "output"
    
    def test_flow_execution_error_handling(self):
        """Test flow execution error handling scenarios."""
        flow_id = uuid4()
        now = datetime.now()
        
        # Failed flow execution
        failed_execution = FlowExecutionResponse(
            flow_id=flow_id,
            job_id="failed_job_001",
            status=FlowExecutionStatus.FAILED,
            id=300,
            error="Flow execution failed: node 'data_processor' encountered timeout",
            created_at=now,
            updated_at=now,
            completed_at=now
        )
        
        # Failed node execution
        failed_node = FlowNodeExecutionResponse(
            flow_execution_id=300,
            node_id="data_processor",
            status=FlowExecutionStatus.FAILED,
            id=301,
            error="Connection timeout after 30 seconds",
            created_at=now,
            updated_at=now,
            completed_at=now
        )
        
        # Detail response with failed execution
        failed_detail = FlowExecutionDetailResponse(
            **failed_execution.model_dump(),
            nodes=[failed_node]
        )
        
        # Verify error handling
        assert failed_execution.status == FlowExecutionStatus.FAILED
        assert "timeout" in failed_execution.error
        assert failed_node.status == FlowExecutionStatus.FAILED
        assert "Connection timeout" in failed_node.error
        assert failed_detail.status == FlowExecutionStatus.FAILED
        assert len(failed_detail.nodes) == 1
        assert failed_detail.nodes[0].status == FlowExecutionStatus.FAILED
    
    def test_flow_execution_status_transitions(self):
        """Test valid flow execution status transitions."""
        flow_id = uuid4()
        now = datetime.now()
        
        # Start with pending execution
        pending_execution = FlowExecutionResponse(
            flow_id=flow_id,
            job_id="transition_job",
            status=FlowExecutionStatus.PENDING,
            id=400,
            created_at=now
        )
        
        # Valid transitions
        transitions = [
            (FlowExecutionStatus.PENDING, FlowExecutionStatus.PREPARING),
            (FlowExecutionStatus.PREPARING, FlowExecutionStatus.RUNNING),
            (FlowExecutionStatus.RUNNING, FlowExecutionStatus.COMPLETED),
            (FlowExecutionStatus.RUNNING, FlowExecutionStatus.FAILED)
        ]
        
        for from_status, to_status in transitions:
            update = FlowExecutionUpdate(status=to_status)
            # Simulate updated execution
            updated_execution = FlowExecutionResponse(
                **pending_execution.model_dump(exclude={"status", "updated_at"}),
                status=to_status,
                updated_at=now
            )
            
            # Verify transition
            assert update.status == to_status
            assert updated_execution.status == to_status
    
    def test_complex_flow_execution_scenario(self):
        """Test complex flow execution with multiple nodes and updates."""
        flow_id = uuid4()
        now = datetime.now()
        
        # Create complex flow execution
        complex_config = {
            "execution_mode": "parallel",
            "max_concurrent_nodes": 3,
            "timeout": 7200,
            "retry_policy": {
                "enabled": True,
                "max_retries": 2,
                "backoff_seconds": [30, 60, 120]
            },
            "monitoring": {
                "metrics_enabled": True,
                "log_level": "DEBUG"
            }
        }
        
        complex_execution = FlowExecutionResponse(
            flow_id=flow_id,
            job_id="complex_flow_job",
            status=FlowExecutionStatus.RUNNING,
            config=complex_config,
            id=500,
            created_at=now,
            updated_at=now
        )
        
        # Create multiple node executions
        node_types = [
            ("input_validator", FlowExecutionStatus.COMPLETED, 1, None),
            ("data_transformer", FlowExecutionStatus.RUNNING, 2, 101),
            ("output_formatter", FlowExecutionStatus.PENDING, None, None),
            ("error_handler", FlowExecutionStatus.PENDING, None, None)
        ]
        
        node_executions = []
        for i, (node_id, status, agent_id, task_id) in enumerate(node_types):
            node_execution = FlowNodeExecutionResponse(
                flow_execution_id=500,
                node_id=node_id,
                status=status,
                agent_id=agent_id,
                task_id=task_id,
                id=i + 501,
                created_at=now,
                result={"processed": True} if status == FlowExecutionStatus.COMPLETED else None
            )
            node_executions.append(node_execution)
        
        # Create complex detail response
        complex_detail = FlowExecutionDetailResponse(
            **complex_execution.model_dump(),
            nodes=node_executions
        )
        
        # Verify complex scenario
        assert complex_detail.config["execution_mode"] == "parallel"
        assert complex_detail.config["retry_policy"]["max_retries"] == 2
        assert len(complex_detail.nodes) == 4
        
        # Count node statuses
        completed_nodes = [n for n in complex_detail.nodes if n.status == FlowExecutionStatus.COMPLETED]
        running_nodes = [n for n in complex_detail.nodes if n.status == FlowExecutionStatus.RUNNING]
        pending_nodes = [n for n in complex_detail.nodes if n.status == FlowExecutionStatus.PENDING]
        
        assert len(completed_nodes) == 1
        assert len(running_nodes) == 1
        assert len(pending_nodes) == 2
        
        # Verify node with agent and task
        data_transformer = next(n for n in complex_detail.nodes if n.node_id == "data_transformer")
        assert data_transformer.agent_id == 2
        assert data_transformer.task_id == 101
        assert data_transformer.status == FlowExecutionStatus.RUNNING