"""
Unit tests for flow execution schemas.

Tests the functionality of Pydantic schemas for flow execution operations
including validation, serialization, and field constraints.
"""

from datetime import datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from src.schemas.flow_execution import (
    FlowExecutionBase,
    FlowExecutionResponse,
    FlowExecutionStatus,
    FlowNodeExecutionBase,
    FlowNodeExecutionResponse,
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
        # Includes HITL status for waiting_for_approval
        expected_values = {
            "pending",
            "preparing",
            "running",
            "completed",
            "failed",
            "waiting_for_approval",
        }
        actual_values = {status.value for status in FlowExecutionStatus}
        assert actual_values == expected_values

    def test_flow_execution_status_iteration(self):
        """Test iterating over FlowExecutionStatus."""
        statuses = list(FlowExecutionStatus)
        assert len(statuses) == 6  # 5 original + 1 HITL status
        assert FlowExecutionStatus.PENDING in statuses
        assert FlowExecutionStatus.PREPARING in statuses
        assert FlowExecutionStatus.RUNNING in statuses
        assert FlowExecutionStatus.COMPLETED in statuses
        assert FlowExecutionStatus.FAILED in statuses
        assert FlowExecutionStatus.WAITING_FOR_APPROVAL in statuses

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
        execution_data = {"flow_id": flow_id, "job_id": "job_123"}
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
            "variables": {"input_path": "/data/input"},
        }
        execution_data = {
            "flow_id": flow_id,
            "job_id": "job_456",
            "status": FlowExecutionStatus.RUNNING,
            "config": config_data,
        }
        execution = FlowExecutionBase(**execution_data)
        assert execution.flow_id == flow_id
        assert execution.job_id == "job_456"
        assert execution.status == FlowExecutionStatus.RUNNING
        assert execution.config == config_data

    def test_flow_execution_base_missing_required_fields(self):
        """Test FlowExecutionBase validation with missing required fields."""
        # flow_id is optional now (for ad-hoc executions), so only job_id is required
        execution = FlowExecutionBase(job_id="test_job")
        assert execution.flow_id is None  # flow_id is optional
        assert execution.job_id == "test_job"

        # Missing job_id should raise ValidationError
        with pytest.raises(ValidationError) as exc_info:
            FlowExecutionBase(flow_id=uuid4())
        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "job_id" in missing_fields

    def test_flow_execution_base_string_flow_id(self):
        """Test FlowExecutionBase with string flow_id."""
        execution = FlowExecutionBase(flow_id="string-flow-id-123", job_id="job_string")
        assert execution.flow_id == "string-flow-id-123"
        assert execution.job_id == "job_string"

    def test_flow_execution_base_various_statuses(self):
        """Test FlowExecutionBase with various status values."""
        flow_id = uuid4()
        for status in FlowExecutionStatus:
            execution = FlowExecutionBase(
                flow_id=flow_id, job_id=f"job_{status.value}", status=status
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
                    "max_backoff": 300,
                },
            },
            "environment": {
                "variables": {
                    "API_KEY": "secret_key",
                    "DATABASE_URL": "postgresql://localhost/db",
                    "DEBUG": True,
                },
                "resources": {"cpu_limit": "2", "memory_limit": "4Gi"},
            },
            "monitoring": {
                "metrics_enabled": True,
                "log_level": "INFO",
                "tracing": {"enabled": True, "sampling_rate": 0.1},
            },
        }

        execution = FlowExecutionBase(
            flow_id=uuid4(), job_id="complex_job", config=complex_config
        )
        assert execution.config["execution_parameters"]["max_parallel_nodes"] == 4
        assert execution.config["environment"]["variables"]["DEBUG"] is True
        assert execution.config["monitoring"]["tracing"]["sampling_rate"] == 0.1


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
            "created_at": now,
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
            "completed_at": now,
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
        base_data = {"flow_id": uuid4(), "job_id": "test_job"}

        # Missing id
        with pytest.raises(ValidationError) as exc_info:
            FlowExecutionResponse(**base_data, created_at=datetime.now())
        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "id" in missing_fields

        # Missing created_at
        with pytest.raises(ValidationError) as exc_info:
            FlowExecutionResponse(**base_data, id=1)
        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "created_at" in missing_fields

    def test_flow_execution_response_model_config(self):
        """Test FlowExecutionResponse model configuration."""
        assert hasattr(FlowExecutionResponse, "model_config")
        assert FlowExecutionResponse.model_config.get("from_attributes") is True

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
            completed_at=now,
        )
        assert error_response.status == FlowExecutionStatus.FAILED
        assert "connection timeout" in error_response.error
        assert error_response.result is None


class TestFlowNodeExecutionBase:
    """Test cases for FlowNodeExecutionBase schema."""

    def test_valid_flow_node_execution_base_minimal(self):
        """Test FlowNodeExecutionBase with minimal required fields."""
        node_execution_data = {"flow_execution_id": 123, "node_id": "node_001"}
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
            "task_id": 101,
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
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "flow_execution_id" in missing_fields

        # Missing node_id
        with pytest.raises(ValidationError) as exc_info:
            FlowNodeExecutionBase(flow_execution_id=1)
        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "node_id" in missing_fields

    def test_flow_node_execution_base_various_statuses(self):
        """Test FlowNodeExecutionBase with various status values."""
        for status in FlowExecutionStatus:
            node_execution = FlowNodeExecutionBase(
                flow_execution_id=1, node_id=f"node_{status.value}", status=status
            )
            assert node_execution.status == status


class TestFlowNodeExecutionResponse:
    """Test cases for FlowNodeExecutionResponse schema."""

    def test_valid_flow_node_execution_response_minimal(self):
        """Test FlowNodeExecutionResponse with minimal required fields."""
        now = datetime.now()
        response_data = {
            "flow_execution_id": 123,
            "node_id": "response_node",
            "id": 456,
            "created_at": now,
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
            "completed_at": now,
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
        base_data = {"flow_execution_id": 1, "node_id": "test_node"}

        # Missing id
        with pytest.raises(ValidationError) as exc_info:
            FlowNodeExecutionResponse(**base_data, created_at=datetime.now())
        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "id" in missing_fields

        # Missing created_at
        with pytest.raises(ValidationError) as exc_info:
            FlowNodeExecutionResponse(**base_data, id=1)
        errors = exc_info.value.errors()
        missing_fields = [
            error["loc"][0] for error in errors if error["type"] == "missing"
        ]
        assert "created_at" in missing_fields

    def test_flow_node_execution_response_model_config(self):
        """Test FlowNodeExecutionResponse model configuration."""
        assert hasattr(FlowNodeExecutionResponse, "model_config")
        assert FlowNodeExecutionResponse.model_config.get("from_attributes") is True
