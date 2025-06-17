"""
Unit tests for flow_execution model.

Tests the functionality of the FlowExecution and FlowNodeExecution database models
including field validation, relationships, and data integrity.
"""
import pytest
from datetime import datetime, timezone
import uuid
from unittest.mock import MagicMock

from src.models.flow_execution import FlowExecution, FlowNodeExecution


class TestFlowExecution:
    """Test cases for FlowExecution model."""

    def test_flow_execution_creation(self):
        """Test basic FlowExecution model creation."""
        # Arrange
        flow_id = uuid.uuid4()
        job_id = "job_12345"
        
        # Act
        flow_execution = FlowExecution(
            flow_id=flow_id,
            job_id=job_id
        )
        
        # Assert
        assert flow_execution.flow_id == flow_id
        assert flow_execution.job_id == job_id
        assert flow_execution.status is None  # Default not applied until DB insert
        assert flow_execution.config == {}  # Set by __init__ method
        assert flow_execution.result is None
        assert flow_execution.error is None
        assert flow_execution.completed_at is None

    def test_flow_execution_with_all_fields(self):
        """Test FlowExecution model creation with all fields populated."""
        # Arrange
        flow_id = uuid.uuid4()
        job_id = "job_comprehensive_test"
        status = "completed"
        config = {
            "agents": ["agent_1", "agent_2"],
            "tasks": ["task_1", "task_2"],
            "max_iterations": 25,
            "verbose": True
        }
        result = {
            "output": "Flow execution completed successfully",
            "total_time": 1200,
            "success": True
        }
        error = None
        created_at = datetime.utcnow()
        updated_at = datetime.utcnow()
        completed_at = datetime.utcnow()
        
        # Act
        flow_execution = FlowExecution(
            flow_id=flow_id,
            job_id=job_id,
            status=status,
            config=config,
            result=result,
            error=error,
            created_at=created_at,
            updated_at=updated_at,
            completed_at=completed_at
        )
        
        # Assert
        assert flow_execution.flow_id == flow_id
        assert flow_execution.job_id == job_id
        assert flow_execution.status == status
        assert flow_execution.config == config
        assert flow_execution.result == result
        assert flow_execution.error == error
        assert flow_execution.created_at == created_at
        assert flow_execution.updated_at == updated_at
        assert flow_execution.completed_at == completed_at

    def test_flow_execution_init_method_logic(self):
        """Test the custom __init__ method logic."""
        # Test 1: When config is None
        flow_execution1 = FlowExecution(
            flow_id=uuid.uuid4(),
            job_id="test_config_none",
            config=None
        )
        assert flow_execution1.config == {}
        
        # Test 2: When config is provided
        provided_config = {"test": "value"}
        flow_execution2 = FlowExecution(
            flow_id=uuid.uuid4(),
            job_id="test_config_provided",
            config=provided_config
        )
        assert flow_execution2.config == provided_config

    def test_flow_execution_different_statuses(self):
        """Test FlowExecution with different status values."""
        statuses = ["pending", "running", "completed", "failed", "cancelled"]
        
        for status in statuses:
            # Act
            flow_execution = FlowExecution(
                flow_id=uuid.uuid4(),
                job_id=f"job_{status}",
                status=status
            )
            
            # Assert
            assert flow_execution.status == status

    def test_flow_execution_with_error(self):
        """Test FlowExecution with error information."""
        # Arrange
        flow_id = uuid.uuid4()
        job_id = "job_with_error"
        status = "failed"
        error = "Agent failed to complete task: Connection timeout to external API"
        
        # Act
        flow_execution = FlowExecution(
            flow_id=flow_id,
            job_id=job_id,
            status=status,
            error=error
        )
        
        # Assert
        assert flow_execution.status == status
        assert flow_execution.error == error
        assert flow_execution.result is None

    def test_flow_execution_complex_config(self):
        """Test FlowExecution with complex configuration."""
        # Arrange
        complex_config = {
            "execution_settings": {
                "max_iterations": 50,
                "timeout": 3600,
                "parallel_execution": True,
                "retry_policy": {
                    "max_retries": 3,
                    "backoff_strategy": "exponential"
                }
            },
            "agents": [
                {
                    "id": "agent_1",
                    "role": "researcher",
                    "tools": ["web_search", "database_query"]
                },
                {
                    "id": "agent_2", 
                    "role": "analyst",
                    "tools": ["data_analysis", "report_generator"]
                }
            ],
            "workflow": {
                "type": "sequential",
                "steps": ["research", "analysis", "reporting"]
            }
        }
        
        # Act
        flow_execution = FlowExecution(
            flow_id=uuid.uuid4(),
            job_id="complex_config_job",
            config=complex_config
        )
        
        # Assert
        assert flow_execution.config == complex_config
        assert flow_execution.config["execution_settings"]["max_iterations"] == 50
        assert len(flow_execution.config["agents"]) == 2
        assert flow_execution.config["workflow"]["type"] == "sequential"

    def test_flow_execution_complex_result(self):
        """Test FlowExecution with complex result data."""
        # Arrange
        complex_result = {
            "execution_summary": {
                "status": "completed",
                "total_duration": 1847,
                "steps_completed": 5,
                "total_steps": 5
            },
            "agent_outputs": [
                {
                    "agent_id": "researcher",
                    "output": "Found 15 relevant research papers",
                    "execution_time": 245
                },
                {
                    "agent_id": "analyst",
                    "output": "Generated comprehensive analysis report",
                    "execution_time": 1102
                }
            ],
            "final_output": {
                "report_url": "https://reports.company.com/analysis_123.pdf",
                "insights": ["Market trend is positive", "ROI projected at 23%"],
                "recommendations": ["Increase investment", "Expand target market"]
            },
            "metrics": {
                "tokens_used": 15423,
                "api_calls": 47,
                "cost_usd": 2.34
            }
        }
        
        # Act
        flow_execution = FlowExecution(
            flow_id=uuid.uuid4(),
            job_id="complex_result_job",
            status="completed",
            result=complex_result
        )
        
        # Assert
        assert flow_execution.result == complex_result
        assert flow_execution.result["execution_summary"]["total_duration"] == 1847
        assert len(flow_execution.result["agent_outputs"]) == 2
        assert flow_execution.result["metrics"]["cost_usd"] == 2.34

    def test_flow_execution_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert FlowExecution.__tablename__ == "flow_executions"

    def test_flow_execution_column_types_and_constraints(self):
        """Test that columns have correct data types and constraints."""
        # Act
        columns = FlowExecution.__table__.columns
        
        # Assert
        # Primary key
        assert columns['id'].primary_key is True
        assert "INTEGER" in str(columns['id'].type)
        
        # Foreign key
        assert columns['flow_id'].nullable is False
        assert "UUID" in str(columns['flow_id'].type)
        
        # Required fields
        assert columns['job_id'].nullable is False
        assert columns['job_id'].unique is True
        assert columns['status'].nullable is False
        
        # Optional fields
        assert columns['result'].nullable is True
        assert columns['error'].nullable is True
        assert columns['completed_at'].nullable is True
        
        # JSON fields
        assert "JSON" in str(columns['config'].type)
        assert "JSON" in str(columns['result'].type)
        
        # Text field
        assert "TEXT" in str(columns['error'].type)
        
        # DateTime fields
        assert "DATETIME" in str(columns['created_at'].type)
        assert "DATETIME" in str(columns['updated_at'].type)
        assert "DATETIME" in str(columns['completed_at'].type)

    def test_flow_execution_default_values(self):
        """Test FlowExecution model default values."""
        # Act
        columns = FlowExecution.__table__.columns
        
        # Assert
        assert columns['status'].default.arg == "pending"
        assert columns['config'].default.arg.__name__ == "dict"

    def test_flow_execution_timestamp_defaults(self):
        """Test timestamp column defaults."""
        # Act
        columns = FlowExecution.__table__.columns
        
        # Assert
        assert columns['created_at'].default is not None
        assert columns['updated_at'].default is not None
        assert columns['updated_at'].onupdate is not None


class TestFlowNodeExecution:
    """Test cases for FlowNodeExecution model."""

    def test_flow_node_execution_creation(self):
        """Test basic FlowNodeExecution model creation."""
        # Arrange
        flow_execution_id = 123
        node_id = "node_start"
        
        # Act
        node_execution = FlowNodeExecution(
            flow_execution_id=flow_execution_id,
            node_id=node_id
        )
        
        # Assert
        assert node_execution.flow_execution_id == flow_execution_id
        assert node_execution.node_id == node_id
        assert node_execution.status is None  # Default not applied until DB insert
        assert node_execution.agent_id is None
        assert node_execution.task_id is None
        assert node_execution.result is None
        assert node_execution.error is None
        assert node_execution.completed_at is None

    def test_flow_node_execution_with_all_fields(self):
        """Test FlowNodeExecution model creation with all fields populated."""
        # Arrange
        flow_execution_id = 456
        node_id = "node_agent_analysis"
        status = "completed"
        agent_id = 789
        task_id = 101112
        result = {
            "output": "Analysis completed successfully",
            "data_points": 1500,
            "insights": ["Trend A", "Trend B"]
        }
        error = None
        created_at = datetime.now(timezone.utc)
        updated_at = datetime.now(timezone.utc)
        completed_at = datetime.now(timezone.utc)
        
        # Act
        node_execution = FlowNodeExecution(
            flow_execution_id=flow_execution_id,
            node_id=node_id,
            status=status,
            agent_id=agent_id,
            task_id=task_id,
            result=result,
            error=error,
            created_at=created_at,
            updated_at=updated_at,
            completed_at=completed_at
        )
        
        # Assert
        assert node_execution.flow_execution_id == flow_execution_id
        assert node_execution.node_id == node_id
        assert node_execution.status == status
        assert node_execution.agent_id == agent_id
        assert node_execution.task_id == task_id
        assert node_execution.result == result
        assert node_execution.error == error
        assert node_execution.created_at == created_at
        assert node_execution.updated_at == updated_at
        assert node_execution.completed_at == completed_at

    def test_flow_node_execution_agent_node(self):
        """Test FlowNodeExecution for agent node type."""
        # Arrange
        flow_execution_id = 100
        node_id = "agent_node_researcher"
        agent_id = 555
        result = {
            "agent_output": "Research completed on market trends",
            "sources": ["source1.pdf", "source2.csv"],
            "confidence": 0.87
        }
        
        # Act
        node_execution = FlowNodeExecution(
            flow_execution_id=flow_execution_id,
            node_id=node_id,
            status="completed",
            agent_id=agent_id,
            result=result
        )
        
        # Assert
        assert node_execution.agent_id == agent_id
        assert node_execution.task_id is None
        assert node_execution.result["agent_output"] == "Research completed on market trends"
        assert len(node_execution.result["sources"]) == 2

    def test_flow_node_execution_task_node(self):
        """Test FlowNodeExecution for task node type."""
        # Arrange
        flow_execution_id = 200
        node_id = "task_node_analysis"
        task_id = 777
        result = {
            "task_output": "Data analysis report generated",
            "metrics": {"accuracy": 0.92, "processing_time": 45},
            "artifacts": ["report.pdf", "data.xlsx"]
        }
        
        # Act
        node_execution = FlowNodeExecution(
            flow_execution_id=flow_execution_id,
            node_id=node_id,
            status="completed",
            task_id=task_id,
            result=result
        )
        
        # Assert
        assert node_execution.task_id == task_id
        assert node_execution.agent_id is None
        assert node_execution.result["task_output"] == "Data analysis report generated"
        assert node_execution.result["metrics"]["accuracy"] == 0.92

    def test_flow_node_execution_with_error(self):
        """Test FlowNodeExecution with error information."""
        # Arrange
        flow_execution_id = 300
        node_id = "failing_node"
        status = "failed"
        error = "Agent execution failed: API rate limit exceeded"
        
        # Act
        node_execution = FlowNodeExecution(
            flow_execution_id=flow_execution_id,
            node_id=node_id,
            status=status,
            error=error
        )
        
        # Assert
        assert node_execution.status == status
        assert node_execution.error == error
        assert node_execution.result is None

    def test_flow_node_execution_different_statuses(self):
        """Test FlowNodeExecution with different status values."""
        statuses = ["pending", "running", "completed", "failed", "skipped"]
        
        for i, status in enumerate(statuses):
            # Act
            node_execution = FlowNodeExecution(
                flow_execution_id=1000 + i,
                node_id=f"node_{status}",
                status=status
            )
            
            # Assert
            assert node_execution.status == status

    def test_flow_node_execution_table_name(self):
        """Test that the table name is correctly set."""
        # Act & Assert
        assert FlowNodeExecution.__tablename__ == "flow_node_executions"

    def test_flow_node_execution_column_types_and_constraints(self):
        """Test that columns have correct data types and constraints."""
        # Act
        columns = FlowNodeExecution.__table__.columns
        
        # Assert
        # Primary key
        assert columns['id'].primary_key is True
        assert "INTEGER" in str(columns['id'].type)
        
        # Foreign key
        assert columns['flow_execution_id'].nullable is False
        
        # Required fields
        assert columns['node_id'].nullable is False
        assert columns['status'].nullable is False
        
        # Optional fields
        assert columns['agent_id'].nullable is True
        assert columns['task_id'].nullable is True
        assert columns['result'].nullable is True
        assert columns['error'].nullable is True
        assert columns['completed_at'].nullable is True
        
        # JSON field
        assert "JSON" in str(columns['result'].type)
        
        # Text field
        assert "TEXT" in str(columns['error'].type)
        
        # DateTime fields
        assert "DATETIME" in str(columns['created_at'].type)
        assert "DATETIME" in str(columns['updated_at'].type)
        assert "DATETIME" in str(columns['completed_at'].type)

    def test_flow_node_execution_default_values(self):
        """Test FlowNodeExecution model default values."""
        # Act
        columns = FlowNodeExecution.__table__.columns
        
        # Assert
        assert columns['status'].default.arg == "pending"

    def test_flow_node_execution_timestamp_defaults(self):
        """Test timestamp column defaults."""
        # Act
        columns = FlowNodeExecution.__table__.columns
        
        # Assert
        assert columns['created_at'].default is not None
        assert columns['updated_at'].default is not None
        assert columns['updated_at'].onupdate is not None


class TestFlowExecutionEdgeCases:
    """Test edge cases and error scenarios for FlowExecution models."""

    def test_flow_execution_very_long_job_id(self):
        """Test FlowExecution with very long job ID."""
        # Arrange
        long_job_id = "very_long_job_id_" * 20  # 340 characters
        
        # Act
        flow_execution = FlowExecution(
            flow_id=uuid.uuid4(),
            job_id=long_job_id
        )
        
        # Assert
        assert flow_execution.job_id == long_job_id
        assert len(flow_execution.job_id) == 340

    def test_flow_execution_large_config(self):
        """Test FlowExecution with large configuration object."""
        # Arrange
        large_config = {
            f"setting_{i}": {
                "value": f"config_value_{i}",
                "metadata": {"type": "string", "required": True}
            }
            for i in range(100)
        }
        
        # Act
        flow_execution = FlowExecution(
            flow_id=uuid.uuid4(),
            job_id="large_config_job",
            config=large_config
        )
        
        # Assert
        assert len(flow_execution.config) == 100
        assert flow_execution.config["setting_0"]["value"] == "config_value_0"
        assert flow_execution.config["setting_99"]["metadata"]["required"] is True

    def test_flow_execution_very_long_error(self):
        """Test FlowExecution with very long error message."""
        # Arrange
        long_error = "Error occurred: " + "A" * 5000  # Very long error message
        
        # Act
        flow_execution = FlowExecution(
            flow_id=uuid.uuid4(),
            job_id="long_error_job",
            status="failed",
            error=long_error
        )
        
        # Assert
        assert flow_execution.error == long_error
        assert len(flow_execution.error) > 5000

    def test_flow_node_execution_very_long_node_id(self):
        """Test FlowNodeExecution with very long node ID."""
        # Arrange
        long_node_id = "very_long_node_id_" * 15  # 270 characters
        
        # Act
        node_execution = FlowNodeExecution(
            flow_execution_id=123,
            node_id=long_node_id
        )
        
        # Assert
        assert node_execution.node_id == long_node_id
        assert len(node_execution.node_id) == 270

    def test_flow_execution_common_workflow_patterns(self):
        """Test FlowExecution for common workflow patterns."""
        # Sequential workflow
        sequential_flow = FlowExecution(
            flow_id=uuid.uuid4(),
            job_id="sequential_workflow",
            config={
                "workflow_type": "sequential",
                "steps": ["research", "analysis", "reporting"],
                "agents": ["researcher", "analyst", "reporter"]
            }
        )
        
        # Parallel workflow
        parallel_flow = FlowExecution(
            flow_id=uuid.uuid4(),
            job_id="parallel_workflow",
            config={
                "workflow_type": "parallel",
                "parallel_branches": 3,
                "merge_strategy": "aggregate",
                "agents": ["agent_1", "agent_2", "agent_3"]
            }
        )
        
        # Conditional workflow
        conditional_flow = FlowExecution(
            flow_id=uuid.uuid4(),
            job_id="conditional_workflow",
            config={
                "workflow_type": "conditional",
                "decision_points": ["quality_check", "approval_gate"],
                "fallback_strategy": "retry"
            }
        )
        
        # Assert
        assert sequential_flow.config["workflow_type"] == "sequential"
        assert len(sequential_flow.config["steps"]) == 3
        
        assert parallel_flow.config["parallel_branches"] == 3
        assert parallel_flow.config["merge_strategy"] == "aggregate"
        
        assert "decision_points" in conditional_flow.config
        assert conditional_flow.config["fallback_strategy"] == "retry"

    def test_flow_node_execution_workflow_nodes(self):
        """Test FlowNodeExecution for different workflow node types."""
        flow_execution_id = 500
        
        # Start node
        start_node = FlowNodeExecution(
            flow_execution_id=flow_execution_id,
            node_id="start",
            status="completed",
            result={"message": "Workflow started"}
        )
        
        # Decision node
        decision_node = FlowNodeExecution(
            flow_execution_id=flow_execution_id,
            node_id="quality_check",
            status="completed",
            result={
                "decision": "approve",
                "confidence": 0.95,
                "next_node": "final_processing"
            }
        )
        
        # Parallel merge node
        merge_node = FlowNodeExecution(
            flow_execution_id=flow_execution_id,
            node_id="merge_results",
            status="completed",
            result={
                "merged_data": ["result_1", "result_2", "result_3"],
                "merge_strategy": "concatenate"
            }
        )
        
        # End node
        end_node = FlowNodeExecution(
            flow_execution_id=flow_execution_id,
            node_id="end",
            status="completed",
            result={"message": "Workflow completed successfully"}
        )
        
        # Assert
        assert start_node.result["message"] == "Workflow started"
        assert decision_node.result["decision"] == "approve"
        assert len(merge_node.result["merged_data"]) == 3
        assert end_node.result["message"] == "Workflow completed successfully"

    def test_flow_execution_lifecycle(self):
        """Test complete FlowExecution lifecycle."""
        flow_id = uuid.uuid4()
        job_id = "lifecycle_test"
        
        # Initial creation
        initial_execution = FlowExecution(
            flow_id=flow_id,
            job_id=job_id,
            status="pending"
        )
        
        # Running state
        running_execution = FlowExecution(
            flow_id=flow_id,
            job_id=f"{job_id}_running",
            status="running",
            config={"started_at": datetime.utcnow().isoformat()}
        )
        
        # Completed state
        completed_execution = FlowExecution(
            flow_id=flow_id,
            job_id=f"{job_id}_completed",
            status="completed",
            result={
                "success": True,
                "output": "All tasks completed",
                "execution_time": 1200
            },
            completed_at=datetime.utcnow()
        )
        
        # Failed state
        failed_execution = FlowExecution(
            flow_id=flow_id,
            job_id=f"{job_id}_failed",
            status="failed",
            error="Agent timeout after 300 seconds",
            completed_at=datetime.utcnow()
        )
        
        # Assert
        assert initial_execution.status == "pending"
        assert running_execution.status == "running"
        assert completed_execution.status == "completed"
        assert completed_execution.result["success"] is True
        assert failed_execution.status == "failed"
        assert "timeout" in failed_execution.error