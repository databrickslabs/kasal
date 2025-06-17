"""
Unit tests for execution history schemas.

Tests the functionality of Pydantic schemas for execution history operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError

from src.schemas.execution_history import (
    ExecutionHistoryItem, ExecutionHistoryList, ExecutionOutput, ExecutionOutputList,
    ExecutionOutputDebug, ExecutionOutputDebugList, DeleteResponse
)


class TestExecutionHistoryItem:
    """Test cases for ExecutionHistoryItem schema."""
    
    def test_valid_execution_history_item_minimal(self):
        """Test ExecutionHistoryItem with minimal required fields."""
        now = datetime.now()
        item_data = {
            "id": 123,
            "job_id": "exec_job_456",
            "created_at": now
        }
        item = ExecutionHistoryItem(**item_data)
        assert item.id == 123
        assert item.job_id == "exec_job_456"
        assert item.created_at == now
        assert item.name is None
        assert item.agents_yaml is None
        assert item.tasks_yaml is None
        assert item.model is None
        assert item.status is None
        assert item.error is None
        assert item.input is None
        assert item.execution_type is None
        assert item.result is None
        assert item.group_email is None
    
    def test_valid_execution_history_item_complete(self):
        """Test ExecutionHistoryItem with all fields."""
        now = datetime.now()
        item_data = {
            "id": 789,
            "job_id": "exec_complete_999",
            "run_name": "Complete Test Execution",  # Using alias
            "agents_yaml": "agent1:\n  role: analyst\n  goal: analyze data",
            "tasks_yaml": "task1:\n  description: analyze dataset\n  agent: agent1",
            "model": "gpt-4",
            "status": "completed",
            "error": None,
            "created_at": now,
            "input": {"dataset": "sales_data.csv", "period": "Q4"},
            "execution_type": "crew",
            "result": {"analysis": "positive trend", "confidence": 0.85},
            "group_email": "analyst@company.com"
        }
        item = ExecutionHistoryItem(**item_data)
        assert item.id == 789
        assert item.job_id == "exec_complete_999"
        assert item.name == "Complete Test Execution"  # Mapped from run_name
        assert item.agents_yaml == "agent1:\n  role: analyst\n  goal: analyze data"
        assert item.tasks_yaml == "task1:\n  description: analyze dataset\n  agent: agent1"
        assert item.model == "gpt-4"
        assert item.status == "completed"
        assert item.error is None
        assert item.input == {"dataset": "sales_data.csv", "period": "Q4"}
        assert item.execution_type == "crew"
        assert item.result == {"analysis": "positive trend", "confidence": 0.85}
        assert item.group_email == "analyst@company.com"
    
    def test_execution_history_item_missing_required_fields(self):
        """Test ExecutionHistoryItem validation with missing required fields."""
        # Missing id
        with pytest.raises(ValidationError) as exc_info:
            ExecutionHistoryItem(job_id="test", created_at=datetime.now())
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        
        # Missing job_id
        with pytest.raises(ValidationError) as exc_info:
            ExecutionHistoryItem(id=1, created_at=datetime.now())
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "job_id" in missing_fields
        
        # Missing created_at
        with pytest.raises(ValidationError) as exc_info:
            ExecutionHistoryItem(id=1, job_id="test")
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "created_at" in missing_fields
    
    def test_execution_history_item_alias_mapping(self):
        """Test ExecutionHistoryItem name field alias mapping."""
        now = datetime.now()
        
        # Test with run_name alias
        item_with_alias = ExecutionHistoryItem(
            id=1,
            job_id="test",
            run_name="Test Run",
            created_at=now
        )
        assert item_with_alias.name == "Test Run"
        
        # Test without run_name (should be None)
        item_without_name = ExecutionHistoryItem(
            id=2,
            job_id="test2",
            created_at=now
        )
        assert item_without_name.name is None
    
    def test_execution_history_item_error_scenarios(self):
        """Test ExecutionHistoryItem with error scenarios."""
        now = datetime.now()
        error_item = ExecutionHistoryItem(
            id=500,
            job_id="exec_error_001",
            name="Failed Execution",
            status="failed",
            error="Model endpoint unavailable",
            created_at=now,
            execution_type="crew"
        )
        assert error_item.status == "failed"
        assert error_item.error == "Model endpoint unavailable"
    
    def test_execution_history_item_model_config(self):
        """Test ExecutionHistoryItem model configuration."""
        assert hasattr(ExecutionHistoryItem, 'model_config')
        assert ExecutionHistoryItem.model_config.get('from_attributes') is True
    
    def test_execution_history_item_various_execution_types(self):
        """Test ExecutionHistoryItem with various execution types."""
        now = datetime.now()
        execution_types = ["crew", "flow", "pipeline", "custom"]
        
        for exec_type in execution_types:
            item = ExecutionHistoryItem(
                id=1,
                job_id=f"exec_{exec_type}",
                execution_type=exec_type,
                created_at=now
            )
            assert item.execution_type == exec_type


class TestExecutionHistoryList:
    """Test cases for ExecutionHistoryList schema."""
    
    def test_valid_execution_history_list_empty(self):
        """Test ExecutionHistoryList with empty executions list."""
        list_data = {
            "executions": [],
            "total": 0,
            "limit": 10,
            "offset": 0
        }
        exec_list = ExecutionHistoryList(**list_data)
        assert exec_list.executions == []
        assert exec_list.total == 0
        assert exec_list.limit == 10
        assert exec_list.offset == 0
    
    def test_valid_execution_history_list_with_items(self):
        """Test ExecutionHistoryList with execution items."""
        now = datetime.now()
        executions = [
            ExecutionHistoryItem(
                id=1,
                job_id="exec_001",
                run_name="First Execution",
                status="completed",
                created_at=now
            ),
            ExecutionHistoryItem(
                id=2,
                job_id="exec_002",
                run_name="Second Execution",
                status="running",
                created_at=now
            )
        ]
        
        list_data = {
            "executions": executions,
            "total": 25,
            "limit": 2,
            "offset": 0
        }
        exec_list = ExecutionHistoryList(**list_data)
        assert len(exec_list.executions) == 2
        assert exec_list.total == 25
        assert exec_list.limit == 2
        assert exec_list.offset == 0
        assert exec_list.executions[0].name == "First Execution"
        assert exec_list.executions[1].status == "running"
    
    def test_execution_history_list_missing_fields(self):
        """Test ExecutionHistoryList validation with missing fields."""
        required_fields = ["executions", "total", "limit", "offset"]
        
        for missing_field in required_fields:
            list_data = {
                "executions": [],
                "total": 0,
                "limit": 10,
                "offset": 0
            }
            del list_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                ExecutionHistoryList(**list_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields
    
    def test_execution_history_list_pagination_scenarios(self):
        """Test ExecutionHistoryList with various pagination scenarios."""
        now = datetime.now()
        base_execution = ExecutionHistoryItem(
            id=1,
            job_id="exec_page_test",
            created_at=now
        )
        
        # First page
        first_page = ExecutionHistoryList(
            executions=[base_execution],
            total=100,
            limit=10,
            offset=0
        )
        assert first_page.offset == 0
        assert first_page.limit == 10
        
        # Middle page
        middle_page = ExecutionHistoryList(
            executions=[base_execution],
            total=100,
            limit=10,
            offset=50
        )
        assert middle_page.offset == 50
        
        # Last page with fewer items
        last_page = ExecutionHistoryList(
            executions=[base_execution],
            total=95,
            limit=10,
            offset=90
        )
        assert last_page.total == 95
        assert last_page.offset == 90


class TestExecutionOutput:
    """Test cases for ExecutionOutput schema."""
    
    def test_valid_execution_output_minimal(self):
        """Test ExecutionOutput with minimal required fields."""
        now = datetime.now()
        output_data = {
            "id": 456,
            "job_id": "exec_output_test",
            "output": "Task completed successfully",
            "timestamp": now
        }
        output = ExecutionOutput(**output_data)
        assert output.id == 456
        assert output.job_id == "exec_output_test"
        assert output.output == "Task completed successfully"
        assert output.timestamp == now
        assert output.task_name is None
        assert output.agent_name is None
    
    def test_valid_execution_output_complete(self):
        """Test ExecutionOutput with all fields."""
        now = datetime.now()
        output_data = {
            "id": 789,
            "job_id": "exec_complete_output",
            "task_name": "data_analysis",
            "agent_name": "analyst_agent",
            "output": "Analysis complete: Found 3 key trends in the data",
            "timestamp": now
        }
        output = ExecutionOutput(**output_data)
        assert output.id == 789
        assert output.job_id == "exec_complete_output"
        assert output.task_name == "data_analysis"
        assert output.agent_name == "analyst_agent"
        assert output.output == "Analysis complete: Found 3 key trends in the data"
        assert output.timestamp == now
    
    def test_execution_output_missing_required_fields(self):
        """Test ExecutionOutput validation with missing required fields."""
        required_fields = ["id", "job_id", "output", "timestamp"]
        
        for missing_field in required_fields:
            output_data = {
                "id": 1,
                "job_id": "test",
                "output": "test output",
                "timestamp": datetime.now()
            }
            del output_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                ExecutionOutput(**output_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields
    
    def test_execution_output_long_content(self):
        """Test ExecutionOutput with long output content."""
        now = datetime.now()
        long_output = "A" * 10000
        
        output = ExecutionOutput(
            id=999,
            job_id="exec_long_output",
            output=long_output,
            timestamp=now
        )
        assert len(output.output) == 10000
    
    def test_execution_output_model_config(self):
        """Test ExecutionOutput model configuration."""
        assert hasattr(ExecutionOutput, 'model_config')
        assert ExecutionOutput.model_config.get('from_attributes') is True


class TestExecutionOutputList:
    """Test cases for ExecutionOutputList schema."""
    
    def test_valid_execution_output_list(self):
        """Test ExecutionOutputList with valid data."""
        now = datetime.now()
        outputs = [
            ExecutionOutput(
                id=1,
                job_id="exec_001",
                output="First output",
                timestamp=now
            ),
            ExecutionOutput(
                id=2,
                job_id="exec_001",
                output="Second output",
                timestamp=now
            )
        ]
        
        list_data = {
            "execution_id": "exec_001",
            "outputs": outputs,
            "total": 15,
            "limit": 2,
            "offset": 0
        }
        output_list = ExecutionOutputList(**list_data)
        assert output_list.execution_id == "exec_001"
        assert len(output_list.outputs) == 2
        assert output_list.total == 15
        assert output_list.limit == 2
        assert output_list.offset == 0
    
    def test_execution_output_list_missing_fields(self):
        """Test ExecutionOutputList validation with missing fields."""
        required_fields = ["execution_id", "outputs", "total", "limit", "offset"]
        
        for missing_field in required_fields:
            list_data = {
                "execution_id": "exec_test",
                "outputs": [],
                "total": 0,
                "limit": 10,
                "offset": 0
            }
            del list_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                ExecutionOutputList(**list_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields


class TestExecutionOutputDebug:
    """Test cases for ExecutionOutputDebug schema."""
    
    def test_valid_execution_output_debug(self):
        """Test ExecutionOutputDebug with valid data."""
        now = datetime.now()
        debug_data = {
            "id": 123,
            "timestamp": now,
            "task_name": "debug_task",
            "agent_name": "debug_agent",
            "output_preview": "Debug output preview..."
        }
        debug = ExecutionOutputDebug(**debug_data)
        assert debug.id == 123
        assert debug.timestamp == now
        assert debug.task_name == "debug_task"
        assert debug.agent_name == "debug_agent"
        assert debug.output_preview == "Debug output preview..."
    
    def test_execution_output_debug_minimal(self):
        """Test ExecutionOutputDebug with minimal fields."""
        now = datetime.now()
        debug = ExecutionOutputDebug(
            id=456,
            timestamp=now
        )
        assert debug.id == 456
        assert debug.timestamp == now
        assert debug.task_name is None
        assert debug.agent_name is None
        assert debug.output_preview is None
    
    def test_execution_output_debug_missing_required_fields(self):
        """Test ExecutionOutputDebug validation with missing required fields."""
        # Missing id
        with pytest.raises(ValidationError) as exc_info:
            ExecutionOutputDebug(timestamp=datetime.now())
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
        
        # Missing timestamp
        with pytest.raises(ValidationError) as exc_info:
            ExecutionOutputDebug(id=1)
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "timestamp" in missing_fields


class TestExecutionOutputDebugList:
    """Test cases for ExecutionOutputDebugList schema."""
    
    def test_valid_execution_output_debug_list(self):
        """Test ExecutionOutputDebugList with valid data."""
        now = datetime.now()
        debug_outputs = [
            ExecutionOutputDebug(
                id=1,
                timestamp=now,
                task_name="task1",
                output_preview="Preview 1"
            ),
            ExecutionOutputDebug(
                id=2,
                timestamp=now,
                task_name="task2",
                output_preview="Preview 2"
            )
        ]
        
        list_data = {
            "run_id": 789,
            "execution_id": "exec_debug_001",
            "total_outputs": 10,
            "outputs": debug_outputs
        }
        debug_list = ExecutionOutputDebugList(**list_data)
        assert debug_list.run_id == 789
        assert debug_list.execution_id == "exec_debug_001"
        assert debug_list.total_outputs == 10
        assert len(debug_list.outputs) == 2
        assert debug_list.outputs[0].task_name == "task1"
        assert debug_list.outputs[1].output_preview == "Preview 2"
    
    def test_execution_output_debug_list_missing_fields(self):
        """Test ExecutionOutputDebugList validation with missing fields."""
        required_fields = ["run_id", "execution_id", "total_outputs", "outputs"]
        
        for missing_field in required_fields:
            list_data = {
                "run_id": 1,
                "execution_id": "exec_test",
                "total_outputs": 0,
                "outputs": []
            }
            del list_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                ExecutionOutputDebugList(**list_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields


class TestDeleteResponse:
    """Test cases for DeleteResponse schema."""
    
    def test_valid_delete_response_by_id(self):
        """Test DeleteResponse for delete by ID operation."""
        response_data = {
            "message": "Execution deleted successfully",
            "deleted_run_id": 123,
            "deleted_outputs": 5
        }
        response = DeleteResponse(**response_data)
        assert response.message == "Execution deleted successfully"
        assert response.deleted_run_id == 123
        assert response.deleted_job_id is None
        assert response.deleted_runs is None
        assert response.deleted_outputs == 5
    
    def test_valid_delete_response_by_job_id(self):
        """Test DeleteResponse for delete by job_id operation."""
        response_data = {
            "message": "Execution deleted by job ID",
            "deleted_job_id": "exec_job_456",
            "deleted_outputs": 3
        }
        response = DeleteResponse(**response_data)
        assert response.message == "Execution deleted by job ID"
        assert response.deleted_run_id is None
        assert response.deleted_job_id == "exec_job_456"
        assert response.deleted_runs is None
        assert response.deleted_outputs == 3
    
    def test_valid_delete_response_bulk_delete(self):
        """Test DeleteResponse for bulk delete operation."""
        response_data = {
            "message": "All executions deleted successfully",
            "deleted_runs": 25,
            "deleted_outputs": 150
        }
        response = DeleteResponse(**response_data)
        assert response.message == "All executions deleted successfully"
        assert response.deleted_run_id is None
        assert response.deleted_job_id is None
        assert response.deleted_runs == 25
        assert response.deleted_outputs == 150
    
    def test_delete_response_missing_message(self):
        """Test DeleteResponse validation with missing message."""
        with pytest.raises(ValidationError) as exc_info:
            DeleteResponse(deleted_run_id=123)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "message" in missing_fields
    
    def test_delete_response_minimal(self):
        """Test DeleteResponse with minimal data."""
        response = DeleteResponse(message="Delete operation completed")
        assert response.message == "Delete operation completed"
        assert response.deleted_run_id is None
        assert response.deleted_job_id is None
        assert response.deleted_runs is None
        assert response.deleted_outputs is None


class TestExecutionHistorySchemaIntegration:
    """Integration tests for execution history schema interactions."""
    
    def test_complete_execution_history_workflow(self):
        """Test complete execution history workflow."""
        now = datetime.now()
        
        # Create execution history item
        execution = ExecutionHistoryItem(
            id=1001,
            job_id="exec_workflow_001",
            name="Data Processing Pipeline",
            agents_yaml="analyst:\n  role: Data Analyst",
            tasks_yaml="analyze:\n  description: Analyze dataset",
            model="gpt-4",
            status="completed",
            created_at=now,
            input={"file": "data.csv"},
            execution_type="crew",
            result={"processed_rows": 1000},
            group_email="team@company.com"
        )
        
        # Create execution outputs
        outputs = [
            ExecutionOutput(
                id=1,
                job_id="exec_workflow_001",
                task_name="analyze",
                agent_name="analyst",
                output="Data analysis started",
                timestamp=now
            ),
            ExecutionOutput(
                id=2,
                job_id="exec_workflow_001",
                task_name="analyze",
                agent_name="analyst",
                output="Processing 1000 rows",
                timestamp=now
            ),
            ExecutionOutput(
                id=3,
                job_id="exec_workflow_001",
                task_name="analyze",
                agent_name="analyst",
                output="Analysis complete: Found 3 trends",
                timestamp=now
            )
        ]
        
        # Create execution history list
        history_list = ExecutionHistoryList(
            executions=[execution],
            total=1,
            limit=10,
            offset=0
        )
        
        # Create execution output list
        output_list = ExecutionOutputList(
            execution_id="exec_workflow_001",
            outputs=outputs,
            total=3,
            limit=10,
            offset=0
        )
        
        # Verify workflow
        assert execution.job_id == "exec_workflow_001"
        assert execution.status == "completed"
        assert history_list.executions[0].job_id == execution.job_id
        assert output_list.execution_id == execution.job_id
        assert len(output_list.outputs) == 3
        assert all(output.job_id == execution.job_id for output in outputs)
    
    def test_execution_debug_workflow(self):
        """Test execution debug information workflow."""
        now = datetime.now()
        
        # Create debug outputs
        debug_outputs = [
            ExecutionOutputDebug(
                id=1,
                timestamp=now,
                task_name="preprocessing",
                agent_name="preprocessor",
                output_preview="Data loaded: 500 records"
            ),
            ExecutionOutputDebug(
                id=2,
                timestamp=now,
                task_name="analysis",
                agent_name="analyzer",
                output_preview="Running statistical analysis..."
            ),
            ExecutionOutputDebug(
                id=3,
                timestamp=now,
                task_name="reporting",
                agent_name="reporter",
                output_preview="Generating final report"
            )
        ]
        
        # Create debug list
        debug_list = ExecutionOutputDebugList(
            run_id=2001,
            execution_id="exec_debug_002",
            total_outputs=3,
            outputs=debug_outputs
        )
        
        # Verify debug workflow
        assert debug_list.run_id == 2001
        assert debug_list.execution_id == "exec_debug_002"
        assert debug_list.total_outputs == 3
        assert len(debug_list.outputs) == 3
        assert debug_list.outputs[0].task_name == "preprocessing"
        assert debug_list.outputs[1].agent_name == "analyzer"
        assert debug_list.outputs[2].output_preview == "Generating final report"
    
    def test_execution_deletion_scenarios(self):
        """Test various execution deletion scenarios."""
        # Delete by run ID
        delete_by_id = DeleteResponse(
            message="Execution 123 deleted successfully",
            deleted_run_id=123,
            deleted_outputs=7
        )
        assert delete_by_id.deleted_run_id == 123
        assert delete_by_id.deleted_outputs == 7
        
        # Delete by job ID
        delete_by_job_id = DeleteResponse(
            message="Execution exec_456 deleted successfully",
            deleted_job_id="exec_456",
            deleted_outputs=12
        )
        assert delete_by_job_id.deleted_job_id == "exec_456"
        assert delete_by_job_id.deleted_outputs == 12
        
        # Bulk delete
        bulk_delete = DeleteResponse(
            message="All user executions deleted",
            deleted_runs=50,
            deleted_outputs=350
        )
        assert bulk_delete.deleted_runs == 50
        assert bulk_delete.deleted_outputs == 350
    
    def test_pagination_across_schemas(self):
        """Test pagination consistency across schemas."""
        now = datetime.now()
        
        # Create execution history with pagination
        executions = [
            ExecutionHistoryItem(
                id=i,
                job_id=f"exec_{i:03d}",
                name=f"Execution {i}",
                created_at=now
            )
            for i in range(1, 6)  # 5 executions
        ]
        
        history_page = ExecutionHistoryList(
            executions=executions,
            total=50,  # Total across all pages
            limit=5,   # 5 per page
            offset=0   # First page
        )
        
        # Create output list with pagination
        outputs = [
            ExecutionOutput(
                id=i,
                job_id="exec_001",
                output=f"Output {i}",
                timestamp=now
            )
            for i in range(1, 11)  # 10 outputs
        ]
        
        output_page = ExecutionOutputList(
            execution_id="exec_001",
            outputs=outputs,
            total=25,  # Total outputs for this execution
            limit=10,  # 10 per page
            offset=0   # First page
        )
        
        # Verify pagination
        assert history_page.limit == 5
        assert len(history_page.executions) == 5
        assert output_page.limit == 10
        assert len(output_page.outputs) == 10
        
        # Simulate next page
        next_history_page = ExecutionHistoryList(
            executions=[],  # Would be filled with next 5 executions
            total=50,
            limit=5,
            offset=5  # Second page
        )
        assert next_history_page.offset == 5
    
    def test_error_execution_tracking(self):
        """Test tracking of failed executions."""
        now = datetime.now()
        
        # Failed execution
        failed_execution = ExecutionHistoryItem(
            id=9999,
            job_id="exec_failed_001",
            name="Failed Analysis",
            model="gpt-4",
            status="failed",
            error="Connection timeout to data source",
            created_at=now,
            execution_type="crew",
            group_email="user@company.com"
        )
        
        # Error output
        error_output = ExecutionOutput(
            id=9999,
            job_id="exec_failed_001",
            task_name="data_load",
            agent_name="loader",
            output="ERROR: Unable to connect to database after 3 retries",
            timestamp=now
        )
        
        # Verify error tracking
        assert failed_execution.status == "failed"
        assert failed_execution.error == "Connection timeout to data source"
        assert "ERROR:" in error_output.output
        assert error_output.job_id == failed_execution.job_id