"""
Unit tests for execution trace schemas.

Tests the functionality of Pydantic schemas for execution trace operations
including validation, serialization, and field constraints.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError

from src.schemas.execution_trace import (
    ExecutionTraceItem, ExecutionTraceList, ExecutionTraceResponseByRunId,
    ExecutionTraceResponseByJobId, DeleteTraceResponse
)


class TestExecutionTraceItem:
    """Test cases for ExecutionTraceItem schema."""
    
    def test_valid_execution_trace_item_minimal(self):
        """Test ExecutionTraceItem with minimal required fields."""
        trace_data = {"id": 123}
        trace = ExecutionTraceItem(**trace_data)
        assert trace.id == 123
        assert trace.run_id is None
        assert trace.job_id is None
        assert trace.timestamp is None
        assert trace.created_at is None
        assert trace.event_source is None
        assert trace.event_context is None
        assert trace.event_type is None
        assert trace.input_data is None
        assert trace.output_data is None
        assert trace.output is None
    
    def test_valid_execution_trace_item_complete(self):
        """Test ExecutionTraceItem with all fields."""
        now = datetime.now()
        trace_data = {
            "id": 456,
            "run_id": 789,
            "job_id": "exec_trace_001",
            "timestamp": now,
            "created_at": now,
            "event_source": "agent_executor",
            "event_context": "task_execution",
            "event_type": "task_start",
            "input_data": {"task_name": "analyze_data", "parameters": {"file": "data.csv"}},
            "output_data": {"status": "started", "task_id": "task_123"},
            "output": {"result": "Task initiated successfully"}
        }
        trace = ExecutionTraceItem(**trace_data)
        assert trace.id == 456
        assert trace.run_id == 789
        assert trace.job_id == "exec_trace_001"
        assert trace.timestamp == now
        assert trace.created_at == now
        assert trace.event_source == "agent_executor"
        assert trace.event_context == "task_execution"
        assert trace.event_type == "task_start"
        assert trace.input_data == {"task_name": "analyze_data", "parameters": {"file": "data.csv"}}
        assert trace.output_data == {"status": "started", "task_id": "task_123"}
        assert trace.output == {"result": "Task initiated successfully"}
    
    def test_execution_trace_item_missing_id(self):
        """Test ExecutionTraceItem validation with missing required id."""
        with pytest.raises(ValidationError) as exc_info:
            ExecutionTraceItem(run_id=123)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "id" in missing_fields
    
    def test_execution_trace_item_various_event_types(self):
        """Test ExecutionTraceItem with various event types."""
        event_types = [
            "task_start", "task_complete", "task_error",
            "agent_start", "agent_complete", "agent_error",
            "tool_call", "tool_response", "llm_request", "llm_response"
        ]
        
        for event_type in event_types:
            trace = ExecutionTraceItem(
                id=1,
                event_type=event_type,
                event_source="test_source"
            )
            assert trace.event_type == event_type
    
    def test_execution_trace_item_various_output_types(self):
        """Test ExecutionTraceItem with various output types."""
        # Dict output
        trace_dict = ExecutionTraceItem(
            id=1,
            output={"result": "success", "data": [1, 2, 3]}
        )
        assert isinstance(trace_dict.output, dict)
        assert trace_dict.output["result"] == "success"
        
        # String output
        trace_string = ExecutionTraceItem(
            id=2,
            output="Simple string output"
        )
        assert isinstance(trace_string.output, str)
        assert trace_string.output == "Simple string output"
        
        # List output
        trace_list = ExecutionTraceItem(
            id=3,
            output=["item1", "item2", "item3"]
        )
        assert isinstance(trace_list.output, list)
        assert len(trace_list.output) == 3
        
        # Number output
        trace_number = ExecutionTraceItem(
            id=4,
            output=42
        )
        assert trace_number.output == 42
        
        # Boolean output
        trace_bool = ExecutionTraceItem(
            id=5,
            output=True
        )
        assert trace_bool.output is True
    
    def test_execution_trace_item_complex_data_structures(self):
        """Test ExecutionTraceItem with complex data structures."""
        complex_input = {
            "workflow": {
                "id": "wf_001",
                "name": "Data Processing Pipeline",
                "steps": [
                    {"step": 1, "action": "load_data", "params": {"source": "db"}},
                    {"step": 2, "action": "clean_data", "params": {"method": "pandas"}},
                    {"step": 3, "action": "analyze", "params": {"model": "linear_regression"}}
                ]
            },
            "execution_context": {
                "user_id": "user_123",
                "session_id": "session_456",
                "timestamp": "2023-01-01T12:00:00Z",
                "environment": "production"
            }
        }
        
        complex_output = {
            "execution_summary": {
                "status": "completed",
                "duration_ms": 15000,
                "steps_completed": 3,
                "errors": []
            },
            "results": {
                "rows_processed": 10000,
                "data_quality_score": 0.95,
                "model_accuracy": 0.87,
                "predictions": [0.1, 0.3, 0.7, 0.9]
            },
            "metadata": {
                "memory_usage_mb": 512,
                "cpu_time_ms": 8000,
                "io_operations": 25
            }
        }
        
        trace = ExecutionTraceItem(
            id=999,
            input_data=complex_input,
            output_data=complex_output
        )
        
        assert trace.input_data["workflow"]["name"] == "Data Processing Pipeline"
        assert len(trace.input_data["workflow"]["steps"]) == 3
        assert trace.output_data["execution_summary"]["status"] == "completed"
        assert trace.output_data["results"]["rows_processed"] == 10000
    
    def test_execution_trace_item_model_config(self):
        """Test ExecutionTraceItem model configuration."""
        assert hasattr(ExecutionTraceItem, 'model_config')
        assert ExecutionTraceItem.model_config.get('from_attributes') is True
    
    def test_execution_trace_item_datetime_handling(self):
        """Test ExecutionTraceItem with datetime handling."""
        now = datetime.now()
        iso_timestamp = "2023-01-01T12:00:00"
        
        # Test with datetime objects
        trace_datetime = ExecutionTraceItem(
            id=1,
            timestamp=now,
            created_at=now
        )
        assert isinstance(trace_datetime.timestamp, datetime)
        assert isinstance(trace_datetime.created_at, datetime)
        
        # Test with ISO string conversion
        trace_iso = ExecutionTraceItem(
            id=2,
            timestamp=iso_timestamp,
            created_at=iso_timestamp
        )
        assert isinstance(trace_iso.timestamp, datetime)
        assert isinstance(trace_iso.created_at, datetime)


class TestExecutionTraceList:
    """Test cases for ExecutionTraceList schema."""
    
    def test_valid_execution_trace_list_empty(self):
        """Test ExecutionTraceList with empty traces list."""
        list_data = {
            "traces": [],
            "total": 0,
            "limit": 10,
            "offset": 0
        }
        trace_list = ExecutionTraceList(**list_data)
        assert trace_list.traces == []
        assert trace_list.total == 0
        assert trace_list.limit == 10
        assert trace_list.offset == 0
    
    def test_valid_execution_trace_list_with_items(self):
        """Test ExecutionTraceList with trace items."""
        now = datetime.now()
        traces = [
            ExecutionTraceItem(
                id=1,
                job_id="exec_001",
                event_type="task_start",
                timestamp=now
            ),
            ExecutionTraceItem(
                id=2,
                job_id="exec_001",
                event_type="task_complete",
                timestamp=now
            ),
            ExecutionTraceItem(
                id=3,
                job_id="exec_002",
                event_type="agent_start",
                timestamp=now
            )
        ]
        
        list_data = {
            "traces": traces,
            "total": 100,
            "limit": 3,
            "offset": 0
        }
        trace_list = ExecutionTraceList(**list_data)
        assert len(trace_list.traces) == 3
        assert trace_list.total == 100
        assert trace_list.limit == 3
        assert trace_list.offset == 0
        assert trace_list.traces[0].event_type == "task_start"
        assert trace_list.traces[1].event_type == "task_complete"
        assert trace_list.traces[2].event_type == "agent_start"
    
    def test_execution_trace_list_missing_fields(self):
        """Test ExecutionTraceList validation with missing fields."""
        required_fields = ["traces", "total", "limit", "offset"]
        
        for missing_field in required_fields:
            list_data = {
                "traces": [],
                "total": 0,
                "limit": 10,
                "offset": 0
            }
            del list_data[missing_field]
            
            with pytest.raises(ValidationError) as exc_info:
                ExecutionTraceList(**list_data)
            
            errors = exc_info.value.errors()
            missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
            assert missing_field in missing_fields
    
    def test_execution_trace_list_pagination_scenarios(self):
        """Test ExecutionTraceList with various pagination scenarios."""
        base_trace = ExecutionTraceItem(id=1, event_type="test")
        
        # First page
        first_page = ExecutionTraceList(
            traces=[base_trace],
            total=50,
            limit=10,
            offset=0
        )
        assert first_page.offset == 0
        assert first_page.limit == 10
        
        # Middle page
        middle_page = ExecutionTraceList(
            traces=[base_trace],
            total=50,
            limit=10,
            offset=20
        )
        assert middle_page.offset == 20
        
        # Last page with fewer items
        last_page = ExecutionTraceList(
            traces=[base_trace],
            total=45,
            limit=10,
            offset=40
        )
        assert last_page.total == 45
        assert last_page.offset == 40


class TestExecutionTraceResponseByRunId:
    """Test cases for ExecutionTraceResponseByRunId schema."""
    
    def test_valid_execution_trace_response_by_run_id(self):
        """Test ExecutionTraceResponseByRunId with valid data."""
        now = datetime.now()
        traces = [
            ExecutionTraceItem(
                id=1,
                run_id=123,
                event_type="execution_start",
                timestamp=now
            ),
            ExecutionTraceItem(
                id=2,
                run_id=123,
                event_type="task_start",
                timestamp=now
            )
        ]
        
        response_data = {
            "run_id": 123,
            "traces": traces
        }
        response = ExecutionTraceResponseByRunId(**response_data)
        assert response.run_id == 123
        assert len(response.traces) == 2
        assert response.traces[0].run_id == 123
        assert response.traces[1].run_id == 123
    
    def test_execution_trace_response_by_run_id_missing_fields(self):
        """Test ExecutionTraceResponseByRunId validation with missing fields."""
        # Missing run_id
        with pytest.raises(ValidationError) as exc_info:
            ExecutionTraceResponseByRunId(traces=[])
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "run_id" in missing_fields
        
        # Missing traces
        with pytest.raises(ValidationError) as exc_info:
            ExecutionTraceResponseByRunId(run_id=123)
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "traces" in missing_fields
    
    def test_execution_trace_response_by_run_id_empty_traces(self):
        """Test ExecutionTraceResponseByRunId with empty traces."""
        response = ExecutionTraceResponseByRunId(
            run_id=456,
            traces=[]
        )
        assert response.run_id == 456
        assert response.traces == []


class TestExecutionTraceResponseByJobId:
    """Test cases for ExecutionTraceResponseByJobId schema."""
    
    def test_valid_execution_trace_response_by_job_id(self):
        """Test ExecutionTraceResponseByJobId with valid data."""
        now = datetime.now()
        traces = [
            ExecutionTraceItem(
                id=1,
                job_id="exec_job_001",
                event_type="workflow_start",
                timestamp=now
            ),
            ExecutionTraceItem(
                id=2,
                job_id="exec_job_001",
                event_type="agent_created",
                timestamp=now
            ),
            ExecutionTraceItem(
                id=3,
                job_id="exec_job_001",
                event_type="workflow_complete",
                timestamp=now
            )
        ]
        
        response_data = {
            "job_id": "exec_job_001",
            "traces": traces
        }
        response = ExecutionTraceResponseByJobId(**response_data)
        assert response.job_id == "exec_job_001"
        assert len(response.traces) == 3
        assert all(trace.job_id == "exec_job_001" for trace in response.traces)
    
    def test_execution_trace_response_by_job_id_missing_fields(self):
        """Test ExecutionTraceResponseByJobId validation with missing fields."""
        # Missing job_id
        with pytest.raises(ValidationError) as exc_info:
            ExecutionTraceResponseByJobId(traces=[])
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "job_id" in missing_fields
        
        # Missing traces
        with pytest.raises(ValidationError) as exc_info:
            ExecutionTraceResponseByJobId(job_id="test")
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "traces" in missing_fields
    
    def test_execution_trace_response_by_job_id_empty_traces(self):
        """Test ExecutionTraceResponseByJobId with empty traces."""
        response = ExecutionTraceResponseByJobId(
            job_id="exec_empty_001",
            traces=[]
        )
        assert response.job_id == "exec_empty_001"
        assert response.traces == []


class TestDeleteTraceResponse:
    """Test cases for DeleteTraceResponse schema."""
    
    def test_valid_delete_trace_response_single(self):
        """Test DeleteTraceResponse for single trace deletion."""
        response_data = {
            "message": "Trace deleted successfully",
            "deleted_trace_id": 123
        }
        response = DeleteTraceResponse(**response_data)
        assert response.message == "Trace deleted successfully"
        assert response.deleted_trace_id == 123
        assert response.deleted_traces is None
    
    def test_valid_delete_trace_response_bulk(self):
        """Test DeleteTraceResponse for bulk trace deletion."""
        response_data = {
            "message": "All traces for execution deleted",
            "deleted_traces": 25
        }
        response = DeleteTraceResponse(**response_data)
        assert response.message == "All traces for execution deleted"
        assert response.deleted_trace_id is None
        assert response.deleted_traces == 25
    
    def test_delete_trace_response_missing_message(self):
        """Test DeleteTraceResponse validation with missing message."""
        with pytest.raises(ValidationError) as exc_info:
            DeleteTraceResponse(deleted_trace_id=123)
        
        errors = exc_info.value.errors()
        missing_fields = [error["loc"][0] for error in errors if error["type"] == "missing"]
        assert "message" in missing_fields
    
    def test_delete_trace_response_minimal(self):
        """Test DeleteTraceResponse with minimal data."""
        response = DeleteTraceResponse(message="Deletion completed")
        assert response.message == "Deletion completed"
        assert response.deleted_trace_id is None
        assert response.deleted_traces is None


class TestExecutionTraceSchemaIntegration:
    """Integration tests for execution trace schema interactions."""
    
    def test_complete_trace_workflow(self):
        """Test complete execution trace workflow."""
        now = datetime.now()
        
        # Create execution traces for a complete workflow
        workflow_traces = [
            ExecutionTraceItem(
                id=1,
                run_id=1001,
                job_id="exec_workflow_001",
                timestamp=now,
                event_source="workflow_engine",
                event_type="execution_start",
                input_data={"workflow_id": "wf_001", "user_id": "user_123"},
                output_data={"status": "started", "execution_id": "exec_workflow_001"}
            ),
            ExecutionTraceItem(
                id=2,
                run_id=1001,
                job_id="exec_workflow_001",
                timestamp=now,
                event_source="agent_manager",
                event_type="agent_created",
                input_data={"agent_type": "data_analyst", "config": {"model": "gpt-4"}},
                output_data={"agent_id": "agent_001", "status": "ready"}
            ),
            ExecutionTraceItem(
                id=3,
                run_id=1001,
                job_id="exec_workflow_001",
                timestamp=now,
                event_source="task_executor",
                event_type="task_start",
                input_data={"task_name": "analyze_data", "agent_id": "agent_001"},
                output_data={"task_id": "task_001", "status": "running"}
            ),
            ExecutionTraceItem(
                id=4,
                run_id=1001,
                job_id="exec_workflow_001",
                timestamp=now,
                event_source="task_executor",
                event_type="task_complete",
                input_data={"task_id": "task_001"},
                output_data={"status": "completed", "result": {"insights": 3, "confidence": 0.85}}
            ),
            ExecutionTraceItem(
                id=5,
                run_id=1001,
                job_id="exec_workflow_001",
                timestamp=now,
                event_source="workflow_engine",
                event_type="execution_complete",
                input_data={"execution_id": "exec_workflow_001"},
                output_data={"status": "completed", "duration_ms": 15000}
            )
        ]
        
        # Create trace list
        trace_list = ExecutionTraceList(
            traces=workflow_traces,
            total=5,
            limit=10,
            offset=0
        )
        
        # Create response by run_id
        response_by_run = ExecutionTraceResponseByRunId(
            run_id=1001,
            traces=workflow_traces
        )
        
        # Create response by job_id
        response_by_job = ExecutionTraceResponseByJobId(
            job_id="exec_workflow_001",
            traces=workflow_traces
        )
        
        # Verify workflow
        assert len(trace_list.traces) == 5
        assert trace_list.traces[0].event_type == "execution_start"
        assert trace_list.traces[-1].event_type == "execution_complete"
        assert response_by_run.run_id == 1001
        assert response_by_job.job_id == "exec_workflow_001"
        assert all(trace.run_id == 1001 for trace in response_by_run.traces)
        assert all(trace.job_id == "exec_workflow_001" for trace in response_by_job.traces)
    
    def test_error_trace_workflow(self):
        """Test execution trace workflow with errors."""
        now = datetime.now()
        
        # Create traces showing error scenario
        error_traces = [
            ExecutionTraceItem(
                id=1,
                run_id=2001,
                job_id="exec_error_001",
                timestamp=now,
                event_source="workflow_engine",
                event_type="execution_start",
                input_data={"workflow_id": "wf_error"},
                output_data={"status": "started"}
            ),
            ExecutionTraceItem(
                id=2,
                run_id=2001,
                job_id="exec_error_001",
                timestamp=now,
                event_source="agent_manager",
                event_type="agent_error",
                input_data={"agent_type": "unreliable_agent"},
                output_data={"error": "Failed to initialize agent", "error_code": "AGENT_INIT_FAILED"}
            ),
            ExecutionTraceItem(
                id=3,
                run_id=2001,
                job_id="exec_error_001",
                timestamp=now,
                event_source="workflow_engine",
                event_type="execution_error",
                input_data={"execution_id": "exec_error_001"},
                output_data={"status": "failed", "error": "Agent initialization failed"}
            )
        ]
        
        # Create error trace response
        error_response = ExecutionTraceResponseByJobId(
            job_id="exec_error_001",
            traces=error_traces
        )
        
        # Verify error workflow
        assert len(error_response.traces) == 3
        assert error_response.traces[1].event_type == "agent_error"
        assert error_response.traces[2].event_type == "execution_error"
        assert "error" in error_response.traces[1].output_data
        assert "AGENT_INIT_FAILED" in error_response.traces[1].output_data["error_code"]
    
    def test_trace_deletion_scenarios(self):
        """Test various trace deletion scenarios."""
        # Single trace deletion
        single_delete = DeleteTraceResponse(
            message="Trace 123 deleted successfully",
            deleted_trace_id=123
        )
        assert single_delete.deleted_trace_id == 123
        assert single_delete.deleted_traces is None
        
        # Bulk deletion by execution
        bulk_delete_execution = DeleteTraceResponse(
            message="All traces for execution exec_001 deleted",
            deleted_traces=15
        )
        assert bulk_delete_execution.deleted_traces == 15
        assert bulk_delete_execution.deleted_trace_id is None
        
        # Bulk deletion by time range
        bulk_delete_timerange = DeleteTraceResponse(
            message="All traces older than 30 days deleted",
            deleted_traces=500
        )
        assert bulk_delete_timerange.deleted_traces == 500
    
    def test_trace_pagination_workflow(self):
        """Test trace pagination across multiple pages."""
        now = datetime.now()
        
        # Create traces for pagination testing
        all_traces = [
            ExecutionTraceItem(
                id=i,
                job_id="exec_paginated_001",
                event_type=f"event_{i}",
                timestamp=now
            )
            for i in range(1, 101)  # 100 traces
        ]
        
        # First page
        first_page = ExecutionTraceList(
            traces=all_traces[:10],  # First 10 traces
            total=100,
            limit=10,
            offset=0
        )
        
        # Middle page
        middle_page = ExecutionTraceList(
            traces=all_traces[40:50],  # Traces 41-50
            total=100,
            limit=10,
            offset=40
        )
        
        # Last page
        last_page = ExecutionTraceList(
            traces=all_traces[90:],  # Last 10 traces
            total=100,
            limit=10,
            offset=90
        )
        
        # Verify pagination
        assert len(first_page.traces) == 10
        assert first_page.offset == 0
        assert first_page.traces[0].id == 1
        
        assert len(middle_page.traces) == 10
        assert middle_page.offset == 40
        assert middle_page.traces[0].id == 41
        
        assert len(last_page.traces) == 10
        assert last_page.offset == 90
        assert last_page.traces[0].id == 91
    
    def test_trace_event_sequence_analysis(self):
        """Test trace sequence for analyzing execution flow."""
        now = datetime.now()
        
        # Create a realistic sequence of traces
        sequence_traces = [
            # Execution setup
            ExecutionTraceItem(
                id=1, event_type="execution_init", event_source="scheduler",
                timestamp=now, input_data={"priority": "high"}
            ),
            ExecutionTraceItem(
                id=2, event_type="resource_allocation", event_source="resource_manager",
                timestamp=now, output_data={"allocated_memory_mb": 1024, "cpu_cores": 4}
            ),
            
            # Agent lifecycle
            ExecutionTraceItem(
                id=3, event_type="agent_spawn", event_source="agent_factory",
                timestamp=now, output_data={"agent_count": 3}
            ),
            ExecutionTraceItem(
                id=4, event_type="agent_config", event_source="config_manager",
                timestamp=now, input_data={"model": "gpt-4", "temperature": 0.7}
            ),
            
            # Task execution
            ExecutionTraceItem(
                id=5, event_type="task_queue", event_source="task_scheduler",
                timestamp=now, output_data={"queued_tasks": 5}
            ),
            ExecutionTraceItem(
                id=6, event_type="task_dispatch", event_source="task_scheduler",
                timestamp=now, output_data={"dispatched_tasks": 3, "pending_tasks": 2}
            ),
            
            # Results processing
            ExecutionTraceItem(
                id=7, event_type="result_aggregation", event_source="result_processor",
                timestamp=now, output_data={"processed_results": 3, "total_insights": 12}
            ),
            ExecutionTraceItem(
                id=8, event_type="execution_finalize", event_source="scheduler",
                timestamp=now, output_data={"status": "success", "total_duration_ms": 25000}
            )
        ]
        
        # Create trace response
        sequence_response = ExecutionTraceResponseByJobId(
            job_id="exec_sequence_001",
            traces=sequence_traces
        )
        
        # Analyze sequence
        event_types = [trace.event_type for trace in sequence_response.traces]
        event_sources = list(set(trace.event_source for trace in sequence_response.traces))
        
        # Verify sequence analysis
        assert len(sequence_response.traces) == 8
        assert event_types[0] == "execution_init"  # First event
        assert event_types[-1] == "execution_finalize"  # Last event
        assert "scheduler" in event_sources
        assert "agent_factory" in event_sources
        assert "task_scheduler" in event_sources
        assert "result_processor" in event_sources
        
        # Verify data flow
        resource_trace = next(t for t in sequence_response.traces if t.event_type == "resource_allocation")
        assert resource_trace.output_data["allocated_memory_mb"] == 1024
        
        final_trace = next(t for t in sequence_response.traces if t.event_type == "execution_finalize")
        assert final_trace.output_data["status"] == "success"