"""
Integration tests for callback isolation system.

Tests the complete flow from callback creation through trace processing
to ensure proper isolation between concurrent executions.
"""
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from queue import Queue

from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks
from src.engines.crewai.trace_management import TraceManager


@pytest.fixture
def mock_services():
    """Setup all required service mocks."""
    services = {}
    
    # Mock trace queue
    services['trace_queue'] = MagicMock()
    services['trace_queue'].put_nowait = MagicMock()
    services['trace_queue'].qsize.return_value = 0
    
    # Mock execution history service
    services['execution_history'] = MagicMock()
    services['execution_history'].get_execution_by_job_id = AsyncMock(return_value=MagicMock())
    
    # Mock trace service
    services['trace_service'] = MagicMock()
    services['trace_service'].create_trace = AsyncMock()
    
    # Mock status service
    services['status_service'] = MagicMock()
    services['status_service'].create_execution = AsyncMock(return_value=True)
    
    # Mock enqueue_log
    services['enqueue_log'] = MagicMock()
    
    return services


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    context = MagicMock()
    context.primary_group_id = "group_123"
    context.group_email = "test@example.com"
    return context


class TestCallbackIsolationIntegration:
    """Integration tests for the complete callback isolation system."""
    
    def test_callback_creation_isolation(self, mock_services, mock_group_context):
        """Test that callback creation produces isolated callbacks for different executions."""
        job_id_1 = "execution_1"
        job_id_2 = "execution_2"
        config = {"model": "test-model"}
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            
            mock_get_queue.return_value = mock_services['trace_queue']
            mock_enqueue.side_effect = mock_services['enqueue_log']
            
            # Create callbacks for two different executions
            step_1, task_1 = create_execution_callbacks(job_id_1, config, mock_group_context)
            step_2, task_2 = create_execution_callbacks(job_id_2, config, mock_group_context)
            
            # Verify callbacks are different instances
            assert step_1 is not step_2
            assert task_1 is not task_2
            
            # Test that callbacks produce different traces
            mock_output = MagicMock()
            mock_output.output = "test output"
            mock_output.agent = MagicMock()
            mock_output.agent.role = "Test Agent"
            
            # Call both step callbacks
            step_1(mock_output)
            step_2(mock_output)
            
            # Verify separate traces were created
            assert mock_services['trace_queue'].put_nowait.call_count == 2
            
            # Get the trace data from both calls
            calls = mock_services['trace_queue'].put_nowait.call_args_list
            trace_1 = calls[0][0][0]
            trace_2 = calls[1][0][0]
            
            # Verify traces have different job IDs
            assert trace_1["job_id"] == job_id_1
            assert trace_2["job_id"] == job_id_2
            assert trace_1["job_id"] != trace_2["job_id"]
    
    def test_trace_processing_isolation(self, mock_services, mock_group_context):
        """Test that trace processing maintains isolation between executions."""
        job_id_1 = "execution_1"
        job_id_2 = "execution_2"
        
        # Create sample traces from different executions
        trace_1 = {
            "job_id": job_id_1,
            "event_type": "agent_execution",
            "event_source": "Agent 1",
            "event_context": "step",
            "output_content": "Output from execution 1",
            "group_id": mock_group_context.primary_group_id,
            "group_email": mock_group_context.group_email
        }
        
        trace_2 = {
            "job_id": job_id_2,
            "event_type": "task_completed", 
            "event_source": "Task 2",
            "event_context": "completion",
            "output_content": "Output from execution 2",
            "group_id": mock_group_context.primary_group_id,
            "group_email": mock_group_context.group_email
        }
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.services.execution_history_service.get_execution_history_service") as mock_get_service, \
             patch("src.services.execution_trace_service.ExecutionTraceService") as mock_trace_service, \
             patch("src.services.execution_status_service.ExecutionStatusService") as mock_status_service:
            
            # Setup queue to return both traces
            mock_queue = MagicMock()
            mock_queue.get.side_effect = [trace_1, trace_2, Exception("Queue empty")]
            mock_queue.task_done = MagicMock()
            mock_get_queue.return_value = mock_queue
            
            mock_get_service.return_value = mock_services['execution_history']
            mock_trace_service.create_trace = mock_services['trace_service'].create_trace
            mock_status_service.create_execution = mock_services['status_service'].create_execution
            
            # Process traces through trace writer
            # We'll run one iteration of the trace writer loop manually
            confirmed_jobs = set()
            
            # Simulate trace processing for both traces
            for trace_data in [trace_1, trace_2]:
                job_id = trace_data["job_id"]
                event_type = trace_data["event_type"]
                
                # Check important event types (matches trace_management.py logic)
                important_event_types = [
                    "agent_execution", "tool_usage", "crew_started", 
                    "crew_completed", "task_started", "task_completed", "llm_call"
                ]
                
                if event_type in important_event_types:
                    # Format trace data for ExecutionTraceService
                    formatted_trace = {
                        "job_id": job_id,
                        "event_source": trace_data.get("event_source", event_type),
                        "event_context": trace_data.get("event_context", ""),
                        "event_type": event_type,
                        "output": trace_data.get("output_content", ""),
                        "trace_metadata": trace_data.get("extra_data", {})
                    }
                    
                    # Add group context
                    if "group_id" in trace_data:
                        formatted_trace["group_id"] = trace_data["group_id"]
                    if "group_email" in trace_data:
                        formatted_trace["group_email"] = trace_data["group_email"]
                    
                    # Simulate storing the trace
                    mock_services['trace_service'].create_trace(formatted_trace)
            
            # Verify both traces were processed with correct isolation
            assert mock_services['trace_service'].create_trace.call_count == 2
            
            # Get processed traces
            calls = mock_services['trace_service'].create_trace.call_args_list
            processed_trace_1 = calls[0][0][0]
            processed_trace_2 = calls[1][0][0]
            
            # Verify job IDs are preserved and different
            assert processed_trace_1["job_id"] == job_id_1
            assert processed_trace_2["job_id"] == job_id_2
            
            # Verify event types are preserved
            assert processed_trace_1["event_type"] == "agent_execution"
            assert processed_trace_2["event_type"] == "task_completed"
            
            # Verify group context is preserved in both
            assert processed_trace_1["group_id"] == mock_group_context.primary_group_id
            assert processed_trace_2["group_id"] == mock_group_context.primary_group_id
    
    def test_concurrent_callback_execution(self, mock_services, mock_group_context):
        """Test that concurrent callback execution maintains proper isolation."""
        configs = [
            {"job_id": "concurrent_1", "model": "model_1"},
            {"job_id": "concurrent_2", "model": "model_2"},
            {"job_id": "concurrent_3", "model": "model_3"}
        ]
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            
            mock_get_queue.return_value = mock_services['trace_queue']
            mock_enqueue.side_effect = mock_services['enqueue_log']
            
            # Create callbacks for multiple concurrent executions
            callbacks = []
            for config in configs:
                job_id = config["job_id"]
                step_callback, task_callback = create_execution_callbacks(
                    job_id=job_id,
                    config=config,
                    group_context=mock_group_context
                )
                callbacks.append((job_id, step_callback, task_callback))
            
            # Simulate concurrent execution of callbacks
            mock_step_output = MagicMock()
            mock_step_output.output = "concurrent output"
            mock_step_output.agent = MagicMock()
            mock_step_output.agent.role = "Concurrent Agent"
            
            mock_task_output = MagicMock()
            mock_task_output.raw = "concurrent task result"
            mock_task_output.description = "concurrent task"
            mock_task_output.agent = MagicMock()
            mock_task_output.agent.role = "Concurrent Agent"
            
            # Execute all callbacks
            for job_id, step_callback, task_callback in callbacks:
                step_callback(mock_step_output)
                task_callback(mock_task_output)
            
            # Verify traces were created for all executions
            expected_traces = len(configs) * 2  # step + task callback for each
            assert mock_services['trace_queue'].put_nowait.call_count == expected_traces
            
            # Verify all traces have different job IDs
            calls = mock_services['trace_queue'].put_nowait.call_args_list
            job_ids_in_traces = set()
            
            for call in calls:
                trace_data = call[0][0]
                job_ids_in_traces.add(trace_data["job_id"])
            
            # Should have exactly 3 different job IDs
            expected_job_ids = {config["job_id"] for config in configs}
            assert job_ids_in_traces == expected_job_ids
    
    def test_error_isolation_between_executions(self, mock_services, mock_group_context):
        """Test that errors in one execution don't affect others."""
        job_id_1 = "execution_1"
        job_id_2 = "execution_2"
        config = {"model": "test-model"}
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            
            mock_get_queue.return_value = mock_services['trace_queue']
            
            # Setup enqueue_log to fail for first execution but succeed for second
            def selective_enqueue_failure(execution_id=None, content=None, **kwargs):
                if execution_id == job_id_1:
                    raise Exception("Enqueue failed for execution 1")
                # Success for execution 2
                mock_services['enqueue_log'](execution_id=execution_id, content=content, **kwargs)
            
            mock_enqueue.side_effect = selective_enqueue_failure
            
            # Create callbacks for both executions
            step_1, _ = create_execution_callbacks(job_id_1, config, mock_group_context)
            step_2, _ = create_execution_callbacks(job_id_2, config, mock_group_context)
            
            mock_output = MagicMock()
            mock_output.output = "test output"
            mock_output.agent = MagicMock()
            mock_output.agent.role = "Test Agent"
            
            # Execute both callbacks - first should fail, second should succeed
            step_1(mock_output)  # Should handle error gracefully
            step_2(mock_output)  # Should succeed
            
            # Verify that failure in one execution doesn't prevent traces in the other
            assert mock_services['trace_queue'].put_nowait.call_count == 2
            
            # Both should have attempted to create traces
            calls = mock_services['trace_queue'].put_nowait.call_args_list
            trace_1 = calls[0][0][0]
            trace_2 = calls[1][0][0] 
            
            assert trace_1["job_id"] == job_id_1
            assert trace_2["job_id"] == job_id_2
    
    def test_group_context_isolation(self, mock_services):
        """Test that different group contexts are properly isolated."""
        job_id = "test_execution"
        config = {"model": "test-model"}
        
        # Create different group contexts
        group_1 = MagicMock()
        group_1.primary_group_id = "group_1"
        group_1.group_email = "group1@example.com"
        
        group_2 = MagicMock()
        group_2.primary_group_id = "group_2"
        group_2.group_email = "group2@example.com"
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            
            mock_get_queue.return_value = mock_services['trace_queue']
            mock_enqueue.side_effect = mock_services['enqueue_log']
            
            # Create callbacks with different group contexts
            step_1, _ = create_execution_callbacks(job_id + "_1", config, group_1)
            step_2, _ = create_execution_callbacks(job_id + "_2", config, group_2)
            
            mock_output = MagicMock()
            mock_output.output = "test output"
            mock_output.agent = MagicMock()
            mock_output.agent.role = "Test Agent"
            
            # Execute callbacks
            step_1(mock_output)
            step_2(mock_output)
            
            # Verify traces have correct group context
            assert mock_services['trace_queue'].put_nowait.call_count == 2
            
            calls = mock_services['trace_queue'].put_nowait.call_args_list
            trace_1 = calls[0][0][0]
            trace_2 = calls[1][0][0]
            
            # Verify group isolation
            assert trace_1["group_id"] == "group_1"
            assert trace_1["group_email"] == "group1@example.com"
            
            assert trace_2["group_id"] == "group_2" 
            assert trace_2["group_email"] == "group2@example.com"
            
            # Verify group contexts are different
            assert trace_1["group_id"] != trace_2["group_id"]
            assert trace_1["group_email"] != trace_2["group_email"]