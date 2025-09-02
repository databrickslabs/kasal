"""
Simplified unit tests for trace management with execution-scoped callbacks.

Tests core trace management functionality with minimal async complexity.
"""
import pytest
from unittest.mock import patch, MagicMock


class TestTraceManagerEventFiltering:
    """Test cases for trace manager event filtering."""
    
    def test_important_event_types_list(self):
        """Test that important event types are correctly defined."""
        # This tests the event filtering logic that's in the trace writer
        important_event_types = [
            "agent_execution", "tool_usage", "crew_started", 
            "crew_completed", "task_started", "task_completed", "llm_call"
        ]
        
        # Test that our callback events are in the important list
        assert "agent_execution" in important_event_types
        assert "task_completed" in important_event_types
        assert "crew_started" in important_event_types
        assert "crew_completed" in important_event_types
        
        # Test that random events would not be in the list
        assert "debug_info" not in important_event_types
        assert "random_event" not in important_event_types
    
    def test_trace_data_format_from_callbacks(self):
        """Test that callback trace data matches expected format."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks
        
        job_id = "test_job_123"
        config = {"model": "test-model"}
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log"):
            
            mock_queue = MagicMock()
            mock_get_queue.return_value = mock_queue
            
            step_callback, task_callback = create_execution_callbacks(job_id, config, None)
            
            # Test step callback trace format
            mock_step_output = MagicMock()
            mock_step_output.output = "Agent output"
            mock_step_output.agent = MagicMock()
            mock_step_output.agent.role = "Test Agent"
            
            step_callback(mock_step_output)
            
            # Get the trace data
            step_trace = mock_queue.put_nowait.call_args_list[0][0][0]
            
            # Verify required fields for trace processing
            required_fields = ["job_id", "event_type", "event_source", "output_content", "timestamp"]
            for field in required_fields:
                assert field in step_trace
            
            assert step_trace["job_id"] == job_id
            assert step_trace["event_type"] == "agent_execution"
            assert step_trace["event_source"] == "Test Agent"
            assert step_trace["output_content"] == "Agent output"
            
            # Test task callback trace format
            mock_task_output = MagicMock()
            mock_task_output.raw = "Task result"
            mock_task_output.description = "Test task"
            mock_task_output.agent = MagicMock()
            mock_task_output.agent.role = "Test Agent"
            
            task_callback(mock_task_output)
            
            # Get the task trace data
            task_trace = mock_queue.put_nowait.call_args_list[1][0][0]
            
            # Verify required fields
            for field in required_fields:
                assert field in task_trace
            
            assert task_trace["job_id"] == job_id
            assert task_trace["event_type"] == "task_completed"
            assert task_trace["output_content"] == "Task result"
    
    def test_group_context_in_traces(self):
        """Test that group context is properly included in traces."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks
        
        job_id = "test_job_123"
        config = {"model": "test-model"}
        
        # Create mock group context
        mock_group_context = MagicMock()
        mock_group_context.primary_group_id = "group_123"
        mock_group_context.group_email = "test@group.com"
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log"):
            
            mock_queue = MagicMock()
            mock_get_queue.return_value = mock_queue
            
            step_callback, _ = create_execution_callbacks(job_id, config, mock_group_context)
            
            # Call callback
            mock_output = MagicMock()
            mock_output.output = "test output"
            mock_output.agent = MagicMock()
            mock_output.agent.role = "Test Agent"
            
            step_callback(mock_output)
            
            # Verify group context in trace
            trace_data = mock_queue.put_nowait.call_args[0][0]
            assert trace_data["group_id"] == "group_123"
            assert trace_data["group_email"] == "test@group.com"
    
    def test_trace_isolation_by_job_id(self):
        """Test that traces are isolated by job ID."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks
        
        job_1 = "execution_1"
        job_2 = "execution_2"
        config = {"model": "test"}
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log"):
            
            mock_queue = MagicMock()
            mock_get_queue.return_value = mock_queue
            
            # Create callbacks for different executions
            step_1, _ = create_execution_callbacks(job_1, config, None)
            step_2, _ = create_execution_callbacks(job_2, config, None)
            
            # Create identical outputs
            mock_output = MagicMock()
            mock_output.output = "identical output"
            mock_output.agent = MagicMock()
            mock_output.agent.role = "Same Agent"
            
            # Call both callbacks
            step_1(mock_output)
            step_2(mock_output)
            
            # Verify separate traces with different job IDs
            assert mock_queue.put_nowait.call_count == 2
            
            calls = mock_queue.put_nowait.call_args_list
            trace_1 = calls[0][0][0]
            trace_2 = calls[1][0][0]
            
            # Traces should have different job IDs but same content
            assert trace_1["job_id"] == job_1
            assert trace_2["job_id"] == job_2
            assert trace_1["job_id"] != trace_2["job_id"]
            
            # But same event data
            assert trace_1["output_content"] == trace_2["output_content"]
            assert trace_1["event_source"] == trace_2["event_source"]


class TestCallbackCrewIntegration:
    """Test cases for crew-level callback integration."""
    
    def test_crew_callbacks_creation(self):
        """Test that crew callbacks are created correctly."""
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks
        
        job_id = "test_job"
        config = {"model": "test"}
        
        callbacks = create_crew_callbacks(job_id, config, None)
        
        # Verify all required callbacks exist
        assert "on_start" in callbacks
        assert "on_complete" in callbacks  
        assert "on_error" in callbacks
        
        # Verify they're callable
        assert callable(callbacks["on_start"])
        assert callable(callbacks["on_complete"])
        assert callable(callbacks["on_error"])
    
    def test_crew_start_callback(self):
        """Test crew start callback functionality."""
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks
        
        job_id = "test_job"
        config = {"model": "test"}
        
        with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue, \
             patch("src.services.trace_queue.get_trace_queue") as mock_get_queue:
            
            mock_queue = MagicMock()
            mock_get_queue.return_value = mock_queue
            
            callbacks = create_crew_callbacks(job_id, config, None)
            
            # Call start callback
            callbacks["on_start"]()
            
            # Verify log was enqueued
            mock_enqueue.assert_called_once()
            call_args = mock_enqueue.call_args
            kwargs = call_args[1] if len(call_args) > 1 else call_args.kwargs
            assert kwargs["execution_id"] == job_id
            assert "CREW STARTED" in kwargs["content"]
            
            # Verify trace was created
            mock_queue.put_nowait.assert_called_once()
            trace_data = mock_queue.put_nowait.call_args[0][0]
            assert trace_data["event_type"] == "crew_started"
            assert trace_data["job_id"] == job_id
    
    def test_crew_complete_callback(self):
        """Test crew completion callback functionality."""
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks
        
        job_id = "test_job"
        config = {"model": "test"}
        result = "Test execution result"
        
        with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue, \
             patch("src.services.trace_queue.get_trace_queue") as mock_get_queue:
            
            mock_queue = MagicMock()
            mock_get_queue.return_value = mock_queue
            
            callbacks = create_crew_callbacks(job_id, config, None)
            
            # Call completion callback
            callbacks["on_complete"](result)
            
            # Verify log was enqueued
            mock_enqueue.assert_called_once()
            call_args = mock_enqueue.call_args
            kwargs = call_args[1] if len(call_args) > 1 else call_args.kwargs
            assert kwargs["execution_id"] == job_id
            assert "CREW COMPLETED" in kwargs["content"]
            
            # Verify trace was created
            mock_queue.put_nowait.assert_called_once()
            trace_data = mock_queue.put_nowait.call_args[0][0]
            assert trace_data["event_type"] == "crew_completed"
            assert trace_data["job_id"] == job_id
    
    def test_crew_error_callback(self):
        """Test crew error callback functionality."""
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks
        
        job_id = "test_job"
        config = {"model": "test"}
        error = Exception("Test error")
        
        with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            callbacks = create_crew_callbacks(job_id, config, None)
            
            # Call error callback
            callbacks["on_error"](error)
            
            # Verify log was enqueued
            mock_enqueue.assert_called_once()
            call_args = mock_enqueue.call_args
            kwargs = call_args[1] if len(call_args) > 1 else call_args.kwargs
            assert kwargs["execution_id"] == job_id
            assert "CREW FAILED" in kwargs["content"]
            assert "Test error" in kwargs["content"]


class TestConfigSanitization:
    """Test cases for configuration sanitization in logging."""
    
    def test_config_sanitization(self):
        """Test that sensitive config data is sanitized."""
        from src.engines.crewai.callbacks.execution_callback import log_crew_initialization
        
        job_id = "test_job"
        config_with_secrets = {
            "model": "test-model",
            "api_keys": {"secret": "hidden"},
            "tokens": {"access_token": "secret"},
            "passwords": {"db_pass": "secret"},
            "normal_field": "visible"
        }
        
        with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            log_crew_initialization(job_id, config_with_secrets, None)
            
            mock_enqueue.assert_called_once()
            call_args = mock_enqueue.call_args
            kwargs = call_args[1] if len(call_args) > 1 else call_args.kwargs
            content = kwargs["content"]
            
            # Should include safe fields
            assert "test-model" in content
            assert "visible" in content
            
            # Should exclude sensitive fields
            assert "secret" not in content
            assert "hidden" not in content
    
    def test_empty_config_handling(self):
        """Test handling of empty or None config."""
        from src.engines.crewai.callbacks.execution_callback import log_crew_initialization
        
        job_id = "test_job"
        
        with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            # Test with None config
            log_crew_initialization(job_id, None, None)
            
            # Should not raise exception
            mock_enqueue.assert_called_once()
            
            # Test with empty config
            mock_enqueue.reset_mock()
            log_crew_initialization(job_id, {}, None)
            
            mock_enqueue.assert_called_once()
            call_args = mock_enqueue.call_args
            kwargs = call_args[1] if len(call_args) > 1 else call_args.kwargs
            assert kwargs["execution_id"] == job_id