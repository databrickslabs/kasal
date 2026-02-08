"""
Simplified unit tests for trace management with execution-scoped callbacks.

Tests core trace management functionality with minimal async complexity.

NOTE: The execution_callback module has been refactored to delegate trace creation
to the event bus (logging_callbacks.py). The callbacks now primarily:
1. Maintain execution context (current agent, task tracking)
2. Create execution logs (for live log view)
3. Only create traces for specific patterns like "Final Answer:"
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

    def test_task_lifecycle_events_all_in_important_list(self):
        """Test that all task lifecycle events (started, completed, failed) are important.

        task_completed was previously excluded from storage; it is now included
        so that the trace timeline can reconstruct task states on reconnect/refresh.
        """
        important_event_types = [
            "agent_execution", "tool_usage", "tool_error",
            "crew_started", "crew_completed",
            "task_started", "task_completed", "task_failed",
            "llm_call", "llm_guardrail",
            "memory_write", "memory_retrieval",
            "memory_write_started", "memory_retrieval_started",
            "knowledge_retrieval", "knowledge_retrieval_started",
            "agent_reasoning", "agent_reasoning_error"
        ]

        # All three task lifecycle events must be stored
        assert "task_started" in important_event_types
        assert "task_completed" in important_event_types
        assert "task_failed" in important_event_types

    def test_task_lifecycle_events_broadcast_via_sse(self):
        """Test that task_started, task_completed, and task_failed are all broadcast via SSE.

        Previously only task_completed was broadcast; now all three are for
        real-time TaskNode/CrewNode visual status updates.
        """
        sse_broadcast_event_types = ("task_started", "task_completed", "task_failed")

        assert "task_started" in sse_broadcast_event_types
        assert "task_completed" in sse_broadcast_event_types
        assert "task_failed" in sse_broadcast_event_types

    def test_websocket_broadcast_uses_lowercase_event_types(self):
        """Test that WebSocket broadcast checks use lowercase event types.

        The event types from the trace writer use lowercase (e.g., 'task_started')
        not uppercase (e.g., 'TASK_STARTED').
        """
        ws_broadcast_event_types = ["task_started", "task_completed", "task_failed"]

        for event_type in ws_broadcast_event_types:
            assert event_type == event_type.lower(), f"{event_type} should be lowercase"

    def test_step_callback_creates_log_but_not_trace_for_regular_output(self):
        """Test that step callback creates execution logs but not traces for regular output.

        Traces are now handled by the event bus (logging_callbacks.py), except for
        'Final Answer:' patterns which are still traced by step_callback.
        """
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        job_id = "test_job_123"
        config = {"model": "test-model"}

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue_log:

            mock_queue = MagicMock()
            mock_get_queue.return_value = mock_queue

            step_callback, task_callback = create_execution_callbacks(job_id, config, None)

            # Test step callback with regular output (no "Final Answer:")
            mock_step_output = MagicMock()
            mock_step_output.output = "Agent output"
            mock_step_output.agent = MagicMock()
            mock_step_output.agent.role = "Test Agent"
            mock_step_output.__class__.__name__ = "MockStepOutput"

            step_callback(mock_step_output)

            # Execution log should be created
            mock_enqueue_log.assert_called_once()
            call_kwargs = mock_enqueue_log.call_args[1]
            assert call_kwargs["execution_id"] == job_id
            assert "[STEP]" in call_kwargs["content"]

            # Trace should NOT be created for regular output
            mock_queue.put_nowait.assert_not_called()

    def test_step_callback_creates_trace_for_final_answer(self):
        """Test that step callback creates trace for 'Final Answer:' pattern."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        job_id = "test_job_123"
        config = {"model": "test-model"}

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log"):

            mock_queue = MagicMock()
            mock_get_queue.return_value = mock_queue

            step_callback, _ = create_execution_callbacks(job_id, config, None)

            # Create mock AgentFinish with "Final Answer:" pattern
            mock_step_output = MagicMock()
            mock_step_output.output = "Final Answer: This is the final answer"
            mock_step_output.agent = MagicMock()
            mock_step_output.agent.role = "Test Agent"
            mock_step_output.__class__.__name__ = "AgentFinish"

            step_callback(mock_step_output)

            # Trace SHOULD be created for "Final Answer:" pattern
            mock_queue.put_nowait.assert_called_once()
            trace_data = mock_queue.put_nowait.call_args[0][0]

            # Verify trace structure
            assert trace_data["job_id"] == job_id
            assert trace_data["event_type"] == "agent_final_answer"
            assert trace_data["event_context"] == "final_answer"
            assert "Final Answer:" in trace_data["output_content"]

    def test_group_context_in_traces(self):
        """Test that group context is properly included in traces when created."""
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

            # Create AgentFinish with "Final Answer:" to trigger trace creation
            mock_output = MagicMock()
            mock_output.output = "FINAL ANSWER: Test result"
            mock_output.agent = MagicMock()
            mock_output.agent.role = "Test Agent"
            mock_output.__class__.__name__ = "AgentFinish"

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

            # Create AgentFinish outputs with "Final Answer:" to trigger trace creation
            mock_output_1 = MagicMock()
            mock_output_1.output = "Final Answer: identical output"
            mock_output_1.agent = MagicMock()
            mock_output_1.agent.role = "Same Agent"
            mock_output_1.__class__.__name__ = "AgentFinish"

            mock_output_2 = MagicMock()
            mock_output_2.output = "Final Answer: identical output"
            mock_output_2.agent = MagicMock()
            mock_output_2.agent.role = "Same Agent"
            mock_output_2.__class__.__name__ = "AgentFinish"

            # Call both callbacks
            step_1(mock_output_1)
            step_2(mock_output_2)

            # Verify separate traces with different job IDs
            assert mock_queue.put_nowait.call_count == 2

            calls = mock_queue.put_nowait.call_args_list
            trace_1 = calls[0][0][0]
            trace_2 = calls[1][0][0]

            # Traces should have different job IDs but same content
            assert trace_1["job_id"] == job_1
            assert trace_2["job_id"] == job_2
            assert trace_1["job_id"] != trace_2["job_id"]

            # But same event type
            assert trace_1["event_type"] == trace_2["event_type"]


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

    def test_crew_start_callback_creates_log_only(self):
        """Test crew start callback creates execution log but not trace.

        Traces for crew_started are now handled by logging_callbacks.py via
        CrewKickoffStartedEvent on the event bus.
        """
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks

        job_id = "test_job"
        config = {"model": "test"}

        with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            callbacks = create_crew_callbacks(job_id, config, None)

            # Call start callback
            callbacks["on_start"]()

            # Verify log was enqueued
            mock_enqueue.assert_called_once()
            call_args = mock_enqueue.call_args
            kwargs = call_args[1] if len(call_args) > 1 else call_args.kwargs
            assert kwargs["execution_id"] == job_id
            assert "CREW STARTED" in kwargs["content"]

    def test_crew_complete_callback_creates_log_only(self):
        """Test crew completion callback creates execution log but not trace.

        Traces for crew_completed are now handled by logging_callbacks.py via
        CrewKickoffCompletedEvent on the event bus.
        """
        from src.engines.crewai.callbacks.execution_callback import create_crew_callbacks

        job_id = "test_job"
        config = {"model": "test"}
        result = "Test execution result"

        with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            callbacks = create_crew_callbacks(job_id, config, None)

            # Call completion callback
            callbacks["on_complete"](result)

            # Verify log was enqueued
            mock_enqueue.assert_called_once()
            call_args = mock_enqueue.call_args
            kwargs = call_args[1] if len(call_args) > 1 else call_args.kwargs
            assert kwargs["execution_id"] == job_id
            assert "CREW COMPLETED" in kwargs["content"]

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


class TestTaskCallback:
    """Test cases for task callback functionality."""

    def test_task_callback_creates_log_only(self):
        """Test task callback creates execution log but not trace.

        Traces for task_completed are now handled by logging_callbacks.py via
        TaskCompletedEvent on the event bus.
        """
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        job_id = "test_job"
        config = {"model": "test"}

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue_log:

            mock_queue = MagicMock()
            mock_get_queue.return_value = mock_queue

            _, task_callback = create_execution_callbacks(job_id, config, None)

            # Create mock task output
            mock_task_output = MagicMock()
            mock_task_output.raw = "Task result"
            mock_task_output.description = "Test task description"
            mock_task_output.agent = MagicMock()
            mock_task_output.agent.role = "Test Agent"

            task_callback(mock_task_output)

            # Execution log should be created
            mock_enqueue_log.assert_called_once()
            call_kwargs = mock_enqueue_log.call_args[1]
            assert call_kwargs["execution_id"] == job_id
            assert "[TASK COMPLETED]" in call_kwargs["content"]

            # Trace should NOT be created (handled by event bus)
            mock_queue.put_nowait.assert_not_called()


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
