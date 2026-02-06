"""
Unit tests for execution-scoped callback system.

Tests the functionality of the execution-scoped callbacks that replace
global event listeners to prevent cross-contamination between concurrent executions.

Note: Most trace creation has been moved to logging_callbacks.py via the event bus.
The step_callback now only creates traces for "Final Answer:" patterns.
The task_callback and crew callbacks only create execution logs, not traces.
"""
import pytest
import asyncio
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timezone
from queue import Queue

from src.engines.crewai.callbacks.execution_callback import (
    create_execution_callbacks,
    create_crew_callbacks,
    log_crew_initialization
)


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    context = MagicMock()
    context.primary_group_id = "group_123"
    context.group_email = "test@example.com"
    return context


@pytest.fixture
def mock_trace_queue():
    """Create a mock trace queue."""
    queue = MagicMock()
    queue.put_nowait = MagicMock()
    return queue


@pytest.fixture
def sample_config():
    """Create a sample configuration."""
    return {
        "model": "test-model",
        "agents": [{"role": "Test Agent"}],
        "tasks": [{"description": "Test task"}]
    }


@pytest.fixture
def mock_crew():
    """Create a mock crew with agents and tasks."""
    crew = MagicMock()
    crew.name = "Test Crew"

    # Create mock agent
    agent = MagicMock()
    agent.role = "Test Agent"
    agent.tools = []

    # Create mock task
    task = MagicMock()
    task.description = "Test task description"
    task.agent = agent
    task._kasal_task_id = "task_123"

    crew.agents = [agent]
    crew.tasks = [task]

    return crew


class TestCreateExecutionCallbacks:
    """Test cases for create_execution_callbacks function."""

    def test_create_callbacks_success(self, mock_group_context, mock_trace_queue, sample_config):
        """Test successful creation of execution callbacks."""
        job_id = "test_job_123"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            mock_get_queue.return_value = mock_trace_queue

            step_callback, task_callback = create_execution_callbacks(
                job_id=job_id,
                config=sample_config,
                group_context=mock_group_context
            )

            assert callable(step_callback)
            assert callable(task_callback)

    def test_step_callback_with_final_answer_creates_trace(self, mock_group_context, mock_trace_queue, sample_config, mock_crew):
        """Test step callback creates trace when output contains 'Final Answer:'."""
        job_id = "test_job_123"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:

            mock_get_queue.return_value = mock_trace_queue

            step_callback, _ = create_execution_callbacks(
                job_id=job_id,
                config=sample_config,
                group_context=mock_group_context,
                crew=mock_crew
            )

            # Create mock AgentFinish output with "Final Answer:" pattern
            mock_step_output = MagicMock()
            mock_step_output.__class__.__name__ = "AgentFinish"
            mock_step_output.output = "Final Answer: This is the final result."

            # Call the step callback
            step_callback(mock_step_output)

            # Verify enqueue_log was called (execution logs)
            mock_enqueue.assert_called_once()
            call_args = mock_enqueue.call_args
            kwargs = call_args[1] if len(call_args) > 1 else call_args.kwargs
            assert kwargs["execution_id"] == job_id
            assert kwargs["group_context"] == mock_group_context

            # Verify trace queue was called for Final Answer
            mock_trace_queue.put_nowait.assert_called_once()
            trace_data = mock_trace_queue.put_nowait.call_args[0][0]
            assert trace_data["job_id"] == job_id
            assert trace_data["event_type"] == "agent_final_answer"
            assert trace_data["group_id"] == mock_group_context.primary_group_id

    def test_step_callback_without_final_answer_no_trace(self, mock_group_context, mock_trace_queue, sample_config):
        """Test step callback does NOT create trace for regular step output."""
        job_id = "test_job_123"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:

            mock_get_queue.return_value = mock_trace_queue

            step_callback, _ = create_execution_callbacks(
                job_id=job_id,
                config=sample_config,
                group_context=mock_group_context
            )

            # Create mock step output WITHOUT "Final Answer:" pattern
            mock_step_output = MagicMock()
            mock_step_output.output = "Regular step output"
            mock_step_output.agent = MagicMock()
            mock_step_output.agent.role = "Test Agent"

            # Call the step callback
            step_callback(mock_step_output)

            # Verify enqueue_log was called (execution logs are always created)
            mock_enqueue.assert_called_once()

            # Verify trace queue was NOT called (traces handled by event bus now)
            mock_trace_queue.put_nowait.assert_not_called()

    def test_task_callback_creates_execution_log_only(self, mock_group_context, mock_trace_queue, sample_config):
        """Test task callback creates execution log but not trace (trace handled by event bus)."""
        job_id = "test_job_123"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:

            mock_get_queue.return_value = mock_trace_queue

            _, task_callback = create_execution_callbacks(
                job_id=job_id,
                config=sample_config,
                group_context=mock_group_context
            )

            # Create mock task output
            mock_task_output = MagicMock()
            mock_task_output.raw = "Test task result"
            mock_task_output.description = "Test task description"
            mock_task_output.agent = MagicMock()
            mock_task_output.agent.role = "Test Agent"

            # Call the task callback
            task_callback(mock_task_output)

            # Verify enqueue_log was called (execution logs)
            mock_enqueue.assert_called_once()
            call_args = mock_enqueue.call_args
            kwargs = call_args[1] if len(call_args) > 1 else call_args.kwargs
            assert kwargs["execution_id"] == job_id
            assert "TASK COMPLETED" in kwargs["content"]
            assert kwargs["group_context"] == mock_group_context

            # Verify trace queue was NOT called (traces now handled by event bus)
            mock_trace_queue.put_nowait.assert_not_called()

    def test_callbacks_without_group_context(self, mock_trace_queue, sample_config):
        """Test callbacks work without group context."""
        job_id = "test_job_123"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:

            mock_get_queue.return_value = mock_trace_queue

            step_callback, task_callback = create_execution_callbacks(
                job_id=job_id,
                config=sample_config,
                group_context=None
            )

            # Test step callback without group context
            mock_step_output = MagicMock()
            mock_step_output.output = "Test output"
            step_callback(mock_step_output)

            # Verify it works without group context
            mock_enqueue.assert_called()

    def test_step_callback_with_final_answer_no_group_context(self, mock_trace_queue, sample_config, mock_crew):
        """Test step callback with Final Answer creates trace without group_id when no group context."""
        job_id = "test_job_123"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:

            mock_get_queue.return_value = mock_trace_queue

            step_callback, _ = create_execution_callbacks(
                job_id=job_id,
                config=sample_config,
                group_context=None,
                crew=mock_crew
            )

            # Create mock AgentFinish output with "Final Answer:" pattern
            mock_step_output = MagicMock()
            mock_step_output.__class__.__name__ = "AgentFinish"
            mock_step_output.output = "Final Answer: Result without group context"

            step_callback(mock_step_output)

            # Verify trace was created without group_id
            mock_trace_queue.put_nowait.assert_called_once()
            trace_data = mock_trace_queue.put_nowait.call_args[0][0]
            assert "group_id" not in trace_data

    def test_callback_error_handling(self, mock_group_context, mock_trace_queue, sample_config):
        """Test callbacks handle errors gracefully."""
        job_id = "test_job_123"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:

            mock_get_queue.return_value = mock_trace_queue
            mock_enqueue.side_effect = Exception("Queue error")

            step_callback, _ = create_execution_callbacks(
                job_id=job_id,
                config=sample_config,
                group_context=mock_group_context
            )

            # Call should not raise exception even if enqueue fails
            mock_step_output = MagicMock()
            mock_step_output.output = "Test output"
            step_callback(mock_step_output)  # Should not raise

    def test_step_callback_agent_action_no_trace(self, mock_group_context, mock_trace_queue, sample_config, mock_crew):
        """Test step callback does NOT create trace for AgentAction (handled by event bus)."""
        job_id = "test_job_123"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:

            mock_get_queue.return_value = mock_trace_queue

            step_callback, _ = create_execution_callbacks(
                job_id=job_id,
                config=sample_config,
                group_context=mock_group_context,
                crew=mock_crew
            )

            # Create mock AgentAction output
            mock_step_output = MagicMock()
            mock_step_output.__class__.__name__ = "AgentAction"
            mock_step_output.tool = "test_tool"
            mock_step_output.tool_input = "test input"

            step_callback(mock_step_output)

            # Verify execution log was created
            mock_enqueue.assert_called_once()

            # Verify NO trace was created (tool usage traces handled by event bus)
            mock_trace_queue.put_nowait.assert_not_called()

    def test_step_callback_agent_finish_without_final_answer_no_trace(self, mock_group_context, mock_trace_queue, sample_config, mock_crew):
        """Test step callback does NOT create trace for AgentFinish without 'Final Answer:'."""
        job_id = "test_job_123"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:

            mock_get_queue.return_value = mock_trace_queue

            step_callback, _ = create_execution_callbacks(
                job_id=job_id,
                config=sample_config,
                group_context=mock_group_context,
                crew=mock_crew
            )

            # Create mock AgentFinish output WITHOUT "Final Answer:" pattern
            mock_step_output = MagicMock()
            mock_step_output.__class__.__name__ = "AgentFinish"
            mock_step_output.output = "Just a regular completion message"

            step_callback(mock_step_output)

            # Verify execution log was created
            mock_enqueue.assert_called_once()

            # Verify NO trace was created
            mock_trace_queue.put_nowait.assert_not_called()


class TestCreateCrewCallbacks:
    """Test cases for create_crew_callbacks function."""

    def test_create_crew_callbacks_success(self, mock_group_context, sample_config):
        """Test successful creation of crew callbacks."""
        job_id = "test_job_123"

        callbacks = create_crew_callbacks(
            job_id=job_id,
            config=sample_config,
            group_context=mock_group_context
        )

        assert "on_start" in callbacks
        assert "on_complete" in callbacks
        assert "on_error" in callbacks
        assert callable(callbacks["on_start"])
        assert callable(callbacks["on_complete"])
        assert callable(callbacks["on_error"])

    def test_on_start_callback_creates_log_only(self, mock_group_context, sample_config):
        """Test crew start callback creates execution log but not trace (trace handled by event bus)."""
        job_id = "test_job_123"

        with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            callbacks = create_crew_callbacks(
                job_id=job_id,
                config=sample_config,
                group_context=mock_group_context
            )

            callbacks["on_start"]()

            # Verify enqueue_log was called
            mock_enqueue.assert_called_once()
            call_args = mock_enqueue.call_args
            kwargs = call_args[1] if len(call_args) > 1 else call_args.kwargs
            assert kwargs["execution_id"] == job_id
            assert "CREW STARTED" in kwargs["content"]

    def test_on_complete_callback_creates_log_only(self, mock_group_context, sample_config):
        """Test crew completion callback creates execution log but not trace (trace handled by event bus)."""
        job_id = "test_job_123"
        result = "Test execution result"

        with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            callbacks = create_crew_callbacks(
                job_id=job_id,
                config=sample_config,
                group_context=mock_group_context
            )

            callbacks["on_complete"](result)

            # Verify enqueue_log was called
            mock_enqueue.assert_called_once()
            call_args = mock_enqueue.call_args
            kwargs = call_args[1] if len(call_args) > 1 else call_args.kwargs
            assert kwargs["execution_id"] == job_id
            assert "CREW COMPLETED" in kwargs["content"]

    def test_on_error_callback(self, mock_group_context, sample_config):
        """Test crew error callback."""
        job_id = "test_job_123"
        error = Exception("Test error")

        with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            callbacks = create_crew_callbacks(
                job_id=job_id,
                config=sample_config,
                group_context=mock_group_context
            )

            callbacks["on_error"](error)

            # Verify enqueue_log was called
            mock_enqueue.assert_called_once()
            call_args = mock_enqueue.call_args
            kwargs = call_args[1] if len(call_args) > 1 else call_args.kwargs
            assert kwargs["execution_id"] == job_id
            assert "CREW FAILED" in kwargs["content"]
            assert "Test error" in kwargs["content"]


class TestLogCrewInitialization:
    """Test cases for log_crew_initialization function."""

    def test_log_initialization_success(self, mock_group_context, sample_config):
        """Test successful crew initialization logging."""
        job_id = "test_job_123"

        with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            log_crew_initialization(
                job_id=job_id,
                config=sample_config,
                group_context=mock_group_context
            )

            mock_enqueue.assert_called_once()
            call_args = mock_enqueue.call_args
            kwargs = call_args[1] if len(call_args) > 1 else call_args.kwargs
            assert kwargs["execution_id"] == job_id
            assert "CREW INITIALIZED" in kwargs["content"]
            assert kwargs["group_context"] == mock_group_context

    def test_log_initialization_sanitizes_config(self, mock_group_context):
        """Test that sensitive config data is sanitized."""
        job_id = "test_job_123"
        config_with_secrets = {
            "model": "test-model",
            "api_keys": {"secret": "hidden"},
            "tokens": {"access_token": "secret"},
            "passwords": {"db_pass": "secret"},
            "normal_field": "visible"
        }

        with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            log_crew_initialization(
                job_id=job_id,
                config=config_with_secrets,
                group_context=mock_group_context
            )

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

    def test_log_initialization_error_handling(self, mock_group_context):
        """Test error handling in crew initialization logging."""
        job_id = "test_job_123"

        with patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            mock_enqueue.side_effect = Exception("Logging error")

            # Should not raise exception even if logging fails
            log_crew_initialization(
                job_id=job_id,
                config={},
                group_context=mock_group_context
            )


class TestCallbackIsolation:
    """Test cases to verify callback isolation between executions."""

    def test_multiple_executions_isolated(self, mock_group_context, mock_trace_queue, sample_config):
        """Test that callbacks from different executions are isolated."""
        job_id_1 = "job_1"
        job_id_2 = "job_2"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:

            mock_get_queue.return_value = mock_trace_queue

            # Create callbacks for two different executions
            step_callback_1, _ = create_execution_callbacks(job_id_1, sample_config, mock_group_context)
            step_callback_2, _ = create_execution_callbacks(job_id_2, sample_config, mock_group_context)

            # Create mock outputs
            mock_output_1 = MagicMock()
            mock_output_1.output = "Output from job 1"

            mock_output_2 = MagicMock()
            mock_output_2.output = "Output from job 2"

            # Call callbacks
            step_callback_1(mock_output_1)
            step_callback_2(mock_output_2)

            # Verify both callbacks were called with correct job IDs
            assert mock_enqueue.call_count == 2

            # Get all calls and verify job isolation
            calls = mock_enqueue.call_args_list
            call_1_job_id = calls[0][1]["execution_id"]
            call_2_job_id = calls[1][1]["execution_id"]

            assert call_1_job_id == job_id_1
            assert call_2_job_id == job_id_2
            assert call_1_job_id != call_2_job_id

    def test_trace_data_isolation_with_final_answer(self, mock_group_context, mock_trace_queue, sample_config, mock_crew):
        """Test that trace data is properly isolated by job ID when Final Answer triggers trace."""
        job_id_1 = "job_1"
        job_id_2 = "job_2"

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log"):

            mock_get_queue.return_value = mock_trace_queue

            # Create callbacks for different executions
            step_callback_1, _ = create_execution_callbacks(job_id_1, sample_config, mock_group_context, mock_crew)
            step_callback_2, _ = create_execution_callbacks(job_id_2, sample_config, mock_group_context, mock_crew)

            # Create mock outputs with "Final Answer:" to trigger trace creation
            mock_output_1 = MagicMock()
            mock_output_1.__class__.__name__ = "AgentFinish"
            mock_output_1.output = "Final Answer: Result from job 1"

            mock_output_2 = MagicMock()
            mock_output_2.__class__.__name__ = "AgentFinish"
            mock_output_2.output = "Final Answer: Result from job 2"

            # Call callbacks
            step_callback_1(mock_output_1)
            step_callback_2(mock_output_2)

            # Verify trace queue calls
            assert mock_trace_queue.put_nowait.call_count == 2

            # Get trace data from both calls
            calls = mock_trace_queue.put_nowait.call_args_list
            trace_1 = calls[0][0][0]
            trace_2 = calls[1][0][0]

            # Verify job IDs are different and correct
            assert trace_1["job_id"] == job_id_1
            assert trace_2["job_id"] == job_id_2
            assert trace_1["job_id"] != trace_2["job_id"]


class TestAgentContextTracking:
    """Test cases for agent context tracking in callbacks."""

    def test_agent_lookup_from_crew(self, mock_group_context, mock_trace_queue):
        """Test that agent lookup is built from crew."""
        job_id = "test_job_123"

        # Create a crew with multiple agents
        mock_crew = MagicMock()
        mock_crew.name = "Multi-Agent Crew"

        agent_1 = MagicMock()
        agent_1.role = "Researcher"
        agent_1.tools = []

        agent_2 = MagicMock()
        agent_2.role = "Writer"
        agent_2.tools = []

        mock_crew.agents = [agent_1, agent_2]
        mock_crew.tasks = []

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            mock_get_queue.return_value = mock_trace_queue

            step_callback, task_callback = create_execution_callbacks(
                job_id=job_id,
                config={},
                group_context=mock_group_context,
                crew=mock_crew
            )

            # Verify callbacks were created successfully
            assert callable(step_callback)
            assert callable(task_callback)

    def test_task_to_agent_mapping(self, mock_group_context, mock_trace_queue):
        """Test that task-to-agent mapping is built from crew."""
        job_id = "test_job_123"

        mock_crew = MagicMock()
        mock_crew.name = "Test Crew"

        agent = MagicMock()
        agent.role = "Test Agent"
        agent.tools = []

        task = MagicMock()
        task.description = "Test task"
        task.agent = agent

        mock_crew.agents = [agent]
        mock_crew.tasks = [task]

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            mock_get_queue.return_value = mock_trace_queue

            step_callback, task_callback = create_execution_callbacks(
                job_id=job_id,
                config={},
                group_context=mock_group_context,
                crew=mock_crew
            )

            # Verify callbacks were created successfully
            assert callable(step_callback)
            assert callable(task_callback)

    def test_tool_to_agent_mapping(self, mock_group_context, mock_trace_queue):
        """Test that tool-to-agent mapping is built from crew agents with tools."""
        job_id = "test_job_123"

        mock_crew = MagicMock()
        mock_crew.name = "Tool Crew"

        # Create agent with tools
        agent = MagicMock()
        agent.role = "Tool User"

        tool_1 = MagicMock()
        tool_1.name = "search_tool"

        tool_2 = MagicMock()
        tool_2.name = "calculate_tool"

        agent.tools = [tool_1, tool_2]

        mock_crew.agents = [agent]
        mock_crew.tasks = []

        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue:
            mock_get_queue.return_value = mock_trace_queue

            step_callback, _ = create_execution_callbacks(
                job_id=job_id,
                config={},
                group_context=mock_group_context,
                crew=mock_crew
            )

            # Verify callback was created successfully
            assert callable(step_callback)
