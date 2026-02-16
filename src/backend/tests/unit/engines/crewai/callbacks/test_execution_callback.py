"""
Unit tests for execution-scoped callback system.

Tests the lightweight execution-scoped callbacks that handle execution log
streaming.  Trace creation is now handled by the event bus handlers
(logging_callbacks.py) and the OTel pipeline.
"""
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from src.engines.crewai.callbacks.execution_callback import (
    create_execution_callbacks,
    create_crew_callbacks,
    log_crew_initialization,
)


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    context = MagicMock()
    context.primary_group_id = "group_123"
    context.group_email = "test@example.com"
    return context


@pytest.fixture
def sample_config():
    """Create a sample configuration."""
    return {
        "model": "test-model",
        "agents": [{"role": "Test Agent"}],
        "tasks": [{"description": "Test task"}],
    }


@pytest.fixture
def mock_crew():
    """Create a mock crew with agents and tasks."""
    crew = MagicMock()
    crew.name = "Test Crew"

    agent = MagicMock()
    agent.role = "Test Agent"
    agent.tools = []

    task = MagicMock()
    task.description = "Test task description"
    task.agent = agent

    crew.agents = [agent]
    crew.tasks = [task]
    return crew


class TestCreateExecutionCallbacks:
    """Test cases for create_execution_callbacks function."""

    def test_create_callbacks_success(self, mock_group_context, sample_config):
        """Test successful creation of execution callbacks."""
        step_callback, task_callback = create_execution_callbacks(
            job_id="test_job_123",
            config=sample_config,
            group_context=mock_group_context,
        )
        assert callable(step_callback)
        assert callable(task_callback)

    def test_step_callback_enqueues_log(self, mock_group_context, sample_config):
        """Test step callback enqueues execution log."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_callback, _ = create_execution_callbacks(
                job_id="test_job_123",
                config=sample_config,
                group_context=mock_group_context,
            )

            mock_step_output = MagicMock()
            mock_step_output.output = "Regular step output"

            step_callback(mock_step_output)

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "test_job_123"
            assert "[STEP]" in kwargs["content"]
            assert kwargs["group_context"] == mock_group_context

    def test_step_callback_handles_raw_attribute(self, mock_group_context, sample_config):
        """Test step callback reads 'raw' attribute when 'output' is missing."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_callback, _ = create_execution_callbacks(
                job_id="j1", config=sample_config, group_context=mock_group_context
            )

            mock_step_output = MagicMock(spec=[])
            mock_step_output.raw = "raw content"

            step_callback(mock_step_output)

            kwargs = mock_enqueue.call_args[1]
            assert "raw content" in kwargs["content"]

    def test_step_callback_truncates_long_content(
        self, mock_group_context, sample_config
    ):
        """Test step callback truncates content longer than 500 chars."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_callback, _ = create_execution_callbacks(
                job_id="j1", config=sample_config, group_context=mock_group_context
            )

            mock_step_output = MagicMock()
            mock_step_output.output = "x" * 600

            step_callback(mock_step_output)

            kwargs = mock_enqueue.call_args[1]
            assert kwargs["content"].endswith("...")
            # 500 chars + "..." + "[STEP] " prefix
            assert len(kwargs["content"]) < 600

    def test_task_callback_enqueues_log(self, mock_group_context, sample_config):
        """Test task callback enqueues execution log with task info."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            _, task_callback = create_execution_callbacks(
                job_id="test_job_123",
                config=sample_config,
                group_context=mock_group_context,
            )

            mock_task_output = MagicMock()
            mock_task_output.raw = "Test task result"
            mock_task_output.description = "Test task description"

            task_callback(mock_task_output)

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "test_job_123"
            assert "TASK COMPLETED" in kwargs["content"]
            assert kwargs["group_context"] == mock_group_context

    def test_task_callback_extracts_description_from_task_attr(
        self, mock_group_context, sample_config
    ):
        """Test task callback extracts description from task_output.task.description."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            _, task_callback = create_execution_callbacks(
                job_id="j1", config=sample_config, group_context=mock_group_context
            )

            mock_task_output = MagicMock(spec=[])
            mock_task_output.task = MagicMock()
            mock_task_output.task.description = "Nested task desc"
            mock_task_output.output = "result"

            task_callback(mock_task_output)

            kwargs = mock_enqueue.call_args[1]
            assert "Nested task desc" in kwargs["content"]

    def test_callbacks_without_group_context(self, sample_config):
        """Test callbacks work without group context."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_callback, _ = create_execution_callbacks(
                job_id="j1", config=sample_config, group_context=None
            )

            mock_step_output = MagicMock()
            mock_step_output.output = "Test output"
            step_callback(mock_step_output)

            mock_enqueue.assert_called_once()

    def test_callback_error_handling(self, mock_group_context, sample_config):
        """Test callbacks handle errors gracefully."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            mock_enqueue.side_effect = Exception("Queue error")

            step_callback, _ = create_execution_callbacks(
                job_id="j1",
                config=sample_config,
                group_context=mock_group_context,
            )

            # Should not raise exception even if enqueue fails
            mock_step_output = MagicMock()
            mock_step_output.output = "Test output"
            step_callback(mock_step_output)  # Should not raise


class TestCreateCrewCallbacks:
    """Test cases for create_crew_callbacks function."""

    def test_create_crew_callbacks_success(self, mock_group_context, sample_config):
        """Test successful creation of crew callbacks."""
        callbacks = create_crew_callbacks(
            job_id="j1", config=sample_config, group_context=mock_group_context
        )

        assert "on_start" in callbacks
        assert "on_complete" in callbacks
        assert "on_error" in callbacks
        assert callable(callbacks["on_start"])
        assert callable(callbacks["on_complete"])
        assert callable(callbacks["on_error"])

    def test_on_start_callback(self, mock_group_context, sample_config):
        """Test crew start callback creates execution log."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            callbacks = create_crew_callbacks(
                job_id="j1", config=sample_config, group_context=mock_group_context
            )

            callbacks["on_start"]()

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "j1"
            assert "CREW STARTED" in kwargs["content"]

    def test_on_complete_callback(self, mock_group_context, sample_config):
        """Test crew completion callback creates execution log."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            callbacks = create_crew_callbacks(
                job_id="j1", config=sample_config, group_context=mock_group_context
            )

            callbacks["on_complete"]("Test result")

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert "CREW COMPLETED" in kwargs["content"]

    def test_on_error_callback(self, mock_group_context, sample_config):
        """Test crew error callback."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            callbacks = create_crew_callbacks(
                job_id="j1", config=sample_config, group_context=mock_group_context
            )

            callbacks["on_error"](Exception("Test error"))

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert "CREW FAILED" in kwargs["content"]
            assert "Test error" in kwargs["content"]


class TestLogCrewInitialization:
    """Test cases for log_crew_initialization function."""

    def test_log_initialization_success(self, mock_group_context, sample_config):
        """Test successful crew initialization logging."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            log_crew_initialization(
                job_id="j1", config=sample_config, group_context=mock_group_context
            )

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "j1"
            assert "CREW INITIALIZED" in kwargs["content"]

    def test_log_initialization_sanitizes_config(self, mock_group_context):
        """Test that sensitive config data is sanitized."""
        config_with_secrets = {
            "model": "test-model",
            "api_keys": {"secret": "hidden"},
            "tokens": {"access_token": "secret"},
            "passwords": {"db_pass": "secret"},
            "normal_field": "visible",
        }

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            log_crew_initialization(
                job_id="j1",
                config=config_with_secrets,
                group_context=mock_group_context,
            )

            content = mock_enqueue.call_args[1]["content"]
            assert "test-model" in content
            assert "visible" in content
            assert "secret" not in content
            assert "hidden" not in content

    def test_log_initialization_error_handling(self, mock_group_context):
        """Test error handling in crew initialization logging."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            mock_enqueue.side_effect = Exception("Logging error")

            # Should not raise
            log_crew_initialization(
                job_id="j1", config={}, group_context=mock_group_context
            )


class TestCallbackIsolation:
    """Test cases to verify callback isolation between executions."""

    def test_multiple_executions_isolated(self, mock_group_context, sample_config):
        """Test that callbacks from different executions are isolated."""
        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_callback_1, _ = create_execution_callbacks(
                "job_1", sample_config, mock_group_context
            )
            step_callback_2, _ = create_execution_callbacks(
                "job_2", sample_config, mock_group_context
            )

            mock_output_1 = MagicMock()
            mock_output_1.output = "Output from job 1"

            mock_output_2 = MagicMock()
            mock_output_2.output = "Output from job 2"

            step_callback_1(mock_output_1)
            step_callback_2(mock_output_2)

            assert mock_enqueue.call_count == 2

            calls = mock_enqueue.call_args_list
            assert calls[0][1]["execution_id"] == "job_1"
            assert calls[1][1]["execution_id"] == "job_2"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
