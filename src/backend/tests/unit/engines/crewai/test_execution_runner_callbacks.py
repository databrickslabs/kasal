"""
Simplified unit tests for execution runner callback integration.

Tests the core callback integration functionality with minimal mocking.
"""
import pytest
from unittest.mock import patch, MagicMock



@pytest.fixture
def mock_crew():
    """Create a mock CrewAI crew."""
    crew = MagicMock()
    crew.agents = []
    crew.tasks = []
    crew.kickoff = MagicMock(return_value="Test result")
    return crew


@pytest.fixture
def mock_group_context():
    """Create a mock group context."""
    context = MagicMock()
    context.primary_group_id = "group_123"
    context.group_email = "test@example.com"
    context.access_token = "token_123"
    return context


@pytest.fixture
def running_jobs():
    """Create a mock running jobs dictionary."""
    return {}


@pytest.fixture
def sample_config():
    """Create a sample configuration."""
    return {
        "model": "test-model",
        "agents": {"agent_1": {"role": "Test Agent", "max_retry_limit": 2}},
        "tasks": {"task_1": {"description": "Test task"}},
        "inputs": {"test_input": "test_value"}
    }


class TestExecutionRunnerCallbackIntegration:
    """Test cases for execution runner callback integration."""

    @pytest.mark.asyncio
    def test_callback_isolation_between_instances(self):
        """Test that different callback instances are isolated."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_1, task_1 = create_execution_callbacks("execution_1", {"model": "test"}, None)
            step_2, task_2 = create_execution_callbacks("execution_2", {"model": "test"}, None)

            assert step_1 is not step_2
            assert task_1 is not task_2

            mock_output_1 = MagicMock()
            mock_output_1.output = "Test output from job 1"
            mock_output_2 = MagicMock()
            mock_output_2.output = "Test output from job 2"

            step_1(mock_output_1)
            step_2(mock_output_2)

            assert mock_enqueue.call_count == 2
            calls = mock_enqueue.call_args_list
            assert calls[0][1]["execution_id"] == "execution_1"
            assert calls[1][1]["execution_id"] == "execution_2"


class TestCallbackFunctionality:
    """Test core callback functionality without complex execution runner mocking."""

    def test_step_callback_creates_execution_log(self):
        """Test that step callback creates execution log."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            step_callback, _ = create_execution_callbacks("test_job", {"model": "test"}, None)

            mock_step_output = MagicMock()
            mock_step_output.output = "This is the agent output"
            step_callback(mock_step_output)

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "test_job"
            assert "[STEP]" in kwargs["content"]

    def test_step_callback_skips_log_on_error(self):
        """Test that step callback handles enqueue errors gracefully."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            mock_enqueue.side_effect = Exception("Enqueue error")
            step_callback, _ = create_execution_callbacks("test_job", {"model": "test"}, None)

            mock_step_output = MagicMock()
            mock_step_output.output = "Regular step output"
            # Should not raise
            step_callback(mock_step_output)

    def test_task_callback_creates_execution_log(self):
        """Test that task callback creates execution log."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks

        with patch(
            "src.engines.crewai.callbacks.execution_callback.enqueue_log"
        ) as mock_enqueue:
            _, task_callback = create_execution_callbacks("test_job", {"model": "test"}, None)

            mock_task_output = MagicMock()
            mock_task_output.raw = "Test task result"
            mock_task_output.description = "Test task description"
            task_callback(mock_task_output)

            mock_enqueue.assert_called_once()
            kwargs = mock_enqueue.call_args[1]
            assert kwargs["execution_id"] == "test_job"
            assert "TASK COMPLETED" in kwargs["content"]
