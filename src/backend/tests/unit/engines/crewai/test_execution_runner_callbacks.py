"""
Simplified unit tests for execution runner callback integration.

Tests the core callback integration functionality with minimal mocking.
"""
import pytest
from unittest.mock import patch, MagicMock

from src.engines.crewai.execution_runner import run_crew


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
    async def test_callbacks_created_and_set(self, mock_crew, mock_group_context, running_jobs, sample_config):
        """Test that execution-scoped callbacks are created and set on crew."""
        execution_id = "test_execution_123"

        mock_step_callback = MagicMock()
        mock_task_callback = MagicMock()

        with patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_create_callbacks, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_create_crew_callbacks, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.services.execution_status_service.ExecutionStatusService.update_status"), \
             patch("src.services.crew_executor.crew_executor.run_crew") as mock_crew_executor_run, \
             patch("src.services.api_keys_service.ApiKeysService.setup_openai_api_key"), \
             patch("src.services.api_keys_service.ApiKeysService.setup_anthropic_api_key"), \
             patch("src.services.api_keys_service.ApiKeysService.setup_gemini_api_key"), \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters"), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry"), \
             patch("src.engines.crewai.trace_management.TraceManager.ensure_writer_started"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener"):

            mock_create_callbacks.return_value = (mock_step_callback, mock_task_callback)
            mock_create_crew_callbacks.return_value = {
                'on_start': MagicMock(),
                'on_complete': MagicMock(),
                'on_error': MagicMock()
            }
            mock_crew_executor_run.return_value = "Test result"
            running_jobs[execution_id] = {"config": sample_config}

            await run_crew(
                execution_id=execution_id,
                crew=mock_crew,
                running_jobs=running_jobs,
                group_context=mock_group_context,
                config=sample_config
            )

            mock_create_callbacks.assert_called_once_with(
                job_id=execution_id,
                config=sample_config,
                group_context=mock_group_context,
                crew=mock_crew
            )

            assert hasattr(mock_crew, 'step_callback')
            assert hasattr(mock_crew, 'task_callback')
            assert mock_crew.step_callback == mock_step_callback
            assert mock_crew.task_callback == mock_task_callback

    @pytest.mark.asyncio
    async def test_callback_error_handling(self, mock_crew, mock_group_context, running_jobs, sample_config):
        """Test that callback setup errors are handled gracefully."""
        execution_id = "test_execution_123"

        with patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_create_callbacks, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_create_crew_callbacks, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.services.execution_status_service.ExecutionStatusService.update_status"), \
             patch("src.services.crew_executor.crew_executor.run_crew") as mock_crew_executor_run, \
             patch("src.services.api_keys_service.ApiKeysService.setup_openai_api_key"), \
             patch("src.services.api_keys_service.ApiKeysService.setup_anthropic_api_key"), \
             patch("src.services.api_keys_service.ApiKeysService.setup_gemini_api_key"), \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters"), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry"), \
             patch("src.engines.crewai.trace_management.TraceManager.ensure_writer_started"), \
             patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener"):

            mock_step_callback = MagicMock()
            mock_task_callback = MagicMock()
            mock_create_callbacks.return_value = (mock_step_callback, mock_task_callback)
            mock_create_crew_callbacks.return_value = {
                'on_start': MagicMock(),
                'on_complete': MagicMock(),
                'on_error': MagicMock()
            }
            mock_crew_executor_run.return_value = "Test result"

            type(mock_crew).step_callback = property(lambda self: None,
                                                   lambda self, value: exec('raise Exception("Callback setting failed")'))
            type(mock_crew).task_callback = property(lambda self: None,
                                                   lambda self, value: exec('raise Exception("Callback setting failed")'))

            running_jobs[execution_id] = {"config": sample_config}

            await run_crew(
                execution_id=execution_id,
                crew=mock_crew,
                running_jobs=running_jobs,
                group_context=mock_group_context,
                config=sample_config
            )

            mock_crew_executor_run.assert_called_once()

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
