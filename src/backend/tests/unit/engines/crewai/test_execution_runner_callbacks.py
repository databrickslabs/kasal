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
        
        # Mock the callback creation functions
        mock_step_callback = MagicMock()
        mock_task_callback = MagicMock()
        
        # Use a minimal set of patches focusing on the key components
        with patch("src.engines.crewai.callbacks.execution_callback.create_execution_callbacks") as mock_create_callbacks, \
             patch("src.engines.crewai.callbacks.execution_callback.create_crew_callbacks") as mock_create_crew_callbacks, \
             patch("src.engines.crewai.callbacks.execution_callback.log_crew_initialization"), \
             patch("src.services.execution_status_service.ExecutionStatusService.update_status"), \
             patch("asyncio.to_thread") as mock_to_thread, \
             patch("src.services.api_keys_service.ApiKeysService.setup_openai_api_key"), \
             patch("src.services.api_keys_service.ApiKeysService.setup_anthropic_api_key"), \
             patch("src.services.api_keys_service.ApiKeysService.setup_gemini_api_key"), \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters"), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry"):
            
            # Setup callback mocks
            mock_create_callbacks.return_value = (mock_step_callback, mock_task_callback)
            mock_create_crew_callbacks.return_value = {
                'on_start': MagicMock(),
                'on_complete': MagicMock(),
                'on_error': MagicMock()
            }
            mock_to_thread.return_value = "Test result"
            
            # Setup config for running jobs
            running_jobs[execution_id] = {"config": sample_config}
            
            # Run the execution
            await run_crew(
                execution_id=execution_id,
                crew=mock_crew,
                running_jobs=running_jobs,
                group_context=mock_group_context,
                config=sample_config
            )
            
            # Verify callbacks were created with correct parameters
            mock_create_callbacks.assert_called_once_with(
                job_id=execution_id,
                config=sample_config,
                group_context=mock_group_context
            )
            
            # Verify callbacks were set on crew instance
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
             patch("asyncio.to_thread") as mock_to_thread, \
             patch("src.services.api_keys_service.ApiKeysService.setup_openai_api_key"), \
             patch("src.services.api_keys_service.ApiKeysService.setup_anthropic_api_key"), \
             patch("src.services.api_keys_service.ApiKeysService.setup_gemini_api_key"), \
             patch("src.engines.crewai.tools.mcp_handler.stop_all_adapters"), \
             patch("src.engines.crewai.execution_runner.update_execution_status_with_retry"):
            
            # Setup mocks - callbacks creation succeeds but setting on crew fails
            mock_step_callback = MagicMock()
            mock_task_callback = MagicMock()
            mock_create_callbacks.return_value = (mock_step_callback, mock_task_callback)
            mock_create_crew_callbacks.return_value = {
                'on_start': MagicMock(),
                'on_complete': MagicMock(),
                'on_error': MagicMock()
            }
            mock_to_thread.return_value = "Test result"
            
            # Make setting callbacks on crew fail by making them read-only properties
            type(mock_crew).step_callback = property(lambda self: None, 
                                                   lambda self, value: exec('raise Exception("Callback setting failed")'))
            type(mock_crew).task_callback = property(lambda self: None, 
                                                   lambda self, value: exec('raise Exception("Callback setting failed")'))
            
            # Setup config for running jobs
            running_jobs[execution_id] = {"config": sample_config}
            
            # Run the execution - should not raise exception
            await run_crew(
                execution_id=execution_id,
                crew=mock_crew,
                running_jobs=running_jobs,
                group_context=mock_group_context,
                config=sample_config
            )
            
            # Verify execution continued despite callback error
            mock_to_thread.assert_called_once()
    
    def test_callback_isolation_between_instances(self):
        """Test that different callback instances are isolated."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks
        
        job_id_1 = "execution_1"
        job_id_2 = "execution_2"
        config = {"model": "test-model"}
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            
            mock_queue = MagicMock()
            mock_get_queue.return_value = mock_queue
            
            # Create callbacks for two different executions
            step_1, task_1 = create_execution_callbacks(job_id_1, config, None)
            step_2, task_2 = create_execution_callbacks(job_id_2, config, None)
            
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
            assert mock_queue.put_nowait.call_count == 2
            
            # Get the trace data from both calls
            calls = mock_queue.put_nowait.call_args_list
            trace_1 = calls[0][0][0]
            trace_2 = calls[1][0][0]
            
            # Verify traces have different job IDs
            assert trace_1["job_id"] == job_id_1
            assert trace_2["job_id"] == job_id_2
            assert trace_1["job_id"] != trace_2["job_id"]


class TestCallbackFunctionality:
    """Test core callback functionality without complex execution runner mocking."""
    
    def test_step_callback_creates_correct_trace(self):
        """Test that step callback creates correct trace data."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks
        
        job_id = "test_job"
        config = {"model": "test"}
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            
            mock_queue = MagicMock()
            mock_get_queue.return_value = mock_queue
            
            step_callback, _ = create_execution_callbacks(job_id, config, None)
            
            # Create mock step output
            mock_step_output = MagicMock()
            mock_step_output.output = "Test step output"
            mock_step_output.agent = MagicMock()
            mock_step_output.agent.role = "Test Agent"
            
            # Call the step callback
            step_callback(mock_step_output)
            
            # Verify trace was created with correct data
            mock_queue.put_nowait.assert_called_once()
            trace_data = mock_queue.put_nowait.call_args[0][0]
            
            assert trace_data["job_id"] == job_id
            assert trace_data["event_type"] == "agent_execution"
            assert trace_data["event_source"] == "Test Agent"
            assert trace_data["output_content"] == "Test step output"
    
    def test_task_callback_creates_correct_trace(self):
        """Test that task callback creates correct trace data."""
        from src.engines.crewai.callbacks.execution_callback import create_execution_callbacks
        
        job_id = "test_job"
        config = {"model": "test"}
        
        with patch("src.engines.crewai.callbacks.execution_callback.get_trace_queue") as mock_get_queue, \
             patch("src.engines.crewai.callbacks.execution_callback.enqueue_log") as mock_enqueue:
            
            mock_queue = MagicMock()
            mock_get_queue.return_value = mock_queue
            
            _, task_callback = create_execution_callbacks(job_id, config, None)
            
            # Create mock task output
            mock_task_output = MagicMock()
            mock_task_output.raw = "Test task result"
            mock_task_output.description = "Test task description"
            mock_task_output.agent = MagicMock()
            mock_task_output.agent.role = "Test Agent"
            
            # Call the task callback
            task_callback(mock_task_output)
            
            # Verify trace was created with correct data
            mock_queue.put_nowait.assert_called_once()
            trace_data = mock_queue.put_nowait.call_args[0][0]
            
            assert trace_data["job_id"] == job_id
            assert trace_data["event_type"] == "task_completed"
            assert trace_data["event_source"] == "task"
            assert trace_data["output_content"] == "Test task result"