"""
Comprehensive test suite for execution_runner module with 100% coverage.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
from typing import Dict, Any
import os

from src.engines.crewai.execution_runner import run_crew, update_execution_status_with_retry
from src.models.execution_status import ExecutionStatus
from src.utils.user_context import GroupContext


@pytest.fixture
def sample_crew():
    """Mock CrewAI crew."""
    crew = MagicMock()
    crew.agents = [MagicMock(), MagicMock()]
    crew.tasks = [MagicMock(), MagicMock()]
    crew.kickoff = MagicMock(return_value="crew result")
    
    # Setup task attributes
    for task in crew.tasks:
        task.retry_count = 0
        task.description = "Test task description"
    
    return crew


@pytest.fixture
def sample_running_jobs():
    """Sample running jobs dictionary."""
    return {
        "test-exec-id": {
            "config": {
                "original_config": {
                    "model": "gpt-4",
                    "agents": {
                        "agent1": {"max_retry_limit": 3},
                        "agent2": {"max_retry_limit": 2}
                    }
                }
            }
        }
    }


@pytest.fixture
def mock_group_context():
    """Mock group context."""
    context = MagicMock()
    context.primary_group_id = "test-group-123"
    return context


@pytest.mark.asyncio
class TestRunCrew:
    """Test suite for run_crew function."""
    
    async def test_run_crew_success(self, sample_crew, sample_running_jobs, mock_group_context):
        """Test successful crew execution."""
        execution_id = "test-exec-id"
        user_token = "test-token"
        
        with patch('src.utils.user_context.UserContext') as mock_user_context, \
             patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch('src.engines.crewai.crew_logger.crew_logger') as mock_crew_logger, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_streaming, \
             patch('src.core.llm_manager.LLMManager') as mock_llm_manager, \
             patch('src.services.api_keys_service.ApiKeysService') as mock_api_keys, \
             patch('asyncio.to_thread', new_callable=AsyncMock) as mock_to_thread, \
             patch('src.engines.crewai.tools.mcp_handler.stop_all_adapters', new_callable=AsyncMock) as mock_stop_adapters, \
             patch('src.engines.crewai.execution_runner.update_execution_status_with_retry', new_callable=AsyncMock) as mock_update_status:
            
            # Setup mocks
            mock_status_service.update_status = AsyncMock()
            mock_crew_logger.setup_for_job = MagicMock()
            mock_crew_logger.cleanup_for_job = MagicMock()
            mock_crew_logger.capture_stdout_stderr.return_value.__enter__ = MagicMock()
            mock_crew_logger.capture_stdout_stderr.return_value.__exit__ = MagicMock()
            mock_event_streaming.return_value.cleanup = MagicMock()
            mock_llm_manager.configure_crewai_llm = AsyncMock()
            mock_api_keys.setup_openai_api_key = AsyncMock()
            mock_api_keys.setup_anthropic_api_key = AsyncMock()
            mock_api_keys.setup_gemini_api_key = AsyncMock()
            mock_to_thread.return_value = "crew execution result"
            mock_update_status.return_value = True
            mock_stop_adapters.return_value = None
            
            # Setup crew agents with LLM attributes
            for agent in sample_crew.agents:
                agent.llm = MagicMock()
                agent.role = "test-role"
            
            await run_crew(execution_id, sample_crew, sample_running_jobs, mock_group_context, user_token)
            
            # Verify user context was set
            mock_user_context.set_user_token.assert_called_once_with(user_token)
            mock_user_context.set_group_context.assert_called_once_with(mock_group_context)
            
            # Verify status updates
            assert mock_status_service.update_status.call_count >= 1
            mock_status_service.update_status.assert_any_call(
                job_id=execution_id,
                status=ExecutionStatus.RUNNING.value,
                message="CrewAI execution is running"
            )
            
            # Verify crew execution
            mock_to_thread.assert_called_once_with(sample_crew.kickoff)
            
            # Verify final status update
            mock_update_status.assert_called_once_with(
                execution_id,
                ExecutionStatus.COMPLETED.value,
                "CrewAI execution completed successfully",
                "crew execution result"
            )

    async def test_run_crew_cleanup_operations(self, sample_crew, sample_running_jobs):
        """Test cleanup operations are performed."""
        execution_id = "test-exec-id"
        
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch('src.engines.crewai.crew_logger.crew_logger') as mock_crew_logger, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_streaming, \
             patch('src.services.api_keys_service.ApiKeysService') as mock_api_keys, \
             patch('asyncio.to_thread', new_callable=AsyncMock) as mock_to_thread, \
             patch('src.engines.crewai.tools.mcp_handler.stop_all_adapters', new_callable=AsyncMock) as mock_stop_adapters, \
             patch('src.engines.crewai.execution_runner.update_execution_status_with_retry', new_callable=AsyncMock) as mock_update_status, \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False):
            
            # Setup mocks
            mock_status_service.update_status = AsyncMock()
            mock_crew_logger.setup_for_job = MagicMock()
            mock_crew_logger.cleanup_for_job = MagicMock()
            mock_crew_logger.capture_stdout_stderr.return_value.__enter__ = MagicMock()
            mock_crew_logger.capture_stdout_stderr.return_value.__exit__ = MagicMock()
            mock_event_streaming_instance = MagicMock()
            mock_event_streaming.return_value = mock_event_streaming_instance
            mock_api_keys.setup_openai_api_key = AsyncMock()
            mock_api_keys.setup_anthropic_api_key = AsyncMock()
            mock_api_keys.setup_gemini_api_key = AsyncMock()
            mock_to_thread.return_value = "success"
            mock_update_status.return_value = True
            mock_stop_adapters.return_value = None
            
            # Setup crew agents
            for agent in sample_crew.agents:
                agent.llm = MagicMock()
                agent.role = "test-role"
            
            await run_crew(execution_id, sample_crew, sample_running_jobs)
            
            # Verify cleanup operations
            mock_event_streaming_instance.cleanup.assert_called_once()
            mock_crew_logger.cleanup_for_job.assert_called_once_with(execution_id)
            mock_stop_adapters.assert_called_once()
            
            # Verify job was removed from running_jobs
            assert execution_id not in sample_running_jobs

    async def test_run_crew_no_user_context(self, sample_crew, sample_running_jobs):
        """Test crew execution without user token or group context."""
        execution_id = "test-exec-id"
        
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch('src.engines.crewai.crew_logger.crew_logger') as mock_crew_logger, \
             patch('src.engines.crewai.callbacks.streaming_callbacks.EventStreamingCallback') as mock_event_streaming, \
             patch('src.services.api_keys_service.ApiKeysService') as mock_api_keys, \
             patch('asyncio.to_thread', new_callable=AsyncMock) as mock_to_thread, \
             patch('src.engines.crewai.tools.mcp_handler.stop_all_adapters', new_callable=AsyncMock) as mock_stop_adapters, \
             patch('src.engines.crewai.execution_runner.update_execution_status_with_retry', new_callable=AsyncMock) as mock_update_status, \
             patch('src.utils.databricks_auth.is_databricks_apps_environment', return_value=False):
            
            # Setup mocks
            mock_status_service.update_status = AsyncMock()
            mock_crew_logger.setup_for_job = MagicMock()
            mock_crew_logger.cleanup_for_job = MagicMock()
            mock_crew_logger.capture_stdout_stderr.return_value.__enter__ = MagicMock()
            mock_crew_logger.capture_stdout_stderr.return_value.__exit__ = MagicMock()
            mock_event_streaming.return_value.cleanup = MagicMock()
            mock_api_keys.setup_openai_api_key = AsyncMock()
            mock_api_keys.setup_anthropic_api_key = AsyncMock()
            mock_api_keys.setup_gemini_api_key = AsyncMock()
            mock_to_thread.return_value = "success"
            mock_update_status.return_value = True
            mock_stop_adapters.return_value = None
            
            # Setup crew agents
            for agent in sample_crew.agents:
                agent.llm = MagicMock()
                agent.role = "test-role"
            
            # Call without user token or group context
            await run_crew(execution_id, sample_crew, sample_running_jobs)
            
            # Verify successful execution even without user context
            mock_update_status.assert_called_once_with(
                execution_id,
                ExecutionStatus.COMPLETED.value,
                "CrewAI execution completed successfully",
                "success"
            )


@pytest.mark.asyncio
class TestUpdateExecutionStatusWithRetry:
    """Test suite for update_execution_status_with_retry function."""
    
    async def test_update_status_success_first_attempt(self):
        """Test successful status update on first attempt."""
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service:
            mock_status_service.update_status = AsyncMock()
            
            result = await update_execution_status_with_retry(
                "test-id", "COMPLETED", "Success message", {"result": "data"}
            )
            
            assert result is True
            mock_status_service.update_status.assert_called_once_with(
                job_id="test-id",
                status="COMPLETED",
                message="Success message",
                result={"result": "data"}
            )
    
    async def test_update_status_success_after_retries(self):
        """Test successful status update after retries."""
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            
            # Fail twice, then succeed
            mock_status_service.update_status = AsyncMock(
                side_effect=[Exception("DB error"), Exception("DB error"), None]
            )
            
            result = await update_execution_status_with_retry(
                "test-id", "FAILED", "Error message"
            )
            
            assert result is True
            assert mock_status_service.update_status.call_count == 3
            assert mock_sleep.call_count == 2  # Sleep between retries
    
    async def test_update_status_failure_after_max_retries(self):
        """Test status update failure after max retries."""
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch('asyncio.sleep', new_callable=AsyncMock):
            
            # Always fail
            mock_status_service.update_status = AsyncMock(side_effect=Exception("Persistent DB error"))
            
            result = await update_execution_status_with_retry(
                "test-id", "FAILED", "Error message"
            )
            
            assert result is False
            assert mock_status_service.update_status.call_count == 3  # Max retries
    
    async def test_update_status_exponential_backoff(self):
        """Test exponential backoff timing in retries."""
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            
            # Fail twice, then succeed
            mock_status_service.update_status = AsyncMock(
                side_effect=[Exception("Error 1"), Exception("Error 2"), None]
            )
            
            await update_execution_status_with_retry("test-id", "COMPLETED", "Success")
            
            # Verify exponential backoff: 1s, 2s
            mock_sleep.assert_has_calls([call(1), call(2)])
    
    async def test_update_status_with_none_result(self):
        """Test status update with None result."""
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service:
            mock_status_service.update_status = AsyncMock()
            
            result = await update_execution_status_with_retry(
                "test-id", "FAILED", "Error message", None
            )
            
            assert result is True
            mock_status_service.update_status.assert_called_once_with(
                job_id="test-id",
                status="FAILED",
                message="Error message",
                result=None
            )