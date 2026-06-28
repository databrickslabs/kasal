"""
Comprehensive test suite for execution_runner module with 100% coverage.
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
from typing import Dict, Any
import os

from src.engines.crewai.paths.crew.execution_runner import update_execution_status_with_retry
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
class TestUpdateExecutionStatusWithRetry:
    """Test suite for update_execution_status_with_retry function."""
    
    async def test_update_status_success_first_attempt(self):
        """Test successful status update on first attempt."""
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service:
            mock_status_service.update_status = AsyncMock(return_value=True)

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
                side_effect=[Exception("DB error"), Exception("DB error"), True]
            )

            result = await update_execution_status_with_retry(
                "test-id", "FAILED", "Error message"
            )

            assert result is True
            assert mock_status_service.update_status.call_count == 3
            assert mock_sleep.call_count == 2  # Sleep between retries

    async def test_update_status_false_return_is_retried(self):
        """update_status returning False (silent failure) must trigger retries.

        Regression test: the wrapper used to ignore the boolean and report
        success, leaving runs stuck in RUNNING until the engine's safety net
        force-completed them.
        """
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:

            mock_status_service.update_status = AsyncMock(return_value=False)

            result = await update_execution_status_with_retry(
                "test-id", "COMPLETED", "Success message"
            )

            assert result is False
            assert mock_status_service.update_status.call_count == 3  # all retries used
            assert mock_sleep.call_count == 2

    async def test_update_status_false_then_true_succeeds(self):
        """A transient False from update_status recovers on retry."""
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service, \
             patch('asyncio.sleep', new_callable=AsyncMock):

            mock_status_service.update_status = AsyncMock(side_effect=[False, True])

            result = await update_execution_status_with_retry(
                "test-id", "COMPLETED", "Success message"
            )

            assert result is True
            assert mock_status_service.update_status.call_count == 2
    
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
                side_effect=[Exception("Error 1"), Exception("Error 2"), True]
            )

            await update_execution_status_with_retry("test-id", "COMPLETED", "Success")
            
            # Verify exponential backoff: 1s, 2s
            mock_sleep.assert_has_calls([call(1), call(2)])
    
    async def test_update_status_with_none_result(self):
        """Test status update with None result."""
        with patch('src.services.execution_status_service.ExecutionStatusService') as mock_status_service:
            mock_status_service.update_status = AsyncMock(return_value=True)
            
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

class TestSubprocessTracebackSurfacing:
    """Regression: the subprocess ships its full traceback in the FAILED
    result dict, but only the one-line error message was logged — leaving
    failures like \"'Agent' object has no attribute 'i18n'\" without any
    frame information anywhere in the logs."""

    @pytest.mark.asyncio
    async def test_failed_result_traceback_is_logged(self, caplog):
        from src.engines.crewai.paths.crew.execution_runner import run_crew_in_process

        failed_result = {
            "status": "FAILED",
            "execution_id": "exec-tb",
            "error": "'Agent' object has no attribute 'i18n'",
            "traceback": "Traceback (most recent call last):\n  File \"x.py\", line 1\nAttributeError: 'Agent' object has no attribute 'i18n'",
        }

        with patch(
            "src.engines.crewai.paths.crew.execution_runner.process_crew_executor.run_crew_isolated",
            new_callable=AsyncMock,
            return_value=failed_result,
        ), patch(
            "src.engines.crewai.paths.crew.execution_runner.update_execution_status_with_retry",
            new_callable=AsyncMock,
            return_value=True,
        ):
            import logging as _logging
            with caplog.at_level(_logging.ERROR, logger="src.engines.crewai.paths.crew.execution_runner"):
                await run_crew_in_process("exec-tb", {"agents": {}}, {})

        log_text = caplog.text
        assert "Subprocess traceback for exec-tb" in log_text
        assert "AttributeError: 'Agent' object has no attribute 'i18n'" in log_text
