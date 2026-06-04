"""
Unit tests for ExecutionCleanupService.cleanup_zombie_jobs
"""
import json
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.execution_cleanup_service import ExecutionCleanupService
from src.models.execution_status import ExecutionStatus


def _build_session_ctx(execute_side_effects=None):
    """Build a mock async session context for 'async with async_session_factory() as db:'"""
    session_mock = AsyncMock()
    if execute_side_effects:
        session_mock.execute = AsyncMock(side_effect=execute_side_effects)

    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=session_mock)
    ctx.__aexit__ = AsyncMock(return_value=False)

    factory_mock = MagicMock(return_value=ctx)
    return factory_mock, session_mock


class TestCleanupZombieJobs:
    @pytest.mark.asyncio
    async def test_no_running_jobs_returns_zero(self):
        """When no jobs are RUNNING, returns 0 recovered."""
        result_mock = MagicMock()
        result_mock.fetchall = MagicMock(return_value=[])
        session_mock = AsyncMock()
        session_mock.execute = AsyncMock(return_value=result_mock)

        factory_mock, _ = _build_session_ctx()
        # Override to return session that yields empty running jobs
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=session_mock)
        ctx.__aexit__ = AsyncMock(return_value=False)
        factory_mock = MagicMock(return_value=ctx)

        with patch("src.services.execution_cleanup_service.async_session_factory", factory_mock):
            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 0

    @pytest.mark.asyncio
    async def test_running_job_with_no_completion_trace_left_alone(self):
        """Running jobs without completion trace are not touched."""
        # First DB call: get running jobs
        first_result = MagicMock()
        first_result.fetchall = MagicMock(return_value=[("job-1",)])
        # Second DB call: no completion trace found
        second_result = MagicMock()
        second_result.fetchone = MagicMock(return_value=None)

        call_count = [0]

        async def execute_mock(stmt, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return first_result
            return second_result

        session_mock = AsyncMock()
        session_mock.execute = execute_mock
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=session_mock)
        ctx.__aexit__ = AsyncMock(return_value=False)
        factory_mock = MagicMock(return_value=ctx)

        with patch("src.services.execution_cleanup_service.async_session_factory", factory_mock):
            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 0

    @pytest.mark.asyncio
    async def test_zombie_job_with_dict_output_recovered(self):
        """Running job with crew_completed trace (dict content) is recovered."""
        first_result = MagicMock()
        first_result.fetchall = MagicMock(return_value=[("job-zombie",)])

        trace_output = {"content": "Final answer from crew"}
        second_result = MagicMock()
        second_result.fetchone = MagicMock(return_value=(trace_output,))

        call_count = [0]

        async def execute_mock(stmt, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return first_result
            return second_result

        session_mock = AsyncMock()
        session_mock.execute = execute_mock
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=session_mock)
        ctx.__aexit__ = AsyncMock(return_value=False)
        factory_mock = MagicMock(return_value=ctx)

        update_status_mock = AsyncMock(return_value=True)

        with patch("src.services.execution_cleanup_service.async_session_factory", factory_mock), \
             patch("src.services.execution_cleanup_service.ExecutionStatusService.update_status",
                   update_status_mock):
            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 1
        update_status_mock.assert_awaited_once_with(
            job_id="job-zombie",
            status=ExecutionStatus.COMPLETED.value,
            message="CrewAI execution completed successfully",
            result="Final answer from crew",
        )

    @pytest.mark.asyncio
    async def test_zombie_job_with_json_string_output_recovered(self):
        """Zombie job with JSON string output is parsed and recovered."""
        first_result = MagicMock()
        first_result.fetchall = MagicMock(return_value=[("job-json",)])

        json_output = json.dumps({"content": "JSON content"})
        second_result = MagicMock()
        second_result.fetchone = MagicMock(return_value=(json_output,))

        call_count = [0]

        async def execute_mock(stmt, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return first_result
            return second_result

        session_mock = AsyncMock()
        session_mock.execute = execute_mock
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=session_mock)
        ctx.__aexit__ = AsyncMock(return_value=False)
        factory_mock = MagicMock(return_value=ctx)

        update_status_mock = AsyncMock(return_value=True)

        with patch("src.services.execution_cleanup_service.async_session_factory", factory_mock), \
             patch("src.services.execution_cleanup_service.ExecutionStatusService.update_status",
                   update_status_mock):
            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 1
        call_kwargs = update_status_mock.call_args[1]
        assert call_kwargs["result"] == "JSON content"

    @pytest.mark.asyncio
    async def test_zombie_job_with_non_json_string_output(self):
        """Zombie job with non-JSON string output falls back to str(output)."""
        first_result = MagicMock()
        first_result.fetchall = MagicMock(return_value=[("job-plain",)])

        plain_output = "plain text result"
        second_result = MagicMock()
        second_result.fetchone = MagicMock(return_value=(plain_output,))

        call_count = [0]

        async def execute_mock(stmt, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return first_result
            return second_result

        session_mock = AsyncMock()
        session_mock.execute = execute_mock
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=session_mock)
        ctx.__aexit__ = AsyncMock(return_value=False)
        factory_mock = MagicMock(return_value=ctx)

        update_status_mock = AsyncMock(return_value=True)

        with patch("src.services.execution_cleanup_service.async_session_factory", factory_mock), \
             patch("src.services.execution_cleanup_service.ExecutionStatusService.update_status",
                   update_status_mock):
            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 1
        call_kwargs = update_status_mock.call_args[1]
        assert call_kwargs["result"] == "plain text result"

    @pytest.mark.asyncio
    async def test_zombie_job_with_null_output(self):
        """Zombie job with null output still gets recovered."""
        first_result = MagicMock()
        first_result.fetchall = MagicMock(return_value=[("job-null",)])

        second_result = MagicMock()
        second_result.fetchone = MagicMock(return_value=(None,))

        call_count = [0]

        async def execute_mock(stmt, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return first_result
            return second_result

        session_mock = AsyncMock()
        session_mock.execute = execute_mock
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=session_mock)
        ctx.__aexit__ = AsyncMock(return_value=False)
        factory_mock = MagicMock(return_value=ctx)

        update_status_mock = AsyncMock(return_value=True)

        with patch("src.services.execution_cleanup_service.async_session_factory", factory_mock), \
             patch("src.services.execution_cleanup_service.ExecutionStatusService.update_status",
                   update_status_mock):
            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 1
        call_kwargs = update_status_mock.call_args[1]
        assert call_kwargs["result"] is None

    @pytest.mark.asyncio
    async def test_multiple_zombie_jobs_all_recovered(self):
        """Multiple zombie jobs are all recovered."""
        running_jobs = [("job-1",), ("job-2",)]
        first_result = MagicMock()
        first_result.fetchall = MagicMock(return_value=running_jobs)

        trace_result = MagicMock()
        trace_result.fetchone = MagicMock(
            return_value=({"content": "done"},)
        )

        # Counter to track calls within same/different sessions
        call_count = [0]

        async def execute_mock(stmt, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return first_result
            return trace_result

        session_mock = AsyncMock()
        session_mock.execute = execute_mock
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=session_mock)
        ctx.__aexit__ = AsyncMock(return_value=False)
        factory_mock = MagicMock(return_value=ctx)

        update_status_mock = AsyncMock(return_value=True)

        with patch("src.services.execution_cleanup_service.async_session_factory", factory_mock), \
             patch("src.services.execution_cleanup_service.ExecutionStatusService.update_status",
                   update_status_mock):
            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 2

    @pytest.mark.asyncio
    async def test_exception_returns_zero(self):
        """Exception during cleanup returns 0."""
        session_mock = AsyncMock()
        session_mock.execute = AsyncMock(side_effect=RuntimeError("DB error"))
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=session_mock)
        ctx.__aexit__ = AsyncMock(return_value=False)
        factory_mock = MagicMock(return_value=ctx)

        with patch("src.services.execution_cleanup_service.async_session_factory", factory_mock):
            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 0
