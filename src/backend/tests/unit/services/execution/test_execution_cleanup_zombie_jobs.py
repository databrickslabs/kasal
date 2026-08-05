"""
Unit tests for ExecutionCleanupService.cleanup_zombie_jobs
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models.execution_status import ExecutionStatus
from src.services.execution.cleanup import ExecutionCleanupService


def _build_session_ctx():
    """Build a mock async session GENERATOR for `async for db in get_smart_db_session()`.

    The service reads executionhistory and execution_trace, which live in Lakebase
    when it is enabled, so it goes through the router rather than the raw
    async_session_factory — the factory is a per-process snapshot that a runtime
    /lakebase/enable never updates.
    """
    session_mock = AsyncMock()

    async def _gen():
        yield session_mock

    factory_mock = MagicMock(side_effect=lambda *a, **k: _gen())
    return factory_mock, session_mock


class TestCleanupZombieJobs:
    @pytest.mark.asyncio
    async def test_no_running_jobs_returns_zero(self):
        """When no jobs are RUNNING, returns 0 recovered."""
        factory_mock, _ = _build_session_ctx()

        with (
            patch("src.services.execution.cleanup.get_smart_db_session", factory_mock),
            patch(
                "src.services.execution.cleanup.ExecutionHistoryRepository"
            ) as MockHistoryRepo,
        ):
            MockHistoryRepo.return_value.get_job_ids_by_statuses = AsyncMock(
                return_value=[]
            )
            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 0

    @pytest.mark.asyncio
    async def test_running_job_with_no_completion_trace_left_alone(self):
        """Running jobs without completion trace are not touched."""
        factory_mock, _ = _build_session_ctx()

        with (
            patch("src.services.execution.cleanup.get_smart_db_session", factory_mock),
            patch(
                "src.services.execution.cleanup.ExecutionHistoryRepository"
            ) as MockHistoryRepo,
            patch(
                "src.services.execution.cleanup.ExecutionTraceRepository"
            ) as MockTraceRepo,
        ):
            MockHistoryRepo.return_value.get_job_ids_by_statuses = AsyncMock(
                return_value=["job-1"]
            )
            MockTraceRepo.return_value.has_completed_trace = AsyncMock(
                return_value=(False, None)
            )

            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 0

    @pytest.mark.asyncio
    async def test_zombie_job_with_dict_output_recovered(self):
        """Running job with crew_completed trace (dict content) is recovered."""
        factory_mock, _ = _build_session_ctx()
        trace_output = {"content": "Final answer from crew"}

        update_status_mock = AsyncMock(return_value=True)

        with (
            patch("src.services.execution.cleanup.get_smart_db_session", factory_mock),
            patch(
                "src.services.execution.cleanup.ExecutionHistoryRepository"
            ) as MockHistoryRepo,
            patch(
                "src.services.execution.cleanup.ExecutionTraceRepository"
            ) as MockTraceRepo,
            patch(
                "src.services.execution.cleanup.ExecutionStatusService.update_status",
                update_status_mock,
            ),
        ):
            MockHistoryRepo.return_value.get_job_ids_by_statuses = AsyncMock(
                return_value=["job-zombie"]
            )
            MockTraceRepo.return_value.has_completed_trace = AsyncMock(
                return_value=(True, trace_output)
            )

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
        factory_mock, _ = _build_session_ctx()
        json_output = json.dumps({"content": "JSON content"})

        update_status_mock = AsyncMock(return_value=True)

        with (
            patch("src.services.execution.cleanup.get_smart_db_session", factory_mock),
            patch(
                "src.services.execution.cleanup.ExecutionHistoryRepository"
            ) as MockHistoryRepo,
            patch(
                "src.services.execution.cleanup.ExecutionTraceRepository"
            ) as MockTraceRepo,
            patch(
                "src.services.execution.cleanup.ExecutionStatusService.update_status",
                update_status_mock,
            ),
        ):
            MockHistoryRepo.return_value.get_job_ids_by_statuses = AsyncMock(
                return_value=["job-json"]
            )
            MockTraceRepo.return_value.has_completed_trace = AsyncMock(
                return_value=(True, json_output)
            )

            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 1
        call_kwargs = update_status_mock.call_args[1]
        assert call_kwargs["result"] == "JSON content"

    @pytest.mark.asyncio
    async def test_zombie_job_with_non_json_string_output(self):
        """Zombie job with non-JSON string output falls back to str(output)."""
        factory_mock, _ = _build_session_ctx()
        plain_output = "plain text result"

        update_status_mock = AsyncMock(return_value=True)

        with (
            patch("src.services.execution.cleanup.get_smart_db_session", factory_mock),
            patch(
                "src.services.execution.cleanup.ExecutionHistoryRepository"
            ) as MockHistoryRepo,
            patch(
                "src.services.execution.cleanup.ExecutionTraceRepository"
            ) as MockTraceRepo,
            patch(
                "src.services.execution.cleanup.ExecutionStatusService.update_status",
                update_status_mock,
            ),
        ):
            MockHistoryRepo.return_value.get_job_ids_by_statuses = AsyncMock(
                return_value=["job-plain"]
            )
            MockTraceRepo.return_value.has_completed_trace = AsyncMock(
                return_value=(True, plain_output)
            )

            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 1
        call_kwargs = update_status_mock.call_args[1]
        assert call_kwargs["result"] == "plain text result"

    @pytest.mark.asyncio
    async def test_zombie_job_with_null_output(self):
        """Zombie job with null output still gets recovered."""
        factory_mock, _ = _build_session_ctx()

        update_status_mock = AsyncMock(return_value=True)

        with (
            patch("src.services.execution.cleanup.get_smart_db_session", factory_mock),
            patch(
                "src.services.execution.cleanup.ExecutionHistoryRepository"
            ) as MockHistoryRepo,
            patch(
                "src.services.execution.cleanup.ExecutionTraceRepository"
            ) as MockTraceRepo,
            patch(
                "src.services.execution.cleanup.ExecutionStatusService.update_status",
                update_status_mock,
            ),
        ):
            MockHistoryRepo.return_value.get_job_ids_by_statuses = AsyncMock(
                return_value=["job-null"]
            )
            MockTraceRepo.return_value.has_completed_trace = AsyncMock(
                return_value=(True, None)
            )

            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 1
        call_kwargs = update_status_mock.call_args[1]
        assert call_kwargs["result"] is None

    @pytest.mark.asyncio
    async def test_multiple_zombie_jobs_all_recovered(self):
        """Multiple zombie jobs are all recovered."""
        factory_mock, _ = _build_session_ctx()

        update_status_mock = AsyncMock(return_value=True)

        with (
            patch("src.services.execution.cleanup.get_smart_db_session", factory_mock),
            patch(
                "src.services.execution.cleanup.ExecutionHistoryRepository"
            ) as MockHistoryRepo,
            patch(
                "src.services.execution.cleanup.ExecutionTraceRepository"
            ) as MockTraceRepo,
            patch(
                "src.services.execution.cleanup.ExecutionStatusService.update_status",
                update_status_mock,
            ),
        ):
            MockHistoryRepo.return_value.get_job_ids_by_statuses = AsyncMock(
                return_value=["job-1", "job-2"]
            )
            MockTraceRepo.return_value.has_completed_trace = AsyncMock(
                return_value=(True, {"content": "done"})
            )

            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 2

    @pytest.mark.asyncio
    async def test_exception_returns_zero(self):
        """Exception during cleanup returns 0."""
        factory_mock, _ = _build_session_ctx()

        with (
            patch("src.services.execution.cleanup.get_smart_db_session", factory_mock),
            patch(
                "src.services.execution.cleanup.ExecutionHistoryRepository"
            ) as MockHistoryRepo,
        ):
            MockHistoryRepo.return_value.get_job_ids_by_statuses = AsyncMock(
                side_effect=RuntimeError("DB error")
            )
            result = await ExecutionCleanupService.cleanup_zombie_jobs()

        assert result == 0
