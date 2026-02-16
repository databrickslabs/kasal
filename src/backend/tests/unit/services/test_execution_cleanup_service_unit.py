"""
Unit tests for ExecutionCleanupService.

Tests cover both static methods:
- cleanup_stale_jobs_on_startup: marks orphaned jobs as CANCELLED on startup
- get_stale_jobs: returns list of active job IDs for monitoring

All database and service dependencies are mocked at their import location
inside src.services.execution_cleanup_service.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.services.execution_cleanup_service import ExecutionCleanupService
from src.models.execution_status import ExecutionStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_job(job_id: str, status: str) -> SimpleNamespace:
    """Create a lightweight mock execution record."""
    return SimpleNamespace(job_id=job_id, status=status)


def _build_async_session_context(repo_mock):
    """
    Build an async context manager mock that yields a session whose
    ExecutionRepository is already wired to *repo_mock*.
    """
    session_mock = AsyncMock()

    # Make the context manager for `async with async_session_factory() as db:`
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=session_mock)
    ctx.__aexit__ = AsyncMock(return_value=False)

    factory_mock = MagicMock(return_value=ctx)
    return factory_mock, session_mock


# ---------------------------------------------------------------------------
# Tests for cleanup_stale_jobs_on_startup
# ---------------------------------------------------------------------------


class TestCleanupStaleJobsOnStartup:
    """Tests for ExecutionCleanupService.cleanup_stale_jobs_on_startup."""

    @pytest.mark.asyncio
    async def test_no_stale_jobs_returns_zero(self):
        """When there are no active jobs, the method returns 0 and logs accordingly."""
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=([], 0))

        factory_mock, session_mock = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
        ):
            result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        assert result == 0
        repo_instance.get_execution_history.assert_awaited_once_with(
            limit=1000,
            offset=0,
            status_filter=[
                ExecutionStatus.PENDING.value,
                ExecutionStatus.PREPARING.value,
                ExecutionStatus.RUNNING.value,
            ],
            system_level=True,
        )

    @pytest.mark.asyncio
    async def test_all_stale_jobs_cancelled_successfully(self):
        """When all stale jobs are cleaned up successfully, returns the count."""
        jobs = [
            _make_job("job-1", ExecutionStatus.RUNNING.value),
            _make_job("job-2", ExecutionStatus.PENDING.value),
            _make_job("job-3", ExecutionStatus.PREPARING.value),
        ]
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=(jobs, 3))

        factory_mock, _ = _build_async_session_context(repo_instance)
        update_status_mock = AsyncMock(return_value=True)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionStatusService.update_status",
                update_status_mock,
            ),
        ):
            result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        assert result == 3
        assert update_status_mock.await_count == 3
        # Verify each call used the correct job_id and status
        for idx, job in enumerate(jobs):
            actual_call = update_status_mock.call_args_list[idx]
            assert actual_call == call(
                job_id=job.job_id,
                status=ExecutionStatus.CANCELLED.value,
                message="Job cancelled - service was restarted while job was running",
            )

    @pytest.mark.asyncio
    async def test_some_updates_fail(self):
        """When some update_status calls fail, only successful ones are counted."""
        jobs = [
            _make_job("job-ok", ExecutionStatus.RUNNING.value),
            _make_job("job-fail", ExecutionStatus.RUNNING.value),
            _make_job("job-ok-2", ExecutionStatus.PENDING.value),
        ]
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=(jobs, 3))

        factory_mock, _ = _build_async_session_context(repo_instance)
        update_status_mock = AsyncMock(side_effect=[True, False, True])

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionStatusService.update_status",
                update_status_mock,
            ),
        ):
            result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        assert result == 2

    @pytest.mark.asyncio
    async def test_all_updates_fail(self):
        """When every update_status call returns False, result is 0."""
        jobs = [
            _make_job("job-1", ExecutionStatus.RUNNING.value),
            _make_job("job-2", ExecutionStatus.RUNNING.value),
        ]
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=(jobs, 2))

        factory_mock, _ = _build_async_session_context(repo_instance)
        update_status_mock = AsyncMock(return_value=False)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionStatusService.update_status",
                update_status_mock,
            ),
        ):
            result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        assert result == 0

    @pytest.mark.asyncio
    async def test_exception_in_repo_returns_zero(self):
        """When the repository raises an exception, the method returns 0."""
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(
            side_effect=RuntimeError("DB connection lost")
        )

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
        ):
            result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        assert result == 0

    @pytest.mark.asyncio
    async def test_exception_during_update_returns_zero(self):
        """When update_status raises an exception mid-loop, the method returns 0."""
        jobs = [
            _make_job("job-1", ExecutionStatus.RUNNING.value),
        ]
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=(jobs, 1))

        factory_mock, _ = _build_async_session_context(repo_instance)
        update_status_mock = AsyncMock(side_effect=Exception("SSE broadcast failed"))

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionStatusService.update_status",
                update_status_mock,
            ),
        ):
            result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        assert result == 0

    @pytest.mark.asyncio
    async def test_exception_from_session_factory_returns_zero(self):
        """When the session factory itself raises, the method returns 0."""
        factory_mock = MagicMock()
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(side_effect=ConnectionError("refused"))
        ctx.__aexit__ = AsyncMock(return_value=False)
        factory_mock.return_value = ctx

        with patch(
            "src.services.execution_cleanup_service.async_session_factory",
            factory_mock,
        ):
            result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        assert result == 0

    @pytest.mark.asyncio
    async def test_single_stale_job_cleaned(self):
        """Edge case: exactly one stale job gets cleaned up."""
        jobs = [_make_job("only-job", ExecutionStatus.PREPARING.value)]
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=(jobs, 1))

        factory_mock, _ = _build_async_session_context(repo_instance)
        update_status_mock = AsyncMock(return_value=True)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionStatusService.update_status",
                update_status_mock,
            ),
        ):
            result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        assert result == 1
        update_status_mock.assert_awaited_once_with(
            job_id="only-job",
            status=ExecutionStatus.CANCELLED.value,
            message="Job cancelled - service was restarted while job was running",
        )

    @pytest.mark.asyncio
    async def test_correct_status_filter_values(self):
        """Verify the exact status values passed to get_execution_history."""
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=([], 0))

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
        ):
            await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        _, kwargs = repo_instance.get_execution_history.call_args
        expected_statuses = ["PENDING", "PREPARING", "RUNNING"]
        assert kwargs["status_filter"] == expected_statuses

    @pytest.mark.asyncio
    async def test_system_level_flag_set_to_true(self):
        """Verify system_level=True is passed for full access during cleanup."""
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=([], 0))

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
        ):
            await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        _, kwargs = repo_instance.get_execution_history.call_args
        assert kwargs["system_level"] is True

    @pytest.mark.asyncio
    async def test_logging_on_successful_cleanup(self):
        """Verify info log is emitted when jobs are cleaned up."""
        jobs = [_make_job("j1", ExecutionStatus.RUNNING.value)]
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=(jobs, 1))

        factory_mock, _ = _build_async_session_context(repo_instance)
        update_status_mock = AsyncMock(return_value=True)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionStatusService.update_status",
                update_status_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.logger"
            ) as mock_logger,
        ):
            result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        assert result == 1
        # Check that an info message about "Cleaned up 1 stale jobs" was logged
        info_messages = [str(c) for c in mock_logger.info.call_args_list]
        assert any("Cleaned up 1 stale jobs" in msg for msg in info_messages)

    @pytest.mark.asyncio
    async def test_logging_on_no_stale_jobs(self):
        """Verify info log when no stale jobs are found."""
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=([], 0))

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
            patch(
                "src.services.execution_cleanup_service.logger"
            ) as mock_logger,
        ):
            result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        assert result == 0
        info_messages = [str(c) for c in mock_logger.info.call_args_list]
        assert any("No stale jobs found" in msg for msg in info_messages)

    @pytest.mark.asyncio
    async def test_logging_on_failed_update(self):
        """Verify error log when update_status returns False."""
        jobs = [_make_job("fail-job", ExecutionStatus.RUNNING.value)]
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=(jobs, 1))

        factory_mock, _ = _build_async_session_context(repo_instance)
        update_status_mock = AsyncMock(return_value=False)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionStatusService.update_status",
                update_status_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.logger"
            ) as mock_logger,
        ):
            result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        assert result == 0
        error_messages = [str(c) for c in mock_logger.error.call_args_list]
        assert any("Failed to clean up stale job: fail-job" in msg for msg in error_messages)

    @pytest.mark.asyncio
    async def test_logging_on_exception(self):
        """Verify error log when an exception occurs."""
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(
            side_effect=ValueError("bad state")
        )

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
            patch(
                "src.services.execution_cleanup_service.logger"
            ) as mock_logger,
        ):
            result = await ExecutionCleanupService.cleanup_stale_jobs_on_startup()

        assert result == 0
        error_messages = [str(c) for c in mock_logger.error.call_args_list]
        assert any("Error during startup job cleanup" in msg for msg in error_messages)


# ---------------------------------------------------------------------------
# Tests for get_stale_jobs
# ---------------------------------------------------------------------------


class TestGetStaleJobs:
    """Tests for ExecutionCleanupService.get_stale_jobs."""

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_active_jobs(self):
        """When no jobs are in active states, returns an empty list."""
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=([], 0))

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
        ):
            result = await ExecutionCleanupService.get_stale_jobs()

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_job_ids_for_active_jobs(self):
        """When active jobs exist, returns their job_id values."""
        jobs = [
            _make_job("job-aaa", ExecutionStatus.RUNNING.value),
            _make_job("job-bbb", ExecutionStatus.PENDING.value),
            _make_job("job-ccc", ExecutionStatus.PREPARING.value),
        ]
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=(jobs, 3))

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
        ):
            result = await ExecutionCleanupService.get_stale_jobs()

        assert result == ["job-aaa", "job-bbb", "job-ccc"]

    @pytest.mark.asyncio
    async def test_returns_single_job_id(self):
        """Edge case: exactly one active job."""
        jobs = [_make_job("single-job", ExecutionStatus.RUNNING.value)]
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=(jobs, 1))

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
        ):
            result = await ExecutionCleanupService.get_stale_jobs()

        assert result == ["single-job"]

    @pytest.mark.asyncio
    async def test_uses_correct_status_filter(self):
        """Verify the status filter passed to get_execution_history."""
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=([], 0))

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
        ):
            await ExecutionCleanupService.get_stale_jobs()

        _, kwargs = repo_instance.get_execution_history.call_args
        assert kwargs["status_filter"] == ["PENDING", "PREPARING", "RUNNING"]

    @pytest.mark.asyncio
    async def test_uses_system_level_true(self):
        """Verify system_level=True is set for monitoring access."""
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=([], 0))

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
        ):
            await ExecutionCleanupService.get_stale_jobs()

        _, kwargs = repo_instance.get_execution_history.call_args
        assert kwargs["system_level"] is True

    @pytest.mark.asyncio
    async def test_uses_correct_limit_and_offset(self):
        """Verify limit=1000 and offset=0 are passed."""
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=([], 0))

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
        ):
            await ExecutionCleanupService.get_stale_jobs()

        _, kwargs = repo_instance.get_execution_history.call_args
        assert kwargs["limit"] == 1000
        assert kwargs["offset"] == 0

    @pytest.mark.asyncio
    async def test_exception_returns_empty_list(self):
        """When the repository raises an exception, returns an empty list."""
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(
            side_effect=RuntimeError("connection reset")
        )

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
        ):
            result = await ExecutionCleanupService.get_stale_jobs()

        assert result == []

    @pytest.mark.asyncio
    async def test_session_factory_exception_returns_empty_list(self):
        """When the session factory raises, returns an empty list."""
        factory_mock = MagicMock()
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(side_effect=OSError("socket closed"))
        ctx.__aexit__ = AsyncMock(return_value=False)
        factory_mock.return_value = ctx

        with patch(
            "src.services.execution_cleanup_service.async_session_factory",
            factory_mock,
        ):
            result = await ExecutionCleanupService.get_stale_jobs()

        assert result == []

    @pytest.mark.asyncio
    async def test_logging_on_exception(self):
        """Verify error log when exception occurs in get_stale_jobs."""
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(
            side_effect=TypeError("unexpected None")
        )

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
            patch(
                "src.services.execution_cleanup_service.logger"
            ) as mock_logger,
        ):
            result = await ExecutionCleanupService.get_stale_jobs()

        assert result == []
        error_messages = [str(c) for c in mock_logger.error.call_args_list]
        assert any("Error getting stale jobs" in msg for msg in error_messages)

    @pytest.mark.asyncio
    async def test_preserves_job_id_order(self):
        """Verify that job IDs are returned in the same order as the repo results."""
        jobs = [
            _make_job("z-last", ExecutionStatus.RUNNING.value),
            _make_job("a-first", ExecutionStatus.PENDING.value),
            _make_job("m-middle", ExecutionStatus.PREPARING.value),
        ]
        repo_instance = AsyncMock()
        repo_instance.get_execution_history = AsyncMock(return_value=(jobs, 3))

        factory_mock, _ = _build_async_session_context(repo_instance)

        with (
            patch(
                "src.services.execution_cleanup_service.async_session_factory",
                factory_mock,
            ),
            patch(
                "src.services.execution_cleanup_service.ExecutionRepository",
                return_value=repo_instance,
            ),
        ):
            result = await ExecutionCleanupService.get_stale_jobs()

        assert result == ["z-last", "a-first", "m-middle"]


# ---------------------------------------------------------------------------
# Tests for ExecutionStatus enum values used by the service
# ---------------------------------------------------------------------------


class TestExecutionStatusEnumConsistency:
    """Verify the enum values the service depends on are correct."""

    def test_pending_value(self):
        assert ExecutionStatus.PENDING.value == "PENDING"

    def test_preparing_value(self):
        assert ExecutionStatus.PREPARING.value == "PREPARING"

    def test_running_value(self):
        assert ExecutionStatus.RUNNING.value == "RUNNING"

    def test_cancelled_value(self):
        assert ExecutionStatus.CANCELLED.value == "CANCELLED"


# ---------------------------------------------------------------------------
# Tests for static method nature
# ---------------------------------------------------------------------------


class TestServiceIsStaticMethods:
    """Verify both public methods are static and can be called without an instance."""

    def test_cleanup_stale_jobs_on_startup_is_static(self):
        assert isinstance(
            ExecutionCleanupService.__dict__["cleanup_stale_jobs_on_startup"],
            staticmethod,
        )

    def test_get_stale_jobs_is_static(self):
        assert isinstance(
            ExecutionCleanupService.__dict__["get_stale_jobs"],
            staticmethod,
        )
