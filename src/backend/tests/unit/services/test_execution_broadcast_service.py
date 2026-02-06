"""
Unit tests for ExecutionBroadcastService.

Tests the functionality of execution status broadcasting via SSE,
including polling, status tracking, and event broadcasting.
"""
import pytest
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from src.services.execution_broadcast_service import (
    ExecutionBroadcastService,
    execution_broadcast_service,
)


class MockExecutionHistory:
    """Mock execution history record for testing."""

    def __init__(
        self,
        id=1,
        job_id="test-job-123",
        status="running",
        error=None,
        result=None,
        group_id="group-123",
        completed_at=None
    ):
        self.id = id
        self.job_id = job_id
        self.status = status
        self.error = error
        self.result = result
        self.group_id = group_id
        self.completed_at = completed_at


class MockSSEManager:
    """Mock SSE manager for testing."""

    def __init__(self):
        self.broadcasted_events = []

    def get_statistics(self):
        return {
            "total_connections": 1,
            "active_jobs": ["job-123"],
            "connections_per_job": {"job-123": 1}
        }

    async def broadcast_to_job(self, job_id, event):
        self.broadcasted_events.append((job_id, event))
        return 1


@pytest.fixture
def mock_execution():
    """Create a mock execution record."""
    return MockExecutionHistory()


@pytest.fixture
def mock_sse_manager():
    """Create a mock SSE manager."""
    return MockSSEManager()


class TestExecutionBroadcastServiceInit:
    """Test cases for ExecutionBroadcastService initialization."""

    def test_init_default_poll_interval(self):
        """Test service initialization with default poll interval."""
        service = ExecutionBroadcastService()

        assert service.poll_interval == 1.0
        assert service._running is False
        assert service._task is None
        assert service._last_statuses == {}
        assert service._last_completed_at == {}

    def test_init_custom_poll_interval(self):
        """Test service initialization with custom poll interval."""
        service = ExecutionBroadcastService(poll_interval=2.5)

        assert service.poll_interval == 2.5

    def test_init_state(self):
        """Test initial state of service."""
        service = ExecutionBroadcastService()

        assert not service._running
        assert service._task is None
        assert len(service._last_statuses) == 0


class TestExecutionBroadcastServiceStart:
    """Test cases for starting the broadcast service."""

    def test_start_creates_task(self):
        """Test that start creates a background task."""
        service = ExecutionBroadcastService()

        with patch.object(asyncio, 'create_task') as mock_create_task:
            mock_task = MagicMock()
            mock_create_task.return_value = mock_task

            service.start()

            assert service._running is True
            mock_create_task.assert_called_once()
            assert service._task == mock_task

    def test_start_when_already_running(self):
        """Test that start does nothing when already running."""
        service = ExecutionBroadcastService()
        service._running = True
        original_task = MagicMock()
        service._task = original_task

        with patch.object(asyncio, 'create_task') as mock_create_task:
            service.start()

            mock_create_task.assert_not_called()
            assert service._task == original_task


class TestExecutionBroadcastServiceStop:
    """Test cases for stopping the broadcast service."""

    def test_stop_cancels_task(self):
        """Test that stop cancels the background task."""
        service = ExecutionBroadcastService()
        mock_task = MagicMock()
        service._task = mock_task
        service._running = True

        service.stop()

        assert service._running is False
        mock_task.cancel.assert_called_once()
        assert service._task is None

    def test_stop_when_not_running(self):
        """Test that stop handles case when not running."""
        service = ExecutionBroadcastService()
        service._running = False
        service._task = None

        # Should not raise
        service.stop()

        assert service._running is False


class TestGetActiveJobIds:
    """Test cases for getting active job IDs."""

    def test_get_active_job_ids(self):
        """Test getting active job IDs from SSE manager."""
        service = ExecutionBroadcastService()

        with patch('src.services.execution_broadcast_service.sse_manager') as mock_manager:
            mock_manager.get_statistics.return_value = {
                "active_jobs": ["job-1", "job-2", "job-3"]
            }

            active_jobs = service._get_active_job_ids()

            assert active_jobs == {"job-1", "job-2", "job-3"}

    def test_get_active_job_ids_excludes_global_streams(self):
        """Test that global stream IDs are excluded."""
        service = ExecutionBroadcastService()

        with patch('src.services.execution_broadcast_service.sse_manager') as mock_manager:
            mock_manager.get_statistics.return_value = {
                "active_jobs": ["job-1", "all_groups_group1-group2", "job-2"]
            }

            active_jobs = service._get_active_job_ids()

            assert active_jobs == {"job-1", "job-2"}
            assert "all_groups_group1-group2" not in active_jobs

    def test_get_active_job_ids_empty(self):
        """Test getting active job IDs when none exist."""
        service = ExecutionBroadcastService()

        with patch('src.services.execution_broadcast_service.sse_manager') as mock_manager:
            mock_manager.get_statistics.return_value = {"active_jobs": []}

            active_jobs = service._get_active_job_ids()

            assert active_jobs == set()


class TestPollLoop:
    """Test cases for the polling loop."""

    @pytest.mark.asyncio
    async def test_poll_loop_polls_for_changes(self):
        """Test that poll loop calls poll_for_status_changes."""
        service = ExecutionBroadcastService(poll_interval=0.1)
        service._running = True

        call_count = 0

        async def mock_poll():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                service._running = False

        with patch.object(service, '_poll_for_status_changes', mock_poll):
            await service._poll_loop()

        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_poll_loop_handles_cancellation(self):
        """Test that poll loop handles cancellation gracefully."""
        service = ExecutionBroadcastService(poll_interval=0.1)
        service._running = True

        async def mock_poll():
            raise asyncio.CancelledError()

        with patch.object(service, '_poll_for_status_changes', mock_poll):
            # Should not raise
            await service._poll_loop()

    @pytest.mark.asyncio
    async def test_poll_loop_handles_errors(self):
        """Test that poll loop continues after errors."""
        service = ExecutionBroadcastService(poll_interval=0.1)
        service._running = True

        call_count = 0

        async def mock_poll():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Test error")
            if call_count >= 2:
                service._running = False

        with patch.object(service, '_poll_for_status_changes', mock_poll):
            await service._poll_loop()

        assert call_count >= 2


class TestPollForStatusChanges:
    """Test cases for polling status changes."""

    @pytest.mark.asyncio
    async def test_poll_no_active_jobs_returns_early(self):
        """Test that poll returns early when no active jobs."""
        service = ExecutionBroadcastService()

        with patch.object(service, '_get_active_job_ids', return_value=set()):
            with patch('src.services.execution_broadcast_service.async_session_factory') as mock_factory:
                await service._poll_for_status_changes()

                mock_factory.assert_not_called()

    @pytest.mark.asyncio
    async def test_poll_cleans_up_inactive_jobs(self):
        """Test that poll removes tracking for inactive jobs."""
        service = ExecutionBroadcastService()
        service._last_statuses = {"job-1": "running", "job-2": "running"}
        service._last_completed_at = {"job-1": None, "job-2": None}

        with patch.object(service, '_get_active_job_ids', return_value={"job-1"}):
            with patch('src.services.execution_broadcast_service.async_session_factory') as mock_factory:
                mock_session = AsyncMock()
                mock_factory.return_value.__aenter__.return_value = mock_session

                with patch.object(service, '_check_and_broadcast_status', new_callable=AsyncMock):
                    await service._poll_for_status_changes()

                assert "job-2" not in service._last_statuses
                assert "job-2" not in service._last_completed_at

    @pytest.mark.asyncio
    async def test_poll_checks_status_for_active_jobs(self):
        """Test that poll checks status for all active jobs."""
        service = ExecutionBroadcastService()

        with patch.object(service, '_get_active_job_ids', return_value={"job-1", "job-2"}):
            with patch('src.services.execution_broadcast_service.async_session_factory') as mock_factory:
                mock_session = AsyncMock()
                mock_factory.return_value.__aenter__.return_value = mock_session

                with patch.object(service, '_check_and_broadcast_status', new_callable=AsyncMock) as mock_check:
                    await service._poll_for_status_changes()

                assert mock_check.call_count == 2


class TestCheckAndBroadcastStatus:
    """Test cases for checking and broadcasting status changes."""

    @pytest.mark.asyncio
    async def test_no_broadcast_when_execution_not_found(self):
        """Test that no broadcast occurs when execution not found."""
        service = ExecutionBroadcastService()
        mock_session = AsyncMock()

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        with patch('src.services.execution_broadcast_service.sse_manager') as mock_manager:
            await service._check_and_broadcast_status(mock_session, "job-123")

            mock_manager.broadcast_to_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_broadcast_on_first_poll(self):
        """Test that no broadcast occurs on first poll (initial tracking)."""
        service = ExecutionBroadcastService()
        mock_session = AsyncMock()

        mock_execution = MockExecutionHistory(status="running")
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute.return_value = mock_result

        with patch('src.services.execution_broadcast_service.sse_manager') as mock_manager:
            await service._check_and_broadcast_status(mock_session, "job-123")

            mock_manager.broadcast_to_job.assert_not_called()
            assert service._last_statuses["job-123"] == "running"

    @pytest.mark.asyncio
    async def test_broadcast_on_status_change(self):
        """Test that broadcast occurs when status changes."""
        service = ExecutionBroadcastService()
        service._last_statuses["job-123"] = "running"
        service._last_completed_at["job-123"] = None
        mock_session = AsyncMock()

        mock_execution = MockExecutionHistory(status="completed")
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute.return_value = mock_result

        with patch('src.services.execution_broadcast_service.sse_manager') as mock_manager:
            mock_manager.broadcast_to_job = AsyncMock(return_value=1)

            await service._check_and_broadcast_status(mock_session, "job-123")

            mock_manager.broadcast_to_job.assert_called_once()
            assert service._last_statuses["job-123"] == "completed"

    @pytest.mark.asyncio
    async def test_broadcast_includes_correct_event_data(self):
        """Test that broadcast event contains correct data."""
        service = ExecutionBroadcastService()
        service._last_statuses["job-123"] = "running"
        service._last_completed_at["job-123"] = None
        mock_session = AsyncMock()

        mock_execution = MockExecutionHistory(
            status="completed",
            error=None,
            group_id="group-abc",
            result={"output": "success"}
        )
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute.return_value = mock_result

        with patch('src.services.execution_broadcast_service.sse_manager') as mock_manager:
            mock_manager.broadcast_to_job = AsyncMock(return_value=1)

            await service._check_and_broadcast_status(mock_session, "job-123")

            call_args = mock_manager.broadcast_to_job.call_args
            assert call_args[0][0] == "job-123"
            event = call_args[0][1]
            assert event.data["job_id"] == "job-123"
            assert event.data["status"] == "completed"
            assert event.data["group_id"] == "group-abc"
            assert event.event == "execution_update"

    @pytest.mark.asyncio
    async def test_no_broadcast_when_status_unchanged(self):
        """Test that no broadcast occurs when status is unchanged."""
        service = ExecutionBroadcastService()
        service._last_statuses["job-123"] = "running"
        service._last_completed_at["job-123"] = None
        mock_session = AsyncMock()

        mock_execution = MockExecutionHistory(status="running")
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute.return_value = mock_result

        with patch('src.services.execution_broadcast_service.sse_manager') as mock_manager:
            mock_manager.broadcast_to_job = AsyncMock(return_value=1)

            await service._check_and_broadcast_status(mock_session, "job-123")

            mock_manager.broadcast_to_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_broadcast_on_completed_at_change(self):
        """Test that broadcast occurs when completed_at changes."""
        service = ExecutionBroadcastService()
        service._last_statuses["job-123"] = "completed"
        service._last_completed_at["job-123"] = None
        mock_session = AsyncMock()

        completed_time = datetime.now()
        mock_execution = MockExecutionHistory(status="completed", completed_at=completed_time)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute.return_value = mock_result

        with patch('src.services.execution_broadcast_service.sse_manager') as mock_manager:
            mock_manager.broadcast_to_job = AsyncMock(return_value=1)

            await service._check_and_broadcast_status(mock_session, "job-123")

            mock_manager.broadcast_to_job.assert_called_once()

    @pytest.mark.asyncio
    async def test_handles_database_errors(self):
        """Test that database errors are handled gracefully."""
        service = ExecutionBroadcastService()
        service._last_statuses["job-123"] = "running"
        mock_session = AsyncMock()

        mock_session.execute.side_effect = Exception("Database error")

        with patch('src.services.execution_broadcast_service.sse_manager') as mock_manager:
            # Should not raise
            await service._check_and_broadcast_status(mock_session, "job-123")

            mock_manager.broadcast_to_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_broadcast_includes_result_when_available(self):
        """Test that result is included in broadcast when available."""
        service = ExecutionBroadcastService()
        service._last_statuses["job-123"] = "running"
        service._last_completed_at["job-123"] = None
        mock_session = AsyncMock()

        mock_execution = MockExecutionHistory(
            status="completed",
            result={"output": "test result", "count": 42}
        )
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_execution
        mock_session.execute.return_value = mock_result

        with patch('src.services.execution_broadcast_service.sse_manager') as mock_manager:
            mock_manager.broadcast_to_job = AsyncMock(return_value=1)

            await service._check_and_broadcast_status(mock_session, "job-123")

            call_args = mock_manager.broadcast_to_job.call_args
            event = call_args[0][1]
            assert "result" in event.data
            assert event.data["result"] == {"output": "test result", "count": 42}


class TestGlobalServiceInstance:
    """Test cases for the global service instance."""

    def test_global_instance_exists(self):
        """Test that global service instance exists."""
        assert execution_broadcast_service is not None
        assert isinstance(execution_broadcast_service, ExecutionBroadcastService)

    def test_global_instance_poll_interval(self):
        """Test global instance has correct poll interval."""
        assert execution_broadcast_service.poll_interval == 1.0

    def test_global_instance_has_required_methods(self):
        """Test that global instance has all required methods."""
        assert hasattr(execution_broadcast_service, 'start')
        assert hasattr(execution_broadcast_service, 'stop')
        assert callable(execution_broadcast_service.start)
        assert callable(execution_broadcast_service.stop)


class TestServiceLifecycle:
    """Test cases for service lifecycle management."""

    def test_start_stop_cycle(self):
        """Test starting and stopping the service."""
        service = ExecutionBroadcastService()

        with patch.object(asyncio, 'create_task') as mock_create_task:
            mock_task = MagicMock()
            mock_create_task.return_value = mock_task

            service.start()
            assert service._running is True

            service.stop()
            assert service._running is False
            mock_task.cancel.assert_called_once()

    def test_multiple_start_calls(self):
        """Test that multiple start calls don't create multiple tasks."""
        service = ExecutionBroadcastService()

        with patch.object(asyncio, 'create_task') as mock_create_task:
            mock_task = MagicMock()
            mock_create_task.return_value = mock_task

            service.start()
            service.start()
            service.start()

            mock_create_task.assert_called_once()

    def test_stop_without_start(self):
        """Test stopping service that was never started."""
        service = ExecutionBroadcastService()

        # Should not raise
        service.stop()

        assert service._running is False
        assert service._task is None
