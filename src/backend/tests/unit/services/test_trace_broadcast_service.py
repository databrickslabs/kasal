"""
Unit tests for TraceBroadcastService.

Tests the functionality of trace broadcasting via SSE,
including polling, trace tracking, and event broadcasting.
"""
import pytest
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.trace_broadcast_service import (
    TraceBroadcastService,
    trace_broadcast_service,
)


class MockExecutionTrace:
    """Mock execution trace record for testing."""

    def __init__(
        self,
        id=1,
        run_id="run-123",
        job_id="test-job-123",
        event_source="agent",
        event_context="task-1",
        event_type="task_start",
        output="Processing task",
        trace_metadata=None,
        created_at=None,
        group_id="group-123",
        group_email="test@example.com"
    ):
        self.id = id
        self.run_id = run_id
        self.job_id = job_id
        self.event_source = event_source
        self.event_context = event_context
        self.event_type = event_type
        self.output = output
        self.trace_metadata = trace_metadata or {}
        self.created_at = created_at or datetime.now()
        self.group_id = group_id
        self.group_email = group_email


@pytest.fixture
def mock_trace():
    """Create a mock trace record."""
    return MockExecutionTrace()


class TestTraceBroadcastServiceInit:
    """Test cases for TraceBroadcastService initialization."""

    def test_init_default_poll_interval(self):
        """Test service initialization with default poll interval."""
        service = TraceBroadcastService()

        assert service.poll_interval == 1.0
        assert service._running is False
        assert service._task is None
        assert service._last_trace_ids == {}

    def test_init_custom_poll_interval(self):
        """Test service initialization with custom poll interval."""
        service = TraceBroadcastService(poll_interval=0.5)

        assert service.poll_interval == 0.5

    def test_init_state(self):
        """Test initial state of service."""
        service = TraceBroadcastService()

        assert not service._running
        assert service._task is None
        assert len(service._last_trace_ids) == 0


class TestTraceBroadcastServiceStart:
    """Test cases for starting the trace broadcast service."""

    def test_start_creates_task(self):
        """Test that start creates a background task."""
        service = TraceBroadcastService()

        with patch.object(asyncio, 'create_task') as mock_create_task:
            mock_task = MagicMock()
            mock_create_task.return_value = mock_task

            service.start()

            assert service._running is True
            mock_create_task.assert_called_once()
            assert service._task == mock_task

    def test_start_when_already_running(self):
        """Test that start does nothing when already running."""
        service = TraceBroadcastService()
        service._running = True
        original_task = MagicMock()
        service._task = original_task

        with patch.object(asyncio, 'create_task') as mock_create_task:
            service.start()

            mock_create_task.assert_not_called()
            assert service._task == original_task


class TestTraceBroadcastServiceStop:
    """Test cases for stopping the trace broadcast service."""

    def test_stop_cancels_task(self):
        """Test that stop cancels the background task."""
        service = TraceBroadcastService()
        mock_task = MagicMock()
        service._task = mock_task
        service._running = True

        service.stop()

        assert service._running is False
        mock_task.cancel.assert_called_once()
        assert service._task is None

    def test_stop_when_not_running(self):
        """Test that stop handles case when not running."""
        service = TraceBroadcastService()
        service._running = False
        service._task = None

        # Should not raise
        service.stop()

        assert service._running is False


class TestGetActiveJobIds:
    """Test cases for getting active job IDs."""

    def test_get_active_job_ids(self):
        """Test getting active job IDs from SSE manager."""
        service = TraceBroadcastService()

        with patch('src.services.trace_broadcast_service.sse_manager') as mock_manager:
            mock_manager.get_statistics.return_value = {
                "active_jobs": ["job-1", "job-2", "job-3"]
            }

            active_jobs = service._get_active_job_ids()

            assert active_jobs == {"job-1", "job-2", "job-3"}

    def test_get_active_job_ids_excludes_global_streams(self):
        """Test that global stream IDs are excluded."""
        service = TraceBroadcastService()

        with patch('src.services.trace_broadcast_service.sse_manager') as mock_manager:
            mock_manager.get_statistics.return_value = {
                "active_jobs": ["job-1", "all_groups_group1-group2", "job-2"]
            }

            active_jobs = service._get_active_job_ids()

            assert active_jobs == {"job-1", "job-2"}
            assert "all_groups_group1-group2" not in active_jobs

    def test_get_active_job_ids_empty(self):
        """Test getting active job IDs when none exist."""
        service = TraceBroadcastService()

        with patch('src.services.trace_broadcast_service.sse_manager') as mock_manager:
            mock_manager.get_statistics.return_value = {"active_jobs": []}

            active_jobs = service._get_active_job_ids()

            assert active_jobs == set()


class TestPollLoop:
    """Test cases for the polling loop."""

    @pytest.mark.asyncio
    async def test_poll_loop_polls_for_traces(self):
        """Test that poll loop calls poll_for_traces."""
        service = TraceBroadcastService(poll_interval=0.1)
        service._running = True

        call_count = 0

        async def mock_poll():
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                service._running = False

        with patch.object(service, '_poll_for_traces', mock_poll):
            await service._poll_loop()

        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_poll_loop_handles_cancellation(self):
        """Test that poll loop handles cancellation gracefully."""
        service = TraceBroadcastService(poll_interval=0.1)
        service._running = True

        async def mock_poll():
            raise asyncio.CancelledError()

        with patch.object(service, '_poll_for_traces', mock_poll):
            # Should not raise
            await service._poll_loop()

    @pytest.mark.asyncio
    async def test_poll_loop_handles_errors(self):
        """Test that poll loop continues after errors."""
        service = TraceBroadcastService(poll_interval=0.1)
        service._running = True

        call_count = 0

        async def mock_poll():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Test error")
            if call_count >= 2:
                service._running = False

        with patch.object(service, '_poll_for_traces', mock_poll):
            await service._poll_loop()

        assert call_count >= 2


class TestPollForTraces:
    """Test cases for polling traces."""

    @pytest.mark.asyncio
    async def test_poll_no_active_jobs_returns_early(self):
        """Test that poll returns early when no active jobs."""
        service = TraceBroadcastService()

        with patch.object(service, '_get_active_job_ids', return_value=set()):
            with patch('src.services.trace_broadcast_service.async_session_factory') as mock_factory:
                await service._poll_for_traces()

                mock_factory.assert_not_called()

    @pytest.mark.asyncio
    async def test_poll_initializes_tracking_for_new_jobs(self):
        """Test that poll initializes tracking from current max ID."""
        service = TraceBroadcastService()

        with patch.object(service, '_get_active_job_ids', return_value={"job-123"}):
            with patch('src.services.trace_broadcast_service.async_session_factory') as mock_factory:
                mock_session = AsyncMock()
                mock_factory.return_value.__aenter__.return_value = mock_session

                # Mock max ID query
                mock_result = MagicMock()
                mock_result.scalar.return_value = 42
                mock_session.execute.return_value = mock_result

                with patch.object(service, '_broadcast_new_traces_for_job', new_callable=AsyncMock):
                    await service._poll_for_traces()

                assert service._last_trace_ids["job-123"] == 42

    @pytest.mark.asyncio
    async def test_poll_cleans_up_inactive_jobs(self):
        """Test that poll removes tracking for inactive jobs."""
        service = TraceBroadcastService()
        service._last_trace_ids = {"job-1": 10, "job-2": 20}

        with patch.object(service, '_get_active_job_ids', return_value={"job-1"}):
            with patch('src.services.trace_broadcast_service.async_session_factory') as mock_factory:
                mock_session = AsyncMock()
                mock_factory.return_value.__aenter__.return_value = mock_session

                # Set up mock to handle the initialization call
                mock_result = MagicMock()
                mock_result.scalar.return_value = 10
                mock_session.execute.return_value = mock_result

                with patch.object(service, '_broadcast_new_traces_for_job', new_callable=AsyncMock):
                    await service._poll_for_traces()

                assert "job-2" not in service._last_trace_ids


class TestBroadcastNewTracesForJob:
    """Test cases for broadcasting new traces."""

    @pytest.mark.asyncio
    async def test_no_broadcast_when_no_new_traces(self):
        """Test that no broadcast occurs when no new traces."""
        service = TraceBroadcastService()
        service._last_trace_ids["job-123"] = 10
        mock_session = AsyncMock()

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        with patch('src.services.trace_broadcast_service.sse_manager') as mock_manager:
            await service._broadcast_new_traces_for_job(mock_session, "job-123")

            mock_manager.broadcast_to_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_broadcast_new_traces(self):
        """Test that new traces are broadcasted."""
        service = TraceBroadcastService()
        service._last_trace_ids["job-123"] = 10
        mock_session = AsyncMock()

        mock_traces = [
            MockExecutionTrace(id=11),
            MockExecutionTrace(id=12),
        ]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_traces
        mock_session.execute.return_value = mock_result

        with patch('src.services.trace_broadcast_service.sse_manager') as mock_manager:
            mock_manager.broadcast_to_job = AsyncMock(return_value=1)

            await service._broadcast_new_traces_for_job(mock_session, "job-123")

            assert mock_manager.broadcast_to_job.call_count == 2
            assert service._last_trace_ids["job-123"] == 12

    @pytest.mark.asyncio
    async def test_broadcast_trace_contains_correct_data(self):
        """Test that broadcast trace contains correct data."""
        service = TraceBroadcastService()
        service._last_trace_ids["job-123"] = 10
        mock_session = AsyncMock()

        trace = MockExecutionTrace(
            id=11,
            run_id="run-abc",
            job_id="job-123",
            event_source="agent",
            event_context="context-1",
            event_type="task_complete",
            output="Task finished",
            trace_metadata={"key": "value"},
            group_id="group-xyz",
            group_email="user@example.com"
        )
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [trace]
        mock_session.execute.return_value = mock_result

        with patch('src.services.trace_broadcast_service.sse_manager') as mock_manager:
            mock_manager.broadcast_to_job = AsyncMock(return_value=1)

            await service._broadcast_new_traces_for_job(mock_session, "job-123")

            call_args = mock_manager.broadcast_to_job.call_args
            assert call_args[0][0] == "job-123"
            event = call_args[0][1]
            assert event.data["id"] == 11
            assert event.data["run_id"] == "run-abc"
            assert event.data["job_id"] == "job-123"
            assert event.data["event_source"] == "agent"
            assert event.data["event_type"] == "task_complete"
            assert event.data["output"] == "Task finished"
            assert event.data["group_id"] == "group-xyz"
            assert event.event == "trace"

    @pytest.mark.asyncio
    async def test_broadcast_updates_last_trace_id(self):
        """Test that last trace ID is updated after broadcast."""
        service = TraceBroadcastService()
        service._last_trace_ids["job-123"] = 10
        mock_session = AsyncMock()

        mock_traces = [
            MockExecutionTrace(id=15),
            MockExecutionTrace(id=20),
            MockExecutionTrace(id=25),
        ]
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = mock_traces
        mock_session.execute.return_value = mock_result

        with patch('src.services.trace_broadcast_service.sse_manager') as mock_manager:
            mock_manager.broadcast_to_job = AsyncMock(return_value=1)

            await service._broadcast_new_traces_for_job(mock_session, "job-123")

            assert service._last_trace_ids["job-123"] == 25

    @pytest.mark.asyncio
    async def test_handles_database_errors(self):
        """Test that database errors are handled gracefully."""
        service = TraceBroadcastService()
        service._last_trace_ids["job-123"] = 10
        mock_session = AsyncMock()

        mock_session.execute.side_effect = Exception("Database error")

        with patch('src.services.trace_broadcast_service.sse_manager') as mock_manager:
            # Should not raise
            await service._broadcast_new_traces_for_job(mock_session, "job-123")

            mock_manager.broadcast_to_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_uses_correct_query_filter(self):
        """Test that query uses correct filter for new traces."""
        service = TraceBroadcastService()
        service._last_trace_ids["job-123"] = 50
        mock_session = AsyncMock()

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        await service._broadcast_new_traces_for_job(mock_session, "job-123")

        # Verify execute was called with a query
        mock_session.execute.assert_called_once()


class TestGlobalServiceInstance:
    """Test cases for the global service instance."""

    def test_global_instance_exists(self):
        """Test that global service instance exists."""
        assert trace_broadcast_service is not None
        assert isinstance(trace_broadcast_service, TraceBroadcastService)

    def test_global_instance_poll_interval(self):
        """Test global instance has correct poll interval."""
        assert trace_broadcast_service.poll_interval == 1.0

    def test_global_instance_has_required_methods(self):
        """Test that global instance has all required methods."""
        assert hasattr(trace_broadcast_service, 'start')
        assert hasattr(trace_broadcast_service, 'stop')
        assert callable(trace_broadcast_service.start)
        assert callable(trace_broadcast_service.stop)


class TestServiceLifecycle:
    """Test cases for service lifecycle management."""

    def test_start_stop_cycle(self):
        """Test starting and stopping the service."""
        service = TraceBroadcastService()

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
        service = TraceBroadcastService()

        with patch.object(asyncio, 'create_task') as mock_create_task:
            mock_task = MagicMock()
            mock_create_task.return_value = mock_task

            service.start()
            service.start()
            service.start()

            mock_create_task.assert_called_once()

    def test_stop_without_start(self):
        """Test stopping service that was never started."""
        service = TraceBroadcastService()

        # Should not raise
        service.stop()

        assert service._running is False
        assert service._task is None


class TestTraceDataFormatting:
    """Test cases for trace data formatting in events."""

    @pytest.mark.asyncio
    async def test_trace_created_at_formatting(self):
        """Test that created_at is formatted as ISO string."""
        service = TraceBroadcastService()
        service._last_trace_ids["job-123"] = 10
        mock_session = AsyncMock()

        test_time = datetime(2024, 1, 15, 10, 30, 45)
        trace = MockExecutionTrace(id=11, created_at=test_time)
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [trace]
        mock_session.execute.return_value = mock_result

        with patch('src.services.trace_broadcast_service.sse_manager') as mock_manager:
            mock_manager.broadcast_to_job = AsyncMock(return_value=1)

            await service._broadcast_new_traces_for_job(mock_session, "job-123")

            call_args = mock_manager.broadcast_to_job.call_args
            event = call_args[0][1]
            assert event.data["created_at"] == test_time.isoformat()

    @pytest.mark.asyncio
    async def test_trace_with_none_created_at(self):
        """Test handling of trace with None created_at."""
        service = TraceBroadcastService()
        service._last_trace_ids["job-123"] = 10
        mock_session = AsyncMock()

        trace = MockExecutionTrace(id=11)
        trace.created_at = None
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [trace]
        mock_session.execute.return_value = mock_result

        with patch('src.services.trace_broadcast_service.sse_manager') as mock_manager:
            mock_manager.broadcast_to_job = AsyncMock(return_value=1)

            await service._broadcast_new_traces_for_job(mock_session, "job-123")

            call_args = mock_manager.broadcast_to_job.call_args
            event = call_args[0][1]
            assert event.data["created_at"] is None

    @pytest.mark.asyncio
    async def test_event_id_format(self):
        """Test that event ID is formatted correctly."""
        service = TraceBroadcastService()
        service._last_trace_ids["job-123"] = 10
        mock_session = AsyncMock()

        trace = MockExecutionTrace(id=42)
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [trace]
        mock_session.execute.return_value = mock_result

        with patch('src.services.trace_broadcast_service.sse_manager') as mock_manager:
            mock_manager.broadcast_to_job = AsyncMock(return_value=1)

            await service._broadcast_new_traces_for_job(mock_session, "job-123")

            call_args = mock_manager.broadcast_to_job.call_args
            event = call_args[0][1]
            assert event.id == "job-123_trace_42"
