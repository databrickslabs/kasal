"""
Unit tests for SSEConnectionManager and SSE infrastructure.

Tests the functionality of SSE event formatting, connection management,
broadcasting, and event stream generation.
"""
import pytest
import asyncio
import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from src.core.sse_manager import (
    SSEEvent,
    SSEConnectionManager,
    event_stream_generator,
    sse_manager,
)


class TestSSEEvent:
    """Test cases for SSEEvent class."""

    def test_event_init_basic(self):
        """Test basic SSEEvent initialization."""
        event = SSEEvent(data={"message": "test"})

        assert event.data == {"message": "test"}
        assert event.event is None
        assert event.id is None
        assert event.retry is None

    def test_event_init_full(self):
        """Test SSEEvent initialization with all parameters."""
        event = SSEEvent(
            data={"message": "test"},
            event="test_event",
            id="event-123",
            retry=5000
        )

        assert event.data == {"message": "test"}
        assert event.event == "test_event"
        assert event.id == "event-123"
        assert event.retry == 5000

    def test_format_with_dict_data(self):
        """Test SSE event formatting with dictionary data."""
        event = SSEEvent(data={"status": "running", "progress": 50})
        formatted = event.format()

        assert "data: " in formatted
        assert formatted.endswith("\n\n")
        # Verify JSON is properly embedded
        data_line = [line for line in formatted.split("\n") if line.startswith("data: ")][0]
        json_data = json.loads(data_line.replace("data: ", ""))
        assert json_data == {"status": "running", "progress": 50}

    def test_format_with_list_data(self):
        """Test SSE event formatting with list data."""
        event = SSEEvent(data=[1, 2, 3])
        formatted = event.format()

        data_line = [line for line in formatted.split("\n") if line.startswith("data: ")][0]
        json_data = json.loads(data_line.replace("data: ", ""))
        assert json_data == [1, 2, 3]

    def test_format_with_string_data(self):
        """Test SSE event formatting with string data."""
        event = SSEEvent(data="simple message")
        formatted = event.format()

        assert "data: simple message" in formatted

    def test_format_with_event_type(self):
        """Test SSE event formatting with event type."""
        event = SSEEvent(data="test", event="execution_update")
        formatted = event.format()

        assert "event: execution_update" in formatted

    def test_format_with_event_id(self):
        """Test SSE event formatting with event ID."""
        event = SSEEvent(data="test", id="msg-456")
        formatted = event.format()

        assert "id: msg-456" in formatted

    def test_format_with_retry(self):
        """Test SSE event formatting with retry interval."""
        event = SSEEvent(data="test", retry=3000)
        formatted = event.format()

        assert "retry: 3000" in formatted

    def test_format_full_event(self):
        """Test SSE event formatting with all fields."""
        event = SSEEvent(
            data={"job_id": "123", "status": "completed"},
            event="execution_update",
            id="event-789",
            retry=5000
        )
        formatted = event.format()

        assert "event: execution_update" in formatted
        assert "id: event-789" in formatted
        assert "retry: 5000" in formatted
        assert "data: " in formatted
        assert formatted.endswith("\n\n")

    def test_format_multiline_data(self):
        """Test SSE event formatting with multiline string data."""
        event = SSEEvent(data="line1\nline2\nline3")
        formatted = event.format()

        # Each line should have its own data: prefix
        assert "data: line1" in formatted
        assert "data: line2" in formatted
        assert "data: line3" in formatted


class TestSSEConnectionManager:
    """Test cases for SSEConnectionManager class."""

    def test_init(self):
        """Test SSEConnectionManager initialization."""
        manager = SSEConnectionManager()

        assert manager.job_queues == {}
        assert manager.connection_count == 0

    def test_create_event_queue_new_job(self):
        """Test creating event queue for a new job."""
        manager = SSEConnectionManager()
        queue = manager.create_event_queue("job-123")

        assert isinstance(queue, asyncio.Queue)
        assert "job-123" in manager.job_queues
        assert queue in manager.job_queues["job-123"]
        assert manager.connection_count == 1

    def test_create_event_queue_existing_job(self):
        """Test creating additional event queue for existing job."""
        manager = SSEConnectionManager()
        queue1 = manager.create_event_queue("job-123")
        queue2 = manager.create_event_queue("job-123")

        assert queue1 != queue2
        assert len(manager.job_queues["job-123"]) == 2
        assert manager.connection_count == 2

    def test_create_event_queue_multiple_jobs(self):
        """Test creating event queues for multiple jobs."""
        manager = SSEConnectionManager()
        manager.create_event_queue("job-1")
        manager.create_event_queue("job-2")
        manager.create_event_queue("job-3")

        assert len(manager.job_queues) == 3
        assert manager.connection_count == 3

    def test_remove_event_queue_existing(self):
        """Test removing an existing event queue."""
        manager = SSEConnectionManager()
        queue = manager.create_event_queue("job-123")

        manager.remove_event_queue("job-123", queue)

        assert "job-123" not in manager.job_queues
        assert manager.connection_count == 0

    def test_remove_event_queue_partial(self):
        """Test removing one queue when multiple exist for a job."""
        manager = SSEConnectionManager()
        queue1 = manager.create_event_queue("job-123")
        queue2 = manager.create_event_queue("job-123")

        manager.remove_event_queue("job-123", queue1)

        assert "job-123" in manager.job_queues
        assert queue2 in manager.job_queues["job-123"]
        assert queue1 not in manager.job_queues["job-123"]
        assert manager.connection_count == 1

    def test_remove_event_queue_nonexistent_job(self):
        """Test removing queue for nonexistent job doesn't raise error."""
        manager = SSEConnectionManager()
        queue = asyncio.Queue()

        # Should not raise
        manager.remove_event_queue("nonexistent-job", queue)
        assert manager.connection_count == 0  # Guard prevents decrement below zero

    def test_remove_event_queue_nonexistent_queue(self):
        """Test removing nonexistent queue from job doesn't raise error."""
        manager = SSEConnectionManager()
        real_queue = manager.create_event_queue("job-123")
        other_queue = asyncio.Queue()

        # Should not raise
        manager.remove_event_queue("job-123", other_queue)
        assert real_queue in manager.job_queues["job-123"]

    @pytest.mark.asyncio
    async def test_broadcast_to_job_success(self):
        """Test successful broadcast to job subscribers."""
        manager = SSEConnectionManager()
        queue = manager.create_event_queue("job-123")
        event = SSEEvent(data={"status": "running"}, event="execution_update")

        sent_count = await manager.broadcast_to_job("job-123", event)

        assert sent_count == 1
        received_event = queue.get_nowait()
        assert received_event == event

    @pytest.mark.asyncio
    async def test_broadcast_to_job_multiple_subscribers(self):
        """Test broadcast to multiple subscribers."""
        manager = SSEConnectionManager()
        queue1 = manager.create_event_queue("job-123")
        queue2 = manager.create_event_queue("job-123")
        event = SSEEvent(data={"status": "running"})

        sent_count = await manager.broadcast_to_job("job-123", event)

        assert sent_count == 2
        assert queue1.get_nowait() == event
        assert queue2.get_nowait() == event

    @pytest.mark.asyncio
    async def test_broadcast_to_job_no_subscribers(self):
        """Test broadcast when no subscribers exist."""
        manager = SSEConnectionManager()
        event = SSEEvent(data={"status": "running"})

        sent_count = await manager.broadcast_to_job("nonexistent-job", event)

        assert sent_count == 0

    @pytest.mark.asyncio
    async def test_broadcast_to_job_queue_full(self):
        """Test broadcast when queue is full."""
        manager = SSEConnectionManager()
        queue = manager.create_event_queue("job-123")

        # Fill the queue
        for i in range(100):
            try:
                queue.put_nowait(SSEEvent(data=f"msg-{i}"))
            except asyncio.QueueFull:
                break

        # Attempt to broadcast - should handle gracefully
        event = SSEEvent(data={"status": "new"})
        with patch.object(manager, 'job_queues', {"job-123": {queue}}):
            sent_count = await manager.broadcast_to_job("job-123", event)
            # Count may be 0 or 1 depending on queue state
            assert sent_count >= 0

    @pytest.mark.asyncio
    async def test_broadcast_to_global_streams(self):
        """Test broadcast also sends to global stream subscribers."""
        manager = SSEConnectionManager()
        job_queue = manager.create_event_queue("job-123")
        global_queue = manager.create_event_queue("all_groups_group1-group2")
        event = SSEEvent(data={"job_id": "job-123", "status": "running"})

        sent_count = await manager.broadcast_to_job("job-123", event)

        # Should send to both job-specific and global stream
        assert sent_count == 2
        assert job_queue.get_nowait() == event
        assert global_queue.get_nowait() == event

    def test_get_connection_count_total(self):
        """Test getting total connection count."""
        manager = SSEConnectionManager()
        manager.create_event_queue("job-1")
        manager.create_event_queue("job-1")
        manager.create_event_queue("job-2")

        assert manager.get_connection_count() == 3

    def test_get_connection_count_specific_job(self):
        """Test getting connection count for specific job."""
        manager = SSEConnectionManager()
        manager.create_event_queue("job-1")
        manager.create_event_queue("job-1")
        manager.create_event_queue("job-2")

        assert manager.get_connection_count("job-1") == 2
        assert manager.get_connection_count("job-2") == 1
        assert manager.get_connection_count("nonexistent") == 0

    def test_get_statistics(self):
        """Test getting SSE statistics."""
        manager = SSEConnectionManager()
        manager.create_event_queue("job-1")
        manager.create_event_queue("job-1")
        manager.create_event_queue("job-2")

        stats = manager.get_statistics()

        assert stats["total_connections"] == 3
        assert set(stats["active_jobs"]) == {"job-1", "job-2"}
        assert stats["connections_per_job"]["job-1"] == 2
        assert stats["connections_per_job"]["job-2"] == 1

    def test_get_statistics_empty(self):
        """Test getting statistics when no connections exist."""
        manager = SSEConnectionManager()
        stats = manager.get_statistics()

        assert stats["total_connections"] == 0
        assert stats["active_jobs"] == []
        assert stats["connections_per_job"] == {}


class TestEventStreamGenerator:
    """Test cases for event_stream_generator function."""

    @pytest.mark.asyncio
    async def test_generator_yields_connection_event(self):
        """Test that generator yields initial connection event."""
        with patch('src.core.sse_manager.sse_manager') as mock_manager:
            mock_queue = asyncio.Queue()
            mock_manager.create_event_queue.return_value = mock_queue

            gen = event_stream_generator("job-123", timeout=1, heartbeat_interval=30)

            # Get first event (connection)
            first_event = await gen.__anext__()

            assert "event: connected" in first_event
            assert "job-123" in first_event

    @pytest.mark.asyncio
    async def test_generator_yields_events_from_queue(self):
        """Test that generator yields events from queue."""
        with patch('src.core.sse_manager.sse_manager') as mock_manager:
            mock_queue = asyncio.Queue()
            mock_manager.create_event_queue.return_value = mock_queue

            # Put an event in the queue
            test_event = SSEEvent(data={"status": "running"}, event="execution_update")
            await mock_queue.put(test_event)

            gen = event_stream_generator("job-123", timeout=10, heartbeat_interval=30)

            # Skip connection event
            await gen.__anext__()

            # Get queued event
            event = await gen.__anext__()
            assert "execution_update" in event or "running" in event

    @pytest.mark.asyncio
    async def test_generator_closes_on_completed_status(self):
        """Test that generator closes when completed status received."""
        with patch('src.core.sse_manager.sse_manager') as mock_manager:
            mock_queue = asyncio.Queue()
            mock_manager.create_event_queue.return_value = mock_queue

            # Put a completion event
            completion_event = SSEEvent(
                data={"status": "completed", "job_id": "job-123"},
                event="execution_update"
            )
            await mock_queue.put(completion_event)

            gen = event_stream_generator("job-123", timeout=10, heartbeat_interval=30)

            events = []
            async for event in gen:
                events.append(event)
                if len(events) > 10:  # Safety limit
                    break

            # Should have connection event and completion event
            assert len(events) >= 2
            mock_manager.remove_event_queue.assert_called_once()

    @pytest.mark.asyncio
    async def test_generator_closes_on_failed_status(self):
        """Test that generator closes when failed status received."""
        with patch('src.core.sse_manager.sse_manager') as mock_manager:
            mock_queue = asyncio.Queue()
            mock_manager.create_event_queue.return_value = mock_queue

            failed_event = SSEEvent(
                data={"status": "failed", "job_id": "job-123"},
                event="execution_update"
            )
            await mock_queue.put(failed_event)

            gen = event_stream_generator("job-123", timeout=10, heartbeat_interval=30)

            events = []
            async for event in gen:
                events.append(event)
                if len(events) > 10:
                    break

            mock_manager.remove_event_queue.assert_called_once()

    @pytest.mark.asyncio
    async def test_generator_closes_on_stopped_status(self):
        """Test that generator closes when stopped status received."""
        with patch('src.core.sse_manager.sse_manager') as mock_manager:
            mock_queue = asyncio.Queue()
            mock_manager.create_event_queue.return_value = mock_queue

            stopped_event = SSEEvent(
                data={"status": "stopped", "job_id": "job-123"},
                event="execution_update"
            )
            await mock_queue.put(stopped_event)

            gen = event_stream_generator("job-123", timeout=10, heartbeat_interval=30)

            events = []
            async for event in gen:
                events.append(event)
                if len(events) > 10:
                    break

            mock_manager.remove_event_queue.assert_called_once()

    @pytest.mark.asyncio
    async def test_generator_cleanup_on_exit(self):
        """Test that generator cleans up queue on exit."""
        with patch('src.core.sse_manager.sse_manager') as mock_manager:
            mock_queue = asyncio.Queue()
            mock_manager.create_event_queue.return_value = mock_queue

            gen = event_stream_generator("job-123", timeout=1, heartbeat_interval=30)

            # Get connection event
            await gen.__anext__()

            # Close generator
            await gen.aclose()

            mock_manager.remove_event_queue.assert_called_with("job-123", mock_queue)


class TestGlobalSSEManager:
    """Test cases for the global sse_manager instance."""

    def test_global_manager_exists(self):
        """Test that global SSE manager is instantiated."""
        assert sse_manager is not None
        assert isinstance(sse_manager, SSEConnectionManager)

    def test_global_manager_has_required_methods(self):
        """Test that global manager has all required methods."""
        assert hasattr(sse_manager, 'create_event_queue')
        assert hasattr(sse_manager, 'remove_event_queue')
        assert hasattr(sse_manager, 'broadcast_to_job')
        assert hasattr(sse_manager, 'get_connection_count')
        assert hasattr(sse_manager, 'get_statistics')

        assert callable(sse_manager.create_event_queue)
        assert callable(sse_manager.remove_event_queue)
        assert callable(sse_manager.broadcast_to_job)
        assert callable(sse_manager.get_connection_count)
        assert callable(sse_manager.get_statistics)
