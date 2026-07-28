"""
Extended unit tests for SSE manager to improve coverage.
"""
import asyncio
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.core.sse_manager import (
    _SSEEncoder,
    SSEEvent,
    SSEConnectionManager,
    event_stream_generator,
)
from uuid import UUID


class TestSSEEncoder:
    def test_encodes_uuid(self):
        """_SSEEncoder serializes UUID to string."""
        import json
        uid = UUID("12345678-1234-5678-1234-567812345678")
        result = json.dumps(uid, cls=_SSEEncoder)
        assert "12345678" in result

    def test_encodes_datetime(self):
        """_SSEEncoder serializes datetime to ISO string."""
        import json
        dt = datetime(2025, 1, 15, 10, 30, 0)
        result = json.dumps(dt, cls=_SSEEncoder)
        assert "2025" in result

    def test_raises_for_unknown_type(self):
        """_SSEEncoder raises TypeError for unknown types."""
        import json
        with pytest.raises(TypeError):
            json.dumps(object(), cls=_SSEEncoder)


class TestSSEEvent:
    def test_format_minimal(self):
        """format outputs minimal data line."""
        event = SSEEvent(data="hello")
        formatted = event.format()
        assert "data: hello" in formatted
        assert formatted.endswith("\n\n")

    def test_format_with_event_type(self):
        """format includes event type when set."""
        event = SSEEvent(data="msg", event="update")
        formatted = event.format()
        assert "event: update" in formatted

    def test_format_with_id_and_retry(self):
        """format includes id and retry when set."""
        event = SSEEvent(data="x", id="42", retry=3000)
        formatted = event.format()
        assert "id: 42" in formatted
        assert "retry: 3000" in formatted

    def test_format_dict_data_as_json(self):
        """format converts dict data to JSON."""
        event = SSEEvent(data={"status": "ok", "count": 1})
        formatted = event.format()
        assert '"status"' in formatted
        assert '"ok"' in formatted

    def test_format_list_data_as_json(self):
        """format converts list data to JSON."""
        event = SSEEvent(data=[1, 2, 3])
        formatted = event.format()
        assert "[1, 2, 3]" in formatted

    def test_format_multiline_data(self):
        """format splits multiline data into separate data: lines."""
        event = SSEEvent(data="line1\nline2")
        formatted = event.format()
        assert "data: line1" in formatted
        assert "data: line2" in formatted


class TestSSEConnectionManager:
    def test_create_event_queue_new_job(self):
        """create_event_queue creates a queue for new job_id."""
        manager = SSEConnectionManager()
        queue = manager.create_event_queue("job-1")
        assert isinstance(queue, asyncio.Queue)
        assert manager.connection_count == 1
        assert "job-1" in manager.job_queues

    def test_create_multiple_queues_same_job(self):
        """Multiple queues can be created for the same job."""
        manager = SSEConnectionManager()
        q1 = manager.create_event_queue("job-1")
        q2 = manager.create_event_queue("job-1")
        assert len(manager.job_queues["job-1"]) == 2
        assert manager.connection_count == 2

    def test_remove_event_queue(self):
        """remove_event_queue removes the queue and decrements count."""
        manager = SSEConnectionManager()
        queue = manager.create_event_queue("job-1")
        manager.remove_event_queue("job-1", queue)
        assert "job-1" not in manager.job_queues
        assert manager.connection_count == 0

    def test_remove_event_queue_nonexistent_job(self):
        """remove_event_queue does not raise for unknown job_id."""
        manager = SSEConnectionManager()
        queue = asyncio.Queue()
        manager.remove_event_queue("nonexistent", queue)  # should not raise

    def test_remove_event_queue_keeps_other_queues(self):
        """remove_event_queue only removes specified queue."""
        manager = SSEConnectionManager()
        q1 = manager.create_event_queue("job-1")
        q2 = manager.create_event_queue("job-1")
        manager.remove_event_queue("job-1", q1)
        assert "job-1" in manager.job_queues
        assert len(manager.job_queues["job-1"]) == 1
        assert manager.connection_count == 1

    @pytest.mark.asyncio
    async def test_broadcast_to_job_delivers_event(self):
        """broadcast_to_job puts event in all job queues."""
        manager = SSEConnectionManager()
        queue = manager.create_event_queue("job-1")
        event = SSEEvent(data={"status": "update"})

        count = await manager.broadcast_to_job("job-1", event)
        assert count == 1
        received = await queue.get()
        assert received.data == {"status": "update"}

    @pytest.mark.asyncio
    async def test_broadcast_to_job_no_listeners(self):
        """broadcast_to_job returns 0 when no listeners."""
        manager = SSEConnectionManager()
        event = SSEEvent(data="test")
        count = await manager.broadcast_to_job("no-listeners", event)
        assert count == 0

    @pytest.mark.asyncio
    async def test_broadcast_to_job_assigns_event_id(self):
        """broadcast_to_job assigns sequential event IDs."""
        manager = SSEConnectionManager()
        e1 = SSEEvent(data="a")
        e2 = SSEEvent(data="b")
        await manager.broadcast_to_job("job-1", e1)
        await manager.broadcast_to_job("job-1", e2)
        assert int(e1.id) < int(e2.id)

    @pytest.mark.asyncio
    async def test_broadcast_also_delivers_to_global_stream(self):
        """broadcast_to_job also sends to all_groups_ subscribers."""
        manager = SSEConnectionManager()
        job_queue = manager.create_event_queue("job-1")
        global_queue = manager.create_event_queue("all_groups_user1")

        event = SSEEvent(data="event")
        count = await manager.broadcast_to_job("job-1", event)
        assert count == 2  # job subscriber + global subscriber

    @pytest.mark.asyncio
    async def test_broadcast_full_queue_drops_event(self):
        """broadcast_to_job drops events when queue is full."""
        manager = SSEConnectionManager()
        queue = manager.create_event_queue("job-full")
        # Fill the queue to capacity (maxsize=100)
        for i in range(100):
            await queue.put(SSEEvent(data=f"event-{i}"))

        event = SSEEvent(data="overflow")
        count = await manager.broadcast_to_job("job-full", event)
        # Should handle gracefully without raising
        assert count == 0

    def test_get_connection_count_global(self):
        """get_connection_count returns total connections."""
        manager = SSEConnectionManager()
        manager.create_event_queue("job-1")
        manager.create_event_queue("job-2")
        assert manager.get_connection_count() == 2

    def test_get_connection_count_for_specific_job(self):
        """get_connection_count returns count for specific job."""
        manager = SSEConnectionManager()
        manager.create_event_queue("job-1")
        manager.create_event_queue("job-1")
        manager.create_event_queue("job-2")
        assert manager.get_connection_count("job-1") == 2
        assert manager.get_connection_count("job-2") == 1
        assert manager.get_connection_count("nonexistent") == 0

    def test_get_statistics(self):
        """get_statistics returns connection summary."""
        manager = SSEConnectionManager()
        manager.create_event_queue("job-1")
        manager.create_event_queue("job-1")
        stats = manager.get_statistics()
        assert stats["total_connections"] == 2
        assert "job-1" in stats["active_jobs"]
        assert stats["connections_per_job"]["job-1"] == 2

    @pytest.mark.asyncio
    async def test_get_replay_events_after_id(self):
        """get_replay_events returns only events after the specified ID."""
        manager = SSEConnectionManager()
        manager.create_event_queue("job-1")

        e1 = SSEEvent(data="first")
        e2 = SSEEvent(data="second")
        e3 = SSEEvent(data="third")
        await manager.broadcast_to_job("job-1", e1)
        await manager.broadcast_to_job("job-1", e2)
        await manager.broadcast_to_job("job-1", e3)

        id1 = int(e1.id)
        replayed = manager.get_replay_events("job-1", id1)
        assert len(replayed) == 2
        assert e2 in replayed
        assert e3 in replayed

    @pytest.mark.asyncio
    async def test_get_replay_events_global_stream(self):
        """get_replay_events uses global buffer for all_groups_ streams."""
        manager = SSEConnectionManager()
        manager.create_event_queue("job-1")
        manager.create_event_queue("all_groups_test")

        e1 = SSEEvent(data="event-a")
        await manager.broadcast_to_job("job-1", e1)

        id0 = int(e1.id) - 1
        replayed = manager.get_replay_events("all_groups_test", id0)
        assert len(replayed) >= 1


class TestEventStreamGenerator:
    @pytest.mark.asyncio
    async def test_generator_yields_connected_event(self):
        """event_stream_generator yields connected event immediately."""
        from src.core.sse_manager import sse_manager

        events = []
        gen = event_stream_generator("test-job-1234", timeout=1, heartbeat_interval=9999)
        # Just collect the first few events
        async for chunk in gen:
            events.append(chunk)
            if len(events) >= 2:
                break

        # Should get connected event and heartbeat
        assert any("connected" in e or "data:" in e for e in events)

    @pytest.mark.asyncio
    async def test_generator_replays_missed_events(self):
        """event_stream_generator replays missed events on reconnect."""
        from src.core.sse_manager import sse_manager

        # First broadcast some events
        sse_manager.create_event_queue("replay-test")
        evt = SSEEvent(data={"msg": "missed"})
        await sse_manager.broadcast_to_job("replay-test", evt)
        last_id = int(evt.id) - 1  # pretend we missed this event

        chunks = []
        gen = event_stream_generator(
            "replay-test", timeout=1, heartbeat_interval=9999, last_event_id=last_id
        )
        async for chunk in gen:
            chunks.append(chunk)
            if len(chunks) >= 3:
                break

        # Should contain the replayed event data
        all_output = "".join(chunks)
        assert "missed" in all_output

    @pytest.mark.asyncio
    async def test_generator_replays_buffered_events_on_fresh_connect(self):
        """Regression: a fast producer can emit its WHOLE sequence before any
        subscriber connects (a crew generation finishing in ~60ms broadcasts to
        zero listeners). A FRESH connect (no Last-Event-ID) must replay the
        per-job buffer so the late subscriber still receives the result instead
        of hanging on an empty queue ('Thinking...' forever)."""
        from src.core.sse_manager import sse_manager

        # Producer emits BEFORE any subscriber exists (no create_event_queue).
        done = SSEEvent(
            data={"type": "generation_complete", "agents": []},
            event="generation_complete",
        )
        await sse_manager.broadcast_to_job("fast-gen-race", done)

        chunks = []
        gen = event_stream_generator(
            "fast-gen-race", timeout=1, heartbeat_interval=9999, last_event_id=None
        )
        async for chunk in gen:
            chunks.append(chunk)
            if len(chunks) >= 3:
                break

        # The completed-before-subscribe event is replayed on the fresh connect.
        assert "generation_complete" in "".join(chunks)

    @pytest.mark.asyncio
    async def test_generator_fresh_connect_skips_global_stream_replay(self):
        """A fresh connect to an 'all_groups_' stream must NOT replay the global
        buffer (unrelated cross-job history) — only reconnects (Last-Event-ID)
        replay there. Guards against flooding a freshly loaded page."""
        from src.core.sse_manager import sse_manager

        # Put an event in the global buffer via a normal job broadcast.
        await sse_manager.broadcast_to_job(
            "some-job", SSEEvent(data={"msg": "old-global"})
        )

        chunks = []
        gen = event_stream_generator(
            "all_groups_userX", timeout=1, heartbeat_interval=9999, last_event_id=None
        )
        async for chunk in gen:
            chunks.append(chunk)
            if len(chunks) >= 2:
                break

        # Fresh all_groups_ connect yields connected/heartbeat, not old history.
        assert "old-global" not in "".join(chunks)
