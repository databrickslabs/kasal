"""SSE events stay inside the workspace that owns the job (audit F04).

The "stream-all" fan-out sent every job's events to every stream-all
subscriber, and the global replay buffer answered every stream-all reconnect
with everything. A job now has an owner; a stream-all subscription has the
groups it may see; fan-out, replay and a generation's terminal event all
filter on that.
"""

import pytest

from src.core.sse_manager import SSEConnectionManager, SSEEvent


def _drain(queue):
    out = []
    while not queue.empty():
        out.append(queue.get_nowait())
    return out


@pytest.mark.asyncio
async def test_fan_out_reaches_only_the_owning_workspaces_streams():
    m = SSEConnectionManager()
    stream_a = m.create_event_queue("all_groups_a", group_ids=["a"])
    stream_b = m.create_event_queue("all_groups_b", group_ids=["b"])
    per_job = m.create_event_queue("job-1")
    m.register_job_owner("job-1", "a")

    await m.broadcast_to_job("job-1", SSEEvent(data={"n": 1}, event="trace"))

    assert [e.data for e in _drain(stream_a)] == [{"n": 1}]
    assert _drain(stream_b) == []
    assert [e.data for e in _drain(per_job)] == [{"n": 1}]


@pytest.mark.asyncio
async def test_an_unowned_job_reaches_no_stream_all_subscriber():
    m = SSEConnectionManager()
    stream_a = m.create_event_queue("all_groups_a", group_ids=["a"])
    per_job = m.create_event_queue("job-x")
    await m.broadcast_to_job("job-x", SSEEvent(data={"n": 1}))
    assert _drain(stream_a) == []
    assert len(_drain(per_job)) == 1


@pytest.mark.asyncio
async def test_a_publisher_that_knows_the_group_registers_it():
    m = SSEConnectionManager()
    stream_b = m.create_event_queue("all_groups_b", group_ids=["b"])
    await m.broadcast_to_job("job-2", SSEEvent(data={"n": 2}), group_id="b")
    assert m.job_owner("job-2") == "b"
    assert [e.data for e in _drain(stream_b)] == [{"n": 2}]


@pytest.mark.asyncio
async def test_a_stream_all_subscription_without_groups_sees_nothing():
    m = SSEConnectionManager()
    bare = m.create_event_queue("all_groups_a")  # no groups declared
    m.register_job_owner("job-1", "a")
    await m.broadcast_to_job("job-1", SSEEvent(data={"n": 1}))
    assert _drain(bare) == []


@pytest.mark.asyncio
async def test_replay_on_reconnect_is_filtered_the_same_way():
    m = SSEConnectionManager()
    m.create_event_queue("all_groups_a", group_ids=["a"])
    m.create_event_queue("all_groups_b", group_ids=["b"])
    m.register_job_owner("job-a", "a")
    m.register_job_owner("job-b", "b")
    await m.broadcast_to_job("job-a", SSEEvent(data={"who": "a"}))
    await m.broadcast_to_job("job-b", SSEEvent(data={"who": "b"}))
    await m.broadcast_to_job("job-?", SSEEvent(data={"who": "nobody"}))

    assert [e.data for e in m.get_replay_events("all_groups_a", 0)] == [{"who": "a"}]
    assert [e.data for e in m.get_replay_events("all_groups_b", 0)] == [{"who": "b"}]
    # The per-job buffer is unchanged: that stream is checked at subscribe time.
    assert [e.data for e in m.get_replay_events("job-?", 0)] == [{"who": "nobody"}]


@pytest.mark.asyncio
async def test_a_generations_terminal_event_answers_only_to_its_workspace():
    m = SSEConnectionManager()
    m.register_job_owner("gen-1", "a")
    await m.broadcast_to_job(
        "gen-1", SSEEvent(data={"execution_id": "run-1"}, event="generation_complete")
    )
    assert m.get_terminal_event("gen-1", group_ids=["a"]) is not None
    assert m.get_terminal_event("gen-1", group_ids=["b"]) is None
    assert m.get_terminal_event("gen-unknown", group_ids=["a"]) is None


def test_the_groups_of_a_closed_stream_are_forgotten():
    m = SSEConnectionManager()
    q = m.create_event_queue("all_groups_a", group_ids=["a"])
    assert m._stream_groups["all_groups_a"] == frozenset({"a"})
    m.remove_event_queue("all_groups_a", q)
    assert "all_groups_a" not in m._stream_groups
