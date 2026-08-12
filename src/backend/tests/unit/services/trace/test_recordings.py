"""Reading earlier runs' tool calls back as a replay cassette."""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.services.trace.recordings import ToolRecordingsService


def _row(
    job_id="run-1",
    tool="PerplexityTool",
    args="{'query': 'a'}",
    content="out",
    task="Research",
):
    return SimpleNamespace(
        job_id=job_id,
        created_at=datetime(2026, 8, 12, 7, 30),
        output={
            "content": content,
            "extra_data": {"tool_name": tool, "tool_args": args, "task_name": task},
        },
        trace_metadata={},
    )


def _service(rows):
    service = ToolRecordingsService(session=None)
    service.repository = SimpleNamespace(tool_recordings=AsyncMock(return_value=rows))
    return service


@pytest.mark.asyncio
async def test_the_since_bound_is_naive():
    """asyncpg REJECTS an aware datetime against TIMESTAMP WITHOUT TIME ZONE,
    and the caller swallows the error — so an aware bound here does not fail
    loudly, it makes replay silently never happen on Postgres."""
    service = _service([_row()])

    await service.cassette_for(
        group_ids=["g"],
        exclude_job_id="run-2",
        max_age_seconds=3600,
    )

    since = service.repository.tool_recordings.await_args.kwargs["since"]
    assert since.tzinfo is None


@pytest.mark.asyncio
async def test_only_the_most_recent_run_is_used():
    """Position only means something inside one run."""
    service = _service([_row(job_id="newest"), _row(job_id="older")])

    cassette = await service.cassette_for(
        group_ids=["g"],
        exclude_job_id="run-2",
        max_age_seconds=3600,
    )

    assert {r.job_id for r in cassette} == {"newest"}


@pytest.mark.asyncio
async def test_recordings_come_back_in_the_order_they_were_made():
    """The rows arrive newest-first; positions count forwards."""
    service = _service([_row(content="second"), _row(content="first")])

    cassette = await service.cassette_for(
        group_ids=["g"],
        exclude_job_id="run-2",
        max_age_seconds=3600,
    )

    assert [r.output for r in cassette] == ["first", "second"]


@pytest.mark.asyncio
async def test_every_tool_is_kept_not_only_the_replayable_ones():
    """Filtering here by tool name is what emptied the cassette on a real run:
    the names available to filter with are catalogue TITLES
    ("ScrapeWebsiteTool") while a recording carries the runtime name ("Read
    website content"). Whether a call may be replayed is the tool policy's
    call, per call."""
    service = _service([_row(tool="Read website content")])

    cassette = await service.cassette_for(
        group_ids=["g"], exclude_job_id="run-2", max_age_seconds=3600
    )

    assert [r.tool_name for r in cassette] == ["Read website content"]


@pytest.mark.asyncio
async def test_a_read_failure_is_no_cassette_rather_than_a_failed_run():
    service = ToolRecordingsService(session=None)
    service.repository = SimpleNamespace(
        tool_recordings=AsyncMock(side_effect=RuntimeError("database is down"))
    )

    assert (
        await service.cassette_for(
            group_ids=["g"],
            exclude_job_id="run-2",
            max_age_seconds=3600,
        )
        == []
    )


# ---------------------------------------------------------------------------
# The chat path records too, in its own shape
# ---------------------------------------------------------------------------


def _chat_row(
    job_id="chat-1", tool="PerplexityTool", args='{"query": "a"}', content="out"
):
    """What services/chat/service.py writes: no span, no extra_data, args as
    JSON under `input`, and an event_context that describes the generated task
    rather than the user's question."""
    return SimpleNamespace(
        job_id=job_id,
        created_at=datetime(2026, 8, 12, 9, 0),
        event_context="Respond directly and helpfully to the user's request.",
        output={
            "tool_name": tool,
            "input": args,
            "content": content,
            "duration_ms": 12,
        },
        trace_metadata={"agent_role": "Assistant", "tool_name": tool},
    )


@pytest.mark.asyncio
async def test_a_chat_row_is_a_recording():
    """Chat does not run the OTel bridge, so its calls are written directly —
    they are still finished calls with arguments and a result."""
    service = _service([_chat_row()])

    cassette = await service.cassette_for(
        group_ids=["g"], exclude_job_id="run-2", max_age_seconds=3600
    )

    assert len(cassette) == 1
    assert cassette[0].tool_name == "PerplexityTool"
    assert cassette[0].output == "out"


@pytest.mark.asyncio
async def test_json_args_and_repr_args_produce_the_same_key():
    """The two paths stringify arguments differently; a recording from one must
    be findable by a call made on the other."""
    from src.services.trace.recordings import canonical_args

    chat = await _service([_chat_row(args='{"query": "a", "n": 1}')]).cassette_for(
        group_ids=["g"], exclude_job_id="x", max_age_seconds=3600
    )

    assert chat[0].args_key == canonical_args("{'n': 1, 'query': 'a'}")


@pytest.mark.asyncio
async def test_a_chat_recording_is_filed_under_its_own_run():
    """So position cannot cross turns. event_context is the SAME constant on
    every chat row ("Respond directly and helpfully to the user's request."),
    so keying position on it would answer one question with another question's
    second search."""
    cassette = await _service([_chat_row(job_id="chat-9")]).cassette_for(
        group_ids=["g"], exclude_job_id="x", max_age_seconds=3600
    )

    assert cassette[0].task_name == "run:chat-9"
    assert "Respond directly" not in cassette[0].task_name


@pytest.mark.asyncio
async def test_a_crew_recording_still_uses_its_task():
    """The crew path has a real task, and position within it is meaningful."""
    cassette = await _service([_row(task="Research the market")]).cassette_for(
        group_ids=["g"], exclude_job_id="x", max_age_seconds=3600
    )

    assert cassette[0].task_name == "Research the market"
