"""Streaming a run — the frames, and the two framings over them.

The run is walked once and encoded per caller, so these check the FRAMES (what a
caller learns and when) separately from the encodings (NDJSON vs SSE).
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.services.external import streaming
from src.services.external.identity import ExternalCaller
from src.services.external.interaction import PendingInteraction
from src.services.external.invocation import InvocationResult
from src.services.external.state import ExternalTaskState


class _Ctx:
    group_ids = ["acme_corp"]
    group_email = "caller@example.com"
    access_token = "tok"
    user_role = "admin"
    highest_role = "admin"
    current_user = None
    primary_group_id = "acme_corp"


def _caller():
    return ExternalCaller(
        group_context=_Ctx(), protocol="mcp", identifier="caller@example.com"
    )


def _statuses(*results):
    """run_status returns each of these in turn."""
    return patch(
        "src.services.external.streaming.run_status",
        new=AsyncMock(side_effect=list(results)),
    )


def _no_pending():
    return patch(
        "src.services.external.streaming.interaction.pending_for_run",
        new=AsyncMock(return_value=[]),
    )


async def _collect(caller=None, run_id="run-1", **kwargs):
    frames = []
    async for frame in streaming.stream_run(
        caller or _caller(), run_id, poll_interval=0, **kwargs
    ):
        frames.append(frame)
    return frames


class TestFrames:
    @pytest.mark.asyncio
    async def test_emits_on_change_not_on_every_poll(self):
        """A caller reading an identical line every second for a ten-minute run
        learns nothing from the 599 duplicates, and the noise buries the
        transitions that matter."""
        working = InvocationResult(run_id="run-1", state=ExternalTaskState.WORKING)
        done = InvocationResult(
            run_id="run-1", state=ExternalTaskState.COMPLETED, output="ok"
        )
        with _statuses(working, working, working, done), _no_pending():
            frames = await _collect()

        assert [f["state"] for f in frames] == ["working", "completed"]

    @pytest.mark.asyncio
    async def test_stops_at_a_terminal_state(self):
        done = InvocationResult(run_id="run-1", state=ExternalTaskState.COMPLETED)
        with _statuses(done), _no_pending():
            frames = await _collect()
        assert len(frames) == 1

    @pytest.mark.asyncio
    async def test_the_terminal_frame_carries_the_output_and_artifact(self):
        done = InvocationResult(
            run_id="run-1", state=ExternalTaskState.COMPLETED, output="the answer"
        )
        with _statuses(done), _no_pending():
            frames = await _collect()

        assert frames[-1]["output"] == "the answer"
        assert frames[-1]["artifact"]["parts"][0]["text"] == "the answer"

    @pytest.mark.asyncio
    async def test_a_paused_run_streams_the_question_inline(self):
        """So a streaming caller can answer without switching call shapes."""
        working = InvocationResult(run_id="run-1", state=ExternalTaskState.WORKING)
        done = InvocationResult(
            run_id="run-1", state=ExternalTaskState.COMPLETED, output="shipped"
        )
        # Paused on the first poll, answered and finished by the second.
        with (
            _statuses(working, done),
            patch(
                "src.services.external.streaming.interaction.pending_for_run",
                new=AsyncMock(
                    side_effect=[
                        [PendingInteraction(approval_id=1, prompt="Ship it?")],
                        [],
                    ]
                ),
            ),
        ):
            frames = await _collect()

        assert frames[0]["state"] == "input_required"
        assert frames[0]["waiting_for"][0]["prompt"] == "Ship it?"
        assert frames[-1]["state"] == "completed"


class TestFailureModes:
    @pytest.mark.asyncio
    async def test_an_error_becomes_a_frame_not_an_exception(self):
        """Once a chunked response has begun the status code is already sent, so
        raising mid-body is an unexplained truncation to the caller."""
        with patch(
            "src.services.external.streaming.run_status",
            new=AsyncMock(side_effect=RuntimeError("db went away")),
        ):
            frames = await _collect()

        assert frames[-1]["state"] == "failed"
        assert "db went away" in frames[-1]["error"]

    @pytest.mark.asyncio
    async def test_a_run_the_caller_may_not_see_ends_the_stream(self):
        with _statuses(None), _no_pending():
            frames = await _collect(run_id="someone-elses")
        assert frames == [
            {"run_id": "someone-elses", "state": "failed", "error": "No such run"}
        ]

    @pytest.mark.asyncio
    async def test_the_ceiling_says_so_rather_than_closing_silently(self):
        """A body that just ends is indistinguishable from a dropped
        connection."""
        working = InvocationResult(run_id="run-1", state=ExternalTaskState.WORKING)
        with (
            patch(
                "src.services.external.streaming.run_status",
                new=AsyncMock(return_value=working),
            ),
            _no_pending(),
        ):
            frames = await _collect(max_seconds=0.0)

        assert "still going" in frames[-1]["detail"]


class TestEncodings:
    @pytest.mark.asyncio
    async def test_ndjson_is_one_object_per_line(self):
        import json

        async def _frames():
            yield {"run_id": "r", "state": "working"}
            yield {"run_id": "r", "state": "completed"}

        chunks = [c async for c in streaming.to_ndjson(_frames())]
        assert all(c.endswith(b"\n") for c in chunks)
        assert json.loads(chunks[0])["state"] == "working"

    @pytest.mark.asyncio
    async def test_sse_frames_are_single_line(self):
        """A bare newline inside a data: field terminates the event, so
        pretty-printed JSON would silently corrupt the framing."""

        async def _frames():
            yield {"run_id": "r", "state": "working", "nested": {"a": 1}}

        chunk = [c async for c in streaming.to_sse(_frames())][0].decode()
        assert chunk.startswith("data: ")
        assert chunk.endswith("\n\n")
        assert chunk.count("\n") == 2  # only the two terminating newlines

    @pytest.mark.asyncio
    async def test_both_encodings_carry_the_same_content(self):
        """A caller picks a wire format, not a feature."""
        import json

        frame = {"run_id": "r", "state": "completed", "output": "x"}

        async def _one():
            yield dict(frame)

        nd = json.loads([c async for c in streaming.to_ndjson(_one())][0])
        sse = json.loads(
            [c async for c in streaming.to_sse(_one())][0].decode()[len("data: ") :]
        )
        assert nd == sse == frame
