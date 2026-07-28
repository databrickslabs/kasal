"""A2A streaming.

The card now promises ``streaming: true``, so these tests exist to keep that
promise honest: the events have to be the spec's shape, ``final`` has to be
right, and the SSE framing has to survive payloads containing newlines.
"""

import json
from unittest.mock import AsyncMock, patch

import pytest

from src.services.a2a.a2a_server import stream as a2a_stream
from src.services.a2a.a2a_server.render import to_stream_events
from src.services.external.identity import ExternalCaller


class _Ctx:
    group_ids = ["acme_corp"]
    group_email = "agent@example.com"
    access_token = "tok"
    user_role = "operator"
    highest_role = "operator"
    current_user = None
    primary_group_id = "acme_corp"


def _caller(role="operator"):
    ctx = _Ctx()
    ctx.user_role = role
    ctx.highest_role = role
    return ExternalCaller(
        group_context=ctx, protocol="a2a", identifier="agent@example.com"
    )


async def _collect(frames):
    async def _gen(*args, **kwargs):
        for frame in frames:
            yield frame

    with patch("src.services.a2a.a2a_server.stream.stream_run", new=_gen):
        return [
            chunk
            async for chunk in a2a_stream.stream_task(_caller(), "run-1", session=None)
        ]


def _events(chunks):
    """SSE bytes -> the decoded payloads, parsed the way a client would."""
    out = []
    for chunk in chunks:
        text = chunk.decode()
        assert text.endswith("\n\n"), "an event must be terminated by a blank line"
        name = None
        for line in text.strip().split("\n"):
            if line.startswith("event: "):
                name = line[7:]
            elif line.startswith("data: "):
                out.append((name, json.loads(line[6:])))
    return out


class TestEventShape:
    def test_a_status_change_becomes_a_status_update_event(self):
        events = to_stream_events({"run_id": "r1", "state": "working"})
        assert [e.kind for e in events] == ["status-update"]
        assert events[0].status.state == "TASK_STATE_WORKING"
        assert events[0].taskId == "r1"

    def test_output_arrives_as_a_separate_artifact_event(self):
        """The spec keeps them apart so a subscriber that only wants results is
        not forced to parse every status change."""
        events = to_stream_events(
            {
                "run_id": "r1",
                "state": "completed",
                "artifact": {"parts": [{"kind": "text", "text": "done"}]},
            }
        )
        assert [e.kind for e in events] == ["status-update", "artifact-update"]
        assert events[1].artifact.parts[0].text == "done"

    def test_a_terminal_frame_is_marked_final(self):
        """Without it a client cannot tell "still working" from "that was the
        last thing you will hear", and holds the connection open forever."""
        assert to_stream_events({"run_id": "r", "state": "completed"})[0].final is True
        assert to_stream_events({"run_id": "r", "state": "failed"})[0].final is True
        assert to_stream_events({"run_id": "r", "state": "canceled"})[0].final is True

    def test_a_non_terminal_frame_is_not_final(self):
        assert to_stream_events({"run_id": "r", "state": "working"})[0].final is False
        assert (
            to_stream_events({"run_id": "r", "state": "input_required"})[0].final
            is False
        )

    def test_a_paused_run_streams_the_question_it_is_waiting_on(self):
        """INPUT_REQUIRED is only actionable if the question travels with it —
        otherwise the caller has to make a second call to learn what to answer."""
        events = to_stream_events(
            {
                "run_id": "r1",
                "state": "input_required",
                "waiting_for": [{"prompt": "Approve deleting the table?"}],
            }
        )
        assert events[0].status.message.parts[0].text == "Approve deleting the table?"

    def test_a_failure_streams_its_reason(self):
        events = to_stream_events(
            {"run_id": "r1", "state": "failed", "error": "connection refused"}
        )
        assert events[0].status.message.parts[0].text == "connection refused"


class TestSseFraming:
    @pytest.mark.asyncio
    async def test_each_event_is_named_and_terminated(self):
        chunks = await _collect(
            [
                {"run_id": "r1", "state": "working"},
                {"run_id": "r1", "state": "completed"},
            ]
        )
        assert [name for name, _ in _events(chunks)] == [
            "status-update",
            "status-update",
        ]

    @pytest.mark.asyncio
    async def test_a_newline_in_the_payload_does_not_split_the_event(self):
        """A bare newline inside ``data:`` terminates the event — a multi-line
        payload would silently truncate every message."""
        chunks = await _collect(
            [
                {
                    "run_id": "r1",
                    "state": "failed",
                    "error": "line one\nline two\nline three",
                }
            ]
        )
        assert len(chunks) == 1
        assert chunks[0].decode().count("data: ") == 1
        _, payload = _events(chunks)[0]
        assert payload["status"]["message"]["parts"][0]["text"].count("\n") == 2

    @pytest.mark.asyncio
    async def test_one_frame_can_emit_two_events(self):
        chunks = await _collect(
            [
                {
                    "run_id": "r1",
                    "state": "completed",
                    "artifact": {"parts": [{"kind": "text", "text": "ok"}]},
                }
            ]
        )
        assert [name for name, _ in _events(chunks)] == [
            "status-update",
            "artifact-update",
        ]

    @pytest.mark.asyncio
    async def test_nulls_are_not_sent(self):
        """A2A clients validate against the spec; a field present as null is not
        the same as absent, and strict parsers reject it."""
        chunks = await _collect([{"run_id": "r1", "state": "working"}])
        _, payload = _events(chunks)[0]
        assert "contextId" not in payload
        assert "message" not in payload["status"]


class TestAuthorisation:
    @pytest.mark.asyncio
    async def test_a_caller_without_a_run_role_is_refused_before_the_first_chunk(self):
        """A streaming response commits its status line before the body, so a
        check that ran mid-stream would return 200 with an error inside it."""
        from src.services.external.permissions import ExternalPermissionError

        gen = a2a_stream.stream_task(_caller(role="viewer"), "run-1", session=None)
        with pytest.raises(ExternalPermissionError):
            await gen.__anext__()

    @pytest.mark.asyncio
    async def test_streaming_does_not_reimplement_polling(self):
        """The frames come from the shared layer. If this ever stops being true,
        the two protocols can drift on when a run reports progress."""
        called = {}

        async def _gen(caller, run_id, session=None, **kwargs):
            called["run_id"] = run_id
            yield {"run_id": run_id, "state": "completed"}

        with patch("src.services.a2a.a2a_server.stream.stream_run", new=_gen):
            async for _ in a2a_stream.stream_task(_caller(), "run-9", session=None):
                pass

        assert called["run_id"] == "run-9"


class TestRouter:
    @pytest.mark.asyncio
    async def test_subscribe_resolves_the_task_before_streaming(self):
        """Otherwise an unknown or foreign task answers 200 with an empty body,
        which reads as "running, nothing yet" instead of "no such task"."""
        import importlib

        # ``from src.api import a2a_router`` resolves to the re-exported
        # APIRouter, not the module the handlers live on.
        a2a_router = importlib.import_module("src.api.a2a_router")
        from src.core.exceptions import NotFoundError
        from src.services.a2a.a2a_server.tasks import UnknownTaskError

        with patch.object(
            a2a_router.a2a_tasks,
            "get_task",
            new=AsyncMock(side_effect=UnknownTaskError("nope")),
        ):
            with pytest.raises(NotFoundError):
                await a2a_router.subscribe_to_task("run-1", _caller(), None)

    @pytest.mark.asyncio
    async def test_streaming_send_starts_the_task_before_returning_the_stream(self):
        """Start and subscribe in one call: as two, a fast task can finish in
        the window between them and the caller waits for events forever."""
        from types import SimpleNamespace

        import importlib

        # ``from src.api import a2a_router`` resolves to the re-exported
        # APIRouter, not the module the handlers live on.
        a2a_router = importlib.import_module("src.api.a2a_router")
        from src.schemas.a2a import Message, Part, SendMessageRequest

        started = AsyncMock(return_value=SimpleNamespace(id="run-7"))
        with patch.object(a2a_router.a2a_tasks, "send_message", new=started):
            response = await a2a_router.stream_message(
                SendMessageRequest(
                    message=Message(parts=[Part(kind="text", text="go")]),
                    skillId="s",
                ),
                _caller(),
                None,
            )

        started.assert_awaited()
        assert response.media_type == "text/event-stream"
        # A proxy that buffers turns a stream into one delivery at the end.
        assert response.headers["x-accel-buffering"] == "no"
