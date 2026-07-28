"""A2AAgentTool — the delegation loop as an agent experiences it.

The behaviour under test is mostly about what a CALLING MODEL is told: a tool
result is prompt text, so "the remote asked a question" and "the remote broke"
have to be distinguishable without the model having to parse anything.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.services.tools.a2a_agent_tool import A2AAgentTool


def _tool(**overrides):
    kwargs = {
        "agent_name": "Researcher",
        "interface_url": "https://remote.example.com/a2a/v1",
        "timeout_seconds": 30,
        "skills": [{"id": "research", "name": "Research", "description": "digs"}],
    }
    kwargs.update(overrides)
    return A2AAgentTool(**kwargs)


def _task(state, text=None, task_id="t-1"):
    task = {"id": task_id, "status": {"state": state}}
    if text:
        task["artifacts"] = [{"parts": [{"kind": "text", "text": text}]}]
    return task


def _client(send=None, get=None):
    return patch.multiple(
        "src.services.a2a.a2a_client.client",
        send_message=AsyncMock(return_value=send or {}),
        get_task=AsyncMock(side_effect=get) if get else AsyncMock(return_value={}),
    )


class TestDescription:
    def test_the_remotes_skills_are_in_the_description(self):
        """The calling model selects on the description; a generic "call an
        agent" tool would make it guess a name it has never seen."""
        tool = _tool()
        assert "research" in tool.description
        assert "digs" in tool.description
        assert tool.name == "Delegate to Researcher"

    def test_a_remote_with_no_advertised_skills_still_describes_itself(self):
        assert "Researcher" in _tool(skills=[]).description


class TestDelegation:
    @pytest.mark.asyncio
    async def test_a_completed_task_returns_its_output(self):
        with _client(send=_task("TASK_STATE_COMPLETED", "the answer")):
            assert await _tool()._delegate("go", None, None) == "the answer"

    @pytest.mark.asyncio
    async def test_it_polls_until_the_remote_finishes(self):
        polls = [
            _task("TASK_STATE_WORKING"),
            _task("TASK_STATE_COMPLETED", "eventually"),
        ]
        with (
            _client(send=_task("TASK_STATE_SUBMITTED"), get=polls),
            patch("asyncio.sleep", new=AsyncMock()),
        ):
            assert await _tool()._delegate("go", None, None) == "eventually"

    @pytest.mark.asyncio
    async def test_a_question_comes_back_with_the_task_id_to_answer_it(self):
        """The calling agent is already an agent: give it the question and it
        can drive the loop itself, rather than stalling the crew behind a human
        who never asked to be involved."""
        with _client(send=_task("TASK_STATE_INPUT_REQUIRED", "Which region?")):
            result = await _tool()._delegate("go", None, None)

        assert "Which region?" in result
        assert "t-1" in result
        assert "task_id" in result

    @pytest.mark.asyncio
    async def test_an_answer_continues_the_existing_remote_task(self):
        send = AsyncMock(return_value=_task("TASK_STATE_COMPLETED", "done"))
        with patch("src.services.a2a.a2a_client.client.send_message", new=send):
            await _tool()._delegate("EMEA", None, "t-1")
        assert send.await_args.kwargs["task_id"] == "t-1"

    @pytest.mark.asyncio
    async def test_a_failure_is_reported_as_text_not_raised(self):
        """A raise aborts the whole task; a failed delegation is information the
        calling agent can act on — try another skill, or do it itself."""
        with patch(
            "src.services.a2a.a2a_client.client.send_message",
            new=AsyncMock(side_effect=RuntimeError("remote is down")),
        ):
            result = _tool()._run(request="go")

        assert result.startswith("Error delegating to 'Researcher'")
        assert "remote is down" in result

    @pytest.mark.asyncio
    async def test_a_rejected_credential_says_who_can_fix_it(self):
        with _client(send=_task("TASK_STATE_AUTH_REQUIRED")):
            result = await _tool()._delegate("go", None, None)
        assert "admin" in result.lower()

    @pytest.mark.asyncio
    async def test_a_timeout_says_the_task_was_not_cancelled(self):
        """It is still running at the far end. Implying otherwise would have the
        calling agent redo work that is about to arrive."""
        with (
            _client(
                send=_task("TASK_STATE_WORKING"),
                get=[_task("TASK_STATE_WORKING")] * 50,
            ),
            patch("asyncio.sleep", new=AsyncMock()),
            patch("time.monotonic", side_effect=[0, 10_000, 10_001]),
        ):
            result = await _tool(timeout_seconds=1)._delegate("go", None, None)

        assert "not cancelled" in result
        assert "t-1" in result

    @pytest.mark.asyncio
    async def test_a_completed_task_with_no_output_says_so(self):
        """Rather than returning an empty string, which reads to a model as a
        tool that did nothing."""
        with _client(send=_task("TASK_STATE_COMPLETED")):
            result = await _tool()._delegate("go", None, None)
        assert "no output" in result


class TestGuards:
    def test_an_unconfigured_remote_refuses_before_calling_anything(self):
        assert "not configured" in _tool(interface_url="")._run(request="go")

    def test_an_empty_request_is_refused(self):
        assert "Error" in _tool()._run(request="")
