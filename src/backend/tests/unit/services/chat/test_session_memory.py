"""A conversation is remembered whichever path answered it.

The light agent remembered its own turns (``chat/service.py``), but a turn
routed to a published crew or flow is answered by that capability — the light
agent never runs, so those exchanges were remembered nowhere. Only the crew's
task output was stored. The conversation itself survived solely as
``flow_states.messages``: checkpoint JSON reachable by one derived thread id
(``uuid5(group:session:flow)``), never embedded. A new session meant a new
thread and nothing said before could be recalled.

``save_message`` is the one funnel every assistant answer passes through,
whichever path produced it, so the record is written there. It is also the
honest definition of "the turn completed" — an answer not saved to the session
did not happen as far as the user is concerned.

The filtering matters as much as the write. A single turn posts many ACTIVITY
cards (crew started, checkpoint saved, restored from an earlier turn); four
observed turns produced 22 of those against 5 real answers, and remembering them
would bury what was actually said under "Checkpoint saved / turn_end".
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.chat.history import ChatHistoryService, _is_activity_card


class _Ctx:
    group_ids = ["acme_corp"]
    primary_group_id = "acme_corp"
    group_email = "user@example.com"
    access_token = None


def _service():
    service = ChatHistoryService(session=MagicMock())
    service.repository = MagicMock()
    service.repository.create = AsyncMock()
    service.repository.get_recent_by_session_and_group = AsyncMock(
        return_value=[
            SimpleNamespace(message_type="user", content="what are the frameworks?"),
            SimpleNamespace(message_type="assistant", content="[ui-card]"),
        ]
    )
    service.session_repository = MagicMock()
    service.session_repository.touch = AsyncMock()
    return service


async def _save(service, **kwargs):
    """Save a message and let the detached memory task finish."""
    defaults = dict(
        session_id="s1",
        user_id="u1",
        message_type="assistant",
        content="- LangGraph\n- CrewAI",
        group_context=_Ctx(),
    )
    defaults.update(kwargs)
    result = await service.save_message(**defaults)
    await asyncio.sleep(0)  # let the ensure_future task run
    await asyncio.sleep(0)
    return result


class TestActivityCardsAreNotAnswers:
    def test_the_ui_card_content_is_a_card(self):
        assert _is_activity_card("[ui-card]", None) is True

    def test_a_trace_envelope_is_a_card(self):
        """The shape a checkpoint/restore card actually carries."""
        payload = {
            "__chatmode": {
                "resultType": "trace",
                "resultData": {"label": "Checkpoint saved", "sublabel": "turn_end"},
            }
        }
        assert _is_activity_card("anything", payload) is True

    def test_a_real_answer_is_not_a_card(self):
        assert _is_activity_card("- LangGraph\n- CrewAI", None) is False

    def test_an_answer_with_a_non_trace_envelope_is_not_a_card(self):
        payload = {"__chatmode": {"resultType": "generation_complete"}}
        assert _is_activity_card("the answer", payload) is False


class TestWhatGetsRemembered:
    @pytest.mark.asyncio
    async def test_an_answer_is_remembered_with_its_question(self):
        """An answer alone is a statement with no subject — "the official
        website is https://ag2.ai" recalls usefully only next to what was
        asked."""
        with (
            patch(
                "src.services.memory.crew_memory.build_session_memory",
                new=AsyncMock(return_value=MagicMock()),
            ),
            patch("src.services.memory.hooks.remember_async") as remember,
        ):
            await _save(_service())

        text = remember.call_args.args[1]
        assert "what are the frameworks?" in text
        assert "- LangGraph" in text
        assert remember.call_args.kwargs["source"] == "chat"
        assert remember.call_args.kwargs["metadata"]["session_id"] == "s1"

    @pytest.mark.asyncio
    async def test_an_activity_card_is_not_remembered(self):
        with (
            patch(
                "src.services.memory.crew_memory.build_session_memory",
                new=AsyncMock(return_value=MagicMock()),
            ),
            patch("src.services.memory.hooks.remember_async") as remember,
        ):
            await _save(_service(), content="[ui-card]")

        remember.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_user_message_is_not_remembered_on_its_own(self):
        """The exchange is the unit; the question is recorded with its answer."""
        with (
            patch(
                "src.services.memory.crew_memory.build_session_memory",
                new=AsyncMock(return_value=MagicMock()),
            ),
            patch("src.services.memory.hooks.remember_async") as remember,
        ):
            await _save(_service(), message_type="user", content="a question")

        remember.assert_not_called()

    @pytest.mark.asyncio
    async def test_an_empty_answer_is_not_remembered(self):
        with (
            patch(
                "src.services.memory.crew_memory.build_session_memory",
                new=AsyncMock(return_value=MagicMock()),
            ),
            patch("src.services.memory.hooks.remember_async") as remember,
        ):
            await _save(_service(), content="   ")

        remember.assert_not_called()


class TestItNeverCostsTheAnswer:
    @pytest.mark.asyncio
    async def test_the_message_is_saved_even_with_memory_unavailable(self):
        """Memory being down must not fail a message already persisted."""
        service = _service()
        with patch(
            "src.services.memory.crew_memory.build_session_memory",
            new=AsyncMock(side_effect=RuntimeError("backend down")),
        ):
            result = await _save(service)

        assert result.content == "- LangGraph\n- CrewAI"
        service.repository.create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_nothing_is_written_without_a_tenant(self):
        """Scope is the tenant boundary; a record with no group has nowhere to
        go that is not someone else's store."""
        with (
            patch(
                "src.services.memory.crew_memory.build_session_memory",
                new=AsyncMock(return_value=MagicMock()),
            ) as build,
            patch("src.services.memory.hooks.remember_async"),
        ):
            await _save(_service(), group_context=None)

        build.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_disabled_memory_backend_writes_nothing(self):
        """`build_session_memory` returns None for the 'Disabled Configuration'."""
        with (
            patch(
                "src.services.memory.crew_memory.build_session_memory",
                new=AsyncMock(return_value=None),
            ),
            patch("src.services.memory.hooks.remember_async") as remember,
        ):
            await _save(_service())

        remember.assert_not_called()
