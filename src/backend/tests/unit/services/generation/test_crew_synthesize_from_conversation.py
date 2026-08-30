"""Unit tests for ChatMode answer-mode "Save to catalog" crew synthesis.

Covers ``CrewGenerationService.synthesize_crew_from_conversation`` and its
``_build_conversation_transcript`` helper: distilling a reusable, MULTI-STEP
crew from the USER's own requests (gather info → build dashboard → …). The
assistant's replies are deliberately never sent to the LLM.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.core.exceptions import BadRequestError
from src.services.generation.crews import CrewGenerationService


def _build_service():
    """A CrewGenerationService with external deps mocked out."""
    with (
        patch("src.services.generation.crews.LLMLogService"),
        patch("src.services.generation.crews.LLMLogRepository"),
        patch("src.services.generation.crews.CrewGeneratorRepository"),
    ):
        return CrewGenerationService(Mock())


def _gc(group_ids=("g1",), primary="g1"):
    # The service only reads group_ids / primary_group_id via getattr, so a
    # lightweight namespace decouples these tests from the GroupContext schema.
    return SimpleNamespace(group_ids=list(group_ids), primary_group_id=primary)


def _msg(mtype, content):
    return SimpleNamespace(message_type=mtype, content=content)


def _patch_history(messages):
    """Stub ChatHistoryService.get_recent_messages to return ``messages``.

    Chat history is that service's domain; this used to patch
    ChatHistoryRepository, because generation constructed it directly.
    """
    return patch(
        "src.services.chat.history.ChatHistoryService.get_recent_messages",
        new=AsyncMock(return_value=messages),
    )


class TestBuildConversationTranscript:
    @pytest.mark.asyncio
    async def test_returns_empty_without_session(self):
        svc = _build_service()
        assert await svc._build_conversation_transcript("", _gc()) == ""

    @pytest.mark.asyncio
    async def test_returns_empty_without_group(self):
        svc = _build_service()
        out = await svc._build_conversation_transcript(
            "s1", _gc(group_ids=(), primary=None)
        )
        assert out == ""

    @pytest.mark.asyncio
    async def test_includes_only_the_users_own_turns_in_order(self):
        svc = _build_service()
        msgs = [
            _msg("user", "gather oil data"),
            _msg("assistant", "here is the data"),
            _msg("user", "now build a dashboard"),
        ]
        with _patch_history(msgs):
            out = await svc._build_conversation_transcript("s1", _gc())
        assert "User: gather oil data" in out
        assert "User: now build a dashboard" in out
        # The assistant's replies never reach the transcript.
        assert "Assistant" not in out and "here is the data" not in out
        # Chronological order preserved (step 1 before step 2).
        assert out.index("gather oil data") < out.index("now build a dashboard")

    @pytest.mark.asyncio
    async def test_skips_placeholders_and_non_chat_roles(self):
        svc = _build_service()
        msgs = [
            _msg("user", "real ask"),
            _msg("assistant", "Thinking..."),
            _msg("assistant", "[ui-card]"),
            _msg("system", "ignored"),
            _msg("assistant", "   "),  # blank after strip
        ]
        with _patch_history(msgs):
            out = await svc._build_conversation_transcript("s1", _gc())
        assert out == "User: real ask"

    @pytest.mark.asyncio
    async def test_huge_assistant_outputs_never_reach_the_transcript(self):
        """A 20KB deck or report is deliverable bytes, not workflow — and it may
        carry third-party content. Only the user's steps go to the LLM."""
        svc = _build_service()
        big = "x" * 5000
        msgs = [
            _msg("user", "step one gather"),
            _msg("assistant", big),
            _msg("user", "step two dashboard"),
            _msg("assistant", big),
        ]
        with _patch_history(msgs):
            out = await svc._build_conversation_transcript("s1", _gc())
        assert "User: step one gather" in out
        assert "User: step two dashboard" in out
        assert "x" * 50 not in out
        assert len(out) < 200

    @pytest.mark.asyncio
    async def test_a_very_long_user_turn_is_capped(self):
        svc = _build_service()
        with _patch_history([_msg("user", "y" * 5000)]):
            out = await svc._build_conversation_transcript("s1", _gc())
        assert out.startswith("User: " + "y" * 100)
        assert out.endswith("…")
        assert len(out) < 900

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_usable_entries(self):
        svc = _build_service()
        with _patch_history([_msg("assistant", "Thinking...")]):
            assert await svc._build_conversation_transcript("s1", _gc()) == ""


class TestSynthesizeCrewFromConversation:
    @pytest.mark.asyncio
    async def test_raises_when_no_conversation(self):
        svc = _build_service()
        with patch.object(
            svc,
            "_build_conversation_transcript",
            new_callable=AsyncMock,
            return_value="",
        ):
            with pytest.raises(BadRequestError):
                await svc.synthesize_crew_from_conversation("s1", _gc())

    @pytest.mark.asyncio
    async def test_distills_multistep_crew_via_create_crew_complete(self):
        svc = _build_service()
        transcript = "User: gather data\nUser: build dashboard"
        created = {"agents": [{"id": "a1"}], "tasks": [{"id": "t1"}, {"id": "t2"}]}
        with (
            patch.object(
                svc,
                "_build_conversation_transcript",
                new_callable=AsyncMock,
                return_value=transcript,
            ),
            patch.object(
                svc,
                "create_crew_complete",
                new_callable=AsyncMock,
                return_value=created,
            ) as ccc,
        ):
            result = await svc.synthesize_crew_from_conversation(
                "s1", _gc(), model="m1"
            )

        assert result is created
        ccc.assert_awaited_once()
        request = ccc.call_args.args[0]
        # The conversation is embedded and the model override is threaded through.
        assert transcript in request.prompt
        assert request.model == "m1"
        # Prompt steers toward a MULTI-step crew, not "ONE agent and ONE task".
        lowered = request.prompt.lower()
        assert "every distinct step" in lowered
        assert "single generic task" in lowered
        # And the prompt announces what it carries: the user's requests alone.
        assert "user requests:" in lowered
        assert "deliberately not included" in lowered
