"""Keeping a long conversation affordable, and noticing when two turns collide.

Truncation kept the newest 100 messages and dropped the rest, so a long thread
forgot what it had been asked to do first. Folding the old turns into a summary
channel keeps the facts and still bounds the size — using the summarizer chat
already has, so the two cannot drift.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.services.flow_builder.conversation.lifecycle import (
    SUMMARY_CHANNEL,
    detect_concurrent_turn,
    fold_thread_history,
    note_base_checkpoint,
    render_transcript,
    split_for_fold,
)
from src.services.flow_builder.conversation.state_model import build_state_model
from src.services.flow_builder.conversation.turn import ConversationState


def messages(count):
    return [
        {"role": "user" if i % 2 == 0 else "assistant", "content": f"m{i}"}
        for i in range(count)
    ]


def state_with_summary(message_count=0):
    model = build_state_model(
        {"properties": {SUMMARY_CHANNEL: {}}}, base=ConversationState
    )
    state = model()
    if message_count:
        state.messages = messages(message_count)
    return state


class TestSplit:
    def test_a_short_conversation_folds_nothing(self):
        # Nothing folds until there is more than the verbatim window, so a short
        # thread never pays for a summarizer call.
        to_fold, verbatim = split_for_fold(messages(5), keep=20)

        assert to_fold == []
        assert len(verbatim) == 5

    def test_the_newest_stay_verbatim(self):
        to_fold, verbatim = split_for_fold(messages(30), keep=20)

        assert len(to_fold) == 10
        assert [m["content"] for m in verbatim][:1] == ["m10"]

    def test_a_missing_conversation_is_not_an_error(self):
        assert split_for_fold(None) == ([], [])


class TestTranscript:
    def test_roles_and_content_are_rendered(self):
        text = render_transcript(
            [{"role": "user", "content": "hi"}, {"role": "assistant", "content": "yo"}]
        )

        assert text == "user: hi\nassistant: yo"

    def test_empty_entries_are_skipped(self):
        assert render_transcript([{"role": "user", "content": "  "}]) == ""


class TestFold:
    @pytest.mark.asyncio
    async def test_old_turns_become_a_summary(self):
        state = state_with_summary(30)

        with patch(
            "src.services.llm.manager.LLMManager.completion",
            new=AsyncMock(
                return_value={
                    "choices": [{"message": {"content": "they discussed news"}}]
                }
            ),
        ):
            folded = await fold_thread_history(state, keep=20)

        assert folded is True
        assert state.summary == "they discussed news"
        assert len(state.messages) == 20

    @pytest.mark.asyncio
    async def test_nothing_to_fold_is_a_no_op(self):
        state = state_with_summary(5)

        assert await fold_thread_history(state, keep=20) is False

    @pytest.mark.asyncio
    async def test_a_flow_without_a_summary_channel_does_not_fold(self):
        # There would be nowhere to put the folded text, and dropping it is the
        # truncation this exists to replace.
        model = build_state_model({}, base=ConversationState)
        state = model()
        state.messages = messages(30)

        assert await fold_thread_history(state, keep=20) is False
        assert len(state.messages) == 30

    @pytest.mark.asyncio
    async def test_a_summarizer_failure_leaves_the_history_alone(self):
        # Never fail a turn the user already has an answer from; the next turn
        # tries again.
        state = state_with_summary(30)

        with patch(
            "src.services.llm.manager.LLMManager.completion",
            new=AsyncMock(side_effect=RuntimeError("model down")),
        ):
            folded = await fold_thread_history(state, keep=20)

        assert folded is False
        assert len(state.messages) == 30

    @pytest.mark.asyncio
    async def test_the_kill_switch_is_honoured(self):
        state = state_with_summary(30)

        with patch(
            "src.services.chat.context_compaction.compaction_enabled",
            return_value=False,
        ):
            assert await fold_thread_history(state, keep=20) is False


class TestConcurrentTurns:
    def test_a_thread_that_moved_under_us_is_reported(self):
        # Two messages sent quickly both restore the same checkpoint, both run,
        # and the slower save overwrites the faster — which looks exactly like
        # the flow ignoring a message.
        state = SimpleNamespace(id="thread-1")
        note_base_checkpoint(state, 10)

        collision = detect_concurrent_turn(state, latest_checkpoint_id=14)

        assert collision is not None
        assert "thread-1" in collision

    def test_an_unmoved_thread_reports_nothing(self):
        state = SimpleNamespace(id="thread-1")
        note_base_checkpoint(state, 10)

        assert detect_concurrent_turn(state, latest_checkpoint_id=10) is None

    def test_a_turn_that_recorded_no_base_cannot_detect(self):
        assert detect_concurrent_turn(SimpleNamespace(id="t"), 14) is None
