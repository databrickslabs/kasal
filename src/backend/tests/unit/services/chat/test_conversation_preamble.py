"""The light agent's transcript: the answer on screen is kept whole.

The preamble truncated EVERY assistant turn to a 240-char stub. Asked the same
question a third time (routed to answer from the conversation), the agent saw
only "…| Type | Open-" of the report it was meant to restate — so it researched
it again. The most recent answer is now pinned: whole under its own cap, and
outside the shared budget. Older answers stay stubs.
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.chat.conversation_preamble import build_conversation_preamble


def _msg(mtype, content):
    return SimpleNamespace(message_type=mtype, content=content)


def _history_patches(messages):
    @asynccontextmanager
    async def _fake_session():
        yield MagicMock(name="db_session")

    repo = MagicMock()
    repo.get_recent_by_session_and_group = AsyncMock(return_value=messages)
    return patch("src.db.session.routed_scoped_session", _fake_session), patch(
        "src.repositories.chat_history_repository.ChatHistoryRepository",
        MagicMock(return_value=repo),
    )


async def _preamble(messages, current_id="auto"):
    for i, message in enumerate(messages):
        message.id = f"message-{i}"
    if current_id == "auto":
        current_id = next(
            (m.id for m in reversed(messages) if m.message_type == "user"), None
        )
    config = SimpleNamespace(
        session_id="sess-1", inputs={"chat_user_message_id": current_id}
    )
    ctx = SimpleNamespace(group_ids=["g1"])
    p_sess, p_repo = _history_patches(messages)
    with p_sess, p_repo:
        return await build_conversation_preamble(config, ctx, "g1", lambda *_: None)


class TestTheAnswerOnScreenIsKeptWhole:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("current_id", [None, "not-saved-yet"])
    async def test_redesign_keeps_original_deck_when_current_user_is_missing(
        self, current_id
    ):
        outline = "How LLMs work for a 10-year-old. Slide 1: Words. Slide 25: Recap."
        out = await _preamble(
            [
                _msg("user", "Explain LLMs to a 10-year-old in up to 25 slides"),
                _msg("assistant", outline),
                _msg("assistant", "[ui-card]"),
            ],
            current_id=current_id,
        )
        assert "Explain LLMs to a 10-year-old" in out
        assert outline in out

    @pytest.mark.asyncio
    async def test_identified_turn_is_boundary_even_when_another_user_turn_follows(
        self,
    ):
        out = await _preamble(
            [
                _msg("user", "Explain LLMs to a 10-year-old"),
                _msg("assistant", "Original LLM presentation"),
                _msg("user", "Make the design nicer"),
                _msg("assistant", "partial current response"),
                _msg("user", "A later request"),
            ],
            current_id="message-2",
        )
        assert "Original LLM presentation" in out
        assert "Make the design nicer" not in out
        assert "partial current response" not in out
        assert "A later request" not in out

    @pytest.mark.asyncio
    async def test_most_recent_answer_is_whole_while_older_ones_are_stubs(self):
        old_report = "OLD " * 200  # 800 chars — over the 240 stub cap
        latest = "# Features\n" + ("| feature | detail |\n" * 300)  # ~6.6k chars
        out = await _preamble(
            [
                _msg("user", "gather crewAI agentic features"),
                _msg("assistant", old_report),
                _msg("user", "gather crewAI agentic features"),
                _msg("assistant", latest),
                _msg("user", "gather crewAI agentic features"),  # current turn
                _msg("assistant", "Thinking..."),
            ]
        )
        assert latest.strip() in out  # whole, not a stub
        assert "OLD " * 200 not in out  # the older answer is still a stub…
        assert "Assistant: " + "OLD " * 60 in out  # …but present
        assert out.count("User: gather crewAI agentic features") == 2

    @pytest.mark.asyncio
    async def test_a_ui_card_after_the_answer_does_not_steal_the_pin(self):
        report = "R" * 1000
        out = await _preamble(
            [
                _msg("user", "report please"),
                _msg("assistant", report),
                _msg("assistant", "[ui-card] {...}"),
                _msg("user", "as a table"),  # current turn
            ]
        )
        assert "Assistant: " + report in out

    @pytest.mark.asyncio
    async def test_the_pinned_answer_has_its_own_ceiling(
        self, engine_setting, monkeypatch
    ):
        """Whole, not unbounded: past the last-answer cap it is cut there —
        not at the 240 stub and not by the shared budget."""
        engine_setting("chat_history_last_answer_char_cap", 1000)
        out = await _preamble(
            [
                _msg("user", "make a deck"),
                _msg("assistant", "Y" * 5000),
                _msg("user", "shorter"),  # current turn
            ]
        )
        assert "Assistant: " + "Y" * 1000 + "…" in out
        assert "Y" * 1001 not in out

    @pytest.mark.asyncio
    async def test_the_budget_never_evicts_the_pinned_answer(
        self, engine_setting, monkeypatch
    ):
        """A tiny budget drops older assistant turns and then the oldest user
        turns — never the answer on screen."""
        engine_setting("chat_history_max_chars", 200)
        latest = "L" * 3000
        messages = [_msg("user", "my name is ada")]
        for i in range(6):
            messages.append(_msg("assistant", f"answer {i} " + "A" * 100))
            messages.append(_msg("user", f"question {i} " + "Q" * 100))
        messages.append(_msg("assistant", latest))
        messages.append(_msg("user", "again"))  # current turn
        out = await _preamble(messages)
        assert "Assistant: " + latest in out
        assert "answer 0" not in out
        assert len(out) - len(latest) <= 200 * 1.5 + 400
