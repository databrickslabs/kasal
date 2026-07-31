"""The conversation the router reads, and what it may point at.

Without it the router judges every message as though nothing came before it, so
"what is this Aviation sector" is a plausible news request that runs a whole
crew to answer a question the text on screen already answers.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.services.chat.conversation_context import (
    TURN_CHAR_CAP,
    recent_turns,
    render_turns,
    turn_by_index,
)


def _row(role, content):
    return SimpleNamespace(message_type=role, content=content)


def _history(rows):
    repo = SimpleNamespace(get_recent_by_session_and_group=AsyncMock(return_value=rows))
    return patch(
        "src.repositories.chat_history_repository.ChatHistoryRepository",
        return_value=repo,
    )


class TestRecentTurns:
    @pytest.mark.asyncio
    async def test_keeps_the_last_exchange_when_the_current_turn_is_unwritten(self):
        # THE bug this module got wrong first time. Routing happens mid-turn and
        # chat_history is written when the turn ENDS, so the current user row
        # usually is not there yet. Dropping "everything from the last user row
        # onward" — the rule the light agent's preamble uses, where that row DOES
        # exist — threw away the previous exchange instead: the very answer the
        # follow-up is asking about. The router then saw a conversation with its
        # most recent turn missing and read the follow-up as a fresh request.
        with _history(
            [
                _row("user", "gather news on databricks"),
                _row("assistant", "# Databricks news"),
            ]
        ):
            turns = await recent_turns(
                None, "s1", ["g1"], exclude_message="what is this Aviation sector"
            )

        assert [t.content for t in turns] == [
            "gather news on databricks",
            "# Databricks news",
        ]

    @pytest.mark.asyncio
    async def test_drops_the_current_turn_when_it_HAS_been_written(self):
        with _history(
            [
                _row("user", "gather news on databricks"),
                _row("assistant", "# Databricks news"),
                _row("user", "what is this Aviation sector"),
            ]
        ):
            turns = await recent_turns(
                None, "s1", ["g1"], exclude_message="what is this Aviation sector"
            )

        assert [t.content for t in turns] == [
            "gather news on databricks",
            "# Databricks news",
        ]

    @pytest.mark.asyncio
    async def test_an_identical_question_asked_EARLIER_is_real_history(self):
        # Matching by content only removes a TRAILING row. The same question
        # asked before, with an answer after it, is context and must survive.
        with _history(
            [
                _row("user", "gather news"),
                _row("assistant", "# The first answer"),
            ]
        ):
            turns = await recent_turns(
                None, "s1", ["g1"], exclude_message="gather news"
            )

        assert [t.content for t in turns] == ["gather news", "# The first answer"]

    @pytest.mark.asyncio
    async def test_drops_the_placeholder_rows_a_run_writes(self):
        with _history(
            [
                _row("user", "gather news"),
                _row("assistant", "Thinking..."),
                _row("assistant", "[ui-card]"),
                _row("assistant", "# The answer"),
            ]
        ):
            turns = await recent_turns(None, "s1", ["g1"], exclude_message="next")

        assert [t.content for t in turns] == ["gather news", "# The answer"]

    @pytest.mark.asyncio
    async def test_numbers_answers_after_the_window_is_chosen(self):
        # An index the router quotes has to match what it was SHOWN, so the
        # numbering cannot be assigned before the window is trimmed.
        rows = [_row("assistant", f"answer {i}") for i in range(10)]
        with _history(rows):
            turns = await recent_turns(None, "s1", ["g1"], limit=3)

        assert [t.index for t in turns] == [1, 2, 3]
        assert [t.content for t in turns] == ["answer 7", "answer 8", "answer 9"]

    @pytest.mark.asyncio
    async def test_no_session_or_no_group_means_no_context(self):
        assert await recent_turns(None, None, ["g1"]) == []
        assert await recent_turns(None, "s1", []) == []

    @pytest.mark.asyncio
    async def test_a_failed_read_is_no_context_not_an_error(self):
        # Routing must never break because history is unavailable.
        repo = SimpleNamespace(
            get_recent_by_session_and_group=AsyncMock(side_effect=RuntimeError("db"))
        )
        with patch(
            "src.repositories.chat_history_repository.ChatHistoryRepository",
            return_value=repo,
        ):
            assert await recent_turns(None, "s1", ["g1"]) == []


class TestRendering:
    def test_only_answers_are_addressable(self):
        # A request refers to an earlier ANSWER, never to an earlier question.
        turns = [
            SimpleNamespace(index=1, role="user", preview="gather news"),
            SimpleNamespace(index=2, role="assistant", preview="# News"),
        ]
        rendered = render_turns(turns)
        assert "[answer 2] Assistant: # News" in rendered
        assert "[answer 1]" not in rendered

    def test_long_answers_are_capped(self):
        # An assistant turn is often a whole deck. The router needs to know what
        # it was about, not to read it.
        with_long = [_row("assistant", "x" * 5000), _row("user", "current")]
        with _history(with_long):
            pass
        preview = render_turns(
            [SimpleNamespace(index=1, role="assistant", preview="x" * TURN_CHAR_CAP)]
        )
        assert len(preview) < 5000

    def test_no_turns_renders_nothing(self):
        assert render_turns([]) == ""


class TestResolvingAReference:
    turns = [
        SimpleNamespace(index=1, role="user", preview="", content="ask"),
        SimpleNamespace(index=2, role="assistant", preview="", content="the answer"),
    ]

    def test_resolves_an_answer_it_was_shown(self):
        assert turn_by_index(self.turns, 2).content == "the answer"

    def test_a_user_turn_is_not_something_to_work_from(self):
        # Pointing at the question again is not finding source material.
        assert turn_by_index(self.turns, 1) is None

    def test_an_index_it_could_not_have_read_binds_nothing(self):
        # Same stance as a value whose span is not in the message: a number it
        # was never shown is a number it made up.
        assert turn_by_index(self.turns, 9) is None
        assert turn_by_index(self.turns, None) is None
        assert turn_by_index(self.turns, "two") is None


class TestChatFurnitureIsNotAnAnswer:
    """Rows a run POSTS are not things a follow-up can refer to.

    Measured on a real misroute: of six rows before the follow-up, one was the
    routed run's own "Running **Gather News**" line, two were [ui-card]
    placeholders, one was a bold agent name, and exactly one was the deck. Left
    in, a window of eight turns is mostly labels — and the router can point
    refers_to at a status line.
    """

    @pytest.mark.asyncio
    async def test_only_the_real_answer_gets_a_number(self):
        with _history(
            [
                _row("user", "gather swiss news"),
                _row("assistant", "Running **Gather News**"),
                _row("assistant", "[ui-card]"),
                _row("assistant", "**News Gathering and Research Specialist**"),
                _row("assistant", "# Swiss News Summary (July 2026)"),
            ]
        ):
            turns = await recent_turns(
                None, "s1", ["g1"], exclude_message="what is the aviation story"
            )

        rendered = render_turns(turns)
        assert rendered == (
            "User: gather swiss news\n"
            "[answer 2] Assistant: # Swiss News Summary (July 2026)"
        )

    @pytest.mark.asyncio
    async def test_an_answer_that_merely_STARTS_bold_is_kept(self):
        # The filter is for rows that are ONLY a label. A real answer opening
        # with a bold heading is an answer.
        with _history([_row("assistant", "**Summary**\n\nThe aviation sector saw…")]):
            turns = await recent_turns(None, "s1", ["g1"])

        assert len(turns) == 1
