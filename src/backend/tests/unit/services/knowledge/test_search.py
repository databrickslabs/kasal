"""Searching a workspace's knowledge, and answering in words.

This capability used to live inside DatabricksKnowledgeSearchTool, reachable
only by an agent, in a crew, mid-run — which is the wrong boundary for "search
the documents this workspace uploaded". Crew generation may want to research
before it plans; a chat turn may want to answer from an attached file without an
agent loop. These tests exercise it directly, with no tool and no agent.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from src.services.knowledge import KnowledgeSearch


def _result(score, content="chunk", source="doc.pdf"):
    return {"content": content, "metadata": {"source": source, "score": score}}


class TestFormat:
    def test_renders_the_matches_it_kept(self):
        answer = KnowledgeSearch.format("q", [_result(0.91, "Some content")])

        assert "Found 1 relevant results:" in answer
        assert "Some content" in answer
        assert "doc.pdf" in answer
        assert "0.910" in answer

    def test_numbers_each_match(self):
        answer = KnowledgeSearch.format(
            "q",
            [_result(0.9, "Content A", "a.pdf"), _result(0.8, "Content B", "b.pdf")],
        )

        assert "Result 1" in answer and "Result 2" in answer
        assert "Content A" in answer and "Content B" in answer

    def test_no_results_says_so_in_terms_that_end_the_search(self):
        answer = KnowledgeSearch.format("expense policy", [])

        assert "No relevant information found" in answer
        assert "Rephrasing the query is unlikely to help" in answer

    def test_twenty_distant_matches_are_not_an_answer(self):
        """An index always returns its top-k. Twenty unrelated chunks read to an
        agent exactly like twenty relevant ones — that is what made it rephrase
        25 times and die on the round limit."""
        answer = KnowledgeSearch.format(
            "expense policy", [_result(0.08) for _ in range(20)]
        )

        assert "does not appear to contain this" in answer

    def test_unscored_results_are_still_shown(self):
        """A scoring regression must degrade to the old behaviour, not return
        nothing for every query."""
        answer = KnowledgeSearch.format("q", [_result(0.0, "Some content")])

        assert "Some content" in answer


class TestSearch:
    @pytest.mark.asyncio
    async def test_formats_what_the_store_returned(self):
        search = KnowledgeSearch(group_id="g1", user_email="dev@localhost")

        with patch.object(
            KnowledgeSearch,
            "raw_results",
            AsyncMock(return_value=[_result(0.9, "Answer text")]),
        ):
            answer = await search.search("q")

        assert "Answer text" in answer

    @pytest.mark.asyncio
    async def test_a_failure_is_a_sentence_not_an_exception(self):
        """The caller is usually mid-answer; an exception here costs a user
        their reply for what is at worst a missing citation."""
        search = KnowledgeSearch(group_id="g1")

        with patch.object(
            KnowledgeSearch,
            "raw_results",
            AsyncMock(side_effect=RuntimeError("index down")),
        ):
            answer = await search.search("q")

        assert "Error searching knowledge base" in answer
        assert "index down" in answer

    @pytest.mark.asyncio
    async def test_a_hung_search_does_not_hold_the_turn_open(self):
        search = KnowledgeSearch(group_id="g1")

        async def _never(*args, **kwargs):
            await asyncio.sleep(60)

        with (
            patch.object(KnowledgeSearch, "raw_results", _never),
            patch("src.services.knowledge.search.SEARCH_TIMEOUT_SECONDS", 0.01),
        ):
            answer = await search.search("q")

        assert "timed out" in answer

    def test_it_carries_the_identity_the_search_runs_under(self):
        """Per-user isolation is not a filter applied afterwards: knowledge is
        scoped to the user who uploaded it."""
        search = KnowledgeSearch(
            group_id="g1",
            execution_id="exec-1",
            user_email="dev@localhost",
            agent_id="a1",
        )

        assert (
            search.group_id,
            search.execution_id,
            search.user_email,
            search.agent_id,
        ) == (
            "g1",
            "exec-1",
            "dev@localhost",
            "a1",
        )
