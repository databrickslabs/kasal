"""Stopping rules for knowledge search.

The failure these exist for: an agent asked for an expense policy that was never
uploaded. The index answered every query with its top-20 — twenty unrelated
chunks from a presales deck, each reported at score 0.000 — so the agent
rephrased the question 25 times and the run died with "Tool-calling did not
converge within 25 rounds". Nothing in the loop misbehaved; the tool simply
never gave the model a reason to stop.
"""

import pytest

from src.engines.kasal.tools.custom.knowledge_search_guard import (
    KnowledgeSearchBudget,
    filter_by_relevance,
    no_relevant_results_notice,
    normalize_query,
)


def _result(score, content="chunk"):
    return {"content": content, "metadata": {"source": "doc.pdf", "score": score}}


class TestRelevanceFloor:
    def test_distant_results_are_dropped(self):
        kept, best, scored = filter_by_relevance(
            [_result(0.81), _result(0.42), _result(0.11)], min_score=0.35
        )

        assert [r["metadata"]["score"] for r in kept] == [0.81, 0.42]
        assert best == 0.81
        assert scored is True

    def test_all_distant_leaves_nothing_to_report(self):
        kept, best, scored = filter_by_relevance(
            [_result(0.12), _result(0.09)], min_score=0.35
        )

        assert kept == []
        assert best == 0.12
        assert scored is True

    def test_unscored_results_are_kept_rather_than_all_dropped(self):
        """A scoring regression — exactly the 0.000-for-everything bug — must
        degrade to the old behaviour, not return nothing for every query."""
        kept, best, scored = filter_by_relevance([_result(0.0), _result(0.0)])

        assert len(kept) == 2
        assert scored is False

    def test_no_results_at_all(self):
        assert filter_by_relevance([]) == ([], 0.0, False)


class TestNoRelevantResultsNotice:
    def test_says_how_close_it_got_and_not_to_retry(self):
        notice = no_relevant_results_notice("expense policy limit", 0.12, min_score=0.35)

        assert "expense policy limit" in notice
        assert "0.12" in notice
        # The part that ends the loop.
        assert "Rephrasing the query is unlikely to help" in notice

    def test_handles_a_search_that_returned_nothing(self):
        assert "Nothing in the knowledge base came close." in no_relevant_results_notice(
            "anything", 0.0
        )


class TestSearchBudget:
    def test_a_repeat_returns_the_first_answer(self):
        budget = KnowledgeSearchBudget()
        budget.record("Expense Policy", "twenty chunks")

        assert budget.previous_answer("expense  policy") == "twenty chunks"
        notice = budget.repeat_notice("expense policy", "twenty chunks")
        assert "already searched" in notice
        assert "twenty chunks" in notice

    def test_a_repeat_does_not_spend_more_budget(self):
        budget = KnowledgeSearchBudget(max_searches=2)
        budget.record("q", "a")
        budget.record("Q", "a")

        assert budget.searches_used == 1

    def test_the_budget_runs_out(self):
        budget = KnowledgeSearchBudget(max_searches=3)
        for i in range(3):
            budget.record(f"query {i}", "results")

        assert budget.exhausted()
        notice = budget.exhausted_notice()
        assert "Search budget reached (3 searches" in notice
        assert "query 0" in notice, "says what was already tried"

    def test_zero_disables_the_budget(self):
        budget = KnowledgeSearchBudget(max_searches=0)
        for i in range(50):
            budget.record(f"query {i}", "results")
        assert not budget.exhausted()

    @pytest.mark.parametrize(
        "a,b",
        [
            ("Expense Policy", "expense policy"),
            ("  travel   limit ", "travel limit"),
            ("DUPLICATE\tclaims", "duplicate claims"),
        ],
    )
    def test_queries_differing_only_in_case_or_spacing_are_one_search(self, a, b):
        assert normalize_query(a) == normalize_query(b)


class TestTheObservedLoop:
    def test_twenty_irrelevant_chunks_now_end_the_search(self):
        """The exact shape of the run that failed: 20 results, none close."""
        results = [_result(0.08) for _ in range(20)]

        kept, best, scored = filter_by_relevance(results, min_score=0.35)

        assert scored and not kept
        notice = no_relevant_results_notice("expense policy limit", best)
        assert "does not appear to contain this" in notice

    def test_rephrasings_are_bounded_even_when_each_one_is_new(self):
        """Every query in the failing run was a NEW phrasing, so deduplication
        alone would not have stopped it — the budget is what does."""
        budget = KnowledgeSearchBudget(max_searches=8)
        rephrasings = [
            "expense policy limit",
            "duplicate expense claim policy",
            "travel expense policy",
            "expense policy travel limit",
            "expense policy meal limit",
            "expense policy software limit",
            "company expense rules",
            "reimbursement limits",
            "expense threshold policy",
        ]

        served = 0
        for query in rephrasings:
            if budget.exhausted():
                break
            budget.record(query, "twenty irrelevant chunks")
            served += 1

        assert served == 8, "the 9th rephrasing is refused, not searched"


class TestAgentWallClockDefault:
    """A round cap alone leaves an agent free to burn minutes on 25 fruitless
    searches. The engine enforces Agent.max_execution_time; nothing was setting
    one, so the field was inert for every generated crew."""

    def _kwargs(self, spec_extra=None):
        from src.engines.kasal.kernel.agent_builder import build_agent_kwargs

        spec = {"role": "R", "goal": "G", "backstory": "B"}
        spec.update(spec_extra or {})
        return build_agent_kwargs(spec, tools=[], llm=object(), label="a")

    def test_a_default_is_applied_when_the_spec_sets_none(self):
        from src.engines.kasal.kernel.agent_builder import (
            DEFAULT_AGENT_MAX_EXECUTION_TIME,
        )

        assert self._kwargs()["max_execution_time"] == DEFAULT_AGENT_MAX_EXECUTION_TIME

    def test_an_explicit_value_always_wins(self):
        assert self._kwargs({"max_execution_time": 60})["max_execution_time"] == 60
