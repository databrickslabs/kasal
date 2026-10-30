"""Evaluation traces: tagged and read per crew AND workspace, and their review."""

from types import SimpleNamespace
from unittest.mock import patch

from src.services.prompt_optimization.labels import review


def _assessment(name, feedback=None, expectation=None, rationale=None):
    return SimpleNamespace(
        name=name,
        rationale=rationale,
        feedback=SimpleNamespace(value=feedback) if feedback is not None else None,
        expectation=(
            SimpleNamespace(value=expectation) if expectation is not None else None
        ),
    )


def _trace(request_time, *assessments):
    return SimpleNamespace(
        info=SimpleNamespace(request_time=request_time),
        search_assessments=lambda: list(assessments),
    )


class TestTags:
    def test_traces_are_tagged_with_crew_and_workspace(self):
        assert review.trace_tags("c1", "group_a") == {
            "kasal_crew_id": "c1",
            "kasal_group_id": "group_a",
        }
        assert review.trace_tags("c1", None)["kasal_group_id"] == "base"

    def test_reads_filter_on_both_tags(self):
        assert review.trace_filter("c1", "group_a") == (
            "tags.kasal_crew_id = 'c1' AND tags.kasal_group_id = 'group_a'"
        )

    def test_quotes_cannot_break_out_of_the_filter(self):
        text = review.trace_filter("c1' OR '1'='1", "g'x")
        assert text.count("'") == 4


class TestHarvest:
    def test_suggestions_are_the_human_expectations_newest_first(self):
        traces = [
            _trace(3, _assessment("human_expectation", expectation="Newest note")),
            _trace(1, _assessment("human_expectation", expectation="Oldest note")),
            _trace(2, _assessment("human_expectation", expectation="Oldest note")),
            _trace(4, _assessment("expected_facts", expectation="not a note")),
        ]
        harvest = review.harvest_review(traces)
        assert harvest.suggestions == ("Newest note", "Oldest note")

    def test_expectations_and_reasons_feed_the_requirements(self):
        traces = [
            _trace(
                1,
                _assessment("human_grade", feedback=2.0, rationale="Wrong region"),
                _assessment("human_expectation", expectation="German side only"),
                _assessment("human_grade", feedback=8.0),
            )
        ]
        harvest = review.harvest_review(traces)
        assert harvest.req_texts == ("Wrong region", "German side only")
        assert harvest.note_count == 3

    def test_suggestions_are_capped(self):
        traces = [
            _trace(i, _assessment("human_expectation", expectation=f"note {i}"))
            for i in range(10)
        ]
        assert len(review.harvest_review(traces).suggestions) == review.MAX_SUGGESTIONS

    def test_read_review_searches_only_this_workspace(self):
        with patch("mlflow.search_traces", return_value=[]) as search:
            assert review.read_review("c1", "group_a") == review.ReviewHarvest()
        assert search.call_args.kwargs["filter_string"] == review.trace_filter(
            "c1", "group_a"
        )
