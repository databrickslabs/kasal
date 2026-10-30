"""Scoring a deliverable with the selected built-in judges.

The runner turns mlflow yes/no verdicts into 1/0, caches per deliverable and
never raises; ``combine`` folds the verdicts into the judge score (weighted
mean for graded judges, a multiplier for a failed gate).
"""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.services.prompt_optimization.builtin_judges import catalog, runner
from src.services.prompt_optimization.builtin_judges.bridge import JudgeRoute
from src.services.prompt_optimization.gepa import reflection

SAFETY = catalog.get("Safety")
RELEVANCE = catalog.get("RelevanceToQuery")
COMPLETENESS = catalog.get("Completeness")


def _verdict(judge, score, rationale="r"):
    return runner.BuiltinVerdict(judge, score, rationale)


class _Replies:
    """Fake LLMManager: the reply depends on which judge's prompt it is."""

    def __init__(self, by_marker, default="yes"):
        self.by_marker = by_marker
        self.default = default
        self.calls = []

    def __call__(self, loop, messages, model, max_tokens, **kwargs):
        text = " ".join(m["content"] for m in messages)
        self.calls.append({"model": model, "text": text})
        for marker, reply in self.by_marker.items():
            if marker in text:
                if isinstance(reply, Exception):
                    raise reply
                return reply
        return json.dumps({"result": self.default, "rationale": "fine"})


def _runner(ids=("Safety", "RelevanceToQuery", "Completeness"), rubric=""):
    return runner.BuiltinJudgeRunner.for_run(
        list(ids), "judge-key", JudgeRoute(loop=MagicMock()), rubric=rubric
    )


class TestToScore:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("yes", 1.0),
            ("No", 0.0),
            (" YES ", 1.0),
            (True, 1.0),
            (False, 0.0),
            (0.5, 0.5),
            (SimpleNamespace(value="no"), 0.0),  # CategoricalRating / Feedback
            ("maybe", None),
            (7, None),
            (None, None),
        ],
    )
    def test_yes_no_to_one_zero(self, value, expected):
        assert runner.to_score(value) == expected

    def test_real_categorical_rating(self):
        from mlflow.genai.judges import CategoricalRating

        assert runner.to_score(CategoricalRating.YES) == 1.0
        assert runner.to_score(CategoricalRating.NO) == 0.0


class TestCombine:
    def test_no_builtins_is_the_plain_mean(self):
        value, lines = runner.combine([0.6, 0.8], [])
        assert value == pytest.approx(0.7)
        assert lines == []

    def test_graded_verdicts_join_the_weighted_mean(self):
        value, lines = runner.combine(
            [0.6], [_verdict(RELEVANCE, 1.0), _verdict(COMPLETENESS, 0.0)]
        )
        assert value == pytest.approx((0.6 + 1.0 + 0.0) / 3)
        assert lines[0].startswith("[builtin:RelevanceToQuery] yes")
        assert lines[1].startswith("[builtin:Completeness] no")

    def test_weights_are_honoured(self):
        heavy = catalog.BuiltinJudge("Completeness", "C", "d", weight=3.0)
        value, _ = runner.combine([0.0], [_verdict(heavy, 1.0)])
        assert value == pytest.approx(3.0 / 4.0)

    def test_a_passed_gate_does_not_move_the_score(self):
        value, _ = runner.combine([0.6], [_verdict(SAFETY, 1.0)])
        assert value == pytest.approx(0.6)

    def test_a_failed_gate_applies_the_penalty(self):
        value, lines = runner.combine(
            [0.9], [_verdict(SAFETY, 0.0, "toxic"), _verdict(RELEVANCE, 1.0)]
        )
        assert value == pytest.approx(0.95 * catalog.GATE_FAILURE_MULTIPLIER)
        assert "[builtin:Safety] no. toxic" in lines
        assert "Gate failed (Safety)" in lines[-1]

    def test_the_penalty_is_the_catalog_constant(self):
        with patch.object(runner, "GATE_FAILURE_MULTIPLIER", 0.5):
            value, _ = runner.combine([0.8], [_verdict(SAFETY, 0.0)])
        assert value == pytest.approx(0.4)

    def test_a_failed_judge_is_left_out_and_the_mean_renormalises(self):
        value, lines = runner.combine(
            [0.6], [_verdict(RELEVANCE, None), _verdict(COMPLETENESS, 1.0)]
        )
        assert value == pytest.approx((0.6 + 1.0) / 2)
        assert not any("RelevanceToQuery" in line for line in lines)

    def test_nothing_scored_is_zero(self):
        assert runner.combine([], [_verdict(RELEVANCE, None)]) == (0.0, [])


class TestRunner:
    def test_for_run_is_none_without_a_selection(self):
        assert _runner(ids=()) is None

    def test_every_judge_calls_llm_manager_with_the_run_judge_model(self):
        fake = _Replies({})
        with patch.object(reflection, "_sync_llm_completion", fake):
            verdicts = _runner().score("objective", "the deliverable")
        assert [(v.judge.id, v.score) for v in verdicts] == [
            ("Safety", 1.0),
            ("RelevanceToQuery", 1.0),
            ("Completeness", 1.0),
        ]
        assert [c["model"] for c in fake.calls] == ["judge-key"] * 3

    def test_no_verdict_becomes_zero(self):
        fake = _Replies({"content safety": '{"result": "no", "rationale": "bad"}'})
        with patch.object(reflection, "_sync_llm_completion", fake):
            verdicts = _runner(ids=["Safety"]).score("objective", "text")
        assert verdicts[0].score == 0.0
        assert verdicts[0].rationale == "bad"

    def test_the_rubric_becomes_the_guidelines(self):
        fake = _Replies({})
        run = _runner(
            ids=["Guidelines"], rubric="- Research: A table\n\n- Cite sources"
        )
        with patch.object(reflection, "_sync_llm_completion", fake):
            run.score("objective", "text")
        assert "Research: A table" in fake.calls[0]["text"]
        assert "Cite sources" in fake.calls[0]["text"]

    def test_scores_are_cached_per_deliverable(self):
        fake = _Replies({})
        run = _runner(ids=["Safety"])
        with patch.object(reflection, "_sync_llm_completion", fake):
            first = run.score("objective", "same text")
            again = run.score("objective", "same text")
            run.score("objective", "other text")
        assert again is first
        assert len(fake.calls) == 2

    @pytest.mark.parametrize(
        "reply",
        [
            RuntimeError("provider down"),
            "I refuse to answer in the format.",
            '{"result": "maybe", "rationale": "unsure"}',
        ],
    )
    def test_a_failing_judge_never_raises_and_is_left_out(self, reply, caplog):
        fake = _Replies({"content safety": reply})
        with patch.object(reflection, "_sync_llm_completion", fake):
            verdicts = _runner(ids=["Safety", "Completeness"]).score("o", "text")
        by_id = {v.judge.id: v.score for v in verdicts}
        assert by_id == {"Safety": None, "Completeness": 1.0}
        assert "Built-in judge Safety failed" in caplog.text

    def test_a_scorer_that_cannot_be_built_is_left_out(self):
        with patch.object(runner, "build_scorer", side_effect=ValueError("gone")):
            verdicts = _runner(ids=["Safety"]).score("o", "text")
        assert verdicts[0].score is None

    def test_list_results_are_averaged(self):
        feedbacks = [SimpleNamespace(value="yes", rationale="a", error=None)] * 3 + [
            SimpleNamespace(value="no", rationale="b", error=None)
        ]
        verdict = runner._verdict(SAFETY, feedbacks)
        assert verdict.score == pytest.approx(0.75)

    def test_a_feedback_carrying_an_error_is_a_failure(self):
        with pytest.raises(ValueError, match="error"):
            runner._verdict(SAFETY, SimpleNamespace(value=None, error="boom"))
