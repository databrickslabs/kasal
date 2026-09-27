"""The judges that need labels: the coverage floor, and scoring through the bridge.

The scorers are the REAL mlflow classes; only the LLM (LLMManager) is fake.
What must hold:

* the floor: a label judge counts only with >=50% AND >=3 rows labelled (all
  rows when there are fewer than 3), else it is skipped with a reason;
* Correctness and ExpectationsGuidelines reach LLMManager with the run's judge
  model and the right expectation fields;
* a row without labels is skipped BEFORE the call, so mlflow never raises.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.services.prompt_optimization.builtin_judges import catalog, coverage, runner
from src.services.prompt_optimization.builtin_judges.bridge import JudgeRoute
from src.services.prompt_optimization.gepa import reflection

CORRECTNESS = catalog.get("Correctness")
EXPECTATIONS = catalog.get("ExpectationsGuidelines")
SAFETY = catalog.get("Safety")
FACTS = {"expected_facts": ["Zurich is listed", "prices are in CHF"]}


class TestCoverageFloor:
    @pytest.mark.parametrize(
        "labelled,total,counts",
        [
            (1, 1, True),  # a crew run: labelled or not
            (0, 1, False),
            (2, 2, True),  # fewer than 3 rows: every row must be labelled
            (1, 2, False),
            (3, 6, True),  # half, and 3
            (2, 4, False),  # half, but only 2
            (3, 7, False),  # 3, but under half
            (10, 10, True),
        ],
    )
    def test_half_and_three_rows(self, labelled, total, counts):
        rows = [FACTS] * labelled + [{}] * (total - labelled)
        assert (coverage.skip_reason(CORRECTNESS, rows) is None) is counts

    def test_the_reasons_say_why(self):
        assert coverage.skip_reason(CORRECTNESS, [{}]) == (
            "Correctness skipped: not labelled"
        )
        reason = coverage.skip_reason(CORRECTNESS, [FACTS, {}, {}, {}])
        assert reason is not None and "1 of 4 rows labelled" in reason

    def test_any_one_label_field_counts(self):
        assert coverage.is_labelled(CORRECTNESS, {"expected_response": "An answer"})
        assert coverage.is_labelled(EXPECTATIONS, {"guidelines": ["Be brief"]})
        assert not coverage.is_labelled(EXPECTATIONS, FACTS)
        assert not coverage.is_labelled(CORRECTNESS, {"expected_facts": []})

    def test_judges_without_labels_always_count(self):
        usable, skipped = coverage.split_by_coverage(
            [SAFETY, CORRECTNESS, EXPECTATIONS], [FACTS]
        )
        assert [j.id for j in usable] == ["Safety", "Correctness"]
        assert skipped == {
            "ExpectationsGuidelines": "Expectations guidelines skipped: not labelled"
        }


class _Replies:
    def __init__(self):
        self.calls = []

    def __call__(self, loop, messages, model, max_tokens, **kwargs):
        text = " ".join(m["content"] for m in messages)
        self.calls.append({"model": model, "text": text})
        return json.dumps({"result": "yes", "rationale": "contains them"})


def _runner(ids, expectations):
    return runner.BuiltinJudgeRunner.for_run(
        ids, "run-judge", JudgeRoute(loop=MagicMock()), expectations=expectations
    )


class TestLabelJudgesThroughTheBridge:
    def test_correctness_reads_the_expected_facts(self):
        fake = _Replies()
        with patch.object(reflection, "_sync_llm_completion", fake):
            verdicts = _runner(["Correctness"], FACTS).score("objective", "text")
        assert [(v.judge.id, v.score) for v in verdicts] == [("Correctness", 1.0)]
        assert [c["model"] for c in fake.calls] == ["run-judge"]
        assert "Zurich is listed" in fake.calls[0]["text"]
        assert "prices are in CHF" in fake.calls[0]["text"]

    def test_correctness_reads_the_expected_answer(self):
        fake = _Replies()
        row = {"expected_response": "A table of five Zurich flats"}
        with patch.object(reflection, "_sync_llm_completion", fake):
            _runner(["Correctness"], row).score("objective", "text")
        assert "A table of five Zurich flats" in fake.calls[0]["text"]

    def test_expectations_guidelines_reads_the_guidelines(self):
        fake = _Replies()
        row = {**FACTS, "guidelines": ["Only German-speaking cities"]}
        with patch.object(reflection, "_sync_llm_completion", fake):
            verdicts = _runner(["ExpectationsGuidelines"], row).score("o", "text")
        assert verdicts[0].score == 1.0
        assert fake.calls[0]["model"] == "run-judge"
        assert "Only German-speaking cities" in fake.calls[0]["text"]
        # Correctness' labels are not its business.
        assert "Zurich is listed" not in fake.calls[0]["text"]

    @pytest.mark.parametrize("judge_id", ["Correctness", "ExpectationsGuidelines"])
    def test_a_row_without_labels_is_skipped_before_the_call(self, judge_id, caplog):
        fake = _Replies()
        with patch.object(reflection, "_sync_llm_completion", fake):
            verdicts = _runner([judge_id], {}).score("o", "text")
        assert verdicts[0].score is None
        assert fake.calls == []
        # Skipped, not failed: mlflow's "requires expected_response" never ran.
        assert "failed" not in caplog.text

    def test_a_row_may_bring_its_own_labels(self):
        fake = _Replies()
        run = _runner(["Correctness"], {})
        with patch.object(reflection, "_sync_llm_completion", fake):
            assert run.score("o", "text")[0].score is None
            assert run.score("o", "text", expectations=FACTS)[0].score == 1.0
        assert len(fake.calls) == 1

    def test_no_label_judges_are_called_without_expectations(self):
        fake = _Replies()
        with patch.object(reflection, "_sync_llm_completion", fake):
            verdicts = _runner(["Safety", "Correctness"], {}).score("o", "text")
        assert {v.judge.id: v.score for v in verdicts} == {
            "Safety": 1.0,
            "Correctness": None,
        }
        assert len(fake.calls) == 1
