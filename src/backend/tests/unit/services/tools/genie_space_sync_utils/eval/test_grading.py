"""Expected results and verdicts: SQL-derived, key facts, text figures, and the combined verdict."""

from src.services.tools.genie_space_sync_utils.eval.answer import COMPLETED, Answer
from src.services.tools.genie_space_sync_utils.eval.grading import (
    FAIL,
    PASS,
    REVIEW,
    combine,
    grade_answer,
    grade_numbers,
    resolve_expected,
)
from src.services.tools.genie_space_sync_utils.eval.questions import EvalQuestion
from src.services.tools.genie_space_sync_utils.eval.tables import statement_table


def _sql_ok(rows):
    payload = {
        "manifest": {"schema": {"columns": [{"name": "total", "type_name": "DOUBLE"}]}},
        "result": {"data_array": rows},
    }
    return lambda sql: {"success": True, "data": payload}


def _answer(text="", tables=(), status=COMPLETED, error=""):
    return Answer(agent="genie", status=status, text=text, tables=tables, error=error)


def test_expected_sql_result_becomes_the_figures_and_the_judge_reference():
    question = EvalQuestion("q1", "Total?", "dataset", expected_sql="SELECT 1")
    expected = resolve_expected(question, _sql_ok([["1234.5"]]))
    assert expected.source == "sql"
    assert [f.value for f in expected.figures] == [1234.5]
    assert "1234.5" in expected.reference


def test_failing_or_unsafe_expected_sql_is_an_expectation_error_not_a_verdict():
    failing = EvalQuestion("q1", "x", "dataset", expected_sql="SELECT 1")
    expected = resolve_expected(
        failing, lambda sql: {"success": False, "error": "boom"}
    )
    assert expected.error.startswith("expected SQL failed")
    unsafe = EvalQuestion("q2", "x", "dataset", expected_sql="DROP TABLE t")
    assert "SELECT or WITH" in resolve_expected(unsafe, _sql_ok([])).error


def test_key_facts_and_answer_text_figures():
    facts = EvalQuestion("q", "x", "dataset", key_facts=(12.5, 3.0))
    assert [f.value for f in resolve_expected(facts, _sql_ok([])).figures] == [
        12.5,
        3.0,
    ]
    text = EvalQuestion(
        "q", "x", "dataset", expected_answer="Revenue was 1,250.5 in 2026"
    )
    expected = resolve_expected(text, _sql_ok([]))
    assert expected.source == "answer_text"
    assert [f.value for f in expected.figures] == [1250.5]


def test_grade_counts_figures_in_answer_and_tables_and_reports_the_gap():
    expected = resolve_expected(
        EvalQuestion("q", "x", "dataset", key_facts=(100.0, 50.0)), _sql_ok([])
    )
    table = statement_table({"result": {"data_array": [["100"]]}})
    grade = grade_numbers(expected.figures, _answer(text="about 105", tables=(table,)))
    assert grade.expected == 2
    assert grade.matched == 1
    assert grade.matched_in_answer == 0
    assert grade.max_relative_difference == 1.0  # 50 has no near figure in the text


def test_verdicts_pass_fail_error_review():
    numbers = resolve_expected(
        EvalQuestion("q", "x", "dataset", key_facts=(7.0,)), _sql_ok([])
    )
    assert grade_answer(_answer(text="7"), numbers, None)[0] == PASS
    assert grade_answer(_answer(text="8"), numbers, None)[0] == FAIL
    assert grade_answer(_answer(text="7"), numbers, False)[0] == FAIL
    assert (
        grade_answer(_answer(status="FAILED", error="x"), numbers, None)[0] == "error"
    )
    nothing = resolve_expected(EvalQuestion("q", "x", "dataset"), _sql_ok([]))
    assert grade_answer(_answer(text="anything"), nothing, None)[0] == REVIEW
    assert combine({"numbers": True, "judge": None}) == PASS
