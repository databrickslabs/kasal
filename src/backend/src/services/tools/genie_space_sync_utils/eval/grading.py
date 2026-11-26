"""Grade one answer against its expected result (pure).

The expected result comes from :func:`resolve_expected` (an expected SQL run through
an injected executor, key facts, or the figures of an expected answer). An answer is
graded on its figures (:func:`grade_numbers`) and, when enabled, by a judge. The
verdict is pass, fail, error (the answer could not be produced or the expectation
could not be run) or review (nothing could be checked).
"""

from __future__ import annotations

from dataclasses import dataclass

from src.services.tools.genie_space_sync_utils.eval.answer import Answer
from src.services.tools.genie_space_sync_utils.eval.numeric import (
    Number,
    expected_from_text,
    extract_numbers,
    figure,
    is_found,
    relative_difference,
)
from src.services.tools.genie_space_sync_utils.eval.questions import (
    EvalQuestion,
    read_only_error,
)
from src.services.tools.genie_space_sync_utils.eval.tables import (
    ResultTable,
    markdown,
    numeric_cells,
    statement_table,
    value_text,
)
from src.services.tools.genie_space_sync_utils.sql_results import SqlFn
from src.services.tools.genie_space_sync_utils.verdicts import short

PASS = "pass"
FAIL = "fail"
ERROR = "error"
REVIEW = "review"
MAX_EXPECTED_FIGURES = 200


@dataclass(frozen=True)
class Expected:
    """What an answer must show: the figures, a reference text for the judge, and where they came from."""

    source: str
    figures: tuple[Number, ...] = ()
    reference: str = ""
    table: ResultTable | None = None
    error: str = ""


@dataclass(frozen=True)
class Grade:
    expected: int
    matched: int
    matched_in_answer: int
    max_relative_difference: float | None

    @property
    def passed(self) -> bool | None:
        return None if not self.expected else self.matched == self.expected


def resolve_expected(question: EvalQuestion, run_sql: SqlFn) -> Expected:
    """The expected result of *question*; *run_sql* runs one read-only statement on the warehouse."""
    if question.expected_sql:
        return _expected_from_sql(question, run_sql)
    if question.key_facts:
        figures = tuple(figure(value) for value in question.key_facts)
        facts = ", ".join(f"{f.value:g}" for f in figures)
        reference = question.expected_answer or f"Expected figures: {facts}"
        return Expected("key_facts", figures, reference)
    if question.expected_answer:
        figures = tuple(expected_from_text(question.expected_answer))
        return Expected("answer_text", figures, question.expected_answer)
    return Expected("none")


def _expected_from_sql(question: EvalQuestion, run_sql: SqlFn) -> Expected:
    problem = read_only_error(question.expected_sql)
    if problem:
        return Expected("sql", error=problem)
    result = run_sql(question.expected_sql)
    if not isinstance(result, dict) or not result.get("success"):
        error = result.get("error") if isinstance(result, dict) else ""
        return Expected(
            "sql", error=f"expected SQL failed: {short(error or 'unknown')}"
        )
    payload = result.get("data")
    table = statement_table(payload if isinstance(payload, dict) else {})
    figures = tuple(numeric_cells(table))[:MAX_EXPECTED_FIGURES]
    if not figures:
        return Expected(
            "sql", table=table, error="expected SQL returned no numeric value"
        )
    return Expected("sql", figures, question.expected_answer or markdown(table), table)


def tables_text(answer: Answer) -> str:
    return "\n".join(value_text(table) for table in answer.tables)


def grade_numbers(figures: tuple[Number, ...], answer: Answer) -> Grade:
    """How many expected figures the answer shows, in its text or in its result tables."""
    in_answer = extract_numbers(answer.text)
    in_all = in_answer + extract_numbers(tables_text(answer))
    gaps = [relative_difference(f, in_all) for f in figures]
    known = [gap for gap in gaps if gap is not None]
    return Grade(
        expected=len(figures),
        matched=sum(is_found(f, in_all) for f in figures),
        matched_in_answer=sum(is_found(f, in_answer) for f in figures),
        max_relative_difference=max(known) if known else None,
    )


def combine(checks: dict[str, bool | None]) -> str:
    """pass when every check that ran passed, fail when one failed, review when none ran."""
    ran = [value for value in checks.values() if value is not None]
    if not ran:
        return REVIEW
    return FAIL if not all(ran) else PASS


def grade_answer(
    answer: Answer,
    expected: Expected,
    judged: bool | None,
) -> tuple[str, Grade | None]:
    """Verdict and number grade of one answer. *judged* is the judge's verdict, or None when no judge ran."""
    if not answer.completed:
        return ERROR, None
    if expected.error:
        return ERROR, None
    grade = grade_numbers(expected.figures, answer) if expected.figures else None
    checks: dict[str, bool | None] = {
        "numbers": grade.passed if grade else None,
        "judge": judged,
    }
    return combine(checks), grade
