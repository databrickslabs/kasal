"""The evaluation loop: resolve each expected result, ask each agent, score the answers.

Questions run one after another. Genie's rate limits are handled by the retrying
client, and a parallel loop would only trade latency for more 429s. Every backend is
injected through :class:`Backends`, so the loop has no I/O of its own.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from src.services.tools.genie_space_sync_utils.eval.answer import Answer
from src.services.tools.genie_space_sync_utils.eval.grading import (
    ERROR,
    Expected,
    Grade,
    grade_answer,
    resolve_expected,
)
from src.services.tools.genie_space_sync_utils.eval.judge import (
    CompleteFn,
    Judgement,
    judge,
)
from src.services.tools.genie_space_sync_utils.eval.questions import EvalQuestion
from src.services.tools.genie_space_sync_utils.sql_results import SqlFn
from src.services.tools.genie_space_sync_utils.verdicts import short


@dataclass(frozen=True)
class Backends:
    """Everything the loop talks to. ``ask_fabric`` and ``complete`` are optional side-by-side and judge."""

    ask_genie: Callable[[str], Answer]
    run_sql: SqlFn
    ask_fabric: Callable[[str], Answer] | None = None
    complete: CompleteFn | None = None


@dataclass(frozen=True)
class Scored:
    answer: Answer
    verdict: str
    grade: Grade | None
    judgement: Judgement | None
    reason: str = ""


@dataclass(frozen=True)
class QuestionOutcome:
    question: EvalQuestion
    expected: Expected
    genie: Scored
    fabric: Scored | None = None


def score_answer(
    question: EvalQuestion,
    expected: Expected,
    answer: Answer,
    complete: CompleteFn | None,
) -> Scored:
    """Grade one agent's answer. The judge runs only on a completed answer with a reference to judge against."""
    if not answer.completed:
        reason = short(f"{answer.status}: {answer.error}".rstrip(": "))
        return Scored(answer, ERROR, None, None, reason)
    if expected.error:
        return Scored(answer, ERROR, None, None, expected.error)
    judgement: Judgement | None = None
    if complete is not None and expected.reference:
        judgement = judge(complete, question.question, expected.reference, answer.text)
        if judgement.error:
            return Scored(answer, ERROR, None, judgement, judgement.error)
    verdict, grade = grade_answer(
        answer, expected, judgement.correct if judgement is not None else None
    )
    return Scored(answer, verdict, grade, judgement, answer.error)


def run_eval(
    questions: list[EvalQuestion], backends: Backends
) -> list[QuestionOutcome]:
    """Ask every question of *questions* to each agent and score the answers, in order."""
    outcomes: list[QuestionOutcome] = []
    for question in questions:
        expected = resolve_expected(question, backends.run_sql)
        genie = score_answer(
            question, expected, backends.ask_genie(question.question), backends.complete
        )
        fabric: Scored | None = None
        if backends.ask_fabric is not None:
            fabric = score_answer(
                question,
                expected,
                backends.ask_fabric(question.question),
                backends.complete,
            )
        outcomes.append(QuestionOutcome(question, expected, genie, fabric))
    return outcomes
