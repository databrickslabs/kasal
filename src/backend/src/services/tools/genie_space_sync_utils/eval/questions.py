"""What an evaluation asks: space benchmarks and supplied datasets (pure).

A question carries at most one expected result, in this order of preference: an
expected SQL statement (run on the warehouse, its result is the expected answer),
key facts (figures given as numbers), or an expected answer (text whose figures are
taken from the words). Expected SQL is limited to one read-only SELECT or WITH
statement, so an eval dataset cannot run anything else on the warehouse.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

BENCHMARK = "benchmark"
DATASET = "dataset"
MAX_QUESTION_CHARS = 2000
MAX_SQL_CHARS = 20000
_READ_ONLY = re.compile(r"^(SELECT|WITH)\b", re.IGNORECASE)


@dataclass(frozen=True)
class EvalQuestion:
    question_id: str
    question: str
    source: str
    expected_sql: str = ""
    expected_answer: str = ""
    key_facts: tuple[float, ...] = ()


def read_only_error(sql: str) -> str:
    """Why *sql* may not run as expected SQL, or "" when it is one read-only statement."""
    text = sql.strip()
    while text.endswith(";"):
        text = text[:-1].rstrip()
    if not text:
        return "expected SQL is empty"
    if len(text) > MAX_SQL_CHARS:
        return f"expected SQL is longer than {MAX_SQL_CHARS} characters"
    if not _READ_ONLY.match(text):
        return "expected SQL must be a single SELECT or WITH statement"
    if ";" in text:
        return "expected SQL must be a single statement"
    return ""


def benchmark_questions(serialized: dict[str, object]) -> list[EvalQuestion]:
    """The space's own benchmarks: each question with the SQL of its answer."""
    section = serialized.get("benchmarks")
    items = section.get("questions") if isinstance(section, dict) else None
    if not isinstance(items, list):
        return []
    questions: list[EvalQuestion] = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        text = _joined(item.get("question"))
        if not text:
            continue
        questions.append(
            EvalQuestion(
                question_id=str(item.get("id") or f"benchmark-{index:03d}"),
                question=text[:MAX_QUESTION_CHARS],
                source=BENCHMARK,
                expected_sql=_benchmark_sql(item.get("answer")),
            )
        )
    return questions


def _joined(value: object) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return "\n".join(str(part) for part in value if isinstance(part, str)).strip()
    return ""


def _benchmark_sql(answers: object) -> str:
    if not isinstance(answers, list):
        return ""
    for answer in answers:
        if isinstance(answer, dict) and str(answer.get("format", "")).upper() == "SQL":
            return _joined(answer.get("content"))
    return ""


def dataset_questions(raw: str) -> list[EvalQuestion]:
    """Questions from a JSON dataset: a list, or an object with a ``questions`` list."""
    try:
        parsed: object = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"eval_questions is not valid JSON: {exc.msg}") from exc
    items = parsed.get("questions") if isinstance(parsed, dict) else parsed
    if not isinstance(items, list) or not items:
        raise ValueError("eval_questions must be a non-empty JSON list of questions")
    questions = [_dataset_question(item, index) for index, item in enumerate(items, 1)]
    ids = [q.question_id for q in questions]
    duplicates = sorted({i for i in ids if ids.count(i) > 1})
    if duplicates:
        raise ValueError(f"duplicate question id(s): {', '.join(duplicates)}")
    return questions


def _dataset_question(item: object, index: int) -> EvalQuestion:
    if not isinstance(item, dict):
        raise ValueError(f"question {index} must be a JSON object")
    question = _text(item.get("question"))
    if not question:
        raise ValueError(f"question {index} has no question text")
    return EvalQuestion(
        question_id=_text(item.get("id")) or f"q{index:03d}",
        question=question[:MAX_QUESTION_CHARS],
        source=DATASET,
        expected_sql=_text(item.get("expected_sql")),
        expected_answer=_text(item.get("expected_answer")),
        key_facts=_key_facts(item.get("key_facts"), index),
    )


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _key_facts(value: object, index: int) -> tuple[float, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"question {index}: key_facts must be a list of numbers")
    facts: list[float] = []
    for fact in value:
        if isinstance(fact, bool) or not isinstance(fact, (int, float)):
            raise ValueError(f"question {index}: key_facts must be numbers")
        facts.append(float(fact))
    return tuple(facts)


def select_questions(
    questions: list[EvalQuestion], max_questions: int
) -> tuple[list[EvalQuestion], int]:
    """The first *max_questions* questions, and how many were left out."""
    kept = questions[:max_questions]
    return kept, len(questions) - len(kept)
