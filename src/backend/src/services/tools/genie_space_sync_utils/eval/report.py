"""Turn evaluation outcomes into the tool's JSON report, with the value policy applied (pure).

``statistics_only`` drops every value the eval produced or was given to compare
against: answer text, result tables, expected answers and expected tables, and judge
reasons. What stays: verdicts, statuses, counts, latencies, token totals, the SQL the
agents ran (a statement, not a value), message and statement ids for cost attribution,
and the largest relative difference between an expected figure and the closest figure
the agent produced.
"""

from __future__ import annotations

import json
import math
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone

from src.services.tools.genie_space_sync_utils.eval.answer import COMPLETED
from src.services.tools.genie_space_sync_utils.eval.grading import (
    ERROR,
    FAIL,
    PASS,
    REVIEW,
)
from src.services.tools.genie_space_sync_utils.eval.runner import (
    QuestionOutcome,
    Scored,
)
from src.services.tools.genie_space_sync_utils.eval.tables import markdown
from src.services.tools.genie_space_sync_utils.verdicts import short

VERDICTS = (PASS, FAIL, ERROR, REVIEW)
AGENTS = ("genie", "fabric")
NOTES: tuple[str, ...] = (
    "Genie reports no token usage, and Fabric's MCP route reports none either: only the "
    "LLM judge's tokens are counted. Attribute Genie DBUs with system.billing.usage using "
    "message_id, statement_ids and the run window.",
)


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _count(value: object) -> int:
    return int(value) if isinstance(value, int) and not isinstance(value, bool) else 0


def _judge_entry(scored: Scored, statistics_only: bool) -> dict[str, object] | None:
    judgement = scored.judgement
    if judgement is None:
        return None
    entry: dict[str, object] = {
        "correct": judgement.correct,
        "error": judgement.error,
        "total_tokens": judgement.total_tokens,
    }
    if not statistics_only:
        entry["reason"] = judgement.reason
    return entry


def _agent_entry(scored: Scored, statistics_only: bool) -> dict[str, object]:
    answer = scored.answer
    grade = scored.grade
    numbers: dict[str, object] | None = None
    if grade is not None:
        numbers = {
            "expected": grade.expected,
            "matched": grade.matched,
            "matched_in_answer": grade.matched_in_answer,
            "max_relative_difference": grade.max_relative_difference,
        }
    entry: dict[str, object] = {
        "verdict": scored.verdict,
        "status": answer.status,
        "latency_s": answer.latency_s,
        "rate_limit_retries": answer.rate_limit_retries,
        "message_id": answer.message_id,
        "statement_ids": list(answer.statement_ids),
        "sql": list(answer.sql),
        "error": short(scored.reason or answer.error),
        "numbers": numbers,
        "judge": _judge_entry(scored, statistics_only),
    }
    if not statistics_only:
        entry["answer"] = answer.text
        entry["tables"] = [markdown(table) for table in answer.tables]
    return entry


def question_entry(
    outcome: QuestionOutcome, statistics_only: bool
) -> dict[str, object]:
    expected = outcome.expected
    expected_entry: dict[str, object] = {
        "source": expected.source,
        "figures": len(expected.figures),
        "error": expected.error,
    }
    entry: dict[str, object] = {
        "question_id": outcome.question.question_id,
        "source": outcome.question.source,
        "question": outcome.question.question,
        "expected_sql": outcome.question.expected_sql,
        "expected": expected_entry,
        "genie": _agent_entry(outcome.genie, statistics_only),
    }
    if outcome.fabric is not None:
        entry["fabric"] = _agent_entry(outcome.fabric, statistics_only)
    if not statistics_only:
        expected_entry["reference"] = expected.reference
        if expected.table is not None:
            entry["expected_table"] = markdown(expected.table)
    return entry


def _latency(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    ordered = sorted(values)

    def nearest_rank(share: float) -> float:
        return ordered[max(0, math.ceil(share * len(ordered)) - 1)]

    return {
        "mean": round(sum(ordered) / len(ordered), 2),
        "p50": nearest_rank(0.5),
        "p95": nearest_rank(0.95),
        "max": ordered[-1],
    }


def summarize_agent(
    entries: list[dict[str, object]], price_per_1k_tokens: float | None
) -> dict[str, object]:
    """Counts, score, latency and judge-token totals for one agent over its entries."""
    counts = {
        verdict: sum(e["verdict"] == verdict for e in entries) for verdict in VERDICTS
    }
    graded = counts[PASS] + counts[FAIL]
    latencies = [
        value
        for value in (
            _number(e["latency_s"]) for e in entries if e["status"] == COMPLETED
        )
        if value is not None
    ]
    tokens = 0
    for entry in entries:
        judge_entry = entry["judge"]
        if isinstance(judge_entry, dict):
            tokens += _count(judge_entry.get("total_tokens"))
    cost = None
    if price_per_1k_tokens is not None:
        cost = round(tokens / 1000 * price_per_1k_tokens, 6)
    return {
        "asked": len(entries),
        **counts,
        "score": round(counts[PASS] / graded, 3) if graded else None,
        "coverage": round(graded / len(entries), 3) if entries else None,
        "completed": sum(e["status"] == COMPLETED for e in entries),
        "latency_s": _latency(latencies),
        "rate_limit_retries": sum(_count(e["rate_limit_retries"]) for e in entries),
        "judge_tokens": tokens,
        "estimated_judge_cost": cost,
    }


def _agent_entries(
    question_entries: list[dict[str, object]], agent: str
) -> list[dict[str, object]]:
    found: list[dict[str, object]] = []
    for entry in question_entries:
        value = entry.get(agent)
        if isinstance(value, dict):
            found.append(value)
    return found


def summarize(
    question_entries: Iterable[dict[str, object]], price_per_1k_tokens: float | None
) -> dict[str, dict[str, object]]:
    """Per-agent summary; the Fabric summary appears only when Fabric ran."""
    entries = list(question_entries)
    agents = ["genie"]
    if any("fabric" in entry for entry in entries):
        agents.append("fabric")
    return {
        agent: summarize_agent(_agent_entries(entries, agent), price_per_1k_tokens)
        for agent in agents
    }


def table_rows(
    *,
    run_id: str,
    run_ts: str,
    space_id: str,
    entries: list[dict[str, object]],
    statistics_only: bool,
) -> list[dict[str, object]]:
    """One results-table row per agent answer. The answer text is left out in statistics-only mode."""
    rows: list[dict[str, object]] = []
    for entry in entries:
        for agent in AGENTS:
            value = entry.get(agent)
            if not isinstance(value, dict):
                continue
            numbers = value.get("numbers")
            numbers = numbers if isinstance(numbers, dict) else {}
            judge_entry = value.get("judge")
            judge_entry = judge_entry if isinstance(judge_entry, dict) else {}
            rows.append(
                {
                    "run_id": run_id,
                    "run_ts": run_ts,
                    "space_id": space_id,
                    "question_id": entry["question_id"],
                    "source": entry["source"],
                    "agent": agent,
                    "verdict": value["verdict"],
                    "status": value["status"],
                    "expected_figures": numbers.get("expected"),
                    "matched_figures": numbers.get("matched"),
                    "max_relative_difference": numbers.get("max_relative_difference"),
                    "judge_correct": judge_entry.get("correct"),
                    "latency_s": value["latency_s"],
                    "total_tokens": judge_entry.get("total_tokens"),
                    "message_id": value["message_id"],
                    "statement_ids": json.dumps(value["statement_ids"]),
                    "error": value["error"],
                    "answer": None if statistics_only else value.get("answer"),
                }
            )
    return rows


def build_eval_report(
    outcomes: list[QuestionOutcome],
    *,
    statistics_only: bool,
    price_per_1k_tokens: float | None,
) -> tuple[list[dict[str, object]], dict[str, dict[str, object]]]:
    """``(question entries, per-agent summary)`` for a finished run."""
    entries = [question_entry(outcome, statistics_only) for outcome in outcomes]
    return entries, summarize(entries, price_per_1k_tokens)


def new_run_identity() -> tuple[str, str]:
    """A short run id and the UTC time the run started."""
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return uuid.uuid4().hex[:12], run_ts
