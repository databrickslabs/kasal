"""The loop and the report: scoring per agent, value redaction, summary, and table rows."""

import json

from src.services.tools.genie_space_sync_utils.eval.answer import Answer
from src.services.tools.genie_space_sync_utils.eval.judge import JudgeReply
from src.services.tools.genie_space_sync_utils.eval.questions import EvalQuestion
from src.services.tools.genie_space_sync_utils.eval.report import (
    build_eval_report,
    table_rows,
)
from src.services.tools.genie_space_sync_utils.eval.runner import Backends, run_eval


def _sql(rows):
    payload = {
        "manifest": {"schema": {"columns": [{"name": "total", "type_name": "DOUBLE"}]}},
        "result": {"data_array": rows},
    }
    return lambda sql: {"success": True, "data": payload}


def _genie(text, status="COMPLETED", latency=1.5):
    return Answer(
        agent="genie",
        status=status,
        text=text,
        sql=("SELECT total",),
        latency_s=latency,
        message_id="m-1",
        statement_ids=("s-1",),
        rate_limit_retries=1,
    )


def _judge_yes(system, user):
    return JudgeReply('{"correct": true, "reason": "same total"}', total_tokens=200)


def test_run_scores_each_question_against_its_expected_sql():
    questions = [
        EvalQuestion(
            "q1",
            "Total?",
            "dataset",
            expected_sql="SELECT total",
            expected_answer="Total 1250.5",
        ),
        EvalQuestion("q2", "Other?", "dataset", key_facts=(9.0,)),
    ]
    answers = {
        "Total?": _genie("The total is 1,250.5"),
        "Other?": _genie("nothing here"),
    }
    backends = Backends(
        ask_genie=lambda q: answers[q],
        run_sql=_sql([["1250.5"]]),
        complete=_judge_yes,
    )
    outcomes = run_eval(questions, backends)
    assert outcomes[0].genie.verdict == "pass"
    assert outcomes[0].genie.judgement.correct is True
    assert outcomes[1].genie.verdict == "fail"


def test_a_broken_expectation_or_failed_answer_is_an_error_verdict():
    questions = [EvalQuestion("q1", "x", "dataset", expected_sql="DELETE FROM t")]
    backends = Backends(ask_genie=lambda q: _genie("ok"), run_sql=_sql([]))
    outcome = run_eval(questions, backends)[0]
    assert outcome.genie.verdict == "error"
    failed = Backends(
        ask_genie=lambda q: _genie("", status="FAILED"), run_sql=_sql([["1"]])
    )
    outcome = run_eval([EvalQuestion("q2", "x", "dataset", key_facts=(1.0,))], failed)[
        0
    ]
    assert outcome.genie.verdict == "error"


def test_fabric_is_scored_side_by_side_with_the_same_expectation():
    questions = [EvalQuestion("q1", "x", "dataset", key_facts=(5.0,))]
    backends = Backends(
        ask_genie=lambda q: _genie("5"),
        run_sql=_sql([]),
        ask_fabric=lambda q: Answer(agent="fabric", status="COMPLETED", text="6"),
    )
    outcome = run_eval(questions, backends)[0]
    assert outcome.genie.verdict == "pass"
    assert outcome.fabric is not None and outcome.fabric.verdict == "fail"


def _report(statistics_only):
    questions = [EvalQuestion("q1", "Total?", "benchmark", key_facts=(1250.5,))]
    backends = Backends(
        ask_genie=lambda q: _genie("total 1250.5 with 1,250 rows"),
        run_sql=_sql([]),
        complete=_judge_yes,
    )
    outcomes = run_eval(questions, backends)
    return build_eval_report(
        outcomes, statistics_only=statistics_only, price_per_1k_tokens=0.5
    )


def test_full_report_keeps_answers_and_summary_counts_tokens_and_cost():
    entries, summary = _report(statistics_only=False)
    genie = entries[0]["genie"]
    assert "1250.5" in genie["answer"]
    assert genie["judge"]["reason"] == "same total"
    assert summary["genie"]["pass"] == 1
    assert summary["genie"]["score"] == 1.0
    assert summary["genie"]["judge_tokens"] == 200
    assert summary["genie"]["estimated_judge_cost"] == 0.1
    assert summary["genie"]["latency_s"]["p50"] == 1.5
    assert "fabric" not in summary


def test_statistics_only_drops_every_value_but_keeps_verdicts_and_gaps():
    entries, summary = _report(statistics_only=True)
    text = json.dumps(entries) + json.dumps(summary)
    assert "1250" not in text.replace("1250.5", "")
    assert "same total" not in text
    assert "answer" not in entries[0]["genie"]
    assert "tables" not in entries[0]["genie"]
    assert "reference" not in entries[0]["expected"]
    genie = entries[0]["genie"]
    assert genie["verdict"] == "pass"
    assert genie["numbers"]["max_relative_difference"] == 0.0
    assert genie["judge"]["correct"] is True


def test_table_rows_leave_out_answers_in_statistics_only_mode():
    entries, _ = _report(statistics_only=False)
    rows = table_rows(
        run_id="r1",
        run_ts="2026-01-01 00:00:00",
        space_id="s",
        entries=entries,
        statistics_only=True,
    )
    assert rows[0]["answer"] is None
    assert rows[0]["verdict"] == "pass"
    assert json.loads(rows[0]["statement_ids"]) == ["s-1"]
    full = table_rows(
        run_id="r1", run_ts="t", space_id="s", entries=entries, statistics_only=False
    )
    assert "1250.5" in full[0]["answer"]
