"""Questions (benchmarks, dataset, read-only SQL, limits) and result tables."""

import json

import pytest

from src.services.tools.genie_space_sync_utils.eval.questions import (
    benchmark_questions,
    dataset_questions,
    read_only_error,
    select_questions,
)
from src.services.tools.genie_space_sync_utils.eval.settings import (
    parse_limit,
    parse_price,
)
from src.services.tools.genie_space_sync_utils.eval.tables import (
    numeric_cells,
    statement_table,
)


def _payload(columns, rows):
    return {
        "manifest": {
            "schema": {"columns": [{"name": n, "type_name": t} for n, t in columns]}
        },
        "result": {"data_array": rows},
    }


def test_benchmarks_yield_their_sql_answers():
    serialized = {
        "benchmarks": {
            "questions": [
                {
                    "id": "b1",
                    "question": ["Fill rate?"],
                    "answer": [{"format": "SQL", "content": ["SELECT 1"]}],
                },
                {"question": ["No answer"]},
                "not-a-dict",
            ]
        }
    }
    found = benchmark_questions(serialized)
    assert [q.question_id for q in found] == ["b1", "benchmark-002"]
    assert found[0].expected_sql == "SELECT 1"
    assert found[1].expected_sql == ""


def test_dataset_accepts_a_list_or_an_object_and_numbers_its_questions():
    raw = json.dumps({"questions": [{"question": "How many?", "key_facts": [3]}]})
    questions = dataset_questions(raw)
    assert questions[0].question_id == "q001"
    assert questions[0].key_facts == (3.0,)
    assert len(dataset_questions(json.dumps([{"question": "x"}]))) == 1


@pytest.mark.parametrize(
    "raw, message",
    [
        ("not json", "not valid JSON"),
        ("[]", "non-empty"),
        ('[{"id": "a"}]', "no question text"),
        ('[{"question": "a", "key_facts": ["x"]}]', "must be numbers"),
        ('[{"question": "a", "key_facts": 3}]', "must be a list"),
        ('[{"id": "a", "question": "x"}, {"id": "a", "question": "y"}]', "duplicate"),
    ],
)
def test_invalid_datasets_are_refused(raw, message):
    with pytest.raises(ValueError, match=message):
        dataset_questions(raw)


@pytest.mark.parametrize(
    "sql, problem",
    [
        ("DELETE FROM t", "SELECT or WITH"),
        ("SELECT 1; DROP TABLE t", "single statement"),
        ("   ", "empty"),
        ("WITH x AS (SELECT 1) SELECT * FROM x;", ""),
        ("select 1", ""),
    ],
)
def test_expected_sql_must_be_one_read_only_statement(sql, problem):
    assert (problem in read_only_error(sql)) if problem else read_only_error(sql) == ""


def test_select_questions_reports_how_many_were_left_out():
    questions = dataset_questions(json.dumps([{"question": f"q{i}"} for i in range(5)]))
    kept, skipped = select_questions(questions, 3)
    assert (len(kept), skipped) == (3, 2)


def test_limit_and_price_settings():
    assert parse_limit(None) == 50
    assert parse_limit("7") == 7
    with pytest.raises(ValueError):
        parse_limit(0)
    with pytest.raises(ValueError):
        parse_limit(True)
    with pytest.raises(ValueError):
        parse_limit("201")
    assert parse_price(None) is None
    assert parse_price("0.002") == 0.002
    with pytest.raises(ValueError):
        parse_price(-1)


def test_statement_table_reads_types_rows_and_caps_length():
    table = statement_table(
        _payload([("region", "STRING"), ("n", "BIGINT")], [["EU", "7"], ["US", None]])
    )
    assert table.columns == ("region", "n")
    assert [c.value for c in numeric_cells(table)] == [7.0]
    many = statement_table(_payload([("n", "INT")], [["1"]] * 600))
    assert len(many.rows) == 500 and many.truncated
