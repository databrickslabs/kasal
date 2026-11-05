"""Results table: literal escaping, statement shape, chunking, and failure reporting."""

import pytest

from src.services.tools.genie_space_sync_utils.eval.results_table import (
    ROWS_PER_INSERT,
    create_statement,
    insert_statements,
    sql_literal,
    write_rows,
)


def test_strings_are_escaped_for_quotes_and_backslashes_and_controls_dropped():
    assert sql_literal("O'Brien") == "'O''Brien'"
    assert sql_literal("a\\b") == "'a\\\\b'"
    assert sql_literal("x\x00y") == "'xy'"
    assert sql_literal("'); DROP TABLE t; --") == "'''); DROP TABLE t; --'"


def test_scalars_and_nulls():
    assert sql_literal(None) == "NULL"
    assert sql_literal(True) == "TRUE"
    assert sql_literal(float("nan")) == "NULL"
    assert sql_literal(3) == "3"


def test_rows_are_chunked_and_missing_keys_become_null():
    rows = [{"run_id": "r", "verdict": "pass"}] * (ROWS_PER_INSERT + 1)
    statements = insert_statements("cat.sch.tbl", rows)
    assert len(statements) == 2
    assert statements[0].startswith("INSERT INTO `cat`.`sch`.`tbl`")
    assert "NULL" in statements[0]


def test_the_table_name_must_be_three_plain_parts():
    with pytest.raises(ValueError):
        create_statement("tbl")
    with pytest.raises(ValueError):
        insert_statements("a.b.c; DROP", [])


def test_write_reports_success_and_the_first_failure():
    calls = []

    def ok(sql):
        calls.append(sql)
        return {"success": True}

    done = write_rows(ok, "a.b.c", [{"verdict": "pass"}])
    assert done == {"table": "a.b.c", "written": 1, "error": ""}
    assert calls[0].startswith("CREATE TABLE IF NOT EXISTS")

    def fail_insert(sql):
        return {"success": "CREATE" in sql, "error": "denied"}

    failed = write_rows(fail_insert, "a.b.c", [{"verdict": "pass"}])
    assert failed["written"] == 0 and failed["error"] == "denied"
