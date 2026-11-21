"""setup_access building blocks: mapping-row parsing, statements, dry-run and runner."""

import json

import pytest

from src.services.tools.ucmv_source_layer_utils.row_access import (
    access_setup_statements,
    parse_access_rows,
    run_setup,
    setup_plan,
)
from src.services.tools.ucmv_source_layer_utils.sql_types import SqlError, SqlResult


def _rows(raw):
    return parse_access_rows(json.dumps(raw))


def test_statements_create_the_objects_in_order_with_the_reference_bodies():
    statements = access_setup_statements("cat.common", [], [])
    assert statements[0].startswith("CREATE SCHEMA IF NOT EXISTS `cat`.`common`")
    assert statements[1].startswith(
        "CREATE TABLE IF NOT EXISTS `cat`.`common`.`row_access_map`"
    )
    assert (
        "CREATE OR REPLACE FUNCTION `cat`.`common`.`object_access`(allowed_groups STRING)"
        in statements[2]
    )
    assert "PERMISSION_DENIED" in statements[2]
    assert (
        "CREATE OR REPLACE FUNCTION `cat`.`common`.`value_access`(access_field STRING, row_value STRING)"
        in statements[3]
    )
    assert "is_account_group_member(m.group_name)" in statements[3]
    assert len(statements) == 4  # no grants or merge without reader groups or rows


def test_grants_per_reader_group_are_deduplicated():
    statements = access_setup_statements("cat.common", ["grp_a", "grp_a", "grp_b"], [])
    grants = [s for s in statements if s.startswith("GRANT")]
    assert len(grants) == 6
    assert "GRANT SELECT ON TABLE `cat`.`common`.`row_access_map` TO `grp_b`" in grants


def test_access_row_values_are_escaped_and_null_reason_is_null():
    rows = _rows(
        [
            {
                "group_name": "grp'x",
                "field": "company_code",
                "value": "a'); DROP TABLE t; --",
            },
            {"group_name": "grp_a", "field": "country", "value": "*", "reason": "it's"},
        ]
    )
    merge = access_setup_statements("cat.common", [], rows)[-1]
    assert merge.startswith("MERGE INTO `cat`.`common`.`row_access_map`")
    assert "'grp''x'" in merge
    assert "'a''); DROP TABLE t; --'" in merge
    assert "'it''s'" in merge
    assert "CAST(NULL AS STRING)" in merge


def test_repeated_mapping_key_keeps_the_last_row():
    rows = _rows(
        [
            {"group_name": "g", "field": "f", "value": "1", "reason": "old"},
            {"group_name": "g", "field": "f", "value": "1", "reason": "new"},
        ]
    )
    assert len(rows) == 1 and rows[0].reason == "new"


@pytest.mark.parametrize(
    "raw, message",
    [
        ([{"group_name": "", "field": "f", "value": "1"}], "group_name"),
        ([{"group_name": "a`b", "field": "f", "value": "1"}], "group_name"),
        ([{"group_name": "g", "field": "bad field", "value": "1"}], "plain identifier"),
        ([{"group_name": "g", "field": "f", "value": ""}], "value is required"),
    ],
)
def test_invalid_mapping_rows_name_the_fix(raw, message):
    with pytest.raises(ValueError, match=message):
        _rows(raw)


def test_dry_run_returns_statements_without_executing():
    plan = setup_plan(access_setup_statements("cat.common", [], []))
    assert plan["mode"] == "dry_run"
    assert len(plan["statements"]) == 4


def test_run_setup_stops_at_first_failure_and_reports_it():
    seen = []

    def sql(statement):
        seen.append(statement)
        if "FUNCTION `cat`.`common`.`value_access`" in statement:
            raise SqlError("permission denied")
        return SqlResult()

    result = run_setup(access_setup_statements("cat.common", ["g"], []), sql)
    assert result["all_applied"] is False
    assert result["steps"][-1]["status"] == "failed"
    assert result["steps"][-1]["error"] == "permission denied"
    assert len(seen) == 4  # schema, table, object_access, value_access (failed) -> stop
