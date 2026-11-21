"""Names, literals, timestamps and input parsing for the source-layer utils."""

from datetime import datetime, timezone

import pytest

from src.services.tools.ucmv_source_layer_utils.inputs import (
    parse_column_map,
    parse_names,
    parse_row_filters,
    parse_scope_rows,
    row_filter_for,
)
from src.services.tools.ucmv_source_layer_utils.names import (
    is_identifier,
    is_uc_fqn,
    parse_utc,
    quote_fqn,
    snake_case,
    sql_literal,
    utc_stamp,
)


def test_snake_case_matches_uc_column_naming():
    assert snake_case("Calendar Day") == "calendar_day"
    assert snake_case("  Sales (EUR) %  ") == "sales_eur"


def test_quote_fqn_quotes_each_part_including_hyphenated_catalogs():
    assert quote_fqn("cat-x.sch.tbl") == "`cat-x`.`sch`.`tbl`"


def test_uc_fqn_and_identifier_checks():
    assert is_uc_fqn("`a-b`.`c`.`d`")
    assert not is_uc_fqn("a.b")
    assert not is_uc_fqn("a.b.c.d")
    assert is_identifier("pbi_targets")
    assert not is_identifier("1abc")


def test_sql_literal_escapes_and_handles_types():
    assert sql_literal(None) == "NULL"
    assert sql_literal(True) == "TRUE"
    assert sql_literal(3) == "3"
    assert sql_literal(float("nan")) == "NULL"
    assert sql_literal("o'neil\\x") == "'o''neil\\\\x'"


def test_parse_utc_accepts_z_long_fractions_and_naive_text():
    expected = datetime(2026, 11, 4, 10, 0, 0, tzinfo=timezone.utc)
    assert parse_utc("2026-11-04T10:00:00Z") == expected
    assert parse_utc("2026-11-04T10:00:00.1234567Z").microsecond == 123456
    assert parse_utc("2026-11-04T10:00:00") == expected
    assert utc_stamp(expected) == "2026-11-04 10:00:00"


def test_parse_names_accepts_lists_and_separated_text():
    assert parse_names("a, b\nc") == ["a", "b", "c"]
    assert parse_names(["x", " ", "y"]) == ["x", "y"]
    assert parse_names(None) == []


def test_parse_column_map_rejects_non_identifiers():
    assert parse_column_map('{"Calendar Day": "calendar_day"}') == {
        "Calendar Day": "calendar_day"
    }
    with pytest.raises(ValueError, match="not an identifier"):
        parse_column_map('{"A": "bad name"}')


def test_parse_row_filters_reports_every_problem_together():
    raw = {
        "Sales": {"function": "main.gov.fn", "columns": ["company"]},
        "Bad": {"function": "nope", "constants": ["z" * 300]},
        "Worse": "not an object",
    }
    with pytest.raises(ValueError) as info:
        parse_row_filters(raw)
    message = str(info.value)
    assert "Bad:" in message and "Worse:" in message
    assert "zzzz" not in message


def test_row_filter_for_prefers_table_entry_then_star_default():
    filters = parse_row_filters(
        {
            "*": {"function": "main.gov.default_fn", "columns": ["company"]},
            "Sales": {"function": "main.gov.sales_fn", "columns": ["company"]},
        }
    )
    assert row_filter_for("Sales", filters).function == "main.gov.sales_fn"
    assert row_filter_for("Other", filters).function == "main.gov.default_fn"
    assert row_filter_for("Other", {}) is None


def test_parse_scope_rows_reads_csv_and_json():
    csv_text = "object_type,pbi_table,name,reason\nrls_filter,Sales,EU,review me\n"
    rows = parse_scope_rows(csv_text)
    assert rows == [
        {
            "object_type": "rls_filter",
            "pbi_table": "Sales",
            "name": "EU",
            "reason": "review me",
        }
    ]
    assert parse_scope_rows('[{"object_type": "measure"}]') == [
        {"object_type": "measure"}
    ]
    assert parse_scope_rows("") == []
