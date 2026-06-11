"""
Security regression tests for the M-Query conversion pipeline identifier
handling (P2 / findings H13 + M8).

PowerBI table & column names are attacker-controllable and were interpolated
into CREATE TABLE / INSERT executed on the SQL warehouse. These tests assert
the table name is coerced to a safe identifier, config catalog/schema are
validated, and column names are backtick-escaped.
"""
import pytest

from src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool import (
    _sanitize_sql_identifier,
    _validate_sql_identifier,
    _quote_sql_column,
)


class TestSanitizeSqlIdentifier:
    @pytest.mark.parametrize(
        "name",
        [
            "t/**/USING/**/DELTA/**/LOCATION/**/'abfss://evil/x'/**/AS/**/SELECT--",
            "t`; DROP TABLE x; --",
            "a.b.c",
            "weird name (1)",
            "Sales;DELETE",
        ],
    )
    def test_output_is_always_a_safe_identifier(self, name):
        out = _sanitize_sql_identifier(name)
        assert out and all(ch.isalnum() or ch == "_" for ch in out)
        assert not out[0].isdigit()

    def test_empty_and_leading_digit(self):
        assert _sanitize_sql_identifier("   ") == "tbl"
        assert _sanitize_sql_identifier("123abc").startswith("t_")


class TestValidateSqlIdentifier:
    def test_accepts_plain(self):
        assert _validate_sql_identifier("main") == "main"
        assert _validate_sql_identifier("my_schema") == "my_schema"

    @pytest.mark.parametrize("bad", ["main; DROP TABLE x", "a b", "a-b", "", "1abc", "a.b"])
    def test_rejects_injection(self, bad):
        with pytest.raises(ValueError):
            _validate_sql_identifier(bad, "catalog/schema")


class TestQuoteSqlColumn:
    def test_escapes_backticks_and_preserves_spaces(self):
        assert _quote_sql_column("a`b") == "`a``b`"
        assert _quote_sql_column("Sales Amount") == "`Sales Amount`"

    def test_backtick_breakout_is_neutralized(self):
        # A column trying to close the identifier and inject is fully contained.
        evil = "x` STRING) USING DELTA LOCATION 'abfss://evil' --"
        quoted = _quote_sql_column(evil)
        assert quoted.startswith("`") and quoted.endswith("`")
        # Every embedded backtick is doubled -> no early close.
        assert "``" in quoted


class TestSafeSqlType:
    """#53: LLM-influenced column types are allow-listed before use in DDL."""

    def test_allows_known_types(self):
        from src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool import _safe_sql_type
        assert _safe_sql_type("STRING") == "STRING"
        assert _safe_sql_type("bigint") == "BIGINT"
        assert _safe_sql_type("decimal(10,2)") == "DECIMAL(10,2)"

    def test_unknown_or_injected_type_defaults_to_string(self):
        from src.engines.crewai.tools.custom.mquery_conversion_pipeline_tool import _safe_sql_type
        assert _safe_sql_type("STRING) LOCATION 'abfss://evil' --") == "STRING"
        assert _safe_sql_type("EVILTYPE") == "STRING"
        assert _safe_sql_type("") == "STRING"
