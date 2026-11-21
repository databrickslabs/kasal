"""Settings parsing, Power BI to Databricks type mapping, literal escaping and identifiers."""

import pytest

from src.services.tools.pbi_snapshot_utils import limits
from src.services.tools.pbi_snapshot_utils.dax_paging import (
    ColumnSpec,
    page_size,
    probe_query,
    single_query_limit,
    unique_int_key,
)
from src.services.tools.pbi_snapshot_utils.limits import SnapshotSettings
from src.services.tools.pbi_snapshot_utils.type_map import (
    SnapshotRefused,
    databricks_type,
    quote_identifier,
    qualified_name,
    sql_literal,
)


class TestSettings:
    def test_default_is_off_with_default_cap(self):
        settings = SnapshotSettings.from_config({})
        assert settings.enabled is False
        assert settings.max_rows == limits.DEFAULT_MAX_ROWS == 200_000

    def test_only_true_switches_it_on(self):
        assert SnapshotSettings.from_config({"pbi_snapshot_tables": True}).enabled
        assert SnapshotSettings.from_config({"pbi_snapshot_tables": "true"}).enabled
        assert SnapshotSettings.from_config({"pbi_snapshot_tables": " TRUE "}).enabled
        assert not SnapshotSettings.from_config({"pbi_snapshot_tables": 1}).enabled
        assert not SnapshotSettings.from_config({"pbi_snapshot_tables": "yes"}).enabled

    def test_cap_must_be_a_positive_integer_within_bounds(self):
        assert (
            SnapshotSettings.from_config({"pbi_snapshot_max_rows": "5000"}).max_rows
            == 5000
        )
        with pytest.raises(ValueError):
            SnapshotSettings.from_config({"pbi_snapshot_max_rows": True})
        with pytest.raises(ValueError):
            SnapshotSettings.from_config({"pbi_snapshot_max_rows": "many"})
        with pytest.raises(ValueError):
            SnapshotSettings.from_config({"pbi_snapshot_max_rows": 0})
        with pytest.raises(ValueError):
            SnapshotSettings.from_config(
                {"pbi_snapshot_max_rows": limits.MAX_ALLOWED_ROWS + 1}
            )


class TestTypeMapping:
    @pytest.mark.parametrize(
        ("pbi", "databricks"),
        [
            ("string", "STRING"),
            ("int64", "BIGINT"),
            ("double", "DOUBLE"),
            ("decimal", "DOUBLE"),
            ("boolean", "BOOLEAN"),
            ("dateTime", "TIMESTAMP_NTZ"),
            ("DATETIME", "TIMESTAMP_NTZ"),
        ],
    )
    def test_known_types_map_explicitly(self, pbi, databricks):
        assert databricks_type(pbi) == databricks

    @pytest.mark.parametrize("pbi", ["binary", "variant", "currency", ""])
    def test_unknown_types_refuse_instead_of_becoming_text(self, pbi):
        with pytest.raises(SnapshotRefused):
            databricks_type(pbi)

    def test_string_literal_is_escaped_not_spliced(self):
        literal = sql_literal("it's a \\ trap'); DROP TABLE x; --", "string")
        assert literal.startswith("'") and literal.endswith("'")
        # the quote is doubled and the backslash escaped: nothing ends the literal early
        assert "''" in literal
        assert "\\\\" in literal

    def test_null_is_null_for_every_type(self):
        for pbi in ("string", "int64", "double", "boolean", "dateTime"):
            assert sql_literal(None, pbi) == "NULL"

    def test_int64_is_cast_from_text_and_range_checked(self):
        assert sql_literal(42, "int64") == "CAST('42' AS BIGINT)"
        with pytest.raises(SnapshotRefused):
            sql_literal(2**63, "int64")
        with pytest.raises(SnapshotRefused):
            sql_literal(1.5, "int64")
        with pytest.raises(SnapshotRefused):
            sql_literal(True, "int64")

    def test_double_is_cast_and_non_finite_refused(self):
        assert sql_literal(1.25, "double") == "CAST('1.25' AS DOUBLE)"
        with pytest.raises(SnapshotRefused):
            sql_literal(float("nan"), "double")
        with pytest.raises(SnapshotRefused):
            sql_literal(float("inf"), "decimal")

    def test_boolean_literals(self):
        assert sql_literal(True, "boolean") == "TRUE"
        assert sql_literal(False, "boolean") == "FALSE"
        with pytest.raises(SnapshotRefused):
            sql_literal(1, "boolean")

    def test_datetime_is_naive_iso_and_refused_with_timezone(self):
        assert (
            sql_literal("2024-01-31T10:20:30", "dateTime")
            == "CAST('2024-01-31 10:20:30' AS TIMESTAMP_NTZ)"
        )
        with pytest.raises(SnapshotRefused):
            sql_literal("2024-01-31T10:20:30+01:00", "dateTime")
        with pytest.raises(SnapshotRefused):
            sql_literal("31/01/2024", "dateTime")

    def test_value_of_the_wrong_kind_refuses_the_table(self):
        with pytest.raises(SnapshotRefused):
            sql_literal(5, "string")
        with pytest.raises(SnapshotRefused):
            sql_literal("5", "int64")


class TestIdentifiers:
    def test_identifier_is_backtick_quoted_with_backticks_doubled(self):
        assert quote_identifier("a`b") == "`a``b`"

    def test_hyphens_and_spaces_stay_inside_the_quotes(self):
        assert quote_identifier("acme-landing zone") == "`acme-landing zone`"

    def test_control_characters_and_empty_names_refuse(self):
        with pytest.raises(SnapshotRefused):
            quote_identifier("bad\nname")
        with pytest.raises(SnapshotRefused):
            quote_identifier("")

    def test_qualified_name_quotes_every_part(self):
        assert qualified_name("c", "s", "t") == "`c`.`s`.`t`"


class TestDaxShapes:
    def test_probe_counts_rows_and_int_keys_only(self):
        columns = [
            ColumnSpec("Name", "string"),
            ColumnSpec("Id", "int64"),
            ColumnSpec("Amount", "double"),
        ]
        dax = probe_query("Sales", columns)
        assert dax.startswith("EVALUATE ROW(")
        assert "COUNTROWS('Sales')" in dax
        assert "DISTINCTCOUNT('Sales'[Id])" in dax
        assert "COUNTBLANK('Sales'[Id])" in dax
        assert "Amount" not in dax

    def test_column_name_with_bracket_is_escaped(self):
        dax = probe_query("T", [ColumnSpec("a]b", "int64")])
        assert "'T'[a]]b]" in dax

    def test_unique_key_needs_distinct_equal_rows_and_no_blanks(self):
        columns = [ColumnSpec("Code", "int64"), ColumnSpec("Other", "int64")]
        ok = {"rows": 4, "u0": 4, "b0": 0, "u1": 2, "b1": 0}
        assert unique_int_key(columns, ok) == "Code"
        duplicates = {"rows": 4, "u0": 3, "b0": 0, "u1": 4, "b1": 0}
        assert unique_int_key(columns, duplicates) == "Other"
        blanks = {"rows": 4, "u0": 3, "b0": 1, "u1": 2, "b1": 2}
        assert unique_int_key(columns, blanks) is None

    def test_limits_follow_the_documented_values_per_query(self):
        assert single_query_limit(2) == 100_000
        assert single_query_limit(20) == 50_000
        assert page_size(20) == 10_000
        assert page_size(200) == 5_000
        with pytest.raises(SnapshotRefused):
            single_query_limit(0)
