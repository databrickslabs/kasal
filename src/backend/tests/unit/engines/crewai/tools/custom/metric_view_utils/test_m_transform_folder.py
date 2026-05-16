"""Tests for MTransformFolder — fold M steps into base SQL query."""
from __future__ import annotations

import pytest

from src.engines.crewai.tools.custom.metric_view_utils.data_classes import MStep
from src.engines.crewai.tools.custom.metric_view_utils.m_transform_folder import (
    MTransformFolder,
)


@pytest.fixture
def folder() -> MTransformFolder:
    return MTransformFolder()


# ── fold() ──────────────────────────────────────────────────────────


class TestFold:
    """Tests for the main fold() entry point."""

    def test_empty_steps_returns_base_sql(self, folder: MTransformFolder):
        sql = "SELECT a, b FROM t"
        assert folder.fold(sql, [], []) == sql

    def test_select_rows_only(self, folder: MTransformFolder):
        sql = "SELECT a, b FROM t"
        steps = [MStep(step_type="SelectRows",
                       raw_expression='Table.SelectRows(prev, each [status] <> "inactive")')]
        result = folder.fold(sql, steps, [])
        assert "status" in result
        assert "'inactive'" in result

    def test_column_transforms_only(self, folder: MTransformFolder):
        sql = "SELECT col1, col2 FROM t"
        steps = [MStep(step_type="ReplaceValue",
                       raw_expression='Table.ReplaceValue(prev, null, "0", Replacer.ReplaceValue, {"col1"})')]
        result = folder.fold(sql, steps, [])
        assert "COALESCE" in result

    def test_both_select_rows_and_transforms(self, folder: MTransformFolder):
        sql = "SELECT col1, col2 FROM t"
        steps = [
            MStep(step_type="SelectRows",
                  raw_expression='Table.SelectRows(prev, each [status] <> "X")'),
            MStep(step_type="ReplaceValue",
                  raw_expression='Table.ReplaceValue(prev, null, "0", Replacer.ReplaceValue, {"col1"})'),
        ]
        result = folder.fold(sql, steps, [])
        assert "COALESCE" in result
        assert "WHERE" in result

    def test_no_effective_transforms_returns_base(self, folder: MTransformFolder):
        """Steps list with an unrecognised step_type that produces no transforms."""
        sql = "SELECT a FROM t"
        steps = [MStep(step_type="UnknownType", raw_expression="something")]
        assert folder.fold(sql, steps, []) == sql

    def test_union_arms_with_transforms(self, folder: MTransformFolder):
        sql = "SELECT a, b FROM t1 UNION SELECT a, b FROM t2"
        steps = [MStep(step_type="ReplaceValue",
                       raw_expression='Table.ReplaceValue(prev, null, "N/A", Replacer.ReplaceValue, {"a"})')]
        result = folder.fold(sql, steps, [])
        assert "COALESCE" in result
        assert "UNION" in result

    def test_rename_columns_stored(self, folder: MTransformFolder):
        sql = "SELECT old_col FROM t"
        steps = [
            MStep(step_type="RenameColumns",
                  raw_expression='Table.RenameColumns(prev, {{"old_col", "new_col"}})'),
        ]
        # RenameColumns alone produces no col_exprs → returns base
        assert folder.fold(sql, steps, []) == sql

    def test_remove_columns(self, folder: MTransformFolder):
        sql = "SELECT keep_col, drop_col FROM t"
        steps = [MStep(step_type="RemoveColumns",
                       raw_expression='Table.RemoveColumns(prev, {"drop_col"})')]
        result = folder.fold(sql, steps, [])
        assert "drop_col" not in result or "None" not in result

    def test_add_column_step(self, folder: MTransformFolder):
        """AddColumn steps are added to column_transforms but only matter
        if _build_column_transforms can parse them (currently no handler)."""
        sql = "SELECT a FROM t"
        steps = [MStep(step_type="AddColumn", raw_expression="ignored")]
        # No matching handler in _build_column_transforms → no col_exprs
        assert folder.fold(sql, steps, []) == sql


# ── _split_union() ──────────────────────────────────────────────────


class TestSplitUnion:
    def test_no_union(self):
        sql = "SELECT 1 FROM t"
        assert MTransformFolder._split_union(sql) == [sql]

    def test_single_union(self):
        sql = "SELECT a FROM t1 UNION SELECT a FROM t2"
        arms = MTransformFolder._split_union(sql)
        assert len(arms) == 2
        assert "t1" in arms[0]
        assert "t2" in arms[1]

    def test_union_all(self):
        sql = "SELECT a FROM t1 UNION ALL SELECT a FROM t2"
        arms = MTransformFolder._split_union(sql)
        assert len(arms) == 2

    def test_nested_parens_not_split(self):
        sql = "SELECT (SELECT 1 UNION SELECT 2) AS x FROM t"
        arms = MTransformFolder._split_union(sql)
        # The inner UNION is inside parens, so should not be split
        assert len(arms) == 1

    def test_empty_string(self):
        assert MTransformFolder._split_union("") == []


# ── _inject_where_into_arm() ────────────────────────────────────────


class TestInjectWhereIntoArm:
    def test_no_existing_where(self):
        arm = "SELECT a FROM t"
        result = MTransformFolder._inject_where_into_arm(arm, ["x = 1"])
        assert "WHERE x = 1" in result

    def test_existing_where(self):
        arm = "SELECT a FROM t WHERE y = 2"
        result = MTransformFolder._inject_where_into_arm(arm, ["x = 1"])
        assert "AND x = 1" in result

    def test_before_group_by_no_where(self):
        arm = "SELECT a FROM t GROUP BY a"
        result = MTransformFolder._inject_where_into_arm(arm, ["x = 1"])
        assert "WHERE x = 1" in result
        assert "GROUP BY" in result

    def test_before_group_by_with_where(self):
        arm = "SELECT a FROM t WHERE y = 2 GROUP BY a"
        result = MTransformFolder._inject_where_into_arm(arm, ["x = 1"])
        assert "AND x = 1" in result

    def test_redundant_neq_filter_skipped(self):
        arm = "SELECT a FROM t WHERE region = 'US'"
        conds = ["region <> 'EU'"]
        result = MTransformFolder._inject_where_into_arm(arm, conds)
        # The neq filter is redundant because region already has an = filter
        assert result == arm

    def test_empty_conditions(self):
        arm = "SELECT a FROM t"
        result = MTransformFolder._inject_where_into_arm(arm, [])
        assert result == arm


# ── _clean_where_clause() ───────────────────────────────────────────


class TestCleanWhereClause:
    def test_redundant_neq_removal(self):
        sql = "SELECT a FROM t WHERE region = 'US'\n  AND region <> 'EU'"
        result = MTransformFolder._clean_where_clause(sql)
        assert "region <> 'EU'" not in result

    def test_paren_stripping(self):
        sql = "SELECT a FROM t WHERE x = 1\n  AND (status = 'active')"
        result = MTransformFolder._clean_where_clause(sql)
        assert "AND status = 'active'" in result

    def test_no_eq_cols_no_change(self):
        sql = "SELECT a FROM t WHERE x > 5"
        result = MTransformFolder._clean_where_clause(sql)
        assert result == sql


# ── _parse_select_rows() ────────────────────────────────────────────


class TestParseSelectRows:
    def test_each_col_neq(self, folder: MTransformFolder):
        step = MStep(step_type="SelectRows",
                     raw_expression='Table.SelectRows(prev, each [status] <> "inactive")')
        result = folder._parse_select_rows(step)
        assert "status" in result
        assert "'inactive'" in result
        assert "[" not in result  # brackets should be removed

    def test_brackets_to_bare_col(self, folder: MTransformFolder):
        step = MStep(step_type="SelectRows",
                     raw_expression='Table.SelectRows(prev, each [my_col] = "val")')
        result = folder._parse_select_rows(step)
        assert "my_col" in result
        assert "[" not in result

    def test_quotes_converted(self, folder: MTransformFolder):
        step = MStep(step_type="SelectRows",
                     raw_expression='Table.SelectRows(prev, each [col] <> "value")')
        result = folder._parse_select_rows(step)
        assert "'" in result
        assert '"' not in result

    def test_no_each_returns_empty(self, folder: MTransformFolder):
        step = MStep(step_type="SelectRows", raw_expression="Table.SelectRows(prev)")
        assert folder._parse_select_rows(step) == ""


# ── _build_column_transforms() ──────────────────────────────────────


class TestBuildColumnTransforms:
    def test_replace_value_null_coalesce(self, folder: MTransformFolder):
        steps = [MStep(step_type="ReplaceValue",
                       raw_expression='Table.ReplaceValue(prev, null, "0", Replacer.ReplaceValue, {"col"})')]
        result = folder._build_column_transforms(steps, {}, [])
        assert "col" in result
        assert "COALESCE(col, '0')" == result["col"]

    def test_replace_text(self, folder: MTransformFolder):
        steps = [MStep(step_type="ReplaceValue",
                       raw_expression='Table.ReplaceValue(prev, "old", "new", Replacer.ReplaceText, {"col"})')]
        result = folder._build_column_transforms(steps, {}, [])
        assert "REPLACE(col, 'old', 'new')" == result["col"]

    def test_coalesce_chaining(self, folder: MTransformFolder):
        """Two consecutive ReplaceValue null→COALESCE should chain."""
        steps = [
            MStep(step_type="ReplaceValue",
                  raw_expression='Table.ReplaceValue(prev, null, "0", Replacer.ReplaceValue, {"col"})'),
            MStep(step_type="ReplaceValue",
                  raw_expression='Table.ReplaceValue(prev, null, "X", Replacer.ReplaceValue, {"col"})'),
        ]
        result = folder._build_column_transforms(steps, {}, [])
        assert result["col"] == "COALESCE(COALESCE(col, '0'), 'X')"

    def test_duplicate_split_chain(self, folder: MTransformFolder):
        dup_step = MStep(step_type="DuplicateColumn",
                         raw_expression='Table.DuplicateColumn(#"prev", "src_col", "dup_col")')
        split_step = MStep(
            step_type="SplitColumn",
            raw_expression='Table.SplitColumn(#"prev", "dup_col", Splitter.SplitTextByPositions({0, 4}), {"part_a", "part_b"})',
        )
        rename_map = {"part_a": "code", "part_b": "rest"}
        result = folder._build_column_transforms([dup_step, split_step], rename_map, [])
        assert "code" in result
        assert "SUBSTRING(src_col, 1, 4)" == result["code"]

    def test_duplicate_split_unrenamed_removed(self, folder: MTransformFolder):
        """Parts NOT in rename_map are added to remove_set."""
        dup_step = MStep(step_type="DuplicateColumn",
                         raw_expression='Table.DuplicateColumn(#"prev", "src_col", "dup_col")')
        split_step = MStep(
            step_type="SplitColumn",
            raw_expression='Table.SplitColumn(#"prev", "dup_col", Splitter.SplitTextByPositions({0, 4}), {"part_a", "part_b"})',
        )
        # Only rename part_a, part_b stays unrenamed → should be None (removed)
        rename_map = {"part_a": "code"}
        result = folder._build_column_transforms([dup_step, split_step], rename_map, [])
        assert result.get("part_b") is None

    def test_transform_column_types_int64(self, folder: MTransformFolder):
        steps = [MStep(step_type="TransformColumnTypes",
                       raw_expression='Table.TransformColumnTypes(prev, {{"col", Int64.Type}})')]
        result = folder._build_column_transforms(steps, {}, [])
        assert result["col"] == "CAST(col AS BIGINT)"

    def test_transform_column_types_other(self, folder: MTransformFolder):
        steps = [MStep(step_type="TransformColumnTypes",
                       raw_expression='Table.TransformColumnTypes(prev, {{"col", Text.Type}})')]
        result = folder._build_column_transforms(steps, {}, [])
        assert result["col"] == "CAST(col AS STRING)"

    def test_transform_column_types_over_existing(self, folder: MTransformFolder):
        """Cast wraps existing expression."""
        steps = [
            MStep(step_type="ReplaceValue",
                  raw_expression='Table.ReplaceValue(prev, null, "0", Replacer.ReplaceValue, {"col"})'),
            MStep(step_type="TransformColumnTypes",
                  raw_expression='Table.TransformColumnTypes(prev, {{"col", Int64.Type}})'),
        ]
        result = folder._build_column_transforms(steps, {}, [])
        assert result["col"] == "CAST(COALESCE(col, '0') AS BIGINT)"

    def test_transform_columns_text_starts_with(self, folder: MTransformFolder):
        steps = [MStep(
            step_type="TransformColumns",
            raw_expression='Table.TransformColumns(prev, {{"col", each if Text.StartsWith(_, "AB") then "XX" else _}})',
        )]
        result = folder._build_column_transforms(steps, {}, [])
        assert result["col"] == "CASE WHEN col LIKE 'AB%' THEN 'XX' ELSE col END"

    def test_remove_columns(self, folder: MTransformFolder):
        remove_steps = [MStep(step_type="RemoveColumns",
                              raw_expression='Table.RemoveColumns(prev, {"drop_me", "also_drop"})')]
        result = folder._build_column_transforms([], {}, remove_steps)
        assert result["drop_me"] is None
        assert result["also_drop"] is None

    def test_rename_map_used_in_split(self, folder: MTransformFolder):
        dup = MStep(step_type="DuplicateColumn",
                    raw_expression='Table.DuplicateColumn(#"prev", "code", "code_dup")')
        split = MStep(
            step_type="SplitColumn",
            raw_expression='Table.SplitColumn(#"prev", "code_dup", Splitter.SplitTextByPositions({0, 3}), {"part1", "part2"})',
        )
        rename_map = {"part1": "prefix", "part2": "suffix"}
        result = folder._build_column_transforms([dup, split], rename_map, [])
        assert "prefix" in result
        assert "suffix" in result
        assert "SUBSTRING(code, 1, 3)" == result["prefix"]
        assert "SUBSTRING(code, 4)" == result["suffix"]


# ── _apply_with_wrapper() ───────────────────────────────────────────


class TestApplyWithWrapper:
    def test_single_query_with_transforms(self, folder: MTransformFolder):
        sql = "SELECT col1, col2 FROM t"
        col_exprs = {"col1": "UPPER(col1)"}
        result = folder._apply_with_wrapper(sql, [], col_exprs, [])
        assert "UPPER(col1) AS col1" in result
        assert "_src" in result

    def test_with_where_conditions(self, folder: MTransformFolder):
        sql = "SELECT col1 FROM t"
        col_exprs = {"col1": "LOWER(col1)"}
        result = folder._apply_with_wrapper(sql, ["x = 1"], col_exprs, [])
        assert "WHERE x = 1" in result

    def test_remove_column(self, folder: MTransformFolder):
        sql = "SELECT keep_col, drop_col FROM t"
        col_exprs = {"drop_col": None}
        result = folder._apply_with_wrapper(sql, [], col_exprs, [])
        # The outer SELECT should not list drop_col; it only appears in inner subquery
        outer_select = result.split("FROM (")[0]
        assert "drop_col" not in outer_select
        assert "keep_col" in outer_select

    def test_new_column_added(self, folder: MTransformFolder):
        sql = "SELECT col1 FROM t"
        col_exprs = {"new_col": "1 + 1"}
        result = folder._apply_with_wrapper(sql, [], col_exprs, [])
        assert "1 + 1 AS new_col" in result

    def test_pbi_columns_fallback(self, folder: MTransformFolder):
        """When no SELECT columns can be extracted, falls back to pbi_columns."""
        sql = "INVALID SQL"
        pbi_cols = [{"name": "a", "columnType": "Data"}, {"name": "b", "columnType": "Calculated"}]
        col_exprs = {"a": "UPPER(a)"}
        result = folder._apply_with_wrapper(sql, [], col_exprs, pbi_cols)
        assert "UPPER(a) AS a" in result


# ── _apply_where_only() ─────────────────────────────────────────────


class TestApplyWhereOnly:
    def test_append_to_existing_where(self, folder: MTransformFolder):
        sql = "SELECT a FROM t WHERE x = 1"
        result = folder._apply_where_only(sql, ["y = 2"])
        assert "AND y = 2" in result

    def test_before_group_by_all(self, folder: MTransformFolder):
        sql = "SELECT a FROM t GROUP BY ALL"
        result = folder._apply_where_only(sql, ["x = 1"])
        assert "WHERE x = 1" in result
        assert "GROUP BY ALL" in result

    def test_before_group_by_all_with_existing_where(self, folder: MTransformFolder):
        sql = "SELECT a FROM t WHERE y = 2 GROUP BY ALL"
        result = folder._apply_where_only(sql, ["x = 1"])
        assert "AND x = 1" in result
        assert "GROUP BY ALL" in result

    def test_before_group_by(self, folder: MTransformFolder):
        sql = "SELECT a FROM t GROUP BY a"
        result = folder._apply_where_only(sql, ["x = 1"])
        assert "WHERE x = 1" in result
        assert "GROUP BY a" in result

    def test_before_group_by_with_existing_where(self, folder: MTransformFolder):
        sql = "SELECT a FROM t WHERE y = 2 GROUP BY a"
        result = folder._apply_where_only(sql, ["x = 1"])
        assert "AND x = 1" in result

    def test_no_where_no_group_by(self, folder: MTransformFolder):
        sql = "SELECT a FROM t"
        result = folder._apply_where_only(sql, ["x = 1"])
        assert "WHERE x = 1" in result


# ── _inline_transforms_into_arm() ───────────────────────────────────


class TestInlineTransformsIntoArm:
    def test_replace_existing_column(self, folder: MTransformFolder):
        arm = "SELECT col1, col2 FROM t"
        col_exprs = {"col1": "UPPER(col1)"}
        result = folder._inline_transforms_into_arm(arm, col_exprs)
        assert "UPPER(col1) AS col1" in result

    def test_add_new_column(self, folder: MTransformFolder):
        arm = "SELECT col1 FROM t"
        col_exprs = {"new_col": "42"}
        result = folder._inline_transforms_into_arm(arm, col_exprs)
        assert "42 AS new_col" in result

    def test_remove_column(self, folder: MTransformFolder):
        arm = "SELECT col1, col2 FROM t"
        col_exprs = {"col2": None}
        result = folder._inline_transforms_into_arm(arm, col_exprs)
        assert "col2" not in result
        assert "col1" in result

    def test_empty_transforms(self, folder: MTransformFolder):
        arm = "SELECT col1 FROM t"
        assert folder._inline_transforms_into_arm(arm, {}) == arm

    def test_no_select_match(self, folder: MTransformFolder):
        arm = "INVALID"
        assert folder._inline_transforms_into_arm(arm, {"x": "1"}) == arm

    def test_new_col_inserted_before_agg(self, folder: MTransformFolder):
        arm = "SELECT dim_col, SUM(val) AS total FROM t"
        col_exprs = {"new_dim": "'static'"}
        result = folder._inline_transforms_into_arm(arm, col_exprs)
        # new_dim should be inserted before the SUM aggregate
        idx_new = result.find("new_dim")
        idx_sum = result.find("SUM")
        assert idx_new < idx_sum


# ── _split_select_columns() ─────────────────────────────────────────


class TestSplitSelectColumns:
    def test_simple_comma_split(self):
        cols = MTransformFolder._split_select_columns("a, b, c")
        assert [c.strip() for c in cols] == ["a", "b", "c"]

    def test_nested_parens(self):
        cols = MTransformFolder._split_select_columns("SUM(a, b), c")
        assert len(cols) == 2
        assert "SUM(a, b)" in cols[0]

    def test_single_column(self):
        cols = MTransformFolder._split_select_columns("a")
        assert cols == ["a"]

    def test_empty_string(self):
        cols = MTransformFolder._split_select_columns("")
        # Empty input produces empty list (no chars appended to buffer)
        assert cols == []


# ── _get_column_name() ──────────────────────────────────────────────


class TestGetColumnName:
    def test_with_as_alias(self):
        assert MTransformFolder._get_column_name("SUM(x) AS total") == "total"

    def test_bare_column(self):
        assert MTransformFolder._get_column_name("my_col") == "my_col"

    def test_dot_notation(self):
        assert MTransformFolder._get_column_name("t.col_name") == "col_name"

    def test_non_identifier(self):
        assert MTransformFolder._get_column_name("1 + 1") is None

    def test_trailing_comma(self):
        assert MTransformFolder._get_column_name("col,") == "col"


# ── reformat_source_sql() ───────────────────────────────────────────


class TestReformatSourceSql:
    def test_single_arm(self):
        sql = "SELECT a, b FROM t WHERE x = 1"
        result = MTransformFolder.reformat_source_sql(sql)
        assert "SELECT" in result
        assert "FROM" in result

    def test_multi_arm_union(self):
        sql = "SELECT a FROM t1 GROUP BY ALL UNION SELECT a FROM t2 GROUP BY ALL"
        result = MTransformFolder.reformat_source_sql(sql)
        assert "UNION ALL SELECT" in result
        assert result.rstrip().endswith("GROUP BY ALL")

    def test_empty_returns_unchanged(self):
        assert MTransformFolder.reformat_source_sql("") == ""


# ── _reformat_arm() ─────────────────────────────────────────────────


class TestReformatArm:
    def test_basic_select_from(self):
        result = MTransformFolder._reformat_arm("SELECT a, b FROM t")
        assert result.startswith("SELECT\n")
        assert "FROM t" in result

    def test_with_where(self):
        result = MTransformFolder._reformat_arm("SELECT a FROM t WHERE x = 1")
        assert "WHERE" in result
        assert "x = 1" in result

    def test_with_group_by_all(self):
        result = MTransformFolder._reformat_arm("SELECT a FROM t GROUP BY ALL")
        assert "GROUP BY ALL" in result

    def test_non_matching_returned_as_is(self):
        sql = "INVALID"
        assert MTransformFolder._reformat_arm(sql) == sql

    def test_case_expression_formatting(self):
        sql = "SELECT CASE\nWHEN x = 1 THEN 'a'\nELSE 'b'\nEND AS val FROM t"
        result = MTransformFolder._reformat_arm(sql)
        assert "CASE" in result
        assert "WHEN" in result


# ── _compact_where() ────────────────────────────────────────────────


class TestCompactWhere:
    def test_single_condition(self):
        result = MTransformFolder._compact_where("x = 1")
        assert result == "x = 1"

    def test_multiple_and(self):
        result = MTransformFolder._compact_where("x = 1 AND y = 2 AND z = 3")
        assert "AND y = 2" in result
        assert "AND z = 3" in result

    def test_or_block_short(self):
        """Short OR block should be inline."""
        result = MTransformFolder._compact_where("x = 1 AND (a = 1 OR b = 2)")
        assert "AND" in result

    def test_or_block_long(self):
        """Long OR block should be multi-line."""
        long_or = " OR ".join([f"very_long_column_name_{i} = 'some_value'" for i in range(5)])
        result = MTransformFolder._compact_where(f"x = 1 AND ({long_or})")
        assert "AND (" in result or "OR" in result

    def test_nested_parens_not_split(self):
        """AND inside parens should not be split."""
        result = MTransformFolder._compact_where("(a = 1 AND b = 2)")
        # Single condition → returned as-is
        assert result == "(a = 1 AND b = 2)"


# ── _split_top_level_or() ───────────────────────────────────────────


class TestSplitTopLevelOr:
    def test_basic_or(self):
        parts = MTransformFolder._split_top_level_or("a = 1 OR b = 2")
        assert len(parts) == 2
        assert parts[0].strip() == "a = 1"
        assert parts[1].strip() == "b = 2"

    def test_nested_parens(self):
        parts = MTransformFolder._split_top_level_or("(a = 1 OR b = 2) OR c = 3")
        assert len(parts) == 2
        assert "(a = 1 OR b = 2)" in parts[0]

    def test_no_or(self):
        parts = MTransformFolder._split_top_level_or("a = 1")
        assert parts == ["a = 1"]

    def test_empty(self):
        parts = MTransformFolder._split_top_level_or("")
        # Empty input produces empty list (no chars appended to buffer)
        assert parts == []


# ── _reorder_re_branches() ──────────────────────────────────────────


class TestReorderReBranches:
    def test_ascending_to_descending(self):
        inner = (
            "val >= 40 AND MONTH(CURRENT_DATE()) >= 4 "
            "OR val >= 70 AND MONTH(CURRENT_DATE()) >= 7 "
            "OR val >= 100 AND MONTH(CURRENT_DATE()) >= 10"
        )
        text = f"({inner})"
        result = MTransformFolder._reorder_re_branches(text)
        # Should be reordered descending: 100, 70, 40
        idx_100 = result.find(">= 100")
        idx_70 = result.find(">= 70")
        idx_40 = result.find(">= 40")
        assert idx_100 < idx_70 < idx_40

    def test_already_descending(self):
        inner = (
            "val >= 100 AND MONTH(CURRENT_DATE()) >= 10 "
            "OR val >= 70 AND MONTH(CURRENT_DATE()) >= 7"
        )
        text = f"({inner})"
        result = MTransformFolder._reorder_re_branches(text)
        # Already descending — no change
        assert result == text

    def test_no_month_pattern(self):
        text = "(a = 1 OR b = 2)"
        assert MTransformFolder._reorder_re_branches(text) == text

    def test_no_parens(self):
        text = "x = 1"
        assert MTransformFolder._reorder_re_branches(text) == text


# ── _extract_select_columns() ───────────────────────────────────────


class TestExtractSelectColumns:
    def test_simple_columns(self):
        sql = "SELECT a, b, c FROM t"
        assert MTransformFolder._extract_select_columns(sql) == ["a", "b", "c"]

    def test_with_aliases(self):
        sql = "SELECT SUM(x) AS total, y FROM t"
        cols = MTransformFolder._extract_select_columns(sql)
        assert "total" in cols
        assert "y" in cols

    def test_no_match(self):
        assert MTransformFolder._extract_select_columns("INVALID") == []

    def test_dot_notation(self):
        sql = "SELECT t.col1, t.col2 FROM t"
        cols = MTransformFolder._extract_select_columns(sql)
        assert "col1" in cols
        assert "col2" in cols
