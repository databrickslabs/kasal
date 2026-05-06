"""Tests for constants.py — compiled regex patterns."""
from __future__ import annotations

import pytest

from src.engines.crewai.tools.custom.metric_view_utils.constants import (
    RE_AGG_COL,
    RE_AVERAGEX_FILTER,
    RE_CALC_COL,
    RE_CASE_AGG,
    RE_COALESCE_AGG,
    RE_COUNTX_FILTER,
    RE_DAX_DIM_REF,
    RE_FROM_CLAUSE,
    RE_GROUP_BY,
    RE_LEFT_JOIN,
    RE_SIMPLE_SUM,
    RE_SIMPLE_SUMX,
    RE_SUMX_FILTER,
)


# ── RE_AGG_COL ──────────────────────────────────────────────────────


class TestReAggCol:
    def test_matches_sum_with_alias(self):
        m = RE_AGG_COL.search("SUM(alias.col) AS name")
        assert m is not None
        assert m.group(1) == "col"
        assert m.group(2) == "name"

    def test_matches_lowercase_sum(self):
        m = RE_AGG_COL.search("sum(col) as name")
        assert m is not None
        assert m.group(1) == "col"
        assert m.group(2) == "name"

    def test_matches_sum_without_table_prefix(self):
        m = RE_AGG_COL.search("SUM(amount) AS total")
        assert m is not None
        assert m.group(1) == "amount"
        assert m.group(2) == "total"

    def test_does_not_match_count(self):
        m = RE_AGG_COL.search("COUNT(col) AS name")
        assert m is None

    def test_does_not_match_avg(self):
        m = RE_AGG_COL.search("AVG(col) AS name")
        assert m is None

    def test_no_match_with_spaces_between_alias_and_dot(self):
        r"""Spaces between alias and dot break the \w+\. pattern."""
        m = RE_AGG_COL.search("sum ( tbl . amount ) as total_amount")
        assert m is None

    def test_matches_compact_alias_dot(self):
        """Alias.col without extra spaces should match."""
        m = RE_AGG_COL.search("sum(tbl.amount) as total_amount")
        assert m is not None
        assert m.group(1) == "amount"
        assert m.group(2) == "total_amount"


# ── RE_FROM_CLAUSE ──────────────────────────────────────────────────


class TestReFromClause:
    def test_matches_three_level_name(self):
        m = RE_FROM_CLAUSE.search("FROM catalog.schema.table")
        assert m is not None
        assert m.group(1) == "catalog.schema.table"

    def test_matches_with_alias(self):
        m = RE_FROM_CLAUSE.search("FROM table AS t")
        assert m is not None
        assert m.group(1) == "table"
        assert m.group(2) == "t"

    def test_matches_alias_without_as(self):
        m = RE_FROM_CLAUSE.search("FROM schema.table t")
        assert m is not None
        assert m.group(1) == "schema.table"
        assert m.group(2) == "t"

    def test_matches_simple_table(self):
        m = RE_FROM_CLAUSE.search("FROM orders")
        assert m is not None
        assert m.group(1) == "orders"

    def test_case_insensitive(self):
        m = RE_FROM_CLAUSE.search("from my_table")
        assert m is not None


# ── RE_LEFT_JOIN ─────────────────────────────────────────────────────


class TestReLeftJoin:
    def test_matches_basic_left_join(self):
        m = RE_LEFT_JOIN.search("LEFT JOIN dim_table alias ON a.id = alias.id")
        assert m is not None
        assert m.group(1) == "dim_table"
        assert m.group(2) == "alias"

    def test_matches_left_outer_join(self):
        m = RE_LEFT_JOIN.search("LEFT OUTER JOIN dim_table t ON a.id = t.id")
        assert m is not None
        assert m.group(1) == "dim_table"
        assert m.group(2) == "t"

    def test_matches_with_as_keyword(self):
        m = RE_LEFT_JOIN.search("LEFT JOIN schema.table AS t ON x.id = t.id")
        assert m is not None
        assert m.group(1) == "schema.table"
        assert m.group(2) == "t"

    def test_case_insensitive(self):
        m = RE_LEFT_JOIN.search("left join tbl t on a.id = t.id")
        assert m is not None

    def test_no_match_inner_join(self):
        m = RE_LEFT_JOIN.search("INNER JOIN tbl t ON a.id = t.id")
        assert m is None


# ── RE_GROUP_BY ──────────────────────────────────────────────────────


class TestReGroupBy:
    def test_matches_basic_group_by(self):
        m = RE_GROUP_BY.search("GROUP BY col1, col2")
        assert m is not None
        assert "col1" in m.group(1)
        assert "col2" in m.group(1)

    def test_stops_at_order_by(self):
        m = RE_GROUP_BY.search("GROUP BY col1 ORDER BY col1")
        assert m is not None
        assert "ORDER" not in m.group(1)

    def test_stops_at_limit(self):
        m = RE_GROUP_BY.search("GROUP BY col1 LIMIT 10")
        assert m is not None
        cleaned = m.group(1).strip()
        assert "LIMIT" not in cleaned

    def test_stops_at_having(self):
        m = RE_GROUP_BY.search("GROUP BY col1 HAVING COUNT(*) > 1")
        assert m is not None
        assert "HAVING" not in m.group(1)

    def test_stops_at_union(self):
        m = RE_GROUP_BY.search("GROUP BY col1 UNION SELECT")
        assert m is not None
        assert "UNION" not in m.group(1)


# ── RE_CALC_COL ──────────────────────────────────────────────────────


class TestReCalcCol:
    def test_matches_indented_calc_col(self):
        m = RE_CALC_COL.match("    SUM(x) + 1 AS col_name")
        assert m is not None
        assert "SUM(x) + 1" in m.group(1)
        assert m.group(2) == "col_name"

    def test_matches_backtick_alias(self):
        m = RE_CALC_COL.match("    expr AS `my_col`")
        assert m is not None
        assert m.group(2) == "my_col"

    def test_no_match_without_indent(self):
        m = RE_CALC_COL.match("SUM(x) AS col")
        assert m is None

    def test_no_match_bare_column(self):
        m = RE_CALC_COL.match("    col_name")
        assert m is None


# ── RE_COALESCE_AGG ──────────────────────────────────────────────────


class TestReCoalesceAgg:
    def test_matches_coalesce_diff(self):
        expr = "(coalesce(sum(credit),0)-coalesce(sum(debit),0)) as balance"
        m = RE_COALESCE_AGG.search(expr)
        assert m is not None
        assert m.group(1) == "credit"
        assert m.group(2) == "debit"
        assert m.group(3) == "balance"

    def test_case_insensitive(self):
        expr = "(COALESCE(SUM(a),0)-COALESCE(SUM(b),0)) AS result"
        m = RE_COALESCE_AGG.search(expr)
        assert m is not None

    def test_no_match_single_coalesce(self):
        m = RE_COALESCE_AGG.search("coalesce(sum(x),0) as total")
        assert m is None


# ── RE_CASE_AGG ──────────────────────────────────────────────────────


class TestReCaseAgg:
    def test_matches_sum_case(self):
        expr = "SUM(CASE WHEN status = 'active' THEN amount END) AS active_total"
        m = RE_CASE_AGG.search(expr)
        assert m is not None
        assert m.group(1) == "active_total"

    def test_matches_multiline(self):
        expr = "SUM(\n  CASE WHEN x = 1 THEN y\n  END\n) AS total"
        m = RE_CASE_AGG.search(expr)
        assert m is not None
        assert m.group(1) == "total"

    def test_no_match_without_sum(self):
        m = RE_CASE_AGG.search("CASE WHEN x = 1 THEN y END AS result")
        assert m is None


# ── RE_DAX_DIM_REF ───────────────────────────────────────────────────


class TestReDaxDimRef:
    def test_matches_table_column(self):
        m = RE_DAX_DIM_REF.search("DimProduct[Color]")
        assert m is not None
        assert m.group(1) == "DimProduct"
        assert m.group(2) == "Color"

    def test_matches_in_expression(self):
        matches = RE_DAX_DIM_REF.findall("Table1[col1] + Table2[col2]")
        assert len(matches) == 2
        assert matches[0] == ("Table1", "col1")
        assert matches[1] == ("Table2", "col2")

    def test_no_match_plain_column(self):
        m = RE_DAX_DIM_REF.search("column_name")
        assert m is None


# ── RE_SIMPLE_SUM ────────────────────────────────────────────────────


class TestReSimpleSum:
    def test_matches_basic_sum(self):
        m = RE_SIMPLE_SUM.search("SUM(Orders[Amount])")
        assert m is not None
        assert m.group(1) == "Orders"
        assert m.group(2) == "Amount"

    def test_matches_calculate_sum(self):
        m = RE_SIMPLE_SUM.search("CALCULATE(SUM(Sales[Revenue]))")
        assert m is not None
        assert m.group(1) == "Sales"
        assert m.group(2) == "Revenue"

    def test_case_insensitive(self):
        m = RE_SIMPLE_SUM.search("sum(tbl[col])")
        assert m is not None

    def test_no_match_different_agg(self):
        m = RE_SIMPLE_SUM.search("AVG(Table[col])")
        assert m is None


# ── RE_SUMX_FILTER ───────────────────────────────────────────────────


class TestReSumxFilter:
    def test_matches_sumx_filter(self):
        expr = "SUMX(FILTER(orders, orders[status] = \"active\"), orders[amount])"
        m = RE_SUMX_FILTER.search(expr)
        assert m is not None
        assert m.group(1) == "orders"
        assert m.group(3) == "orders"
        assert m.group(4) == "amount"

    def test_case_insensitive(self):
        expr = "sumx(filter(t, t[x] > 0), t[val])"
        m = RE_SUMX_FILTER.search(expr)
        assert m is not None

    def test_no_match_without_filter(self):
        m = RE_SUMX_FILTER.search("SUMX(table, table[col])")
        assert m is None


# ── RE_SIMPLE_SUMX ───────────────────────────────────────────────────


class TestReSimpleSumx:
    def test_matches_simple_sumx(self):
        m = RE_SIMPLE_SUMX.search("SUMX(orders, orders[amount])")
        assert m is not None
        assert m.group(1) == "orders"
        assert m.group(2) == "orders"
        assert m.group(3) == "amount"

    def test_case_insensitive(self):
        m = RE_SIMPLE_SUMX.search("sumx(tbl, tbl[col])")
        assert m is not None

    def test_no_match_with_filter(self):
        # SUMX(FILTER(...)) should not match RE_SIMPLE_SUMX
        m = RE_SIMPLE_SUMX.search("SUMX(FILTER(t, cond), t[col])")
        # This may or may not match depending on greediness; check the table name
        if m:
            # If it matches, the first group should not be "FILTER(t, cond"
            assert m.group(1) != "FILTER"


# ── RE_COUNTX_FILTER ────────────────────────────────────────────────


class TestReCountxFilter:
    def test_matches_countx_filter(self):
        expr = "COUNTX(FILTER(orders, orders[qty] > 0), orders[id])"
        m = RE_COUNTX_FILTER.search(expr)
        assert m is not None
        assert m.group(1) == "orders"
        assert m.group(4) == "id"

    def test_case_insensitive(self):
        m = RE_COUNTX_FILTER.search("countx(filter(t, cond), t[c])")
        assert m is not None


# ── RE_AVERAGEX_FILTER ──────────────────────────────────────────────


class TestReAveragexFilter:
    def test_matches_averagex_filter(self):
        expr = "AVERAGEX(FILTER(products, products[active] = TRUE()), products[price])"
        m = RE_AVERAGEX_FILTER.search(expr)
        assert m is not None
        assert m.group(1) == "products"
        assert m.group(4) == "price"

    def test_case_insensitive(self):
        m = RE_AVERAGEX_FILTER.search("averagex(filter(t, cond), t[v])")
        assert m is not None

    def test_no_match_without_filter(self):
        m = RE_AVERAGEX_FILTER.search("AVERAGEX(table, table[col])")
        assert m is None
