"""Generated metric-view SQL keeps model and warehouse values inside literals.

Databricks SQL processes backslash escapes in string literals, so doubling
single quotes is not enough on its own: a value ending in ``\\`` turns its
closing quote into ``\\'`` and the NEXT value is parsed as SQL. The SWITCH
filter sets also interpolated raw ``'{v}'`` into SQL the deployer executes.
Both found by the Apps security scan (2026-10-28).
"""

from src.services.tools.metric_view_utils.dax_condition_sql import (
    _sql_in_list,
    _sql_str_literal,
)
from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline

# The first value smuggles an escaped quote; the second is then read as SQL.
BREAKOUT = ["a\\", ") OR 1=1 --"]


def _outside_literals(sql: str) -> str:
    """The SQL text that is NOT inside a string literal, lexed as Databricks
    SQL does: ``\\`` escapes the next character, ``''`` is a quote."""
    out, i, in_literal = [], 0, False
    while i < len(sql):
        ch = sql[i]
        if not in_literal:
            if ch == "'":
                in_literal = True
            else:
                out.append(ch)
        elif ch == "\\":
            i += 1
        elif ch == "'":
            if sql[i + 1 : i + 2] == "'":
                i += 1
            else:
                in_literal = False
        i += 1
    return "".join(out)


class TestStringLiteralEscaping:
    def test_a_trailing_backslash_cannot_end_the_literal(self):
        assert _outside_literals(_sql_in_list(BREAKOUT)).strip() == ","

    def test_backslash_and_quote_are_both_escaped(self):
        assert _sql_str_literal("C:\\data\\") == "'C:\\\\data\\\\'"
        assert _sql_str_literal("it's") == "'it''s'"


class TestSwitchFilterSets:
    def _expr(self, num_fs, num="amount"):
        defn = {"name": "m", "num": num, "den": "qty", "num_fs": num_fs, "den_fs": "A"}
        pipeline = MetricViewPipeline(mapping=[], mquery_tables={})
        return (
            pipeline._build_switch_measure(
                defn, join_alias="d", join_col="code", filter_sets={"A": BREAKOUT}
            ).sql_expr
            or ""
        )

    def test_filter_clause_values_stay_literals(self):
        sql = self._expr("A")
        assert "OR 1=1" not in _outside_literals(sql)
        assert "IN ('a\\\\', ') OR 1=1 --')" in sql

    def test_placeholder_values_stay_literals(self):
        sql = self._expr(None, num="SUM(source.amount) FILTER {a}")
        assert "OR 1=1" not in _outside_literals(sql)
