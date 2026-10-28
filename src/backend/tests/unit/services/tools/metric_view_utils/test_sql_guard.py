"""SQL that Kasal executes from Power BI content stays inside its guards.

Apps security scan (2026-10-28): the M-Query pipeline interpolated DATE and
TIMESTAMP row values raw into the INSERTs it runs (strings escaped quotes but
not backslashes), executed transpiled / LLM-rewritten SQL with no read-only
check, and the dashboard mapper's fallback queries skipped the safe-SELECT gate
the LLM path applies.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.tools.metric_view_utils.sql_guard import (
    is_read_only_select,
    sql_value_literal,
)
from src.services.tools.mquery_conversion_pipeline_tool import (
    MqueryConversionPipelineTool,
)
from src.services.tools.pbi_visual_ucmv_mapper_tool import PBIVisualUCMVMapperTool
from tests.unit.services.tools.metric_view_utils.test_sql_literal_security import (
    _outside_literals,
)


class TestTypedValues:
    def test_date_and_timestamp_values_are_escaped(self):
        assert sql_value_literal("2024'); DR", "DATE") == "DATE '2024''); DR'"
        hostile = "2024-01-01' || (SELECT 1) || '"
        rendered = sql_value_literal(hostile, "TIMESTAMP")
        assert rendered.startswith("TIMESTAMP '")
        assert "SELECT" not in _outside_literals(rendered)

    def test_a_string_ending_in_backslash_cannot_end_its_literal(self):
        row = ", ".join(sql_value_literal(v, "STRING") for v in ["a\\", ") OR 1=1 --"])
        assert "OR 1=1" not in _outside_literals(row)

    def test_numbers_booleans_and_nulls_are_rerendered(self):
        assert sql_value_literal("42.0", "BIGINT") == "42"
        assert sql_value_literal("1; DROP", "INT") == "NULL"
        assert sql_value_literal("2.5", "DOUBLE") == "2.5"
        assert sql_value_literal("yes", "BOOLEAN") == "TRUE"
        assert sql_value_literal(None, "STRING") == "NULL"


class TestReadOnlySelect:
    def test_allows_selects_and_ctes(self):
        assert is_read_only_select("SELECT a FROM t")
        assert is_read_only_select("WITH x AS (SELECT 1) SELECT * FROM x;")

    def test_allows_a_parenthesized_subquery_body(self):
        # _extract_select_body returns a subquery body as-is: "(SELECT …)".
        assert is_read_only_select("(SELECT a FROM t) UNION ALL (SELECT a FROM u)")
        assert not is_read_only_select("(DELETE FROM t)")

    def test_refuses_writes_stacks_and_breakouts(self):
        for sql in ("DELETE FROM t", "SELECT 1; DROP TABLE t", "SELECT $$x$$", ""):
            assert not is_read_only_select(sql)


class TestMqueryValidationRunsOnlyReadOnlySql:
    def _validate(self, tool, sql):
        conv = MagicMock(databricks_sql=sql, create_view_sql=None)
        conv.original_expression = "let Source = ...\nin\nSource"
        return asyncio.run(
            tool._validate_databricks_table(
                "T1",
                conv,
                "ws",
                "ds",
                "tok",
                "https://workspace.example.com",
                "wh",
                "pat",
                3,
                {},
            )
        )

    def test_a_non_select_is_never_executed(self):
        tool = MqueryConversionPipelineTool()
        with patch.object(tool, "_run_dbsql", new_callable=AsyncMock) as run:
            result = self._validate(tool, "SELECT 1 FROM t; DROP TABLE t")
        assert result["status"] == "skipped"
        run.assert_not_called()

    def test_an_llm_correction_is_checked_before_it_runs(self):
        tool = MqueryConversionPipelineTool()
        failed = {"success": False, "error": "syntax error"}
        with (
            patch.object(
                tool, "_run_dbsql", new_callable=AsyncMock, return_value=failed
            ) as run,
            patch.object(
                tool,
                "_llm_correct_sql",
                new_callable=AsyncMock,
                return_value="DELETE FROM t",
            ),
        ):
            result = self._validate(tool, "SELECT * FROM t")
        assert result["status"] == "skipped"
        assert run.await_count == 1  # the original only; the rewrite never ran


class TestDashboardFallbackSql:
    def _sql(self, view, measures, dims, visual_type="table"):
        tool = PBIVisualUCMVMapperTool()
        visuals = [{"visual_id": "v1", "page_name": "p", "visual_type": visual_type}]
        summaries = [{"view_name": view, "measures": measures, "dimensions": dims}]
        return tool._fallback_mapping(visuals, summaries)[0]["sql"]

    def test_names_are_quoted(self):
        sql = self._sql("main.sales.mv", ["Total Sales"], ["region"])
        assert sql == (
            "SELECT `region`, MEASURE(`Total Sales`) FROM `main`.`sales`.`mv` "
            "GROUP BY `region`"
        )

    def test_a_name_closing_the_quote_stays_one_identifier(self):
        sql = self._sql("main.sales.mv", ["x`) FROM other_table --"], ["d"])
        assert "MEASURE(`x``) FROM other_table --`)" in sql

    def test_a_query_failing_the_gate_is_dropped(self):
        assert self._sql("main.sales.mv", ["m"], ["d; DROP TABLE t"]) is None
