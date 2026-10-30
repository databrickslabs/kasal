"""SQL a tool stores or falls back to is read-only and names its own target.

Apps security scan follow-up (2026-10-28):

* The dashboard creator stored any mapping ``sql`` (mappings can come straight
  from agent input) as a Lakeview dataset query that runs when the dashboard
  opens, and interpolated the view name unquoted into its default query.
* The M-Query pipeline's count fallback built ``SELECT * FROM <catalog>.<schema>.
  <table>`` from raw config and the RAW table name — while the rows were loaded
  into the sanitized one.
"""

from src.services.tools.databricks_dashboard_creator_tool import (
    DatabricksDashboardCreatorTool,
)


class TestDashboardDatasetSql:
    def _query(self, view, sql):
        tool = DatabricksDashboardCreatorTool()
        return tool._build_dataset(view, sql, "ds")["query"]

    def test_a_non_read_only_query_is_not_stored(self):
        query = self._query("main.sales.mv", "DELETE FROM main.sales.orders")
        assert query == "SELECT * FROM `main`.`sales`.`mv` LIMIT 1000"

    def test_a_read_only_query_is_kept(self):
        sql = "SELECT region, MEASURE(`total`) FROM main.sales.mv GROUP BY region"
        assert self._query("main.sales.mv", sql) == sql

    def test_the_default_query_quotes_the_view(self):
        assert self._query("main.sales.m`v", None) == (
            "SELECT * FROM `main`.`sales`.`m``v` LIMIT 1000"
        )
