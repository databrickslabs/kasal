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
    """Mapping SQL (agent input) is never stored: datasets are the metric views themselves,
    or SQL the creator builds from quoted names and gates as read-only."""

    _MAPPING = {
        "visual_id": "v1",
        "page_name": "P",
        "visual_type": "clusteredColumnChart",
        "ucmv_view": "main.sales.m`v",
        "dimensions": ["region"],
        "measures": ["total"],
        "sql": "DELETE FROM main.sales.orders",
    }

    def _datasets(self, mode):
        tool = DatabricksDashboardCreatorTool()
        return tool._build_lakeview_json([self._MAPPING], "wh", dataset_mode=mode)["datasets"]

    def test_a_metric_view_dataset_names_the_view_and_stores_no_query(self):
        [ds] = self._datasets("metric_view")
        assert ds["asset_name"] == "main.sales.m`v" and "query" not in ds

    def test_the_sql_mode_ignores_the_mapping_sql_and_quotes_the_names(self):
        [ds] = self._datasets("sql")
        assert "DELETE" not in ds["query"]
        assert ds["query"] == (
            "SELECT `region`, MEASURE(`total`) AS `total` FROM `main`.`sales`.`m``v` GROUP BY ALL"
        )
