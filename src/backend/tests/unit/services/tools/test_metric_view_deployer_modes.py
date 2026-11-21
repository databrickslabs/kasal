"""MetricViewDeployerTool: grant preservation, post-deploy definition check, verify and check
modes. Every SQL statement goes through a fake executor; nothing touches the network."""

import json
from unittest.mock import MagicMock, patch

from src.services.tools.metric_view_deployer_tool import MetricViewDeployerTool

YAML = """version: 0.1
source: main.sales.orders
dimensions:
  - name: region
    expr: region
measures:
  - name: total_revenue
    expr: SUM(amount)
    format:
      type: currency
  - name: order_count
    expr: COUNT(1)
"""

VIEW = "main.default.fact_sales"
GRANT_ROW = ["analysts", "SELECT"]
OK = {"success": True, "state": "SUCCEEDED"}


def _grant_rows(*rows) -> dict:
    return {
        "success": True,
        "state": "SUCCEEDED",
        "data": {"result": {"data_array": [list(r) for r in rows]}},
    }


def _describe(
    view_text: str, measures=("total_revenue", "order_count"), dims=("region",)
):
    cols = [{"name": d, "is_measure": False} for d in dims]
    cols += [{"name": m, "is_measure": True} for m in measures]
    doc = {"view_text": view_text, "columns": cols}
    return {
        "success": True,
        "state": "SUCCEEDED",
        "data": {"result": {"data_array": [[json.dumps(doc)]]}},
    }


class FakeWarehouse:
    """Routes each statement to the first response whose key appears in the SQL."""

    def __init__(self, routes: list[tuple[str, object]]):
        self.routes = routes
        self.calls: list[str] = []

    def __call__(self, sql, workspace_url, warehouse_id, headers):
        self.calls.append(sql)
        for needle, response in self.routes:
            if needle in sql:
                return response(sql) if callable(response) else response
        return OK

    def statements(self, needle: str) -> list[str]:
        return [c for c in self.calls if needle in c]


def _run(tool, warehouse: FakeWarehouse, **kwargs) -> dict:
    auth = MagicMock()
    auth.workspace_url = "https://test.azuredatabricks.net"
    auth.get_headers.return_value = {"Authorization": "Bearer t"}
    with (
        patch.object(tool, "_authenticate", return_value=auth),
        patch.object(tool, "_execute_sql_sync", side_effect=warehouse),
        patch(
            "src.services.tools.metric_view_utils.yaml_emitter._check_dangerous_sql",
            return_value=True,
        ),
    ):
        return json.loads(
            tool._run(
                yaml_specs_json=json.dumps({"fact_sales": YAML}),
                warehouse_id="wh-1",
                **kwargs,
            )
        )


def _deploy_routes(*, grants=None, describe_text=None, grant_ok=True):
    """Routes for a successful deploy; ``describe_text`` is the live view_text."""
    routes = [
        ("information_schema.table_privileges", _grant_rows(*(grants or []))),
        ("CREATE OR REPLACE VIEW", OK),
        ("DESCRIBE TABLE EXTENDED", _describe(describe_text or "pre\n" + YAML)),
    ]
    if grant_ok:
        routes.insert(0, ("GRANT ", OK))
    else:
        routes.insert(0, ("GRANT ", {"success": False, "error": "no such principal"}))
    return routes


class TestGrantPreservation:
    def test_grants_read_before_ddl_and_reapplied_after(self):
        warehouse = FakeWarehouse(_deploy_routes(grants=[GRANT_ROW]))
        data = _run(MetricViewDeployerTool(), warehouse, dry_run=False)

        names = [
            "information_schema.table_privileges",
            "CREATE OR REPLACE VIEW",
            "GRANT ",
        ]
        positions = [
            next(i for i, c in enumerate(warehouse.calls) if n in c) for n in names
        ]
        assert positions == sorted(positions)
        assert warehouse.statements("GRANT ") == [
            f"GRANT SELECT ON TABLE {VIEW} TO `analysts`"
        ]
        entry = data["deployment_results"]["fact_sales"]
        assert entry["status"] == "deployed"
        assert entry["grants"] == {
            "before": 1,
            "after": 1,
            "preserved": True,
            "failures": [],
        }

    def test_preserve_grants_false_skips_grant_handling(self):
        warehouse = FakeWarehouse(_deploy_routes(grants=[GRANT_ROW]))
        data = _run(
            MetricViewDeployerTool(), warehouse, dry_run=False, preserve_grants=False
        )
        assert warehouse.statements("information_schema.table_privileges") == []
        assert warehouse.statements("GRANT ") == []
        assert "grants" not in data["deployment_results"]["fact_sales"]

    def test_unreadable_grants_block_the_redeploy(self):
        routes = [
            (
                "information_schema.table_privileges",
                {"success": False, "http_status": 403, "error": "denied"},
            ),
            ("CREATE OR REPLACE VIEW", OK),
        ]
        warehouse = FakeWarehouse(routes)
        data = _run(MetricViewDeployerTool(), warehouse, dry_run=False)
        entry = data["deployment_results"]["fact_sales"]
        assert entry["status"] == "error"
        assert entry["http_status"] == 403
        assert "preserve_grants=false" in entry["message"]
        assert warehouse.statements("CREATE OR REPLACE VIEW") == []

    def test_failed_regrant_reports_deployed_grants_incomplete(self):
        warehouse = FakeWarehouse(_deploy_routes(grants=[GRANT_ROW], grant_ok=False))
        data = _run(MetricViewDeployerTool(), warehouse, dry_run=False)
        entry = data["deployment_results"]["fact_sales"]
        assert entry["status"] == "deployed_grants_incomplete"
        assert entry["grants"]["preserved"] is False
        assert data["summary"]["deployed_grants_incomplete"] == 1
        assert data["summary"]["deployed"] == 0

    def test_dry_run_issues_no_sql(self):
        warehouse = FakeWarehouse([])
        data = _run(MetricViewDeployerTool(), warehouse, dry_run=True)
        assert warehouse.calls == []
        assert data["deployment_results"]["fact_sales"]["status"] == "validated"


class TestPostDeployDefinitionCheck:
    def test_live_definition_equal_to_yaml_is_verified(self):
        warehouse = FakeWarehouse(_deploy_routes(describe_text="pre\n" + YAML))
        data = _run(MetricViewDeployerTool(), warehouse, dry_run=False)
        entry = data["deployment_results"]["fact_sales"]
        assert entry["status"] == "deployed"
        assert entry["schema_check"]["definition"]["verified"] is True
        assert entry["schema_check"]["definition"]["measures_without_format"] == 1

    def test_stale_live_definition_is_flagged_on_the_entry(self):
        stale = "pre\n" + YAML.replace("SUM(amount)", "SUM(legacy)")
        warehouse = FakeWarehouse(_deploy_routes(describe_text=stale))
        data = _run(MetricViewDeployerTool(), warehouse, dry_run=False)
        entry = data["deployment_results"]["fact_sales"]
        assert entry["status"] == "deployed"
        definition = entry["schema_check"]["definition"]
        assert definition["verified"] is False
        assert definition["differences"] == ["measure total_revenue (expr)"]
        assert "differs from the YAML" in entry["message"]


class TestVerifyMode:
    def test_verify_reports_field_level_differences_and_never_deploys(self):
        drifted = "pre\n" + YAML.replace("COUNT(1)", "COUNT(DISTINCT id)")
        routes = [("DESCRIBE TABLE EXTENDED", _describe(drifted))]
        warehouse = FakeWarehouse(routes)
        data = _run(MetricViewDeployerTool(), warehouse, mode="verify")
        assert data["mode"] == "verify"
        result = data["results"]["fact_sales"]
        assert result["status"] == "differs"
        assert result["differences"] == ["measure order_count (expr)"]
        assert data["summary"]["differs"] == 1
        assert warehouse.statements("CREATE OR REPLACE VIEW") == []
        assert warehouse.statements("GRANT ") == []

    def test_verify_ok_when_identical(self):
        warehouse = FakeWarehouse(
            [("DESCRIBE TABLE EXTENDED", _describe("pre\n" + YAML))]
        )
        data = _run(MetricViewDeployerTool(), warehouse, mode="verify")
        assert data["results"]["fact_sales"]["status"] == "verified"
        assert data["results"]["fact_sales"]["measures_without_format"] == 1

    def test_verify_missing_view(self):
        warehouse = FakeWarehouse(
            [("DESCRIBE TABLE EXTENDED", {"success": False, "error": "not found"})]
        )
        data = _run(MetricViewDeployerTool(), warehouse, mode="verify")
        assert data["results"]["fact_sales"]["status"] == "missing"
        assert data["summary"]["missing"] == 1


class TestCheckMode:
    def test_every_measure_runs_as_a_count(self):
        warehouse = FakeWarehouse([])
        data = _run(MetricViewDeployerTool(), warehouse, mode="check")
        measure_sql = warehouse.statements("MEASURE(")
        assert len(measure_sql) == 2
        assert all(sql.startswith("SELECT COUNT(*) FROM") for sql in measure_sql)
        result = data["results"]["fact_sales"]
        assert result["status"] == "ok"
        assert result["measures"] == 2
        assert data["summary"]["measures_failing"] == 0

    def test_failing_measure_is_reported_without_values(self):
        def route(sql):
            if "`order_count`" in sql:
                return {"success": False, "error": "window failed"}
            return {"success": True, "data": {"result": {"data_array": [["1"]]}}}

        warehouse = FakeWarehouse([("MEASURE(", route)])
        data = _run(MetricViewDeployerTool(), warehouse, mode="check")
        result = data["results"]["fact_sales"]
        assert result["status"] == "failing"
        assert result["per_measure"]["order_count"]["status"] == "fail"
        assert data["summary"]["measures_failing"] == 1

    def test_group_by_runs_each_measure_grouped(self):
        warehouse = FakeWarehouse([])
        _run(MetricViewDeployerTool(), warehouse, mode="check", group_by="region")
        assert all("GROUP BY `region`" in s for s in warehouse.statements("MEASURE("))

    def test_group_by_unknown_dimension_is_skipped(self):
        warehouse = FakeWarehouse([])
        data = _run(MetricViewDeployerTool(), warehouse, mode="check", group_by="nope")
        assert data["results"]["fact_sales"]["status"] == "skipped"
        assert warehouse.statements("MEASURE(") == []

    def test_timing_option_adds_seconds_and_slow_flag(self):
        warehouse = FakeWarehouse([])
        with patch(
            "src.services.tools.metric_view_deployer_utils.measure_check.time.perf_counter",
            side_effect=[0.0, 45.0, 0.0, 1.0],
        ):
            data = _run(
                MetricViewDeployerTool(),
                warehouse,
                mode="check",
                timing=True,
                slow_threshold_s=30,
            )
        per_measure = data["results"]["fact_sales"]["per_measure"]
        assert per_measure["total_revenue"]["status"] == "slow"
        assert per_measure["total_revenue"]["seconds"] == 45.0
        assert per_measure["order_count"]["status"] == "ok"
        assert data["results"]["fact_sales"]["slow"] == 1

    def test_unknown_mode_is_an_error(self):
        data = _run(MetricViewDeployerTool(), FakeWarehouse([]), mode="bogus")
        assert "Unknown mode" in data["error"]
