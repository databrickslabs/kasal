"""The verify-numbers loop creates the source layer before the metric views (read mode), in
dependency order, once per DDL change, and reports every view it could not create."""

import json
from unittest.mock import patch

from src.services.tools.metric_view_deployer_tool import MetricViewDeployerTool
from src.services.tools.ucmv_iterative_recon import run_iterative_ucmv_generation
from src.services.tools.ucmv_recon_source_layer import (
    deploy_source_layer,
    planned_views,
)

DDL = {
    "C_Dim_calendar": {
        "ddl": "CREATE OR REPLACE VIEW `cat`.`sch`.`c_dim_calendar` AS SELECT 1",
        "error": None,
    },
    "Fact": {
        "ddl": "CREATE OR REPLACE VIEW `cat`.`sch`.`fact` AS SELECT * FROM `cat`.`sch`.`c_dim_calendar`"
    },
    "Sheet1": {"ddl": "", "error": "Excel source: load a snapshot first"},
}


def _session(statements, fail_on=()):
    def runner(sql):
        statements.append(sql)
        if any(name in sql for name in fail_on) and sql.startswith(
            "CREATE OR REPLACE VIEW"
        ):
            return {"success": False, "error": "[TABLE_OR_VIEW_NOT_FOUND] udm.x"}
        return {"success": True}

    return lambda self, opts: (runner, None)


def test_views_are_read_from_the_ddl_and_unemitted_ones_are_named():
    views, failures = planned_views(DDL)
    assert views["Fact"][0] == "`cat`.`sch`.`fact`"
    assert failures == {"Sheet1": "not emitted: Excel source: load a snapshot first"}


def test_dependencies_first_and_a_failure_does_not_stop_the_rest():
    statements = []
    with patch.object(
        MetricViewDeployerTool,
        "_open_session",
        _session(statements, fail_on=("`fact`",)),
    ):
        report = deploy_source_layer(
            MetricViewDeployerTool(),
            DDL,
            {"catalog": "cat", "schema_name": "sch", "warehouse_id": "wh"},
        )
    creates = [s for s in statements if s.startswith("CREATE OR REPLACE VIEW")]
    assert "c_dim_calendar" in creates[0] and "`fact`" in creates[1]
    assert statements[0].startswith("CREATE SCHEMA IF NOT EXISTS")
    assert report["deployed"] == ["C_Dim_calendar"]
    assert set(report["failures"]) == {"Fact", "Sheet1"}


class _Generator:
    def __init__(self):
        self.calls = 0

    def _run(self, **kwargs):
        self.calls += 1
        return json.dumps(
            {
                "yaml": {"Fact": "yaml"},
                "pbi_ucmv_mapping": {"Fact": "mapping"},
                "source_layer_ddl": DDL,
            }
        )


def test_in_read_mode_the_loop_creates_the_source_layer_once_and_reports_it():
    statements, deploys = [], []

    def fake_deploy(self, **kwargs):
        deploys.append(kwargs)
        return json.dumps({"deployment_results": {"Fact": {"status": "deployed"}}})

    class _Reconciler:
        def __init__(self):
            self.n = 0

        def _run(self, **kwargs):
            self.n += 1
            return json.dumps(
                {
                    "views": {},
                    "overall": {"cells_total": 2, "overall_cell_pct": 50.0 + self.n},
                }
            )

    with (
        patch.object(MetricViewDeployerTool, "_open_session", _session(statements)),
        patch.object(MetricViewDeployerTool, "_run", fake_deploy),
    ):
        out = run_iterative_ucmv_generation(
            generator=_Generator(),
            deployer=MetricViewDeployerTool(),
            reconciler=_Reconciler(),
            gen_kwargs={"config_json": json.dumps({"read_from_source_layer": True})},
            deploy_kwargs={
                "catalog": "cat",
                "schema_name": "sch",
                "warehouse_id": "wh",
            },
            max_cycles=2,
        )
    creates = [s for s in statements if s.startswith("CREATE OR REPLACE VIEW")]
    assert len(creates) == 2  # unchanged DDL in cycle 2 is not deployed again
    assert len(deploys) == 2
    cycle = out["cycles"][0]
    assert cycle["note"].startswith("source layer: 2 of 3 views created")
    assert "source_layer:Sheet1" in cycle["deploy_failures"]


def test_without_read_mode_the_source_layer_is_left_alone():
    statements = []
    with (
        patch.object(MetricViewDeployerTool, "_open_session", _session(statements)),
        patch.object(
            MetricViewDeployerTool,
            "_run",
            lambda self, **k: json.dumps(
                {"deployment_results": {"Fact": {"status": "deployed"}}}
            ),
        ),
    ):
        run_iterative_ucmv_generation(
            generator=_Generator(),
            deployer=MetricViewDeployerTool(),
            reconciler=type(
                "R",
                (),
                {"_run": lambda self, **k: json.dumps({"views": {}, "overall": {}})},
            )(),
            max_cycles=1,
        )
    assert statements == []
