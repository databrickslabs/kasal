"""Unit tests for UCMVDriftMonitorTool — input validation and I/O wiring.

The drift logic itself is covered in metric_view_utils/drift/test_drift_core.py; here
extraction, Databricks SQL and the generator are mocked at the tool's seams.
"""

import json
from unittest.mock import patch

from src.services.tools.ucmv_drift_monitor_tool import UCMVDriftMonitorTool

BASELINE = "version: 1.1\nsource: c.s.fact\nmeasures:\n  - name: revenue\n    expr: SUM(source.amount)\n    comment: 'PBI: Revenue'"


def _tool(**overrides):
    cfg = {
        "ucmv_names": "c.s.mv_sales",
        "warehouse_id": "wh-1",
        "workspace_id": "ws",
        "dataset_id": "ds",
        "tenant_id": "t",
        "client_id": "id",
        "client_secret": "secret",
        "admin_client_id": "aid",
        "admin_client_secret": "asecret",
        "llm_compare_legacy": False,
    }
    cfg.update(overrides)
    return UCMVDriftMonitorTool(**cfg)


def _describe(statement):
    doc = {"type": "METRIC_VIEW", "view_text": BASELINE}
    return {"success": True, "data": {"result": {"data_array": [[json.dumps(doc)]]}}}


def test_requires_view_names():
    out = json.loads(_tool(ucmv_names="")._run())
    assert out["drift_monitor"] is True
    assert "ucmv_names is required" in out["error"]


def test_rejects_malformed_view_names():
    out = json.loads(_tool(ucmv_names="just_a_view")._run())
    assert "not catalog.schema.view" in out["error"]


def test_requires_warehouse():
    out = json.loads(_tool(warehouse_id="")._run())
    assert "warehouse_id is required" in out["error"]


def test_empty_agent_kwargs_do_not_shadow_form_values():
    tool = _tool()
    with patch.object(
        UCMVDriftMonitorTool, "_extract", return_value={"error": "boom"}
    ) as ex:
        out = json.loads(tool._run(ucmv_names="", warehouse_id=""))
    assert ex.called
    assert "extraction failed: boom" in out["error"]


def test_extraction_uses_view_catalog_and_no_warehouse():
    tool = _tool()
    with patch(
        "src.services.tools.pipeline_config_generator_tool.PipelineConfigGeneratorTool"
    ) as cg:
        cg.return_value._run.return_value = json.dumps({"error": "stop"})
        tool._run()
    kwargs = cg.call_args.kwargs
    assert kwargs["catalog"] == "c" and kwargs["schema_name"] == "s"
    assert "warehouse_id" not in kwargs  # extraction stays the LLM-free path
    assert kwargs["client_secret"] == "secret"


def test_full_run_wires_report():
    tool = _tool()
    extraction = {
        "measures_json": [
            {
                "measure_name": "Revenue",
                "original_name": "Revenue",
                "dax_expression": "SUM(S[Amount])",
                "proposed_allocation": "Fact",
            }
        ],
        "summary": {"measures_extracted": 1, "dax_degraded": False},
        "warnings": ["config-gen note"],
    }
    with (
        patch.object(UCMVDriftMonitorTool, "_extract", return_value=extraction),
        patch.object(UCMVDriftMonitorTool, "_sql_fn", return_value=_describe),
    ):
        out = json.loads(tool._run())
    assert out["summary"]["views_checked"] == 1
    # legacy (untagged fingerprint) measure, LLM check disabled → stays unverified
    assert out["summary"]["legacy_unverified"] == 1
    assert out["semantic_check"]["enabled"] is False
    assert out["pbi"]["dataset_id"] == "ds"
    assert "config-gen note" in out["warnings"]


def test_degraded_dax_adds_warning_first():
    tool = _tool()
    extraction = {"measures_json": [], "summary": {"dax_degraded": True}}
    with (
        patch.object(UCMVDriftMonitorTool, "_extract", return_value=extraction),
        patch.object(UCMVDriftMonitorTool, "_sql_fn", return_value=_describe),
    ):
        out = json.loads(tool._run())
    assert out["warnings"][0].startswith("Power BI returned bare DAX")


def test_connection_failure_is_reported():
    tool = _tool()
    with (
        patch.object(
            UCMVDriftMonitorTool, "_extract", return_value={"measures_json": []}
        ),
        patch.object(
            UCMVDriftMonitorTool, "_sql_fn", side_effect=RuntimeError("auth failed")
        ),
    ):
        out = json.loads(tool._run())
    assert "cannot connect to Databricks: auth failed" in out["error"]


def test_registered_in_tool_factory():
    from src.services.tools import tool_factory

    assert tool_factory.UCMVDriftMonitorTool is UCMVDriftMonitorTool
