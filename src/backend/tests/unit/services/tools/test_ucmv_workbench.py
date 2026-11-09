"""UCMV Workbench: working copy, one-measure edits, sandbox deploy + reconcile loop, classification
and export, with the deployer and the reconciliation tool faked."""

import json
from unittest.mock import patch

import pytest
import yaml

from src.services.tools.ucmv_workbench_tool import UCMVWorkbenchTool
from src.services.tools.ucmv_workbench_utils import edits
from src.services.tools.ucmv_workbench_utils.state import Workbench

VIEW_YAML = """version: '1.1'
source: cat.sch.fact_sales
dimensions:
  - name: region
    expr: source.region
measures:
  - name: revenue
    expr: SUM(source.amount)
    format:
      type: number
"""
MAPPING = {
    "binding": {"ucmv_table": "cat.sch.fact_sales", "pbi_semantic_model_id": "m", "pbi_fact_table": "Sales",
                "time_dimension": {}, "default_dimension": "region", "dimensions": {"region": {}}},
    "measures": [
        {"ucmv_measure": "revenue", "pbi_measure": "Revenue", "pbi_kind": "direct"},
        {"ucmv_measure": "margin_pct", "pbi_measure": "Margin %", "pbi_kind": "unresolved"},
    ],
}
GENERATOR_OUTPUT = {
    "yaml": {"Fact_Sales": VIEW_YAML},
    "pbi_ucmv_mapping": {"Fact_Sales": yaml.safe_dump(MAPPING)},
    "untranslatable_items": [
        {"table_key": "Fact_Sales", "original_name": "Margin %", "dax_expression": "DIVIDE([Margin], [Revenue])",
         "skip_reason": "measure reference", "category": "ratio", "proposal": "SUM(margin) / SUM(amount)",
         "used_in_visuals": [{"page": "P"}]},
    ],
}


def test_set_measure_adds_and_replaces_one_measure_and_keeps_its_format():
    text, replaced = edits.set_measure(VIEW_YAML, "margin_pct", "SUM(source.margin) / SUM(source.amount)")
    assert not replaced and edits.measure_names(text) == ["revenue", "margin_pct"]
    text, replaced = edits.set_measure(text, "revenue", "SUM(source.net_amount)")
    entry = edits.measure_entry(text, "revenue")
    assert replaced and entry["expr"] == "SUM(source.net_amount)" and entry["format"] == {"type": "number"}


@pytest.mark.parametrize("name, expr", [("bad name", "SUM(x)"), ("m", ""), ("m", "SUM(x); DROP TABLE t")])
def test_set_measure_refuses_bad_names_empty_and_dangerous_sql(name, expr):
    with pytest.raises(edits.EditError):
        edits.set_measure(VIEW_YAML, name, expr)


def test_mapping_is_rebound_to_the_sandbox_and_new_measures_resolve():
    mapping = edits.rebind_mapping(MAPPING, "sandbox.work.fact_sales")
    assert mapping["binding"]["ucmv_table"] == "sandbox.work.fact_sales"
    mapping = edits.upsert_mapping_measure(mapping, "margin_pct", "Margin %")
    entry = next(m for m in mapping["measures"] if m["ucmv_measure"] == "margin_pct")
    assert entry["pbi_kind"] == "direct"
    mapping = edits.upsert_mapping_measure(mapping, "units", "Units")
    assert mapping["measures"][-1] == {"ucmv_measure": "units", "pbi_measure": "Units", "pbi_kind": "direct"}
    with pytest.raises(edits.EditError):
        edits.rebind_mapping(MAPPING, "not a name; DROP")


def test_gaps_close_by_translation_reconciliation_or_classification():
    bench = Workbench.from_generator_output(GENERATOR_OUTPUT)
    gaps = bench.gaps("fact_sales")  # key lookup is case/format tolerant
    assert [g["pbi_measure"] for g in gaps["untranslated"]] == ["Margin %"]
    assert gaps["untranslated"][0]["used_in_visuals"] == 1
    bench.set_measure("Fact_Sales", "margin_pct", "SUM(source.margin) / SUM(source.amount)", "Margin %")
    assert bench.gaps("Fact_Sales")["untranslated"] == []
    bench.record_reconciliation("Fact_Sales", {"revenue": {"pct_aligned": 100.0}, "margin_pct": {"pct_aligned": 92.5}})
    assert [g["measure"] for g in bench.gaps("Fact_Sales")["below_100"]] == ["margin_pct"]
    assert bench.report()["done"] is False
    with pytest.raises(edits.EditError):
        bench.classify("Fact_Sales", "margin_pct", "data_drift", "")
    bench.classify("Fact_Sales", "margin_pct", "data_drift", "PBI refreshed after the UC load; last period only")
    report = bench.report()
    assert report["done"] is True and report["open_gaps"] == 0
    assert report["classified"][0]["classification"] == "data_drift"


def _tool():
    return UCMVWorkbenchTool(
        ucmv_output=json.dumps(GENERATOR_OUTPUT), catalog="sandbox", schema_name="work", warehouse_id="wh",
        workspace_id="ws", dataset_id="ds", tenant_id="t", client_id="c", client_secret="s",
    )


def _call(tool, **kwargs):
    return json.loads(tool._run(**kwargs))


def test_the_loop_deploys_to_the_sandbox_reconciles_and_exports():
    tool = _tool()
    deployed, reconciled = [], []

    def fake_deploy(self, **kwargs):
        deployed.append(kwargs)
        return json.dumps({"deployment_results": {"Fact_Sales": {"status": "deployed"}}, "summary": {"deployed": 1}})

    def fake_reconcile(self, **kwargs):
        reconciled.append((dict(self._default_config), kwargs))
        return json.dumps({"views": {"sandbox.work.fact_sales": {"measures": {
            "revenue": {"pct_aligned": 100.0, "classification": "aligned"},
            "margin_pct": {"pct_aligned": 100.0, "classification": "aligned"},
        }, "summary": {"measures_at_100": 2}}}})

    with patch("src.services.tools.metric_view_deployer_tool.MetricViewDeployerTool._run", fake_deploy), \
         patch("src.services.tools.ucmv_reconciliation_tool.UCMVReconciliationTool._run", fake_reconcile):
        assert _call(tool, action="overview")["views"][0]["untranslated_open"] == 1
        assert "deploy this view first" in _call(tool, action="reconcile", view="Fact_Sales")["error"]
        out = _call(tool, action="set_measure", view="Fact_Sales", measure="margin_pct",
                    expr="SUM(source.margin) / SUM(source.amount)", pbi_measure="Margin %")
        assert out["change"] == "added"
        assert _call(tool, action="deploy", view="Fact_Sales")["status"] == "deployed"
        assert _call(tool, action="reconcile", view="Fact_Sales")["measures"]["margin_pct"]["pct_aligned"] == 100.0
        export = _call(tool, action="export")

    assert deployed[0]["catalog"] == "sandbox" and deployed[0]["schema_name"] == "work"
    assert "margin_pct" in json.loads(deployed[0]["yaml_specs_json"])["Fact_Sales"]
    config, kwargs = reconciled[0]
    assert config["statistics_only"] is True and config["pbi_dataset_id"] == "ds"
    mapping = json.loads(kwargs["mappings_json"])["sandbox.work.fact_sales"]
    assert mapping["binding"]["ucmv_table"] == "sandbox.work.fact_sales"
    assert export["finishing_report"]["done"] is True
    assert "margin_pct" in export["yaml"]["Fact_Sales"]


def test_errors_come_back_to_the_agent_instead_of_raising():
    tool = _tool()
    assert "no view" in _call(tool, action="gaps", view="Nope")["error"]
    assert "unknown action" in _call(tool, action="explode")["error"]
    empty = UCMVWorkbenchTool(ucmv_output=json.dumps({"yaml": {}}))
    with patch("src.services.tools.metric_view_validator_tool.MetricViewValidatorTool._fetch_latest_ucmv_from_db",
               return_value={}):
        assert "UC Metric View Generator first" in _call(empty, action="overview")["error"]
