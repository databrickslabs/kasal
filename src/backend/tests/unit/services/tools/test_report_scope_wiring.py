"""Scope wiring across the tools: tracked-table rows, page filters into the mapping draft, scope problems."""

import json
from types import SimpleNamespace

from src.services.tools.metric_view_utils.data_classes import MetricViewSpec, TranslationResult
from src.services.tools.metric_view_utils.drift.semantic_step import run_semantic_sync
from src.services.tools.metric_view_utils.pbi_ucmv_mapping import derive_pbi_ucmv_mapping
from src.services.tools.pipeline_config_generator_utils.scope_outputs import (
    parse_tracked_tables,
    report_scope,
    tracked_rows,
)
from src.services.tools.uc_metric_view_generator_utils.visual_usage_triage import stamp_page_filters


class _Response:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body

    def json(self):
        return self._body


def _measure(name, original):
    return TranslationResult(
        measure_name=name,
        original_name=original,
        sql_expr="SUM(source.x)",
        is_translatable=True,
        skip_reason="",
        dax_expression="SUM(Fact[x])",
        confidence="high",
        category="single_table",
    )


def _spec(*measures):
    return MetricViewSpec(
        fact_table_key="fact",
        source_table="c.s.fact",
        view_name="ucm_fact",
        comment="",
        joins=[],
        dimensions=[],
        measures=list(measures),
        untranslatable=[],
    )


def test_tracked_table_names_and_rows():
    assert parse_tracked_tables('["Status", "Map"]') == ["Status", "Map"]
    assert parse_tracked_tables("Status, Map\nOther") == ["Status", "Map", "Other"]
    queries, warnings = [], []

    def dax(query):
        queries.append(query)
        if "Broken" in query:
            return _Response(400, {})
        return _Response(200, {"results": [{"tables": [{"rows": [{"T[Kpi]": "A", "T[Status]": "On"}]}]}]})

    rows = tracked_rows(["KPI's Status", "Broken"], dax, warnings)
    assert queries[0] == "EVALUATE TOPN(2000, 'KPI''s Status')"
    assert rows == {"KPI's Status": [{"T[Kpi]": "A", "T[Status]": "On"}]}
    assert warnings == ["tracked table 'Broken' not read (HTTP 400)"]


def test_report_scope_needs_both_parts_and_writes_page_filters_into_the_config():
    config = {}
    assert report_scope([], None, [], config) == {}
    assert config == {}


def test_page_filters_reach_the_mapping_draft_as_extra_filter():
    output = _measure("output", "Output")
    plain = _measure("plain", "Plain")
    pipeline = SimpleNamespace(
        config={"page_filters_by_measure": {"Output": [{"table": "Line", "column": "type", "values": ["A", "B"]}]}},
        all_specs={"fact": _spec(output, plain)},
    )
    assert stamp_page_filters(pipeline) == 1
    draft = derive_pbi_ucmv_mapping(pipeline.all_specs)["ucm_fact"]
    output_entry = draft.split("- ucmv_measure: output")[1].split("- ucmv_measure:")[0]
    plain_entry = draft.split("- ucmv_measure: plain")[1]
    assert "extra_filter:" in output_entry and "column: type" in output_entry
    assert "extra_filter:" not in plain_entry


def test_a_signed_off_scope_is_checked_against_the_model_and_the_deployed_views():
    snapshot = {"objects": [{"object_type": "measure", "pbi_table": "Fact", "name": "Sales", "definition": "", "definition_hash": "1"}]}
    scope = [
        {"object_type": "measure", "pbi_table": "Fact", "name": "Sales", "decision": "keep", "mirrored_as": "ucm_fact.salez"},
        {"object_type": "measure", "pbi_table": "Fact", "name": "Gone", "decision": "keep", "mirrored_as": "ucm_fact.sales"},
    ]
    drift = {"views": [{"name": "ucm_fact", "baseline_yaml": "measures:\n  - name: sales\n    expr: SUM(source.x)\n"}]}
    out = run_semantic_sync(snapshot, baseline=json.dumps(snapshot), scope=scope, drift_report=drift)
    text = "\n".join(out["scope_problems"])
    assert "'ucm_fact.salez' is not a measure of the deployed metric views" in text
    assert "measure Fact[Gone]: the model no longer has this measure" in text
