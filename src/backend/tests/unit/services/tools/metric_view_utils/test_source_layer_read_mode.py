"""End-to-end tests for read_from_source_layer through MetricViewPipeline.

One fact plus two dimensions: the emitted metric-view YAML must parse, and in read
mode it must reference only source-layer objects (no view-level filter, no inline
source SQL). Default mode must keep today's raw binding.
"""

from types import SimpleNamespace

import yaml

from src.services.tools.metric_view_utils.mquery_parser import MQueryParser
from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline
from src.services.tools.metric_view_utils.source_artifacts import build_source_layer

MEASURES = [
    {
        "measure_name": "Total Amount",
        "dax_expression": "SUM(Fact_Sales[amount])",
        "original_name": "Total Amount",
        "proposed_allocation": "Fact_Sales",
    }
]
MQUERY = [
    {
        "table_name": "Fact_Sales",
        "transpiled_sql": "SELECT plant_code, SUM(amount) AS amount FROM cat.sch.sales GROUP BY plant_code",
        "validation_passed": "Yes",
    },
    {
        "table_name": "Dim_Plant",
        "transpiled_sql": "SELECT plant_code, plant_name FROM cat.sch.plant",
        "validation_passed": "Yes",
    },
    {
        "table_name": "Dim_Material",
        "transpiled_sql": "SELECT material_code, mat_name FROM cat.sch.material",
        "validation_passed": "Yes",
    },
]
JOIN_KEY_MAP = {
    "Dim_Plant": {
        "alias": "dim_plant",
        "join_key": "plant_code",
        "dim_key": "plant_code",
        "source_table": "cat.sch.plant",
        "dim_columns": [{"name": "plant_name", "expr": "plant_name"}],
    }
}
CAT, SCH = "tgt", "ucmv"


def _run(config: dict) -> MetricViewPipeline:
    tables = MQueryParser().parse_json(MQUERY)
    pipeline = MetricViewPipeline(
        mapping=MEASURES,
        mquery_tables=tables,
        config={"join_key_map": JOIN_KEY_MAP, **config},
    )
    pipeline.run()
    return pipeline


def _yaml(pipeline: MetricViewPipeline) -> dict:
    out = pipeline.emit_all_yaml(catalog=CAT, schema=SCH)
    assert set(out) == {"Fact_Sales"}
    return yaml.safe_load(out["Fact_Sales"])


class TestReadMode:
    def test_default_mode_keeps_raw_binding(self):
        doc = _yaml(_run({}))
        assert doc["source"] == "cat.sch.sales"
        assert doc["joins"][0]["source"] == "cat.sch.plant"
        assert "source_layer_binding" not in _run({})._limitations

    def test_read_mode_points_fact_and_join_at_source_layer(self):
        doc = _yaml(_run({"read_from_source_layer": True}))
        assert doc["source"] == "tgt.ucmv.src_fact_sales"
        assert [(j["name"], j["source"]) for j in doc["joins"]] == [
            ("dim_plant", "tgt.ucmv.src_dim_plant")
        ]

    def test_read_mode_emits_no_view_level_filter_or_source_sql(self):
        doc = _yaml(_run({"read_from_source_layer": True}))
        assert "filter" not in doc
        assert "source_sql" not in doc
        assert isinstance(doc["source"], str)

    def test_read_mode_yaml_references_only_source_layer_objects(self):
        doc = _yaml(_run({"read_from_source_layer": True}))
        refs = [doc["source"], *(j["source"] for j in doc.get("joins", []))]
        assert refs and all(r.startswith("tgt.ucmv.src_") for r in refs)

    def test_table_name_naming_flows_into_yaml(self):
        doc = _yaml(
            _run({"read_from_source_layer": True, "source_layer_naming": "table_name"})
        )
        assert doc["source"] == "tgt.ucmv.fact_sales"
        assert doc["joins"][0]["source"] == "tgt.ucmv.dim_plant"

    def test_yaml_names_match_the_ddl_the_source_layer_creates(self):
        pipeline = _run(
            {"read_from_source_layer": True, "source_layer_naming": "table_name"}
        )
        pipeline.emit_all_yaml(catalog=CAT, schema=SCH)
        ddl = pipeline.source_layer_views
        assert "`tgt`.`ucmv`.`fact_sales`" in ddl["Fact_Sales"]["ddl"]
        assert "`tgt`.`ucmv`.`dim_plant`" in ddl["Dim_Plant"]["ddl"]
        assert "`tgt`.`ucmv`.`dim_material`" in ddl["Dim_Material"]["ddl"]

    def test_all_pbi_tables_get_views_only_in_read_mode(self):
        default = _run({})
        default.emit_all_yaml(catalog=CAT, schema=SCH)
        read = _run({"read_from_source_layer": True})
        read.emit_all_yaml(catalog=CAT, schema=SCH)
        default_views = default.source_layer_views or {}
        read_views = read.source_layer_views or {}
        assert set(default_views) == {"Fact_Sales"}
        assert set(read_views) == {"Fact_Sales", "Dim_Plant", "Dim_Material"}

    def test_source_layer_views_are_materialized_in_read_mode(self):
        pipeline = _run({"read_from_source_layer": True})
        pipeline.emit_all_yaml(catalog=CAT, schema=SCH)
        assert "MATERIALIZED VIEW" in pipeline.source_layer_views["Fact_Sales"]["ddl"]
        default = _run({})
        default.emit_all_yaml(catalog=CAT, schema=SCH)
        assert "MATERIALIZED" not in default.source_layer_views["Fact_Sales"]["ddl"]

    def test_binding_is_recorded_and_reaches_the_migration_report(self):
        pipeline = _run({"read_from_source_layer": True})
        pipeline.emit_all_yaml(catalog=CAT, schema=SCH)
        results = pipeline.get_results()
        assert "## Source-Layer Binding" in results["migration_report"]
        assert results["limitations"]["source_layer_binding"]["bound_facts"] == [
            "Fact_Sales"
        ]

    def test_default_mode_has_no_binding_section(self):
        pipeline = _run({})
        pipeline.emit_all_yaml(catalog=CAT, schema=SCH)
        assert "Source-Layer Binding" not in pipeline.get_results()["migration_report"]

    def test_invalid_naming_disables_binding_with_a_note(self):
        pipeline = _run(
            {"read_from_source_layer": True, "source_layer_naming": "bogus"}
        )
        doc = _yaml(pipeline)
        assert doc["source"] == "cat.sch.sales"
        binding = pipeline._limitations["source_layer_binding"]
        assert "source_layer_naming" in binding["disabled_reason"]
        assert "not applied" in pipeline.get_results()["migration_report"]


class TestBuildSourceLayerGuards:
    def test_table_name_mode_refuses_a_view_that_would_overwrite_its_source(self):
        specs = {"Fact_Sales": SimpleNamespace(source_table="tgt.ucmv.fact_sales")}
        mq = {
            "Fact_Sales": SimpleNamespace(
                native_query_sql="",
                source_table="tgt.ucmv.fact_sales",
                group_by_columns=[],
            )
        }
        cfg = {"source_layer_naming": "table_name"}
        out = build_source_layer(specs, mq, cfg, CAT, SCH)
        assert out["Fact_Sales"]["ddl"] == ""
        assert "overwrite" in out["Fact_Sales"]["error"]

    def test_src_prefix_mode_does_not_collide_with_the_same_source(self):
        specs = {"Fact_Sales": SimpleNamespace(source_table="tgt.ucmv.fact_sales")}
        mq = {
            "Fact_Sales": SimpleNamespace(
                native_query_sql="",
                source_table="tgt.ucmv.fact_sales",
                group_by_columns=[],
            )
        }
        out = build_source_layer(specs, mq, {}, CAT, SCH)
        assert out["Fact_Sales"]["error"] is None
        assert "`src_fact_sales`" in out["Fact_Sales"]["ddl"]

    def test_invalid_naming_reports_every_planned_view(self):
        specs = {"A": SimpleNamespace(source_table="c.s.a")}
        out = build_source_layer(specs, {}, {"source_layer_naming": "nope"}, CAT, SCH)
        assert set(out) == {"A"} and out["A"]["error"]


class TestToolStepReusesPipelineViews:
    def test_pipeline_built_views_are_reused_not_replanned(self, monkeypatch):
        from src.services.tools.uc_metric_view_generator_utils import source_layer_steps

        pipeline = _run({"read_from_source_layer": True})
        pipeline.emit_all_yaml(catalog=CAT, schema=SCH)

        def _boom(*_a, **_k):
            raise AssertionError("views must be reused, not rebuilt")

        monkeypatch.setattr(
            "src.services.tools.metric_view_utils.source_artifacts.build_source_layer",
            _boom,
        )
        outputs = source_layer_steps.build_source_layer_outputs(
            pipeline=pipeline,
            mquery_tables=pipeline.mquery_tables,
            config={"read_from_source_layer": True},
            catalog=CAT,
            schema=SCH,
            yaml_output={},
            validate_against_pbi=False,
            access_token=None,
            workspace_id=None,
            dataset_id=None,
            pbi_api_base_url=None,
        )
        assert outputs.source_layer_ddl is pipeline.source_layer_views
