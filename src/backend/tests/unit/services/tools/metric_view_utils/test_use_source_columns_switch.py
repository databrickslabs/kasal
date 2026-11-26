"""The switch ``use_source_columns`` (pipeline config, default on): off, the generator ignores the
source views' columns it is given, so the output is the same as a run that was never given them.
"""

import json

import yaml

from src.services.tools.metric_view_utils.mquery_parser import MQueryParser
from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline
from src.services.tools.metric_view_utils.source_columns import source_columns_enabled
from src.services.tools.uc_metric_view_generator_tool import UCMetricViewGeneratorTool

MQUERY = [
    {
        "table_name": "Fact_Sales",
        "transpiled_sql": "SELECT plant_code, source, SUM(amount) AS amount FROM cat.sch.sales GROUP BY plant_code, source",
        "validation_passed": "Yes",
    },
]
MEASURES = [
    {
        "measure_name": "Source Rows",
        "dax_expression": "COUNT(Fact_Sales[source])",
        "original_name": "Source Rows",
        "proposed_allocation": "Fact_Sales",
    },
]
COLUMNS = {"Fact_Sales": ["plant_code", "source", "amount"]}


def test_the_switch_defaults_to_on_and_reads_off_values():
    assert source_columns_enabled({}) is True
    assert source_columns_enabled({"use_source_columns": True}) is True
    assert source_columns_enabled({"use_source_columns": False}) is False
    assert source_columns_enabled({"use_source_columns": "off"}) is False
    assert source_columns_enabled({"use_source_columns": "true"}) is True


def _pipeline(config: dict) -> MetricViewPipeline:
    pipeline = MetricViewPipeline(
        mapping=[dict(m) for m in MEASURES],
        mquery_tables=MQueryParser().parse_json(MQUERY),
        config={"read_from_source_layer": True, **config},
        source_columns=COLUMNS,
    )
    pipeline.run()
    return pipeline


def test_off_the_pipeline_keeps_no_columns_and_renames_nothing():
    pipeline = _pipeline({"use_source_columns": False})
    assert pipeline.source_columns == {}
    assert pipeline.column_renames == {}
    doc = yaml.safe_load(
        pipeline.emit_all_yaml(catalog="tgt", schema="ucmv")["Fact_Sales"]
    )
    exprs = {m["name"]: m["expr"] for m in doc["measures"]}
    assert exprs["source_rows"] == "COUNT(source.source)"


def test_on_the_same_inputs_are_used():
    pipeline = _pipeline({})
    assert pipeline.source_columns == COLUMNS
    assert pipeline.column_renames == {"Fact_Sales": {"source": "source_"}}


def test_the_generator_given_columns_with_the_switch_off_matches_a_run_without_them():
    def run(extra: dict) -> dict:
        tool = UCMetricViewGeneratorTool(
            measures_json=json.dumps(MEASURES),
            mquery_json=json.dumps(MQUERY),
            config_json=json.dumps(
                {"read_from_source_layer": True, "use_source_columns": False}
            ),
            catalog="tgt",
            schema_name="ucmv",
            use_llm_fallback=False,
            **extra,
        )
        return json.loads(tool._run())

    given = run(
        {
            "source_columns_json": json.dumps(
                {"Fact_Sales": [{"name": "source", "type": "STRING"}]}
            )
        }
    )
    plain = run({})
    assert given["yaml"] == plain["yaml"]
    assert given["source_layer_ddl"] == plain["source_layer_ddl"]
    assert "EXCEPT" not in json.dumps(given["source_layer_ddl"])
