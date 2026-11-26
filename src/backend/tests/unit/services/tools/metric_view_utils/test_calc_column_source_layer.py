"""A Power BI calculated column becomes a real column of its source view, end to end.

Model (invented names): ``Fact_Sales`` carries a calculated ``date_key`` derived from its
``posting_date``; a relationship ``Fact_Sales[date_key] -> Dim_Cal[date_id]`` joins the calendar,
and the calendar's ``month_name`` is a dimension. The source views and the metric-view YAML are
built by ``MetricViewPipeline`` in read mode, the same path the generator runs.
"""

import copy

import yaml

from src.services.tools.metric_view_utils.mquery_parser import MQueryParser
from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline

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
        "transpiled_sql": "SELECT plant_code, posting_date, amount FROM cat.sch.sales",
        "validation_passed": "Yes",
    },
    {
        "table_name": "Dim_Cal",
        "transpiled_sql": "SELECT date_id, month_name FROM cat.sch.calendar",
        "validation_passed": "Yes",
    },
]
JOIN_KEY_MAP = {
    "Dim_Cal": {
        "alias": "dim_cal",
        "join_key": "date_key",
        "dim_key": "date_id",
        "source_table": "cat.sch.calendar",
        "dim_columns": [{"name": "month_name", "expr": "month_name"}],
    }
}
SOURCE_COLUMNS = {
    "Fact_Sales": ["plant_code", "posting_date", "amount"],
    "Dim_Cal": ["date_id", "month_name"],
}
KINDS = {"Fact_Sales": {"posting_date": "date", "amount": "number"}}
DATE_KEY = (
    "YEAR(Fact_Sales[posting_date]) * 10000 + MONTH(Fact_Sales[posting_date]) * 100"
)
TODO_TODAY = "DATEDIFF(Fact_Sales[posting_date], TODAY(), DAY)"
OLD_TODO = "DAX calculated column — verify/translate before use"


def _config(calc_expression: str | None = DATE_KEY, **extra) -> dict:
    cfg = {"read_from_source_layer": True, "join_key_map": copy.deepcopy(JOIN_KEY_MAP)}
    if calc_expression is not None:
        cfg["calculated_columns"] = {
            "Fact_Sales": [{"name": "date_key", "expression": calc_expression}]
        }
        cfg["calculated_column_kinds"] = copy.deepcopy(KINDS)
    cfg.update(extra)
    return cfg


def _run(config: dict, source_columns=SOURCE_COLUMNS) -> MetricViewPipeline:
    """Run and emit once: the views and the YAML are built together, as the generator does."""
    pipeline = MetricViewPipeline(
        mapping=copy.deepcopy(MEASURES),
        mquery_tables=MQueryParser().parse_json(copy.deepcopy(MQUERY)),
        config=config,
        source_columns=copy.deepcopy(source_columns),
    )
    pipeline.run()
    pipeline.emitted_yaml = pipeline.emit_all_yaml(catalog="tgt", schema="ucmv")
    return pipeline


def _yaml(pipeline: MetricViewPipeline) -> dict:
    return yaml.safe_load(pipeline.emitted_yaml["Fact_Sales"])


def _ddl(pipeline: MetricViewPipeline) -> str:
    return pipeline.source_layer_views["Fact_Sales"]["ddl"]


class TestTranslatedCalculatedColumn:
    def test_the_view_defines_the_column(self):
        ddl = _ddl(_run(_config()))
        assert "AS date_key" in ddl
        assert "-- TODO calc column" not in ddl
        assert "year(" in ddl and "`posting_date`" in ddl

    def test_calendar_join_survives_and_keys_on_the_column(self):
        doc = _yaml(_run(_config()))
        assert [(j["name"], j["source"]) for j in doc["joins"]] == [
            ("dim_cal", "tgt.ucmv.src_dim_cal")
        ]
        assert doc["joins"][0]["on"] == "source.date_key = dim_cal.date_id"

    def test_calendar_dimension_survives(self):
        doc = _yaml(_run(_config()))
        dims = {d["name"]: d["expr"] for d in doc["dimensions"]}
        assert dims["month_name"] == "dim_cal.month_name"

    def test_no_gap_or_note_is_recorded_for_it(self):
        pipeline = _run(_config())
        assert "calculated_column_joins" not in pipeline._limitations
        assert not pipeline._limitations.get("source_column_gaps")

    def test_joins_on_a_translated_column_are_not_dropped_by_the_yaml_check(self):
        # The join's key is not in the fact's GROUP BY; only the view defines it.
        doc = _yaml(_run(_config()))
        assert any(j["name"] == "dim_cal" for j in doc["joins"])
        assert "dropped" not in doc["comment"]


class TestUntranslatableCalculatedColumn:
    def test_column_stays_a_todo_with_its_reason(self):
        ddl = _ddl(_run(_config(TODO_TODAY)))
        assert (
            "-- TODO calc column 'date_key': DAX calculated column not translated:"
            in ddl
        )
        assert "TODAY()" in ddl
        assert "AS date_key" not in ddl

    def test_join_is_dropped_and_the_drop_carries_the_reason(self):
        pipeline = _run(_config(TODO_TODAY))
        doc = _yaml(pipeline)
        assert not [j for j in doc.get("joins") or [] if j["name"] == "dim_cal"]
        assert not [d for d in doc.get("dimensions") or [] if d["name"] == "month_name"]
        [note] = pipeline._limitations["calculated_column_joins"]
        assert note["join"] == "dim_cal"
        assert note["column"] == "date_key"
        assert "TODAY()" in note["reason"]

    def test_switch_off_keeps_the_old_todo_and_still_reports_the_drop(self):
        pipeline = _run(_config(DATE_KEY, calculated_columns_in_source_layer=False))
        ddl = _ddl(pipeline)
        assert f"-- TODO calc column 'date_key': {OLD_TODO}" in ddl
        assert not [
            j for j in _yaml(pipeline).get("joins") or [] if j["name"] == "dim_cal"
        ]

    def test_without_known_view_columns_the_column_is_not_translated(self):
        pipeline = _run(_config(), source_columns={})
        assert f"-- TODO calc column 'date_key': {OLD_TODO}" in _ddl(pipeline)


class TestFromAScannerPayload:
    """The calculated column comes from an Admin Scanner result, as the config generator reads it."""

    SCAN = {
        "workspaces": [
            {
                "datasets": [
                    {
                        "id": "ds",
                        "tables": [
                            {
                                "name": "Fact_Sales",
                                "columns": [
                                    {
                                        "name": "plant_code",
                                        "dataType": "String",
                                        "columnType": "Data",
                                    },
                                    {
                                        "name": "posting_date",
                                        "dataType": "DateTime",
                                        "columnType": "Data",
                                    },
                                    {
                                        "name": "amount",
                                        "dataType": "Double",
                                        "columnType": "Data",
                                    },
                                    {
                                        "name": "date_key",
                                        "dataType": "Int64",
                                        "columnType": "Calculated",
                                        "expression": DATE_KEY,
                                    },
                                ],
                            }
                        ],
                    }
                ]
            }
        ]
    }

    def test_scan_to_calendar_join_and_dimension(self):
        from src.services.powerbi.scan_calculated_columns import (
            scan_calculated_columns,
            scan_column_kinds,
        )

        calc = scan_calculated_columns(self.SCAN)
        config = _config(calc_expression=None)
        config["calculated_columns"] = calc
        config["calculated_column_kinds"] = scan_column_kinds(self.SCAN, calc.keys())
        pipeline = _run(config)
        assert "AS date_key" in _ddl(pipeline)
        doc = _yaml(pipeline)
        assert doc["joins"][0]["on"] == "source.date_key = dim_cal.date_id"
        assert "month_name" in {d["name"] for d in doc["dimensions"]}


class TestModelWithoutCalculatedColumns:
    def test_switch_changes_nothing_when_there_is_no_calculated_column(self):
        base = _run(_config(calc_expression=None))
        switched_on = _run(
            _config(calc_expression=None, calculated_columns_in_source_layer=True)
        )
        switched_off = _run(
            _config(calc_expression=None, calculated_columns_in_source_layer=False)
        )
        assert _ddl(base) == _ddl(switched_on) == _ddl(switched_off)
        assert _yaml(base) == _yaml(switched_on) == _yaml(switched_off)
        assert "calculated_column_joins" not in base._limitations
