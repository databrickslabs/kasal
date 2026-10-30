"""Value-changing type conversions are reported, renames are simultaneous,
and a dimension is only joined on its raw table when its M keeps every row."""

import pytest

from src.services.tools.metric_view_utils.join_detector import JoinDetector
from src.services.tools.metric_view_utils.mquery_handoff import (
    build_ucmv_mquery,
    enrich_join_source_tables,
    plain_join_source,
    resolve_mquery_with_notes,
)
from src.services.tools.metric_view_utils.mquery_parser import (
    MQueryParser,
    resolve_mquery_to_sql,
)
from src.services.tools.metric_view_utils.mquery_steps import type_change_sql

_NAV = (
    'Source = Databricks.Catalogs("host.example.com", "/sql/1.0/warehouses/x", null),\n'
    '    c = Source{[Name="main",Kind="Database"]}[Data],\n'
    '    s = c{[Name="sales",Kind="Schema"]}[Data],\n'
    '    t = s{[Name="{table}",Kind="Table"]}[Data]'
)


def _m(*steps: str, table: str = "orders", result: str = "t") -> str:
    body = ",\n    ".join([_NAV.replace("{table}", table), *steps])
    return f"let\n    {body}\nin\n    {result}"


TYPED = _m(
    'Typed = Table.TransformColumnTypes(t, {{"amount", Int64.Type}, '
    '{"order_date", type date}, {"price", type number}, {"fee", Currency.Type}})',
    result="Typed",
)


class TestTypeConversionsAreReported:
    def test_value_changing_types_are_named(self):
        sql, notes = resolve_mquery_with_notes(TYPED)
        assert sql == "SELECT * FROM main.sales.orders"
        assert notes == [
            "amount -> Int64.Type: apply CAST(BROUND(amount) AS BIGINT)",
            "order_date -> type date: apply CAST(order_date AS DATE)",
            "fee -> Currency.Type: apply BROUND(fee, 4)",
        ]

    @pytest.mark.parametrize(
        "type_text,sql",
        [
            ("Int64.Type", "CAST(BROUND(c) AS BIGINT)"),
            ("Int32.Type", "CAST(BROUND(c) AS INT)"),
            ("Int16.Type", "CAST(BROUND(c) AS SMALLINT)"),
            ("Int8.Type", "CAST(BROUND(c) AS TINYINT)"),
            ("Currency.Type", "BROUND(c, 4)"),
            ("type date", "CAST(c AS DATE)"),
            ("Date.Type", "CAST(c AS DATE)"),
            ("type logical", "CAST(c AS BOOLEAN)"),
            ("Logical.Type", "CAST(c AS BOOLEAN)"),
            # Text.From formats by culture and Spark has no TIME type
            ("type text", None),
            ("type time", None),
        ],
    )
    def test_each_conversion_has_its_sql(self, type_text, sql):
        assert type_change_sql("c", type_text) == sql
        m = _m(
            f'T2 = Table.TransformColumnTypes(t, {{{{"c", {type_text}}}}})',
            result="T2",
        )
        _, notes = resolve_mquery_with_notes(m)
        expected = f"apply {sql}" if sql else "no exact SQL equivalent"
        assert notes == [f"c -> {type_text}: {expected}"]

    def test_metric_view_report_lists_them(self):
        from src.services.tools.metric_view_utils.report_emitter import (
            emit_migration_report,
        )

        note = "amount -> Int64.Type: apply CAST(BROUND(amount) AS BIGINT)"
        report = emit_migration_report(
            {}, {}, limitations={"type_conversions_not_applied": {"Orders": [note]}}
        )
        assert "## Power Query Type Conversions Not Applied" in report
        assert "CAST(BROUND(amount) AS BIGINT)" in report

    def test_handoff_entry_and_run_warning(self):
        from src.services.tools.pipeline_config_generator_tool import (
            PipelineConfigGeneratorTool,
        )

        warnings: list[str] = []
        out = PipelineConfigGeneratorTool._build_ucmv_mquery(
            {"Orders": {"mquery_expression": TYPED}}, {}, warnings
        )
        assert out[0]["type_changes"][0].startswith("amount -> Int64.Type: apply")
        assert len(warnings) == 1
        assert "Orders: amount -> Int64.Type" in warnings[0]
        assert "not applied" in warnings[0]

    def test_label_only_types_are_not_reported(self):
        m = _m(
            'Typed = Table.TransformColumnTypes(t, {{"price", type number}, '
            '{"ts", type datetime}})',
            result="Typed",
        )
        warnings: list[str] = []
        out = build_ucmv_mquery({"Orders": {"mquery_expression": m}}, {}, warnings)
        assert "type_changes" not in out[0] and warnings == []


class TestRenamesAreSimultaneous:
    def test_swap_without_projection(self):
        m = _m('R = Table.RenameColumns(t, {{"a", "b"}, {"b", "a"}})', result="R")
        assert resolve_mquery_to_sql(m) == (
            "SELECT * EXCEPT (a, b), a AS b, b AS a FROM main.sales.orders"
        )

    def test_swap_after_projection(self):
        m = _m(
            'P = Table.SelectColumns(t, {"a", "b"})',
            'R = Table.RenameColumns(P, {{"a", "b"}, {"b", "a"}})',
            result="R",
        )
        assert (
            resolve_mquery_to_sql(m) == "SELECT a AS b, b AS a FROM main.sales.orders"
        )

    def test_two_columns_renamed_to_one_name_is_refused(self):
        m = _m('R = Table.RenameColumns(t, {{"a", "x"}, {"b", "x"}})', result="R")
        assert resolve_mquery_to_sql(m) is None


DEDUPED_DIM = _m(
    'D = Table.Distinct(t, {"customer_id"})', table="dim_customer", result="D"
)


class TestJoinSourcesKeepEveryRow:
    def _config(self) -> dict:
        return {"join_key_map": {"Dim_Customer": {"alias": "c", "join_key": "k"}}}

    def test_deduplicated_dimension_is_not_joined_raw(self):
        from src.services.tools.pipeline_config_generator_tool import (
            PipelineConfigGeneratorTool,
        )

        config = self._config()
        log = PipelineConfigGeneratorTool._enrich_source_tables_from_mquery(
            config, {"Dim_Customer": {"mquery_expression": DEDUPED_DIM}}
        )
        assert "source_table" not in config["join_key_map"]["Dim_Customer"]
        assert log[0]["status"] == "skipped"
        assert "not joined on the raw table" in log[0]["detail"]
        assert "Table.Distinct" in log[0]["detail"]

    def test_column_only_dimension_is_joined(self):
        config = self._config()
        m = _m(
            'P = Table.SelectColumns(t, {"k", "name"})',
            table="dim_customer",
            result="P",
        )
        enrich_join_source_tables(config, {"Dim_Customer": {"mquery_expression": m}})
        assert config["join_key_map"]["Dim_Customer"]["source_table"] == (
            "main.sales.dim_customer"
        )

    def test_native_query_with_where_is_not_joined_raw(self):
        m = (
            'let Source = Value.NativeQuery(Databricks.Query("h","p"), '
            '"SELECT * FROM main.sales.dim_customer WHERE active = 1") in Source'
        )
        fqn, reason = plain_join_source(m)
        assert fqn is None and "not joined on the raw table" in reason

    def test_plain_native_query_is_joined(self):
        m = (
            'let Source = Value.NativeQuery(Databricks.Query("h","p"), '
            '"SELECT * FROM main.sales.dim_customer") in Source'
        )
        assert plain_join_source(m) == ("main.sales.dim_customer", "")


class TestDroppedDimensionIsReported:
    def test_join_without_a_safe_source_is_listed_in_the_report(self):
        from src.services.tools.metric_view_utils.report_emitter import (
            emit_migration_report,
        )

        fact_sql = (
            "SELECT customer_id, SUM(amount) AS amount FROM main.sales.orders "
            "GROUP BY customer_id"
        )
        entries = build_ucmv_mquery(
            {
                "Orders": {"mquery_expression": fact_sql},
                "Dim_Customer": {"mquery_expression": DEDUPED_DIM},
            }
        )
        tables = MQueryParser().parse_json(entries)
        config = {
            "join_key_map": {"Dim_Customer": {"alias": "c", "join_key": "customer_id"}}
        }
        enrich_join_source_tables(
            config, {"Dim_Customer": {"mquery_expression": DEDUPED_DIM}}
        )
        detector = JoinDetector(tables, config)
        joins = detector.detect(
            "Orders",
            [{"dax_expression": "SUM(Dim_Customer[segment])"}],
            tables["Orders"],
        )
        assert joins == []  # no safe source: the join is not built ...
        assert (
            detector.dropped_dims[0]["dimension"] == "Dim_Customer"
        )  # ... but reported
        report = emit_migration_report(
            {}, {}, limitations={"dropped_dimension_joins": detector.dropped_dims}
        )
        assert "## Dimension Joins Not Emitted" in report
        assert "| Orders | Dim_Customer |" in report

    def test_pipeline_output_reports_dropped_join_and_type_changes(self):
        from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline

        fact = _m(
            'Typed = Table.TransformColumnTypes(t, {{"amount", Int64.Type}})',
            result="Typed",
        )
        entries = build_ucmv_mquery(
            {
                "Orders": {"mquery_expression": fact},
                "Dim_Customer": {"mquery_expression": DEDUPED_DIM},
            }
        )
        config = {
            "join_key_map": {"Dim_Customer": {"alias": "c", "join_key": "customer_id"}}
        }
        measures = [
            {
                "measure_name": "Revenue",
                "dax_expression": "SUM(Orders[amount])",
                "proposed_allocation": "Orders",
            },
            {
                "measure_name": "Segments",
                "dax_expression": "DISTINCTCOUNT(Dim_Customer[segment])",
                "proposed_allocation": "Orders",
            },
        ]
        pipeline = MetricViewPipeline(
            mapping=measures,
            mquery_tables=MQueryParser().parse_json(entries),
            config=config,
        )
        pipeline.run()
        results = pipeline.get_results()
        assert results["limitations"]["type_conversions_not_applied"]["Orders"] == [
            "amount -> Int64.Type: apply CAST(BROUND(amount) AS BIGINT)"
        ]
        assert results["limitations"]["dropped_dimension_joins"][0]["dimension"] == (
            "Dim_Customer"
        )
        assert "## Dimension Joins Not Emitted" in results["migration_report"]
        assert (
            "## Power Query Type Conversions Not Applied" in results["migration_report"]
        )
