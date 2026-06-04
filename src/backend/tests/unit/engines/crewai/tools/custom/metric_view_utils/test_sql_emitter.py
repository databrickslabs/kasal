"""Tests for sql_emitter.py — deployment reference SQL generation."""
from __future__ import annotations

import pytest

from src.engines.crewai.tools.custom.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.engines.crewai.tools.custom.metric_view_utils.sql_emitter import emit_deploy_sql


def _make_translation(name: str, translatable: bool = True) -> TranslationResult:
    return TranslationResult(
        measure_name=name,
        original_name=name,
        sql_expr="SUM(x)" if translatable else None,
        is_translatable=translatable,
        skip_reason="" if translatable else "complex DAX",
        dax_expression="SUM(Table[col])",
        confidence="high" if translatable else "none",
        category="single_table",
    )


def _make_spec(
    *,
    view_name: str = "test_view",
    fact_table_key: str = "FactSales",
    source_table: str = "catalog.schema.fact_sales",
    num_measures: int = 3,
    num_untranslatable: int = 1,
    base_count: int = 2,
    dax_count: int = 1,
    switch_count: int = 0,
) -> MetricViewSpec:
    measures = [_make_translation(f"m{i}") for i in range(num_measures)]
    untranslatable = [_make_translation(f"u{i}", translatable=False) for i in range(num_untranslatable)]
    return MetricViewSpec(
        fact_table_key=fact_table_key,
        source_table=source_table,
        view_name=view_name,
        comment="test",
        joins=[],
        dimensions=[],
        measures=measures,
        untranslatable=untranslatable,
        base_measure_count=base_count,
        dax_measure_count=dax_count,
        switch_measure_count=switch_count,
    )


class TestEmitDeploySql:
    def test_basic_output_contains_view_name(self):
        spec = _make_spec(view_name="my_metric_view")
        result = emit_deploy_sql(spec)
        assert "my_metric_view" in result

    def test_contains_fact_table_key(self):
        spec = _make_spec(fact_table_key="FactOrders")
        result = emit_deploy_sql(spec)
        assert "FactOrders" in result

    def test_contains_source_table(self):
        spec = _make_spec(source_table="cat.sch.tbl")
        result = emit_deploy_sql(spec)
        assert "cat.sch.tbl" in result

    def test_measure_counts(self):
        spec = _make_spec(num_measures=5, num_untranslatable=2)
        result = emit_deploy_sql(spec)
        assert "5 translated" in result
        assert "2 skipped" in result

    def test_base_dax_switch_counts(self):
        spec = _make_spec(base_count=10, dax_count=5, switch_count=3)
        result = emit_deploy_sql(spec)
        assert "Base: 10" in result
        assert "DAX: 5" in result
        assert "SWITCH: 3" in result

    def test_deployment_instructions(self):
        spec = _make_spec()
        result = emit_deploy_sql(spec)
        assert "databricks metric-views create" in result
        assert "REST API" in result
        assert "POST" in result

    def test_custom_catalog_schema(self):
        spec = _make_spec(view_name="mv")
        result = emit_deploy_sql(spec, catalog="prod", schema="analytics")
        assert "prod.analytics.mv" in result

    def test_default_catalog_schema(self):
        spec = _make_spec(view_name="mv")
        result = emit_deploy_sql(spec)
        assert "main.default.mv" in result

    def test_empty_measures(self):
        spec = _make_spec(num_measures=0, num_untranslatable=0)
        result = emit_deploy_sql(spec)
        assert "0 translated" in result
        assert "0 skipped" in result

    def test_three_level_source_table(self):
        spec = _make_spec(source_table="my_catalog.my_schema.my_table")
        result = emit_deploy_sql(spec)
        assert "my_catalog.my_schema.my_table" in result

    def test_yaml_file_reference(self):
        spec = _make_spec(view_name="sales_metrics")
        result = emit_deploy_sql(spec)
        assert "sales_metrics.yml" in result

    def test_result_is_string(self):
        spec = _make_spec()
        assert isinstance(emit_deploy_sql(spec), str)

    def test_lines_are_comments(self):
        spec = _make_spec()
        result = emit_deploy_sql(spec)
        for line in result.strip().splitlines():
            if line.strip():
                assert line.startswith("--")
