"""Tests for metric_view_utils data classes."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.data_classes import (
    TranslationResult, TableInfo, MetricViewSpec, MStep, ScanTableInfo
)


class TestTranslationResult:
    def test_basic_creation(self):
        r = TranslationResult(
            measure_name='total_sales',
            original_name='Total Sales',
            sql_expr='SUM(source.sales)',
            is_translatable=True,
            skip_reason='',
            dax_expression='SUM(Sales[Amount])',
            confidence='high',
            category='single_table',
        )
        assert r.measure_name == 'total_sales'
        assert r.is_translatable is True
        assert r.window_spec is None

    def test_with_window_spec(self):
        r = TranslationResult(
            measure_name='py_sales',
            original_name='PY Sales',
            sql_expr='SUM(source.sales)',
            is_translatable=True,
            skip_reason='',
            dax_expression='CALCULATE(SUM(...), SAMEPERIODLASTYEAR(...))',
            confidence='high',
            category='single_table',
            window_spec={'order': 'fiscper', 'range': 'trailing 12 month'},
        )
        assert r.window_spec['order'] == 'fiscper'

    def test_untranslatable(self):
        r = TranslationResult(
            measure_name='color_measure',
            original_name='Color Measure',
            sql_expr=None,
            is_translatable=False,
            skip_reason='Color/conditional formatting',
            dax_expression='IF(x, "red", "green")',
            confidence='none',
            category='unassigned',
        )
        assert r.sql_expr is None
        assert r.is_translatable is False


class TestTableInfo:
    def test_fact_table(self):
        t = TableInfo(
            table_name='fact_pe002',
            source_table='catalog.schema.pe002_table',
            aggregate_columns=[{'name': 'paid_hours', 'source_col': 'paid_hours'}],
            group_by_columns=['comp_code', 'fiscper'],
            calculated_columns=[],
            is_fact=True,
            full_sql='SELECT ...',
        )
        assert t.is_fact is True
        assert len(t.aggregate_columns) == 1

    def test_dim_table(self):
        t = TableInfo(
            table_name='dim_company',
            source_table='catalog.schema.dim_company',
            aggregate_columns=[],
            group_by_columns=['comp_code', 'country'],
            calculated_columns=[],
            is_fact=False,
            full_sql='SELECT ...',
        )
        assert t.is_fact is False
        assert t.static_filters == []
        assert t.dim_source_tables == {}


class TestMetricViewSpec:
    def test_creation(self):
        spec = MetricViewSpec(
            fact_table_key='fact_pe002',
            source_table='catalog.schema.pe002',
            view_name='fact_pe002_uc_metric_view',
            comment='Test view',
            joins=[],
            dimensions=[],
            measures=[],
            untranslatable=[],
        )
        assert spec.base_measure_count == 0
        assert spec.source_filter == ''
        assert spec.source_sql == ''


class TestMStep:
    def test_creation(self):
        step = MStep(step_type='SelectRows', raw_expression='each [col] <> "val"')
        assert step.step_type == 'SelectRows'


class TestScanTableInfo:
    def test_creation(self):
        info = ScanTableInfo(
            pbi_table_name='fact_pe002',
            raw_m_expression='let ...',
            native_sql='SELECT * FROM table',
            m_steps=[],
            has_union=False,
            pbi_columns=[],
        )
        assert info.has_union is False
