"""Tests for YAML emitter (matches original monolith's emit_yaml signature)."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.data_classes import MetricViewSpec, TranslationResult
from src.engines.crewai.tools.custom.metric_view_utils.yaml_emitter import emit_yaml


@pytest.fixture
def basic_spec():
    return MetricViewSpec(
        fact_table_key='fact_test',
        source_table='catalog.schema.test_table',
        view_name='fact_test_uc_metric_view',
        comment='Test metric view',
        joins=[],
        dimensions=[{'name': 'region', 'expr': 'source.region', 'comment': 'Region'}],
        measures=[
            TranslationResult(
                measure_name='total_sales',
                original_name='Total Sales',
                sql_expr='SUM(source.sales)',
                is_translatable=True,
                skip_reason='Total sales',
                dax_expression='SUM(Sales[Amount])',
                confidence='high',
                category='base',
            )
        ],
        untranslatable=[],
    )


class TestEmitYaml:
    def test_basic_output(self, basic_spec):
        yaml = emit_yaml(basic_spec)
        assert "version: '1.1'" in yaml
        assert 'source: catalog.schema.test_table' in yaml
        assert 'measures:' in yaml
        assert 'total_sales' in yaml
        assert 'SUM(source.sales)' in yaml
        # Note: 'region' dimension gets dropped by phantom detection
        # (it doesn't appear in any measure expression) — correct behavior

    def test_with_joins(self):
        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.tbl',
            view_name='test_view',
            comment='Test',
            joins=[{'name': 'dim_geo', 'source': 'cat.sch.dim', 'join_on': 'source.key = dim_geo.key'}],
            dimensions=[],
            measures=[
                TranslationResult(
                    measure_name='val', original_name='val',
                    sql_expr='SUM(source.val)', is_translatable=True,
                    skip_reason='Val', dax_expression='', confidence='high', category='base',
                )
            ],
            untranslatable=[],
        )
        yaml = emit_yaml(spec)
        assert 'joins:' in yaml
        assert 'dim_geo' in yaml

    def test_with_source_sql(self):
        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.tbl',
            view_name='test_view',
            comment='Test',
            joins=[],
            dimensions=[],
            measures=[
                TranslationResult(
                    measure_name='val', original_name='val',
                    sql_expr='SUM(source.val)', is_translatable=True,
                    skip_reason='Val', dax_expression='', confidence='high', category='base',
                )
            ],
            untranslatable=[],
            source_sql='SELECT * FROM cat.sch.tbl WHERE x = 1',
        )
        yaml = emit_yaml(spec)
        assert 'source: |-' in yaml
        assert 'SELECT * FROM' in yaml

    def test_empty_measures_returns_empty(self):
        spec = MetricViewSpec(
            fact_table_key='fact',
            source_table='cat.sch.tbl',
            view_name='test_view',
            comment='Test',
            joins=[],
            dimensions=[],
            measures=[],
            untranslatable=[],
        )
        yaml = emit_yaml(spec)
        assert yaml == ''
