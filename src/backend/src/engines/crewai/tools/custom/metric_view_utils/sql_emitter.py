"""SQL Emitter — generate deployment reference from MetricViewSpec."""
from __future__ import annotations

from .data_classes import MetricViewSpec


def emit_deploy_sql(spec: MetricViewSpec,
                    catalog: str = 'main',
                    schema: str = 'default') -> str:
    """Emit deployment instructions for a UC Metric View.

    UC Metric Views are created via YAML definitions, not SQL DDL.
    This generates a reference SQL comment block with deployment metadata.
    """
    view_name = f'{catalog}.{schema}.{spec.view_name}'

    lines = [
        f'-- UC Metric View: {spec.view_name}',
        f'-- Fact table: {spec.fact_table_key}',
        f'-- Source: {spec.source_table}',
        f'-- Measures: {len(spec.measures)} translated, {len(spec.untranslatable)} skipped',
        f'-- Base: {spec.base_measure_count}, DAX: {spec.dax_measure_count}, SWITCH: {spec.switch_measure_count}',
        f'--',
        f'-- Deploy via Databricks CLI:',
        f'--   databricks metric-views create {view_name} --yaml-file {spec.view_name}.yml',
        f'--',
        f'-- Or via REST API:',
        f'--   POST /api/2.0/unity-catalog/metric-views',
        f'--   Body: {{"name": "{view_name}", "yaml_body": "<contents of {spec.view_name}.yml>"}}',
        '',
    ]

    return '\n'.join(lines)
