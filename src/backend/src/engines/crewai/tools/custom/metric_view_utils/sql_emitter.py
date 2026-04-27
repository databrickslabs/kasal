"""SQL Emitter — generate deploy SQL from MetricViewSpec."""
from __future__ import annotations

from .data_classes import MetricViewSpec


def emit_deploy_sql(spec: MetricViewSpec,
                    catalog: str = 'main',
                    schema: str = 'default') -> str:
    """Emit CREATE OR ALTER METRIC VIEW SQL for deployment.

    Returns the SQL string.
    """
    view_name = f'{catalog}.{schema}.{spec.view_name}'

    lines = [
        f'-- Deploy UC Metric View: {spec.view_name}',
        f'-- Fact table: {spec.fact_table_key}',
        f'-- Measures: {len(spec.measures)} translated, {len(spec.untranslatable)} untranslatable',
        f'-- Base: {spec.base_measure_count}, DAX: {spec.dax_measure_count}, SWITCH: {spec.switch_measure_count}',
        '',
        f'CREATE OR REPLACE METRIC VIEW {view_name}',
        f"  COMMENT '{_sql_escape(spec.comment[:200] if spec.comment else spec.view_name)}'",
        f'AS',
        f"  SELECT * FROM {spec.source_table}",
        ';',
        '',
    ]

    return '\n'.join(lines)


def _sql_escape(s: str) -> str:
    """Escape a string for SQL single-quoted literal."""
    return s.replace("'", "''").replace('\n', ' ')
