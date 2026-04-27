"""YAML Emitter — generate UC Metric View YAML from MetricViewSpec."""
from __future__ import annotations

import re
from typing import Any

from .data_classes import MetricViewSpec, TranslationResult
from .metadata_generator import MetadataGenerator


def emit_yaml(spec: MetricViewSpec,
              catalog: str = 'main',
              schema: str = 'default',
              metadata_gen: MetadataGenerator | None = None,
              enrichment_joins: list[dict] | None = None,
              dimension_exclusions: set[str] | None = None,
              dimension_metadata: dict | None = None,
              measure_metadata: dict | None = None,
              comment_override: str | None = None,
              dimension_order: list[str] | None = None) -> str:
    """Emit UC Metric View YAML for a single fact table spec.

    Returns the YAML string (not written to file).
    """
    meta = metadata_gen or MetadataGenerator()
    table_key = spec.fact_table_key
    dim_excl = dimension_exclusions or set()
    dim_meta = dimension_metadata or {}
    meas_meta = measure_metadata or {}
    dim_order = dimension_order or []

    lines: list[str] = []

    # Header
    lines.append(f'name: {spec.view_name}')
    lines.append(f'catalog: {catalog}')
    lines.append(f'schema: {schema}')
    lines.append(f'version: "1.1"')
    lines.append('')

    # Comment
    comment = comment_override or spec.comment
    if comment:
        if '\n' in comment:
            lines.append('comment: |-')
            for cl in comment.split('\n'):
                lines.append(f'  {cl}')
        else:
            lines.append(f'comment: "{_yaml_escape(comment)}"')
    lines.append('')

    # Source
    if spec.source_sql:
        lines.append('source: |-')
        for sl in spec.source_sql.split('\n'):
            lines.append(f'  {sl}')
    else:
        lines.append(f'source: {spec.source_table}')

    # Source filter
    if spec.source_filter:
        lines.append(f'filter: "{_yaml_escape(spec.source_filter)}"')
    lines.append('')

    # Joins
    all_joins = list(spec.joins)
    if enrichment_joins:
        existing_aliases = {j['name'] for j in all_joins}
        for ej in enrichment_joins:
            if ej['name'] not in existing_aliases:
                all_joins.append(ej)

    if all_joins:
        lines.append('joins:')
        for j in all_joins:
            if j.get('_union_mode') or j.get('_source_embed'):
                continue
            lines.append(f'  - name: {j["name"]}')
            source_val = j.get('source', '')
            if '\n' in source_val:
                lines.append('    source: |-')
                for sl in source_val.split('\n'):
                    lines.append(f'      {sl}')
            else:
                lines.append(f'    source: {source_val}')
            lines.append(f'    on: "{_yaml_escape(j["join_on"])}"')
            if j.get('join_type'):
                lines.append(f'    type: {j["join_type"]}')
        lines.append('')

    # Dimensions
    dims = spec.dimensions
    # Exclude internal-only dimensions
    if dim_excl:
        dims = [d for d in dims if d['name'] not in dim_excl]

    # Apply ordering
    if dim_order:
        ordered = []
        remaining = list(dims)
        for name in dim_order:
            for d in remaining:
                if d['name'] == name:
                    ordered.append(d)
                    remaining.remove(d)
                    break
        ordered.extend(remaining)
        dims = ordered

    if dims:
        lines.append('dimensions:')
        for d in dims:
            lines.append(f'  - name: {d["name"]}')
            lines.append(f'    expr: "{_yaml_escape(d["expr"])}"')
            # Metadata
            d_meta = dim_meta.get(d['name'], {})
            comment_text = d_meta.get('comment') or d.get('comment', '')
            if comment_text:
                lines.append(f'    comment: "{_yaml_escape(comment_text)}"')
            display = d_meta.get('display_name')
            if display:
                lines.append(f'    display_name: "{_yaml_escape(display)}"')
            synonyms = d_meta.get('synonyms')
            if synonyms:
                lines.append(f'    synonyms: [{", ".join(repr(s) for s in synonyms)}]')
        lines.append('')

    # Measures
    if spec.measures:
        lines.append('measures:')
        for m in spec.measures:
            lines.append(f'  - name: {m.measure_name}')
            expr = m.sql_expr or 'NULL'
            if len(expr) > 80 or '\n' in expr:
                lines.append(f'    expr: |-')
                for el in expr.split('\n'):
                    lines.append(f'      {el}')
            else:
                lines.append(f'    expr: "{_yaml_escape(expr)}"')

            # Metadata
            m_meta = meas_meta.get(m.measure_name, meta.get_measure_meta(m.measure_name, table_key))
            m_display = m_meta.get('display_name')
            if m_display:
                lines.append(f'    display_name: "{_yaml_escape(m_display)}"')
            m_comment = m_meta.get('comment', '')
            if m_comment:
                lines.append(f'    comment: "{_yaml_escape(m_comment)}"')
            m_synonyms = m_meta.get('synonyms')
            if m_synonyms:
                lines.append(f'    synonyms: [{", ".join(repr(s) for s in m_synonyms)}]')
            fmt = m_meta.get('format')
            if fmt:
                lines.append(f'    format:')
                lines.append(f'      type: {fmt["type"]}')
                if 'decimal_places' in fmt:
                    dp = fmt['decimal_places']
                    lines.append(f'      decimal_places:')
                    lines.append(f'        type: {dp["type"]}')
                    lines.append(f'        places: {dp["places"]}')

            # Window spec (semiadditive, trailing period)
            if m.window_spec:
                ws = m.window_spec
                lines.append(f'    window:')
                if ws.get('order'):
                    lines.append(f'      order: {ws["order"]}')
                if ws.get('range'):
                    lines.append(f'      range: {ws["range"]}')
                if ws.get('semiadditive'):
                    lines.append(f'      semiadditive: {ws["semiadditive"]}')
        lines.append('')

    return '\n'.join(lines)


def _yaml_escape(s: str) -> str:
    """Escape a string for YAML double-quoted scalar."""
    return s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
