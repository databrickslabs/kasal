"""Migration Report Emitter — generates markdown report from pipeline results."""
from __future__ import annotations

import re
from datetime import datetime

from .data_classes import MetricViewSpec


def emit_migration_report(
    all_specs: dict[str, MetricViewSpec],
    stats: dict[str, dict],
    config: dict | None = None,
    limitations: dict | None = None,
) -> str:
    """Generate a markdown migration report.

    Includes:
    - Executive summary (total measures, translation rate, table count)
    - Per-table breakdown (translated, untranslatable, artifacts, confidence)
    - Join map (which dimensions are joined to which facts)
    - Untranslatable DAX measures with explanations
    - M:N relationships (if any were skipped)
    - Recommendations
    """
    lines: list[str] = []
    cfg = config or {}

    # ── Executive Summary ──────────────────────────────────────────────────

    lines.append('# UC Metric View Migration Report')
    lines.append('')
    lines.append(f'Generated: {datetime.now():%Y-%m-%d %H:%M}')
    lines.append('')

    total_measures = sum(
        s.get('total', 0) for k, s in stats.items() if k != '__unassigned__')
    total_translated = sum(
        s.get('translated', 0) for k, s in stats.items() if k != '__unassigned__')
    total_artifacts = sum(
        s.get('artifacts', 0) for k, s in stats.items() if k != '__unassigned__')
    total_views = len(all_specs)
    real_scope = total_measures - total_artifacts
    real_pct = total_translated * 100 // real_scope if real_scope > 0 else 0

    lines.append('## Executive Summary')
    lines.append('')
    lines.append('| Metric | Value |')
    lines.append('|--------|-------|')
    lines.append(f'| Metric views generated | {total_views} |')
    lines.append(f'| Total measures in scope | {total_measures} |')
    lines.append(f'| PBI UI artifacts (excluded) | {total_artifacts} |')
    lines.append(f'| Real business measures | {real_scope} |')
    lines.append(f'| **Successfully translated** | **{total_translated}** |')
    lines.append(f'| **Business coverage** | **{real_pct}%** |')
    lines.append(f'| Gaps remaining | {real_scope - total_translated} |')
    lines.append('')

    # ── Per-Table Breakdown ────────────────────────────────────────────────

    lines.append('## Per-Table Results')
    lines.append('')
    lines.append(
        '| Table | Translated | Artifacts | Gaps | Scope | Coverage | Source |')
    lines.append(
        '|-------|-----------|-----------|------|-------|----------|--------|')

    for table_key in sorted(stats.keys()):
        s = stats[table_key]
        if table_key == '__unassigned__':
            lines.append(
                f'| {table_key} | \u2014 | \u2014 | {s["total"]} '
                f'| {s["total"]} | \u2014 | Unassigned |')
            continue
        if s.get('skipped'):
            lines.append(
                f'| {table_key} | \u2014 | \u2014 | {s["total"]} '
                f'| \u2014 | \u2014 | {s.get("skip_reason", "Skipped")} |')
            continue
        art = s.get('artifacts', 0)
        gaps = s.get('untranslatable', 0) - art
        scope = s['total'] - art
        pct = s['translated'] * 100 // scope if scope > 0 else 0
        base = s.get('base', 0)
        dax = s.get('dax', 0)
        sw = s.get('switch', 0)
        lines.append(
            f'| {table_key} | {s["translated"]} ({base}b+{dax}d+{sw}s) '
            f'| {art} | {gaps} | {scope} | {pct}% | MQuery |')
    lines.append('')

    # ── Join Map ───────────────────────────────────────────────────────────

    lines.append('## Join Map')
    lines.append('')
    lines.append('| Fact Table | Join | Source | ON |')
    lines.append('|-----------|------|--------|-----|')
    for table_key, spec in sorted(all_specs.items()):
        for j in spec.joins:
            source = j.get('source', '')
            if len(source) > 60:
                source = source[:57] + '...'
            on_clause = j.get('join_on', j.get('on', ''))
            if len(on_clause) > 60:
                on_clause = on_clause[:57] + '...'
            lines.append(
                f'| {table_key} | {j["name"]} | `{source}` | `{on_clause}` |')
    lines.append('')

    # ── Inactive Relationships (USERELATIONSHIP) ────────────────────────
    _lim = limitations or {}
    if _lim.get('inactive_relationships'):
        lines.append('## Inactive Relationships (USERELATIONSHIP)')
        lines.append('')
        lines.append('These PBI inactive relationships were detected. Measures using USERELATIONSHIP')
        lines.append('get alternate join aliases in the metric view.')
        lines.append('')
        lines.append('| From Table | From Column | To Table | To Column | Alias |')
        lines.append('|-----------|-------------|----------|----------|-------|')
        for irel in _lim['inactive_relationships']:
            alias = f"{irel['to_table'].lower()}_{irel['from_column'].lower()}"
            lines.append(
                f"| {irel['from_table']} | {irel['from_column']} "
                f"| {irel['to_table']} | {irel['to_column']} | {alias} |")
        lines.append('')

    # ── Untranslatable Measures ────────────────────────────────────────────

    lines.append('## Untranslatable Measures')
    lines.append('')
    lines.append('| Table | Measure | Reason |')
    lines.append('|-------|---------|--------|')
    for table_key, spec in sorted(all_specs.items()):
        for m in spec.untranslatable:
            reason = m.skip_reason[:80] if m.skip_reason else 'Unknown'
            lines.append(
                f'| {table_key} | {m.original_name} | {reason} |')
    lines.append('')

    # ── M:N Relationships ─────────────────────────────────────────────────

    if _lim.get('m2n_relationships'):
        lines.append('## M:N Relationships (Not Migrated)')
        lines.append('')
        lines.append(
            'These many-to-many relationships cannot be directly represented '
            'as UC Metric View joins.')
        lines.append(
            'Workarounds: (1) Create a bridge table view in Databricks, '
            '(2) Use pre-joined inline SQL, (3) Skip and handle manually.')
        lines.append('')
        lines.append('| From Table | From Column | To Table | To Column |')
        lines.append('|-----------|-------------|----------|----------|')
        for rel in _lim['m2n_relationships']:
            lines.append(
                f"| {rel['from_table']} | {rel['from_column']} "
                f"| {rel['to_table']} | {rel['to_column']} |")
        lines.append('')

    # ── Incremental Refresh Policies ─────────────────────────────────────

    if _lim.get('refresh_policies'):
        lines.append('## Incremental Refresh Policies')
        lines.append('')
        lines.append('These tables have incremental refresh policies in PBI.')
        lines.append('Consider adding a date filter to the UC Metric View for performance.')
        lines.append('')
        for rp in _lim['refresh_policies']:
            lines.append(f"- **{rp['table_name']}**")
        lines.append('')

    # ── Summarization Override Warnings ───────────────────────────────────

    if _lim.get('summarization_warnings'):
        lines.append('## Summarization Override Warnings')
        lines.append('')
        lines.append('These columns have SummarizeBy=None in PBI (should not be aggregated).')
        lines.append('Verify they are not included as SUM measures in the metric view.')
        lines.append('')
        lines.append('| Table | Column |')
        lines.append('|-------|--------|')
        for sw in _lim['summarization_warnings']:
            lines.append(f"| {sw['table_name']} | {sw['column_name']} |")
        lines.append('')

    # ── Row-Level Security Warning ────────────────────────────────────────

    if _lim.get('rls_tables'):
        lines.append('## Row-Level Security Warning')
        lines.append('')
        lines.append('These tables have RLS in PBI. UC Metric Views do not enforce RLS.')
        lines.append('Configure Databricks row filters separately.')
        lines.append('')
        for t in sorted(_lim['rls_tables']):
            lines.append(f'- {t}')
        lines.append('')

    # ── Aggregation Table Warnings ─────────────────────────────────────────

    if _lim.get('aggregation_warnings'):
        lines.append('## Aggregation Table Warnings')
        lines.append('')
        lines.append('These tables use Import storage mode and may be aggregation tables.')
        lines.append('Verify the source grain matches your metric view expectations.')
        lines.append('')
        for w in _lim['aggregation_warnings']:
            lines.append(f"- **{w['table']}**: {w['warning']}")
        lines.append('')

    # ── Perspectives (Agent 9) ────────────────────────────────────────────

    if _lim.get('perspectives'):
        lines.append('## Perspectives (Not Migrated)')
        lines.append('')
        lines.append('PBI perspectives control table/measure visibility per user group.')
        lines.append('UC Metric Views do not support perspectives. Consider separate UCMV sets or Databricks permissions.')
        lines.append('')
        for p in _lim['perspectives']:
            name = p.get('name', p) if isinstance(p, dict) else str(p)
            lines.append(f'- {name}')
        lines.append('')

    # ── Field Parameters (Agent 9) ────────────────────────────────────────

    if _lim.get('field_parameters'):
        lines.append('## Field Parameters (Not Migrated)')
        lines.append('')
        lines.append('PBI field parameters let users dynamically switch displayed measures.')
        lines.append('No UC equivalent. Consider separate metric views or Genie space configuration.')
        lines.append('')
        for fp in _lim['field_parameters']:
            name = fp.get('name', fp) if isinstance(fp, dict) else str(fp)
            lines.append(f'- {name}')
        lines.append('')

    # ── PBI Native Features — Migration Status (Agent 10) ────────────────
    lines.append('## PBI Native Features — Migration Status')
    lines.append('')
    lines.append('| Feature | Status | Count | Details |')
    lines.append('|---------|--------|-------|---------|')

    # Inactive relationships
    inactive = _lim.get('inactive_relationships', [])
    lines.append(f"| USERELATIONSHIP | {'Migrated' if inactive else 'N/A'} | {len(inactive)} | Alternate join aliases generated |")

    # M:N
    m2n = _lim.get('m2n_relationships', [])
    lines.append(f"| M:N Relationships | {'Flagged' if m2n else 'N/A'} | {len(m2n)} | Requires bridge table |")

    # RLS
    rls = _lim.get('rls_tables', set())
    lines.append(f"| Row-Level Security | {'Flagged' if rls else 'N/A'} | {len(rls)} | Configure Databricks row filters |")

    # Aggregation
    agg = _lim.get('aggregation_warnings', [])
    lines.append(f"| Aggregation Tables | {'Flagged' if agg else 'N/A'} | {len(agg)} | Verify source grain |")

    # Refresh policies
    rp = _lim.get('refresh_policies', [])
    lines.append(f"| Incremental Refresh | {'Flagged' if rp else 'N/A'} | {len(rp)} | Add date filter for performance |")

    # Summarization
    sw = _lim.get('summarization_warnings', [])
    lines.append(f"| Default Summarization | {'Flagged' if sw else 'N/A'} | {len(sw)} | SummarizeBy=None columns |")

    # Calc groups
    cg = _lim.get('calculation_groups_expanded', [])
    cg_count = sum(c.get('expanded_count', 0) for c in cg)
    lines.append(f"| Calculation Groups | {'Expanded' if cg else 'N/A'} | {cg_count} | Measures auto-generated |")

    # Perspectives
    persp = _lim.get('perspectives', [])
    lines.append(f"| Perspectives | {'Flagged' if persp else 'N/A'} | {len(persp)} | Not migrated |")

    # Field parameters
    fp_list = _lim.get('field_parameters', [])
    lines.append(f"| Field Parameters | {'Flagged' if fp_list else 'N/A'} | {len(fp_list)} | Not migrated |")

    # Conditional formatting
    lines.append(f"| Conditional Formatting | Improved | — | Business logic now detected |")

    lines.append('')

    # ── Recommendations ────────────────────────────────────────────────────

    lines.append('## Recommendations')
    lines.append('')
    lines.append(
        '1. **Unassigned measures**: Review table allocation manually')
    lines.append(
        '2. **Cross-table measures**: Create pre-joined SQL views '
        'combining required fact tables')
    lines.append(
        '3. **SELECTEDVALUE+SWITCH**: Already decomposed into individual '
        'measures via pipeline config')
    lines.append(
        '4. **FORMAT/Color/ISBLANK**: PBI display artifacts \u2014 '
        'no SQL equivalent needed')
    lines.append('')

    return '\n'.join(lines)
