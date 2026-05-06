"""Migration Report Emitter — generates markdown report from pipeline results."""
from __future__ import annotations

import re
from datetime import datetime

from .data_classes import MetricViewSpec


def emit_migration_report(
    all_specs: dict[str, MetricViewSpec],
    stats: dict[str, dict],
    config: dict | None = None,
) -> str:
    """Generate a markdown migration report.

    Includes:
    - Executive summary (total measures, translation rate, table count)
    - Per-table breakdown (translated, untranslatable, artifacts, confidence)
    - Join map (which dimensions are joined to which facts)
    - Untranslatable DAX measures with explanations
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
