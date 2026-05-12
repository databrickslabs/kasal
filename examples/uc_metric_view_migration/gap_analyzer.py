#!/usr/bin/env python3
"""Analyze pipeline gaps and recommend what to configure next.

Runs the full pipeline (same as run_locally.py) then analyzes untranslatable
measures to produce a prioritized "what to fix next" report.

Usage (from src/backend/):
    source .venv/bin/activate
    python ../../examples/uc_metric_view_migration/gap_analyzer.py
"""
from __future__ import annotations

import json
import os
import re
import sys
from collections import Counter, defaultdict

# ── sys.path setup (same as run_locally.py) ─────────────────────────
script_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(script_dir, '..', '..', 'src', 'backend')
sys.path.insert(0, backend_dir)
sys.path.insert(0, os.path.join(backend_dir, 'src'))

from src.engines.crewai.tools.custom.metric_view_utils.pipeline import MetricViewPipeline
from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
from src.engines.crewai.tools.custom.metric_view_utils.relationships_loader import RelationshipsLoader
from src.engines.crewai.tools.custom.metric_view_utils.scan_data_parser import ScanDataParser

# ── Categories that are genuinely untranslatable (PBI-specific, not gaps) ──
NOT_A_GAP = frozenset({
    'SELECTEDVALUE (slicer)',
    'ISBLANK+BLANK guard',
    'FORMAT function',
    'ISFILTERED',
    'PBI artifact cascade',
    'BLANK() placeholder',
    'DAX expression not available',
    'Covered by SWITCH',
    'Covered on primary table',
    'Color formatting',
})


def categorize_reason(skip_reason: str) -> str:
    """Categorize an untranslatable skip_reason into a bucket."""
    if not skip_reason:
        return 'Other'
    if 'SELECTEDVALUE+SWITCH' in skip_reason or ('SELECTEDVALUE' in skip_reason and 'SWITCH' in skip_reason):
        return 'SELECTEDVALUE+SWITCH'
    if 'SELECTEDVALUE' in skip_reason:
        return 'SELECTEDVALUE (slicer)'
    if 'ISBLANK' in skip_reason and 'BLANK' in skip_reason:
        return 'ISBLANK+BLANK guard'
    if 'Cannot resolve' in skip_reason:
        return 'Cannot resolve [ref]'
    if 'Covered by SWITCH' in skip_reason:
        return 'Covered by SWITCH'
    if 'FORMAT' in skip_reason:
        return 'FORMAT function'
    if 'ISFILTERED' in skip_reason:
        return 'ISFILTERED'
    if 'PY/DIVIDE over PBI' in skip_reason or 'DIVIDE over PBI' in skip_reason:
        return 'PBI artifact cascade'
    if 'DIVIDE sub-expression' in skip_reason:
        return 'DIVIDE sub-expression'
    if 'Color' in skip_reason:
        return 'Color formatting'
    if 'BLANK()' in skip_reason:
        return 'BLANK() placeholder'
    if 'Covered on primary' in skip_reason:
        return 'Covered on primary table'
    if 'No matching pattern' in skip_reason or 'No pattern match' in skip_reason:
        return 'No pattern match'
    if 'DAX expression not available' in skip_reason or 'Not available' in skip_reason:
        return 'DAX expression not available'
    return 'Other'


def is_artifact(category: str) -> bool:
    """Check if a category is a PBI artifact (correctly excluded, not a real gap)."""
    return category in NOT_A_GAP


def calculate_unlock_potential(
    untranslatable_measures: list,
    all_measures_dax: list[dict],
) -> dict[str, int]:
    """For each untranslatable measure, count how many other measures reference it.

    Returns a dict: measure_name -> unlock_score (self + downstream refs).
    Only counts standalone [Ref] references, not Table[col] patterns.
    """
    ref_count: Counter = Counter()
    for m in all_measures_dax:
        dax = m.get('dax_expression', '')
        if not dax or dax == 'Not available':
            continue
        for ref_match in re.finditer(r'\[([^\]]+)\]', dax):
            ref_name = ref_match.group(1)
            # Skip Table[col] — check if there's a word char immediately before the bracket
            before = dax[:ref_match.start()].rstrip()
            if before and re.search(r'\w$', before):
                continue
            ref_count[ref_name] += 1

    unlock: dict[str, int] = {}
    for m in untranslatable_measures:
        name = m.original_name
        unlock[name] = ref_count.get(name, 0) + 1  # +1 for itself
    return unlock


def main():
    """Load inputs, run pipeline, and produce gap analysis report."""
    # ── Load inputs (same as run_locally.py) ──
    print('Loading inputs...')
    measures = json.load(open(os.path.join(script_dir, 'measure_table_mapping.json')))
    mquery_entries = json.load(open(os.path.join(script_dir, 'mquery_transpilation.json')))
    rels_raw = json.load(open(os.path.join(script_dir, 'pbi_relationships.json')))
    config = json.load(open(os.path.join(script_dir, 'pipeline_config.json')))

    CATALOG = 'david_test_metrics'
    SCHEMA = 'test_schema'
    for tbl_cfg in config.get('mapping_only_tables', {}).values():
        tbl_cfg['source_table'] = tbl_cfg['source_table'].format(catalog=CATALOG, schema=SCHEMA)

    # ── Parse ──
    parser = MQueryParser()
    mquery_tables = parser.parse_json(mquery_entries)
    fact_tables = {k for k, v in mquery_tables.items() if v.is_fact}
    rel_loader = RelationshipsLoader()
    rel_enrich = rel_loader.load(rels_raw, mquery_tables, fact_tables)
    scan_parser = ScanDataParser()
    scan_path = os.path.join(script_dir, 'scan_result_debug.json')
    scan_data = scan_parser.parse(scan_path) if os.path.exists(scan_path) else {}

    # ── Run pipeline ──
    print('Running pipeline...')
    pipeline = MetricViewPipeline(
        mapping=measures,
        mquery_tables=mquery_tables,
        config=config,
        relationships_enrichment=rel_enrich,
        inactive_relationships=rel_loader.get_inactive_relationships() or None,
        scan_data=scan_data,
        unflatten_tables=True,
        refresh_policy_tables=scan_parser.get_refresh_policy_tables() or None,
        no_summarize_columns=scan_parser.get_no_summarize_columns() or None,
        rls_tables=scan_parser.get_rls_tables() or None,
    )
    specs = pipeline.run()

    # ── Collect all untranslatable measures across all tables ──
    all_untranslatable: list[tuple[str, object]] = []
    for table_key, spec in specs.items():
        for m in spec.untranslatable:
            all_untranslatable.append((table_key, m))

    # Also collect unassigned / cross-table measures
    for m in pipeline.cross_table_measures:
        all_untranslatable.append(('__unassigned__', m))

    # ── Stats ──
    total_t = sum(s.get('translated', 0) for k, s in pipeline.stats.items() if k != '__unassigned__')
    total_m = sum(s.get('total', 0) for k, s in pipeline.stats.items() if k != '__unassigned__')
    total_art = sum(s.get('artifacts', 0) for k, s in pipeline.stats.items() if k != '__unassigned__')
    business_scope = total_m - total_art
    overall_pct = total_t * 100 // total_m if total_m else 0
    business_pct = total_t * 100 // business_scope if business_scope > 0 else 0

    # ── Categorize ──
    by_category: Counter = Counter()
    by_table_category: dict[str, Counter] = defaultdict(Counter)
    for table_key, m in all_untranslatable:
        cat = categorize_reason(m.skip_reason)
        by_category[cat] += 1
        by_table_category[table_key][cat] += 1

    real_gaps = sum(c for cat, c in by_category.items() if not is_artifact(cat))

    # ── Unlock potential ──
    just_measures = [m for _, m in all_untranslatable]
    unlock = calculate_unlock_potential(just_measures, measures)

    # ── Group gaps by config action needed ──
    config_recommendations: list[dict] = []

    # SWITCH gaps by table
    switch_tables: dict[str, list] = defaultdict(list)
    for table_key, m in all_untranslatable:
        cat = categorize_reason(m.skip_reason)
        if cat == 'SELECTEDVALUE+SWITCH':
            switch_tables[table_key].append(m)
    for table_key, ms in sorted(switch_tables.items(), key=lambda x: -len(x[1])):
        total_unlock = sum(unlock.get(m.original_name, 1) for m in ms)
        config_recommendations.append({
            'action': f'Add switch_decompositions for {table_key}',
            'measures': len(ms),
            'unlock': total_unlock,
            'config_key': 'switch_decompositions',
        })

    # Resolve [ref] gaps
    resolve_measures = [(t, m) for t, m in all_untranslatable if 'Cannot resolve' in m.skip_reason]
    if resolve_measures:
        refs: set[str] = set()
        for _, m in resolve_measures:
            ref_match = re.search(r'Cannot resolve \[([^\]]+)\]', m.skip_reason)
            if ref_match:
                refs.add(ref_match.group(1))
        total_unlock = sum(unlock.get(m.original_name, 1) for _, m in resolve_measures)
        config_recommendations.append({
            'action': f'Add measure_resolutions for {len(refs)} unresolved refs',
            'measures': len(resolve_measures),
            'unlock': total_unlock,
            'config_key': 'measure_resolutions',
        })

    # DIVIDE / No pattern gaps
    manual_measures = [
        (t, m) for t, m in all_untranslatable
        if categorize_reason(m.skip_reason) in ('DIVIDE sub-expression', 'No pattern match')
    ]
    if manual_measures:
        total_unlock = sum(unlock.get(m.original_name, 1) for _, m in manual_measures)
        config_recommendations.append({
            'action': 'Add manual_overrides or enable LLM fallback',
            'measures': len(manual_measures),
            'unlock': total_unlock,
            'config_key': 'manual_overrides / use_llm_fallback',
        })

    # Sort by unlock potential (highest first)
    config_recommendations.sort(key=lambda x: -x['unlock'])

    # ── Output ──
    print(f'\n{"="*60}')
    print('GAP ANALYSIS')
    print(f'{"="*60}')
    print(f'COVERAGE: {total_t}/{total_m} translated ({overall_pct}% overall, {business_pct}% business coverage)')
    print(f'  PBI artifacts excluded: {total_art} (correctly not translated)')
    print(f'  Real business gaps: {real_gaps} measures')

    print(f'\nUNTRANSLATABLE BY CATEGORY:')
    for cat, count in by_category.most_common():
        marker = ' [artifact]' if is_artifact(cat) else ''
        print(f'  {count:3d}  {cat}{marker}')

    if config_recommendations:
        print(f'\nTOP GAPS BY UNLOCK POTENTIAL:')
        for i, rec in enumerate(config_recommendations[:5], 1):
            downstream = rec['unlock'] - rec['measures']
            print(f'  {i}. {rec["action"]} -> {rec["measures"]} measures (+{downstream} downstream refs)')

    if config_recommendations:
        top = config_recommendations[0]
        print(f'\nRECOMMENDED NEXT ACTION:')
        print(f'  {top["action"]}')
        print(f'  Config key: {top["config_key"]}')
        if top['config_key'] == 'switch_decompositions':
            print(f'  Template:')
            print(f'    {{"name": "measure_name", "raw_expr": "TODO: SQL expression", "comment": "SWITCH branch"}}')

    # ── Per-table breakdown (real gaps only) ──
    print(f'\nPER-TABLE GAP BREAKDOWN:')
    for table_key in sorted(by_table_category.keys()):
        cats = by_table_category[table_key]
        real = sum(c for cat, c in cats.items() if not is_artifact(cat))
        if real > 0:
            print(f'  {table_key}: {real} real gaps')
            for cat, c in cats.most_common():
                if not is_artifact(cat):
                    print(f'    {c:3d} {cat}')


if __name__ == '__main__':
    main()
