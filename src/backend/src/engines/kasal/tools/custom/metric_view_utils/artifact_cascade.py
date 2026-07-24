"""Artifact cascade — extracted from pipeline.py for readability.

Contains cross-table artifact reclassification, artifact counting,
unassigned measure collection, and measure grouping by table.
"""
from __future__ import annotations

import re
from typing import Any

from .data_classes import MetricViewSpec, TranslationResult
from .utils import to_snake_case

# Keywords that classify a measure as a PBI UI artifact (not a real business measure)
_ARTIFACT_SKIP_KEYWORDS = (
    'FORMAT', 'Color', 'ISBLANK+BLANK', 'SELECTEDVALUE+SWITCH',
    'SELECTEDVALUE', 'DAX expression not available', 'ISFILTERED',
    'DIVIDE over PBI artifacts', 'PY/DIVIDE over PBI artifacts',
    'BLANK() placeholder', 'cross-table',
    'Covered by SWITCH decomposition',
    'Covered on primary table',
)


def cross_table_artifact_cascade(
    all_specs: dict[str, MetricViewSpec],
    mapping: list[dict],
    pbi_artifact_patterns: re.Pattern,
) -> None:
    """Reclassify measures whose [refs] are ALL artifacts across ANY table.

    The per-table cascade only sees artifacts from the same table. This
    global pass builds an artifact name set from ALL tables, so VS% measures
    referencing e.g. [CF_Line_Item_Wrapper_Actual] (in FT_BPC003) from
    FT_bpc003_losses get correctly reclassified.

    Mutates untranslatable measures in-place (updates skip_reason).
    """
    kw = _ARTIFACT_SKIP_KEYWORDS

    for _pass in range(3):
        global_artifact_names: set[str] = set()
        global_translated_names: set[str] = set()
        for spec in all_specs.values():
            for m in spec.untranslatable:
                if any(k in m.skip_reason for k in kw):
                    global_artifact_names.add(m.original_name)
            for m in spec.measures:
                global_translated_names.add(m.original_name)
        # Also include artifact measures that were skipped during grouping
        for m_entry in mapping:
            dax = m_entry.get('dax_expression', '')
            if pbi_artifact_patterns.search(dax):
                global_artifact_names.add(m_entry.get('measure_name', ''))

        changed = 0
        for spec in all_specs.values():
            for m in spec.untranslatable:
                if any(k in m.skip_reason for k in kw):
                    continue
                cascade_skip = (
                    'DIVIDE sub-expression' in m.skip_reason
                    or 'No pattern match' in m.skip_reason
                    or 'SAMEPERIODLASTYEAR' in m.skip_reason
                )
                if not cascade_skip:
                    continue
                refs: set[str] = set()
                for ref_match in re.finditer(r'\[([^\]]+)\]', m.dax_expression):
                    ref_name = ref_match.group(1)
                    before = m.dax_expression[:ref_match.start()].rstrip()
                    if before and re.search(r"[\w']$", before):
                        continue
                    refs.add(ref_name)
                if refs and all(
                    r in global_artifact_names or r in global_translated_names
                    for r in refs
                ) and any(r in global_artifact_names for r in refs):
                    m.skip_reason = 'PY/DIVIDE over PBI artifacts (display-only, cross-table)'
                    changed += 1
        if changed == 0:
            break


def count_artifacts(untranslatable: list) -> int:
    """Count untranslatable measures that are PBI UI artifacts (no SQL needed)."""
    count = 0
    for m in untranslatable:
        reason = m.skip_reason if hasattr(m, 'skip_reason') else ''
        if any(kw in reason for kw in _ARTIFACT_SKIP_KEYWORDS):
            count += 1
    return count


def collect_unassigned(
    measures: list[dict],
    translator: Any,
    cross_table_measures: list[TranslationResult],
    stats: dict[str, dict],
) -> None:
    """Translate unassigned measures and store as cross-table.

    Mutates cross_table_measures and stats in-place.
    """
    for m in measures:
        result = translator.translate(m, '__unassigned__')
        result.category = 'unassigned'
        cross_table_measures.append(result)
    stats['__unassigned__'] = {
        'total': len(measures), 'translated': 0,
        'untranslatable': len(measures), 'cross_table': len(measures),
        'base': 0, 'dax': 0,
        'skipped': True, 'skip_reason': 'Unassigned (multi-table or no fact table refs)',
    }


def group_by_table(
    mapping: list[dict],
    pbi_artifact_patterns: re.Pattern,
    mquery_tables: dict,
    config: dict,
    scan_data: dict | None,
) -> dict[str, list[dict]]:
    """Group measures by their allocations, supporting multi-fact allocation.

    Measures with all_allocations get placed into every table they reference.
    Secondary allocations are tagged with _allocation_role='secondary'.
    PBI display artifacts (FORMAT, Color, etc.) are NOT multi-allocated.
    """
    groups: dict[str, list[dict]] = {}
    for m in mapping:
        allocations = m.get('all_allocations', [])
        if not allocations:
            table = m.get('proposed_allocation', '__unassigned__')
            groups.setdefault(table, []).append(m)
        else:
            dax = m.get('dax_expression', '')
            is_artifact = bool(pbi_artifact_patterns.search(dax))
            for alloc in allocations:
                if alloc['role'] == 'secondary' and is_artifact:
                    continue
                table = alloc['table']
                mapping_only = config.get('mapping_only_tables', {})
                if (table not in mquery_tables
                        and table not in mapping_only
                        and table not in (scan_data or {})):
                    continue
                enriched = {**m, '_allocation_role': alloc['role']}
                groups.setdefault(table, []).append(enriched)
    return groups
