#!/usr/bin/env python3
"""Auto-propose pipeline_config.json from PBI extraction JSONs.

Reads the 4 extraction JSONs (same as run_locally.py) and proposes a
pipeline_config.json that covers 60-70% of the configuration.  The SA
reviews / edits the proposal, then runs Tool 86 with it.

Usage (from src/backend/):
    source .venv/bin/activate
    python ../../examples/uc_metric_view_migration/config_scaffold.py
"""
from __future__ import annotations

import json
import os
import re
import sys
from typing import Any

# ── sys.path setup (same as run_locally.py) ─────────────────────────
script_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(script_dir, '..', '..', 'src', 'backend')
sys.path.insert(0, backend_dir)
sys.path.insert(0, os.path.join(backend_dir, 'src'))

from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
from src.engines.crewai.tools.custom.metric_view_utils.relationships_loader import RelationshipsLoader
from src.engines.crewai.tools.custom.metric_view_utils.scan_data_parser import ScanDataParser
from src.engines.crewai.tools.custom.metric_view_utils.pipeline import MetricViewPipeline
from src.engines.crewai.tools.custom.metric_view_utils.utils import to_snake_case


# ── 1. join_key_map ──────────────────────────────────────────────────

def propose_join_key_map(
    enrichment_joins: dict[str, list[dict]],
    mquery_tables: dict,
) -> dict[str, dict]:
    """Build join_key_map from auto-detected enrichment joins.

    For each enrichment join, extract:
    - alias: the join name (already lowercase / convention)
    - join_key: the fact-side column (parsed from join_on)
    - dim_columns: columns carried from the dim table
    """
    join_key_map: dict[str, dict] = {}
    seen_dim_tables: set[str] = set()

    for _fact_key, joins in enrichment_joins.items():
        for join_entry in joins:
            alias = join_entry.get('name', '')
            join_on = join_entry.get('join_on', '')
            dim_cols = join_entry.get('dim_columns', [])

            # Derive the PBI dim table name from the alias
            # The RelationshipsLoader uses dim_key (original PBI name) as dict key;
            # the alias is the lowercased variant.  We store the alias as-is.
            dim_table_key = _alias_to_dim_key(alias, mquery_tables)
            if not dim_table_key or dim_table_key in seen_dim_tables:
                continue
            seen_dim_tables.add(dim_table_key)

            # Parse join key from join_on: "source.col = alias.dim_col"
            fact_col, dim_col = _parse_join_on(join_on, alias)

            entry: dict[str, Any] = {
                'alias': alias,
                'join_key': fact_col,
                'dim_columns': dim_cols,
            }
            if dim_col and dim_col != fact_col:
                entry['dim_key'] = dim_col

            join_key_map[dim_table_key] = entry

    return join_key_map


def _alias_to_dim_key(alias: str, mquery_tables: dict) -> str:
    """Resolve a join alias back to the original PBI table name."""
    # Direct match
    if alias in mquery_tables:
        return alias
    # Try adding C_ prefix (convention: C_Dim_X -> dim_x alias)
    for tbl_key in mquery_tables:
        if tbl_key.lower() == alias:
            return tbl_key
        # c_dim_calendar -> alias is dim_calendar (loader strips 'c_')
        candidate = tbl_key.lower()
        if candidate.startswith('c_'):
            candidate = candidate[2:]
        if candidate == alias:
            return tbl_key
    return ''


def _parse_join_on(join_on: str, alias: str) -> tuple[str, str]:
    """Extract (fact_column, dim_column) from 'source.col = alias.col'."""
    m = re.match(
        r'source\.(\w+)\s*=\s*' + re.escape(alias) + r'\.(\w+)',
        join_on.strip(),
    )
    if m:
        return m.group(1), m.group(2)
    # Reverse form: alias.col = source.col
    m2 = re.match(
        re.escape(alias) + r'\.(\w+)\s*=\s*source\.(\w+)',
        join_on.strip(),
    )
    if m2:
        return m2.group(2), m2.group(1)
    return '', ''


# ── 2. enrichment_joins (pass-through) ──────────────────────────────

def propose_enrichment_joins(
    enrichment_joins: dict[str, list[dict]],
) -> dict[str, list[dict]]:
    """Pass-through from RelationshipsLoader — 100% accuracy."""
    return enrichment_joins


# ── 3. column_overrides ──────────────────────────────────────────────

def propose_column_overrides(
    mquery_tables: dict,
    measures: list[dict],
) -> dict[str, str]:
    """Detect mismatches between DAX column references and MQuery SQL columns.

    Scans DAX expressions for Table[Column] patterns and compares against
    the snake_case-normalised MQuery column names for each table.
    """
    overrides: dict[str, str] = {}

    # Build column index per table: {table_key: {snake_col_name, ...}}
    table_columns: dict[str, set[str]] = {}
    for tbl_key, info in mquery_tables.items():
        cols: set[str] = set()
        for agg in info.aggregate_columns:
            cols.add(agg['name'])
        for gc in info.group_by_columns:
            cols.add(gc)
        table_columns[tbl_key] = cols

    # Regex for DAX Table[Column] references
    dax_ref_re = re.compile(r"'?(\w[\w\s]*?)'?\[(\w+)\]")

    for m_entry in measures:
        dax = m_entry.get('dax_expression', '')
        alloc_table = m_entry.get('proposed_allocation', '')
        if not dax or dax == 'Not available':
            continue

        for ref_match in dax_ref_re.finditer(dax):
            _tbl_ref = ref_match.group(1).strip()
            dax_col = ref_match.group(2)
            dax_col_snake = to_snake_case(dax_col)

            # Find matching table
            target_table = ''
            for tbl_key in mquery_tables:
                if tbl_key == _tbl_ref or tbl_key.replace(' ', '_') == _tbl_ref.replace(' ', '_'):
                    target_table = tbl_key
                    break

            if not target_table:
                continue

            sql_cols = table_columns.get(target_table, set())
            if not sql_cols:
                continue

            # Check if the DAX column name differs from what MQuery has
            if dax_col_snake not in sql_cols and dax_col not in sql_cols:
                # Try to find the closest match in sql_cols
                for sql_col in sql_cols:
                    if sql_col.lower() == dax_col_snake.lower():
                        override_key = f'{target_table}.{dax_col}'
                        overrides[override_key] = sql_col
                        break

    return overrides


# ── 4. mapping_only_tables ───────────────────────────────────────────

def propose_mapping_only_tables(
    measures: list[dict],
    mquery_tables: dict,
    scan_data: dict,
) -> dict[str, dict]:
    """Identify tables in measure mapping but missing from MQuery.

    These need manual source_table and dimension specs in the config.
    """
    mapping_only: dict[str, dict] = {}

    # Collect all tables that have measures allocated
    allocated_tables: set[str] = set()
    for m_entry in measures:
        alloc = m_entry.get('proposed_allocation', '')
        if alloc:
            allocated_tables.add(alloc)

    mquery_keys = set(mquery_tables.keys())

    for tbl in sorted(allocated_tables - mquery_keys):
        entry: dict[str, Any] = {
            'source_table': '{catalog}.{schema}.' + to_snake_case(tbl),
            'dimensions': [],
        }

        # Try to enrich from scan data
        scan_info = scan_data.get(tbl)
        if scan_info:
            sql = getattr(scan_info, 'native_sql', '')
            if sql:
                # Extract source table from FROM clause
                from_match = re.search(r'\bFROM\s+([\w.]+)', sql, re.IGNORECASE)
                if from_match and '.' in from_match.group(1):
                    entry['source_table'] = from_match.group(1)
                # Try to extract columns
                grp_cols = _extract_group_by_from_sql(sql)
                if grp_cols:
                    entry['dimensions'] = grp_cols

        entry['_TODO'] = 'Verify source_table and dimensions'
        mapping_only[tbl] = entry

    return mapping_only


def _extract_group_by_from_sql(sql: str) -> list[str]:
    """Extract GROUP BY columns from SQL."""
    gb_match = re.search(
        r'GROUP\s+BY\s+(.*?)(?:ORDER\s+BY|HAVING|$)',
        sql, re.IGNORECASE | re.DOTALL,
    )
    if not gb_match:
        return []
    cols = []
    for part in gb_match.group(1).split(','):
        col = part.strip().rstrip(',')
        if '.' in col:
            col = col.split('.')[-1]
        col = col.strip()
        if col and re.match(r'^[a-zA-Z_]\w*$', col):
            cols.append(col)
    return cols


# ── 5. switch_decompositions ─────────────────────────────────────────

def propose_switch_decompositions(
    measures: list[dict],
) -> dict[str, list[dict]]:
    """Detect SELECTEDVALUE+SWITCH measures and produce skeletons.

    These require human review to map the SWITCH branches to SQL FILTER
    expressions, but we can extract the branch names and DAX snippets.
    """
    decompositions: dict[str, list[dict]] = {}

    for m_entry in measures:
        dax = m_entry.get('dax_expression', '')
        name = m_entry.get('measure_name', '')
        alloc_table = m_entry.get('proposed_allocation', '')

        if not dax or dax == 'Not available':
            continue
        if 'SELECTEDVALUE' not in dax.upper() or 'SWITCH' not in dax.upper():
            continue

        branches = _extract_switch_branches(dax)
        skeleton: dict[str, Any] = {
            'name': to_snake_case(name),
            'raw_expr': 'TODO: human fills SQL',
            'comment': f'SWITCH measure from {name}',
        }
        if branches:
            skeleton['_branches'] = branches
            skeleton['comment'] = (
                f'SWITCH({len(branches)} branches): '
                + ', '.join(b.get('case_value', '?') for b in branches[:5])
            )

        decompositions.setdefault(alloc_table, []).append(skeleton)

    return decompositions


def _extract_switch_branches(dax: str) -> list[dict]:
    """Parse SWITCH branches from DAX expression.

    Handles: SWITCH(TRUE(), var = "X", expr_x, var = "Y", expr_y, ...)
    """
    branches: list[dict] = []

    # Try to find SWITCH(TRUE(), ...) block
    switch_match = re.search(
        r'SWITCH\s*\(\s*TRUE\s*\(\s*\)\s*,\s*(.+)',
        dax, re.IGNORECASE | re.DOTALL,
    )
    if not switch_match:
        return branches

    body = switch_match.group(1)

    # Extract branches: condition, expression pairs
    # Pattern: variable = "value", expression, ...
    branch_re = re.compile(
        r'(\w+)\s*=\s*"([^"]+)"\s*,\s*([^,]+?)(?=,\s*\w+\s*=\s*"|$)',
        re.DOTALL,
    )
    for bm in branch_re.finditer(body):
        var_name = bm.group(1)
        case_value = bm.group(2)
        expr_snippet = bm.group(3).strip().rstrip(',').strip()
        branches.append({
            'variable': var_name,
            'case_value': case_value,
            'dax_snippet': expr_snippet[:200],  # truncate long expressions
        })

    return branches


# ── 6. measure_resolutions ───────────────────────────────────────────

def propose_measure_resolutions(
    pipeline: MetricViewPipeline,
) -> dict[str, dict]:
    """Find unresolved [ref] references and try to match them to known measures.

    After a first-pass pipeline run, scan untranslatable measures for
    "Cannot resolve" skip reasons and attempt to map them.
    """
    resolutions: dict[str, dict] = {}

    # Build a lookup: measure_name -> measure entry for quick searching
    measure_lookup: dict[str, dict] = {}
    for m_entry in pipeline.mapping:
        measure_lookup[m_entry['measure_name']] = m_entry

    for _table_key, spec in pipeline.all_specs.items():
        for m in spec.untranslatable:
            reason = m.skip_reason or ''
            if 'Cannot resolve' not in reason:
                continue

            # Extract unresolved references: [Something]
            refs = re.findall(r'\[([^\]]+)\]', reason)
            for ref in refs:
                if ref in resolutions:
                    continue
                # Try to find a matching measure
                matched = measure_lookup.get(ref)
                if matched:
                    resolutions[ref] = {
                        'base_expr': 'TODO: fill SQL expression',
                        'base_filters': [],
                        '_hint': f'Matches measure "{ref}" on table {matched.get("proposed_allocation", "?")}',
                    }

    return resolutions


# ── 7. parameter_defaults ────────────────────────────────────────────

def propose_parameter_defaults(
    mquery_tables: dict,
) -> dict[str, str]:
    """Detect PBI parameters from MQuery SQL patterns.

    Scans for ${ParameterName}, #"ParameterName", and common PBI patterns.
    """
    params: dict[str, str] = {}

    # Patterns for PBI parameters in transpiled SQL
    param_patterns = [
        re.compile(r'\$\{(\w+)\}'),                       # ${ParamName}
        re.compile(r'#"(\w+)"'),                           # #"ParamName"
        re.compile(r':(\w+(?:Filter|Version|Range))\b'),   # :CurrencyFilter
    ]

    for _tbl_key, info in mquery_tables.items():
        sql = info.full_sql or ''
        for pat in param_patterns:
            for pm in pat.finditer(sql):
                param_name = pm.group(1)
                if param_name not in params:
                    params[param_name] = 'TODO: set default value'

    # Also scan raw_transpiled_sql for RowLimit patterns (PBI parameter)
    for _tbl_key, info in mquery_tables.items():
        raw_sql = info.raw_transpiled_sql or ''
        if 'RowLimit' in raw_sql and 'RowLimit' not in params:
            params['RowLimit'] = '0'

    return params


# ── 8. filter_sets ───────────────────────────────────────────────────

def propose_filter_sets(
    switch_decompositions: dict[str, list[dict]],
    measures: list[dict],
) -> dict[str, list[str]]:
    """Collect filter value lists from SWITCH decomposition analysis and DAX.

    Groups filter values by the column being filtered.
    """
    filter_sets: dict[str, list[str]] = {}

    # From switch decomposition branches, collect unique case values
    for _table_key, decomps in switch_decompositions.items():
        for decomp in decomps:
            branches = decomp.get('_branches', [])
            if not branches:
                continue
            # Group by the variable being switched on
            var_name = branches[0].get('variable', '')
            if not var_name:
                continue
            set_key = to_snake_case(var_name).upper()
            values = [b['case_value'] for b in branches if b.get('case_value')]
            if values and set_key not in filter_sets:
                filter_sets[set_key] = sorted(set(values))

    # Scan DAX for IN (...) patterns to detect filter value lists
    in_pattern = re.compile(
        r"(\w+)\s+IN\s*\(\s*\{([^}]+)\}\s*\)",
        re.IGNORECASE,
    )
    for m_entry in measures:
        dax = m_entry.get('dax_expression', '')
        if not dax:
            continue
        for im in in_pattern.finditer(dax):
            col_name = im.group(1)
            values_str = im.group(2)
            values = [v.strip().strip('"').strip("'") for v in values_str.split(',')]
            set_key = to_snake_case(col_name).upper()
            if values and set_key not in filter_sets:
                filter_sets[set_key] = sorted(set(values))

    return filter_sets


# ── Main ─────────────────────────────────────────────────────────────

def main():
    """Load inputs, propose config keys, and write proposed_pipeline_config.json."""
    # ── Load inputs ──
    print('Loading inputs...')
    measures_path = os.path.join(script_dir, 'measure_table_mapping.json')
    mquery_path = os.path.join(script_dir, 'mquery_transpilation.json')
    rels_path = os.path.join(script_dir, 'pbi_relationships.json')
    scan_path = os.path.join(script_dir, 'scan_result_debug.json')

    if not os.path.exists(measures_path):
        print(f'ERROR: {measures_path} not found')
        sys.exit(1)

    with open(measures_path) as f:
        measures = json.load(f)
    with open(mquery_path) as f:
        mquery_entries = json.load(f)
    with open(rels_path) as f:
        rels_raw = json.load(f)

    scan_data: dict = {}
    scan_parser = ScanDataParser()
    if os.path.exists(scan_path):
        scan_data = scan_parser.parse(scan_path)
        print(f'  Scan data: {len(scan_data)} tables')
    else:
        print('  Scan data: not found (skipping)')

    print(f'  Measures: {len(measures)}')
    print(f'  MQuery entries: {len(mquery_entries)}')

    # ── Parse MQuery ──
    parser = MQueryParser()
    mquery_tables = parser.parse_json(mquery_entries)
    fact_tables = {k for k, v in mquery_tables.items() if v.is_fact}
    print(f'  Parsed: {len(mquery_tables)} tables, {len(fact_tables)} fact tables')

    # ── Parse relationships ──
    rel_loader = RelationshipsLoader()
    enrichment = rel_loader.load(rels_raw, mquery_tables, fact_tables)
    total_auto = sum(len(v) for v in enrichment.values())
    print(f'  Relationships: {total_auto} enrichment joins')

    # ── Build config incrementally ──
    print('\nProposing config keys...')
    config: dict[str, Any] = {}

    # 1. join_key_map
    config['join_key_map'] = propose_join_key_map(enrichment, mquery_tables)
    print(f'  join_key_map: {len(config["join_key_map"])} entries')

    # 2. enrichment_joins (pass-through)
    config['enrichment_joins'] = propose_enrichment_joins(enrichment)
    ej_count = sum(len(v) for v in config['enrichment_joins'].values())
    print(f'  enrichment_joins: {ej_count} joins across {len(config["enrichment_joins"])} tables')

    # 3. column_overrides
    config['column_overrides'] = propose_column_overrides(mquery_tables, measures)
    print(f'  column_overrides: {len(config["column_overrides"])} overrides')

    # 4. mapping_only_tables
    config['mapping_only_tables'] = propose_mapping_only_tables(measures, mquery_tables, scan_data)
    print(f'  mapping_only_tables: {len(config["mapping_only_tables"])} tables')

    # 5. switch_decompositions
    switch_decomps = propose_switch_decompositions(measures)
    config['switch_decompositions'] = switch_decomps
    sw_count = sum(len(v) for v in switch_decomps.values())
    print(f'  switch_decompositions: {sw_count} skeletons across {len(switch_decomps)} tables')

    # 6. parameter_defaults
    config['parameter_defaults'] = propose_parameter_defaults(mquery_tables)
    print(f'  parameter_defaults: {len(config["parameter_defaults"])} parameters')

    # 7. filter_sets
    config['filter_sets'] = propose_filter_sets(switch_decomps, measures)
    print(f'  filter_sets: {len(config["filter_sets"])} sets')

    # 8. measure_resolutions (requires first-pass pipeline run)
    print('\n  Running first-pass pipeline for measure_resolutions...')
    try:
        pipeline = MetricViewPipeline(
            mapping=measures,
            mquery_tables=mquery_tables,
            config=config,
            relationships_enrichment=enrichment,
        )
        pipeline.run()
        config['measure_resolutions'] = propose_measure_resolutions(pipeline)
        print(f'  measure_resolutions: {len(config["measure_resolutions"])} proposed')
    except Exception as e:
        print(f'  measure_resolutions: skipped (pipeline error: {e})')
        config['measure_resolutions'] = {}

    # ── Clean up internal keys before output ──
    _clean_internal_keys(config)

    # ── Output ──
    output_path = os.path.join(script_dir, 'proposed_pipeline_config.json')
    with open(output_path, 'w') as f:
        json.dump(config, f, indent=2, default=str)

    # ── Summary ──
    total_keys = sum(1 for v in config.values() if v)
    todo_count = _count_todos(config)
    print(f'\n{"="*60}')
    print(f'PROPOSED CONFIG SUMMARY')
    print(f'{"="*60}')
    print(f'Config keys with data: {total_keys}/8')
    print(f'Items needing human review (TODO): {todo_count}')
    print(f'Output: {output_path}')
    print()
    for key in [
        'join_key_map', 'enrichment_joins', 'column_overrides',
        'mapping_only_tables', 'switch_decompositions',
        'parameter_defaults', 'filter_sets', 'measure_resolutions',
    ]:
        val = config.get(key)
        if isinstance(val, dict):
            count = sum(len(v) if isinstance(v, list) else 1 for v in val.values())
        elif isinstance(val, list):
            count = len(val)
        else:
            count = 0
        status = 'OK' if val else 'EMPTY'
        print(f'  {key}: {count} items [{status}]')


def _clean_internal_keys(config: dict):
    """Remove internal/debug keys (prefixed with _) from the output."""
    for key, val in config.items():
        if isinstance(val, dict):
            for sub_key, sub_val in val.items():
                if isinstance(sub_val, dict):
                    for k in list(sub_val.keys()):
                        if k.startswith('_'):
                            del sub_val[k]
                elif isinstance(sub_val, list):
                    for item in sub_val:
                        if isinstance(item, dict):
                            for k in list(item.keys()):
                                if k.startswith('_'):
                                    del item[k]


def _count_todos(config: dict) -> int:
    """Count values containing 'TODO' anywhere in the config tree."""
    count = 0
    text = json.dumps(config)
    count = text.count('TODO')
    return count


if __name__ == '__main__':
    main()
