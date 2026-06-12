"""Table processor — extracted from pipeline.py for readability.

Contains the per-table translation pipeline (Pass 1, Pass 2, overrides,
SWITCH decomposition, source SQL enrichment, union/embed injection).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

from .data_classes import MetricViewSpec, TableInfo, TranslationResult
from .dependency_graph import build_dependency_graph
from .m_transform_folder import MTransformFolder
from .pbi_parameter_resolver import PbiParameterResolver
from .sql_post_processor import SqlPostProcessor
from .utils import to_snake_case, spark_sql_compat, col_to_readable, run_async

logger = logging.getLogger(__name__)

# Keywords that classify a measure as a PBI UI artifact (not a real business measure)
_ARTIFACT_SKIP_KEYWORDS = (
    'FORMAT', 'Color', 'ISBLANK+BLANK', 'SELECTEDVALUE+SWITCH',
    'SELECTEDVALUE', 'DAX expression not available', 'ISFILTERED',
    'DIVIDE over PBI artifacts', 'PY/DIVIDE over PBI artifacts',
    'BLANK() placeholder', 'cross-table',
    'Covered by SWITCH decomposition',
    'Covered on primary table',
)


@dataclass
class TableProcessorContext:
    """Bundles all pipeline instance variables needed by process_table."""

    config: dict
    mquery_tables: dict[str, TableInfo]
    translator: Any  # DaxTranslator
    join_detector: Any  # JoinDetector
    scan_data: dict
    enrichment_joins: dict
    inactive_rels: list[dict]
    unflatten_tables: bool
    llm_config: dict
    calc_groups: list
    inner_dim_joins: bool
    dimension_exclusions: dict
    cross_table_measures: list[TranslationResult]
    filter_warnings: list[str]
    limitations: dict[str, list]


def expand_calculation_groups(
    base_measures: list[TranslationResult],
    calc_groups: list,
    limitations: dict[str, list],
) -> list[TranslationResult]:
    """Expand calculation groups x base measures into explicit UCMV measures."""
    if not calc_groups:
        return []
    expanded: list[TranslationResult] = []
    for cg in calc_groups:
        cg_name = cg.get('name', 'unknown')
        for item in cg.get('items', []):
            item_name = item.get('name', '')
            item_expr_template = item.get('expression', 'SELECTEDMEASURE()')
            for base in base_measures:
                expanded_name = f"{base.measure_name}_{to_snake_case(item_name)}"
                # Replace SELECTEDMEASURE() with MEASURE(base)
                expanded_expr = item_expr_template.replace(
                    'SELECTEDMEASURE()', f'MEASURE({base.measure_name})')
                expanded.append(TranslationResult(
                    measure_name=expanded_name,
                    original_name=f"{base.original_name} ({item_name})",
                    sql_expr=expanded_expr,
                    is_translatable=True,
                    skip_reason=f'Expanded from calculation group: {cg_name}',
                    dax_expression=f'SELECTEDMEASURE() from {cg_name}/{item_name}',
                    confidence='medium',
                    category='calculation_group',
                ))
    if expanded:
        limitations.setdefault('calculation_groups_expanded', []).append({
            'group_count': len(calc_groups),
            'expanded_count': len(expanded),
        })
    return expanded


def process_table(
    table_key: str,
    table_info: TableInfo,
    dax_measures: list[dict],
    ctx: TableProcessorContext,
    *,
    build_switch_measure_fn: Any,
    resolve_var_chain_fn: Any,
    extract_divide_args_fn: Any,
    clean_unresolved_vars_fn: Any,
    validate_filter_consistency_fn: Any,
) -> MetricViewSpec:
    """Process a single fact table through the translation pipeline.

    Returns a MetricViewSpec.
    """
    source_table = table_info.source_table

    # ── Step 1: Auto-generate base measures from MQuery SUM columns ───
    base_measures: list[TranslationResult] = []
    base_names: set[str] = set()
    for col in table_info.aggregate_columns:
        name = col['name']
        if 'expr' in col:
            expr = col['expr']
        else:
            expr = f"SUM(source.{col['source_col']})"
        base_measures.append(TranslationResult(
            measure_name=name,
            original_name=name,
            sql_expr=expr,
            is_translatable=True,
            skip_reason=col_to_readable(name),
            dax_expression='',
            confidence='high',
            category='base',
        ))
        base_names.add(name)

    # ── Step 2: Auto-generate dimensions from GROUP BY + calculated columns
    dimensions: list[dict] = []
    for col in table_info.group_by_columns:
        dimensions.append({
            'name': col,
            'expr': f'source.{col}',
            'comment': col_to_readable(col),
        })
    for calc in table_info.calculated_columns:
        expr = calc['expr']
        for gb_col in table_info.group_by_columns:
            expr = re.sub(
                rf'(?<![.\w])\b{re.escape(gb_col)}\b(?!\s*\()',
                f'source.{gb_col}', expr,
            )
        # Normalize DATE() -> MAKE_DATE() for Databricks UC Metric Views
        expr = re.sub(r'\bDATE\s*\(', 'MAKE_DATE(', expr)
        dimensions.append({
            'name': calc['name'],
            'expr': expr,
            'comment': f"Calculated: {col_to_readable(calc['name'])}",
        })

    # ── Step 3: Auto-detect joins from DAX dimension refs ─────────────
    joins = ctx.join_detector.detect(table_key, dax_measures, table_info,
                                     inner_dim_joins=ctx.inner_dim_joins)

    # 3b. Auto-detect fact-to-fact joins for cross-table DIVIDEs
    fact_joins = ctx.join_detector.detect_fact_joins(table_key, dax_measures, table_info)
    joins.extend(fact_joins)

    # 3c. Add enrichment joins (domain-specific lookups not in DAX).
    enrichment_cfg = ctx.enrichment_joins.get(table_key, [])
    detected_aliases = {j['name'] for j in joins}
    for ej in enrichment_cfg:
        if ej['name'] in detected_aliases:
            continue  # DAX detector already added this join
        joins.append({
            'name': ej['name'],
            'source': ej['source'],
            'join_on': ej['join_on'],
        })
        detected_aliases.add(ej['name'])
        for col in ej.get('dim_columns', []):
            if isinstance(col, dict):
                dim_name = col['name']
                dim_expr = f"{ej['name']}.{col['expr']}"
            else:
                dim_name = col
                dim_expr = f"{ej['name']}.{col}"
            dimensions.append({
                'name': dim_name,
                'expr': dim_expr,
                'comment': f"{col_to_readable(dim_name)} from {ej['name']}",
            })

    # 3d. Detect USERELATIONSHIP measures and generate alternate join aliases
    userel_pattern = re.compile(
        r'USERELATIONSHIP\s*\(\s*(\w+)\[(\w+)\]\s*,\s*(\w+)\[(\w+)\]\s*\)',
        re.IGNORECASE,
    )
    inactive_rels = ctx.inactive_rels
    for m in dax_measures:
        dax = m.get('dax_expression', '')
        for match in userel_pattern.finditer(dax):
            fact_col = match.group(2)   # e.g. ShipDate
            dim_table = match.group(3)  # e.g. Calendar
            dim_col = match.group(4)    # e.g. Date
            # Find matching inactive relationship
            for irel in inactive_rels:
                if (irel['from_column'].lower() == fact_col.lower()
                        and irel['to_table'].lower() == dim_table.lower()):
                    # Generate alternate join alias
                    base_alias = dim_table.lower()
                    alt_alias = f"{base_alias}_{fact_col.lower()}"
                    # Check if this join already exists
                    if not any(j['name'] == alt_alias for j in joins):
                        dim_info = ctx.mquery_tables.get(irel['to_table'])
                        if dim_info and dim_info.source_table:
                            joins.append({
                                'name': alt_alias,
                                'source': dim_info.source_table,
                                'join_on': f'source.{fact_col.lower()} = {alt_alias}.{dim_col.lower()}',
                                '_userelationship': True,
                            })

    # ── Step 4: Add dimension columns from joined tables ──────────────
    dim_dims = ctx.join_detector.get_dim_dimensions(joins, table_info)
    dimensions.extend(dim_dims)

    # 4b. Normalize dimension names to lowercase
    for d in dimensions:
        if d['name'] != d['name'].lower():
            old_name = d['name']
            d['name'] = old_name.lower()
            d['expr'] = d['expr'].replace(old_name, old_name.lower())

    # 4c. Remove excluded dimensions (join/filter-only columns, not user-facing)
    excluded = ctx.dimension_exclusions.get(table_key, set())
    if excluded:
        dimensions = [d for d in dimensions if d['name'] not in excluded]

    # ── Step 5: Translate DAX measures (pass fact_joins for cross-table resolution)
    translated: list[TranslationResult] = []
    untranslatable: list[TranslationResult] = []
    cross_table_translated_count = 0

    # Set fact joins on translator for cross-table resolution
    ctx.translator.set_fact_joins(fact_joins)

    for m in dax_measures:
        result = ctx.translator.translate(m, table_key)
        if result.is_translatable:
            result.sql_expr = clean_unresolved_vars_fn(result.sql_expr)
            if result.measure_name not in base_names:
                translated.append(result)
                base_names.add(result.measure_name)
                if result.category == 'cross_table_translated':
                    cross_table_translated_count += 1
        else:
            untranslatable.append(result)
            if result.category == 'cross_table':
                ctx.cross_table_measures.append(result)

    # 5a-fix. Resolve window.order: replace hardcoded 'date_key' with actual
    # period dimension from the table's GROUP BY columns.
    _PERIOD_DIM_PRIORITY = ctx.config.get(
        'period_dim_priority', ['fiscper', 'fiscal_year_period', 'date_key'])
    period_dim = next(
        (d for d in _PERIOD_DIM_PRIORITY if d in table_info.group_by_columns),
        'date_key',
    )
    for m in translated:
        if m.window_spec and m.window_spec.get('order') == 'date_key' and period_dim != 'date_key':
            m.window_spec['order'] = period_dim

    # 5a-fix2. Drop window measures on INT period columns (configurable).
    _INT_PERIOD_DIMS = set(ctx.config.get(
        'int_period_dims', ['fiscper', 'fiscal_year_period']))
    if period_dim in _INT_PERIOD_DIMS:
        win_translated = []
        for m in translated:
            if m.window_spec:
                m.window_spec = None
                m.sql_expr = None
                m.is_translatable = False
                m.skip_reason = f'SAMEPERIODLASTYEAR on INT period column ({period_dim}) — needs DATE type'
                untranslatable.append(m)
            else:
                win_translated.append(m)
        translated = win_translated

    # ── Step 5a-override: inject manual overrides ──
    manual_overrides = ctx.config.get('manual_overrides', {}).get(table_key, [])
    _untranslatable_names = {m.measure_name for m in untranslatable}
    for override in manual_overrides:
        override_name = override['name']
        # Skip if already translated AND not still untranslatable
        # (duplicate measure entries can appear in both lists)
        if override_name in base_names and override_name not in _untranslatable_names:
            continue
        result = TranslationResult(
            measure_name=override_name,
            original_name=override.get('original_name', override_name),
            sql_expr=override['expr'],
            is_translatable=True,
            skip_reason=override.get('comment', 'Manual override'),
            dax_expression='manual override',
            confidence='high',
            category='manual_override',
            window_spec=override.get('window'),
        )
        translated.append(result)
        base_names.add(override_name)
        # Remove from untranslatable if it was there
        untranslatable = [m for m in untranslatable if m.measure_name != override_name]

    # ── Step 5b: Pass 2 — measure arithmetic: [A]-[B] -> MEASURE() refs
    original_to_snake: dict[str, str] = {}
    for m in base_measures + translated:
        original_to_snake[m.original_name] = m.measure_name

    # Build dependency graph for topological ordering of untranslatable measures
    dep_graph = build_dependency_graph(
        [{'measure_name': m.original_name, 'dax_expression': m.dax_expression} for m in untranslatable]
    )
    # Mark cyclic measures as untranslatable with specific reason
    for cycle_group in dep_graph.get('cycles', []):
        cycle_names = set(cycle_group)
        for m in untranslatable:
            if m.original_name in cycle_names:
                m.skip_reason = f"Circular dependency: {' ↔ '.join(sorted(cycle_group)[:3])}"
    # Build topo order lookup for processing priority
    _topo_priority = {name: i for i, name in enumerate(dep_graph.get('topo_order', []))}

    for _pass2_iter in range(5):
        pass2_resolved: list[TranslationResult] = []
        still_untranslatable: list[TranslationResult] = []
        # Process in topological order (leaves first, composed measures later)
        sorted_untranslatable = sorted(
            untranslatable,
            key=lambda m: _topo_priority.get(m.original_name, 999)
        )
        for m in sorted_untranslatable:
            dax_expr = m.dax_expression
            # Pre-clean: strip comment lines and date var lines BEFORE ref extraction
            dax_clean_lines = []
            for _line in dax_expr.split('\n'):
                _s = _line.strip()
                if _s.startswith('//'):
                    continue
                if re.match(
                    r'^var\s+\w+\s*=\s*CALCULATE\s*\(\s*\[(F_Start_date|F_End_date|PY_Start_date|PY_End_date)\]',
                    _s, re.IGNORECASE,
                ):
                    continue
                if re.match(r'^var\s+(std|etd)\b', _s, re.IGNORECASE):
                    continue
                dax_clean_lines.append(_s)
            dax_clean = '\n'.join(dax_clean_lines)
            # Find standalone [Ref] (not Table[col])
            measure_refs: set[str] = set()
            for ref_match in re.finditer(r'\[([^\]]+)\]', dax_clean):
                ref_name = ref_match.group(1)
                before = dax_clean[:ref_match.start()].rstrip()
                if before and re.search(r'\w$', before):
                    continue  # Table[col] -- skip
                measure_refs.add(ref_name)
            # Check all refs resolve to translated measures on THIS table
            if measure_refs and all(
                original_to_snake.get(r, to_snake_case(r)) in base_names
                for r in measure_refs
            ):
                expr = dax_expr
                # Strip var/return and comments
                expr_lines: list[str] = []
                for line in expr.split('\n'):
                    s = line.strip()
                    if not s or s.startswith('//'):
                        continue
                    if re.match(r'^var\s+(std|etd|x|y)\b', s, re.IGNORECASE):
                        continue
                    if re.match(r'^var\s+\w+\s*=\s*CALCULATE\s*\(\s*\[(F_Start|F_End|PY_Start|PY_End)', s, re.IGNORECASE):
                        continue
                    vm = re.match(r'^var\s+(\w+)\s*=\s*(.+)$', s, re.IGNORECASE)
                    if vm:
                        expr_lines.append(s)
                        continue
                    if s.lower().startswith('return '):
                        s = s[7:]
                    expr_lines.append(s)
                expr = ' '.join(l.strip() for l in expr_lines if l.strip())
                # Resolve simple var chains
                expr = resolve_var_chain_fn(expr)
                # Replace [Ref] with MEASURE(snake_case)
                for ref_name in sorted(measure_refs, key=len, reverse=True):
                    snake = original_to_snake.get(ref_name, to_snake_case(ref_name))
                    expr = expr.replace(f'[{ref_name}]', f'MEASURE({snake})')
                # Convert DIVIDE(a, b) -> a / NULLIF(b, 0)
                while 'DIVIDE(' in expr.upper():
                    div_args = extract_divide_args_fn(expr)
                    if not div_args:
                        break
                    d_start, d_end, num, den = div_args
                    expr = expr[:d_start] + f'{num.strip()} / NULLIF({den.strip()}, 0)' + expr[d_end:]
                # Clean up DAX remnants
                expr = re.sub(r'\bBLANK\(\)', 'NULL', expr, flags=re.IGNORECASE)
                expr = re.sub(
                    r'\bIF\s*\(\s*ISBLANK\s*\(\s*(MEASURE\([^)]+\))\s*\)\s*,\s*0\s*,\s*\1\s*\)',
                    r'\1', expr, flags=re.IGNORECASE,
                )
                # Strip SAMEPERIODLASTYEAR wrapper BEFORE CALCULATE strip
                expr = re.sub(r',\s*SAMEPERIODLASTYEAR\([^)]+\)', '', expr, flags=re.IGNORECASE)
                # Strip leftover CALCULATE() wrappers (bare, no filter)
                expr = re.sub(r'\bCALCULATE\(\s*(MEASURE\([^)]+\))\s*\)', r'\1', expr, flags=re.IGNORECASE)

                # Convert CALCULATE(SUM(Table[col]))*N -> SUM(source.col) * N
                def _repl_calc_sum(cm):
                    s = f"SUM(source.{cm.group(1)})"
                    if cm.group(2):
                        s += f" * {cm.group(2)}"
                    if cm.group(3):
                        s += f" * {cm.group(3)}"
                    return s
                expr = re.sub(
                    r'\bCALCULATE\s*\(\s*SUM\s*\(\s*\w+\[(\w+)\]\s*\)\s*(?:\*\s*(\d+)\s*)?\)'
                    r'(?:\s*\*\s*(\d+))?',
                    _repl_calc_sum, expr, flags=re.IGNORECASE,
                )
                # Convert bare SUM(Table[col]) -> SUM(source.col)
                expr = re.sub(r'\bSUM\s*\(\s*\w+\[(\w+)\]\s*\)', r'SUM(source.\1)', expr, flags=re.IGNORECASE)
                # Convert bare Table[col] refs -> source.col
                expr = re.sub(r'\b\w+\[(\w+)\]', r'source.\1', expr)
                # Validation gate: reject if DAX-only functions/keywords remain
                # Strip MEASURE(...) wrappers before checking — MEASURE() is valid SQL
                _DAX_ONLY = re.compile(
                    r'\b(SELECTEDVALUE|ISFILTERED|HASONEVALUE|FORMAT|CONTAINSSTRING|'
                    r'SWITCH|CALCULATE\s*\(|SAMEPERIODLASTYEAR|DATEADD|'
                    r'FIRSTDATE|LASTDATE|EARLIER|VALUES|SUMX|SUMMARIZE|'
                    r'var\s+\w+\s*=|return\s)\b',
                    re.IGNORECASE,
                )
                _check_str = re.sub(r'MEASURE\s*\([^)]*\)', '', expr)
                if _DAX_ONLY.search(_check_str):
                    still_untranslatable.append(m)
                    continue
                m.sql_expr = expr
                m.is_translatable = True
                m.skip_reason = ''
                m.category = 'measure_arithmetic'
                pass2_resolved.append(m)
            else:
                still_untranslatable.append(m)
        if not pass2_resolved:
            break  # no progress
        for m in pass2_resolved:
            original_to_snake[m.original_name] = m.measure_name
            base_names.add(m.measure_name)
        translated.extend(pass2_resolved)
        untranslatable = still_untranslatable

    # ── Step 5d: LLM fallback for remaining untranslatable measures (opt-in) ──
    if ctx.llm_config and ctx.llm_config.get('use_llm_fallback'):
        from .dax_llm_fallback import translate_batch_with_llm
        try:
            llm_results = run_async(
                translate_batch_with_llm(
                    measures=untranslatable,
                    table_key=table_key,
                    base_names=base_names,
                    original_to_snake=original_to_snake,
                    model=ctx.llm_config.get('llm_model', 'databricks-claude-sonnet-4'),
                )
            )
            llm_translated = []
            for m in llm_results:
                if m.is_translatable:
                    llm_translated.append(m)
                    base_names.add(m.measure_name)
                    original_to_snake[m.original_name] = m.measure_name
            if llm_translated:
                translated.extend(llm_translated)
                untranslatable = [m for m in untranslatable if m.measure_name not in {t.measure_name for t in llm_translated}]
                logger.info(f"[DAX_LLM] {len(llm_translated)} measures translated via LLM fallback for {table_key}")
        except Exception as e:
            logger.warning(f"[DAX_LLM] LLM fallback failed for {table_key}: {e}")
            # Fail-open: LLM errors never block measures

    # ── Step 5c: Multi-pass cascade — reclassify DIVIDE/SAMEPERIODLASTYEAR/No-match
    for _cascade_pass in range(5):
        artifact_names = {m.original_name for m in untranslatable
                          if any(kw in m.skip_reason
                                 for kw in _ARTIFACT_SKIP_KEYWORDS)}
        changed = 0
        for m in untranslatable:
            cascade_skip = (
                'DIVIDE sub-expression' in m.skip_reason
                or 'No pattern match' in m.skip_reason
                or 'SAMEPERIODLASTYEAR' in m.skip_reason
            )
            if cascade_skip:
                refs: set[str] = set()
                for ref_match in re.finditer(r'\[([^\]]+)\]', m.dax_expression):
                    ref_name = ref_match.group(1)
                    before = m.dax_expression[:ref_match.start()].rstrip()
                    if before and re.search(r"[\w']$", before):
                        continue  # Table[col] or 'Table'[col]
                    refs.add(ref_name)
                if refs and all(r in artifact_names or r in base_names for r in refs) and any(r in artifact_names for r in refs):
                    m.skip_reason = 'PY/DIVIDE over PBI artifacts (display-only)'
                    changed += 1
        if changed == 0:
            break

    # ── Step 6: SWITCH decomposition ──────────────────────────────────
    switch_decomps = ctx.config.get('switch_decompositions', {})
    switch_measures: list[TranslationResult] = []
    if table_key in switch_decomps:
        dax_translated_names = {m.measure_name for m in translated}
        entries = switch_decomps[table_key]
        # Support both list (original monolith) and dict (Kasal structured) formats
        if isinstance(entries, list):
            for defn in entries:
                if defn['name'] not in base_names or defn['name'] in dax_translated_names:
                    switch_measures.append(
                        build_switch_measure_fn(defn, filter_sets=ctx.translator.filter_sets)
                    )
                    base_names.add(defn['name'])
        elif isinstance(entries, dict):
            for parent_name, branches in entries.items():
                if not isinstance(branches, dict):
                    continue
                for branch_name, branch_config in branches.items():
                    snake = to_snake_case(branch_name)
                    sql_expr = branch_config.get('sql_expr', '')
                    if sql_expr:
                        switch_measures.append(TranslationResult(
                            measure_name=snake,
                            original_name=branch_name,
                            sql_expr=sql_expr,
                            is_translatable=True,
                            skip_reason='',
                            dax_expression=f'SWITCH branch of [{parent_name}]',
                            confidence='high',
                            category='switch_decomposition',
                        ))
                        base_names.add(snake)

        if isinstance(entries, list):
            # When SWITCH decompositions exist, remove DAX stubs that are:
            # a) identity ratios (num == den -> always 1.0) -- replaced by decomposed measures
            # b) duplicated by a SWITCH measure with same name
            switch_names = {d['name'] for d in entries}
            added_switch_names = {sm.measure_name for sm in switch_measures}
            filtered_translated = []
            for m in translated:
                expr = m.sql_expr or ''
                # Detect identity ratio: same expression on both sides of / NULLIF(...)
                identity_match = re.match(
                    r'^\(?(.*?)\)?\s*/\s*NULLIF\(\(?(\1)\)?,\s*0\)$', expr)
                if identity_match:
                    continue
                if m.measure_name in switch_names and m.measure_name in added_switch_names:
                    continue
                filtered_translated.append(m)
            translated = filtered_translated

            # 6b. Reclassify untranslatable DAX measures covered by SWITCH decompositions
            reclassified_untranslatable = []
            for m in untranslatable:
                m_snake = to_snake_case(m.original_name) if hasattr(m, 'original_name') else m.measure_name
                if m_snake in switch_names:
                    m.skip_reason = 'Covered by SWITCH decomposition (not a gap)'
                reclassified_untranslatable.append(m)
            untranslatable = reclassified_untranslatable

    # ── Step 6c: Auto-add joins referenced by SWITCH/DAX measures ──────
    _all_translated_measures = translated + switch_measures
    if _all_translated_measures:
        existing_aliases = {j['name'] for j in joins}
        join_key_map = ctx.config.get('join_key_map', {})
        for sm in _all_translated_measures:
            if not sm.sql_expr:
                continue
            for alias_match in re.finditer(r'\b(\w+)\.(\w+)', sm.sql_expr):
                alias = alias_match.group(1)
                if alias == 'source' or alias in existing_aliases:
                    continue
                # Find this alias in join_key_map
                for dim_name, jk in join_key_map.items():
                    if jk.get('alias') == alias:
                        # Try mquery_tables first, then fact table's dim_source_tables
                        dim_info = ctx.mquery_tables.get(dim_name)
                        source_table = ''
                        if dim_info and dim_info.source_table:
                            source_table = dim_info.source_table
                        else:
                            # Fallback: check fact table's dim_source_tables (from MQuery JOINs)
                            fact_dims = table_info.dim_source_tables
                            for fact_alias, fact_src in fact_dims.items():
                                if fact_alias.lower() == alias or dim_name.lower() in fact_alias.lower():
                                    source_table = fact_src
                                    break
                        # Fallback 3: check scan_data for the dimension table
                        if not source_table and ctx.scan_data:
                            scan_entry = ctx.scan_data.get(dim_name)
                            if scan_entry and hasattr(scan_entry, 'native_sql'):
                                # Extract source table from native SQL
                                from_m = re.search(
                                    r'\bFROM\s+([\w.]+)',
                                    scan_entry.native_sql, re.IGNORECASE,
                                )
                                if from_m:
                                    source_table = from_m.group(1)
                        # Fallback 4: use source_table from join_key_map config if provided
                        if not source_table:
                            source_table = jk.get('source_table', '')
                        if not source_table:
                            logger.warning(
                                f"[{table_key}] Cannot auto-add join '{alias}' — "
                                f"no source table found for {dim_name}. "
                                f"Add 'source_table' to join_key_map['{dim_name}'] in config."
                            )
                            break
                        join_key = jk.get('join_key', '')
                        alt_on = jk.get('alt_join_on', '')
                        join_on = alt_on if alt_on else f'source.{join_key} = {alias}.{jk.get("dim_key", join_key)}'
                        joins.append({
                            'name': alias,
                            'source': source_table,
                            'join_on': join_on,
                        })
                        existing_aliases.add(alias)
                        logger.info(f"[{table_key}] Auto-added join '{alias}' → {source_table}")
                        # Add curated dim columns as dimensions
                        for dc in jk.get('dim_columns', []):
                            if isinstance(dc, dict):
                                d_name = dc['name']
                                d_expr = f"{alias}.{dc['expr']}"
                            else:
                                d_name = dc
                                d_expr = f"{alias}.{dc}"
                            if d_name not in {d['name'] for d in dimensions}:
                                dimensions.append({
                                    'name': d_name,
                                    'expr': d_expr,
                                    'comment': f'{d_name.replace("_", " ").capitalize()} from {alias}',
                                })
                        break

    # ── Step 7: Merge: base + DAX translated + SWITCH decomposed ──────
    all_measures = base_measures + translated + switch_measures

    # Expand calculation groups if configured (Agent 8)
    calc_expanded = expand_calculation_groups(
        base_measures, ctx.calc_groups, ctx.limitations)
    if calc_expanded:
        all_measures.extend(calc_expanded)

    # ── Step 7a: Rewrite cross-fact FILTER expressions for pivoted joins.
    for fj in fact_joins:
        pivot_kbi_map = fj.get('_pivot_kbi_map')
        if not pivot_kbi_map:
            continue
        alias = fj['name']
        fj_cfg = fj.get('_fact_join_config', {})
        val_col = fj_cfg.get('value_col', 'val')
        _cm = fj_cfg.get('column_map', {})
        val_col = _cm.get(val_col, val_col)
        kbi_col = fj_cfg.get('pivot_col', 'kbi_col')

        _filter_start = re.compile(
            rf'SUM\({re.escape(alias)}\.{re.escape(val_col)}\)\s*FILTER\s*\(WHERE\s+'
            rf'{re.escape(alias)}\.{re.escape(kbi_col)}\s*'
            rf'(?:=\s*\'(\w+)\'|IN\s*\(([^)]+)\))',
            re.IGNORECASE,
        )

        def _rewrite(expr: str | None, _fs=_filter_start, _alias=alias,
                     _pkm=pivot_kbi_map) -> str | None:
            if not expr:
                return expr
            result_parts: list[str] = []
            pos = 0
            for hit in _fs.finditer(expr):
                result_parts.append(expr[pos:hit.start()])
                if hit.group(1):  # = 'CODE'
                    code = hit.group(1)
                    replacement = f'SUM({_alias}.{_pkm.get(code, f"sc_{code.lower()}")})'
                else:  # IN ('X','Y')
                    codes = re.findall(r"'([A-Z0-9]+)'", hit.group(2))
                    parts = [f'SUM({_alias}.{_pkm.get(c, f"sc_{c.lower()}")})'
                             for c in codes]
                    inner = ' + '.join(parts) if parts else hit.group(0)
                    replacement = f'({inner})' if len(parts) > 1 else inner
                result_parts.append(replacement)
                filter_paren = expr.index('(', hit.start() + expr[hit.start():].upper().find('FILTER') + 6)
                depth, j = 1, filter_paren + 1
                while j < len(expr) and depth > 0:
                    if expr[j] == '(':
                        depth += 1
                    elif expr[j] == ')':
                        depth -= 1
                    j += 1
                pos = j
            result_parts.append(expr[pos:])
            return ''.join(result_parts)

        for m in all_measures:
            m.sql_expr = _rewrite(m.sql_expr)

    # ── Step 7b: Validate filter consistency ──────────────────────────
    fc_warnings = validate_filter_consistency_fn(all_measures)
    if fc_warnings:
        ctx.filter_warnings.extend(
            [f'{table_key}: {w}' for w in fc_warnings]
        )

    # ── Source filter validation ──────────────────────────────────────
    source_filter = ''
    if table_info.static_filters:
        known_cols = set(table_info.group_by_columns)
        known_cols.update(c['name'] for c in table_info.aggregate_columns)
        known_cols.update(c.get('source_col', '') for c in table_info.aggregate_columns)
        known_cols.update(c['name'] for c in table_info.calculated_columns)
        for col_m in re.finditer(r'\b(\w+)\s+AS\s+`?(\w+)`?', table_info.full_sql, re.IGNORECASE):
            known_cols.add(col_m.group(2))
        _CTE_ARTIFACT_COLS = {'row_num', 'rn', 'row_number'}
        valid_filters = []
        for filt in table_info.static_filters:
            ref_cols = set(re.findall(r'\bsource\.(\w+)', filt))
            bare_cols = set(re.findall(r'\b(\w+)\s*(?:=|<>|!=|IN\b|<|>|<=|>=)', filt))
            all_ref_cols = ref_cols | bare_cols
            if all_ref_cols & _CTE_ARTIFACT_COLS:
                ctx.filter_warnings.append(
                    f'{table_key}: dropped filter "{filt}" — CTE artifact column: {all_ref_cols & _CTE_ARTIFACT_COLS}')
                continue
            if known_cols and ref_cols and not ref_cols.issubset(known_cols):
                unknown = ref_cols - known_cols
                ctx.filter_warnings.append(
                    f'{table_key}: dropped filter "{filt}" — unknown columns: {unknown}')
                continue
            _SQL_KEYWORDS = {
                'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'BETWEEN', 'LIKE',
                'TRUE', 'FALSE', 'CURRENT_DATE', 'CAST', 'AS', 'INT', 'SELECT',
                'FROM', 'WHERE', 'HAVING', 'GROUP', 'BY', 'ORDER', 'LIMIT',
                'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'EXISTS',
            }
            real_bare = {c for c in bare_cols if c.upper() not in _SQL_KEYWORDS and not c.isdigit()}
            unknown_bare = real_bare - known_cols - _CTE_ARTIFACT_COLS
            if unknown_bare:
                ctx.filter_warnings.append(
                    f'{table_key}: dropped filter "{filt}" — unknown bare columns: {unknown_bare}')
                continue
            valid_filters.append(filt)
        source_filter = ' AND '.join(valid_filters)

    # ── Source SQL enrichment: inline SQL from PBI native queries ─────
    source_sql = ''

    # Fallback: use raw_transpiled_sql directly when scan data is absent
    if (not source_sql
            and table_info.raw_transpiled_sql
            and table_key not in (ctx.scan_data or {})):
        raw = table_info.raw_transpiled_sql
        as_match = re.search(r'\bAS\s*\n', raw, re.IGNORECASE)
        if not as_match:
            as_match = re.search(r'\bAS\s+(?=SELECT|WITH)', raw, re.IGNORECASE)
        if as_match:
            candidate = raw[as_match.end():].strip()
            first_kw = candidate.lstrip().split()[0].upper() if candidate.strip() else ''
            if first_kw in ('SELECT', 'WITH'):
                resolver = PbiParameterResolver(
                    parameter_defaults=ctx.config.get('parameter_defaults'),
                )
                candidate = resolver.resolve(candidate)
                source_sql = candidate
                source_filter = ''

    if ctx.scan_data and table_key in ctx.scan_data:
        scan_info = ctx.scan_data[table_key]

        base_sql = ''
        if table_info.raw_transpiled_sql:
            raw = table_info.raw_transpiled_sql
            as_match = re.search(r'\bAS\s*\n', raw, re.IGNORECASE)
            if as_match:
                base_sql = raw[as_match.end():].strip()
            else:
                as_match2 = re.search(r'\bAS\s+(?=SELECT|WITH)', raw, re.IGNORECASE)
                if as_match2:
                    base_sql = raw[as_match2.end():].strip()
        if not base_sql:
            base_sql = scan_info.native_sql

        resolver = PbiParameterResolver(
            parameter_defaults=ctx.config.get('parameter_defaults'),
        )
        folder = MTransformFolder()
        post_processor = SqlPostProcessor(
            unflatten_tables=ctx.unflatten_tables,
            expand_re_version=bool(ctx.config.get('parameter_defaults', {}).get('RE_Version_ranges')),
        )
        resolved_sql = resolver.resolve(base_sql)
        resolved_sql = spark_sql_compat(resolved_sql)

        # Count UNION arms before folding to detect arm loss
        _pre_fold_arms = len(folder._split_union(resolved_sql))
        final_sql = folder.fold(resolved_sql, scan_info.m_steps, scan_info.pbi_columns)
        _post_fold_arms = len(re.split(
            r'\bUNION\s+ALL\b|\bUNION\b', final_sql, flags=re.IGNORECASE,
        ))
        if _pre_fold_arms > 1 and _post_fold_arms < _pre_fold_arms:
            logger.warning(
                f"[{table_key}] UNION arm loss during fold: {_pre_fold_arms} → {_post_fold_arms}. "
                f"Falling back to unfolded SQL."
            )
            final_sql = resolved_sql

        final_sql = post_processor.process(final_sql)

        # Normalize mixed-case column aliases to lowercase
        def _lc_alias(m_alias):
            a = m_alias.group(1)
            return f'AS {a.lower()}' if a != a.upper() and a != a.lower() else m_alias.group(0)
        final_sql = re.sub(r'\bAS\s+(\w+)', _lc_alias, final_sql)
        source_sql = final_sql
        source_filter = ''  # folded into the inline SQL

        # Flag aggregation tables (Import mode alongside DirectQuery tables)
        if scan_info.storage_mode == 'Import':
            ctx.limitations.setdefault('aggregation_warnings', []).append({
                'table': table_key,
                'storage_mode': scan_info.storage_mode,
                'warning': 'Import-mode table may be an aggregation table. Verify source grain.',
            })

        # Add dimensions for columns created by M transforms (not in native SQL)
        existing_dim_names = {d['name'] for d in dimensions}
        first_arm = re.split(r'\bUNION\b', final_sql, maxsplit=1, flags=re.IGNORECASE)[0]
        sel_m = re.match(r'\s*SELECT\s+(.*?)\s+FROM\s+', first_arm, re.IGNORECASE | re.DOTALL)
        if sel_m:
            for col_part in MTransformFolder._split_select_columns(sel_m.group(1)):
                col_part = col_part.strip()
                if not col_part or re.match(r'^\s*(SUM|AVG|COUNT|MIN|MAX)\s*\(', col_part, re.IGNORECASE):
                    continue
                as_m = re.search(r'\bAS\s+(\w+)\s*$', col_part, re.IGNORECASE)
                col_name = as_m.group(1) if as_m else col_part.split('.')[-1].strip()
                col_name = re.sub(r'\W+$', '', col_name)
                if not col_name or col_name == '*':
                    continue  # SELECT * or bare expression — skip
                _excl = ctx.dimension_exclusions.get(table_key, set())
                if (col_name.lower() not in existing_dim_names
                        and col_name not in existing_dim_names
                        and col_name.lower() not in _excl):
                    dimensions.append({
                        'name': col_name.lower(),
                        'expr': f'source.{col_name.lower()}',
                        'comment': col_to_readable(col_name),
                    })
                    existing_dim_names.add(col_name.lower())

    # ── Step 7c-union: UNION ALL injection ────────────────────────────
    last_union_arms = [j for j in fact_joins if j.get('_union_mode')]
    union_arms = last_union_arms
    if union_arms and source_sql:
        for ua in union_arms:
            arm_sql = ua['_union_arm_sql']
            null_cols = ua['_null_pivot_cols']
            excl_filter = ua.get('_primary_exclude_filter', '')
            pivot_map = ua.get('_pivot_kbi_map', {})

            wrapped = (
                f"SELECT *, {null_cols}\n"
                f"FROM (\n{source_sql}\n) _primary_source"
            )
            if excl_filter:
                wrapped += f"\nWHERE {excl_filter}"

            agg_nulls = ', '.join(
                f"CAST(NULL AS DOUBLE) AS {c['name']}"
                for c in table_info.aggregate_columns
            )
            union_arm = (
                f"SELECT *, {agg_nulls}\n"
                f"FROM (\n{arm_sql}\n) _union_arm"
            )
            source_sql = f"{wrapped}\nUNION ALL\n{union_arm}"

            existing_dim_names = {d['name'] for d in dimensions}
            for col_name in pivot_map.values():
                if col_name not in existing_dim_names:
                    dimensions.append({
                        'name': col_name,
                        'expr': f'source.{col_name}',
                        'comment': f'Pivot column from union arm ({col_name})',
                    })
                    existing_dim_names.add(col_name)

        joins = [j for j in joins if not j.get('_union_mode')]

    # ── Step 7c-embed: source-embed injection ─────────────────────────
    embed_joins = [j for j in fact_joins if j.get('_source_embed')]
    last_embed_joins = embed_joins
    if embed_joins and source_sql:
        for ej_embed in embed_joins:
            inline_sql = ej_embed['_embed_inline_sql']
            join_keys = ej_embed['_embed_join_key']
            embed_cols = ej_embed['_embed_columns']
            co012_alias = f"_co012_{ej_embed['name']}"

            max_selects = ', '.join(
                f"MAX({co012_alias}.{src_col}) AS {tgt_col}"
                for src_col, tgt_col in embed_cols.items()
            )
            on_clause = ' AND '.join(
                f"_inner.{k} = {co012_alias}.{k}" for k in join_keys
            )
            source_sql = (
                f"SELECT _inner.*, {max_selects}\n"
                f"FROM (\n{source_sql}\n) _inner\n"
                f"LEFT JOIN (\n{inline_sql}\n) {co012_alias}\n"
                f"  ON {on_clause}\n"
                f"GROUP BY ALL"
            )
            existing_dim_names = {d['name'] for d in dimensions}
            for tgt_col in embed_cols.values():
                if tgt_col not in existing_dim_names:
                    dimensions.append({
                        'name': tgt_col,
                        'expr': f'source.{tgt_col}',
                        'comment': f'Embedded from {ej_embed["_pbi_name"]} (grain-safe)',
                    })
                    existing_dim_names.add(tgt_col)

        fact_joins = [j for j in fact_joins if not j.get('_source_embed')]
        joins = [j for j in joins if not j.get('_source_embed')]

    # ── Step 7d-embed: rewrite measure expressions for embedded joins ─
    for ej_emb in last_embed_joins:
        alias_e = ej_emb['name']
        embed_cols = ej_emb['_embed_columns']
        fj_cfg = ej_emb.get('_fact_join_config', {})
        col_map = fj_cfg.get('column_map', {})
        _embed_filter_pat = re.compile(
            rf'\s*FILTER\s*\(WHERE\s+{re.escape(alias_e)}\.[^)]+\)',
            re.IGNORECASE,
        )
        for src_col, tgt_col in embed_cols.items():
            phys_col = col_map.get(src_col, src_col)
            for m in all_measures:
                if m.sql_expr:
                    for agg in ('SUM', 'MAX', 'MIN', 'AVG'):
                        m.sql_expr = m.sql_expr.replace(
                            f'{agg}({alias_e}.{phys_col})',
                            f'SUM(source.{tgt_col})',
                        )
        for m in all_measures:
            if m.sql_expr and alias_e in m.sql_expr:
                m.sql_expr = _embed_filter_pat.sub('', m.sql_expr)

    # ── Step 7b-union: COALESCE rewrite for union-mode joins ──────────
    for fj_u in last_union_arms:
        alias_u = fj_u['name']
        pmap_u = fj_u.get('_pivot_kbi_map', {})
        _src_filter_u = re.compile(
            r'SUM\(source\.(\w+)\)\s*FILTER\s*\(WHERE\s+[^)]+\)',
            re.IGNORECASE,
        )
        for m in all_measures:
            if not m.sql_expr:
                continue
            for col in pmap_u.values():
                m.sql_expr = m.sql_expr.replace(
                    f'SUM({alias_u}.{col})',
                    f'COALESCE(SUM(source.{col}), 0)',
                )
            if 'COALESCE(SUM(source.' in m.sql_expr:
                m.sql_expr = _src_filter_u.sub(
                    lambda hit: f'COALESCE({hit.group(0)}, 0)',
                    m.sql_expr,
                )

    # ── Step 7e: Unflatten join source table names ────────────────────
    if ctx.unflatten_tables and joins:
        for j in joins:
            src = j.get('source', '')
            if '__' in src and '\n' not in src:
                parts = src.split('.')
                if len(parts) == 3 and '__' in parts[2]:
                    sub = parts[2].split('__')
                    if len(sub) >= 3:
                        j['source'] = '.'.join(sub)
                elif len(parts) == 2 and '__' in parts[1]:
                    sub = parts[1].split('__')
                    if len(sub) >= 3:
                        j['source'] = '.'.join(sub)

    # ── Build comment block ───────────────────────────────────────────
    t_count = len(translated)
    sw_count = len(switch_measures)
    u_count = len(untranslatable)
    table_short = table_key.replace('fact_', '').replace('FT_', '').replace('Fact_', '')
    comment_lines = [
        f'{table_short.upper()} — UC Metric View ({table_key})',
        'Auto-generated from MQuery Conversion Report + DAX translation.',
        '',
        f'Source: {source_table.split(".")[-1]}',
        f'Target catalog/schema: {".".join(source_table.split(".")[:2])}',
        '',
        f'{len(base_measures)} base measures (from MQuery SUM columns)',
        f'{t_count} DAX-translated measures'
        + (f' ({cross_table_translated_count} cross-table)' if cross_table_translated_count else ''),
        *([] if sw_count == 0 else [f'{sw_count} SWITCH-decomposed measures']),
        f'{u_count} untranslatable DAX measures (documented below)',
    ]

    if untranslatable:
        comment_lines.append('')
        comment_lines.append('Untranslatable:')
        for r in untranslatable:
            comment_lines.append(f'  {r.original_name} — {r.skip_reason}')

    view_name = f'mv_{table_key.lower().replace("ft_", "").replace("fact_", "")}'

    return MetricViewSpec(
        fact_table_key=table_key,
        source_table=source_table,
        view_name=view_name,
        comment='\n'.join(comment_lines),
        joins=joins,
        dimensions=dimensions,
        measures=all_measures,
        untranslatable=untranslatable,
        base_measure_count=len(base_measures),
        dax_measure_count=len(translated),
        switch_measure_count=len(switch_measures),
        source_filter=source_filter,
        source_sql=source_sql,
    )
