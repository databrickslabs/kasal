"""MetricViewPipeline — orchestrator that ties all modules together.

Ported from the monolith's MetricViewPipeline (lines 5081-6792) with
exact output parity.  Unlike the source monolith (which writes files),
this version returns structured data (dict[str, MetricViewSpec] + stats)
for use as a Kasal tool.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import re
from datetime import datetime

from .data_classes import MetricViewSpec, TableInfo, TranslationResult
from .dax_translator import DaxTranslator
from .join_detector import JoinDetector
from .metadata_generator import MetadataGenerator
from .mquery_parser import MQueryParser
from .pbi_parameter_resolver import PbiParameterResolver
from .scan_data_parser import ScanDataParser
from .sql_post_processor import SqlPostProcessor
from .m_transform_folder import MTransformFolder
from .relationships_loader import RelationshipsLoader
from .utils import to_snake_case, spark_sql_compat, col_to_readable
from .yaml_emitter import emit_yaml
from .sql_emitter import emit_deploy_sql
from .report_emitter import emit_migration_report
from .dependency_graph import build_dependency_graph, _find_measure_refs

logger = logging.getLogger(__name__)


def _run_async(coro):
    """Safely run async code from sync context."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # Inside a running event loop — must use a separate thread
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result(timeout=300)
    else:
        return asyncio.run(coro)


# Keywords that classify a measure as a PBI UI artifact (not a real business measure)
_ARTIFACT_SKIP_KEYWORDS = (
    'FORMAT', 'Color', 'ISBLANK+BLANK', 'SELECTEDVALUE+SWITCH',
    'SELECTEDVALUE', 'DAX expression not available', 'ISFILTERED',
    'DIVIDE over PBI artifacts', 'PY/DIVIDE over PBI artifacts',
    'BLANK() placeholder', 'cross-table',
    'Covered by SWITCH decomposition',
    'Covered on primary table',
)


class MetricViewPipeline:
    """Orchestrate the full MQuery -> UC Metric View generation pipeline.

    Returns structured results (not files). Use emit_yaml/emit_deploy_sql
    to serialize the MetricViewSpec objects.
    """

    _ARTIFACT_SKIP_KEYWORDS = _ARTIFACT_SKIP_KEYWORDS

    # Quick-reject patterns for PBI display-only measures (not worth multi-allocating)
    _PBI_ARTIFACT_PATTERNS = re.compile(
        r'FORMAT\(|_COLOR\b|ISBLANK\(.*BLANK\(\)|SELECTEDVALUE\(|ISFILTERED\(',
        re.IGNORECASE,
    )

    def __init__(
        self,
        mapping: list[dict],
        mquery_tables: dict[str, TableInfo],
        config: dict | None = None,
        inner_dim_joins: bool = False,
        scan_data: dict | None = None,
        unflatten_tables: bool = False,
        relationships_enrichment: dict | None = None,
        llm_config: dict | None = None,
    ):
        self.mapping = mapping
        self.mquery_tables = mquery_tables
        self.config = config or {}
        self.inner_dim_joins = inner_dim_joins
        self.scan_data = scan_data or {}
        self.unflatten_tables = unflatten_tables
        self.llm_config = llm_config or {}
        self.translator = DaxTranslator(config=self.config)
        self.join_detector = JoinDetector(mquery_tables, config=self.config)

        # Merge auto-enrichment joins from PBI relationships.
        # Hardcoded _ENRICHMENT_JOINS take precedence -- auto entries are only added
        # when no existing hardcoded entry covers the same alias.
        # Deduplication against DAX-detected joins happens later in _process_table().
        self._enrichment_joins = dict(self.config.get('enrichment_joins', {}))
        if relationships_enrichment:
            def _base_table_name(source: str) -> str:
                """Normalise any source string to the leaf physical table name.

                Handles both simple dotted refs and inline SQL with FROM clauses,
                plus flattened '__'-separated names. Used for same-table dedup.
                """
                from_hits = re.findall(r'\bFROM\s+([\w.`]+)', source, re.IGNORECASE)
                tbl = from_hits[-1].strip('`') if from_hits else source.strip()
                tbl = tbl.rsplit('.', 1)[-1]   # last dot-segment
                tbl = tbl.rsplit('__', 1)[-1]  # last dunder-segment
                return tbl.lower()

            for tbl_key, auto_joins in relationships_enrichment.items():
                existing = self._enrichment_joins.setdefault(tbl_key, [])
                existing_aliases = {j['name'] for j in existing}
                existing_base_tables = {
                    _base_table_name(j['source'])
                    for j in existing if j.get('source')
                }
                for j in auto_joins:
                    if j['name'] in existing_aliases:
                        continue
                    if _base_table_name(j.get('source', '')) in existing_base_tables:
                        continue
                    existing.append(j)
                    existing_aliases.add(j['name'])
                    existing_base_tables.add(_base_table_name(j.get('source', '')))

        self._dimension_exclusions = self.config.get('dimension_exclusions', {})
        self._measure_metadata = self.config.get('measure_metadata', {})
        self._comment_overrides = self.config.get('comment_overrides', {})
        self._dimension_metadata = self.config.get('dimension_metadata', {})
        self._dimension_order = self.config.get('dimension_order', {})
        self.stats: dict[str, dict] = {}
        self.cross_table_measures: list[TranslationResult] = []
        self._filter_warnings: list[str] = []
        self.all_specs: dict[str, MetricViewSpec] = {}

        # Pipeline helper instances
        self.metadata_gen = MetadataGenerator(
            column_metadata=self.config.get('column_metadata', {}),
            measure_metadata=self._measure_metadata,
            dimension_metadata=self._dimension_metadata,
            comment_overrides=self._comment_overrides,
            dimension_exclusions=self._dimension_exclusions,
            dimension_order=self._dimension_order,
            name_prefixes_to_strip=tuple(self.config.get('name_prefixes_to_strip', ())),
        )
        self.sql_post_processor = SqlPostProcessor(
            unflatten_tables=unflatten_tables,
            expand_re_version=bool(self.config.get('parameter_defaults', {}).get('RE_Version_ranges')),
        )
        self.m_transform_folder = MTransformFolder()
        self.pbi_parameter_resolver = PbiParameterResolver(
            parameter_defaults=self.config.get('parameter_defaults'),
        )

    # ── run() — full pipeline ─────────────────────────────────────────────

    def run(self) -> dict[str, MetricViewSpec]:
        """Execute the full pipeline and return specs per fact table.

        Returns:
            Dict mapping fact_table_key -> MetricViewSpec
        """
        measure_groups = self._group_by_table()

        # Phase 1: Process all tables and collect specs
        self.all_specs = {}

        for table_key, table_info in self.mquery_tables.items():
            if not table_info.is_fact:
                continue

            if not table_info.source_table:
                self.stats[table_key] = {
                    'total': 0, 'translated': 0, 'untranslatable': 0,
                    'cross_table': 0, 'base': 0, 'dax': 0,
                    'skipped': True, 'skip_reason': 'No source table found in SQL',
                }
                continue

            dax_measures = measure_groups.get(table_key, [])
            spec = self._process_table(table_key, table_info, dax_measures)
            self.all_specs[table_key] = spec

        # Handle mapping-only tables (measures in PBI mapping but no MQuery SQL)
        mapping_only_cfg = self.config.get('mapping_only_tables', {})
        for table_key, tbl_cfg in mapping_only_cfg.items():
            dax_measures = measure_groups.get(table_key, [])
            if not dax_measures:
                continue
            stub_info = TableInfo(
                table_name=table_key,
                source_table=tbl_cfg['source_table'],
                aggregate_columns=tbl_cfg.get('aggregate_columns', []),
                group_by_columns=tbl_cfg.get('dimensions', []),
                calculated_columns=[],
                is_fact=True,
                full_sql='',
            )
            spec = self._process_table(table_key, stub_info, dax_measures)
            self.all_specs[table_key] = spec

        # Handle scan-data-only tables (in PBI scan + have DAX measures, but no MQuery/mapping entry)
        if self.scan_data:
            for table_key, scan_info in self.scan_data.items():
                if table_key in self.all_specs:
                    continue  # already processed
                dax_measures = measure_groups.get(table_key, [])
                if not dax_measures:
                    continue  # no measures -> skip
                agg_cols, grp_cols = self._extract_columns_from_scan(scan_info)
                source_table = self._extract_source_table_from_scan(scan_info)
                if not source_table:
                    source_table = f'scan_only.{table_key}'
                stub_info = TableInfo(
                    table_name=table_key,
                    source_table=source_table,
                    aggregate_columns=agg_cols,
                    group_by_columns=grp_cols,
                    calculated_columns=[],
                    is_fact=True,
                    full_sql='',
                )
                spec = self._process_table(table_key, stub_info, dax_measures)
                self.all_specs[table_key] = spec

        # Phase 2: Cross-table artifact cascade
        self._cross_table_artifact_cascade(self.all_specs)

        # Phase 2b: Reclassify secondary allocations that are covered on primary table
        global_covered: set[str] = set()
        for spec in self.all_specs.values():
            for m in spec.measures:
                global_covered.add(m.original_name)
                global_covered.add(to_snake_case(m.original_name))
            for m in spec.untranslatable:
                if any(k in m.skip_reason for k in MetricViewPipeline._ARTIFACT_SKIP_KEYWORDS):
                    global_covered.add(m.original_name)
                    global_covered.add(to_snake_case(m.original_name))
        # Build mapping: measure_name -> primary table allocation
        measure_primary: dict[str, str] = {}
        for m_entry in self.mapping:
            for alloc in m_entry.get('all_allocations', []):
                if alloc['role'] == 'primary':
                    measure_primary[m_entry['measure_name']] = alloc['table']
        kw = MetricViewPipeline._ARTIFACT_SKIP_KEYWORDS
        for spec in self.all_specs.values():
            for m in spec.untranslatable:
                if any(k in m.skip_reason for k in kw):
                    continue  # already an artifact
                primary_table = measure_primary.get(m.original_name)
                if primary_table and primary_table != spec.fact_table_key:
                    if m.original_name in global_covered or to_snake_case(m.original_name) in global_covered:
                        m.skip_reason = 'Covered on primary table (secondary allocation)'

        # Phase 2c: Rebuild YAML comment blocks to reflect updated skip_reasons
        for spec in self.all_specs.values():
            comment_override = self._comment_overrides.get(spec.fact_table_key)
            if comment_override:
                spec.comment = comment_override
            else:
                MetricViewPipeline._rebuild_comment(spec)

        # Phase 3: Collect stats (no file I/O in Kasal — emit is done by caller)
        for table_key, spec in self.all_specs.items():
            m_meta = self._measure_metadata.get(table_key, {})
            d_meta = self._dimension_metadata.get(table_key, {})
            d_order = self._dimension_order.get(table_key)
            yaml_content = emit_yaml(spec, measure_metadata=m_meta,
                                     dimension_metadata=d_meta,
                                     dimension_order=d_order,
                                     percentage_multiplier_patterns=self.config.get('percentage_multiplier_patterns'))

            if not yaml_content:
                self.stats[table_key] = {
                    'total': len(spec.untranslatable),
                    'translated': 0, 'untranslatable': len(spec.untranslatable),
                    'artifacts': self._count_artifacts(spec.untranslatable),
                    'cross_table': 0, 'base': 0, 'dax': 0, 'switch': 0,
                    'skipped': True,
                    'skip_reason': 'All measures dropped by validation (no deployable measures)',
                }
                continue

            self.stats[table_key] = {
                'total': len(spec.measures) + len(spec.untranslatable),
                'translated': len(spec.measures),
                'untranslatable': len(spec.untranslatable),
                'artifacts': self._count_artifacts(spec.untranslatable),
                'cross_table': sum(1 for m in spec.untranslatable if m.category == 'cross_table'),
                'base': spec.base_measure_count,
                'dax': spec.dax_measure_count,
                'switch': spec.switch_measure_count,
                'skipped': False,
            }

        # Handle unassigned measures
        unassigned = measure_groups.get('__unassigned__', [])
        if unassigned:
            self._collect_unassigned(unassigned)

        return self.all_specs

    # ── _process_table() — full per-table pipeline ────────────────────────

    def _process_table(self, table_key: str, table_info: TableInfo,
                       dax_measures: list[dict]) -> MetricViewSpec:
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
        joins = self.join_detector.detect(table_key, dax_measures, table_info,
                                          inner_dim_joins=self.inner_dim_joins)

        # 3b. Auto-detect fact-to-fact joins for cross-table DIVIDEs
        fact_joins = self.join_detector.detect_fact_joins(table_key, dax_measures, table_info)
        joins.extend(fact_joins)

        # 3c. Add enrichment joins (domain-specific lookups not in DAX).
        enrichment_cfg = self._enrichment_joins.get(table_key, [])
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

        # ── Step 4: Add dimension columns from joined tables ──────────────
        dim_dims = self.join_detector.get_dim_dimensions(joins, table_info)
        dimensions.extend(dim_dims)

        # 4b. Normalize dimension names to lowercase
        for d in dimensions:
            if d['name'] != d['name'].lower():
                old_name = d['name']
                d['name'] = old_name.lower()
                d['expr'] = d['expr'].replace(old_name, old_name.lower())

        # 4c. Remove excluded dimensions (join/filter-only columns, not user-facing)
        excluded = self._dimension_exclusions.get(table_key, set())
        if excluded:
            dimensions = [d for d in dimensions if d['name'] not in excluded]

        # ── Step 5: Translate DAX measures (pass fact_joins for cross-table resolution)
        translated: list[TranslationResult] = []
        untranslatable: list[TranslationResult] = []
        cross_table_translated_count = 0

        # Set fact joins on translator for cross-table resolution
        self.translator.set_fact_joins(fact_joins)

        for m in dax_measures:
            result = self.translator.translate(m, table_key)
            if result.is_translatable:
                result.sql_expr = self._clean_unresolved_vars(result.sql_expr)
                if result.measure_name not in base_names:
                    translated.append(result)
                    base_names.add(result.measure_name)
                    if result.category == 'cross_table_translated':
                        cross_table_translated_count += 1
            else:
                untranslatable.append(result)
                if result.category == 'cross_table':
                    self.cross_table_measures.append(result)

        # 5a-fix. Resolve window.order: replace hardcoded 'date_key' with actual
        # period dimension from the table's GROUP BY columns.
        _PERIOD_DIM_PRIORITY = self.config.get(
            'period_dim_priority', ['fiscper', 'fiscal_year_period', 'date_key'])
        period_dim = next(
            (d for d in _PERIOD_DIM_PRIORITY if d in table_info.group_by_columns),
            'date_key',
        )
        for m in translated:
            if m.window_spec and m.window_spec.get('order') == 'date_key' and period_dim != 'date_key':
                m.window_spec['order'] = period_dim

        # 5a-fix2. Drop window measures on INT period columns (configurable).
        _INT_PERIOD_DIMS = set(self.config.get(
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

        # ── Step 5b: Pass 2 — measure arithmetic: [A]-[B] -> MEASURE() refs
        original_to_snake: dict[str, str] = {}
        for m in base_measures + translated:
            original_to_snake[m.original_name] = m.measure_name

        # Build dependency graph for topological ordering of untranslatable measures
        _all_untranslatable_names = {m.original_name for m in untranslatable}
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
                    expr = MetricViewPipeline._resolve_var_chain(expr)
                    # Replace [Ref] with MEASURE(snake_case)
                    for ref_name in sorted(measure_refs, key=len, reverse=True):
                        snake = original_to_snake.get(ref_name, to_snake_case(ref_name))
                        expr = expr.replace(f'[{ref_name}]', f'MEASURE({snake})')
                    # Convert DIVIDE(a, b) -> a / NULLIF(b, 0)
                    while 'DIVIDE(' in expr.upper():
                        div_args = MetricViewPipeline._extract_divide_args_static(expr)
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
                    _DAX_ONLY = re.compile(
                        r'\b(SELECTEDVALUE|ISFILTERED|HASONEVALUE|FORMAT|CONTAINSSTRING|'
                        r'SWITCH|CALCULATE\s*\(|SAMEPERIODLASTYEAR|DATEADD|'
                        r'FIRSTDATE|LASTDATE|EARLIER|VALUES|SUMX|SUMMARIZE|'
                        r'var\s+\w+\s*=|return\s)\b',
                        re.IGNORECASE,
                    )
                    if _DAX_ONLY.search(expr):
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
        if self.llm_config and self.llm_config.get('use_llm_fallback'):
            from .dax_llm_fallback import translate_batch_with_llm
            try:
                llm_results = _run_async(
                    translate_batch_with_llm(
                        measures=untranslatable,
                        table_key=table_key,
                        base_names=base_names,
                        original_to_snake=original_to_snake,
                        model=self.llm_config.get('llm_model', 'databricks-claude-sonnet-4'),
                        workspace_url=self.llm_config.get('llm_workspace_url', ''),
                        token=self.llm_config.get('llm_token', ''),
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
                                     for kw in MetricViewPipeline._ARTIFACT_SKIP_KEYWORDS)}
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
        switch_decomps = self.config.get('switch_decompositions', {})
        switch_measures: list[TranslationResult] = []
        if table_key in switch_decomps:
            dax_translated_names = {m.measure_name for m in translated}
            entries = switch_decomps[table_key]
            # Support both list (original monolith) and dict (Kasal structured) formats
            if isinstance(entries, list):
                for defn in entries:
                    if defn['name'] not in base_names or defn['name'] in dax_translated_names:
                        switch_measures.append(
                            self._build_switch_measure(defn, filter_sets=self.translator.filter_sets)
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

        # ── Step 7: Merge: base + DAX translated + SWITCH decomposed ──────
        all_measures = base_measures + translated + switch_measures

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
        fc_warnings = self._validate_filter_consistency(all_measures)
        if fc_warnings:
            self._filter_warnings.extend(
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
                    self._filter_warnings.append(
                        f'{table_key}: dropped filter "{filt}" — CTE artifact column: {all_ref_cols & _CTE_ARTIFACT_COLS}')
                    continue
                if known_cols and ref_cols and not ref_cols.issubset(known_cols):
                    unknown = ref_cols - known_cols
                    self._filter_warnings.append(
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
                    self._filter_warnings.append(
                        f'{table_key}: dropped filter "{filt}" — unknown bare columns: {unknown_bare}')
                    continue
                valid_filters.append(filt)
            source_filter = ' AND '.join(valid_filters)

        # ── Source SQL enrichment: inline SQL from PBI native queries ─────
        source_sql = ''

        # Fallback: use raw_transpiled_sql directly when scan data is absent
        if (not source_sql
                and table_info.raw_transpiled_sql
                and table_key not in (self.scan_data or {})):
            raw = table_info.raw_transpiled_sql
            as_match = re.search(r'\bAS\s*\n', raw, re.IGNORECASE)
            if not as_match:
                as_match = re.search(r'\bAS\s+(?=SELECT|WITH)', raw, re.IGNORECASE)
            if as_match:
                candidate = raw[as_match.end():].strip()
                first_kw = candidate.lstrip().split()[0].upper() if candidate.strip() else ''
                if first_kw in ('SELECT', 'WITH'):
                    resolver = PbiParameterResolver(
                        parameter_defaults=self.config.get('parameter_defaults'),
                    )
                    candidate = resolver.resolve(candidate)
                    source_sql = candidate
                    source_filter = ''

        if self.scan_data and table_key in self.scan_data:
            scan_info = self.scan_data[table_key]

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
                parameter_defaults=self.config.get('parameter_defaults'),
            )
            folder = MTransformFolder()
            post_processor = SqlPostProcessor(
                unflatten_tables=self.unflatten_tables,
                expand_re_version=bool(self.config.get('parameter_defaults', {}).get('RE_Version_ranges')),
            )
            resolved_sql = resolver.resolve(base_sql)
            resolved_sql = spark_sql_compat(resolved_sql)
            final_sql = folder.fold(resolved_sql, scan_info.m_steps, scan_info.pbi_columns)
            final_sql = post_processor.process(final_sql)

            # Normalize mixed-case column aliases to lowercase
            def _lc_alias(m_alias):
                a = m_alias.group(1)
                return f'AS {a.lower()}' if a != a.upper() and a != a.lower() else m_alias.group(0)
            final_sql = re.sub(r'\bAS\s+(\w+)', _lc_alias, final_sql)
            source_sql = final_sql
            source_filter = ''  # folded into the inline SQL

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
                    _excl = self._dimension_exclusions.get(table_key, set())
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
        self._last_union_arms = [j for j in fact_joins if j.get('_union_mode')]
        union_arms = self._last_union_arms
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
        self._last_embed_joins = embed_joins
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
        for ej_emb in getattr(self, '_last_embed_joins', []):
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
        for fj_u in getattr(self, '_last_union_arms', []):
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
        if self.unflatten_tables and joins:
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

    # ── Helper methods ────────────────────────────────────────────────────

    @staticmethod
    def _rebuild_comment(spec: MetricViewSpec):
        """Rebuild the YAML comment block from current spec state.

        Called after cross-table cascade to ensure skip_reasons in the comment
        match the (possibly updated) untranslatable list.
        """
        src = spec.source_table
        table_short = spec.fact_table_key.replace('fact_', '').replace('FT_', '').replace('Fact_', '')
        cross_ct = sum(1 for m in spec.measures if getattr(m, 'category', '') == 'cross_table')
        lines = [
            f'{table_short.upper()} — UC Metric View ({spec.fact_table_key})',
            'Auto-generated from MQuery Conversion Report + DAX translation.',
            '',
            f'Source: {src.split(".")[-1]}',
            f'Target catalog/schema: {".".join(src.split(".")[:2])}',
            '',
            f'{spec.base_measure_count} base measures (from MQuery SUM columns)',
            f'{spec.dax_measure_count} DAX-translated measures'
            + (f' ({cross_ct} cross-table)' if cross_ct else ''),
        ]
        if spec.switch_measure_count:
            lines.append(f'{spec.switch_measure_count} SWITCH-decomposed measures')
        lines.append(f'{len(spec.untranslatable)} untranslatable DAX measures (documented below)')
        if spec.untranslatable:
            lines.append('')
            lines.append('Untranslatable:')
            for r in spec.untranslatable:
                lines.append(f'  {r.original_name} — {r.skip_reason}')
        spec.comment = '\n'.join(lines)

    def _cross_table_artifact_cascade(self, all_specs: dict[str, MetricViewSpec]):
        """Reclassify measures whose [refs] are ALL artifacts across ANY table.

        The per-table cascade only sees artifacts from the same table. This
        global pass builds an artifact name set from ALL tables, so VS% measures
        referencing e.g. [CF_Line_Item_Wrapper_Actual] (in FT_BPC003) from
        FT_bpc003_losses get correctly reclassified.
        """
        kw = MetricViewPipeline._ARTIFACT_SKIP_KEYWORDS

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
            for m_entry in self.mapping:
                dax = m_entry.get('dax_expression', '')
                if self._PBI_ARTIFACT_PATTERNS.search(dax):
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

    @staticmethod
    def _count_artifacts(untranslatable: list) -> int:
        """Count untranslatable measures that are PBI UI artifacts (no SQL needed)."""
        count = 0
        for m in untranslatable:
            reason = m.skip_reason if hasattr(m, 'skip_reason') else ''
            if any(kw in reason for kw in MetricViewPipeline._ARTIFACT_SKIP_KEYWORDS):
                count += 1
        return count

    def _group_by_table(self) -> dict[str, list[dict]]:
        """Group measures by their allocations, supporting multi-fact allocation.

        Measures with all_allocations get placed into every table they reference.
        Secondary allocations are tagged with _allocation_role='secondary'.
        PBI display artifacts (FORMAT, Color, etc.) are NOT multi-allocated.
        """
        groups: dict[str, list[dict]] = {}
        for m in self.mapping:
            allocations = m.get('all_allocations', [])
            if not allocations:
                table = m.get('proposed_allocation', '__unassigned__')
                groups.setdefault(table, []).append(m)
            else:
                dax = m.get('dax_expression', '')
                is_artifact = bool(self._PBI_ARTIFACT_PATTERNS.search(dax))
                for alloc in allocations:
                    if alloc['role'] == 'secondary' and is_artifact:
                        continue
                    table = alloc['table']
                    mapping_only = self.config.get('mapping_only_tables', {})
                    if (table not in self.mquery_tables
                            and table not in mapping_only
                            and table not in (self.scan_data or {})):
                        continue
                    enriched = {**m, '_allocation_role': alloc['role']}
                    groups.setdefault(table, []).append(enriched)
        return groups

    def _collect_unassigned(self, measures: list[dict]):
        """Translate unassigned measures and store as cross-table."""
        for m in measures:
            result = self.translator.translate(m, '__unassigned__')
            result.category = 'unassigned'
            self.cross_table_measures.append(result)
        self.stats['__unassigned__'] = {
            'total': len(measures), 'translated': 0,
            'untranslatable': len(measures), 'cross_table': len(measures),
            'base': 0, 'dax': 0,
            'skipped': True, 'skip_reason': 'Unassigned (multi-table or no fact table refs)',
        }

    @staticmethod
    def _extract_columns_from_scan(scan_info) -> tuple[list[dict], list[str]]:
        """Extract aggregate and group-by columns from scan data SQL.

        Returns: (aggregate_columns, group_by_columns)
        """
        sql = scan_info.native_sql
        first_arm = re.split(r'\bunion\b', sql, maxsplit=1, flags=re.IGNORECASE)[0]
        m = re.match(r'\s*SELECT\s+(.*?)\s+FROM\s+', first_arm, re.IGNORECASE | re.DOTALL)
        if not m:
            return [], []
        select_body = m.group(1)
        agg_cols = []
        grp_cols = []
        for part in MTransformFolder._split_select_columns(select_body):
            part = part.strip()
            if not part:
                continue
            as_match = re.search(r'\bAS\s+(\w+)\s*$', part, re.IGNORECASE)
            alias = as_match.group(1) if as_match else part.split('.')[-1].strip()
            alias = re.sub(r'\W+$', '', alias)
            if re.match(r'^\s*(SUM|AVG|COUNT|MIN|MAX)\s*\(', part, re.IGNORECASE):
                source_col = alias
                inner_match = re.match(r'^\s*\w+\s*\(([^()]+)\)\s*', part)
                if inner_match:
                    inner = inner_match.group(1).strip()
                    source_col = inner.split('.')[-1].strip()
                agg_cols.append({'name': alias, 'source_col': source_col})
            elif re.match(r'^[a-zA-Z_]\w*$', alias):
                grp_cols.append(alias)
        return agg_cols, grp_cols

    @staticmethod
    def _extract_source_table_from_scan(scan_info) -> str:
        """Extract source table name from scan data SQL's FROM clause."""
        sql = scan_info.native_sql
        first_arm = re.split(r'\bunion\b', sql, maxsplit=1, flags=re.IGNORECASE)[0]
        m = re.search(r'\bFROM\s+([\w.]+)', first_arm, re.IGNORECASE)
        return m.group(1) if m else ''

    @staticmethod
    def _clean_unresolved_vars(expr: str | None) -> str | None:
        """Strip unresolved scorecard variable artifacts like +a1, +b1, +res11."""
        if not expr:
            return expr
        expr = re.sub(r'\)\s*\+\s*[a-z]\w*\b', ')', expr)
        return expr

    @staticmethod
    def _validate_filter_consistency(measures: list[TranslationResult]) -> list[str]:
        """Check DIVIDE measures for mismatched filter sets on numerator vs denominator.

        Returns list of warning strings for measures where numerator and denominator
        use different FILTER clauses (which may cause NULL vs value discrepancies).
        """
        warnings = []
        for m in measures:
            expr = m.sql_expr or ''
            if '/ NULLIF(' not in expr:
                continue
            parts = expr.split('/ NULLIF(', 1)
            if len(parts) != 2:
                continue
            num_filters = set(re.findall(r"FILTER\s*\(WHERE\s+([^)]+)\)", parts[0]))
            den_filters = set(re.findall(r"FILTER\s*\(WHERE\s+([^)]+)\)", parts[1]))
            if num_filters and den_filters and num_filters != den_filters:
                warnings.append(
                    f'{m.measure_name}: numerator and denominator have different FILTER clauses — '
                    f'may produce NULL vs value discrepancies in LEFT JOIN scenarios'
                )
        return warnings

    def _build_switch_measure(self, defn: dict, join_alias: str | None = None,
                              join_col: str | None = None,
                              filter_sets: dict | None = None) -> TranslationResult:
        """Build a TranslationResult from a SWITCH decomposition definition."""
        join_alias = join_alias or self.config.get('switch_join_alias', 'dim_wkctr')
        join_col = join_col or self.config.get('switch_join_col', 'bic_cwc_type')
        _fs = filter_sets or {}

        def _filter_clause(fs_key: str) -> str:
            values = _fs[fs_key]
            quoted = ', '.join(f"'{v}'" for v in values)
            return f"FILTER (WHERE {join_alias}.{join_col} IN ({quoted}))"

        # Support raw_expr for pre-built SQL expressions
        if 'raw_expr' in defn:
            return TranslationResult(
                measure_name=defn['name'],
                original_name=defn['name'],
                sql_expr=defn['raw_expr'],
                is_translatable=True,
                skip_reason=defn.get('comment', 'SWITCH decomposition'),
                dax_expression='SWITCH decomposition',
                confidence='high',
                category='switch_decomposition',
                window_spec=defn.get('window'),
            )

        num = defn['num']
        den = defn['den']
        num_fs = defn['num_fs']
        den_fs = defn['den_fs']

        if num_fs is None:
            # Complex expression -- already has filter placeholders
            for fs_key, fs_vals in _fs.items():
                quoted = ', '.join(f"'{v}'" for v in fs_vals)
                placeholder = '{' + fs_key.lower() + '}'
                fc = f"(WHERE {join_alias}.{join_col} IN ({quoted}))"
                num = num.replace(placeholder, fc)
            num_expr = num
        else:
            num_expr = f"SUM(source.{num}) {_filter_clause(num_fs)}"

        if den_fs is None:
            den_expr = den
        else:
            den_expr = f"SUM(source.{den}) {_filter_clause(den_fs)}"

        num_wrapped = f'({num_expr})' if '+' in num_expr or '-' in num_expr else num_expr
        sql_expr = f"{num_wrapped} / NULLIF({den_expr}, 0)"

        return TranslationResult(
            measure_name=defn['name'],
            original_name=defn['name'],
            sql_expr=sql_expr,
            is_translatable=True,
            skip_reason=defn.get('comment', 'SWITCH decomposition'),
            dax_expression='SWITCH decomposition',
            confidence='high',
            category='switch_decomposition',
        )

    @staticmethod
    def _resolve_var_chain(dax: str) -> str:
        """Resolve chained VAR assignments up to 5 levels deep.

        For example:
            var a = SUM(t[x])
            var b = a + 1
            return b
        becomes:
            var b = SUM(t[x]) + 1
            return b

        Only resolves simple scalar assignments (not CALCULATE/SUMX blocks).
        """
        # Normalize single-line multi-var
        dax = re.sub(r'\s+(?=\bvar\s)', '\n', dax, flags=re.IGNORECASE)
        dax = re.sub(r'\s+(?=\breturn\s)', '\n', dax, flags=re.IGNORECASE)
        var_map: dict[str, str] = {}
        lines = dax.split('\n')
        for line in lines:
            m = re.match(r'\s*var\s+(\w+)\s*=\s*(.+)', line.strip(), re.IGNORECASE)
            if m:
                var_name = m.group(1)
                var_expr = m.group(2).strip()
                if var_expr.count('(') != var_expr.count(')'):
                    continue
                if re.search(r'\b(CALCULATE|SUMX|FILTER|COUNTX|AVERAGEX)\s*\(', var_expr, re.IGNORECASE):
                    continue
                for prev_var in sorted(var_map.keys(), key=len, reverse=True):
                    var_expr = re.sub(rf'\b{re.escape(prev_var)}\b', var_map[prev_var], var_expr)
                var_map[var_name] = var_expr
        if not var_map:
            return dax
        result_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped.lower().startswith('return '):
                ret_expr = stripped[7:].strip()
                for var_name in sorted(var_map.keys(), key=len, reverse=True):
                    ret_expr = re.sub(rf'\b{re.escape(var_name)}\b', var_map[var_name], ret_expr)
                result_lines.append(f'return {ret_expr}')
            else:
                result_lines.append(line)
        return '\n'.join(result_lines)

    @staticmethod
    def _extract_divide_args_static(raw: str) -> tuple[int, int, str, str] | None:
        """Return (start, end, numerator, denominator) of the first DIVIDE(...) in *raw*."""
        m = re.search(r'DIVIDE\s*\(', raw, re.IGNORECASE)
        if not m:
            return None
        div_start = m.start()
        inner_start = m.end()
        depth = 1
        pos = inner_start
        comma_positions: list[int] = []
        while pos < len(raw) and depth > 0:
            ch = raw[pos]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    break
            elif ch == ',' and depth == 1:
                comma_positions.append(pos)
            pos += 1
        if len(comma_positions) >= 1:
            num = raw[inner_start:comma_positions[0]]
            if len(comma_positions) >= 2:
                den = raw[comma_positions[0] + 1:comma_positions[1]]
            else:
                den = raw[comma_positions[0] + 1:pos]
            div_end = pos + 1  # past the closing paren
            return div_start, div_end, num.strip(), den.strip()
        return None

    # ── Serialization ─────────────────────────────────────────────────────

    def get_results(self) -> dict:
        """Return pipeline results as a serializable dict."""
        results = {
            'specs': {},
            'stats': self.stats,
            'cross_table_count': len(self.cross_table_measures),
            'filter_warnings': self._filter_warnings,
        }
        for table_key, spec in self.all_specs.items():
            results['specs'][table_key] = {
                'fact_table_key': spec.fact_table_key,
                'source_table': spec.source_table,
                'view_name': spec.view_name,
                'measures_count': len(spec.measures),
                'untranslatable_count': len(spec.untranslatable),
                'base_measure_count': spec.base_measure_count,
                'dax_measure_count': spec.dax_measure_count,
                'switch_measure_count': spec.switch_measure_count,
                'joins_count': len(spec.joins),
                'dimensions_count': len(spec.dimensions),
                'measures': [
                    {
                        'name': m.measure_name,
                        'original_name': m.original_name,
                        'sql_expr': m.sql_expr,
                        'confidence': m.confidence,
                        'category': m.category,
                    }
                    for m in spec.measures
                ],
                'untranslatable': [
                    {
                        'name': m.original_name,
                        'skip_reason': m.skip_reason,
                        'category': m.category,
                    }
                    for m in spec.untranslatable
                ],
            }
        results['migration_report'] = emit_migration_report(
            self.all_specs, self.stats, self.config)
        return results

    def emit_all_yaml(self, catalog: str = 'main', schema: str = 'default') -> dict[str, str]:
        """Emit YAML for all specs. Returns dict[table_key -> yaml_string]."""
        result = {}
        for table_key, spec in self.all_specs.items():
            dim_meta = self._dimension_metadata.get(table_key, {})
            meas_meta = self._measure_metadata.get(table_key, {})
            dim_order = self.metadata_gen.get_dimension_order(table_key)
            result[table_key] = emit_yaml(
                spec,
                measure_metadata=meas_meta,
                dimension_metadata=dim_meta,
                dimension_order=dim_order,
                column_alias_map=getattr(self, '_column_alias_map', {}),
                known_missing_tables=getattr(self, '_known_missing_tables', set()),
                fact_join_map=getattr(self, '_fact_join_map', {}),
                percentage_multiplier_patterns=self.config.get('percentage_multiplier_patterns'),
                budget_suffix=self.config.get('budget_suffix'),
            )
        return result

    def emit_all_sql(self, catalog: str = 'main', schema: str = 'default') -> dict[str, str]:
        """Emit deploy SQL for all specs. Returns dict[table_key -> sql_string]."""
        result = {}
        for table_key, spec in self.all_specs.items():
            result[table_key] = emit_deploy_sql(spec, catalog=catalog, schema=schema)
        return result
