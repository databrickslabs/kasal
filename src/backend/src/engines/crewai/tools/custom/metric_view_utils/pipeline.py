"""MetricViewPipeline — orchestrator that ties all modules together.

Ported from the monolith's MetricViewPipeline (lines 5081-6792) with
exact output parity.  Unlike the source monolith (which writes files),
this version returns structured data (dict[str, MetricViewSpec] + stats)
for use as a Kasal tool.

Heavy lifting is delegated to:
  - table_processor.py  (per-table translation pipeline)
  - artifact_cascade.py (cross-table reclassification, grouping, counting)
"""
from __future__ import annotations

import logging
import re
from dataclasses import replace

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
from .utils import to_snake_case, col_to_readable
from .yaml_emitter import emit_yaml
from .sql_emitter import emit_deploy_sql
from .report_emitter import emit_migration_report
from .table_processor import (
    process_table,
    expand_calculation_groups,
    TableProcessorContext,
    _ARTIFACT_SKIP_KEYWORDS,
)
from .artifact_cascade import (
    cross_table_artifact_cascade,
    count_artifacts,
    collect_unassigned,
    group_by_table,
)

logger = logging.getLogger(__name__)


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
        inactive_relationships: list[dict] | None = None,
        m2n_relationships: list[dict] | None = None,
        refresh_policy_tables: list[dict] | None = None,
        no_summarize_columns: list[dict] | None = None,
        rls_tables: set[str] | None = None,
    ):
        self.mapping = mapping
        self.mquery_tables = mquery_tables
        self.config = config or {}
        self.inner_dim_joins = inner_dim_joins
        self.scan_data = scan_data or {}
        self.unflatten_tables = unflatten_tables
        self.llm_config = llm_config or {}
        self._inactive_rels: list[dict] = inactive_relationships or []
        self._limitations: dict[str, list] = {}
        if self._inactive_rels:
            self._limitations['inactive_relationships'] = self._inactive_rels
        if m2n_relationships:
            self._limitations['m2n_relationships'] = m2n_relationships
        if refresh_policy_tables:
            self._limitations['refresh_policies'] = refresh_policy_tables
        if no_summarize_columns:
            self._limitations['summarization_warnings'] = no_summarize_columns
        if rls_tables:
            self._limitations['rls_tables'] = sorted(rls_tables)

        # Calculation groups (Agent 8)
        self._calc_groups = self.config.get('calculation_groups', [])

        # Perspectives & field parameters (Agent 9)
        perspectives = self.config.get('perspectives', [])
        if perspectives:
            self._limitations['perspectives'] = perspectives

        field_parameters = self.config.get('field_parameters', [])
        if field_parameters:
            self._limitations['field_parameters'] = field_parameters

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

    # ── Calculation group expansion (Agent 8) — delegates to table_processor
    def _expand_calculation_groups(self, base_measures: list[TranslationResult]) -> list[TranslationResult]:
        return expand_calculation_groups(base_measures, self._calc_groups, self._limitations)

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

        # Phase 1b: Measure-driven facts ──────────────────────────────────────
        # A table can parse WITH a source_table yet NOT be detected as a fact
        # because its M-Query resolved to a plain table read (no aggregate SQL /
        # GROUP BY) — typical of "click-together" Power Query sources. If such a
        # table has DAX measures allocated to it, the aggregation lives in the
        # MEASURES, not the source query, so it IS a fact. Promote it here.
        #
        # STRICTLY ADDITIVE + ISOLATED:
        #   - only touches tables NOT already in all_specs (Phase 1 SQL facts and
        #     mapping-only facts are never re-processed),
        #   - only tables the parser gave a real source_table (nothing invented),
        #   - only tables that actually have measures allocated,
        #   - gated by allow_measure_driven_facts (default on; set False to get
        #     the exact prior behaviour).
        # The only tables it can affect are ones that produce ZERO output today.
        if self.config.get('allow_measure_driven_facts', True):
            promoted = 0
            for table_key, table_info in self.mquery_tables.items():
                if table_key in self.all_specs:
                    continue                      # Phase 1 / mapping-only owns it
                if table_info.is_fact:
                    continue                      # real SQL fact — not ours
                if not table_info.source_table:
                    continue                      # no source to point a view at
                dax_measures = measure_groups.get(table_key, [])
                if not dax_measures:
                    continue                      # no measures → correctly skipped
                forced_info = replace(table_info, is_fact=True)
                spec = self._process_table(table_key, forced_info, dax_measures)
                self.all_specs[table_key] = spec
                promoted += 1
            if promoted:
                logger.info(
                    "[MetricViewPipeline] Promoted %d measure-driven fact table(s) "
                    "(plain source + allocated DAX measures, no aggregate SQL)", promoted)

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
                    'cross_table': 0, 'base': 0, 'dax': 0, 'switch': 0, 'manual_override': 0,
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
                'manual_override': sum(1 for m in spec.measures if getattr(m, 'category', '') == 'manual_override'),
                'skipped': False,
            }

        # Handle unassigned measures
        unassigned = measure_groups.get('__unassigned__', [])
        if unassigned:
            self._collect_unassigned(unassigned)

        return self.all_specs

    # ── _process_table() — delegates to table_processor module ────────

    def _process_table(self, table_key: str, table_info: TableInfo,
                       dax_measures: list[dict]) -> MetricViewSpec:
        ctx = TableProcessorContext(
            config=self.config,
            mquery_tables=self.mquery_tables,
            translator=self.translator,
            join_detector=self.join_detector,
            scan_data=self.scan_data,
            enrichment_joins=self._enrichment_joins,
            inactive_rels=self._inactive_rels,
            unflatten_tables=self.unflatten_tables,
            llm_config=self.llm_config,
            calc_groups=self._calc_groups,
            inner_dim_joins=self.inner_dim_joins,
            dimension_exclusions=self._dimension_exclusions,
            cross_table_measures=self.cross_table_measures,
            filter_warnings=self._filter_warnings,
            limitations=self._limitations,
        )
        spec = process_table(
            table_key, table_info, dax_measures, ctx,
            build_switch_measure_fn=self._build_switch_measure,
            resolve_var_chain_fn=MetricViewPipeline._resolve_var_chain,
            extract_divide_args_fn=MetricViewPipeline._extract_divide_args_static,
            clean_unresolved_vars_fn=MetricViewPipeline._clean_unresolved_vars,
            validate_filter_consistency_fn=MetricViewPipeline._validate_filter_consistency,
        )
        self._sanitize_spec_measures(spec)
        return spec

    def _sanitize_spec_measures(self, spec) -> None:
        """P5 correctness batch: apply the SQL measure sanitizer to every measure
        in the spec (no-op divisions, self-divisions, NULL-safe base aggregates)."""
        from .sql_measure_sanitizer import sanitize_measure_sql
        for m in getattr(spec, 'measures', None) or []:
            is_base = getattr(m, 'category', '') == 'base'
            new_sql, note = sanitize_measure_sql(m.sql_expr, is_base=is_base)
            m.sql_expr = new_sql
            if note:
                # Preserve the original reason but append the sanitizer marker so
                # reviewers see why a ratio collapsed. Non-fatal — value still emits.
                _reason = getattr(m, 'skip_reason', '') or ''
                if 'self-division' not in _reason:
                    m.skip_reason = (f'{_reason} [{note}]').strip()
                self._filter_warnings.append(f'{spec.fact_table_key}: {m.measure_name}: {note}')

    # ── Delegates to artifact_cascade module ─────────────────────────

    def _cross_table_artifact_cascade(self, all_specs: dict[str, MetricViewSpec]):
        cross_table_artifact_cascade(all_specs, self.mapping, self._PBI_ARTIFACT_PATTERNS)

    @staticmethod
    def _count_artifacts(untranslatable: list) -> int:
        return count_artifacts(untranslatable)

    def _group_by_table(self) -> dict[str, list[dict]]:
        return group_by_table(
            self.mapping, self._PBI_ARTIFACT_PATTERNS,
            self.mquery_tables, self.config, self.scan_data,
        )

    def _collect_unassigned(self, measures: list[dict]):
        collect_unassigned(measures, self.translator,
                          self.cross_table_measures, self.stats)

    # ── Helper methods (kept in pipeline.py — small, used locally) ───

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
        join_alias = join_alias or self.config.get('switch_join_alias', '')
        join_col = join_col or self.config.get('switch_join_col', '')
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
                        # dax_expression is the ORIGINAL DAX — the validator needs
                        # it to compare against the translated SQL (filter/agg check).
                        'dax_expression': m.dax_expression,
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
        total_measures = sum(s.get('total', 0) for k, s in self.stats.items() if k != '__unassigned__')
        total_translated = sum(s.get('translated', 0) for k, s in self.stats.items() if k != '__unassigned__')
        total_artifacts = sum(s.get('artifacts', 0) for k, s in self.stats.items() if k != '__unassigned__')
        business_scope = total_measures - total_artifacts
        results['business_coverage'] = {
            'total_measures': total_measures,
            'translated': total_translated,
            'artifacts_excluded': total_artifacts,
            'business_scope': business_scope,
            'overall_pct': total_translated * 100 // total_measures if total_measures else 0,
            'business_pct': total_translated * 100 // business_scope if business_scope > 0 else 0,
        }
        results['limitations'] = self._limitations
        results['migration_report'] = emit_migration_report(
            self.all_specs, self.stats, self.config,
            limitations=self._limitations)
        return results

    @staticmethod
    def _resolve_placeholders_in_spec(spec, catalog: str, schema: str) -> None:
        """Substitute any surviving {catalog}/{schema} tokens in a spec's join
        sources + source table. Idempotent, mutates in place. Belt-and-suspenders
        net at the single emit choke point where catalog/schema are known —
        covers API/self-extract mode and externally-supplied configs, not just the
        config-gen path (which also resolves them at build_config time)."""
        def _sub(val):
            if isinstance(val, str) and ('{catalog}' in val or '{schema}' in val):
                return val.replace('{catalog}', catalog).replace('{schema}', schema)
            return val
        if getattr(spec, 'source_table', None):
            spec.source_table = _sub(spec.source_table)
        for j in getattr(spec, 'joins', None) or []:
            if 'source' in j:
                j['source'] = _sub(j['source'])

    def emit_all_yaml(self, catalog: str = 'main', schema: str = 'default') -> dict[str, str]:
        """Emit YAML for all specs. Returns dict[table_key -> yaml_string]."""
        result = {}
        for table_key, spec in self.all_specs.items():
            self._resolve_placeholders_in_spec(spec, catalog, schema)
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
            self._resolve_placeholders_in_spec(spec, catalog, schema)
            result[table_key] = emit_deploy_sql(spec, catalog=catalog, schema=schema)
        return result
