"""MetricViewPipeline — orchestrator that ties all modules together.

Unlike the source monolith (which writes files), this version returns
structured data (dict[str, MetricViewSpec] + stats) for use as a Kasal tool.
"""
from __future__ import annotations

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

logger = logging.getLogger(__name__)

# Keywords that classify a measure as a PBI UI artifact (not a real business measure)
_ARTIFACT_SKIP_KEYWORDS = (
    'Color', 'FORMAT', 'ISBLANK+BLANK', 'SELECTEDVALUE+SWITCH',
    'SELECTEDVALUE', 'ISFILTERED', 'DIVIDE over PBI artifacts',
    'PY/DIVIDE over PBI artifacts', 'Covered on primary table',
    'Covered by SWITCH decomposition', 'BLANK() placeholder',
    'DAX expression not available', 'cross-table',
)


class MetricViewPipeline:
    """Orchestrate the full MQuery → UC Metric View generation pipeline.

    Returns structured results (not files). Use emit_yaml/emit_deploy_sql
    to serialize the MetricViewSpec objects.
    """

    _ARTIFACT_SKIP_KEYWORDS = _ARTIFACT_SKIP_KEYWORDS

    def __init__(
        self,
        mapping: list[dict],
        mquery_tables: dict[str, TableInfo],
        config: dict | None = None,
        inner_dim_joins: bool = False,
        scan_data: dict | None = None,
        unflatten_tables: bool = False,
        relationships_enrichment: dict | None = None,
    ):
        self.mapping = mapping
        self.mquery_tables = mquery_tables
        self.config = config or {}
        self.inner_dim_joins = inner_dim_joins
        self.scan_data = scan_data or {}
        self.unflatten_tables = unflatten_tables
        self.relationships_enrichment = relationships_enrichment or {}

        # Pipeline components
        self.dax_translator = DaxTranslator(config=self.config)
        self.join_detector = JoinDetector(mquery_tables, config=self.config)
        self.metadata_gen = MetadataGenerator(
            column_metadata=self.config.get('column_metadata', {}),
            measure_metadata=self.config.get('measure_metadata', {}),
            dimension_metadata=self.config.get('dimension_metadata', {}),
            comment_overrides=self.config.get('comment_overrides', {}),
            dimension_exclusions=self.config.get('dimension_exclusions', {}),
            dimension_order=self.config.get('dimension_order', {}),
        )
        self.sql_post_processor = SqlPostProcessor(unflatten_tables=unflatten_tables)
        self.m_transform_folder = MTransformFolder()
        self.pbi_parameter_resolver = PbiParameterResolver()

        # Results
        self.all_specs: dict[str, MetricViewSpec] = {}
        self.stats: dict[str, dict] = {}
        self.cross_table_measures: list[TranslationResult] = []
        self._enrichment_joins: dict[str, list[dict]] = {}
        self._filter_warnings: list[str] = []

    def run(self) -> dict[str, MetricViewSpec]:
        """Execute the full pipeline and return specs per fact table.

        Returns:
            Dict mapping fact_table_key → MetricViewSpec
        """
        # Step 1: Group measures by proposed_allocation
        measure_groups: dict[str, list[dict]] = {}
        for m in self.mapping:
            table = m.get('proposed_allocation', '__unassigned__')
            measure_groups.setdefault(table, []).append(m)

        # Step 2: Initialize enrichment joins from config + relationships
        self._enrichment_joins = dict(self.config.get('enrichment_joins', {}))
        if self.relationships_enrichment:
            self._merge_relationship_enrichments()

        # Step 3: Process each fact table
        fact_tables = {k: v for k, v in self.mquery_tables.items() if v.is_fact}

        # Also process mapping-only tables
        mapping_only = self.config.get('mapping_only_tables', {})

        for table_key in sorted(set(list(measure_groups.keys()) + list(mapping_only.keys()))):
            if table_key == '__unassigned__':
                self.stats[table_key] = {
                    'total': len(measure_groups.get(table_key, [])),
                    'translated': 0,
                    'untranslatable': len(measure_groups.get(table_key, [])),
                }
                continue

            measures = measure_groups.get(table_key, [])
            if not measures:
                continue

            table_info = self.mquery_tables.get(table_key)
            if table_info is None and table_key in mapping_only:
                # Create a synthetic TableInfo for mapping-only tables
                mot = mapping_only[table_key]
                table_info = TableInfo(
                    table_name=table_key,
                    source_table=mot.get('source_table', ''),
                    aggregate_columns=[],
                    group_by_columns=mot.get('group_by_columns', []),
                    calculated_columns=[],
                    is_fact=True,
                    full_sql='',
                )

            if table_info is None:
                self.stats[table_key] = {
                    'total': len(measures),
                    'translated': 0,
                    'untranslatable': len(measures),
                    'skipped': True,
                    'skip_reason': 'Not found in MQuery tables',
                }
                continue

            spec = self._process_table(table_key, table_info, measures)
            if spec:
                self.all_specs[table_key] = spec
                self.stats[table_key] = {
                    'total': len(measures),
                    'translated': len(spec.measures),
                    'untranslatable': len(spec.untranslatable),
                    'base': spec.base_measure_count,
                    'dax': spec.dax_measure_count,
                    'switch': spec.switch_measure_count,
                    'artifacts': sum(
                        1 for m in spec.untranslatable
                        if any(k in m.skip_reason for k in _ARTIFACT_SKIP_KEYWORDS)
                    ),
                }

        return self.all_specs

    def get_results(self) -> dict:
        """Return pipeline results as a serializable dict."""
        results = {
            'specs': {},
            'stats': self.stats,
            'cross_table_count': len(self.cross_table_measures),
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
        return results

    def _merge_relationship_enrichments(self):
        """Merge auto-detected relationship joins into enrichment joins."""
        def _base_table_name(source: str) -> str:
            from_hits = re.findall(r'\bFROM\s+([\w.`]+)', source, re.IGNORECASE)
            tbl = from_hits[-1].strip('`') if from_hits else source.strip()
            tbl = tbl.rsplit('.', 1)[-1]
            tbl = tbl.rsplit('__', 1)[-1]
            return tbl.lower()

        for tbl_key, auto_joins in self.relationships_enrichment.items():
            existing = self._enrichment_joins.setdefault(tbl_key, [])
            existing_aliases = {j['name'] for j in existing}
            existing_base_tables = {
                _base_table_name(j['source'])
                for j in existing if j.get('source')
            }
            for j in auto_joins:
                if j['name'] in existing_aliases:
                    continue
                j_base = _base_table_name(j.get('source', ''))
                if j_base in existing_base_tables:
                    continue
                existing.append(j)

    def _process_table(
        self,
        table_key: str,
        table_info: TableInfo,
        measures: list[dict],
    ) -> MetricViewSpec | None:
        """Process a single fact table → MetricViewSpec."""
        # Detect joins
        dim_joins = self.join_detector.detect(
            table_key, measures, table_info,
            inner_dim_joins=self.inner_dim_joins,
        )
        fact_joins = self.join_detector.detect_fact_joins(
            table_key, measures, table_info,
        )
        all_joins = dim_joins + fact_joins

        # Set fact joins on DAX translator for cross-table resolution
        self.dax_translator.set_fact_joins(all_joins)

        # Build base dimensions from GROUP BY columns
        dimensions = []
        for col in table_info.group_by_columns:
            dimensions.append({
                'name': col,
                'expr': f'source.{col}',
                'comment': col_to_readable(col),
            })

        # Add dimensions from dim joins
        dim_dims = self.join_detector.get_dim_dimensions(dim_joins, table_info)
        dimensions.extend(dim_dims)

        # Add dimensions from enrichment joins
        enrichment = self._enrichment_joins.get(table_key, [])
        for ej in enrichment:
            for col in ej.get('dim_columns', []):
                col_name = col if isinstance(col, str) else col.get('name', '')
                if col_name and not any(d['name'] == col_name for d in dimensions):
                    alias = ej['name']
                    dimensions.append({
                        'name': col_name,
                        'expr': f'{alias}.{col_name}',
                        'comment': f'{col_to_readable(col_name)} from {ej["name"]}',
                    })

        # Translate base measures (from MQuery SUM columns)
        base_measures = []
        for agg in table_info.aggregate_columns:
            col = agg['name']
            source_col = agg.get('source_col', col)
            expr = agg.get('expr')
            if expr:
                sql_expr = expr
            else:
                sql_expr = f'SUM(source.{source_col})'
            base_measures.append(TranslationResult(
                measure_name=col,
                original_name=col,
                sql_expr=sql_expr,
                is_translatable=True,
                skip_reason='',
                dax_expression='',
                confidence='high',
                category='base',
            ))

        # Translate DAX measures
        translated = []
        untranslatable = []
        for m in measures:
            dax = m.get('dax_expression', '')
            if not dax or dax.strip() in ('', 'Not available'):
                continue
            result = self.dax_translator.translate(m, table_key)
            # Skip if already covered as base measure
            if any(b.measure_name == result.measure_name for b in base_measures):
                continue
            if result.is_translatable:
                translated.append(result)
            else:
                untranslatable.append(result)

        # SWITCH decomposition measures
        switch_measures = self._process_switch_decompositions(
            table_key, measures, table_info)

        all_measures = base_measures + translated + switch_measures

        # Build source SQL (from scan data enrichment)
        source_table = table_info.source_table
        source_sql = ''
        source_filter = ''

        # Scan data enrichment: use native SQL + M transforms as inline source
        scan_info = self.scan_data.get(table_key)
        if scan_info:
            native_sql = scan_info.native_sql
            # Resolve PBI parameters
            native_sql = self.pbi_parameter_resolver.resolve(native_sql)
            # Apply Spark SQL compatibility
            native_sql = spark_sql_compat(native_sql)
            # Fold M transforms
            if scan_info.m_steps:
                native_sql = self.m_transform_folder.fold(
                    native_sql, scan_info.m_steps, scan_info.pbi_columns)
            # Post-process
            source_sql = self.sql_post_processor.process(native_sql)
        elif table_info.static_filters:
            source_filter = ' AND '.join(table_info.static_filters)

        # Build view name
        view_name = f'{table_key.lower()}_uc_metric_view'

        # Build comment
        comment_override = self.metadata_gen.get_comment_override(table_key)
        comment_lines = [
            comment_override or f'UC Metric View for {table_key}',
            f'Generated: {datetime.now():%Y-%m-%d}',
            f'Measures: {len(all_measures)} translated, {len(untranslatable)} untranslatable',
        ]

        # Filter consistency warnings
        filter_warnings = self._validate_filter_consistency(all_measures)
        self._filter_warnings.extend(filter_warnings)

        # Remove internal join markers from YAML-safe joins
        clean_joins = [j for j in all_joins if not j.get('_union_mode') and not j.get('_source_embed')]

        return MetricViewSpec(
            fact_table_key=table_key,
            source_table=source_table,
            view_name=view_name,
            comment='\n'.join(comment_lines),
            joins=clean_joins,
            dimensions=dimensions,
            measures=all_measures,
            untranslatable=untranslatable,
            base_measure_count=len(base_measures),
            dax_measure_count=len(translated),
            switch_measure_count=len(switch_measures),
            source_filter=source_filter,
            source_sql=source_sql,
        )

    def _process_switch_decompositions(
        self,
        table_key: str,
        measures: list[dict],
        table_info: TableInfo,
    ) -> list[TranslationResult]:
        """Process SWITCH decomposition config for this table."""
        switch_config = self.config.get('switch_decompositions', {})
        if table_key not in switch_config:
            return []

        results = []
        for parent_name, branches in switch_config[table_key].items():
            for branch_name, branch_config in branches.items():
                snake = to_snake_case(branch_name)
                sql_expr = branch_config.get('sql_expr', '')
                if sql_expr:
                    results.append(TranslationResult(
                        measure_name=snake,
                        original_name=branch_name,
                        sql_expr=sql_expr,
                        is_translatable=True,
                        skip_reason='',
                        dax_expression=f'SWITCH branch of [{parent_name}]',
                        confidence='high',
                        category='switch_decomposition',
                    ))
        return results

    @staticmethod
    def _validate_filter_consistency(measures: list[TranslationResult]) -> list[str]:
        """Check DIVIDE measures for mismatched filter sets."""
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
                    f'{m.measure_name}: numerator/denominator have different FILTER clauses'
                )
        return warnings

    def emit_all_yaml(self, catalog: str = 'main', schema: str = 'default') -> dict[str, str]:
        """Emit YAML for all specs. Returns dict[table_key → yaml_string]."""
        result = {}
        for table_key, spec in self.all_specs.items():
            enrichment = self._enrichment_joins.get(table_key, [])
            dim_excl = self.metadata_gen.get_dimension_exclusions(table_key)
            dim_meta = self.config.get('dimension_metadata', {}).get(table_key, {})
            meas_meta = self.config.get('measure_metadata', {}).get(table_key, {})
            comment_override = self.metadata_gen.get_comment_override(table_key)
            dim_order = self.metadata_gen.get_dimension_order(table_key)
            result[table_key] = emit_yaml(
                spec,
                catalog=catalog,
                schema=schema,
                metadata_gen=self.metadata_gen,
                enrichment_joins=enrichment,
                dimension_exclusions=dim_excl,
                dimension_metadata=dim_meta,
                measure_metadata=meas_meta,
                comment_override=comment_override,
                dimension_order=dim_order,
            )
        return result

    def emit_all_sql(self, catalog: str = 'main', schema: str = 'default') -> dict[str, str]:
        """Emit deploy SQL for all specs. Returns dict[table_key → sql_string]."""
        result = {}
        for table_key, spec in self.all_specs.items():
            result[table_key] = emit_deploy_sql(spec, catalog=catalog, schema=schema)
        return result
