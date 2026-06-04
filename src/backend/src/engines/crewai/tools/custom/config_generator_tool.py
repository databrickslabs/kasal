"""Config Generator Tool for CrewAI — auto-propose pipeline_config.json."""
import json
import logging
import re
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)


class ConfigGeneratorSchema(BaseModel):
    """Input schema for ConfigGeneratorTool."""
    workspace_id: Optional[str] = Field(
        None, description="PBI workspace ID — used to auto-load metadata from Tool 79 cache")
    dataset_id: Optional[str] = Field(
        None, description="PBI dataset ID — used to auto-load metadata from Tool 79 cache")
    measures_json: Optional[str] = Field(
        None, description="JSON string of measure_table_mapping (from Tool 73/79). If not provided, reads from Tool 79 cache.")
    mquery_json: Optional[str] = Field(
        None, description="JSON string of mquery_transpilation (from Tool 74)")
    relationships_json: Optional[str] = Field(
        None, description="JSON string of pbi_relationships (from Tool 75)")
    scan_data_json: Optional[str] = Field(
        None, description="JSON string of scan data (from Tool 86 API mode)")
    catalog: Optional[str] = Field(None, description="Target UC catalog for mapping_only_tables")
    schema_name: Optional[str] = Field(None, description="Target UC schema")


class ConfigGeneratorTool(BaseTool):
    """Auto-propose pipeline_config.json from PBI extraction output."""
    name: str = "Config Generator"
    description: str = (
        "Auto-propose a pipeline_config.json from PBI extraction output. "
        "Takes the same JSON inputs as Tool 86 (measures, MQuery, relationships, scan data) "
        "and produces a proposed config with join_key_map, enrichment_joins, column_overrides, "
        "mapping_only_tables, switch_decompositions (skeleton), measure_resolutions, "
        "parameter_defaults, and filter_sets. Also runs gap analysis showing what to fix next. "
        "Input: measures_json + mquery_json + relationships_json (+ optional scan_data_json). "
        "Output: proposed config JSON + gap analysis + confidence scores per key."
    )
    args_schema: Type[BaseModel] = ConfigGeneratorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        config_keys = ('workspace_id', 'dataset_id', 'measures_json', 'mquery_json',
                       'relationships_json', 'scan_data_json', 'catalog', 'schema_name')
        default_config = {}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _run(self, **kwargs: Any) -> str:
        def _get(key):
            val = kwargs.get(key)
            if val is not None:
                return val
            return self._default_config.get(key)

        measures_raw = _get('measures_json') or '[]'
        mquery_raw = _get('mquery_json') or '[]'
        relationships_raw = _get('relationships_json')
        scan_raw = _get('scan_data_json')
        catalog = _get('catalog') or 'main'
        schema = _get('schema_name') or 'default'
        workspace_id = _get('workspace_id')
        dataset_id = _get('dataset_id')

        try:
            # Lazy imports (same pattern as Tool 86)
            from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
            from src.engines.crewai.tools.custom.metric_view_utils.relationships_loader import RelationshipsLoader
            from src.engines.crewai.tools.custom.metric_view_utils.scan_data_parser import ScanDataParser
            from src.engines.crewai.tools.custom.metric_view_utils.pipeline import MetricViewPipeline
            from src.engines.crewai.tools.custom.metric_view_utils.utils import to_snake_case

            # Auto-load from Tool 79 cache if measures_json not provided
            if measures_raw == '[]' and workspace_id and dataset_id:
                logger.info(f"[ConfigGenerator] No measures_json provided, trying Tool 79 cache for workspace={workspace_id}, dataset={dataset_id}")
                try:
                    import asyncio
                    from src.services.powerbi_semantic_model_cache_service import PowerBISemanticModelCacheService
                    from src.db.session import async_session_factory
                    from src.utils.user_context import UserContext

                    group_id = (
                        (getattr(self, 'trace_context', None) or {})
                        .get('group_context', {}).get('primary_group_id')
                    ) or getattr(UserContext, '_group_id', None) or 'default'
                    logger.info(f"[ConfigGenerator] Using group_id={group_id} for cache lookup")

                    async def _load_cache():
                        async with async_session_factory() as session:
                            svc = PowerBISemanticModelCacheService(session)
                            return await svc.get_cached_metadata(
                                group_id=group_id,
                                dataset_id=dataset_id,
                                workspace_id=workspace_id,
                                any_report_id=True,
                            )

                    from src.engines.crewai.tools.custom.metric_view_utils.utils import run_async
                    cached = run_async(_load_cache())

                    if cached:
                        logger.info(f"[ConfigGenerator] Loaded metadata from Tool 79 cache: {len(str(cached))} chars")
                        # Extract measures from cached metadata
                        cached_measures = cached.get('measures', [])
                        cached_tables = cached.get('tables', [])
                        cached_relationships = cached.get('relationships', [])

                        if cached_measures:
                            # Convert to measure_table_mapping format
                            measures_raw = json.dumps([{
                                'measure_name': m.get('name', ''),
                                'dax_expression': m.get('expression', m.get('dax_expression', '')),
                                'table_name': m.get('table', m.get('table_name', '')),
                                'proposed_allocation': m.get('table', m.get('table_name', '')),
                                'description': m.get('description', ''),
                                'is_hidden': m.get('isHidden', False),
                            } for m in cached_measures])
                            logger.info(f"[ConfigGenerator] Extracted {len(cached_measures)} measures from cache")

                        if cached_relationships:
                            relationships_raw = json.dumps(cached_relationships)
                            logger.info(f"[ConfigGenerator] Extracted {len(cached_relationships)} relationships from cache")
                    else:
                        logger.warning("[ConfigGenerator] No cached metadata found for this workspace/dataset")
                except Exception as e:
                    logger.warning(f"[ConfigGenerator] Cache lookup failed: {e}")

            # Parse inputs
            measures = json.loads(measures_raw) if isinstance(measures_raw, str) else measures_raw
            mquery_entries = json.loads(mquery_raw) if isinstance(mquery_raw, str) else mquery_raw

            mquery_tables = MQueryParser().parse_json(mquery_entries)
            fact_tables = {k for k, v in mquery_tables.items() if v.is_fact}

            # Relationships
            enrichment = {}
            rel_loader = RelationshipsLoader()
            if relationships_raw:
                rels = json.loads(relationships_raw) if isinstance(relationships_raw, str) else relationships_raw
                enrichment = rel_loader.load(rels, mquery_tables, fact_tables)

            # Scan data
            scan_data = {}
            scan_parser = ScanDataParser()
            if scan_raw:
                try:
                    scan_obj = json.loads(scan_raw) if isinstance(scan_raw, str) else scan_raw
                    scan_data = scan_parser.parse(scan_obj)
                except Exception as e:
                    logger.warning(f"Failed to parse scan data: {e}")

            # === Propose config keys ===
            config = {}
            confidence = {}

            # 1. join_key_map
            join_key_map = {}
            for fact_key, joins in enrichment.items():
                for j in joins:
                    alias = j.get('name', '')
                    join_on = j.get('join_on', j.get('on', ''))
                    # Parse: source.FK = alias.PK
                    on_match = re.search(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', join_on)
                    if on_match:
                        dim_alias = on_match.group(3)
                        dim_key = on_match.group(4)
                        dim_name = alias.replace('dim_', '').capitalize()
                        # Get dim columns from mquery
                        dim_table = None
                        for tname, tinfo in mquery_tables.items():
                            if tname.lower().replace(' ', '_') == alias.lower() or alias in tname.lower():
                                dim_table = tinfo
                                break
                        dim_cols = dim_table.group_by_columns if dim_table else []
                        if dim_name not in join_key_map:
                            join_key_map[dim_name] = {
                                'alias': alias,
                                'join_key': dim_key,
                                'dim_columns': dim_cols[:10],  # limit
                            }
            config['join_key_map'] = join_key_map
            confidence['join_key_map'] = 'high' if join_key_map else 'low'

            # 2. enrichment_joins (pass-through)
            config['enrichment_joins'] = {k: v for k, v in enrichment.items() if v}
            confidence['enrichment_joins'] = 'high'

            # 3. column_overrides
            column_overrides = {}
            for m in measures:
                dax = m.get('dax_expression', '')
                if not dax or dax == 'Not available':
                    continue
                table = m.get('proposed_allocation', '')
                if not table or table not in mquery_tables:
                    continue
                # Extract Table[Col] references
                for ref_match in re.finditer(r'(\w+)\[(\w+)\]', dax):
                    dax_col = ref_match.group(2)
                    snake = to_snake_case(dax_col)
                    # Check if this column exists in MQuery
                    tinfo = mquery_tables[table]
                    agg_names = {a['name'] for a in tinfo.aggregate_columns}
                    grp_names = set(tinfo.group_by_columns)
                    all_cols = agg_names | grp_names
                    if snake not in all_cols and dax_col.lower() not in {c.lower() for c in all_cols}:
                        # Column name differs
                        if table not in column_overrides:
                            column_overrides[table] = {}
                        column_overrides[table][dax_col] = snake
            config['column_overrides'] = column_overrides
            confidence['column_overrides'] = 'medium'

            # 4. mapping_only_tables
            mapping_tables = {m.get('proposed_allocation', '') for m in measures} - {'', '__unassigned__'}
            mquery_table_names = set(mquery_tables.keys())
            mapping_only = {}
            for t in sorted(mapping_tables - mquery_table_names):
                mapping_only[t] = {
                    'source_table': f'{catalog}.{schema}.{t.lower()}',
                    'dimensions': [],
                }
            config['mapping_only_tables'] = mapping_only
            confidence['mapping_only_tables'] = 'high'

            # 5. switch_decompositions (skeleton)
            switch_decomps = {}
            for m in measures:
                dax = m.get('dax_expression', '')
                if 'SELECTEDVALUE' in dax.upper() and 'SWITCH' in dax.upper():
                    table = m.get('proposed_allocation', '__unassigned__')
                    name = to_snake_case(m.get('measure_name', ''))
                    if table not in switch_decomps:
                        switch_decomps[table] = []
                    # Extract SWITCH branches (simplified)
                    branches = re.findall(r'["\']([^"\']+)["\']\s*,\s*(\[[^\]]+\]|[^,\n]+)', dax)
                    if branches:
                        for branch_name, branch_expr in branches[:5]:  # limit per measure
                            switch_decomps[table].append({
                                'name': to_snake_case(branch_name),
                                'raw_expr': 'TODO: human fills SQL expression',
                                'comment': f'SWITCH branch: {branch_name} → {branch_expr[:80]}',
                            })
                    else:
                        switch_decomps[table].append({
                            'name': name,
                            'raw_expr': 'TODO: human fills SQL expression',
                            'comment': 'SELECTEDVALUE+SWITCH measure (parse branches manually)',
                        })
            config['switch_decompositions'] = switch_decomps
            confidence['switch_decompositions'] = 'low'  # skeletons only

            # 6. parameter_defaults
            param_defaults = {}
            for tname, tinfo in mquery_tables.items():
                sql = tinfo.full_sql or ''
                for pm in re.finditer(r'\$\{(\w+)\}', sql):
                    param_defaults[pm.group(1)] = 'TODO'
                for pm in re.finditer(r'#"(\w+)"', sql):
                    param_defaults[pm.group(1)] = 'TODO'
            config['parameter_defaults'] = param_defaults
            confidence['parameter_defaults'] = 'high' if param_defaults else 'low'

            # 7. measure_resolutions (from first-pass pipeline)
            measure_resolutions = {}
            pipeline = None
            try:
                pipeline = MetricViewPipeline(
                    mapping=measures, mquery_tables=mquery_tables,
                    config=config, relationships_enrichment=enrichment,
                    scan_data=scan_data, unflatten_tables=bool(scan_data),
                )
                pipeline.run()

                # Collect "Cannot resolve" errors
                for spec in pipeline.all_specs.values():
                    for m in spec.untranslatable:
                        ref_match = re.search(r'Cannot resolve \[([^\]]+)\]', m.skip_reason)
                        if ref_match:
                            ref = ref_match.group(1)
                            # Look up in all measures
                            for candidate in measures:
                                if candidate.get('measure_name', '') == ref:
                                    measure_resolutions[f'[{ref}]'] = to_snake_case(ref)
                                    break
            except Exception as e:
                logger.warning(f"First-pass pipeline failed: {e}")
            config['measure_resolutions'] = measure_resolutions
            confidence['measure_resolutions'] = 'medium'

            # 8. filter_sets
            filter_sets = {}
            for table_decomps in switch_decomps.values():
                for d in table_decomps:
                    comment = d.get('comment', '')
                    # Extract filter values from comment
                    in_match = re.search(r"IN\s*\(([^)]+)\)", comment)
                    if in_match:
                        values = [v.strip().strip("'\"") for v in in_match.group(1).split(',')]
                        fs_key = f'FS_{len(filter_sets) + 1}'
                        filter_sets[fs_key] = values
            config['filter_sets'] = filter_sets
            confidence['filter_sets'] = 'low'

            # === Gap analysis ===
            gap_summary = {}
            try:
                from collections import Counter
                if pipeline is not None:
                    categories = Counter()
                    for spec in pipeline.all_specs.values():
                        for m in spec.untranslatable:
                            reason = m.skip_reason.split('(')[0].strip()
                            categories[reason] += 1
                    gap_summary = {
                        'total_untranslatable': sum(categories.values()),
                        'top_categories': dict(categories.most_common(10)),
                    }
            except Exception:
                pass

            # === Output ===
            output = {
                'proposed_config': config,
                'confidence': confidence,
                'gap_analysis': gap_summary,
                'summary': {
                    'keys_proposed': sum(1 for v in config.values() if v),
                    'keys_total': len(config),
                    'todo_count': sum(
                        1 for v in config.values()
                        if isinstance(v, dict) and any('TODO' in str(vv) for vv in v.values())
                    ),
                },
            }
            return json.dumps(output, indent=2, default=str)

        except Exception as e:
            logger.exception("Config generator failed")
            return json.dumps({"error": str(e)})
