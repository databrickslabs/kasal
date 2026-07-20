"""UC Metric View Generator Tool for CrewAI — full pipeline."""
import json
import logging
import os
import re
import urllib.parse
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)


from src.engines.crewai.tools.custom.metric_view_utils.utils import run_async as _run_async


class UCMetricViewGeneratorSchema(BaseModel):
    """Input schema for UCMetricViewGeneratorTool."""
    measures_json: Optional[str] = Field(
        None, description="JSON string of measure_table_mapping (from Measure Conversion Pipeline)")
    mquery_json: Optional[str] = Field(
        None, description="JSON string of mquery_transpilation (from MQuery Conversion Pipeline)")
    relationships_json: Optional[str] = Field(
        None, description="JSON string of PBI relationships (from Relationships Tool)")
    scan_data_json: Optional[str] = Field(
        None, description="JSON string of PBI scan data (optional, for enrichment)")
    config_json: Optional[str] = Field(
        None, description="JSON pipeline config overrides (join_key_map, fact_join_map, etc.)")
    catalog: Optional[str] = Field(None, description="Target UC catalog name")
    schema_name: Optional[str] = Field(None, description="Target UC schema name")
    inner_dim_joins: bool = Field(False, description="Use INNER JOIN for dimensions")
    unflatten_tables: bool = Field(False, description="Unflatten __-separated table names")
    use_llm_fallback: bool = Field(False, description="Enable LLM fallback for unmatched DAX patterns (opt-in)")
    translation_mode: Optional[str] = Field(None, description="'llm_first' (default when LLM enabled — skill-corpus-driven) or 'regex_first' (regex patterns primary)")
    llm_model: Optional[str] = Field(None, description="LLM model for fallback (default: databricks-claude-sonnet-4-5)")
    llm_workspace_url: Optional[str] = Field(None, description="Databricks workspace URL for LLM endpoint")
    llm_token: Optional[str] = Field(None, description="Databricks token for LLM endpoint")

    # ===== PBI API EXTRACTION (optional — alternative to providing pre-extracted JSON) =====
    workspace_id: Optional[str] = Field(None, description="Power BI workspace/group ID. When provided with credentials, extracts data from PBI API instead of requiring pre-extracted JSON.")
    dataset_id: Optional[str] = Field(None, description="Power BI dataset/semantic model ID")
    tenant_id: Optional[str] = Field(None, description="Azure AD tenant ID for Service Principal auth")
    client_id: Optional[str] = Field(None, description="Azure AD application/client ID")
    client_secret: Optional[str] = Field(None, description="Client secret for Service Principal auth", repr=False)
    username: Optional[str] = Field(None, description="Service account username (alternative to SP)")
    password: Optional[str] = Field(None, description="Service account password", repr=False)
    auth_method: Optional[str] = Field(None, description="Auth method: 'service_principal', 'service_account', or auto-detect")
    access_token: Optional[str] = Field(None, description="Pre-obtained OAuth access token (alternative to SP/SA)", repr=False)
    pbi_api_base_url: Optional[str] = Field(None, description="Power BI API base URL. Defaults to commercial cloud. Use 'https://api.powerbigov.us/v1.0/myorg' for GCC, 'https://api.powerbi.cn/v1.0/myorg' for China cloud.")


class UCMetricViewGeneratorTool(BaseTool):
    """Generate UC Metric View YAML + deploy SQL from PBI measures and MQuery data."""
    name: str = "UC Metric View Generator"
    description: str = (
        "Full pipeline: generates UC Metric View YAML + deploy SQL per fact table. "
        "TWO MODES: (1) API mode — provide workspace_id + dataset_id + PBI credentials, "
        "and the tool extracts measures, MQuery, and relationships from the PBI API automatically. "
        "(2) JSON mode — provide pre-extracted measures_json + mquery_json from upstream tools. "
        "Combines MQuery parsing, DAX translation (14+ patterns + LLM fallback), "
        "Kahn's dependency graph, join detection, and YAML/SQL emission."
    )
    args_schema: Type[BaseModel] = UCMetricViewGeneratorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    @staticmethod
    def _mask_secret(value: str | None) -> str:
        """Mask a secret value for logging."""
        if not value:
            return 'none'
        if len(value) <= 8:
            return '***'
        return f'{value[:4]}...{value[-4:]}'

    def __init__(self, **kwargs: Any) -> None:
        config_keys = ('measures_json', 'mquery_json', 'relationships_json',
                       'scan_data_json', 'config_json', 'catalog', 'schema_name',
                       'inner_dim_joins', 'unflatten_tables',
                       'use_llm_fallback', 'translation_mode', 'llm_model', 'llm_workspace_url', 'llm_token',
                       'workspace_id', 'dataset_id', 'tenant_id', 'client_id',
                       'client_secret', 'username', 'password', 'auth_method',
                       'access_token', 'pbi_api_base_url')
        default_config = {}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _run(self, **kwargs: Any) -> str:
        from src.engines.crewai.tools.custom.metric_view_utils.pipeline import MetricViewPipeline
        from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
        from src.engines.crewai.tools.custom.metric_view_utils.relationships_loader import RelationshipsLoader
        from src.engines.crewai.tools.custom.metric_view_utils.scan_data_parser import ScanDataParser

        def _get(key):
            return kwargs.get(key) or self._default_config.get(key)

        # JSON inputs (measures/mquery/config/relationships/scan) are injected into
        # _default_config by the flow handoff. A capable agent, told to "call the
        # tool with ALL inputs", often passes its OWN placeholder for these (e.g.
        # a description string or "<measures_json>") which is truthy and overrides
        # the good injected value via _get's `or` — then json.loads() fails at
        # char 0. Guard: for JSON-shaped fields, if the kwarg isn't valid JSON but
        # the injected default IS present, use the injected default.
        _JSON_KEYS = ('measures_json', 'mquery_json', 'config_json',
                      'relationships_json', 'scan_data_json')

        def _get_json(key):
            kw_val = kwargs.get(key)
            default_val = self._default_config.get(key)

            def _looks_like_json(v):
                if not isinstance(v, (str, list, dict)):
                    return False
                if isinstance(v, (list, dict)):
                    return True
                s = v.strip()
                return s.startswith('{') or s.startswith('[')

            # Prefer a valid kwarg; else the injected default; else the kwarg as-is.
            if kw_val is not None and _looks_like_json(kw_val):
                return kw_val
            if default_val is not None and _looks_like_json(default_val):
                if kw_val is not None and not _looks_like_json(kw_val):
                    logger.warning(
                        f"[UCMV] agent passed non-JSON {key}={str(kw_val)[:40]!r}; "
                        f"using flow-injected value instead"
                    )
                return default_val
            return kw_val or default_val

        measures_raw = _get_json('measures_json') or '[]'
        mquery_raw = _get_json('mquery_json') or '[]'
        relationships_raw = _get_json('relationships_json')
        scan_raw = _get_json('scan_data_json')
        config_raw = _get_json('config_json') or '{}'
        catalog = _get('catalog') or 'main'
        schema = _get('schema_name') or 'default'
        inner_joins = _get('inner_dim_joins') or False
        unflatten = _get('unflatten_tables') or False

        # Check if API extraction mode (PBI credentials provided)
        workspace_id = _get('workspace_id')
        dataset_id = _get('dataset_id')

        if workspace_id and dataset_id:
            pbi_api_base_url = _get('pbi_api_base_url') or ''
            valid, err_msg = self._validate_pbi_inputs(workspace_id, dataset_id, pbi_api_base_url)
            if not valid:
                return json.dumps({"error": f"PBI input validation failed: {err_msg}"})
            logger.info(f"[UCMV] API extraction mode: workspace={workspace_id}, dataset={dataset_id}, token={self._mask_secret(_get('access_token'))}")
            try:
                extracted = self._extract_from_pbi_api(
                    workspace_id=workspace_id,
                    dataset_id=dataset_id,
                    tenant_id=_get('tenant_id') or '',
                    client_id=_get('client_id') or '',
                    client_secret=_get('client_secret') or '',
                    username=_get('username') or '',
                    password=_get('password') or '',
                    auth_method=_get('auth_method'),
                    access_token=_get('access_token') or '',
                    pbi_api_base_url=_get('pbi_api_base_url') or '',
                )
                # Use extracted data (override only when manually provided JSON is empty/default)
                if extracted.get('measures') and measures_raw == '[]':
                    measures_raw = json.dumps(extracted['measures'])
                if extracted.get('mquery') and mquery_raw == '[]':
                    mquery_raw = json.dumps(extracted['mquery'])
                if extracted.get('relationships') and not relationships_raw:
                    relationships_raw = json.dumps(extracted['relationships'])
                if extracted.get('scan_data') and not scan_raw:
                    scan_raw = json.dumps(extracted['scan_data'])
            except Exception as e:
                logger.error(f"[UCMV] API extraction failed: {e}")
                return json.dumps({"error": f"PBI API extraction failed: {e}"})

        # Build LLM config if fallback enabled
        llm_config = None
        use_llm = _get('use_llm_fallback') or False
        if use_llm:
            llm_config = {
                'use_llm_fallback': True,
                'llm_model': _get('llm_model') or 'databricks-claude-sonnet-4-5',
                'llm_workspace_url': _get('llm_workspace_url') or os.environ.get('DATABRICKS_HOST', ''),
                'llm_token': _get('llm_token') or os.environ.get('DATABRICKS_TOKEN', ''),
                # LLM-first translation (skill-corpus driven) is the default; the
                # regex patterns become a trivial fast-path. 'regex_first' restores
                # the prior regex-primary behaviour.
                'translation_mode': _get('translation_mode') or 'llm_first',
            }

        def _parse_json_input(raw, default):
            """Parse a JSON input; treat empty/blank as the default (never error)."""
            if not isinstance(raw, str):
                return raw if raw is not None else default
            s = raw.strip()
            if not s:
                return default
            return json.loads(s)

        try:
            measures = _parse_json_input(measures_raw, [])
            mquery_entries = _parse_json_input(mquery_raw, [])
            config = _parse_json_input(config_raw, {})
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON input: {e}"})

        # ── Raw Power Query M → SQL source recovery (opt-in) ────────────────
        # When a table's source is raw M (`let ... in ...`) with no embedded
        # native SQL, MQueryParser cannot extract a FROM clause → the table is
        # neither a fact nor has a source → 0 views. If LLM fallback is enabled,
        # rewrite those entries' transpiled_sql to a Spark SQL SELECT the parser
        # CAN read. Fail-open: entries the LLM can't translate are left as-is.
        if use_llm and isinstance(mquery_entries, list) and mquery_entries:
            try:
                from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import (
                    looks_like_raw_mquery,
                )
                raw_m_count = sum(
                    1 for e in mquery_entries
                    if isinstance(e, dict) and looks_like_raw_mquery(e.get('transpiled_sql') or '')
                )
                if raw_m_count:
                    from src.engines.crewai.tools.custom.metric_view_utils.mquery_llm_fallback import (
                        recover_sources_with_llm,
                    )
                    logger.info(f"[UCMV] {raw_m_count} raw M-Query table(s) detected; attempting M→SQL LLM recovery")
                    mquery_entries, _recovered = _run_async(recover_sources_with_llm(
                        mquery_entries,
                        model=(_get('llm_model') or 'databricks-claude-sonnet-4-5'),
                    ))
                    logger.info(f"[UCMV] M→SQL recovery: {_recovered}/{raw_m_count} table(s) recovered")
            except Exception as _m_err:
                logger.warning(f"[UCMV] M→SQL LLM recovery skipped (non-fatal): {_m_err}")

        # Parse MQuery
        parser = MQueryParser()
        mquery_tables = parser.parse_json(mquery_entries)

        # Parse relationships
        relationships_enrichment = {}
        m2n_relationships: list[dict] = []
        inactive_relationships: list[dict] = []
        if relationships_raw:
            try:
                rel_data = json.loads(relationships_raw) if isinstance(relationships_raw, str) else relationships_raw
                loader = RelationshipsLoader()
                fact_keys = {k for k, v in mquery_tables.items() if v.is_fact}
                relationships_enrichment = loader.load(rel_data, mquery_tables, fact_keys)
                m2n_relationships = loader.get_skipped_m2n()
                inactive_relationships = loader.get_inactive_relationships()
            except Exception as e:
                logger.warning(f"Failed to parse relationships: {e}")

        # Parse scan data
        scan_data = {}
        scan_parser = ScanDataParser()
        if scan_raw:
            try:
                scan_obj = json.loads(scan_raw) if isinstance(scan_raw, str) else scan_raw
                scan_data = scan_parser.parse(scan_obj)
            except Exception as e:
                logger.warning(f"Failed to parse scan data: {e}")

        # Run pipeline
        pipeline = MetricViewPipeline(
            mapping=measures,
            mquery_tables=mquery_tables,
            config=config,
            inner_dim_joins=inner_joins,
            scan_data=scan_data,
            unflatten_tables=unflatten or bool(scan_data),
            relationships_enrichment=relationships_enrichment,
            llm_config=llm_config,
            inactive_relationships=inactive_relationships or None,
            m2n_relationships=m2n_relationships or None,
            refresh_policy_tables=scan_parser.get_refresh_policy_tables() or None,
            no_summarize_columns=scan_parser.get_no_summarize_columns() or None,
            rls_tables=scan_parser.get_rls_tables() or None,
        )
        pipeline.run()

        # Emit YAML + SQL
        yaml_output = pipeline.emit_all_yaml(catalog=catalog, schema=schema)
        sql_output = pipeline.emit_all_sql(catalog=catalog, schema=schema)
        results = pipeline.get_results()

        # Run validation (optional — compares DAX structure vs generated SQL)
        validation_results = {}
        try:
            from src.engines.crewai.tools.custom.metric_view_validation_utils.pipeline import (
                MetricExpressionValidatorPipeline,
            )
            if measures_raw and measures_raw != '[]':
                mapping_for_val = json.loads(measures_raw) if isinstance(measures_raw, str) else measures_raw
                import tempfile  # NOTE: os is already imported at module level; importing it
                # here too would make `os` a function-local for all of _run() and break the
                # earlier os.environ.get(...) calls with UnboundLocalError.
                for table_key, yml in yaml_output.items():
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as yf:
                        yf.write(yml)
                        yf_path = yf.name
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as mf:
                        json.dump(mapping_for_val, mf)
                        mf_path = mf.name
                    try:
                        validator = MetricExpressionValidatorPipeline(
                            table_mappings={table_key: 'source'})
                        vr = validator.run(
                            metrics_view_yaml_path=yf_path,
                            table_mapping_json_path=mf_path)
                        evaluated = vr.get('evaluated', [])
                        if evaluated:
                            valid = sum(1 for m in evaluated
                                        if m.get('measure_eval_result', {}).get('status') == 'VALID')
                            validation_results[table_key] = {
                                'evaluated': len(evaluated),
                                'valid': valid,
                            }
                    finally:
                        os.unlink(yf_path)
                        os.unlink(mf_path)
        except ImportError:
            pass  # Validation package not available
        except Exception as e:
            logger.warning(f"Validation failed: {e}")

        # Store measures with DAX for downstream validation
        _measures_for_validation = []
        if isinstance(measures, list):
            _measures_for_validation = measures

        # Resolved measure→DAX map, keyed by FACT TABLE (the YAML table key).
        # This is what the Quality Validator needs: each translated measure paired
        # with its ORIGINAL DAX, allocated to the correct fact table. Unlike the
        # raw `measures` list (keyed by PBI holder-table), this uses the pipeline's
        # own allocation so the validator can pair YAML measures ↔ DAX and run the
        # filter/aggregation comparison.
        resolved_measures_by_table = {}
        for table_key, spec in (results.get('specs', {}) or {}).items():
            rows = []
            for m in spec.get('measures', []):
                rows.append({
                    'measure_name': m.get('name', ''),
                    'original_name': m.get('original_name', ''),
                    'sql_expr': m.get('sql_expr', ''),
                    'dax_expression': m.get('dax_expression', ''),
                    'proposed_allocation': table_key,  # fact-table key, matches YAML
                    'table_name': table_key,
                })
            if rows:
                resolved_measures_by_table[table_key] = rows

        # ── Worst-case fallback: per-table tabular extract ─────────────────
        # Even when NO views generate (e.g. an all-raw-Power-Query-M model where
        # neither fact detection nor the M→SQL fallback could produce a UCMV),
        # the customer should still get the raw material they can act on: for
        # each PBI table, its M-Query source expression plus the measures/DAX
        # associated with it. Built from the tool's inputs, so it is populated
        # regardless of whether the transpilation pipeline succeeded.
        fallback_extract = self._build_fallback_extract(measures, mquery_entries)

        output = {
            'yaml': yaml_output,
            'sql': sql_output,
            'stats': results['stats'],
            'migration_report': results.get('migration_report', ''),
            'limitations': results.get('limitations', {}),
            'validation': validation_results,
            'measures_with_dax': _measures_for_validation,
            'resolved_measures_by_table': resolved_measures_by_table,
            'mquery_raw': mquery_entries if isinstance(mquery_entries, list) else [],
            # Always present; the UI shows it as a tabular reference and falls back
            # to it as the primary artifact when `yaml` is empty (0 views).
            'fallback_extract': fallback_extract,
            'views_generated': len(yaml_output) if isinstance(yaml_output, dict) else 0,
            'specs_summary': {
                k: {
                    'view_name': v.get('view_name'),
                    'measures': v.get('measures_count'),
                    'untranslatable': v.get('untranslatable_count'),
                }
                for k, v in results.get('specs', {}).items()
            },
        }
        output_json = json.dumps(output, indent=2)

        # Write to /tmp so the Validator tool can find it as a fallback
        # (in case flow injection into _default_config fails for any reason).
        try:
            import glob as _glob
            # Clean up old UCMV tmp files to avoid stale data
            for _old in _glob.glob('/tmp/ucmv_latest_*.json'):
                try:
                    os.unlink(_old)
                except OSError:
                    pass
            _tmp_path = f'/tmp/ucmv_latest_{os.getpid()}.json'
            with open(_tmp_path, 'w') as _f:
                _f.write(output_json)
            logger.info(f"[UCMVGenerator] Wrote output to {_tmp_path} for validator fallback")
        except Exception as _tmp_err:
            logger.debug(f"[UCMVGenerator] Could not write /tmp fallback: {_tmp_err}")

        # ── Durable raw DAX persistence (Lakebase / conversion_history) ──────
        try:
            _run_async(self._save_dax_to_conversion_history(
                raw_dax=self._build_raw_dax_extract(_measures_for_validation),
                yaml_output=yaml_output,
                sql_output=sql_output,
                workspace_id=_get('workspace_id'),
                dataset_id=_get('dataset_id'),
                catalog=catalog,
                schema=schema,
            ))
        except Exception as _hist_err:
            logger.warning(f"[UCMVGenerator] conversion_history persistence skipped: {_hist_err}")
        # ────────────────────────────────────────────────────────────────────

        return output_json

    # ------------------------------------------------------------------
    # Durable raw DAX persistence (conversion_history / Lakebase)
    # ------------------------------------------------------------------

    @staticmethod
    def _build_raw_dax_extract(measures: Any) -> list:
        """Collect the full, untruncated original DAX expression per measure.

        Mirrors the M-Query extract produced by the MQuery Conversion Pipeline:
        the complete raw source DAX is retained verbatim so it can be persisted
        and retrieved later, rather than only living in the transient result.
        """
        extract = []
        if not isinstance(measures, list):
            return extract
        for m in measures:
            if not isinstance(m, dict):
                continue
            extract.append({
                'measure_name': m.get('measure_name') or m.get('original_name') or '',
                'original_name': m.get('original_name') or m.get('measure_name') or '',
                'dax_expression': m.get('dax_expression') or '',
                'proposed_allocation': m.get('proposed_allocation') or '',
            })
        return extract

    @staticmethod
    def _build_fallback_extract(measures: Any, mquery_entries: Any) -> list:
        """Per-table tabular extract of M-Query source + associated measures/DAX.

        The worst-case safety net: when no UC Metric View can be generated (e.g.
        an all-raw-Power-Query-M model), the customer still gets the raw material
        organised per table — the M-Query source expression and every measure
        (with its DAX) allocated to that table. Rendered as a table in the UI.

        Groups by the table each measure is allocated to (proposed_allocation /
        table_name) and each M-Query entry's table_name. Tables that appear in
        either source are included, so a table with M-Query but no measures (and
        vice-versa) is still listed.
        """
        by_table: dict[str, dict] = {}

        def _slot(name: str) -> dict:
            key = name or '__unassigned__'
            if key not in by_table:
                by_table[key] = {'table_name': key, 'mquery': '', 'measures': []}
            return by_table[key]

        # M-Query source per table
        if isinstance(mquery_entries, list):
            for e in mquery_entries:
                if not isinstance(e, dict):
                    continue
                tname = e.get('table_name') or ''
                if not tname:
                    continue
                slot = _slot(tname)
                # Prefer a non-empty source; keep the first seen otherwise.
                src = e.get('transpiled_sql') or e.get('mquery_expression') or ''
                if src and not slot['mquery']:
                    slot['mquery'] = src

        # Measures + DAX per table
        if isinstance(measures, list):
            for m in measures:
                if not isinstance(m, dict):
                    continue
                alloc = (
                    m.get('proposed_allocation')
                    or m.get('table_name')
                    or m.get('table')
                    or '__unassigned__'
                )
                _slot(alloc)['measures'].append({
                    'measure_name': m.get('measure_name') or m.get('original_name') or '',
                    'dax_expression': m.get('dax_expression') or m.get('expression') or '',
                })

        # Stable, readable ordering: tables with measures first, then by name.
        rows = list(by_table.values())
        rows.sort(key=lambda r: (-len(r['measures']), r['table_name']))
        for r in rows:
            r['measure_count'] = len(r['measures'])
            r['has_mquery'] = bool(r['mquery'])
        return rows

    async def _save_dax_to_conversion_history(
        self,
        raw_dax: list,
        yaml_output: Any,
        sql_output: Any,
        workspace_id: Optional[str],
        dataset_id: Optional[str],
        catalog: Optional[str],
        schema: Optional[str],
    ) -> None:
        """Persist the full raw DAX extract to conversion_history (fail-open).

        Durable, queryable counterpart to the transient tool result: the
        untruncated source DAX lands in ``input_data.dax_raw`` and is retrievable
        afterwards via ``GET /conversion-history`` (filter by
        ``source_format=powerbi_dax`` / ``execution_id``) or
        ``GET /conversion-history/{id}``. Any failure here is non-fatal — it must
        never break the generation itself.
        """
        try:
            from src.engines.crewai.tools.tool_session_provider import ToolSessionProvider
            from src.schemas.conversion import ConversionHistoryCreate
            from src.utils.user_context import UserContext

            raw_dax_count = len(raw_dax)
            # In JSON/flow mode the tool does not re-extract, so raw_dax is empty
            # even though views ARE generated (measures arrive via config_json /
            # task context). Count the actual views produced so the diagnostic
            # reflects success instead of a misleading "0 measures".
            view_count = len(yaml_output) if isinstance(yaml_output, dict) else 0
            # measure_count reflects extracted DAX when present, else views built.
            measure_count = raw_dax_count or view_count
            history_data = ConversionHistoryCreate(
                execution_id=(getattr(self, "trace_context", None) or {}).get("job_id"),
                source_format="powerbi_dax",
                target_format="uc_metrics",
                input_data={
                    "workspace_id": workspace_id or "",
                    "dataset_id": dataset_id or "",
                    "dax_raw": raw_dax,
                },
                input_summary=(
                    f"DAX extract: {raw_dax_count} measure(s)"
                    + (f" from workspace {workspace_id}" if workspace_id else "")
                )[:500],
                output_data={
                    "yaml": yaml_output,
                    "sql": sql_output,
                    "catalog": catalog,
                    "schema": schema,
                },
                output_summary=(
                    f"Generated {view_count} UC metric view(s)"
                    + (f" from {raw_dax_count} extracted measure(s)" if raw_dax_count else " (JSON/flow mode)")
                )[:500],
                configuration={
                    "workspace_id": workspace_id,
                    "dataset_id": dataset_id,
                    "catalog": catalog,
                    "schema": schema,
                },
                status="success",
                measure_count=measure_count,
            )

            group_id = None
            try:
                group_context = UserContext.get_group_context()
                if group_context:
                    group_id = getattr(group_context, "primary_group_id", None)
            except Exception as _gc_err:
                logger.debug(f"[UCMVGenerator] Could not resolve group_id for history: {_gc_err}")

            async with ToolSessionProvider.conversion_repo() as repo:
                record = await repo.create(history_data.model_dump())
                if group_id:
                    record.group_id = group_id
                await repo.session.commit()
                logger.info(
                    f"[UCMVGenerator] Saved conversion_history record id={record.id} "
                    f"(source_format=powerbi_dax, views={view_count}, extracted_measures={raw_dax_count})"
                )
        except Exception as e:
            logger.warning(f"[UCMVGenerator] Failed to save conversion_history (non-fatal): {e}")

    # ------------------------------------------------------------------
    # PBI API input validation (SSRF prevention)
    # ------------------------------------------------------------------

    _UUID_PATTERN = re.compile(r'^[a-fA-F0-9\-]{8,50}$')
    _ALLOWED_PBI_DOMAINS = frozenset({
        'api.powerbi.com', 'api.powerbigov.us', 'api.powerbi.cn',
        'api.powerbi.de', 'api.microsoftcloud.de',
    })

    def _validate_pbi_inputs(self, workspace_id: str, dataset_id: str, pbi_api_base_url: str) -> tuple[bool, str]:
        """Validate PBI API inputs to prevent SSRF."""
        if not self._UUID_PATTERN.match(workspace_id):
            return False, f"Invalid workspace_id format: {workspace_id}"
        if not self._UUID_PATTERN.match(dataset_id):
            return False, f"Invalid dataset_id format: {dataset_id}"
        parsed = urllib.parse.urlparse(pbi_api_base_url or 'https://api.powerbi.com')
        if parsed.hostname not in self._ALLOWED_PBI_DOMAINS:
            return False, f"Untrusted PBI API domain: {parsed.hostname}"
        return True, ""

    # ------------------------------------------------------------------
    # PBI API extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _import_generate_config():
        """Import the standalone generate_config module (bundled alongside this tool)."""
        import sys as _sys
        this_dir = os.path.dirname(os.path.abspath(__file__))
        candidates = [this_dir]
        project_root = os.path.abspath(
            os.path.join(this_dir, "..", "..", "..", "..", "..", "..", "..")
        )
        candidates.append(os.path.join(project_root, "examples", "uc_metric_view_migration"))
        gen_config_dir = next(
            (c for c in candidates if os.path.isfile(os.path.join(c, "generate_config.py"))),
            None,
        )
        if gen_config_dir is None:
            raise ImportError(
                f"generate_config.py not found in any of: [{', '.join(candidates)}]"
            )
        if gen_config_dir not in _sys.path:
            _sys.path.insert(0, gen_config_dir)
        import generate_config  # noqa: E402
        return generate_config

    @staticmethod
    def _tmdl_tables_to_measures(admin_tables: dict) -> list:
        """Convert generate_config TMDL/admin table dict → UCMV measure shape."""
        measures = []
        for tbl_name, tbl_info in (admin_tables or {}).items():
            for m in tbl_info.get('measures', []):
                name = m.get('name', '')
                if not name:
                    continue
                measures.append({
                    'measure_name': name,
                    'original_name': name,
                    'dax_expression': m.get('expression', '') or '',
                    'proposed_allocation': tbl_name or '__unassigned__',
                    'table_refs': [],
                })
        return measures

    @staticmethod
    def _tmdl_tables_to_mquery(admin_tables: dict) -> list:
        """Convert generate_config TMDL table dict → UCMV mquery entry shape.

        UCMV expects [{table_name, transpiled_sql, validation_passed}].

        IMPORTANT: validation_passed MUST start with 'Yes'. MQueryParser.parse_json
        silently drops any entry whose validation_passed is not 'Yes...' unless the
        SQL contains both SUM( and GROUP BY. The earlier 'No' value meant every
        TMDL-recovered table was discarded → the fallback recovered rows but the
        parser produced zero tables → 0 fact tables → 0 views. The source here is
        the authoritative partition expression (embedded native SQL where the
        datasource is a SQL DB, otherwise raw M), so mark it accepted and let the
        parser extract what it can.
        """
        entries = []
        for tbl_name, tbl_info in (admin_tables or {}).items():
            src = (tbl_info.get('mquery_expression') or '').strip()
            if not src:
                continue
            entries.append({
                'table_name': tbl_name,
                'transpiled_sql': src,
                'validation_passed': 'Yes',
            })
        return entries

    def _extract_mquery_fallback(
        self, workspace_id, dataset_id, tenant_id, client_id,
        client_secret, username, password,
    ) -> list:
        """Recover MQuery/table-source when the Admin Scanner fails for a Service Account.

        Tier 1: Fabric TMDL with the SA (works if the workspace is Fabric-enabled).
        Tier 2: Fabric TMDL with a Service Principal (if client_secret provided).
        Reuses generate_config so the logic is shared with the config generator.
        """
        try:
            gen = self._import_generate_config()
        except Exception as e:
            logger.warning(f"[UCMV] Could not load generate_config for MQuery fallback: {e}")
            return []

        for label, sa_user, sa_pw, sp_secret in (
            ("SA", username, password, None),
            ("SP", None, None, client_secret),
        ):
            if label == "SA" and not (sa_user and sa_pw):
                continue
            if label == "SP" and not sp_secret:
                continue
            try:
                fabric_token = gen.get_fabric_token(
                    tenant_id, client_id, sp_secret,
                    username=sa_user, password=sa_pw,
                )
                tmdl_parts = gen.fetch_tmdl_parts(fabric_token, workspace_id, dataset_id)
                if tmdl_parts:
                    tables = gen.parse_tmdl_to_admin_tables(tmdl_parts, dataset_id=dataset_id)
                    entries = self._tmdl_tables_to_mquery(tables)
                    if entries:
                        logger.info(f"[UCMV] MQuery TMDL fallback ({label}) recovered {len(entries)} tables")
                        return entries
            except Exception as e:
                logger.warning(f"[UCMV] MQuery TMDL fallback ({label}) failed: {e}")
        return []

    def _extract_measures_fallback(
        self, workspace_id, dataset_id, tenant_id, client_id,
        client_secret, username, password,
    ) -> list:
        """Recover measure DAX when Execute Queries/XMLA fails for a Service Account.

        Tier 1: Fabric TMDL with the SA (works if the workspace is Fabric-enabled).
        Tier 2: Fabric TMDL with a Service Principal (if client_secret provided).
        Both reuse the standalone helpers in generate_config so the logic is
        shared with the Pipeline Config Generator.
        """
        try:
            gen = self._import_generate_config()
        except Exception as e:
            logger.warning(f"[UCMV] Could not load generate_config for fallback: {e}")
            return []

        # Tier 1: TMDL with whatever creds are present (SA preferred, else SP).
        for label, sa_user, sa_pw, sp_secret in (
            ("SA", username, password, None),
            ("SP", None, None, client_secret),
        ):
            if label == "SA" and not (sa_user and sa_pw):
                continue
            if label == "SP" and not sp_secret:
                continue
            try:
                fabric_token = gen.get_fabric_token(
                    tenant_id, client_id, sp_secret,
                    username=sa_user, password=sa_pw,
                )
                tmdl_parts = gen.fetch_tmdl_parts(fabric_token, workspace_id, dataset_id)
                if tmdl_parts:
                    tables = gen.parse_tmdl_to_admin_tables(tmdl_parts, dataset_id=dataset_id)
                    measures = self._tmdl_tables_to_measures(tables)
                    if measures:
                        logger.info(f"[UCMV] TMDL fallback ({label}) recovered {len(measures)} measures")
                        return measures
            except Exception as e:
                logger.warning(f"[UCMV] TMDL fallback ({label}) failed: {e}")
        return []

    def _extract_from_pbi_api(
        self,
        workspace_id: str,
        dataset_id: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        username: str,
        password: str,
        auth_method: Optional[str],
        access_token: str,
        pbi_api_base_url: str = '',
    ) -> dict:
        """Extract measures, MQuery, relationships, and scan data from PBI API.

        Returns dict with keys: measures, mquery, relationships, scan_data
        """
        auth_config = {
            'tenant_id': tenant_id,
            'client_id': client_id,
            'client_secret': client_secret,
            'username': username,
            'password': password,
            'auth_method': auth_method,
            'access_token': access_token,
        }

        result: dict = {}

        # Obtain an OAuth token (unless a pre-obtained one was supplied)
        token = access_token
        if not token:
            from src.engines.crewai.tools.custom.powerbi_auth_utils import (
                get_powerbi_access_token_from_config,
            )
            token = _run_async(get_powerbi_access_token_from_config(auth_config))

        # 1. Extract measures via Execute Queries API
        try:
            from src.converters.services.powerbi.connector import PowerBIConnector

            connector = PowerBIConnector(
                semantic_model_id=dataset_id,
                group_id=workspace_id,
                access_token=token,
            )
            connector.connect()
            kpis = connector.extract_measures(include_hidden=True)

            measures = []
            for kpi in kpis:
                # kpi.technical_name is derived from the actual PBI measure name (snake_cased).
                # kpi.description may contain a textual description, not the measure name.
                measure_name = kpi.technical_name or kpi.description
                measures.append({
                    'measure_name': measure_name,
                    'original_name': measure_name,
                    'dax_expression': kpi.formula or '',
                    'proposed_allocation': kpi.source_table or '__unassigned__',
                    'table_refs': [],
                })
            result['measures'] = measures
            logger.info(f"[UCMV] Extracted {len(measures)} measures from PBI API")
        except Exception as e:
            logger.warning(f"[UCMV] Measure extraction failed: {e}")

        # 1b. Measure DAX fallback — the Execute Queries / XMLA path above is
        # frequently rejected for Service-Account (ROPC) tokens, yielding zero
        # measures. Recover via (a) Fabric TMDL (which an SA CAN read) and, if a
        # client_secret is available, (b) a Service-Principal retry. Mirrors the
        # Semantic Model Fetcher's TMDL→SP DAX strategy.
        if not result.get('measures'):
            recovered = self._extract_measures_fallback(
                workspace_id=workspace_id, dataset_id=dataset_id,
                tenant_id=tenant_id, client_id=client_id, client_secret=client_secret,
                username=username, password=password,
            )
            if recovered:
                result['measures'] = recovered
                logger.info(f"[UCMV] Recovered {len(recovered)} measures via TMDL/SP fallback")

        # 2. Extract MQuery via Admin API scan
        try:
            from src.converters.services.mquery.scanner import PowerBIAdminScanner

            scanner = PowerBIAdminScanner(access_token=token)

            scan_result, raw_scan = _run_async(
                scanner.scan_workspace(workspace_id, dataset_id=dataset_id)
            )

            if raw_scan:
                result['scan_data'] = raw_scan

            mquery_entries = []
            for model in scan_result:
                for table in model.tables:
                    for expr in table.source_expressions:
                        mquery_entries.append({
                            'table_name': table.name,
                            'transpiled_sql': expr.embedded_sql or expr.raw_expression or '',
                            'validation_passed': 'Yes' if expr.embedded_sql else 'No',
                        })
            result['mquery'] = mquery_entries
            logger.info(f"[UCMV] Extracted {len(mquery_entries)} MQuery tables from PBI Admin API")
        except Exception as e:
            logger.warning(f"[UCMV] MQuery extraction failed: {e}")

        # 2b. MQuery fallback — the Admin Scanner (used above) rejects
        # Service-Account tokens (401/403), leaving mquery empty. Without MQuery
        # no fact table is detected and the generator emits 0 views even when
        # measures were extracted. Recover the table source expressions via
        # Fabric TMDL (SA-readable) or a Service-Principal retry, mirroring the
        # measure fallback and the Semantic Model Fetcher.
        if not result.get('mquery'):
            recovered_mq = self._extract_mquery_fallback(
                workspace_id=workspace_id, dataset_id=dataset_id,
                tenant_id=tenant_id, client_id=client_id, client_secret=client_secret,
                username=username, password=password,
            )
            if recovered_mq:
                result['mquery'] = recovered_mq
                logger.info(f"[UCMV] Recovered {len(recovered_mq)} MQuery tables via TMDL/SP fallback")

        # 3. Extract relationships via Execute Queries API
        try:
            import requests as req_lib

            pbi_api_base = pbi_api_base_url or 'https://api.powerbi.com/v1.0/myorg'
            url = f"{pbi_api_base}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json',
            }
            payload = {
                "queries": [{"query": "EVALUATE INFO.VIEW.RELATIONSHIPS()"}],
                "serializerSettings": {"includeNulls": True},
            }

            resp = req_lib.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code == 200:
                raw = resp.json()
                rows = (
                    raw.get('results', [{}])[0]
                    .get('tables', [{}])[0]
                    .get('rows', [])
                )
                relationships = []
                for row in rows:
                    relationships.append({
                        'from_table': row.get('[FromTable]', ''),
                        'from_column': row.get('[FromColumn]', ''),
                        'from_cardinality': row.get('[FromCardinality]', 'Many'),
                        'to_table': row.get('[ToTable]', ''),
                        'to_column': row.get('[ToColumn]', ''),
                        'to_cardinality': row.get('[ToCardinality]', 'One'),
                        'is_active': row.get('[IsActive]', True),
                    })
                result['relationships'] = relationships
                logger.info(f"[UCMV] Extracted {len(relationships)} relationships from PBI API")
            else:
                logger.warning(f"[UCMV] Relationships API returned {resp.status_code}")
        except Exception as e:
            logger.warning(f"[UCMV] Relationship extraction failed: {e}")

        return result
