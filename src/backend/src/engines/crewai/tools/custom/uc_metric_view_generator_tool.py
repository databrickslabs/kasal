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
    llm_model: Optional[str] = Field(None, description="LLM model for fallback (default: databricks-claude-sonnet-4)")
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
                       'use_llm_fallback', 'llm_model', 'llm_workspace_url', 'llm_token',
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

        measures_raw = _get('measures_json') or '[]'
        mquery_raw = _get('mquery_json') or '[]'
        relationships_raw = _get('relationships_json')
        scan_raw = _get('scan_data_json')
        config_raw = _get('config_json') or '{}'
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
                'llm_model': _get('llm_model') or 'databricks-claude-sonnet-4',
                'llm_workspace_url': _get('llm_workspace_url') or os.environ.get('DATABRICKS_HOST', ''),
                'llm_token': _get('llm_token') or os.environ.get('DATABRICKS_TOKEN', ''),
            }

        try:
            measures = json.loads(measures_raw) if isinstance(measures_raw, str) else measures_raw
            mquery_entries = json.loads(mquery_raw) if isinstance(mquery_raw, str) else mquery_raw
            config = json.loads(config_raw) if isinstance(config_raw, str) else config_raw
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON input: {e}"})

        # Parse MQuery
        parser = MQueryParser()
        mquery_tables = parser.parse_json(mquery_entries)

        # Parse relationships
        relationships_enrichment = {}
        if relationships_raw:
            try:
                rel_data = json.loads(relationships_raw) if isinstance(relationships_raw, str) else relationships_raw
                loader = RelationshipsLoader()
                fact_keys = {k for k, v in mquery_tables.items() if v.is_fact}
                relationships_enrichment = loader.load(rel_data, mquery_tables, fact_keys)
            except Exception as e:
                logger.warning(f"Failed to parse relationships: {e}")

        # Parse scan data
        scan_data = {}
        if scan_raw:
            try:
                scan_obj = json.loads(scan_raw) if isinstance(scan_raw, str) else scan_raw
                scan_parser = ScanDataParser()
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
        )
        pipeline.run()

        # Emit YAML + SQL
        yaml_output = pipeline.emit_all_yaml(catalog=catalog, schema=schema)
        sql_output = pipeline.emit_all_sql(catalog=catalog, schema=schema)
        results = pipeline.get_results()

        output = {
            'yaml': yaml_output,
            'sql': sql_output,
            'stats': results['stats'],
            'specs_summary': {
                k: {
                    'view_name': v.get('view_name'),
                    'measures': v.get('measures_count'),
                    'untranslatable': v.get('untranslatable_count'),
                }
                for k, v in results.get('specs', {}).items()
            },
        }
        return json.dumps(output, indent=2)

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
