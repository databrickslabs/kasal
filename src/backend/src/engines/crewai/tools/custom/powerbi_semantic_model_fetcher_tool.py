"""
Power BI Semantic Model Fetcher Tool for CrewAI

Extracts and caches semantic model metadata from Power BI:
1. Authenticates via Service Principal, Service Account, or User OAuth
2. Checks cache for today's metadata
3. Extracts model context (measures, relationships, tables, columns) via 3-tier fallback
4. Enriches with column metadata and sample values
5. Extracts default filters from report (if report_id provided)
6. Saves to cache for same-day reuse

Output is JSON consumable by the DAX tool (PowerBISemanticModelDaxTool).

Author: Kasal Team
Date: 2026
"""

import asyncio
import base64
import contextvars
import logging
import json
import re
from typing import Any, Optional, Type, Dict, List
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr
import httpx

from src.services.powerbi_semantic_model_cache_service import PowerBISemanticModelCacheService
from src.db.session import async_session_factory

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

_EXECUTOR = ThreadPoolExecutor(max_workers=5)


def _run_async_in_sync_context(coro):
    """
    Safely run an async coroutine from a synchronous context.
    Handles nested event loop scenarios (e.g., FastAPI).
    Propagates contextvars (like execution_id) to worker threads.
    """
    try:
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        future = _EXECUTOR.submit(ctx.run, asyncio.run, coro)
        return future.result()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


class PowerBISemanticModelFetcherSchema(BaseModel):
    """Input schema for PowerBISemanticModelFetcherTool."""

    # ===== POWER BI CONFIGURATION =====
    workspace_id: Optional[str] = Field(
        None,
        description="[Power BI] Workspace ID (GUID) containing the semantic model."
    )
    dataset_id: Optional[str] = Field(
        None,
        description="[Power BI] Dataset/Semantic Model ID (GUID) to extract metadata from."
    )
    report_id: Optional[str] = Field(
        None,
        description="[Power BI] Optional Report ID (GUID) to auto-extract default filters from."
    )

    # ===== SERVICE PRINCIPAL AUTHENTICATION =====
    tenant_id: Optional[str] = Field(
        None,
        description="[Auth] Azure AD tenant ID for Service Principal or Service Account."
    )
    client_id: Optional[str] = Field(
        None,
        description="[Auth] Application/Client ID (SemanticModel.ReadWrite.All permission)."
    )
    client_secret: Optional[str] = Field(
        None,
        description="[Auth] Client secret for Service Principal."
    )

    # ===== SERVICE ACCOUNT AUTHENTICATION =====
    username: Optional[str] = Field(
        None,
        description="[Auth] Service account username/UPN for Service Account authentication."
    )
    password: Optional[str] = Field(
        None,
        description="[Auth] Service account password for Service Account authentication."
    )
    auth_method: Optional[str] = Field(
        None,
        description="[Auth] Authentication method: 'service_principal', 'service_account', or auto-detect."
    )

    # ===== OR USER OAUTH =====
    access_token: Optional[str] = Field(
        None,
        description="[Auth] Access token for user OAuth authentication (alternative to SP/SA)."
    )

    # ===== OPTIONS =====
    skip_system_tables: bool = Field(
        True,
        description="[Options] Skip system tables like LocalDateTable."
    )
    enable_info_columns: bool = Field(
        False,
        description="[Options] Enable INFO.COLUMNS() metadata enrichment (requires DMV permissions). Default False."
    )
    output_format: str = Field(
        "json",
        description="[Output] Output format: 'json' (default, machine-parseable) or 'markdown'."
    )


class PowerBISemanticModelFetcherTool(BaseTool):
    """
    Power BI Semantic Model Fetcher — extracts and caches model metadata.

    Extracts measures, relationships, tables, columns, sample data, and default filters
    from a Power BI semantic model. Output is JSON that can be fed directly into the
    PowerBI Semantic Model DAX Generator tool.

    **Authentication** (choose one):
    - Service Principal: client_id + client_secret + tenant_id
    - Service Account: username + password + client_id + tenant_id
    - User OAuth: access_token

    **Extraction Strategy** (3-tier fallback):
    1. Fabric TMDL API (full context, Fabric workspaces)
    2. Admin Scanner API (full context, any workspace with admin permissions)
    3. DAX fallback (partial — tables from relationships only)
    """

    name: str = "Power BI Semantic Model Fetcher"
    description: str = (
        "Extracts and caches semantic model metadata (measures, tables, relationships, "
        "columns, sample data, default filters) from Power BI. Output is JSON that can be "
        "fed directly into the 'Power BI Semantic Model DAX Generator' tool. "
        "Connection credentials are pre-configured — do not provide them unless overriding."
    )
    args_schema: Type[BaseModel] = PowerBISemanticModelFetcherSchema

    _instance_id: str = PrivateAttr()
    _default_config: Dict[str, Any] = PrivateAttr()

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        import uuid
        instance_id = str(uuid.uuid4())[:8]
        logger.info(f"[FetcherTool.__init__] Instance ID: {instance_id}")

        default_config = {
            "workspace_id": kwargs.get("workspace_id"),
            "dataset_id": kwargs.get("dataset_id"),
            "report_id": kwargs.get("report_id"),
            "tenant_id": kwargs.get("tenant_id"),
            "client_id": kwargs.get("client_id"),
            "client_secret": kwargs.get("client_secret"),
            "username": kwargs.get("username"),
            "password": kwargs.get("password"),
            "auth_method": kwargs.get("auth_method"),
            "access_token": kwargs.get("access_token"),
            "skip_system_tables": kwargs.get("skip_system_tables", True),
            "enable_info_columns": kwargs.get("enable_info_columns", False),
            "output_format": kwargs.get("output_format", "json"),
        }

        tool_kwargs = {k: v for k, v in kwargs.items() if k not in default_config}
        super().__init__(**tool_kwargs)

        self._instance_id = instance_id
        self._default_config = default_config

    def _is_placeholder_value(self, value: Any) -> bool:
        """Check if a value looks like a placeholder/example that should be ignored."""
        if not isinstance(value, str):
            return False
        placeholder_patterns = [
            r'^[0-9]{8}-[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{12}$',
            r'your_.*_here', r'your-.*-here', r'<.*>', r'\{.*\}',
            r'placeholder', r'example\.com', r'^https://your-',
            r'^https://.*-url\.com$',
        ]
        value_lower = value.lower()
        for pattern in placeholder_patterns:
            if re.search(pattern, value_lower):
                return True
        return False

    def _run(self, **kwargs: Any) -> str:
        """Execute the fetcher pipeline."""
        try:
            instance_id = getattr(self, '_instance_id', 'UNKNOWN')
            logger.info(f"[FetcherTool] Instance {instance_id} - _run() called")

            # Filter out placeholder values
            filtered_kwargs = {}
            for k, v in kwargs.items():
                if v is not None and not self._is_placeholder_value(v):
                    filtered_kwargs[k] = v
                elif self._is_placeholder_value(v):
                    logger.info(f"[FetcherTool] Ignoring placeholder for '{k}'")

            # Merge: default config takes precedence for auth/connection params
            merged_config = {}
            config_params = [
                "workspace_id", "dataset_id", "report_id", "tenant_id", "client_id",
                "client_secret", "username", "password", "auth_method", "access_token",
            ]
            for key in config_params:
                default_val = self._default_config.get(key)
                kwarg_val = filtered_kwargs.get(key)
                merged_config[key] = default_val if default_val is not None else kwarg_val

            # Options — prefer kwargs if provided
            for key in ["skip_system_tables", "enable_info_columns", "output_format"]:
                kwarg_val = filtered_kwargs.get(key)
                default_val = self._default_config.get(key)
                merged_config[key] = kwarg_val if kwarg_val is not None else default_val

            # Validate required params
            workspace_id = merged_config.get("workspace_id")
            dataset_id = merged_config.get("dataset_id")
            if not workspace_id:
                return "Error: workspace_id is required."
            if not dataset_id:
                return "Error: dataset_id is required."

            # Validate authentication
            has_sp_auth = all([
                merged_config.get("tenant_id"),
                merged_config.get("client_id"),
                merged_config.get("client_secret"),
            ])
            has_sa_auth = all([
                merged_config.get("tenant_id"),
                merged_config.get("client_id"),
                merged_config.get("username"),
                merged_config.get("password"),
            ])
            has_oauth = bool(merged_config.get("access_token"))

            if not has_sp_auth and not has_sa_auth and not has_oauth:
                return (
                    "Error: Authentication required.\n"
                    "Provide one of:\n"
                    "- Service Principal: tenant_id, client_id, client_secret\n"
                    "- Service Account: tenant_id, client_id, username, password\n"
                    "- User OAuth: access_token"
                )

            result = _run_async_in_sync_context(self._execute_fetcher_pipeline(merged_config))
            return result

        except Exception as e:
            logger.error(f"[FetcherTool] Error: {str(e)}", exc_info=True)
            return f"Error: {str(e)}"

    async def _execute_fetcher_pipeline(self, config: Dict[str, Any]) -> str:
        """Execute the fetcher pipeline: auth → cache check → extract → enrich → cache save."""
        workspace_id = config["workspace_id"]
        dataset_id = config["dataset_id"]
        report_id = config.get("report_id")
        output_format = config.get("output_format", "json")

        logger.info(f"[FetcherTool] Starting pipeline: workspace={workspace_id}, dataset={dataset_id}")

        # Step 1: Get access token
        try:
            access_token = await self._get_access_token(config)
            logger.info("[FetcherTool] Access token obtained successfully")
        except Exception as e:
            return json.dumps({"error": f"Authentication error: {str(e)}"})

        # Step 1.5: Check cache
        group_id = config.get("group_id", "default")
        model_context = {
            "measures": [],
            "relationships": [],
            "tables": [],
            "columns": [],
            "sample_data": {},
        }
        default_filters = {}
        cache_hit = False

        try:
            async with async_session_factory() as session:
                cache_service = PowerBISemanticModelCacheService(session)
                cached_metadata = await cache_service.get_cached_metadata(
                    group_id=group_id,
                    dataset_id=dataset_id,
                    workspace_id=workspace_id,
                    report_id=report_id,
                )
            if cached_metadata:
                cache_hit = True
                logger.info(f"[CACHE HIT] Using cached metadata for dataset {dataset_id}")
                cached_tables = cached_metadata.get("schema", {}).get("tables", [])
                cached_columns = cached_metadata.get("schema", {}).get("columns", [])
                # Rebuild top-level columns from per-table columns if cache has stale empty list
                if not cached_columns and cached_tables:
                    for table in cached_tables:
                        table_name = table.get("name", "")
                        for col in table.get("columns", []):
                            cached_columns.append({"table": table_name, "column": col})
                    if cached_columns:
                        logger.info(f"[CACHE FIX] Rebuilt {len(cached_columns)} top-level columns from per-table data")
                model_context = {
                    "measures": cached_metadata.get("measures", []),
                    "relationships": cached_metadata.get("relationships", []),
                    "tables": cached_tables,
                    "columns": cached_columns,
                    "sample_data": cached_metadata.get("sample_data", {}),
                }
                if report_id and "default_filters" in cached_metadata:
                    default_filters = cached_metadata["default_filters"] or {}
                # Re-fetch sample data if cache has empty sample_data but tables have columns
                if not model_context.get("sample_data") and model_context.get("columns"):
                    try:
                        logger.info("[CACHE FIX] Re-fetching sample data for cached metadata with empty sample_data")
                        sample_values = await self._fetch_sample_column_values(
                            workspace_id, dataset_id, access_token, model_context, config
                        )
                        if sample_values:
                            model_context["sample_data"] = sample_values
                            logger.info(f"[CACHE FIX] Fetched sample values for {len(sample_values)} columns")
                            # Update cache with sample data
                            try:
                                async with async_session_factory() as session:
                                    cache_service = PowerBISemanticModelCacheService(session)
                                    cache_metadata = cache_service.build_metadata_dict(
                                        measures=model_context.get("measures", []),
                                        relationships=model_context.get("relationships", []),
                                        schema={
                                            "tables": model_context.get("tables", []),
                                            "columns": model_context.get("columns", []),
                                        },
                                        sample_data=sample_values,
                                        default_filters=default_filters if report_id else None,
                                    )
                                    await cache_service.save_metadata(
                                        group_id=group_id,
                                        dataset_id=dataset_id,
                                        workspace_id=workspace_id,
                                        metadata=cache_metadata,
                                        report_id=report_id,
                                    )
                                    logger.info("[CACHE FIX] Updated cache with sample data")
                            except Exception as e:
                                logger.warning(f"[CACHE FIX] Failed to update cache: {e}")
                    except Exception as e:
                        logger.warning(f"[CACHE FIX] Could not re-fetch sample data: {e}")
            else:
                logger.info(f"[CACHE MISS] Fetching fresh metadata for dataset {dataset_id}")
        except Exception as e:
            logger.warning(f"[Cache] Cache check failed: {e}")

        # Step 2: Extract model context (if not cached)
        if not cache_hit:
            try:
                model_context = await self._extract_model_context(
                    workspace_id, dataset_id, access_token, config
                )
                logger.info(
                    f"[FetcherTool] Extracted: {len(model_context['measures'])} measures, "
                    f"{len(model_context['relationships'])} relationships"
                )

                # Step 2b: Enrich
                try:
                    model_context = await self._enrich_model_context_with_metadata(
                        model_context, workspace_id, dataset_id, access_token, config
                    )
                except Exception as e:
                    logger.warning(f"[FetcherTool] Enrichment failed (continuing): {e}")

                # Step 2c: Extract default filters
                if report_id:
                    try:
                        default_filters = await self._extract_default_filters(
                            workspace_id, report_id, access_token
                        )
                    except Exception as e:
                        logger.warning(f"[FetcherTool] Filter extraction failed: {e}")

                # Step 2d: Save to cache
                try:
                    async with async_session_factory() as session:
                        cache_service = PowerBISemanticModelCacheService(session)
                        cache_metadata = cache_service.build_metadata_dict(
                            measures=model_context.get("measures", []),
                            relationships=model_context.get("relationships", []),
                            schema={
                                "tables": model_context.get("tables", []),
                                "columns": model_context.get("columns", []),
                            },
                            sample_data=model_context.get("sample_data", {}),
                            default_filters=default_filters if report_id else None,
                        )
                        await cache_service.save_metadata(
                            group_id=group_id,
                            dataset_id=dataset_id,
                            workspace_id=workspace_id,
                            metadata=cache_metadata,
                            report_id=report_id,
                        )
                        logger.info(f"[CACHE SAVED] Metadata cached for dataset {dataset_id}")
                except Exception as e:
                    logger.warning(f"[Cache] Failed to save: {e}")

            except Exception as e:
                logger.error(f"[FetcherTool] Model extraction error: {e}")
                return json.dumps({"error": f"Model extraction error: {str(e)}"})

        # Build output
        output = {
            "workspace_id": workspace_id,
            "dataset_id": dataset_id,
            "report_id": report_id,
            "cache_hit": cache_hit,
            "measures": model_context.get("measures", []),
            "relationships": model_context.get("relationships", []),
            "tables": model_context.get("tables", []),
            "columns": model_context.get("columns", []),
            "sample_data": model_context.get("sample_data", {}),
            "default_filters": default_filters,
            "summary": {
                "measure_count": len(model_context.get("measures", [])),
                "table_count": len(model_context.get("tables", [])),
                "relationship_count": len(model_context.get("relationships", [])),
                "filter_count": len(default_filters),
            },
        }

        if output_format == "markdown":
            return self._format_as_markdown(output)
        return json.dumps(output, indent=2, default=str)

    def _format_as_markdown(self, output: Dict[str, Any]) -> str:
        """Format fetcher output as markdown."""
        lines = []
        lines.append("# Power BI Semantic Model Metadata\n")
        lines.append(f"**Workspace**: `{output['workspace_id']}`")
        lines.append(f"**Dataset**: `{output['dataset_id']}`")
        lines.append(f"**Cache Hit**: {output['cache_hit']}\n")

        summary = output["summary"]
        lines.append(f"- **Measures**: {summary['measure_count']}")
        lines.append(f"- **Tables**: {summary['table_count']}")
        lines.append(f"- **Relationships**: {summary['relationship_count']}")
        lines.append(f"- **Default Filters**: {summary['filter_count']}\n")

        measures = output.get("measures", [])
        if measures:
            lines.append("## Measures\n")
            for m in measures[:30]:
                expr = m.get("expression", "")[:80]
                lines.append(f"- **{m['name']}** (Table: {m.get('table', '')}): `{expr}...`")
            lines.append("")

        tables = output.get("tables", [])
        if tables:
            lines.append("## Tables\n")
            for t in tables[:20]:
                cols = t.get("columns", [])
                col_str = ", ".join(cols[:10])
                lines.append(f"- **{t['name']}**: {col_str}")
            lines.append("")

        filters = output.get("default_filters", {})
        if filters:
            lines.append("## Default Filters\n")
            for name, desc in filters.items():
                lines.append(f"- **{name}**: {desc}")
            lines.append("")

        return "\n".join(lines)

    # =====================================================================
    # Authentication
    # =====================================================================

    async def _get_access_token(self, config: Dict[str, Any]) -> str:
        """Get OAuth access token using centralized auth utilities."""
        from src.engines.crewai.tools.custom.powerbi_auth_utils import get_powerbi_access_token_from_config
        return await get_powerbi_access_token_from_config(config)

    async def _get_fabric_token(self, config: Dict[str, Any]) -> str:
        """Get Fabric API token for TMDL access."""
        from src.engines.crewai.tools.custom.powerbi_auth_utils import get_fabric_access_token_from_config
        return await get_fabric_access_token_from_config(config)

    # =====================================================================
    # Model Extraction (3-tier fallback)
    # =====================================================================

    async def _extract_model_context(
        self, workspace_id: str, dataset_id: str, access_token: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Extract measures, relationships, and tables from the semantic model."""
        model_context: Dict[str, Any] = {
            "measures": [],
            "relationships": [],
            "tables": [],
        }

        # Get Fabric token for TMDL
        fabric_token = access_token
        try:
            if config.get("tenant_id") and config.get("client_id") and config.get("client_secret"):
                fabric_token = await self._get_fabric_token(config)
        except Exception as e:
            logger.warning(f"Could not get Fabric token, using Power BI token: {e}")

        # 3-tier fallback: Fabric TMDL → Admin Scanner → DAX
        tmdl_parts = await self._fetch_tmdl_via_fabric(workspace_id, dataset_id, fabric_token)
        if tmdl_parts is not None:
            measures, tables = self._parse_tmdl_for_measures_and_tables(tmdl_parts, config)
            logger.info(f"[Model Context] Fabric TMDL: {len(measures)} measure(s), {len(tables)} table(s)")
        else:
            logger.info("[Model Context] Fabric API unavailable — trying Admin Scanner API")
            measures, tables = await self._fetch_model_via_admin_scanner(
                workspace_id, dataset_id, access_token, config
            )
            if not measures and not tables:
                logger.info("[Model Context] Admin Scanner unavailable — falling back to DAX")
                measures, tables = await self._fetch_model_via_powerbi_dax(
                    workspace_id, dataset_id, access_token, config
                )
        model_context["measures"] = measures
        model_context["tables"] = tables

        # Fetch relationships via DAX
        relationships = await self._fetch_relationships(workspace_id, dataset_id, access_token, config)
        model_context["relationships"] = relationships

        # Build top-level columns list from per-table columns
        all_columns = []
        for table in model_context["tables"]:
            table_name = table["name"]
            for col in table.get("columns", []):
                all_columns.append({"table": table_name, "column": col})
        model_context["columns"] = all_columns

        return model_context

    async def _fetch_tmdl_via_fabric(
        self, workspace_id: str, dataset_id: str, access_token: str
    ) -> Optional[List[Dict[str, Any]]]:
        """Fetch TMDL definition from the Fabric REST API."""
        url = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
            f"/semanticModels/{dataset_id}/getDefinition?format=TMDL"
        )
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        async with httpx.AsyncClient(timeout=180.0) as client:
            try:
                response = await client.post(url, headers=headers)

                if response.status_code == 202:
                    location = response.headers.get("Location")
                    if not location:
                        return None
                    for _ in range(60):
                        await asyncio.sleep(2)
                        poll_response = await client.get(location, headers=headers)
                        poll_data = poll_response.json()
                        if poll_data.get("status") == "Succeeded":
                            result_url = location + "/result"
                            result_response = await client.get(result_url, headers=headers)
                            result_response.raise_for_status()
                            return result_response.json().get("definition", {}).get("parts", [])
                        elif poll_data.get("status") == "Failed":
                            logger.error(f"[TMDL] Fabric operation failed: {poll_data}")
                            return None
                    logger.warning("[TMDL] Fabric polling timed out after 120 s")
                    return None

                elif response.status_code == 200:
                    return response.json().get("definition", {}).get("parts", [])
                else:
                    logger.warning(f"[TMDL] Fabric API returned HTTP {response.status_code}")
                    return None

            except Exception as e:
                logger.error(f"[TMDL] Fabric API exception: {e}")
                return None

    async def _fetch_model_via_admin_scanner(
        self, workspace_id: str, dataset_id: str, access_token: str, config: Dict[str, Any]
    ) -> tuple:
        """Fetch full model schema via the Power BI Admin Metadata Scanner API."""
        base = "https://api.powerbi.com/v1.0/myorg/admin/workspaces"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        skip_system = config.get("skip_system_tables", True)

        async with httpx.AsyncClient(timeout=120.0) as client:
            # Step 1: kick off the scan
            try:
                response = await client.post(
                    f"{base}/getInfo"
                    "?lineage=false"
                    "&datasourceDetails=false"
                    "&datasetSchema=true"
                    "&datasetExpressions=true",
                    headers=headers,
                    json={"workspaces": [workspace_id]},
                )
                if response.status_code in (401, 403):
                    logger.info(f"[Admin Scanner] SP lacks admin permissions (HTTP {response.status_code})")
                    return [], []
                response.raise_for_status()
                scan_id = response.json().get("id")
                if not scan_id:
                    return [], []
            except httpx.HTTPStatusError:
                return [], []
            except Exception as e:
                logger.warning(f"[Admin Scanner] getInfo exception: {e}")
                return [], []

            # Step 2: poll
            try:
                for _ in range(30):
                    await asyncio.sleep(2)
                    poll = await client.get(f"{base}/scanStatus/{scan_id}", headers=headers)
                    poll.raise_for_status()
                    status = poll.json().get("status", "")
                    if status == "Succeeded":
                        break
                    if status == "Failed":
                        return [], []
                else:
                    return [], []
            except Exception:
                return [], []

            # Step 3: fetch result
            try:
                result_resp = await client.get(f"{base}/scanResult/{scan_id}", headers=headers)
                result_resp.raise_for_status()
                workspaces = result_resp.json().get("workspaces", [])
            except Exception:
                return [], []

        # Step 4: parse
        measures: List[Dict[str, Any]] = []
        tables: List[Dict[str, Any]] = []

        target_ws = next(
            (ws for ws in workspaces if ws.get("id", "").lower() == workspace_id.lower()), None
        )
        if not target_ws:
            return [], []
        target_ds = next(
            (ds for ds in target_ws.get("datasets", []) if ds.get("id", "").lower() == dataset_id.lower()), None
        )
        if not target_ds:
            return [], []

        for table in target_ds.get("tables", []):
            table_name = table.get("name", "")
            if skip_system and ("LocalDateTable" in table_name or "DateTableTemplate" in table_name):
                continue
            columns = [
                col.get("name", "")
                for col in table.get("columns", [])
                if col.get("name") and not col.get("isHidden", False)
            ]
            tables.append({"name": table_name, "columns": columns})
            for measure in table.get("measures", []):
                measure_name = measure.get("name", "")
                expression = measure.get("expression", "")
                if not measure_name or not expression.strip():
                    continue
                measures.append({"name": measure_name, "table": table_name, "expression": expression.strip()})

        logger.info(f"[Admin Scanner] {len(measures)} measure(s), {len(tables)} table(s)")
        return measures, tables

    async def _fetch_model_via_powerbi_dax(
        self, workspace_id: str, dataset_id: str, access_token: str, config: Dict[str, Any]
    ) -> tuple:
        """Fallback: derive table names from INFO.VIEW.RELATIONSHIPS()."""
        query_url = (
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
            f"/datasets/{dataset_id}/executeQueries"
        )
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        tables: List[Dict[str, Any]] = []

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    query_url, headers=headers,
                    json={"queries": [{"query": "EVALUATE INFO.VIEW.RELATIONSHIPS()"}], "serializerSettings": {"includeNulls": True}},
                )
            response.raise_for_status()
            rows = response.json().get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
            seen: set = set()
            for row in rows:
                for key in ("[FromTable]", "[ToTable]"):
                    name = row.get(key, "")
                    if not name or name in seen:
                        continue
                    if config.get("skip_system_tables", True) and ("LocalDateTable" in name or "DateTableTemplate" in name):
                        continue
                    seen.add(name)
                    tables.append({"name": name})
        except Exception as e:
            logger.warning(f"[PowerBI Fallback] INFO.VIEW.RELATIONSHIPS() failed: {e}")

        # Enrich tables with columns via a single INFO.VIEW.COLUMNS() query
        if tables:
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    col_response = await client.post(
                        query_url, headers=headers,
                        json={
                            "queries": [{"query": "EVALUATE SELECTCOLUMNS(INFO.VIEW.COLUMNS(), [TableName], [ExplicitName], [DataType], [IsHidden])"}],
                            "serializerSettings": {"includeNulls": True},
                        },
                    )
                col_response.raise_for_status()
                col_rows = col_response.json().get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])

                # Group columns by table name
                columns_by_table: Dict[str, List[str]] = {}
                for row in col_rows:
                    tbl = row.get("[TableName]", "")
                    col_name = row.get("[ExplicitName]", "")
                    is_hidden = row.get("[IsHidden]", False)
                    if tbl and col_name and not is_hidden:
                        columns_by_table.setdefault(tbl, []).append(col_name)

                for table in tables:
                    cols = columns_by_table.get(table["name"], [])
                    if cols:
                        table["columns"] = cols
                logger.info(f"[PowerBI Fallback] Enriched {sum(1 for t in tables if t.get('columns'))} tables with columns via INFO.VIEW.COLUMNS()")
            except Exception as e:
                logger.warning(f"[PowerBI Fallback] INFO.VIEW.COLUMNS() failed: {e}")

        measures: List[Dict[str, Any]] = []
        return measures, tables

    def _parse_tmdl_for_measures_and_tables(
        self, tmdl_parts: List[Dict[str, Any]], config: Dict[str, Any]
    ) -> tuple:
        """Parse TMDL parts to extract measures and tables."""
        measures = []
        tables = []

        for part in tmdl_parts:
            path = part.get("path", "")
            payload = part.get("payload", "")
            if not path.startswith("definition/tables/") or not path.endswith(".tmdl"):
                continue
            try:
                tmdl_content = base64.b64decode(payload).decode("utf-8")
                table_match = re.match(r"table\s+(?:'([^']+)'|(\w+))", tmdl_content.strip())
                if not table_match:
                    continue
                table_name = table_match.group(1) or table_match.group(2)
                if config.get("skip_system_tables", True):
                    if "LocalDateTable" in table_name or "DateTableTemplate" in table_name:
                        continue

                tables.append({"name": table_name})

                # Columns
                column_pattern = re.compile(r"column\s+(?:'([^']+)'|(\w+))", re.MULTILINE)
                columns = []
                for col_match in column_pattern.finditer(tmdl_content):
                    col_name = col_match.group(1) or col_match.group(2)
                    columns.append(col_name)
                if columns:
                    tables[-1]["columns"] = columns

                # Measures
                measure_pattern = re.compile(
                    r"measure\s+(?:'([^']+)'|(\w+))\s*=\s*([\s\S]*?)(?=\n\s*measure|\n\s*column|\n\t[^\t]|\Z)",
                    re.MULTILINE,
                )
                for match in measure_pattern.finditer(tmdl_content):
                    measure_name = match.group(1) or match.group(2)
                    expression = match.group(3).strip()
                    clean_lines = []
                    for line in expression.split('\n'):
                        stripped = line.strip()
                        if stripped.startswith(('lineageTag:', 'formatString:', 'annotation', 'isHidden')):
                            break
                        clean_lines.append(line)
                    measures.append({
                        "name": measure_name,
                        "table": table_name,
                        "expression": '\n'.join(clean_lines).strip(),
                    })
            except Exception as e:
                logger.warning(f"Error parsing TMDL from {path}: {e}")

        return measures, tables

    async def _fetch_relationships(
        self, workspace_id: str, dataset_id: str, access_token: str, config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Extract relationships using INFO.VIEW.RELATIONSHIPS() DAX function."""
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        payload = {
            "queries": [{"query": "EVALUATE INFO.VIEW.RELATIONSHIPS()"}],
            "serializerSettings": {"includeNulls": True},
        }

        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                rows = response.json().get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
                relationships = []
                seen_ids = set()
                for row in rows:
                    rel_id = row.get("[ID]")
                    if rel_id in seen_ids:
                        continue
                    seen_ids.add(rel_id)
                    from_table = row.get("[FromTable]", "")
                    to_table = row.get("[ToTable]", "")
                    if config.get("skip_system_tables", True):
                        if "LocalDateTable" in from_table or "LocalDateTable" in to_table:
                            continue
                    relationships.append({
                        "from_table": from_table,
                        "from_column": row.get("[FromColumn]", ""),
                        "to_table": to_table,
                        "to_column": row.get("[ToColumn]", ""),
                        "is_active": row.get("[IsActive]", True),
                    })
                return relationships
            except Exception as e:
                logger.error(f"Relationships extraction error: {e}")
                return []

    # =====================================================================
    # Enrichment
    # =====================================================================

    async def _enrich_model_context_with_metadata(
        self, model_context: Dict[str, Any], workspace_id: str, dataset_id: str,
        access_token: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Enrich model context with column metadata and sample values."""
        enriched_context = {**model_context}

        if config.get("enable_info_columns", False):
            try:
                tables = enriched_context.get("tables", [])
                total_columns_enriched = 0
                tables_enriched = 0
                for table in tables[:10]:
                    table_name = table["name"]
                    if config.get("skip_system_tables", True):
                        if "LocalDateTable" in table_name or "DateTableTemplate" in table_name:
                            continue
                    try:
                        columns_metadata = await self._fetch_column_metadata_for_table(
                            workspace_id, dataset_id, access_token, table_name, config
                        )
                        if columns_metadata:
                            table["column_metadata"] = columns_metadata
                            table["column_types"] = {c["column_name"]: c["data_type"] for c in columns_metadata}
                            table["column_descriptions"] = {
                                c["column_name"]: c.get("description", "")
                                for c in columns_metadata if c.get("description")
                            }
                            total_columns_enriched += len(columns_metadata)
                            tables_enriched += 1
                    except Exception as e:
                        logger.debug(f"[Context Enrichment] Could not fetch metadata for '{table_name}': {e}")
                if tables_enriched > 0:
                    logger.info(f"[Context Enrichment] Added column metadata for {tables_enriched} tables")
            except Exception as e:
                logger.warning(f"[Context Enrichment] Column metadata enrichment error: {e}")

        # Sample values
        try:
            sample_values = await self._fetch_sample_column_values(
                workspace_id, dataset_id, access_token, enriched_context, config
            )
            enriched_context["sample_data"] = sample_values
            logger.info(f"[Context Enrichment] Added sample values for {len(sample_values)} columns")
        except Exception as e:
            logger.warning(f"[Context Enrichment] Could not fetch sample values: {e}")

        return enriched_context

    async def _fetch_column_metadata_for_table(
        self, workspace_id: str, dataset_id: str, access_token: str,
        table_name: str, config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Fetch column metadata for a specific table using INFO.COLUMNS()."""
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        dax_query = f'EVALUATE INFO.COLUMNS("{table_name}")'
        payload = {"queries": [{"query": dax_query}], "serializerSettings": {"includeNulls": True}}

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(url, headers=headers, json=payload)
                if response.status_code != 200:
                    return []
                response.raise_for_status()
                rows = response.json().get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
                columns = []
                for row in rows:
                    column_name = row.get("[ExplicitName]", "") or row.get("[Name]", "")
                    columns.append({
                        "table_name": table_name,
                        "column_name": column_name,
                        "data_type": row.get("[DataType]", ""),
                        "is_hidden": row.get("[IsHidden]", False),
                        "description": row.get("[Description]", ""),
                    })
                return columns
            except Exception:
                return []

    async def _fetch_sample_column_values(
        self, workspace_id: str, dataset_id: str, access_token: str,
        model_context: Dict[str, Any], config: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch sample values for key columns."""
        sample_values: Dict[str, Dict[str, Any]] = {}
        tables = model_context.get("tables", [])[:5]

        # Suffixes that indicate ID/key columns (match end of name or standalone)
        _skip_suffixes = ("id", "key", "pk", "fk", "_id", "_key", "_pk", "_fk")

        for table in tables:
            table_name = table["name"]
            columns = table.get("columns", [])[:10]
            if not columns:
                logger.debug(f"[Sample Values] Skipping table '{table_name}' — no columns")
                continue
            for column in columns:
                try:
                    col_lower = column.lower().rstrip()
                    # Skip columns whose name ends with an ID/key suffix
                    if col_lower.endswith(_skip_suffixes):
                        continue
                    # Quote table name for DAX (handles spaces/special chars)
                    dax_query = f"EVALUATE TOPN(10, SUMMARIZECOLUMNS('{table_name}'[{column}]))"
                    result = await self._execute_dax_query(workspace_id, dataset_id, access_token, dax_query)
                    if result.get("success") and result.get("data"):
                        values = [list(row.values())[0] for row in result["data"][:10]]
                        sample_values[f"{table_name}[{column}]"] = {
                            "type": "categorical",
                            "sample_values": values,
                        }
                    elif result.get("error"):
                        logger.debug(f"[Sample Values] DAX error for '{table_name}'[{column}]: {result['error']}")
                except Exception as e:
                    logger.debug(f"[Sample Values] Exception for '{table_name}'[{column}]: {e}")
                    continue
        logger.info(f"[Sample Values] Fetched {len(sample_values)} column sample value sets")
        return sample_values

    async def _execute_dax_query(
        self, workspace_id: str, dataset_id: str, access_token: str, dax_query: str
    ) -> Dict[str, Any]:
        """Execute DAX query via Power BI Execute Queries API (for sample values)."""
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        payload = {"queries": [{"query": dax_query}], "serializerSettings": {"includeNulls": True}}
        result: Dict[str, Any] = {"success": False, "data": [], "row_count": 0, "columns": [], "error": None}

        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
                if "error" in data:
                    result["error"] = data["error"].get("message", str(data["error"]))
                    return result
                tables = data.get("results", [{}])[0].get("tables", [])
                if tables:
                    rows = tables[0].get("rows", [])
                    result["data"] = rows
                    result["row_count"] = len(rows)
                    result["success"] = True
                    if rows:
                        result["columns"] = list(rows[0].keys())
                return result
            except httpx.HTTPStatusError as e:
                result["error"] = f"HTTP {e.response.status_code}: {e.response.text}"
                return result
            except Exception as e:
                result["error"] = str(e)
                return result

    # =====================================================================
    # Filter Extraction
    # =====================================================================

    async def _extract_default_filters(
        self, workspace_id: str, report_id: str, access_token: str
    ) -> Dict[str, Any]:
        """Extract default filters from Power BI report definition via Fabric API."""
        logger.info(f"[Filter Extraction] Extracting filters from report {report_id}")
        filters: Dict[str, Any] = {}

        try:
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/reports/{report_id}/getDefinition"
            headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers, json={}, timeout=60.0)

                if response.status_code == 202:
                    location = response.headers.get("Location")
                    if location:
                        for _ in range(10):
                            await asyncio.sleep(2)
                            poll_response = await client.get(location, headers=headers)
                            poll_data = poll_response.json()
                            if poll_data.get("status") == "Succeeded":
                                result_url = location + "/result"
                                result_response = await client.get(result_url, headers=headers)
                                result_response.raise_for_status()
                                report_parts = result_response.json().get("definition", {}).get("parts", [])
                                filters = self._parse_tmdl_for_filters(report_parts)
                                break
                            elif poll_data.get("status") == "Failed":
                                break
                elif response.status_code == 200:
                    report_parts = response.json().get("definition", {}).get("parts", [])
                    filters = self._parse_tmdl_for_filters(report_parts)

        except Exception as e:
            logger.warning(f"[Filter Extraction] Error: {e}")

        return filters

    def _parse_tmdl_for_filters(self, tmdl_parts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Parse report definition (PBIR format) to extract filters."""
        filters: Dict[str, Any] = {}
        try:
            for part in tmdl_parts:
                payload = part.get("payload", "")
                path = part.get("path", "")
                if "report.json" in path.lower():
                    try:
                        content = base64.b64decode(payload).decode("utf-8")
                        report_json = json.loads(content)
                        if "filters" in report_json:
                            filters_str = report_json["filters"]
                            if isinstance(filters_str, str):
                                filter_definitions = json.loads(filters_str)
                            else:
                                filter_definitions = filters_str
                            for filter_def in filter_definitions:
                                filter_name, filter_description = self._extract_filter_from_definition(filter_def)
                                if filter_name and filter_description:
                                    filters[filter_name] = filter_description
                    except json.JSONDecodeError as e:
                        logger.warning(f"[Filter Extraction] Failed to parse JSON: {e}")
        except Exception as e:
            logger.warning(f"[Filter Extraction] Error parsing report: {e}")
        return filters

    def _extract_filter_from_definition(self, filter_def: Dict[str, Any]) -> tuple:
        """Extract filter name and description from Power BI filter definition."""
        try:
            expression = filter_def.get("expression", {})
            column_expr = expression.get("Column", {})
            expr = column_expr.get("Expression", {})
            source_ref = expr.get("SourceRef", {})
            table = source_ref.get("Entity", "")
            column = column_expr.get("Property", "")
            if not table or not column:
                return (None, None)
            filter_name = f"{table}[{column}]"
            filter_obj = filter_def.get("filter", {})
            where_clauses = filter_obj.get("Where", [])
            if not where_clauses:
                return (filter_name, "has filter (unknown type)")
            condition = where_clauses[0].get("Condition", {})
            filter_description = self._parse_filter_condition(condition)
            return (filter_name, filter_description)
        except Exception:
            return (None, None)

    def _parse_filter_condition(self, condition: Dict[str, Any]) -> str:
        """Parse a Power BI filter condition into a human-readable description."""
        try:
            if "Not" in condition:
                not_expr = condition["Not"]["Expression"]
                if "In" in not_expr:
                    in_expr = not_expr["In"]
                    values = in_expr.get("Values", [])
                    if values and len(values) > 0:
                        first_value = values[0]
                        if len(first_value) > 0:
                            literal = first_value[0].get("Literal", {})
                            if literal.get("Value") == "null":
                                return "NOT NULL"
                    value_strs = []
                    for val_list in values:
                        for val in val_list:
                            lit_val = val.get("Literal", {}).get("Value", "")
                            value_strs.append(lit_val.strip("'\""))
                    if value_strs:
                        return f"NOT IN ({', '.join(value_strs)})"
                elif "StartsWith" in not_expr:
                    starts_with = not_expr["StartsWith"]
                    right_val = starts_with.get("Right", {}).get("Literal", {}).get("Value", "")
                    cleaned_val = right_val.strip("'\"")
                    return f"NOT STARTS WITH '{cleaned_val}'"
            elif "In" in condition:
                in_expr = condition["In"]
                values = in_expr.get("Values", [])
                value_strs = []
                for val_list in values:
                    for val in val_list:
                        lit_val = val.get("Literal", {}).get("Value", "")
                        value_strs.append(lit_val.strip("'\""))
                if len(value_strs) == 1:
                    return f"= '{value_strs[0]}'"
                elif len(value_strs) > 1:
                    return f"IN ({', '.join(value_strs)})"
            elif "Comparison" in condition:
                comparison = condition["Comparison"]
                operator = comparison.get("ComparisonKind", 0)
                right = comparison.get("Right", {}).get("Literal", {}).get("Value", "")
                op_map = {0: "=", 1: "!=", 2: ">", 3: ">=", 4: "<", 5: "<="}
                op_str = op_map.get(operator, "=")
                return f"{op_str} {right}"
            return "has complex filter"
        except Exception:
            return "has filter"
