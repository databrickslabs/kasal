"""
Power BI Semantic Model Fetcher Tool for CrewAI

Extracts and caches semantic model metadata from Power BI:
1. Authenticates via Service Principal, Service Account, or User OAuth
2. Checks cache for today's metadata
3. Extracts model context (measures, relationships, tables, columns) via 3-tier fallback
4. Enriches with column metadata and sample values
5. Extracts default filters from report (if report_id provided)
6. Extracts slicer visuals from report (if report_id provided)
7. Saves to cache for same-day reuse

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
            "slicers": [],
        }
        default_filters = {}
        slicers: List[Dict[str, Any]] = []
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
                cached_tables = cached_metadata.get("schema", {}).get("tables", [])
                cached_columns = cached_metadata.get("schema", {}).get("columns", [])
                cached_sample_data = cached_metadata.get("sample_data", {})
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
                    "sample_data": cached_sample_data,
                }
                if report_id and "default_filters" in cached_metadata:
                    default_filters = cached_metadata["default_filters"] or {}
                if "slicers" in cached_metadata:
                    slicers = cached_metadata["slicers"] or []
                logger.info(
                    f"[CACHE HIT] dataset {dataset_id}: "
                    f"{len(model_context['measures'])} measures, "
                    f"{len(cached_tables)} tables, "
                    f"{len(cached_columns)} columns, "
                    f"{len(cached_sample_data)} sample_data entries, "
                    f"{len(slicers)} slicers"
                )
                # Re-fetch sample data ONCE if cache has empty sample_data but tables have columns
                if not cached_sample_data and model_context.get("columns"):
                    try:
                        logger.info("[CACHE FIX] Sample data missing from cache — fetching once")
                        sample_values = await self._fetch_sample_column_values(
                            workspace_id, dataset_id, access_token, model_context, config
                        )
                        model_context["sample_data"] = sample_values or {}
                        logger.info(f"[CACHE FIX] Fetched {len(sample_values)} sample value sets — persisting to cache")
                        # Persist updated cache so next run skips re-fetch
                        try:
                            async with async_session_factory() as session:
                                cache_service = PowerBISemanticModelCacheService(session)
                                updated_metadata = cache_service.build_metadata_dict(
                                    measures=model_context.get("measures", []),
                                    relationships=model_context.get("relationships", []),
                                    schema={
                                        "tables": model_context.get("tables", []),
                                        "columns": model_context.get("columns", []),
                                    },
                                    sample_data=model_context["sample_data"],
                                    default_filters=default_filters if report_id else None,
                                    slicers=slicers if report_id else None,
                                )
                                await cache_service.save_metadata(
                                    group_id=group_id,
                                    dataset_id=dataset_id,
                                    workspace_id=workspace_id,
                                    metadata=updated_metadata,
                                    report_id=report_id,
                                )
                                logger.info("[CACHE UPDATED] Sample data now persisted — next run will be instant")
                        except Exception as e:
                            logger.warning(f"[CACHE FIX] Failed to update cache with sample data: {e}")
                    except Exception as e:
                        logger.warning(f"[CACHE FIX] Could not fetch sample data: {e}")
                # Re-fetch slicers ONCE if cache has no slicers but report_id is present
                if report_id and "slicers" not in cached_metadata:
                    try:
                        logger.info("[CACHE FIX] Slicers missing from cache — fetching once")
                        report_parts = await self._extract_report_definition_parts(
                            workspace_id, report_id, access_token
                        )
                        slicers = self._extract_slicers_from_report(report_parts)
                        logger.info(f"[CACHE FIX] Fetched {len(slicers)} slicers — fetching distinct values")
                        # Fetch distinct values for slicer columns
                        if slicers:
                            try:
                                await self._fetch_slicer_distinct_values(
                                    workspace_id, dataset_id, access_token, slicers, model_context
                                )
                            except Exception as e:
                                logger.warning(f"[CACHE FIX] Slicer distinct values failed: {e}")
                        # Persist updated cache so next run skips re-fetch
                        try:
                            async with async_session_factory() as session:
                                cache_service = PowerBISemanticModelCacheService(session)
                                updated_metadata = cache_service.build_metadata_dict(
                                    measures=model_context.get("measures", []),
                                    relationships=model_context.get("relationships", []),
                                    schema={
                                        "tables": model_context.get("tables", []),
                                        "columns": model_context.get("columns", []),
                                    },
                                    sample_data=model_context.get("sample_data", {}),
                                    default_filters=default_filters if report_id else None,
                                    slicers=slicers,
                                )
                                await cache_service.save_metadata(
                                    group_id=group_id,
                                    dataset_id=dataset_id,
                                    workspace_id=workspace_id,
                                    metadata=updated_metadata,
                                    report_id=report_id,
                                )
                                logger.info("[CACHE UPDATED] Slicers now persisted — next run will be instant")
                        except Exception as e:
                            logger.warning(f"[CACHE FIX] Failed to update cache with slicers: {e}")
                    except Exception as e:
                        logger.warning(f"[CACHE FIX] Could not fetch slicers: {e}")
                # Backfill slicer distinct values if slicers exist but sample_data lacks them
                if slicers and not any(
                    v.get("type") == "slicer_values"
                    for v in model_context.get("sample_data", {}).values()
                ):
                    try:
                        logger.info("[CACHE FIX] Slicer distinct values missing — fetching once")
                        await self._fetch_slicer_distinct_values(
                            workspace_id, dataset_id, access_token, slicers, model_context
                        )
                        # Re-persist cache with updated sample_data
                        try:
                            async with async_session_factory() as session:
                                cache_service = PowerBISemanticModelCacheService(session)
                                updated_metadata = cache_service.build_metadata_dict(
                                    measures=model_context.get("measures", []),
                                    relationships=model_context.get("relationships", []),
                                    schema={
                                        "tables": model_context.get("tables", []),
                                        "columns": model_context.get("columns", []),
                                    },
                                    sample_data=model_context.get("sample_data", {}),
                                    default_filters=default_filters if report_id else None,
                                    slicers=slicers,
                                )
                                await cache_service.save_metadata(
                                    group_id=group_id,
                                    dataset_id=dataset_id,
                                    workspace_id=workspace_id,
                                    metadata=updated_metadata,
                                    report_id=report_id,
                                )
                                logger.info("[CACHE UPDATED] Slicer distinct values now persisted")
                        except Exception as e:
                            logger.warning(f"[CACHE FIX] Failed to update cache with slicer values: {e}")
                    except Exception as e:
                        logger.warning(f"[CACHE FIX] Could not fetch slicer distinct values: {e}")
                # Re-validate filters: skip parameters + check datatypes (one-time backfill)
                if report_id and default_filters and "_filters_validated" not in cached_metadata:
                    try:
                        logger.info("[CACHE FIX] Re-validating filters (parameter exclusion + datatype check)")
                        report_parts = await self._extract_report_definition_parts(
                            workspace_id, report_id, access_token
                        )
                        old_count = len(default_filters)
                        default_filters = self._parse_tmdl_for_filters(
                            report_parts, model_context=model_context
                        )
                        logger.info(
                            f"[CACHE FIX] Filters re-validated: {old_count} → {len(default_filters)} "
                            f"(removed {old_count - len(default_filters)} parameter filters)"
                        )
                        # Re-persist with validated filters + marker
                        try:
                            async with async_session_factory() as session:
                                cache_service = PowerBISemanticModelCacheService(session)
                                updated_metadata = cache_service.build_metadata_dict(
                                    measures=model_context.get("measures", []),
                                    relationships=model_context.get("relationships", []),
                                    schema={
                                        "tables": model_context.get("tables", []),
                                        "columns": model_context.get("columns", []),
                                    },
                                    sample_data=model_context.get("sample_data", {}),
                                    default_filters=default_filters,
                                    slicers=slicers if slicers else None,
                                )
                                updated_metadata["_filters_validated"] = True
                                await cache_service.save_metadata(
                                    group_id=group_id,
                                    dataset_id=dataset_id,
                                    workspace_id=workspace_id,
                                    metadata=updated_metadata,
                                    report_id=report_id,
                                )
                                logger.info("[CACHE UPDATED] Validated filters now persisted")
                        except Exception as e:
                            logger.warning(f"[CACHE FIX] Failed to update cache with validated filters: {e}")
                    except Exception as e:
                        logger.warning(f"[CACHE FIX] Could not re-validate filters: {e}")
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
                    f"{len(model_context['relationships'])} relationships, "
                    f"{len(slicers)} slicers"
                )

                # Step 2b: Enrich
                try:
                    model_context = await self._enrich_model_context_with_metadata(
                        model_context, workspace_id, dataset_id, access_token, config
                    )
                except Exception as e:
                    logger.warning(f"[FetcherTool] Enrichment failed (continuing): {e}")

                # Step 2c: Extract default filters and slicers from report definition
                if report_id:
                    try:
                        report_parts = await self._extract_report_definition_parts(
                            workspace_id, report_id, access_token
                        )
                        default_filters = await self._extract_default_filters(
                            workspace_id, report_id, access_token,
                            report_parts=report_parts,
                            model_context=model_context,
                        )
                        slicers = self._extract_slicers_from_report(report_parts)
                    except Exception as e:
                        logger.warning(f"[FetcherTool] Report extraction failed: {e}")

                # Step 2c.2: Fetch distinct values for slicer columns
                if slicers:
                    try:
                        await self._fetch_slicer_distinct_values(
                            workspace_id, dataset_id, access_token, slicers, model_context
                        )
                    except Exception as e:
                        logger.warning(f"[FetcherTool] Slicer distinct values failed: {e}")

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
                            slicers=slicers if report_id else None,
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

        # Merge slicer default selections into default_filters
        self._merge_slicer_defaults_into_filters(slicers, default_filters)

        # Log full model context details
        self._log_model_context_details(model_context, default_filters, slicers)

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
            "slicers": slicers,
            "summary": {
                "measure_count": len(model_context.get("measures", [])),
                "table_count": len(model_context.get("tables", [])),
                "relationship_count": len(model_context.get("relationships", [])),
                "filter_count": len(default_filters),
                "slicer_count": len(slicers),
            },
        }

        if output_format == "markdown":
            return self._format_as_markdown(output)
        return json.dumps(output, indent=2, default=str)

    def _merge_slicer_defaults_into_filters(
        self,
        slicers: List[Dict[str, Any]],
        default_filters: Dict[str, Any],
    ) -> None:
        """Merge slicer default selections into default_filters.

        If a slicer has a default_value (user input baked into the report),
        add it to default_filters so downstream tools know about it.
        Only adds if not already present in default_filters.
        """
        for s in slicers:
            default_value = s.get("default_value", "")
            if not default_value:
                continue
            table = s.get("table", "")
            column = s.get("column", "")
            if not table or not column:
                continue
            filter_key = f"{table}[{column}]"
            if filter_key not in default_filters:
                default_filters[filter_key] = default_value
                logger.info(
                    f"[Filter Merge] Added slicer default: "
                    f"{filter_key} → {default_value}"
                )

    def _log_model_context_details(
        self,
        model_context: Dict[str, Any],
        default_filters: Dict[str, Any],
        slicers: List[Dict[str, Any]],
    ) -> None:
        """Log every item in the model context line-by-line for crew.log diagnostics."""
        tag = "[Model Context]"

        # Measures
        for m in model_context.get("measures", []):
            expr = (m.get("expression") or "")[:120]
            logger.info(f"{tag} MEASURE: {m.get('table', '')}.{m['name']} = {expr}")

        # Relationships
        for r in model_context.get("relationships", []):
            logger.info(
                f"{tag} RELATIONSHIP: {r.get('fromTable', '')}.{r.get('fromColumn', '')} "
                f"→ {r.get('toTable', '')}.{r.get('toColumn', '')} "
                f"({r.get('crossFilteringBehavior', '')})"
            )

        # Tables + columns
        for t in model_context.get("tables", []):
            cols = t.get("columns", [])
            logger.info(f"{tag} TABLE: {t['name']} ({len(cols)} columns): {cols}")

        # Sample data
        for table_col, values in model_context.get("sample_data", {}).items():
            preview = str(values)[:200]
            logger.info(f"{tag} SAMPLE: {table_col} → {preview}")

        # Default filters
        for name, desc in default_filters.items():
            logger.info(f"{tag} FILTER: {name} → {desc}")

        # Slicers
        for s in slicers:
            logger.info(
                f"{tag} SLICER: {s.get('title', '')} "
                f"(Page: {s.get('page_name', '')}) → "
                f"{s.get('table', '')}[{s.get('column', '')}]"
            )

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
        lines.append(f"- **Default Filters**: {summary['filter_count']}")
        lines.append(f"- **Slicers**: {summary.get('slicer_count', 0)}\n")

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

        slicer_list = output.get("slicers", [])
        if slicer_list:
            lines.append("## Slicers\n")
            for s in slicer_list:
                title = s.get("title", s.get("visual_type", "Slicer"))
                page = s.get("page_name", "Unknown")
                table = s.get("table", "")
                column = s.get("column", "")
                lines.append(f"- **{title}** (Page: {page}): `{table}[{column}]`")
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

    async def _fetch_slicer_distinct_values(
        self, workspace_id: str, dataset_id: str, access_token: str,
        slicers: List[Dict[str, Any]], model_context: Dict[str, Any]
    ) -> None:
        """Fetch ALL distinct values for slicer columns and replace sample_data entries.

        Slicers need the full list of filterable values, not just a TOPN sample.
        Mutates model_context["sample_data"] in place.
        """
        sample_data = model_context.setdefault("sample_data", {})

        # Deduplicate slicer columns (same table+column can appear on multiple pages)
        seen: set = set()
        unique_slicer_cols: List[tuple] = []
        for s in slicers:
            table = s.get("table", "")
            column = s.get("column", "")
            if not table or not column:
                continue
            key = (table, column)
            if key not in seen:
                seen.add(key)
                unique_slicer_cols.append(key)

        logger.info(f"[Slicer Values] Fetching distinct values for {len(unique_slicer_cols)} slicer columns")

        for table, column in unique_slicer_cols:
            try:
                dax_query = f"EVALUATE DISTINCT('{table}'[{column}])"
                result = await self._execute_dax_query(workspace_id, dataset_id, access_token, dax_query)
                if result.get("success") and result.get("data"):
                    values = [list(row.values())[0] for row in result["data"]]
                    sample_key = f"{table}[{column}]"
                    old_count = len(sample_data.get(sample_key, {}).get("sample_values", []))
                    sample_data[sample_key] = {
                        "type": "slicer_values",
                        "sample_values": values,
                    }
                    logger.info(
                        f"[Slicer Values] {sample_key}: {len(values)} distinct values "
                        f"(replaced {old_count} sample values)"
                    )
                elif result.get("error"):
                    logger.warning(f"[Slicer Values] DAX error for '{table}'[{column}]: {result['error']}")
            except Exception as e:
                logger.warning(f"[Slicer Values] Exception for '{table}'[{column}]: {e}")
                continue

        logger.info(f"[Slicer Values] Done — {len(unique_slicer_cols)} slicer columns processed")

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

    async def _extract_report_definition_parts(
        self, workspace_id: str, report_id: str, access_token: str
    ) -> List[Dict[str, Any]]:
        """Fetch report definition parts from Fabric API.

        Returns the raw list of report definition parts (base64-encoded payloads).
        Handles both synchronous (200) and asynchronous (202) API responses.
        """
        logger.info(f"[Report Definition] Fetching report definition for report {report_id}")

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
                            parts = result_response.json().get("definition", {}).get("parts", [])
                            logger.info(f"[Report Definition] Got {len(parts)} parts (async)")
                            return parts
                        elif poll_data.get("status") == "Failed":
                            logger.warning("[Report Definition] Async definition request failed")
                            return []
            elif response.status_code == 200:
                parts = response.json().get("definition", {}).get("parts", [])
                logger.info(f"[Report Definition] Got {len(parts)} parts (sync)")
                return parts

        return []

    async def _extract_default_filters(
        self, workspace_id: str, report_id: str, access_token: str,
        report_parts: Optional[List[Dict[str, Any]]] = None,
        model_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Extract default filters from Power BI report definition via Fabric API.

        Args:
            workspace_id: Power BI workspace ID
            report_id: Power BI report ID
            access_token: OAuth access token
            report_parts: Pre-fetched report definition parts (avoids duplicate API call)
            model_context: Model context with column_types for datatype validation
        """
        logger.info(f"[Filter Extraction] Extracting filters from report {report_id}")

        try:
            if report_parts is None:
                report_parts = await self._extract_report_definition_parts(
                    workspace_id, report_id, access_token
                )
            return self._parse_tmdl_for_filters(report_parts, model_context=model_context)
        except Exception as e:
            logger.warning(f"[Filter Extraction] Error: {e}")
            return {}

    # =====================================================================
    # Slicer Extraction
    # =====================================================================

    def _extract_slicers_from_report(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract slicer visuals from Power BI report definition (PBIR format).

        Supports two PBIR formats:
        1. Separate files: definition/pages/{pageId}/visuals/{visualId}/visual.json
        2. Embedded in report.json: visuals inside each page's configuration
        """
        slicers: List[Dict[str, Any]] = []
        SLICER_TYPES = {
            "slicer", "listSlicer", "dateSlicer", "relativeDateSlicer",
            "advancedSlicerVisual", "chicletSlicer", "timeline",
        }


        # Step 1: Build page_id → page_name map from page.json files
        page_names: Dict[str, str] = {}
        for part in report_parts:
            path = part.get("path", "")
            path_lower = path.lower()
            if "/pages/" in path_lower and path_lower.endswith("/page.json"):
                try:
                    payload = part.get("payload", "")
                    content = base64.b64decode(payload).decode("utf-8")
                    page_data = json.loads(content)
                    path_parts = path.split("/")
                    page_id = None
                    for key in ("pages", "Pages"):
                        if key in path_parts:
                            idx = path_parts.index(key) + 1
                            page_id = path_parts[idx] if idx < len(path_parts) else None
                            break
                    if not page_id and len(path_parts) >= 2:
                        page_id = path_parts[-2]
                    if page_id:
                        display_name = page_data.get("displayName", page_data.get("name", page_id))
                        page_names[page_id] = display_name
                except Exception as e:
                    logger.debug(f"[Slicer Extraction] Error parsing page.json at {path}: {e}")

        # Step 2: Parse visual.json files (Method 1 — separate files)
        for part in report_parts:
            path = part.get("path", "")
            path_lower = path.lower()
            if "/visuals/" in path_lower and path_lower.endswith("/visual.json"):
                try:
                    payload = part.get("payload", "")
                    content = base64.b64decode(payload).decode("utf-8")
                    visual_data = json.loads(content)

                    visual_type = visual_data.get("visual", {}).get("visualType", "")
                    if visual_type.lower() not in {s.lower() for s in SLICER_TYPES}:
                        continue

                    # Extract page_id and visual_id from path
                    path_parts = path.split("/")
                    page_id = None
                    visual_id = None
                    for key in ("pages", "Pages"):
                        if key in path_parts:
                            idx = path_parts.index(key) + 1
                            page_id = path_parts[idx] if idx < len(path_parts) else None
                            break
                    for key in ("visuals", "Visuals"):
                        if key in path_parts:
                            idx = path_parts.index(key) + 1
                            visual_id = path_parts[idx] if idx < len(path_parts) else None
                            break
                    if not visual_id and len(path_parts) >= 2:
                        visual_id = path_parts[-2]

                    # Extract title
                    title = visual_data.get("visual", {}).get("title", "")
                    if not title:
                        vcobjects = visual_data.get("visual", {}).get("vcObjects", {})
                        title_obj = vcobjects.get("title", [{}])
                        if title_obj and isinstance(title_obj, list) and len(title_obj) > 0:
                            title = title_obj[0].get("properties", {}).get("text", {}).get("expr", {}).get("Literal", {}).get("Value", "")
                            if isinstance(title, str):
                                title = title.strip("'\"")

                    # Extract table/column binding from queryDefinition
                    table, column = self._extract_slicer_binding(visual_data.get("visual", {}))

                    # Extract active slicer selection
                    default_value = self._extract_slicer_selection(visual_data, visual_data.get("visual", {}))

                    slicer = {
                        "page_id": page_id,
                        "page_name": page_names.get(page_id, "Unknown") if page_id else "Unknown",
                        "visual_id": visual_id,
                        "visual_type": visual_type,
                        "title": title or visual_type,
                        "table": table,
                        "column": column,
                        "default_value": default_value,
                    }
                    slicers.append(slicer)
                    dv_str = f" [default: {default_value}]" if default_value else ""
                    logger.info(f"[Slicer Extraction] Found slicer: {slicer['title']} → {table}[{column}]{dv_str}")

                except Exception as e:
                    logger.debug(f"[Slicer Extraction] Error parsing visual.json at {path}: {e}")

        # Step 3: Fallback — parse from report.json embedded format (Method 2)
        if not slicers:
            slicers = self._extract_slicers_from_embedded_report(report_parts, SLICER_TYPES)

        logger.info(f"[Slicer Extraction] Total slicers found: {len(slicers)}")
        return slicers

    def _extract_slicers_from_embedded_report(
        self, report_parts: List[Dict[str, Any]], slicer_types: set
    ) -> List[Dict[str, Any]]:
        """Extract slicers from report.json embedded format (older PBIR)."""
        slicers: List[Dict[str, Any]] = []

        for part in report_parts:
            path = part.get("path", "")
            if not (path.lower() == "report.json" or path.lower().endswith("/report.json")):
                continue

            try:
                payload = part.get("payload", "")
                content = base64.b64decode(payload).decode("utf-8")
                report_data = json.loads(content)

                # Find pages array
                pages_data = (
                    report_data.get("pages")
                    or report_data.get("sections")
                    or report_data.get("reportPages")
                )
                if not pages_data or not isinstance(pages_data, list):
                    continue

                for page_data in pages_data:
                    if not isinstance(page_data, dict):
                        continue
                    page_name = page_data.get("displayName", page_data.get("name", "Unknown"))
                    page_id = page_data.get("name") or page_data.get("id")

                    # Find visuals within the page
                    visuals_data = page_data.get("visualContainers") or page_data.get("visuals")
                    if not visuals_data or not isinstance(visuals_data, list):
                        continue

                    for vis_idx, vis_data in enumerate(visuals_data):
                        if not isinstance(vis_data, dict):
                            continue

                        # Parse config (may be a JSON string)
                        parsed_config = {}
                        visual_type = ""
                        if "config" in vis_data:
                            config_val = vis_data["config"]
                            if isinstance(config_val, str):
                                try:
                                    parsed_config = json.loads(config_val)
                                except json.JSONDecodeError:
                                    continue
                            elif isinstance(config_val, dict):
                                parsed_config = config_val
                            visual_type = parsed_config.get("singleVisual", {}).get("visualType", "")
                        elif "visualType" in vis_data:
                            visual_type = vis_data.get("visualType", "")
                            parsed_config = vis_data
                        elif "visual" in vis_data and isinstance(vis_data["visual"], dict):
                            visual_type = vis_data["visual"].get("visualType", "")
                            parsed_config = vis_data

                        if visual_type.lower() not in {s.lower() for s in slicer_types}:
                            continue

                        visual_id = vis_data.get("name") or vis_data.get("id") or f"visual_{vis_idx}"

                        # Extract title from config
                        title = ""
                        sv = parsed_config.get("singleVisual", {})
                        vcobjects = sv.get("vcObjects", {})
                        title_obj = vcobjects.get("title", [{}])
                        if title_obj and isinstance(title_obj, list) and len(title_obj) > 0:
                            title = title_obj[0].get("properties", {}).get("text", {}).get("expr", {}).get("Literal", {}).get("Value", "")
                            if isinstance(title, str):
                                title = title.strip("'\"")

                        # Extract table/column binding
                        table, column = self._extract_slicer_binding_embedded(parsed_config)

                        # Extract active slicer selection from visual-level filters
                        default_value = self._extract_slicer_selection(vis_data, parsed_config)

                        slicer = {
                            "page_id": page_id,
                            "page_name": page_name,
                            "visual_id": visual_id,
                            "visual_type": visual_type,
                            "title": title or visual_type,
                            "table": table,
                            "column": column,
                            "default_value": default_value,
                        }
                        slicers.append(slicer)
                        dv_str = f" [default: {default_value}]" if default_value else ""
                        logger.info(f"[Slicer Extraction] Found embedded slicer: {slicer['title']} → {table}[{column}]{dv_str}")

            except Exception as e:
                logger.debug(f"[Slicer Extraction] Error parsing report.json: {e}")

        return slicers

    def _extract_slicer_selection(
        self, vis_data: Dict[str, Any], parsed_config: Dict[str, Any]
    ) -> str:
        """Extract active/default selection from a slicer visual.

        Checks the visual's filters array for any pre-set values.
        Returns a human-readable description of the selection, or empty string.
        """
        try:
            # Visual-level filters can be in vis_data["filters"] (JSON string or list)
            filters_raw = vis_data.get("filters") or parsed_config.get("filters")
            if not filters_raw:
                return ""

            if isinstance(filters_raw, str):
                try:
                    filters_list = json.loads(filters_raw)
                except json.JSONDecodeError:
                    return ""
            elif isinstance(filters_list := filters_raw, list):
                pass
            else:
                return ""

            for f in filters_list:
                if not isinstance(f, dict):
                    continue
                filter_obj = f.get("filter", {})
                where = filter_obj.get("Where", [])
                if not where:
                    continue
                condition = where[0].get("Condition", {})
                desc = self._parse_filter_condition(condition)
                if desc and desc not in ("has filter", "has complex filter"):
                    return desc
        except Exception as e:
            logger.debug(f"[Slicer Extraction] Error extracting selection: {e}")

        return ""

    def _extract_slicer_binding(self, visual: Dict[str, Any]) -> tuple:
        """Extract table and column from a PBIR separate-file visual's queryDefinition.

        Returns:
            (table_name, column_name) tuple
        """
        table = ""
        column = ""
        try:
            query_def = visual.get("queryDefinition", {})
            # Try "from" → entities
            from_items = query_def.get("from", [])
            entity_map: Dict[str, str] = {}
            for item in from_items:
                alias = item.get("name", "")
                entity = item.get("entity", "")
                if alias and entity:
                    entity_map[alias] = entity

            # Try "select" → column property
            select_items = query_def.get("select", [])
            for sel in select_items:
                col_ref = sel.get("column", sel.get("Column", {}))
                if col_ref:
                    expr = col_ref.get("expression", col_ref.get("Expression", {}))
                    source_ref = expr.get("sourceRef", expr.get("SourceRef", {}))
                    alias = source_ref.get("source", source_ref.get("Source", ""))
                    prop = col_ref.get("property", col_ref.get("Property", ""))
                    if alias and prop:
                        table = entity_map.get(alias, alias)
                        column = prop
                        return (table, column)

            # Fallback: dataTransforms.selects
            data_transforms = visual.get("dataTransforms", {})
            selects = data_transforms.get("selects", [])
            for sel in selects:
                query_ref = sel.get("queryRef", "")
                if "." in query_ref:
                    parts = query_ref.split(".", 1)
                    alias = parts[0]
                    column = parts[1]
                    table = entity_map.get(alias, alias)
                    return (table, column)
                display_name = sel.get("displayName", "")
                if display_name:
                    column = display_name
                    # Try to find table from entity_map
                    if entity_map:
                        table = next(iter(entity_map.values()), "")
                    return (table, column)

        except Exception as e:
            logger.debug(f"[Slicer Extraction] Error extracting binding: {e}")

        return (table, column)

    def _extract_slicer_binding_embedded(self, parsed_config: Dict[str, Any]) -> tuple:
        """Extract table and column from an embedded-format visual's prototypeQuery.

        Returns:
            (table_name, column_name) tuple
        """
        table = ""
        column = ""
        try:
            sv = parsed_config.get("singleVisual", {})

            # Try prototypeQuery
            proto_query = sv.get("prototypeQuery", {})
            from_items = proto_query.get("From", [])
            entity_map: Dict[str, str] = {}
            for item in from_items:
                alias = item.get("Name", "")
                entity = item.get("Entity", "")
                if alias and entity:
                    entity_map[alias] = entity

            select_items = proto_query.get("Select", [])
            for sel in select_items:
                col_ref = sel.get("Column", {})
                if col_ref:
                    expr = col_ref.get("Expression", {})
                    source_ref = expr.get("SourceRef", {})
                    alias = source_ref.get("Source", "")
                    prop = col_ref.get("Property", "")
                    if alias and prop:
                        table = entity_map.get(alias, alias)
                        column = prop
                        return (table, column)

            # Fallback: dataTransforms
            data_transforms = sv.get("dataTransforms", parsed_config.get("dataTransforms", {}))
            selects = data_transforms.get("selects", [])
            for sel in selects:
                query_ref = sel.get("queryRef", "")
                if "." in query_ref:
                    parts = query_ref.split(".", 1)
                    alias = parts[0]
                    column = parts[1]
                    table = entity_map.get(alias, alias)
                    return (table, column)
                display_name = sel.get("displayName", "")
                if display_name:
                    column = display_name
                    if entity_map:
                        table = next(iter(entity_map.values()), "")
                    return (table, column)

        except Exception as e:
            logger.debug(f"[Slicer Extraction] Error extracting embedded binding: {e}")

        return (table, column)

    # =====================================================================
    # Filter Parsing
    # =====================================================================

    # Common Power BI parameter table name patterns
    _PARAMETER_TABLE_PATTERNS = (
        "parameter", "__parameter", "param_", "_param",
        "daterange", "date range", "what-if",
    )

    def _is_parameter_table(self, table_name: str) -> bool:
        """Check if a table name looks like a Power BI parameter table."""
        lower = table_name.lower().strip()
        for pattern in self._PARAMETER_TABLE_PATTERNS:
            if pattern in lower:
                return True
        return False

    def _is_parameter_filter(self, filter_def: Dict[str, Any]) -> str:
        """Check if a filter definition is a parameter (dynamic/what-if).

        Returns:
            Empty string if not a parameter, otherwise the reason why it was identified as one.
        """
        # Method 1: Check if expression uses HierarchyLevel (parameter pattern)
        expression = filter_def.get("expression", {})
        if "HierarchyLevel" in expression:
            return "HierarchyLevel expression"

        # Method 2: Check table name against known parameter patterns
        column_expr = expression.get("Column", {})
        expr = column_expr.get("Expression", {})
        source_ref = expr.get("SourceRef", {})
        table = source_ref.get("Entity", "")
        if table and self._is_parameter_table(table):
            return f"table name matches parameter pattern ('{table}')"

        # Method 3: Check filter "type" field
        filter_type = filter_def.get("type", "")
        if filter_type in ("RelativeDate", "RelativeTime", "TopN"):
            return f"filter type is '{filter_type}'"

        return ""

    def _parse_tmdl_for_filters(
        self, tmdl_parts: List[Dict[str, Any]],
        model_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Parse report definition (PBIR format) to extract filters.

        Skips parameter/dynamic filters and validates filter value datatypes
        against column metadata when model_context is available.
        """
        filters: Dict[str, Any] = {}

        # Build column type lookup: "TableName[Column]" → data_type
        column_type_map: Dict[str, str] = {}
        if model_context:
            for table in model_context.get("tables", []):
                table_name = table.get("name", "")
                for col_name, dtype in table.get("column_types", {}).items():
                    column_type_map[f"{table_name}[{col_name}]"] = str(dtype)

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
                                # Skip parameter / dynamic filters
                                param_reason = self._is_parameter_filter(filter_def)
                                if param_reason:
                                    expression = filter_def.get("expression", {})
                                    col = expression.get("Column", {})
                                    entity = col.get("Expression", {}).get("SourceRef", {}).get("Entity", "")
                                    prop = col.get("Property", "")
                                    logger.info(
                                        f"[Filter Extraction] Skipping parameter filter: "
                                        f"{entity}[{prop}] — reason: {param_reason}"
                                    )
                                    continue

                                filter_name, filter_description = self._extract_filter_from_definition(filter_def)
                                if filter_name and filter_description:
                                    # Validate datatype if column metadata available
                                    if column_type_map and filter_name in column_type_map:
                                        filter_description = self._validate_filter_datatype(
                                            filter_name, filter_description,
                                            column_type_map[filter_name],
                                        )
                                    filters[filter_name] = filter_description
                                else:
                                    # Log unparseable filters for debugging
                                    expr_keys = list(filter_def.get("expression", {}).keys())
                                    logger.info(
                                        f"[Filter Extraction] Could not parse filter "
                                        f"(expression keys: {expr_keys}): "
                                        f"{json.dumps(filter_def, default=str)[:300]}"
                                    )
                    except json.JSONDecodeError as e:
                        logger.warning(f"[Filter Extraction] Failed to parse JSON: {e}")
        except Exception as e:
            logger.warning(f"[Filter Extraction] Error parsing report: {e}")
        return filters

    # Power BI DataType enum → human-readable name
    _PBI_DTYPE_MAP = {
        "1": "whole_number", "2": "decimal", "3": "currency",
        "4": "date", "5": "boolean", "6": "string", "7": "binary",
        "8": "datetime", "9": "time", "10": "duration",
    }

    def _validate_filter_datatype(
        self, filter_name: str, filter_description: str, column_dtype: str
    ) -> str:
        """Validate filter values against the column's actual datatype.

        If a mismatch is detected, append a warning to the description.
        """
        dtype_name = self._PBI_DTYPE_MAP.get(column_dtype, column_dtype)
        numeric_types = {"1", "2", "3"}  # whole_number, decimal, currency
        date_types = {"4", "8", "9"}     # date, datetime, time
        string_type = "6"

        # Extract literal values from filter description for validation
        # Check for quoted string values in a numeric/date column
        has_quoted = "'" in filter_description and filter_description not in ("has filter", "has complex filter")
        is_numeric_col = column_dtype in numeric_types
        is_date_col = column_dtype in date_types
        is_string_col = column_dtype == string_type

        mismatch = False
        if is_numeric_col and has_quoted:
            # Filter has string-quoted values but column is numeric
            mismatch = True
        elif is_date_col and has_quoted and not re.search(r"\d{4}-\d{2}-\d{2}", filter_description):
            # Date column but filter values don't look like dates
            mismatch = True
        elif is_string_col and re.match(r"^[><=!]+ \d+\.?\d*$", filter_description):
            # String column but filter uses numeric comparison
            mismatch = True

        if mismatch:
            logger.warning(
                f"[Filter Validation] DATATYPE MISMATCH: {filter_name} "
                f"(column type: {dtype_name}) has filter: {filter_description}"
            )
            return f"{filter_description} ⚠️ DATATYPE MISMATCH (column is {dtype_name})"

        return filter_description

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
