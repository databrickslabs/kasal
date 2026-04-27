"""
M-Query Conversion Pipeline Tool for CrewAI

Extracts M-Query expressions from Power BI semantic models and converts them to Databricks SQL.
Uses the Power BI Admin API for extraction and LLM-powered conversion for complex expressions.
"""

import asyncio
import logging
from typing import Any, Optional, Type, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

from src.services.powerbi_semantic_model_cache_service import PowerBISemanticModelCacheService
from src.db.session import async_session_factory

logger = logging.getLogger(__name__)

# Thread pool executor for running async operations from sync context
_EXECUTOR = ThreadPoolExecutor(max_workers=5)


def run_sync(coro):
    """
    Run an async coroutine from a synchronous context.

    Handles both cases:
    1. When called from an async context (e.g., FastAPI) - uses ThreadPoolExecutor
    2. When called from a sync context - creates new event loop

    Args:
        coro: The coroutine to run

    Returns:
        The result of the coroutine
    """
    try:
        # Try to get the current running loop
        loop = asyncio.get_running_loop()
        # We're already in an async context, run in executor to avoid nested loop issues
        logger.debug("Detected running event loop, using ThreadPoolExecutor")
        future = _EXECUTOR.submit(asyncio.run, coro)
        return future.result()
    except RuntimeError:
        # No running loop, create a new one
        logger.debug("No running event loop, creating new loop")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


class MqueryConversionPipelineSchema(BaseModel):
    """Input schema for MqueryConversionPipelineTool."""

    # ===== POWER BI ADMIN API CONFIGURATION =====
    workspace_id: Optional[str] = Field(
        None,
        description="[Power BI] Workspace ID to scan (required)"
    )
    dataset_id: Optional[str] = Field(
        None,
        description="[Power BI] Specific dataset/semantic model ID to filter (optional, scans all if not provided)"
    )

    # Service Principal authentication for Admin API
    tenant_id: Optional[str] = Field(
        None,
        description="[Power BI Auth] Azure AD tenant ID (required for Service Principal or Service Account auth)"
    )
    client_id: Optional[str] = Field(
        None,
        description="[Power BI Auth] Application/Client ID (required for Service Principal or Service Account auth)"
    )
    client_secret: Optional[str] = Field(
        None,
        description="[Power BI Auth] Client secret (required for Service Principal auth)"
    )

    # Service Account authentication (alternative to Service Principal)
    username: Optional[str] = Field(
        None,
        description="[Power BI Auth] Service account username/UPN (for Service Account authentication)"
    )
    password: Optional[str] = Field(
        None,
        description="[Power BI Auth] Service account password (for Service Account authentication)"
    )
    auth_method: Optional[str] = Field(
        None,
        description="[Power BI Auth] Authentication method: 'service_principal', 'service_account', or auto-detect"
    )

    # User OAuth token (alternative to Service Principal/Service Account)
    access_token: Optional[str] = Field(
        None,
        description="[Power BI Auth] Pre-obtained OAuth access token (alternative to SP/Service Account credentials)."
    )

    # ===== LLM CONVERSION CONFIGURATION =====
    llm_workspace_url: Optional[str] = Field(
        None,
        description="[LLM] Databricks workspace URL for LLM conversion (optional, uses rule-based if not provided)"
    )
    llm_token: Optional[str] = Field(
        None,
        description="[LLM] Databricks API token for LLM access (optional)"
    )
    llm_model: str = Field(
        "databricks-claude-sonnet-4",
        description="[LLM] Model endpoint name for conversion (default: databricks-claude-sonnet-4)"
    )
    use_llm: bool = Field(
        True,
        description="[LLM] Whether to use LLM for complex conversions (default: True)"
    )

    # ===== TARGET CONFIGURATION =====
    target_catalog: str = Field(
        "main",
        description="[Target] Unity Catalog catalog name for generated SQL (default: 'main')"
    )
    target_schema: str = Field(
        "default",
        description="[Target] Unity Catalog schema name for generated SQL (default: 'default')"
    )

    # ===== DATA RETRIEVAL CREDENTIALS (optional — for PBI Execute Queries / EVALUATE) =====
    # The Admin API SP (above) scans M-Query but may lack Dataset.ReadWrite.All.
    # Set these to a SP or token that IS a workspace member with dataset read access.
    exec_tenant_id: Optional[str] = Field(None, description="[Data Retrieval] Azure AD tenant ID for Execute Queries SP")
    exec_client_id: Optional[str] = Field(None, description="[Data Retrieval] Client ID for Execute Queries SP")
    exec_client_secret: Optional[str] = Field(None, description="[Data Retrieval] Client secret for Execute Queries SP")
    exec_access_token: Optional[str] = Field(None, description="[Data Retrieval] Pre-obtained OAuth token with Dataset.ReadWrite.All (alternative to SP credentials)")

    # ===== DBSQL VALIDATION (optional — enables classify-first + DAX vs SQL comparison) =====
    databricks_sql_endpoint: Optional[str] = Field(
        None,
        description="[Validation] Databricks SQL endpoint URL, e.g. "
                    "https://workspace.cloud.databricks.com/api/2.0/mcp/sql. "
                    "When set together with databricks_pat, enables validation mode."
    )
    databricks_pat: Optional[str] = Field(
        None,
        description="[Validation] Databricks PAT for SQL execution and INSERT operations."
    )
    max_iterations: int = Field(
        10,
        description="[Validation] Max LLM correction iterations per table when SQL vs DAX aggregates differ (default: 10)."
    )

    # ===== SCAN OPTIONS =====
    include_lineage: bool = Field(
        True,
        description="[Scan] Include lineage information in scan (default: True)"
    )
    include_datasource_details: bool = Field(
        True,
        description="[Scan] Include data source details in scan (default: True)"
    )
    include_dataset_schema: bool = Field(
        True,
        description="[Scan] Include dataset schema in scan (default: True)"
    )
    include_dataset_expressions: bool = Field(
        True,
        description="[Scan] Include dataset expressions (M-Query) in scan (default: True)"
    )
    include_hidden_tables: bool = Field(
        False,
        description="[Scan] Include hidden tables in extraction (default: False)"
    )
    skip_static_tables: bool = Field(
        True,
        description="[Scan] Skip static tables (Table.FromRows) in conversion (default: True)"
    )

    # ===== OUTPUT OPTIONS =====
    include_summary: bool = Field(
        True,
        description="[Output] Include summary report in output (default: True)"
    )


class MqueryConversionPipelineTool(BaseTool):
    """
    M-Query to Databricks SQL Conversion Pipeline.

    Extracts M-Query expressions from Power BI semantic models using the Admin API
    and converts them to Databricks SQL. Supports both rule-based conversion for
    simple expressions and LLM-powered conversion for complex transformations.

    **Capabilities**:
    - Scan Power BI workspaces via Admin API
    - Extract M-Query expressions from semantic models
    - Parse Value.NativeQuery, DatabricksMultiCloud.Catalogs, Sql.Database, etc.
    - Convert M-Query transformations to SQL equivalents
    - Generate CREATE VIEW statements for Unity Catalog

    **Note**: For relationship extraction, use the dedicated Power BI Relationships Tool
    which uses INFO.VIEW.RELATIONSHIPS() and supports workspace member Service Principals.

    **Expression Types Supported**:
    - **Native Query**: SQL embedded in Value.NativeQuery
    - **Databricks Catalog**: Direct Databricks catalog connections
    - **SQL Database**: SQL Server/Azure SQL connections
    - **Table.FromRows**: Static data tables (skippable)
    - **ODBC**: ODBC data source connections
    - **Oracle/Snowflake**: Other database connections

    **Example Use Cases**:
    1. Migrate Power BI data models to Databricks
    2. Extract SQL logic from Power BI for documentation
    3. Generate Unity Catalog views from Power BI tables
    4. Analyze M-Query transformations for migration planning

    **Configuration**:
    - Configure Power BI Admin API credentials (Service Principal)
    - Optionally configure LLM for complex conversions
    - Set target Unity Catalog location
    - Execute extraction and conversion
    """

    name: str = "M-Query Conversion Pipeline"
    description: str = (
        "M-Query to Databricks SQL conversion pipeline - PRE-CONFIGURED. "
        "IMPORTANT: All credentials are pre-configured. DO NOT provide workspace_id, tenant_id, client_id, or client_secret. "
        "Simply call this tool with NO PARAMETERS to execute the extraction and conversion. "
        "The tool will use the pre-configured Power BI Admin API credentials to scan the workspace, "
        "extract M-Query expressions, and convert them to Databricks SQL CREATE VIEW statements."
    )
    args_schema: Type[BaseModel] = MqueryConversionPipelineSchema

    # Private attributes (not part of schema)
    _instance_id: str = PrivateAttr()
    _default_config: Dict[str, Any] = PrivateAttr()

    # Allow extra attributes for config
    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the M-Query Conversion Pipeline tool."""
        import uuid
        instance_id = str(uuid.uuid4())[:8]

        logger.info(f"[MqueryConversionPipelineTool.__init__] Instance ID: {instance_id}")
        logger.info(f"[MqueryConversionPipelineTool.__init__] Received kwargs keys: {list(kwargs.keys())}")
        logger.info(f"[MqueryConversionPipelineTool.__init__] workspace_id: {kwargs.get('workspace_id', 'NOT PROVIDED')}")

        # Extract execution_inputs if provided (for dynamic parameter resolution)
        execution_inputs = kwargs.get("execution_inputs", {})

        # Store config values temporarily
        default_config = {
            # Power BI Admin API
            "workspace_id": kwargs.get("workspace_id"),
            "dataset_id": kwargs.get("dataset_id"),
            # Admin API Authentication
            "tenant_id": kwargs.get("tenant_id"),
            "client_id": kwargs.get("client_id"),
            "client_secret": kwargs.get("client_secret"),
            # Service Account authentication
            "username": kwargs.get("username"),
            "password": kwargs.get("password"),
            "auth_method": kwargs.get("auth_method"),
            # User OAuth token (alternative to Service Principal/Service Account)
            "access_token": kwargs.get("access_token"),
            # LLM Configuration
            "llm_workspace_url": kwargs.get("llm_workspace_url"),
            "llm_token": kwargs.get("llm_token"),
            "llm_model": kwargs.get("llm_model", "databricks-claude-sonnet-4"),
            "use_llm": kwargs.get("use_llm", True),
            # Target Configuration
            "target_catalog": kwargs.get("target_catalog", "main"),
            "target_schema": kwargs.get("target_schema", "default"),
            # Data retrieval credentials (for Execute Queries / EVALUATE)
            "exec_tenant_id": kwargs.get("exec_tenant_id"),
            "exec_client_id": kwargs.get("exec_client_id"),
            "exec_client_secret": kwargs.get("exec_client_secret"),
            "exec_access_token": kwargs.get("exec_access_token"),
            # DBSQL validation (optional)
            "databricks_sql_endpoint": kwargs.get("databricks_sql_endpoint"),
            "databricks_pat": kwargs.get("databricks_pat"),
            "max_iterations": kwargs.get("max_iterations", 10),
            # Scan Options
            "include_lineage": kwargs.get("include_lineage", True),
            "include_datasource_details": kwargs.get("include_datasource_details", True),
            "include_dataset_schema": kwargs.get("include_dataset_schema", True),
            "include_dataset_expressions": kwargs.get("include_dataset_expressions", True),
            "include_hidden_tables": kwargs.get("include_hidden_tables", False),
            "skip_static_tables": kwargs.get("skip_static_tables", True),
            # Output Options
            "include_summary": kwargs.get("include_summary", True),
        }

        # DYNAMIC PARAMETER RESOLUTION: If execution_inputs provided, resolve placeholders during init
        if execution_inputs:
            logger.info(f"[MqueryConversionPipelineTool.__init__] Instance {instance_id} - Resolving parameters from execution_inputs: {list(execution_inputs.keys())}")
            resolved_config = {}
            for key, value in default_config.items():
                if isinstance(value, str) and '{' in value:
                    import re
                    placeholders = re.findall(r'\{(\w+)\}', value)
                    if placeholders:
                        resolved_value = value
                        for placeholder in placeholders:
                            if placeholder in execution_inputs:
                                replacement = str(execution_inputs[placeholder])
                                resolved_value = resolved_value.replace(f'{{{placeholder}}}', replacement)
                                logger.info(f"[INIT RESOLUTION] Resolved {key}: {{{placeholder}}} → {replacement}")
                        resolved_config[key] = resolved_value
                    else:
                        resolved_config[key] = value
                else:
                    resolved_config[key] = value
            default_config = resolved_config

        # Call parent __init__ with filtered kwargs
        tool_kwargs = {k: v for k, v in kwargs.items() if k not in default_config}
        super().__init__(**tool_kwargs)

        # Set private attributes AFTER super().__init__()
        self._instance_id = instance_id
        self._default_config = default_config

        logger.info(f"[MqueryConversionPipelineTool.__init__] Instance {instance_id} initialized with config keys: {list(default_config.keys())}")

    def _resolve_parameter(self, value: Any, execution_inputs: Dict[str, Any]) -> Any:
        """
        Resolve parameter placeholders in configuration values.

        Args:
            value: Configuration value (may contain {placeholders})
            execution_inputs: Dictionary of execution input values

        Returns:
            Resolved value with placeholders replaced
        """
        if not isinstance(value, str):
            return value

        import re
        placeholders = re.findall(r'\{(\w+)\}', value)

        if not placeholders:
            return value

        resolved_value = value
        for placeholder in placeholders:
            if placeholder in execution_inputs:
                replacement = str(execution_inputs[placeholder])
                resolved_value = resolved_value.replace(f'{{{placeholder}}}', replacement)
                logger.info(f"[PARAM RESOLUTION] Resolved {{{placeholder}}} → {replacement}")
            else:
                logger.warning(f"[PARAM RESOLUTION] Placeholder {{{placeholder}}} not found in execution_inputs")

        return resolved_value

    def _run(self, **kwargs: Any) -> str:
        """
        Execute M-Query conversion pipeline.

        Args:
            workspace_id: Power BI workspace ID to scan
            dataset_id: Optional specific dataset ID
            tenant_id: Azure AD tenant ID
            client_id: Application client ID
            client_secret: Client secret
            [additional parameters from schema]

        Returns:
            Formatted output with converted SQL and metadata
        """
        try:
            instance_id = getattr(self, '_instance_id', 'UNKNOWN')
            logger.info(f"[TOOL CALL] Instance {instance_id} - _run() called")
            logger.info(f"[TOOL CALL] Instance {instance_id} - Received kwargs: {list(kwargs.keys())}")

            # Extract execution_inputs if provided
            execution_inputs = kwargs.pop('execution_inputs', {})
            logger.info(f"[TOOL CALL] Instance {instance_id} - Execution inputs: {list(execution_inputs.keys())}")

            # Merge agent-provided kwargs with pre-configured defaults
            # Filter out None values AND placeholder-like strings that agents often generate
            def is_placeholder(value: Any) -> bool:
                """Check if a value looks like an agent-generated placeholder."""
                if not isinstance(value, str):
                    return False
                placeholder_patterns = [
                    "your_", "your-", "<your", "[your",
                    "placeholder", "example_", "example-",
                    "xxx", "yyy", "zzz",
                    "insert_", "enter_", "put_",
                    "replace_", "fill_in",
                ]
                value_lower = value.lower()
                return any(pattern in value_lower for pattern in placeholder_patterns)

            filtered_kwargs = {
                k: v for k, v in kwargs.items()
                if v is not None and not is_placeholder(v)
            }
            logger.info(f"[TOOL CALL] Instance {instance_id} - Filtered kwargs (removed None/placeholders): {list(filtered_kwargs.keys())}")
            logger.info(f"[TOOL CALL] Instance {instance_id} - Pre-configured defaults: workspace_id={self._default_config.get('workspace_id', 'NOT SET')[:20] if self._default_config.get('workspace_id') else 'NOT SET'}...")

            # Pre-configured values take precedence over agent-provided placeholders
            # IMPORTANT: default_config must be second to override filtered_kwargs
            merged_kwargs = {**filtered_kwargs, **self._default_config}

            # DYNAMIC PARAMETER RESOLUTION
            if execution_inputs:
                logger.info(f"[PARAM RESOLUTION] Resolving parameters with execution_inputs")
                resolved_kwargs = {}
                for key, value in merged_kwargs.items():
                    resolved_kwargs[key] = self._resolve_parameter(value, execution_inputs)
                merged_kwargs = resolved_kwargs

            # Extract parameters
            workspace_id = merged_kwargs.get("workspace_id")
            dataset_id = merged_kwargs.get("dataset_id")

            # Build auth config for validation
            auth_config = {
                "tenant_id": merged_kwargs.get("tenant_id"),
                "client_id": merged_kwargs.get("client_id"),
                "client_secret": merged_kwargs.get("client_secret"),
                "username": merged_kwargs.get("username"),
                "password": merged_kwargs.get("password"),
                "auth_method": merged_kwargs.get("auth_method"),
                "access_token": merged_kwargs.get("access_token"),
            }

            # DEBUG: Log auth config being used
            logger.info("=" * 80)
            logger.info("[MQueryConversionPipelineTool] AUTH CONFIG DEBUG")
            logger.info("=" * 80)
            logger.info(f"  tenant_id: {auth_config.get('tenant_id')}")
            logger.info(f"  client_id: {auth_config.get('client_id')}")
            logger.info(f"  client_secret: {'*' * len(auth_config.get('client_secret') or '') if auth_config.get('client_secret') else 'None'}")
            logger.info(f"  username: {auth_config.get('username')}")
            logger.info(f"  password: {'*' * len(auth_config.get('password') or '') if auth_config.get('password') else 'None'}")
            logger.info(f"  auth_method: {auth_config.get('auth_method')} (type: {type(auth_config.get('auth_method'))})")
            logger.info(f"  access_token: {'*' * 10 if auth_config.get('access_token') else 'None'}")
            logger.info("=" * 80)

            # Validate required parameters
            if not workspace_id:
                return "Error: workspace_id is required"

            # Validate authentication using shared utility
            from src.engines.crewai.tools.custom.powerbi_auth_utils import validate_auth_config
            is_valid, error_msg = validate_auth_config(auth_config)
            if not is_valid:
                return f"Error: {error_msg}"

            logger.info(f"[TOOL CALL] Instance {instance_id} - Executing M-Query extraction for workspace: {workspace_id}")

            # ── Same-day cache check ─────────────────────────────────────────
            _cache_dataset_key = workspace_id + ("__" + dataset_id if dataset_id else "__all")
            try:
                cached_output = run_sync(self._get_mquery_cache(_cache_dataset_key, workspace_id))
                if cached_output:
                    logger.info(f"[CACHE HIT] M-Query conversion for workspace {workspace_id} — returning cached result")
                    return cached_output
                logger.info(f"[CACHE MISS] Running fresh M-Query conversion for workspace {workspace_id}")
            except Exception as _cache_err:
                logger.warning(f"[Cache] Cache check failed (continuing without cache): {_cache_err}")
            # ────────────────────────────────────────────────────────────────

            # ── DBSQL validation path ────────────────────────────────────────
            # If databricks_warehouse_id + databricks_pat are configured, use the
            # classify-first validation flow (no LLM for non-Databricks tables,
            # DAX vs SQL comparison, static EVALUATE+INSERT).
            _sql_endpoint = merged_kwargs.get("databricks_sql_endpoint")
            _dbsql_pat = merged_kwargs.get("databricks_pat")
            if _sql_endpoint and _dbsql_pat:
                logger.info(f"[TOOL CALL] DBSQL validation configured — running classify-first validation flow")
                output = run_sync(self._execute_with_validation(merged_kwargs))
                # Cache the validation output too
                try:
                    run_sync(self._save_mquery_cache(_cache_dataset_key, workspace_id, {
                        "formatted_output": output,
                        "workspace_id": workspace_id,
                        "dataset_id": dataset_id,
                        "validation": True,
                    }))
                except Exception as _ce:
                    logger.warning(f"[Cache] Failed to save validation output: {_ce}")
                return output
            # ────────────────────────────────────────────────────────────────

            # Import M-Query converter (lazy import to avoid circular dependencies)
            from src.converters.services.mquery import MQueryConnector, MQueryConversionConfig

            # Get LLM configuration - auto-detect from environment if not provided
            llm_workspace_url = merged_kwargs.get("llm_workspace_url")
            llm_token = merged_kwargs.get("llm_token")

            # Auto-detect Databricks credentials for LLM if not provided
            if not llm_workspace_url or not llm_token:
                import os
                # Try to get from environment
                env_workspace_url = os.environ.get("DATABRICKS_HOST") or os.environ.get("DATABRICKS_WORKSPACE_URL")
                env_token = os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_API_KEY")

                if env_workspace_url and env_token:
                    llm_workspace_url = llm_workspace_url or env_workspace_url
                    llm_token = llm_token or env_token
                    logger.info(f"[TOOL CALL] Auto-detected Databricks credentials for LLM from environment")
                else:
                    logger.warning(f"[TOOL CALL] LLM credentials not provided and not found in environment. Complex M-Query expressions may not convert properly.")

            # Ensure workspace URL has https:// prefix
            if llm_workspace_url and not llm_workspace_url.startswith("http"):
                llm_workspace_url = f"https://{llm_workspace_url}"

            use_llm = merged_kwargs.get("use_llm", True) and bool(llm_workspace_url) and bool(llm_token)
            logger.info(f"[TOOL CALL] LLM conversion enabled: {use_llm}")

            # Create configuration based on authentication method
            has_token_auth = bool(auth_config.get("access_token"))
            logger.info(f"[TOOL CALL] Creating config with auth_method: {'access_token' if has_token_auth else 'service_principal/service_account'}")

            config = MQueryConversionConfig(
                # Authentication - pass all auth params, let the config handle it
                tenant_id=auth_config.get("tenant_id"),
                client_id=auth_config.get("client_id"),
                client_secret=auth_config.get("client_secret"),
                username=auth_config.get("username"),
                password=auth_config.get("password"),
                auth_method=auth_config.get("auth_method"),
                access_token=auth_config.get("access_token"),
                # Required
                workspace_id=workspace_id,
                dataset_id=dataset_id,
                # LLM Configuration
                llm_workspace_url=llm_workspace_url if llm_workspace_url else None,
                llm_token=llm_token if llm_token else None,
                llm_model=merged_kwargs.get("llm_model", "databricks-claude-sonnet-4"),
                # Target Configuration
                target_catalog=merged_kwargs.get("target_catalog", "main"),
                target_schema=merged_kwargs.get("target_schema", "default"),
                # Scan Options
                include_lineage=merged_kwargs.get("include_lineage", True),
                include_datasource_details=merged_kwargs.get("include_datasource_details", True),
                include_dataset_schema=merged_kwargs.get("include_dataset_schema", True),
                include_dataset_expressions=merged_kwargs.get("include_dataset_expressions", True),
                include_hidden_tables=merged_kwargs.get("include_hidden_tables", False),
                skip_static_tables=merged_kwargs.get("skip_static_tables", True),
            )

            # Log which authentication method will be used (auto-detect if not specified)
            detected_auth_method = auth_config.get("auth_method")
            if not detected_auth_method:
                # Replicate AadService._determine_auth_method() logic
                if auth_config.get("access_token"):
                    detected_auth_method = "user_oauth (pre-obtained token)"
                elif (auth_config.get("client_id") and auth_config.get("client_secret") and
                      auth_config.get("tenant_id")):
                    detected_auth_method = "service_principal (auto-detected)"
                elif (auth_config.get("username") and auth_config.get("password") and
                      auth_config.get("client_id") and auth_config.get("tenant_id")):
                    detected_auth_method = "service_account (auto-detected)"
                else:
                    detected_auth_method = "UNKNOWN - insufficient credentials"

            logger.info("=" * 80)
            logger.info("[MQueryConversionPipelineTool] 🔑 AUTHENTICATION METHOD DETECTION")
            logger.info("=" * 80)
            logger.info(f"  Detected auth method: {detected_auth_method}")
            logger.info("=" * 80)

            # Execute async conversion (use_llm was computed earlier)
            include_summary = merged_kwargs.get("include_summary", True)

            # Run async conversion in sync context (handles both async and sync calling contexts)
            result = run_sync(self._execute_conversion(config, use_llm))

            if not result["success"]:
                return f"Error: Conversion failed - {result.get('error', 'Unknown error')}"

            # Format output
            formatted_output = self._format_output(
                result=result,
                workspace_id=workspace_id,
                dataset_id=dataset_id,
                include_summary=include_summary
            )

            # ── Save to same-day cache ───────────────────────────────────────
            try:
                run_sync(self._save_mquery_cache(_cache_dataset_key, workspace_id, {
                    "formatted_output": formatted_output,
                    "workspace_id": workspace_id,
                    "dataset_id": dataset_id,
                    "model_count": result.get("model_count", 0),
                }))
                logger.info(f"[CACHE SAVED] M-Query conversion cached for workspace {workspace_id}")
            except Exception as _save_err:
                logger.warning(f"[Cache] Failed to save M-Query cache: {_save_err}")
            # ────────────────────────────────────────────────────────────────

            return formatted_output

        except Exception as e:
            logger.error(f"M-Query Conversion Pipeline error: {str(e)}", exc_info=True)
            return f"Error: {str(e)}"

    async def _execute_conversion(
        self,
        config: Any,  # MQueryConversionConfig type
        use_llm: bool
    ) -> Dict[str, Any]:
        """Execute the async M-Query conversion."""
        try:
            # Import M-Query converter
            from src.converters.services.mquery import MQueryConnector

            async with MQueryConnector(config) as connector:
                # Scan workspace
                models = await connector.scan_workspace()

                if not models:
                    return {
                        "success": False,
                        "error": "No semantic models found in workspace"
                    }

                # Convert all tables
                all_results = {}
                for model in models:
                    results = await connector.convert_all_tables(model, use_llm=use_llm)
                    all_results[model.name] = {
                        "tables": results
                    }

                # Generate summary
                summary = connector.generate_summary_report()

                return {
                    "success": True,
                    "models": all_results,
                    "summary": summary,
                    "model_count": len(models)
                }

        except Exception as e:
            logger.error(f"Conversion execution error: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }

    # ── Cache helpers ────────────────────────────────────────────────────────

    _CACHE_GROUP = "mquery_conversion"

    async def _get_mquery_cache(self, dataset_key: str, workspace_id: str) -> Optional[str]:
        """Return today's cached formatted output, or None on miss."""
        async with async_session_factory() as session:
            svc = PowerBISemanticModelCacheService(session)
            cached = await svc.get_cached_metadata(
                group_id=self._CACHE_GROUP,
                dataset_id=dataset_key,
                workspace_id=workspace_id,
                report_id=None,
            )
        if cached and "formatted_output" in cached:
            return str(cached["formatted_output"])
        return None

    async def _save_mquery_cache(self, dataset_key: str, workspace_id: str, metadata: Dict[str, Any]) -> None:
        """Persist the conversion result so same-day reruns are instant."""
        async with async_session_factory() as session:
            svc = PowerBISemanticModelCacheService(session)
            await svc.save_metadata(
                group_id=self._CACHE_GROUP,
                dataset_id=dataset_key,
                workspace_id=workspace_id,
                metadata=metadata,
                report_id=None,
            )

    # ─────────────────────────────────────────────────────────────────────────

    def _format_output(
        self,
        result: Dict[str, Any],
        workspace_id: str,
        dataset_id: Optional[str],
        include_summary: bool
    ) -> str:
        """Format the conversion output."""
        output = []
        output.append("# M-Query to Databricks SQL Conversion Results\n")
        output.append(f"**Workspace**: {workspace_id}")
        if dataset_id:
            output.append(f"**Dataset Filter**: {dataset_id}")
        output.append(f"**Models Processed**: {result.get('model_count', 0)}\n")

        # Process each model
        for model_name, model_data in result.get("models", {}).items():
            output.append(f"\n## Semantic Model: {model_name}\n")

            # Process tables
            tables = model_data.get("tables", {})
            output.append(f"### Tables ({len(tables)} converted)\n")

            for table_name, conversions in tables.items():
                output.append(f"\n#### {table_name}\n")

                for conv in conversions:
                    if conv.success:
                        output.append(f"**Expression Type**: {conv.expression_type.value}\n")

                        # Show original M-Query expression if available
                        if conv.original_expression:
                            # Truncate very long expressions for readability
                            orig_expr = conv.original_expression
                            if len(orig_expr) > 500:
                                orig_expr = orig_expr[:500] + "..."
                            output.append("**Original M-Query**:")
                            output.append("```powerquery")
                            output.append(orig_expr)
                            output.append("```\n")

                        # Show converted SQL
                        output.append("**Converted SQL**:")
                        if conv.create_view_sql:
                            output.append("```sql")
                            output.append(conv.create_view_sql)
                            output.append("```\n")
                        elif conv.databricks_sql:
                            output.append("```sql")
                            output.append(conv.databricks_sql)
                            output.append("```\n")

                        if conv.parameters:
                            output.append("**Parameters**:")
                            for param in conv.parameters:
                                output.append(f"  - {param.get('name', 'unknown')}: {param.get('type', 'STRING')}")

                        if conv.notes:
                            output.append(f"\n*{conv.notes}*\n")
                    else:
                        output.append(f"**Status**: Failed - {conv.error_message}\n")

        # Add summary
        if include_summary:
            summary = result.get("summary", {})
            if summary and "error" not in summary:
                output.append("\n## Summary Report\n")
                output.append(f"- **Total Tables**: {summary.get('total_tables', 0)}")
                output.append(f"- **Total Measures**: {summary.get('total_measures', 0)}")
                output.append(f"- **Relationships**: {summary.get('relationships_count', 0)}")

                expr_types = summary.get("expression_types", {})
                if expr_types:
                    output.append("\n**Expression Types**:")
                    for expr_type, count in expr_types.items():
                        output.append(f"  - {expr_type}: {count}")

        return "\n".join(output)

    # =========================================================================
    # VALIDATION PATH — classify-first, DAX vs SQL comparison, static INSERT
    # Triggered only when databricks_warehouse_id + databricks_pat are set.
    # Core conversion methods (_execute_conversion, _format_output) are untouched.
    # =========================================================================

    # ── Source classification ─────────────────────────────────────────────────

    _DATABRICKS_KW = [
        "databricksmulticloud.catalogs", "databricks.catalogs",
        "value.nativequery", "spark.databricks",
    ]
    _STATIC_KW = [
        "excel.workbook", "json.document", "csv.document", "yaml.document",
        "xml.document", "web.page", "table.fromrows", "table.fromrecords",
        "table.fromvalue", "table.fromlist",
    ]
    _SKIP_REASONS: Dict[str, str] = {
        "sql_database": "Azure SQL / Synapse — use Lakehouse Federation or ETL",
        "odbc":         "ODBC connection — no direct Databricks equivalent",
        "oracle":       "Oracle Database — use Federation or ETL migration",
        "snowflake":    "Snowflake — use Lakehouse Federation or Delta Sharing",
        "other":        "Source type not recognised — manual rewrite required",
    }

    def _classify_table(self, expr_type_str: str, raw_expr: str) -> str:
        """Return 'databricks' | 'static' | 'non_transpilable'."""
        et = expr_type_str.lower()
        orig = raw_expr.lower()
        if et in ("native_query", "databricks_catalog") or any(k in orig for k in self._DATABRICKS_KW):
            return "databricks"
        if et == "table_from_rows" or any(k in orig for k in self._STATIC_KW):
            return "static"
        return "non_transpilable"

    # ── Main validation entry point ───────────────────────────────────────────

    async def _execute_with_validation(self, cfg: Dict[str, Any]) -> str:
        """
        Classify-first validation flow:
          Databricks  → LLM convert → compare SQL vs DAX → iterate with LLM fix
          Static      → EVALUATE <TableName> via PBI → CREATE TABLE + INSERT on DBSQL
          Other       → flag as NOT TRANSPILABLE (no LLM called)
        """
        from src.converters.services.mquery import MQueryConnector, MQueryConversionConfig
        from src.engines.crewai.tools.custom.powerbi_auth_utils import get_powerbi_access_token

        workspace_id = cfg.get("workspace_id", "")
        dataset_id = cfg.get("dataset_id")
        sql_endpoint = cfg.get("databricks_sql_endpoint", "")
        pat = cfg.get("databricks_pat", "")
        # Resolve workspace URL + warehouse from the endpoint URL
        try:
            workspace_url, warehouse_id = await self._resolve_dbsql(sql_endpoint, pat)
        except Exception as e:
            return f"Error resolving DBSQL connection from endpoint '{sql_endpoint}': {e}"
        target = f"{cfg.get('target_catalog','main')}.{cfg.get('target_schema','default')}"
        max_iter = int(cfg.get("max_iterations", 10))

        # PBI auth token for Admin API (scan / convert)
        try:
            pbi_token = await get_powerbi_access_token(
                tenant_id=cfg.get("tenant_id"),
                client_id=cfg.get("client_id"),
                client_secret=cfg.get("client_secret"),
                access_token=cfg.get("access_token"),
                username=cfg.get("username"),
                password=cfg.get("password"),
                auth_method=cfg.get("auth_method"),
            )
        except Exception as e:
            return f"Error obtaining PBI access token: {e}"

        # Separate Execute Queries token (Dataset.ReadWrite.All) — falls back to main token
        exec_token = pbi_token
        if cfg.get("exec_access_token") or cfg.get("exec_client_id"):
            try:
                exec_token = await get_powerbi_access_token(
                    tenant_id=cfg.get("exec_tenant_id") or cfg.get("tenant_id"),
                    client_id=cfg.get("exec_client_id"),
                    client_secret=cfg.get("exec_client_secret"),
                    access_token=cfg.get("exec_access_token"),
                    auth_method="service_principal" if cfg.get("exec_client_id") else None,
                )
                logger.info("[Validation] Using separate data retrieval credentials for Execute Queries")
            except Exception as e:
                logger.warning(f"[Validation] Data retrieval token failed, falling back to main token: {e}")
                exec_token = pbi_token

        mq_cfg = MQueryConversionConfig(
            tenant_id=cfg.get("tenant_id"),
            client_id=cfg.get("client_id"),
            client_secret=cfg.get("client_secret"),
            username=cfg.get("username"),
            password=cfg.get("password"),
            auth_method=cfg.get("auth_method"),
            access_token=cfg.get("access_token"),
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            llm_model=cfg.get("llm_model", "databricks-claude-sonnet-4"),
            llm_workspace_url=cfg.get("llm_workspace_url"),
            llm_token=cfg.get("llm_token"),
            target_catalog=cfg.get("target_catalog", "main"),
            target_schema=cfg.get("target_schema", "default"),
            include_hidden_tables=cfg.get("include_hidden_tables", False),
            skip_static_tables=False,
        )
        use_llm = bool(cfg.get("use_llm", True)) and bool(cfg.get("llm_workspace_url")) and bool(cfg.get("llm_token"))

        validated: list = []
        inserted: list = []
        skipped: list = []

        try:
            async with MQueryConnector(mq_cfg) as connector:
                models = await connector.scan_workspace()
                if not models:
                    return "No semantic models found in workspace."

                for model in models:
                    for table in model.tables:
                        tname = table.name
                        raw_expr = ""
                        expr_type_str = "other"
                        if table.source_expressions:
                            se = table.source_expressions[0]
                            raw_expr = getattr(se, "raw_expression", "") or ""
                            et = getattr(se, "expression_type", None)
                            expr_type_str = (et.value if hasattr(et, "value") else str(et)) if et else "other"

                        lane = self._classify_table(expr_type_str, raw_expr)

                        if lane == "databricks":
                            logger.info(f"[Validation] Databricks table: {tname}")
                            try:
                                convs = await connector.convert_table(table, use_llm=use_llm)
                            except Exception as ce:
                                validated.append({"table": tname, "status": "conversion_error", "error": str(ce)})
                                continue
                            for conv in convs:
                                r = await self._validate_databricks_table(
                                    tname, conv, workspace_id, dataset_id, exec_token,
                                    workspace_url, warehouse_id, pat, max_iter, cfg
                                )
                                validated.append(r)

                        elif lane == "static":
                            logger.info(f"[Validation] Static table: {tname}")
                            r = await self._insert_static_table(
                                tname, workspace_id, dataset_id, exec_token,
                                warehouse_id, pat, workspace_url, target, cfg
                            )
                            inserted.append(r)

                        else:
                            skipped.append({
                                "table": tname, "model": model.name,
                                "source_type": expr_type_str,
                                "reason": self._SKIP_REASONS.get(expr_type_str, "Manual rewrite required"),
                                "mquery_preview": raw_expr[:300],
                            })

        except Exception as e:
            logger.error(f"[Validation] Error: {e}", exc_info=True)
            return f"Error during validation: {e}"

        return self._format_validation_report(validated, inserted, skipped, target)

    # ── Databricks table: convert + validate ──────────────────────────────────

    async def _validate_databricks_table(
        self, tname: str, conv: Any, workspace_id: str, dataset_id: Optional[str],
        pbi_token: str, workspace_url: str, warehouse_id: str, pat: str,
        max_iter: int, cfg: Dict[str, Any]
    ) -> Dict[str, Any]:
        sql = conv.databricks_sql or conv.create_view_sql or ""
        if not sql:
            return {"table": tname, "status": "skipped", "reason": "No SQL generated"}
        if not (warehouse_id and pat):
            return {"table": tname, "status": "skipped", "reason": "DBSQL not configured"}

        # Extract SELECT body from CREATE VIEW wrapper
        select_sql = self._extract_select_body(sql, cfg, tname)

        dax = f'EVALUATE ROW("_cnt", COUNTROWS({tname}))'
        last_diff = None
        last_missing_table: Optional[str] = None  # track TABLE_OR_VIEW_NOT_FOUND repeats

        for iteration in range(max_iter):
            agg_sql = self._build_count_sql(select_sql)

            # Log SQL and DAX so progress is visible in crew.log
            logger.info(f"[Validation] [{tname}] iter={iteration+1} SQL:\n{agg_sql}")
            logger.info(f"[Validation] [{tname}] iter={iteration+1} DAX:\n{dax}")

            sql_res = await self._run_dbsql(agg_sql, workspace_url, warehouse_id, pat)
            if not sql_res["success"]:
                sql_error = sql_res["error"]
                logger.warning(f"[Validation] [{tname}] DBSQL error: {sql_error}")

                # TABLE_OR_VIEW_NOT_FOUND — try LLM correction once, but if the same
                # missing table recurs on the next iteration the table simply doesn't
                # exist and LLM cannot fix it → give up immediately.
                import re as _tvre
                _tvm = _tvre.search(r'`([^`]+)`\.`([^`]+)`', sql_error)
                _missing = _tvm.group(0) if _tvm else sql_error[:80]
                if "TABLE_OR_VIEW_NOT_FOUND" in str(sql_error):
                    if _missing == last_missing_table:
                        logger.warning(f"[Validation] [{tname}] same missing table on 2 consecutive iterations — giving up")
                        return {"table": tname, "status": "table_not_found",
                                "error": sql_error, "sql": select_sql, "dax": dax}
                    last_missing_table = _missing

                # SQL syntax/runtime error — try LLM correction
                if iteration < max_iter - 1:
                    fixed = await self._llm_correct_sql(
                        select_sql, f"SQL execution error: {sql_error}",
                        getattr(conv, "original_expression", "") or "", cfg
                    )
                    if fixed and fixed.strip() != select_sql.strip():
                        select_sql = fixed
                        continue
                return {"table": tname, "status": "dbsql_error", "error": sql_error,
                        "sql": select_sql, "dax": dax}

            dax_res = await self._run_pbi_query(dax, workspace_id, dataset_id or "", pbi_token)
            if not dax_res["success"]:
                logger.warning(f"[Validation] [{tname}] DAX error: {dax_res.get('error')}")
                return {"table": tname, "status": "dax_error", "error": dax_res.get("error"),
                        "sql": select_sql, "dax": dax}

            diff = self._diff_counts(sql_res, dax_res)
            if diff is None:
                logger.info(f"[Validation] [{tname}] ✅ VERIFIED after {iteration+1} iteration(s)")
                return {"table": tname, "status": "validated", "iterations": iteration + 1,
                        "sql": select_sql, "dax": dax}

            logger.warning(f"[Validation] [{tname}] ❌ diff detected: {diff}")
            last_diff = diff
            if iteration < max_iter - 1:
                fixed = await self._llm_correct_sql(
                    select_sql, diff, getattr(conv, "original_expression", "") or "", cfg
                )
                if fixed and fixed.strip() != select_sql.strip():
                    select_sql = fixed
                else:
                    break

        logger.warning(f"[Validation] [{tname}] validation_failed after {max_iter} iterations — diff: {last_diff}")
        return {"table": tname, "status": "validation_failed", "iterations": max_iter,
                "diff": last_diff, "sql": select_sql, "dax": dax}

    def _build_count_sql(self, select_sql: str) -> str:
        """
        Build a COUNT(*) wrapper that works for both plain SELECTs and CTE queries.

        Spark SQL does not support nested CTEs inside subqueries:
          BAD:  SELECT COUNT(*) FROM (WITH cte AS (...) SELECT ...) _t
          GOOD: WITH cte AS (...) SELECT COUNT(*) FROM (SELECT ...) _t

        For CTE queries, keep the WITH block at the top level and wrap only
        the final SELECT in the count subquery.
        """
        import re as _re
        stripped = select_sql.strip()
        if not stripped.upper().startswith("WITH"):
            return f"SELECT COUNT(*) AS _cnt FROM ({stripped}) _t"

        # Find the last top-level SELECT (depth 0 — not inside any parentheses)
        depth = 0
        last_top_select = -1
        upper = stripped.upper()
        i = 0
        while i < len(stripped):
            ch = stripped[i]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif depth == 0 and upper[i:i + 7] == "SELECT ":
                last_top_select = i
            i += 1

        if last_top_select == -1:
            # Fallback: can't parse, try wrapping as-is
            return f"SELECT COUNT(*) AS _cnt FROM ({stripped}) _t"

        cte_part = stripped[:last_top_select]
        final_select = stripped[last_top_select:].strip()

        # Remove trailing ORDER BY / LIMIT — not needed for COUNT
        final_select = _re.sub(r'\s+ORDER\s+BY\s+.*$', '', final_select, flags=_re.IGNORECASE | _re.DOTALL)
        final_select = _re.sub(r'\s+LIMIT\s+\S+.*$', '', final_select, flags=_re.IGNORECASE)

        return f"{cte_part}SELECT COUNT(*) AS _cnt FROM ({final_select.strip()}) _cnt_agg"

    def _extract_select_body(self, sql: str, config: Dict[str, Any], table_name: str) -> str:
        """
        Robustly extract the SELECT body from a SQL string that may or may not
        be wrapped in CREATE VIEW ... AS <body>.

        Fixes the max() bug in the old approach which found the LAST ' AS '
        in the SQL body (e.g. 'fiscyear AS Year') instead of the CREATE VIEW boundary.
        """
        import re as _re
        stripped = sql.strip()
        upper = stripped.upper()

        # Already a bare SELECT or subquery / CTE
        if upper.startswith("SELECT") or upper.startswith("(") or upper.startswith("WITH"):
            return stripped

        # CREATE VIEW ... AS <body>
        if "CREATE" in upper and "VIEW" in upper:
            # Match up to and including the AS keyword at the end of the view header
            m = _re.search(
                r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?\S+\s+AS\s*',
                stripped, _re.IGNORECASE
            )
            if m:
                body = stripped[m.end():].strip()
                # Body should now start with SELECT, (, or WITH
                if body.upper().startswith(("SELECT", "(", "WITH")):
                    return body
                # Body starts with column list (LLM forgot SELECT keyword) — add it
                if "FROM" in body.upper():
                    return f"SELECT {body}"
                return body

        # No CREATE VIEW wrapper but column list present (LLM skipped SELECT)
        if "FROM" in upper and not upper.startswith("SELECT"):
            return f"SELECT {stripped}"

        target = f"{config.get('target_catalog','main')}.{config.get('target_schema','default')}.{table_name}"
        return f"SELECT * FROM {target}"

    def _diff_counts(self, sql_res: Dict, dax_res: Dict) -> Optional[str]:
        """Return None if counts match within 0.1%, else a diff string."""
        try:
            sql_cnt = int(sql_res.get("rows", [[0]])[0][0])
            dax_rows = dax_res.get("data", [])
            dax_cnt = None
            if dax_rows:
                row = dax_rows[0]
                for v in (row.values() if isinstance(row, dict) else row):
                    try:
                        dax_cnt = int(float(str(v))); break
                    except (ValueError, TypeError):
                        pass
            if dax_cnt is None or (sql_cnt == 0 and dax_cnt == 0):
                return None
            rel = abs(sql_cnt - dax_cnt) / max(dax_cnt, 1)
            return None if rel < 0.001 else f"SQL={sql_cnt}, DAX={dax_cnt} (delta={rel:.1%})"
        except Exception:
            return None

    async def _llm_correct_sql(self, sql: str, diff: str, mquery: str, cfg: Dict) -> Optional[str]:
        """Ask LLM to fix SQL given the row-count diff vs DAX."""
        import re as _re
        try:
            from crewai import LLM
            model = cfg.get("llm_model", "databricks-claude-sonnet-4")
            workspace_url = cfg.get("llm_workspace_url") or ""
            llm_token = cfg.get("llm_token") or ""
            if not workspace_url or not llm_token:
                import os
                workspace_url = workspace_url or os.environ.get("DATABRICKS_HOST") or os.environ.get("DATABRICKS_WORKSPACE_URL", "")
                llm_token = llm_token or os.environ.get("DATABRICKS_TOKEN") or os.environ.get("DATABRICKS_API_KEY", "")
            if workspace_url and not workspace_url.startswith("http"):
                workspace_url = f"https://{workspace_url}"
            llm = LLM(
                model=model,
                base_url=f"{workspace_url}/serving-endpoints" if workspace_url else None,
                api_key=llm_token or None,
                max_tokens=2000,
            )
            prompt = (
                f"You are a Databricks SQL expert. This SQL was transpiled from a Power BI M-Query "
                f"but the row count does not match: {diff}\n\n"
                f"The SQL produces MORE rows than Power BI — this almost always means a missing "
                f"deduplication step. Look carefully at the M-Query for:\n"
                f"- Table.Distinct() → add SELECT DISTINCT\n"
                f"- Table.Group() → add GROUP BY\n"
                f"- Table.SelectRows() with complex conditions → tighten WHERE\n"
                f"- Table.FirstN() / Table.Last() → add LIMIT or ROW_NUMBER filter\n\n"
                f"Original M-Query (source of truth — the SQL must match its row count):\n"
                f"{mquery[:1000]}\n\n"
                f"Current SQL (produces wrong row count):\n{sql}\n\n"
                f"Return ONLY the corrected SQL SELECT statement that faithfully implements "
                f"the M-Query transformation. No explanation."
            )
            response = llm.call([{"role": "user", "content": prompt}])
            if isinstance(response, str):
                m = _re.search(r"```(?:sql)?\s*(.*?)```", response, _re.DOTALL | _re.IGNORECASE)
                return m.group(1).strip() if m else response.strip()
        except Exception as e:
            logger.warning(f"[Validation] LLM SQL fix failed: {e}")
        return None

    # ── Static table: EVALUATE → CREATE TABLE + INSERT ────────────────────────

    async def _insert_static_table(
        self, tname: str, workspace_id: str, dataset_id: Optional[str],
        pbi_token: str, warehouse_id: str, pat: str, workspace_url: str,
        target_prefix: str, cfg: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        if not (warehouse_id and pat):
            return {"table": tname, "status": "skipped", "reason": "DBSQL not configured"}

        # 1. Fetch all rows from PBI via EVALUATE (DAX)
        pbi = await self._run_pbi_query(
            f"EVALUATE {tname}", workspace_id, dataset_id or "", pbi_token
        )
        if not pbi["success"]:
            return {"table": tname, "status": "pbi_error", "error": pbi.get("error")}

        rows = pbi.get("data", [])
        columns = pbi.get("columns", [])
        if not columns:
            return {"table": tname, "status": "empty", "rows_inserted": 0}

        # Clean PBI column names: "[Table].[Column]" → "Column"
        clean_cols = [c.split("].[")[-1].rstrip("]").lstrip("[") for c in columns]
        safe_name = tname.replace(" ", "_").lower()
        full_target = f"{target_prefix}.{safe_name}"

        # 2. Ask LLM to generate CREATE TABLE + INSERT based on the actual data
        create_sql, insert_sqls = await self._llm_generate_insert_sql(
            tname, full_target, clean_cols, rows, cfg or {}
        )

        # 3. Execute CREATE TABLE
        cr = await self._run_dbsql(create_sql, workspace_url, warehouse_id, pat)
        if not cr["success"]:
            return {"table": tname, "status": "create_failed", "error": cr["error"], "sql": create_sql}

        if not insert_sqls:
            return {"table": tname, "status": "inserted", "target": full_target,
                    "rows_inserted": 0, "sql": create_sql}

        # 4. Execute INSERT statements in batches
        total = 0
        for ins in insert_sqls:
            ir = await self._run_dbsql(ins, workspace_url, warehouse_id, pat)
            if ir["success"]:
                total += ins.count("),(") + 1
            else:
                logger.warning(f"[Validation] Batch insert error for {tname}: {ir['error']}")

        return {
            "table": tname, "status": "inserted", "target": full_target,
            "rows_inserted": total,
            "sql": "\n\n".join([create_sql] + insert_sqls[:2]) + (" ..." if len(insert_sqls) > 2 else ""),
        }

    async def _llm_generate_insert_sql(
        self, tname: str, full_target: str, columns: List[str],
        rows: list, cfg: Dict[str, Any]
    ) -> Tuple[str, List[str]]:
        """
        Use LLM to generate CREATE TABLE + INSERT statements from PBI data.
        The LLM sees the actual data and infers proper types (DATE, DOUBLE, etc.).
        Falls back to mechanical generation if LLM is unavailable.
        """
        import json as _json
        import re as _re

        sample = rows[:20]  # first 20 rows for type inference
        sample_json = _json.dumps(sample, default=str)[:3000]  # cap payload size

        # Prepare batch INSERT chunks (mechanical, LLM only does schema + types)
        # LLM generates the CREATE TABLE; we do the INSERT VALUES mechanically
        # but with the correct types from the LLM-generated schema.
        try:
            from crewai import LLM
            model = cfg.get("llm_model", "databricks-claude-sonnet-4")
            workspace_url = cfg.get("llm_workspace_url") or ""
            llm_token = cfg.get("llm_token") or ""
            if not workspace_url or not llm_token:
                import os as _os
                workspace_url = workspace_url or _os.environ.get("DATABRICKS_HOST") or _os.environ.get("DATABRICKS_WORKSPACE_URL", "")
                llm_token = llm_token or _os.environ.get("DATABRICKS_TOKEN") or _os.environ.get("DATABRICKS_API_KEY", "")
            if workspace_url and not workspace_url.startswith("http"):
                workspace_url = f"https://{workspace_url}"
            llm = LLM(
                model=model,
                base_url=f"{workspace_url}/serving-endpoints" if workspace_url else None,
                api_key=llm_token or None,
                max_tokens=1500,
            )
            prompt = (
                f"You are a Databricks SQL expert. A Power BI table called '{tname}' "
                f"has been fetched via EVALUATE and needs to be inserted into Delta table `{full_target}`.\n\n"
                f"Columns: {columns}\n\n"
                f"Sample data (first 20 rows as JSON):\n{sample_json}\n\n"
                f"Generate ONLY a CREATE TABLE IF NOT EXISTS statement for `{full_target}` "
                f"with correct Spark SQL types (STRING, BIGINT, DOUBLE, DATE, TIMESTAMP, BOOLEAN). "
                f"Infer types from the sample data. Return only the SQL, no explanation."
            )
            response = llm.call([{"role": "user", "content": prompt}])
            if isinstance(response, str):
                m = _re.search(r"```(?:sql)?\s*(.*?)```", response, _re.DOTALL | _re.IGNORECASE)
                create_sql = m.group(1).strip() if m else response.strip()
                # Extract column types from LLM output for mechanical INSERT generation
                type_map = self._parse_types_from_create(create_sql, columns)
            else:
                raise ValueError("LLM returned non-string response")
        except Exception as e:
            logger.warning(f"[Validation] LLM schema generation failed for {tname}, using fallback: {e}")
            type_map = self._infer_schema_types(columns, rows[:10])
            schema_def = ", ".join(f"`{c}` {t}" for c, t in type_map.items())
            create_sql = f"CREATE TABLE IF NOT EXISTS {full_target} ({schema_def})"

        # Generate INSERT VALUES batches mechanically using the inferred types
        col_list = ", ".join(f"`{c}`" for c in columns)
        insert_sqls = []
        for i in range(0, len(rows), 500):
            batch = rows[i:i + 500]
            vals = []
            for row in batch:
                items = list(row.values()) if isinstance(row, dict) else list(row)
                escaped = []
                for j, v in enumerate(items):
                    col = columns[j] if j < len(columns) else f"col{j}"
                    col_type = type_map.get(col, "STRING").upper()
                    if v is None:
                        escaped.append("NULL")
                    elif "BIGINT" in col_type or "INT" in col_type:
                        try:
                            escaped.append(str(int(float(str(v)))))
                        except (ValueError, TypeError):
                            escaped.append("NULL")
                    elif "DOUBLE" in col_type or "FLOAT" in col_type or "DECIMAL" in col_type:
                        try:
                            escaped.append(str(float(str(v))))
                        except (ValueError, TypeError):
                            escaped.append("NULL")
                    elif "BOOLEAN" in col_type:
                        escaped.append("TRUE" if str(v).lower() in ("true", "1", "yes") else "FALSE")
                    elif "DATE" in col_type and "TIME" not in col_type:
                        escaped.append(f"DATE '{str(v)[:10]}'")
                    elif "TIMESTAMP" in col_type:
                        escaped.append(f"TIMESTAMP '{v}'")
                    else:
                        escaped.append("'" + str(v).replace("'", "''") + "'")
                vals.append(f"({', '.join(escaped)})")
            if vals:
                insert_sqls.append(f"INSERT INTO {full_target} ({col_list}) VALUES {', '.join(vals)}")

        return create_sql, insert_sqls

    def _parse_types_from_create(self, create_sql: str, columns: List[str]) -> Dict[str, str]:
        """Extract column→type mapping from a CREATE TABLE SQL string."""
        import re as _re
        type_map: Dict[str, str] = {}
        # Match backtick or plain column names followed by a type
        for col in columns:
            pat = rf"`?{_re.escape(col)}`?\s+(\w+(?:\s*\(\d+(?:,\s*\d+)?\))?)"
            m = _re.search(pat, create_sql, _re.IGNORECASE)
            type_map[col] = m.group(1).upper() if m else "STRING"
        return type_map

    def _infer_schema_types(self, cols: list, sample_rows: list) -> Dict[str, str]:
        """Fallback: infer types mechanically from sample values."""
        import re as _re
        types: Dict[str, str] = {c: "STRING" for c in cols}
        for row in sample_rows:
            vals = list(row.values()) if isinstance(row, dict) else list(row)
            for i, v in enumerate(vals):
                if i >= len(cols) or v is None or types[cols[i]] != "STRING":
                    continue
                try:
                    int(v); types[cols[i]] = "BIGINT"; continue
                except (ValueError, TypeError):
                    pass
                try:
                    float(v); types[cols[i]] = "DOUBLE"; continue
                except (ValueError, TypeError):
                    pass
                if _re.match(r"^\d{4}-\d{2}-\d{2}T", str(v)):
                    types[cols[i]] = "TIMESTAMP"
                elif _re.match(r"^\d{4}-\d{2}-\d{2}$", str(v)):
                    types[cols[i]] = "DATE"
        return types

    def _infer_schema(self, cols: list, sample_rows: list) -> str:
        import re as _re
        types: Dict[str, str] = {c: "STRING" for c in cols}
        for row in sample_rows:
            vals = list(row.values()) if isinstance(row, dict) else list(row)
            for i, v in enumerate(vals):
                if i >= len(cols) or v is None or types[cols[i]] != "STRING":
                    continue
                try:
                    int(v); types[cols[i]] = "BIGINT"; continue
                except (ValueError, TypeError):
                    pass
                try:
                    float(v); types[cols[i]] = "DOUBLE"; continue
                except (ValueError, TypeError):
                    pass
                if _re.match(r"^\d{4}-\d{2}-\d{2}", str(v)):
                    types[cols[i]] = "DATE"
        return ", ".join(f"`{c}` {t}" for c, t in types.items())

    # ── DBSQL connection resolver ─────────────────────────────────────────────

    async def _resolve_dbsql(self, sql_endpoint: str, pat: str) -> Tuple[str, str]:
        """
        Extract workspace URL and warehouse ID from a SQL endpoint URL.

        Accepts formats like:
          https://workspace.cloud.databricks.com/api/2.0/mcp/sql
          https://workspace.cloud.databricks.com/sql/1.0/warehouses/{id}
          https://workspace.cloud.databricks.com   (bare workspace URL)

        If warehouse_id cannot be parsed from the URL, auto-detects the best
        running SQL warehouse via the warehouses API.
        """
        import re as _re
        from urllib.parse import urlparse
        import httpx as _httpx

        parsed = urlparse(sql_endpoint)
        workspace_url = f"{parsed.scheme}://{parsed.netloc}"

        # Try to parse warehouse ID from URL path
        m = _re.search(r"/warehouses/([a-f0-9]+)", parsed.path)
        if m:
            return workspace_url, m.group(1)

        # Auto-detect: list warehouses and pick best running one
        headers = {"Authorization": f"Bearer {pat}"}
        async with _httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(f"{workspace_url}/api/2.0/sql/warehouses", headers=headers)
            resp.raise_for_status()
            warehouses = resp.json().get("warehouses", [])

        if not warehouses:
            raise ValueError("No SQL warehouses found in workspace")

        # Prefer RUNNING, then STOPPED (can be started), smallest first
        running = [w for w in warehouses if w.get("state") == "RUNNING"]
        if running:
            return workspace_url, running[0]["id"]
        return workspace_url, warehouses[0]["id"]

    # ── DBSQL Statement API ───────────────────────────────────────────────────

    async def _run_dbsql(self, sql: str, workspace_url: str, warehouse_id: str, pat: str) -> Dict[str, Any]:
        import httpx as _httpx
        base = workspace_url.rstrip("/")
        if not base:
            return {"success": False, "error": "databricks_workspace_url not configured — set llm_workspace_url"}
        url = f"{base}/api/2.0/sql/statements"
        headers = {"Authorization": f"Bearer {pat}", "Content-Type": "application/json"}
        payload = {"statement": sql, "warehouse_id": warehouse_id, "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"}
        try:
            async with _httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(url, headers=headers, json=payload)
                resp.raise_for_status()
                data = resp.json()
                sid = data.get("statement_id")
                state = data.get("status", {}).get("state", "")
                polls = 0
                while state in ("RUNNING", "PENDING") and polls < 30:
                    await asyncio.sleep(2)
                    pr = await client.get(f"{url}/{sid}", headers=headers)
                    data = pr.json(); state = data.get("status", {}).get("state", ""); polls += 1
                if state == "SUCCEEDED":
                    result = data.get("result", {})
                    manifest = data.get("manifest", {})
                    cols = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
                    return {"success": True, "columns": cols, "rows": result.get("data_array", [])}
                err = data.get("status", {}).get("error", {})
                return {"success": False, "error": err.get("message", f"State: {state}")}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── PBI Execute Queries API ───────────────────────────────────────────────

    async def _run_pbi_query(self, dax: str, workspace_id: str, dataset_id: str, token: str) -> Dict[str, Any]:
        import httpx as _httpx
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
        try:
            async with _httpx.AsyncClient(timeout=120.0) as client:
                resp = await client.post(url, headers=headers, json=payload)
                resp.raise_for_status()
                data = resp.json()
                if "error" in data:
                    return {"success": False, "error": data["error"].get("message", str(data["error"]))}
                tables = data.get("results", [{}])[0].get("tables", [])
                if tables:
                    rows = tables[0].get("rows", [])
                    return {"success": True, "data": rows, "columns": list(rows[0].keys()) if rows else []}
                return {"success": True, "data": [], "columns": []}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── Validation report ─────────────────────────────────────────────────────

    def _format_validation_report(
        self, validated: list, inserted: list, skipped: list, target: str
    ) -> str:
        lines = ["# M-Query Conversion & Validation Report\n"]
        lines.append(f"**Target**: `{target}`")
        lines.append(
            f"**Summary**: {len(validated)} Databricks tables validated | "
            f"{len(inserted)} static tables inserted | "
            f"{len(skipped)} flagged as non-transpilable\n"
        )

        if validated:
            lines.append("---\n## Databricks Tables — SQL vs DAX Validation\n")
            for r in validated:
                status = r.get("status", "unknown")
                icon = "✅" if status == "validated" else ("🔷" if status == "table_not_found" else ("⚠️" if status == "validation_failed" else "❌"))
                badge = {
                    "validated": "**✅ VERIFIED**",
                    "table_not_found": "**🔷 TABLE NOT MIGRATED YET** — SQL correct but target table missing",
                    "validation_failed": "**⚠️ VALIDATION FAILED** — count mismatch after max iterations",
                }.get(status, f"**❌ {status.upper()}**")
                lines.append(f"### {icon} `{r['table']}` — {badge}")
                if r.get("iterations"):
                    lines.append(f"- Iterations: {r['iterations']}")
                if r.get("diff"):
                    lines.append(f"- **Row count diff**: {r['diff']}")
                if r.get("error"):
                    lines.append(f"- **Error**: {r['error']}")
                if r.get("reason"):
                    lines.append(f"- Note: {r['reason']}")
                if r.get("sql"):
                    preview = r["sql"][:400] + ("..." if len(r["sql"]) > 400 else "")
                    lines.append(f"\n**Translated SQL (M-Query → Databricks SQL):**\n```sql\n{preview}\n```")
                if r.get("dax"):
                    lines.append(f"\n**DAX executed on PBI:**\n```dax\n{r['dax']}\n```")
                lines.append("")

        if inserted:
            lines.append("---\n## Static Tables — Inserted into Delta\n")
            for r in inserted:
                icon = "✅" if r.get("status") == "inserted" else "❌"
                lines.append(f"### {icon} `{r['table']}` → `{r.get('target','')}`")
                if r.get("rows_inserted") is not None:
                    lines.append(f"- Rows inserted: {r['rows_inserted']}")
                if r.get("error"):
                    lines.append(f"- **Error**: {r['error']}")
                if r.get("sql"):
                    preview = r["sql"][:400] + ("..." if len(r["sql"]) > 400 else "")
                    lines.append(f"\n**Generated SQL (CREATE TABLE + INSERT):**\n```sql\n{preview}\n```")
                lines.append("")

        if skipped:
            lines.append("---\n## 🚫 Non-Transpilable — Manual Review Required\n")
            for r in skipped:
                lines.append(f"### `{r['table']}` — `{r.get('source_type','?')}`")
                lines.append(f"- {r.get('reason','')}")
                if r.get("mquery_preview"):
                    lines.append(f"\n```powerquery\n{r['mquery_preview']}\n```")
                lines.append("")

        return "\n".join(lines)
