"""
M-Query Conversion Pipeline Tool for CrewAI

Extracts M-Query expressions from Power BI semantic models and converts them to Databricks SQL.
Uses the Power BI Admin API for extraction and LLM-powered conversion for complex expressions.
"""

import asyncio
import logging
from typing import Any, Optional, Type, Dict
from concurrent.futures import ThreadPoolExecutor

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

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

    # Service Principal authentication for Admin API (required)
    tenant_id: Optional[str] = Field(
        None,
        description="[Power BI Auth] Azure AD tenant ID for Admin API (required)"
    )
    client_id: Optional[str] = Field(
        None,
        description="[Power BI Auth] Application/Client ID for Admin API (required)"
    )
    client_secret: Optional[str] = Field(
        None,
        description="[Power BI Auth] Client secret for Admin API (required)"
    )

    # User OAuth token (alternative to Service Principal)
    access_token: Optional[str] = Field(
        None,
        description="[Power BI Auth] Pre-obtained OAuth access token (alternative to Service Principal credentials). Use this when authenticating as a user instead of Service Principal."
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
            # Admin API Authentication (required)
            "tenant_id": kwargs.get("tenant_id"),
            "client_id": kwargs.get("client_id"),
            "client_secret": kwargs.get("client_secret"),
            # User OAuth token (alternative to Service Principal)
            "access_token": kwargs.get("access_token"),
            # LLM Configuration
            "llm_workspace_url": kwargs.get("llm_workspace_url"),
            "llm_token": kwargs.get("llm_token"),
            "llm_model": kwargs.get("llm_model", "databricks-claude-sonnet-4"),
            "use_llm": kwargs.get("use_llm", True),
            # Target Configuration
            "target_catalog": kwargs.get("target_catalog", "main"),
            "target_schema": kwargs.get("target_schema", "default"),
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
            merged_kwargs = {**self._default_config, **filtered_kwargs}

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
            tenant_id = merged_kwargs.get("tenant_id")
            client_id = merged_kwargs.get("client_id")
            client_secret = merged_kwargs.get("client_secret")
            access_token = merged_kwargs.get("access_token")

            # Validate required parameters
            if not workspace_id:
                return "Error: workspace_id is required"

            # Check authentication - need either Service Principal OR access_token
            has_spn_auth = all([tenant_id, client_id, client_secret])
            has_token_auth = bool(access_token)

            if not has_spn_auth and not has_token_auth:
                return ("Error: Authentication required.\n"
                        "Provide either:\n"
                        "  - Service Principal: tenant_id + client_id + client_secret\n"
                        "  - User OAuth: access_token")

            logger.info(f"[TOOL CALL] Instance {instance_id} - Executing M-Query extraction for workspace: {workspace_id}")

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
            logger.info(f"[TOOL CALL] Creating config with auth_method: {'access_token' if has_token_auth else 'service_principal'}")

            config = MQueryConversionConfig(
                # Authentication - pass based on method
                tenant_id=tenant_id if has_spn_auth else None,
                client_id=client_id if has_spn_auth else None,
                client_secret=client_secret if has_spn_auth else None,
                access_token=access_token if has_token_auth else None,
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

            # Execute async conversion (use_llm was computed earlier)
            include_summary = merged_kwargs.get("include_summary", True)

            # Run async conversion in sync context (handles both async and sync calling contexts)
            result = run_sync(self._execute_conversion(config, use_llm))

            if not result["success"]:
                return f"Error: Conversion failed - {result.get('error', 'Unknown error')}"

            # Format output
            return self._format_output(
                result=result,
                workspace_id=workspace_id,
                dataset_id=dataset_id,
                include_summary=include_summary
            )

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
