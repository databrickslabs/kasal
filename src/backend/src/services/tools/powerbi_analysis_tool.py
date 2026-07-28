"""
Power BI Analysis Tool for CrewAI

Orchestrates Power BI model analysis and DAX query execution:
1. Calls Measure Conversion Pipeline to extract measures and model context
2. Uses LLM to generate intelligent DAX based on user questions
3. Executes DAX queries via Power BI Execute Queries API
4. Searches for visual references in reports

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

from src.services.tools.base import BaseTool
from pydantic import BaseModel, Field, PrivateAttr
import httpx

from src.services.tools.tool_session_provider import ToolSessionProvider
from src.services.tools.powerbi_analysis_utils import (
    PowerBIModelFetchMixin,
    PowerBITmdlParsingMixin,
    PowerBIDaxGenerationMixin,
    PowerBIDaxFilterMixin,
    PowerBISemanticContextMixin,
    PowerBIReportReferenceMixin,
    PowerBIOutputMixin,
)


logger = logging.getLogger(__name__)

# Ensure logger level is set to DEBUG to capture all DAX Generation logs
logger.setLevel(logging.DEBUG)

# Thread pool executor for running async operations from sync context
_EXECUTOR = ThreadPoolExecutor(max_workers=5)


def _run_async_in_sync_context(coro):
    """
    Safely run an async coroutine from a synchronous context.
    Handles nested event loop scenarios (e.g., FastAPI).
    Propagates contextvars (like execution_id) to worker threads.
    """
    try:
        loop = asyncio.get_running_loop()
        # Copy the current context to propagate to the worker thread
        ctx = contextvars.copy_context()
        # Run asyncio.run in the copied context
        future = _EXECUTOR.submit(ctx.run, asyncio.run, coro)
        return future.result()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


class PowerBIAnalysisSchema(BaseModel):
    """Input schema for PowerBIAnalysisTool."""

    # ===== USER QUESTION =====
    user_question: Optional[str] = Field(
        None,
        description="The business question to answer using Power BI data. This should come from the task description or be pre-configured in tool_configs."
    )

    # NOTE: connection / auth / LLM plumbing (workspace_id, dataset_id,
    # report_id, tenant_id, client_id, client_secret, username, password,
    # auth_method, access_token, llm_*) is deliberately NOT part of this
    # schema. Those values are injected at tool-construction time from
    # tool_configs (see __init__/_default_config) — exposing them as
    # LLM-fillable parameters added ~KBs of schema to every LLM call and
    # invited the model to hallucinate or echo credentials into transcripts.

    # ===== CONTEXT ENRICHMENT (Microsoft Copilot-style) =====
    business_mappings: Optional[Dict[str, str]] = Field(
        None,
        description="[Context] Business terminology mappings - natural language to DAX expressions. Example: {'Complete CGR': \"[Initial_Sizing][description] = 'Complete CGR'\", 'Italian BU': \"[Initial_Sizing][BU] = 'Italy'\"}"
    )
    field_synonyms: Optional[Dict[str, List[str]]] = Field(
        None,
        description="[Context] Field synonyms for natural language understanding. Example: {'num_customers': ['number of customers', 'customer count', 'total customers'], 'BU': ['business unit', 'region']}"
    )
    active_filters: Optional[Dict[str, Any]] = Field(
        None,
        description="[Context] Currently active filters/slicers that should be automatically applied. Example: {'BU': 'Italy', 'Week': 1, 'Mandatory_Version': ['Landline', 'Mobile']}"
    )
    session_id: Optional[str] = Field(
        None,
        description="[Context] Session ID for tracking conversation history and maintaining context across queries."
    )
    visible_tables: Optional[List[str]] = Field(
        None,
        description="[Context] Tables currently visible/in use (simulates page-level context). Example: ['Initial_Sizing', 'Customer_Details']"
    )
    conversation_history: Optional[List[Dict[str, str]]] = Field(
        None,
        description="[Context] Previous questions and answers in this session. Example: [{'question': 'What is total revenue?', 'answer': '1.5M', 'filters_used': {'BU': 'Italy'}}]"
    )

    # ===== OPTIONS =====
    include_visual_references: bool = Field(
        True,
        description="[Options] Search for visual references after DAX execution."
    )
    skip_system_tables: bool = Field(
        True,
        description="[Options] Skip system tables like LocalDateTable."
    )
    max_dax_retries: int = Field(
        5,
        description="[Options] Maximum number of retry attempts if DAX execution fails (1-10)."
    )
    output_format: str = Field(
        "markdown",
        description="[Output] Output format: 'markdown' or 'json'."
    )
    enable_info_columns: bool = Field(
        False,
        description="[Options] Enable INFO.COLUMNS() metadata enrichment (requires DMV permissions). Default False - most environments don't support this."
    )


class PowerBIAnalysisTool(
    PowerBIModelFetchMixin,
    PowerBITmdlParsingMixin,
    PowerBIDaxGenerationMixin,
    PowerBIDaxFilterMixin,
    PowerBISemanticContextMixin,
    PowerBIReportReferenceMixin,
    PowerBIOutputMixin,
    BaseTool,
):
    """
    Power BI Analysis Tool - Microsoft Copilot-Style Question-to-DAX-to-Results Pipeline.

    **🚀 NEW: Enhanced Context Enrichment**:
    This tool now includes advanced context awareness for simplified natural language queries:
    - **Business Term Mappings**: Translate natural language to DAX expressions
      Example: "Complete CGR" → "[Initial_Sizing][description] = 'Complete CGR'"
    - **Field Synonyms**: Understand alternative names for fields
      Example: "number of customers" → [num_customers]
    - **Active Filters**: Auto-apply current view state filters (implicit context)
      Example: BU=Italy, Week=1 applied automatically even if not mentioned
    - **Session Context**: Use conversation history for context
    - **Sample Values**: Understand data patterns for better query generation

    **Flow**:
    1. **Extract Model Context**: Fetches measures, relationships, column metadata from semantic model
    2. **Enrich Context**: Adds business mappings, synonyms, active filters, conversation history
    3. **Generate DAX**: Uses LLM with enriched context to convert simple questions into accurate DAX
    4. **Execute DAX**: Runs the query via Power BI Execute Queries API with retry logic
    5. **Find Visual References**: Identifies which reports/visuals use the queried measures

    **Authentication** (choose one):
    - **Service Principal**: client_id + client_secret + tenant_id (App Owns Data)
    - **Service Account**: username + password + client_id + tenant_id (User credentials)
    - **User OAuth**: access_token (pre-obtained token)

    **Context Enrichment Parameters** (optional but recommended):
    - **business_mappings**: Dict mapping natural language terms to DAX filter expressions
    - **field_synonyms**: Dict mapping field names to alternative names/synonyms
    - **active_filters**: Dict of currently active filters (auto-applied to queries)
    - **session_id**: Session ID for tracking conversation history
    - **visible_tables**: List of currently visible tables (page-level context)
    - **conversation_history**: Previous Q&A for context awareness

    **Use Cases**:
    - Answer business questions using Power BI data with simple natural language
    - Automatically apply implicit filters from view state (like Microsoft Copilot)
    - Generate and validate DAX queries with business terminology understanding
    - Understand measure usage across reports and pages
    """

    name: str = "Power BI Intelligent Analysis (Copilot-Style)"
    description: str = (
        "Analyzes Power BI data by converting SIMPLE natural language questions into DAX queries using enriched context. "
        "ENHANCED: Now supports Microsoft Copilot-style context enrichment - business term mappings, field synonyms, "
        "active filters, and session context for simplified queries. "
        "IMPORTANT: Extract the user's business question from the task description and pass it as 'user_question' parameter. "
        "Optional: Provide business_mappings, field_synonyms, and active_filters for enhanced natural language understanding. "
        "The tool will: 1) Extract Power BI model context, 2) Enrich with metadata, 3) Generate accurate DAX using LLM, "
        "4) Execute the query with auto-retry, 5) Return results. "
        "Connection credentials (workspace_id, dataset_id, authentication) are pre-configured - do not provide them unless overriding."
    )
    args_schema: Type[BaseModel] = PowerBIAnalysisSchema

    # Private attributes
    _instance_id: str = PrivateAttr()
    _default_config: Dict[str, Any] = PrivateAttr()

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the Analysis tool."""
        import uuid
        instance_id = str(uuid.uuid4())[:8]

        logger.info(f"[PowerBIAnalysisTool.__init__] Instance ID: {instance_id}")
        logger.info(f"[PowerBIAnalysisTool.__init__] Received user_question in kwargs: {kwargs.get('user_question', 'NOT PROVIDED')}")

        # Store configuration
        default_config = {
            "workspace_id": kwargs.get("workspace_id"),
            "dataset_id": kwargs.get("dataset_id"),
            "report_id": kwargs.get("report_id"),  # Optional: for auto-extracting default filters
            "tenant_id": kwargs.get("tenant_id"),
            "client_id": kwargs.get("client_id"),
            "client_secret": kwargs.get("client_secret"),
            "username": kwargs.get("username"),
            "password": kwargs.get("password"),
            "auth_method": kwargs.get("auth_method"),
            "access_token": kwargs.get("access_token"),
            "llm_workspace_url": kwargs.get("llm_workspace_url"),
            "llm_token": kwargs.get("llm_token"),
            "llm_model": kwargs.get("llm_model", "databricks-claude-sonnet-4-5"),
            "include_visual_references": kwargs.get("include_visual_references", True),
            "skip_system_tables": kwargs.get("skip_system_tables", True),
            "max_dax_retries": kwargs.get("max_dax_retries", 5),
            "output_format": kwargs.get("output_format", "markdown"),
            "enable_info_columns": kwargs.get("enable_info_columns", False),  # Disabled by default
            "user_question": kwargs.get("user_question"),  # Pre-configured question from frontend
            # Context enrichment fields (Microsoft Copilot-style)
            "business_mappings": kwargs.get("business_mappings", {}),
            "field_synonyms": kwargs.get("field_synonyms", {}),
            "active_filters": kwargs.get("active_filters", {}),
            "session_id": kwargs.get("session_id"),
            "visible_tables": kwargs.get("visible_tables", []),
            "conversation_history": kwargs.get("conversation_history", []),
        }

        # Call parent init
        tool_kwargs = {k: v for k, v in kwargs.items() if k not in default_config}
        super().__init__(**tool_kwargs)

        self._instance_id = instance_id
        self._default_config = default_config

        logger.info(f"[PowerBIAnalysisTool.__init__] Stored in default_config - user_question: {default_config.get('user_question', 'NOT SET')}")

    def _is_placeholder_value(self, value: Any) -> bool:
        """Check if a value looks like a placeholder/example that should be ignored."""
        if not isinstance(value, str):
            return False

        # Common placeholder patterns
        placeholder_patterns = [
            # UUID-like placeholders (12345678-1234-1234-1234-123456789012)
            r'^[0-9]{8}-[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{12}$',
            # Explicit placeholder strings
            r'your_.*_here',
            r'your-.*-here',
            r'<.*>',
            r'\{.*\}',
            r'placeholder',
            r'example\.com',
            r'^https://your-',
            r'^https://.*-url\.com$',
        ]

        import re
        value_lower = value.lower()
        for pattern in placeholder_patterns:
            if re.search(pattern, value_lower):
                return True

        return False

    def _run(self, **kwargs: Any) -> str:
        """Execute the Power BI analysis pipeline."""
        try:
            instance_id = getattr(self, '_instance_id', 'UNKNOWN')
            logger.info(f"[PowerBIAnalysisTool] Instance {instance_id} - _run() called")
            logger.info(f"[PowerBIAnalysisTool] Default config keys: {list(self._default_config.keys())}")
            logger.info(f"[PowerBIAnalysisTool] Runtime kwargs keys: {list(kwargs.keys())}")

            # Filter out placeholder/example values from kwargs
            filtered_kwargs = {}
            for k, v in kwargs.items():
                if v is not None and not self._is_placeholder_value(v):
                    filtered_kwargs[k] = v
                elif self._is_placeholder_value(v):
                    logger.info(f"[PowerBIAnalysisTool] Ignoring placeholder value for '{k}': {v[:30] if isinstance(v, str) else v}...")

            # Merge configurations:
            # - For user_question: prefer kwargs (the actual question from the agent)
            # - For auth/connection params: prefer default config (pre-configured values)
            # - For options: prefer kwargs if provided, else default config
            merged_config = {}

            # Connection and auth parameters - default config takes precedence
            config_params = ["workspace_id", "dataset_id", "report_id", "tenant_id", "client_id",
                           "client_secret", "username", "password", "auth_method",
                           "access_token", "llm_workspace_url", "llm_token", "llm_model"]
            for key in config_params:
                default_val = self._default_config.get(key)
                kwarg_val = filtered_kwargs.get(key)
                # Use default config if available, otherwise use kwargs
                merged_config[key] = default_val if default_val is not None else kwarg_val

            # User question - prefer default config (pre-configured) over agent's input
            # This ensures the tool_configs question takes precedence
            kwarg_question = filtered_kwargs.get("user_question")
            default_question = self._default_config.get("user_question")
            merged_config["user_question"] = default_question if default_question is not None else kwarg_question

            # Options - prefer kwargs if provided
            for key in ["include_visual_references", "skip_system_tables", "max_dax_retries", "output_format", "enable_info_columns"]:
                kwarg_val = filtered_kwargs.get(key)
                default_val = self._default_config.get(key)
                merged_config[key] = kwarg_val if kwarg_val is not None else default_val

            # Context enrichment parameters (Microsoft Copilot-style)
            # These can come as JSON strings from frontend or as dicts
            context_enrichment_keys = ["business_mappings", "field_synonyms", "active_filters", "session_id", "visible_tables", "conversation_history"]
            logger.info("=" * 80)
            logger.info("[CONTEXT ENRICHMENT DEBUG] Raw values before merging:")
            for key in context_enrichment_keys:
                kwarg_val = filtered_kwargs.get(key)
                default_val = self._default_config.get(key)

                # Debug: Show raw values
                logger.info(f"[CONTEXT ENRICHMENT DEBUG]   {key}:")
                logger.info(f"[CONTEXT ENRICHMENT DEBUG]     - default_config: type={type(default_val).__name__}, value={str(default_val)[:100]}")
                logger.info(f"[CONTEXT ENRICHMENT DEBUG]     - kwargs: type={type(kwarg_val).__name__}, value={str(kwarg_val)[:100]}")

                # Use kwarg if it has actual content, else use default config
                # Check for empty collections ({}, [], "") not just None
                kwarg_has_value = kwarg_val is not None and kwarg_val not in ({}, [], "")
                default_has_value = default_val is not None and default_val not in ({}, [], "")

                if kwarg_has_value:
                    value = kwarg_val
                    logger.info(f"[CONTEXT ENRICHMENT DEBUG]     → Using kwargs value")
                elif default_has_value:
                    value = default_val
                    logger.info(f"[CONTEXT ENRICHMENT DEBUG]     → Using default_config value")
                else:
                    # Both empty - use appropriate empty collection
                    if key in ["business_mappings", "field_synonyms", "active_filters"]:
                        value = {}
                    elif key in ["visible_tables", "conversation_history"]:
                        value = []
                    else:  # session_id
                        value = None
                    logger.info(f"[CONTEXT ENRICHMENT DEBUG]     → Both empty, using {type(value).__name__}")

                # Parse JSON strings if needed (for business_mappings, field_synonyms, active_filters)
                if value and isinstance(value, str) and key in ["business_mappings", "field_synonyms", "active_filters"]:
                    try:
                        value = json.loads(value)
                        logger.info(f"[CONTEXT ENRICHMENT DEBUG]     ✅ Parsed JSON string for '{key}': {len(value)} items")
                    except json.JSONDecodeError as e:
                        logger.warning(f"[CONTEXT ENRICHMENT DEBUG]     ❌ Failed to parse JSON for '{key}': {e}. Using empty dict.")
                        value = {}

                merged_config[key] = value
            logger.info("=" * 80)

            logger.info(f"[PowerBIAnalysisTool] DEFAULT CONFIG user_question: {self._default_config.get('user_question', 'NOT SET')}")
            logger.info(f"[PowerBIAnalysisTool] KWARGS user_question: {filtered_kwargs.get('user_question', 'NOT SET')}")
            logger.info(f"[PowerBIAnalysisTool] MERGED user_question: {merged_config.get('user_question', 'NOT SET')}")
            logger.info(f"[PowerBIAnalysisTool] Merged config - workspace_id: {merged_config.get('workspace_id')}, "
                       f"question: {merged_config.get('user_question', '')[:50] if merged_config.get('user_question') else 'None'}...")

            # Log context enrichment configuration
            logger.info("=" * 80)
            logger.info("[CONTEXT ENRICHMENT] Configuration:")
            business_mappings = merged_config.get('business_mappings') or {}
            field_synonyms = merged_config.get('field_synonyms') or {}
            active_filters = merged_config.get('active_filters') or {}
            logger.info(f"[CONTEXT ENRICHMENT]   business_mappings: {len(business_mappings)} terms")
            if business_mappings:
                for term, expr in list(business_mappings.items())[:3]:  # Show first 3
                    logger.info(f"[CONTEXT ENRICHMENT]     - '{term}' → {expr[:50]}...")
            logger.info(f"[CONTEXT ENRICHMENT]   field_synonyms: {len(field_synonyms)} fields")
            if field_synonyms:
                for field, synonyms in list(field_synonyms.items())[:3]:  # Show first 3
                    logger.info(f"[CONTEXT ENRICHMENT]     - '{field}' → {synonyms}")
            logger.info(f"[CONTEXT ENRICHMENT]   active_filters: {len(active_filters)} filters")
            if active_filters:
                for key, val in active_filters.items():
                    logger.info(f"[CONTEXT ENRICHMENT]     - '{key}' = {val}")
            logger.info("=" * 80)

            # Validate required parameters
            user_question = merged_config.get("user_question")
            workspace_id = merged_config.get("workspace_id")
            dataset_id = merged_config.get("dataset_id")

            if not user_question:
                return "Error: user_question is required. Please provide a business question to answer."
            if not workspace_id:
                return "Error: workspace_id is required."
            if not dataset_id:
                return "Error: dataset_id is required."

            # DEBUG: Log authentication parameters to diagnose Service Account issue
            logger.info("=" * 80)
            logger.info("[AUTH DEBUG] Checking authentication credentials:")
            logger.info(f"[AUTH DEBUG]   tenant_id: {'✓ SET' if merged_config.get('tenant_id') else '✗ MISSING'}")
            logger.info(f"[AUTH DEBUG]   client_id: {'✓ SET' if merged_config.get('client_id') else '✗ MISSING'}")
            logger.info(f"[AUTH DEBUG]   client_secret: {'✓ SET' if merged_config.get('client_secret') else '✗ MISSING'}")
            logger.info(f"[AUTH DEBUG]   username: {'✓ SET' if merged_config.get('username') else '✗ MISSING'}")
            logger.info(f"[AUTH DEBUG]   password: {'✓ SET' if merged_config.get('password') else '✗ MISSING'}")
            logger.info(f"[AUTH DEBUG]   access_token: {'✓ SET' if merged_config.get('access_token') else '✗ MISSING'}")
            logger.info(f"[AUTH DEBUG]   auth_method: {merged_config.get('auth_method', 'NOT SET')}")

            # Show actual values (masked) to help diagnose
            if merged_config.get('username'):
                logger.info(f"[AUTH DEBUG]   username value: {merged_config.get('username')}")
            if merged_config.get('password'):
                logger.info(f"[AUTH DEBUG]   password length: {len(merged_config.get('password', ''))}")
            logger.info("=" * 80)

            # Validate authentication
            has_sp_auth = all([
                merged_config.get("tenant_id"),
                merged_config.get("client_id"),
                merged_config.get("client_secret")
            ])
            has_sa_auth = all([
                merged_config.get("tenant_id"),
                merged_config.get("client_id"),
                merged_config.get("username"),
                merged_config.get("password")
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

            # Run async pipeline
            result = _run_async_in_sync_context(self._execute_analysis_pipeline(merged_config))

            return result

        except Exception as e:
            logger.error(f"[PowerBIAnalysisTool] Error: {str(e)}", exc_info=True)
            return f"Error: {str(e)}"

    async def _execute_analysis_pipeline(self, config: Dict[str, Any]) -> str:
        """Execute the full analysis pipeline."""
        user_question = config["user_question"]
        workspace_id = config["workspace_id"]
        dataset_id = config["dataset_id"]
        output_format = config.get("output_format", "markdown")

        logger.info(f"Starting analysis pipeline: question='{user_question[:50]}...', workspace={workspace_id}")

        # Initialize results
        results = {
            "user_question": user_question,
            "workspace_id": workspace_id,
            "dataset_id": dataset_id,
            "model_context": {
                "measures": [],
                "relationships": [],
                "tables": []
            },
            "generated_dax": None,
            "dax_execution": {
                "success": False,
                "data": [],
                "row_count": 0,
                "error": None
            },
            "visual_references": [],
            "errors": []
        }

        # Step 1: Get access token
        try:
            access_token = await self._get_access_token(config)
            logger.info("Access token obtained successfully")
        except Exception as e:
            results["errors"].append(f"Authentication error: {str(e)}")
            return self._format_output(results, output_format)

        # Step 1.5: Check cache for today's semantic model metadata
        # This caches: measures, relationships, schema, sample_data, default_filters
        # User inputs (business_mappings, field_synonyms, active_filters) are always fresh from config
        cache_hit = False
        cached_metadata = None
        group_id = config.get("group_id") or (getattr(self, "trace_context", None) or {}).get("group_context", {}).get("primary_group_id") or "default"
        report_id = config.get("report_id")

        # Initialize model_context (will be populated from cache or fresh fetch)
        model_context = {
            "measures": [],
            "relationships": [],
            "tables": [],
            "columns": [],
            "sample_data": {}
        }

        try:
            async with ToolSessionProvider.cache_service() as cache_service:
                cached_metadata = await cache_service.get_cached_metadata(
                    group_id=group_id,
                    dataset_id=dataset_id,
                    workspace_id=workspace_id,
                    report_id=report_id
                )

            if cached_metadata:
                cache_hit = True
                logger.info(f"✨ [CACHE HIT] Using cached metadata for dataset {dataset_id} (date: {date.today()})")
                logger.info(f"   Cached: {len(cached_metadata.get('measures', []))} measures, "
                           f"{len(cached_metadata.get('relationships', []))} relationships, "
                           f"{len(cached_metadata.get('sample_data', {}))} sample columns")

                # Load model context from cache
                model_context = {
                    "measures": cached_metadata.get("measures", []),
                    "relationships": cached_metadata.get("relationships", []),
                    "tables": cached_metadata.get("schema", {}).get("tables", []),
                    "columns": cached_metadata.get("schema", {}).get("columns", []),
                    "sample_data": cached_metadata.get("sample_data", {})
                }

                # Load default filters from cache (if report_id was provided)
                if report_id and "default_filters" in cached_metadata:
                    cached_filters = cached_metadata["default_filters"]
                    if cached_filters:
                        # Merge cached report-level filters with user-provided active_filters
                        existing_filters = config.get("active_filters", {})
                        if not existing_filters:
                            existing_filters = {}
                        merged_filters = {**cached_filters, **existing_filters}
                        config["active_filters"] = merged_filters
                        logger.info(f"   Loaded {len(cached_filters)} default filters from cache")

            else:
                logger.info(f"⚡ [CACHE MISS] Fetching fresh metadata for dataset {dataset_id}")

        except Exception as e:
            logger.warning(f"[Cache] Cache check failed, fetching fresh data: {e}")
            cache_hit = False

        # Step 2: Extract model context (measures, relationships) - ONLY if not cached
        if not cache_hit:
            try:
                model_context = await self._extract_model_context(
                    workspace_id, dataset_id, access_token, config
                )
                logger.info(f"Model context extracted: {len(model_context['measures'])} measures, {len(model_context['relationships'])} relationships")

                # Step 2b: Enrich model context with metadata (Microsoft Copilot-style)
                # This adds column descriptions, sample values, and enhanced metadata
                try:
                    model_context = await self._enrich_model_context_with_metadata(
                        model_context, workspace_id, dataset_id, access_token, config
                    )
                    logger.info("[Context Enrichment] Model context enriched with metadata")
                except Exception as e:
                    logger.warning(f"[Context Enrichment] Metadata enrichment failed (continuing with basic context): {e}")

                # Step 2c: Auto-extract default filters from report (if report_id provided)
                # These are report-level filters that apply to all pages
                # They are MERGED with any user-provided active_filters
                if report_id:
                    try:
                        report_level_filters = await self._extract_default_filters(
                            workspace_id, report_id, access_token
                        )
                        if report_level_filters:
                            # Get existing active_filters (user-provided or empty)
                            existing_filters = config.get("active_filters", {})
                            if not existing_filters:
                                existing_filters = {}

                            # Merge: report-level filters first, then user filters (user filters take precedence)
                            merged_filters = {**report_level_filters, **existing_filters}

                            config["active_filters"] = merged_filters
                            logger.info(f"[Context Enrichment] Auto-extracted {len(report_level_filters)} report-level filters")
                            logger.info(f"[Context Enrichment] Total active filters: {len(merged_filters)} (report-level + user-provided)")
                    except Exception as e:
                        logger.warning(f"[Context Enrichment] Failed to extract default filters (continuing without): {e}")

                # Step 2d: Save to cache for next time (same day, same dataset)
                try:
                    async with ToolSessionProvider.cache_service() as cache_service:
                        # Build metadata dict for caching
                        cache_metadata = cache_service.build_metadata_dict(
                            measures=model_context.get("measures", []),
                            relationships=model_context.get("relationships", []),
                            schema={
                                "tables": model_context.get("tables", []),
                                "columns": model_context.get("columns", [])
                            },
                            sample_data=model_context.get("sample_data", {}),
                            default_filters=config.get("active_filters") if report_id else None
                        )

                        await cache_service.save_metadata(
                            group_id=group_id,
                            dataset_id=dataset_id,
                            workspace_id=workspace_id,
                            metadata=cache_metadata,
                            report_id=report_id
                        )

                        logger.info(f"💾 [CACHE SAVED] Metadata cached for dataset {dataset_id} (date: {date.today()})")

                except Exception as e:
                    logger.warning(f"[Cache] Failed to save cache (continuing without): {e}")

            except Exception as e:
                results["errors"].append(f"Model extraction error: {str(e)}")
                logger.error(f"Model extraction failed: {e}")

        # Set model context in results (either from cache or fresh fetch)
        results["model_context"] = model_context

        # Step 3: Generate DAX using LLM with retry mechanism
        max_retries = config.get("max_dax_retries", 5)
        dax_attempts = []

        if results["model_context"]["measures"] or results["model_context"]["tables"]:
            for attempt in range(max_retries):
                try:
                    # Generate DAX (with error feedback on retries)
                    if attempt == 0:
                        # First attempt - no previous errors
                        generated_dax = await self._generate_dax_with_llm(
                            user_question, results["model_context"], config
                        )
                    else:
                        # Retry with error feedback
                        logger.info(f"[DAX Generation] Retry attempt {attempt + 1}/{max_retries}")
                        generated_dax = await self._generate_dax_with_self_correction(
                            user_question,
                            results["model_context"],
                            config,
                            dax_attempts
                        )

                    results["generated_dax"] = generated_dax
                    logger.info(f"DAX generated (attempt {attempt + 1}): {generated_dax[:100] if generated_dax else 'None'}...")

                    # Try to execute the generated DAX
                    if generated_dax:
                        execution_result = await self._execute_dax_query(
                            workspace_id, dataset_id, access_token, generated_dax
                        )

                        # Defensive check: ensure execution_result is a dict
                        if not isinstance(execution_result, dict):
                            logger.error(f"[DAX EXECUTION] execution_result is not a dict! Type: {type(execution_result)}, Value: {execution_result}")
                            execution_result = {
                                "success": False,
                                "error": f"Invalid execution result type: {type(execution_result).__name__}",
                                "row_count": 0
                            }

                        # Store attempt info
                        dax_attempts.append({
                            "attempt": attempt + 1,
                            "dax": generated_dax,
                            "success": execution_result.get("success", False),
                            "error": execution_result.get("error"),
                            "row_count": execution_result.get("row_count", 0)
                        })

                        # If successful, break out of retry loop
                        if execution_result.get("success", False):
                            results["dax_execution"] = execution_result
                            logger.info(f"✅ DAX execution successful on attempt {attempt + 1}: rows={execution_result.get('row_count', 0)}")
                            break
                        else:
                            # Failed - log and retry
                            logger.warning(f"❌ DAX execution failed on attempt {attempt + 1}: {execution_result.get('error', 'Unknown error')}")
                            results["dax_execution"] = execution_result

                            # If this was the last attempt, keep the error
                            if attempt == max_retries - 1:
                                results["errors"].append(f"DAX execution failed after {max_retries} attempts: {execution_result.get('error')}")
                                logger.error(f"DAX execution failed after {max_retries} attempts")
                    else:
                        logger.warning(f"No DAX generated on attempt {attempt + 1}")
                        if attempt == max_retries - 1:
                            results["errors"].append("Failed to generate valid DAX query")

                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"DAX generation/execution error on attempt {attempt + 1}: {error_msg}")

                    # Store failed attempt
                    dax_attempts.append({
                        "attempt": attempt + 1,
                        "dax": results.get("generated_dax"),
                        "success": False,
                        "error": error_msg,
                        "row_count": 0
                    })

                    # If last attempt, add to errors
                    if attempt == max_retries - 1:
                        results["errors"].append(f"DAX generation error after {max_retries} attempts: {error_msg}")

        # Store all attempts for debugging
        results["dax_attempts"] = dax_attempts

        # Step 5: Find visual references (optional)
        if config.get("include_visual_references", True) and results["model_context"]["measures"]:
            try:
                # Get measures used in the generated DAX
                used_measures = self._extract_measures_from_dax(
                    results["generated_dax"] or "",
                    [m["name"] for m in results["model_context"]["measures"]]
                )
                if used_measures:
                    visual_refs = await self._find_visual_references(
                        workspace_id, dataset_id, access_token, used_measures
                    )
                    results["visual_references"] = visual_refs
                    logger.info(f"Found {len(visual_refs)} visual references")
            except Exception as e:
                results["errors"].append(f"Visual reference error: {str(e)}")
                logger.error(f"Visual reference search failed: {e}")

        return self._format_output(results, output_format)


