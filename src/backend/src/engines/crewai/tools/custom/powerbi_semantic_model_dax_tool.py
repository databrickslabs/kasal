"""
Power BI Semantic Model DAX Generator Tool for CrewAI

Generates and executes DAX queries from natural language using LLM:
1. Accepts model context JSON from the Fetcher tool (or reads from cache as fallback)
2. Generates DAX via LLM with self-correction retry loop
3. Executes DAX via Power BI Execute Queries API
4. Finds visual references in reports (optional)

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


class PowerBISemanticModelDaxSchema(BaseModel):
    """Input schema for PowerBISemanticModelDaxTool."""

    # ===== USER QUESTION =====
    user_question: Optional[str] = Field(
        None,
        description="The business question to answer using Power BI data."
    )

    # ===== MODEL CONTEXT (from Fetcher tool output) =====
    model_context_json: Optional[str] = Field(
        None,
        description="[Context] JSON string from the Fetcher tool output. If provided, skips cache lookup."
    )

    # ===== POWER BI CONFIGURATION =====
    workspace_id: Optional[str] = Field(
        None,
        description="[Power BI] Workspace ID (GUID) — needed for DAX execution and cache fallback."
    )
    dataset_id: Optional[str] = Field(
        None,
        description="[Power BI] Dataset/Semantic Model ID (GUID) — needed for DAX execution."
    )
    report_id: Optional[str] = Field(
        None,
        description="[Power BI] Optional Report ID (GUID) for visual reference lookup."
    )

    # ===== CONTEXT ENRICHMENT =====
    business_mappings: Optional[Dict[str, str]] = Field(
        None,
        description="[Context] Business terminology mappings — natural language to DAX expressions."
    )
    field_synonyms: Optional[Dict[str, List[str]]] = Field(
        None,
        description="[Context] Field synonyms for natural language understanding."
    )
    active_filters: Optional[Dict[str, Any]] = Field(
        None,
        description="[Context] Currently active filters/slicers to auto-apply."
    )
    session_id: Optional[str] = Field(
        None, description="[Context] Session ID for conversation history."
    )
    visible_tables: Optional[List[str]] = Field(
        None, description="[Context] Tables currently visible/in use."
    )
    conversation_history: Optional[List[Dict[str, str]]] = Field(
        None, description="[Context] Previous Q&A for context."
    )

    # ===== SERVICE PRINCIPAL AUTHENTICATION =====
    tenant_id: Optional[str] = Field(None, description="[Auth] Azure AD tenant ID.")
    client_id: Optional[str] = Field(None, description="[Auth] Application/Client ID.")
    client_secret: Optional[str] = Field(None, description="[Auth] Client secret for SP.")
    username: Optional[str] = Field(None, description="[Auth] Service account username.")
    password: Optional[str] = Field(None, description="[Auth] Service account password.")
    auth_method: Optional[str] = Field(None, description="[Auth] 'service_principal', 'service_account', or auto.")
    access_token: Optional[str] = Field(None, description="[Auth] Pre-obtained OAuth token.")

    # ===== LLM CONFIGURATION =====
    llm_workspace_url: Optional[str] = Field(None, description="[LLM] Databricks workspace URL.")
    llm_token: Optional[str] = Field(None, description="[LLM] Databricks token for LLM access.")
    llm_model: str = Field("databricks-claude-sonnet-4", description="[LLM] Model for DAX generation.")

    # ===== OPTIONS =====
    include_visual_references: bool = Field(True, description="[Options] Search for visual references.")
    max_dax_retries: int = Field(5, description="[Options] Max retry attempts if DAX execution fails (1-10).")
    output_format: str = Field("markdown", description="[Output] 'markdown' or 'json'.")


class PowerBISemanticModelDaxTool(BaseTool):
    """
    Power BI Semantic Model DAX Generator — generates and executes DAX from natural language.

    Accepts model context from the Fetcher tool output (JSON) or reads from cache as fallback.
    Uses LLM with self-correction retry loop to generate accurate DAX queries.

    **Input Priority**:
    1. model_context_json provided → parse and use directly
    2. Else → try cache lookup via PowerBISemanticModelCacheService
    3. Else → error (require fetcher to run first)
    """

    name: str = "Power BI Semantic Model DAX Generator"
    description: str = (
        "Generates and executes DAX queries from natural language questions using LLM. "
        "Accepts model context JSON from the 'Power BI Semantic Model Fetcher' tool output, "
        "or reads from cache as fallback. Features self-correction retry loop (up to N retries) "
        "and optional visual reference lookup. "
        "IMPORTANT: Extract the user's business question from the task description and pass it as 'user_question'. "
        "Connection credentials are pre-configured — do not provide them unless overriding."
    )
    args_schema: Type[BaseModel] = PowerBISemanticModelDaxSchema

    _instance_id: str = PrivateAttr()
    _default_config: Dict[str, Any] = PrivateAttr()

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        import uuid
        instance_id = str(uuid.uuid4())[:8]
        logger.info(f"[DaxTool.__init__] Instance ID: {instance_id}")

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
            "llm_workspace_url": kwargs.get("llm_workspace_url"),
            "llm_token": kwargs.get("llm_token"),
            "llm_model": kwargs.get("llm_model", "databricks-claude-sonnet-4"),
            "include_visual_references": kwargs.get("include_visual_references", True),
            "max_dax_retries": kwargs.get("max_dax_retries", 5),
            "output_format": kwargs.get("output_format", "markdown"),
            "user_question": kwargs.get("user_question"),
            "business_mappings": kwargs.get("business_mappings", {}),
            "field_synonyms": kwargs.get("field_synonyms", {}),
            "active_filters": kwargs.get("active_filters", {}),
            "session_id": kwargs.get("session_id"),
            "visible_tables": kwargs.get("visible_tables", []),
            "conversation_history": kwargs.get("conversation_history", []),
        }

        tool_kwargs = {k: v for k, v in kwargs.items() if k not in default_config}
        super().__init__(**tool_kwargs)

        self._instance_id = instance_id
        self._default_config = default_config

    def _is_placeholder_value(self, value: Any) -> bool:
        """Check if a value looks like a placeholder that should be ignored."""
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
        """Execute the DAX generation pipeline."""
        try:
            logger.info(f"[DaxTool] Instance {self._instance_id} - _run() called")

            # Filter out placeholder values
            filtered_kwargs = {}
            for k, v in kwargs.items():
                if v is not None and not self._is_placeholder_value(v):
                    filtered_kwargs[k] = v

            # Merge configs
            merged_config = {}

            # Connection/auth — default config takes precedence
            config_params = [
                "workspace_id", "dataset_id", "report_id", "tenant_id", "client_id",
                "client_secret", "username", "password", "auth_method", "access_token",
                "llm_workspace_url", "llm_token", "llm_model",
            ]
            for key in config_params:
                default_val = self._default_config.get(key)
                kwarg_val = filtered_kwargs.get(key)
                merged_config[key] = default_val if default_val is not None else kwarg_val

            # user_question — prefer default config (pre-configured) over agent input
            kwarg_question = filtered_kwargs.get("user_question")
            default_question = self._default_config.get("user_question")
            merged_config["user_question"] = default_question if default_question is not None else kwarg_question

            # Options
            for key in ["include_visual_references", "max_dax_retries", "output_format"]:
                kwarg_val = filtered_kwargs.get(key)
                default_val = self._default_config.get(key)
                merged_config[key] = kwarg_val if kwarg_val is not None else default_val

            # model_context_json from kwargs (not stored in default_config — always runtime)
            merged_config["model_context_json"] = filtered_kwargs.get("model_context_json")

            # Context enrichment
            context_keys = ["business_mappings", "field_synonyms", "active_filters",
                            "session_id", "visible_tables", "conversation_history"]
            for key in context_keys:
                kwarg_val = filtered_kwargs.get(key)
                default_val = self._default_config.get(key)
                kwarg_has = kwarg_val is not None and kwarg_val not in ({}, [], "")
                default_has = default_val is not None and default_val not in ({}, [], "")

                if kwarg_has:
                    value = kwarg_val
                elif default_has:
                    value = default_val
                else:
                    if key in ["business_mappings", "field_synonyms", "active_filters"]:
                        value = {}
                    elif key in ["visible_tables", "conversation_history"]:
                        value = []
                    else:
                        value = None

                # Parse JSON strings
                if value and isinstance(value, str) and key in ["business_mappings", "field_synonyms", "active_filters"]:
                    try:
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        value = {}

                merged_config[key] = value

            # Validate
            if not merged_config.get("user_question"):
                return "Error: user_question is required."
            if not merged_config.get("workspace_id"):
                return "Error: workspace_id is required."
            if not merged_config.get("dataset_id"):
                return "Error: dataset_id is required."

            # Auth validation
            has_sp = all([merged_config.get("tenant_id"), merged_config.get("client_id"), merged_config.get("client_secret")])
            has_sa = all([merged_config.get("tenant_id"), merged_config.get("client_id"),
                          merged_config.get("username"), merged_config.get("password")])
            has_oauth = bool(merged_config.get("access_token"))
            if not has_sp and not has_sa and not has_oauth:
                return (
                    "Error: Authentication required.\n"
                    "Provide one of:\n"
                    "- Service Principal: tenant_id, client_id, client_secret\n"
                    "- Service Account: tenant_id, client_id, username, password\n"
                    "- User OAuth: access_token"
                )

            result = _run_async_in_sync_context(self._execute_dax_pipeline(merged_config))
            return result

        except Exception as e:
            logger.error(f"[DaxTool] Error: {str(e)}", exc_info=True)
            return f"Error: {str(e)}"

    async def _execute_dax_pipeline(self, config: Dict[str, Any]) -> str:
        """Execute DAX pipeline: resolve context → generate DAX → execute → visual refs."""
        user_question = config["user_question"]
        workspace_id = config["workspace_id"]
        dataset_id = config["dataset_id"]
        output_format = config.get("output_format", "markdown")

        results: Dict[str, Any] = {
            "user_question": user_question,
            "workspace_id": workspace_id,
            "dataset_id": dataset_id,
            "model_context": {"measures": [], "relationships": [], "tables": []},
            "generated_dax": None,
            "dax_execution": {"success": False, "data": [], "row_count": 0, "error": None},
            "visual_references": [],
            "errors": [],
        }

        # Step 1: Get access token
        try:
            access_token = await self._get_access_token(config)
        except Exception as e:
            results["errors"].append(f"Authentication error: {str(e)}")
            return self._format_output(results, output_format)

        # Step 2: Resolve model context
        model_context = await self._resolve_model_context(config)
        if model_context is None:
            results["errors"].append(
                "No model context available. Run the 'Power BI Semantic Model Fetcher' tool first, "
                "or provide model_context_json."
            )
            return self._format_output(results, output_format)
        results["model_context"] = model_context

        # Merge default filters from context into active_filters
        default_filters = model_context.get("default_filters") if isinstance(model_context.get("default_filters"), dict) else {}
        if default_filters:
            existing = config.get("active_filters", {}) or {}
            config["active_filters"] = {**default_filters, **existing}

        # Step 3: Generate + Execute DAX with retry
        max_retries = config.get("max_dax_retries", 5)
        dax_attempts: List[Dict[str, Any]] = []

        if model_context.get("measures") or model_context.get("tables"):
            for attempt in range(max_retries):
                try:
                    if attempt == 0:
                        generated_dax = await self._generate_dax_with_llm(user_question, model_context, config)
                    else:
                        logger.info(f"[DAX] Retry attempt {attempt + 1}/{max_retries}")
                        generated_dax = await self._generate_dax_with_self_correction(
                            user_question, model_context, config, dax_attempts
                        )

                    results["generated_dax"] = generated_dax

                    if generated_dax:
                        execution_result = await self._execute_dax_query(
                            workspace_id, dataset_id, access_token, generated_dax
                        )
                        if not isinstance(execution_result, dict):
                            execution_result = {"success": False, "error": f"Invalid result type: {type(execution_result).__name__}", "row_count": 0}

                        dax_attempts.append({
                            "attempt": attempt + 1,
                            "dax": generated_dax,
                            "success": execution_result.get("success", False),
                            "error": execution_result.get("error"),
                            "row_count": execution_result.get("row_count", 0),
                        })

                        if execution_result.get("success", False):
                            results["dax_execution"] = execution_result
                            logger.info(f"DAX success on attempt {attempt + 1}: rows={execution_result.get('row_count', 0)}")
                            break
                        else:
                            results["dax_execution"] = execution_result
                            if attempt == max_retries - 1:
                                results["errors"].append(f"DAX failed after {max_retries} attempts: {execution_result.get('error')}")
                    else:
                        if attempt == max_retries - 1:
                            results["errors"].append("Failed to generate valid DAX query")

                except Exception as e:
                    dax_attempts.append({
                        "attempt": attempt + 1, "dax": results.get("generated_dax"),
                        "success": False, "error": str(e), "row_count": 0,
                    })
                    if attempt == max_retries - 1:
                        results["errors"].append(f"DAX error after {max_retries} attempts: {str(e)}")

        results["dax_attempts"] = dax_attempts

        # Step 4: Visual references
        if config.get("include_visual_references", True) and model_context.get("measures"):
            try:
                used_measures = self._extract_measures_from_dax(
                    results["generated_dax"] or "",
                    [m["name"] for m in model_context["measures"]],
                )
                if used_measures:
                    visual_refs = await self._find_visual_references(
                        workspace_id, dataset_id, access_token, used_measures
                    )
                    results["visual_references"] = visual_refs
            except Exception as e:
                results["errors"].append(f"Visual reference error: {str(e)}")

        return self._format_output(results, output_format)

    async def _resolve_model_context(self, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Resolve model context from JSON input or cache."""
        # Priority 1: model_context_json provided directly
        model_context_json = config.get("model_context_json")
        if model_context_json:
            try:
                parsed = json.loads(model_context_json) if isinstance(model_context_json, str) else model_context_json
                logger.info(f"[DaxTool] Using model context from input JSON ({len(parsed.get('measures', []))} measures)")
                return {
                    "measures": parsed.get("measures", []),
                    "relationships": parsed.get("relationships", []),
                    "tables": parsed.get("tables", []),
                    "columns": parsed.get("columns", []),
                    "sample_data": parsed.get("sample_data", {}),
                    "default_filters": parsed.get("default_filters", {}),
                }
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"[DaxTool] Failed to parse model_context_json: {e}")

        # Priority 2: Cache fallback
        dataset_id = config.get("dataset_id")
        workspace_id = config.get("workspace_id")
        report_id = config.get("report_id")
        group_id = config.get("group_id", "default")

        if dataset_id and workspace_id:
            try:
                async with async_session_factory() as session:
                    cache_service = PowerBISemanticModelCacheService(session)
                    cached = await cache_service.get_cached_metadata(
                        group_id=group_id, dataset_id=dataset_id,
                        workspace_id=workspace_id, report_id=report_id,
                    )
                if cached:
                    logger.info(f"[DaxTool] Using cached model context for dataset {dataset_id}")
                    return {
                        "measures": cached.get("measures", []),
                        "relationships": cached.get("relationships", []),
                        "tables": cached.get("schema", {}).get("tables", []),
                        "columns": cached.get("schema", {}).get("columns", []),
                        "sample_data": cached.get("sample_data", {}),
                        "default_filters": cached.get("default_filters", {}),
                    }
            except Exception as e:
                logger.warning(f"[DaxTool] Cache lookup failed: {e}")

        return None

    # =====================================================================
    # Authentication
    # =====================================================================

    async def _get_access_token(self, config: Dict[str, Any]) -> str:
        """Get OAuth access token using centralized auth utilities."""
        from src.engines.crewai.tools.custom.powerbi_auth_utils import get_powerbi_access_token_from_config
        return await get_powerbi_access_token_from_config(config)

    # =====================================================================
    # DAX Generation
    # =====================================================================

    def _build_enriched_semantic_context(self, model_context: Dict[str, Any], config: Dict[str, Any]) -> str:
        """Build enriched semantic context for LLM prompt (Copilot-style)."""
        sections = []
        sections.append("## SEMANTIC MODEL SCHEMA\n")

        tables = model_context.get("tables", [])
        measures = model_context.get("measures", [])
        visible_tables = config.get("visible_tables", [])

        # Tables with measures are critical
        tables_with_measures = set()
        for measure in measures[:20]:
            table_name = measure.get("table")
            if table_name:
                tables_with_measures.add(table_name)

        tables_to_show = []
        tables_seen = set()

        for table in tables:
            if table["name"] in tables_with_measures:
                tables_to_show.append(table)
                tables_seen.add(table["name"])
        if visible_tables:
            for table in tables:
                if table["name"] in visible_tables and table["name"] not in tables_seen:
                    tables_to_show.append(table)
                    tables_seen.add(table["name"])
        remaining_slots = 15 - len(tables_to_show)
        for table in tables:
            if table["name"] not in tables_seen and remaining_slots > 0:
                tables_to_show.append(table)
                tables_seen.add(table["name"])
                remaining_slots -= 1

        for table in tables_to_show:
            table_name = table["name"]
            sections.append(f"### Table: **{table_name}**")
            columns = table.get("columns", [])
            if columns:
                column_types = table.get("column_types", {})
                column_list = []
                for col in columns[:15]:
                    col_type = column_types.get(col, "")
                    column_list.append(f"{col} ({col_type})" if col_type else col)
                sections.append(f"**Columns**: {', '.join(column_list)}")
            column_descriptions = table.get("column_descriptions", {})
            if column_descriptions:
                sections.append("**Column Descriptions**:")
                for col, desc in list(column_descriptions.items())[:5]:
                    sections.append(f"  - {col}: {desc}")
            sections.append("")

        if measures:
            sections.append("### Available Measures")
            for measure in measures[:20]:
                sections.append(f"- **{measure['name']}** (Table: {measure.get('table', '')})")
                sections.append(f"  Expression: `{measure.get('expression', '')[:100]}...`")
            sections.append("")

        relationships = model_context.get("relationships", [])
        if relationships:
            relevant = [r for r in relationships if r['from_table'] in tables_seen or r['to_table'] in tables_seen]
            if relevant:
                sections.append("### Table Relationships")
                for rel in relevant:
                    sections.append(f"- {rel['from_table']}[{rel['from_column']}] -> {rel['to_table']}[{rel['to_column']}]")
                sections.append("")

        # Business terminology
        business_mappings = config.get("business_mappings", {})
        field_synonyms = config.get("field_synonyms", {})
        if business_mappings or field_synonyms:
            sections.append("## BUSINESS TERMINOLOGY\n")
            if business_mappings:
                sections.append("### Business Term Mappings")
                for term, expression in business_mappings.items():
                    sections.append(f'- **"{term}"** -> `{expression}`')
                sections.append("")
            if field_synonyms:
                sections.append("### Field Synonyms")
                for field, synonyms in field_synonyms.items():
                    sections.append(f"- **{field}**: {', '.join(synonyms)}")
                sections.append("")

        # Sample data
        sample_values = model_context.get("sample_data", {}) or model_context.get("sample_values", {})
        if sample_values:
            sections.append("## SAMPLE DATA VALUES\n")
            for column, value_info in list(sample_values.items())[:10]:
                if value_info.get("type") == "categorical":
                    values = value_info.get("sample_values", [])
                    sections.append(f"- **{column}**: {', '.join([str(v) for v in values[:5]])}")
            sections.append("")

        # Active filters
        active_filters = config.get("active_filters", {})
        if active_filters:
            sections.append("## CURRENT VIEW STATE (AUTO-APPLY FILTERS)\n")
            sections.append("**IMPORTANT**: These filters are CURRENTLY ACTIVE and should be applied:\n")
            for filter_name, filter_value in active_filters.items():
                if isinstance(filter_value, list):
                    quoted = ', '.join([f"'{v}'" for v in filter_value])
                    sections.append(f"- **{filter_name}** IN ({quoted})")
                else:
                    sections.append(f"- **{filter_name}** = {filter_value}")
            sections.append("")

        # Conversation history
        conversation_history = config.get("conversation_history", [])
        if conversation_history:
            sections.append("## RECENT CONVERSATION HISTORY\n")
            for i, turn in enumerate(conversation_history[-3:], 1):
                sections.append(f"**Q{i}**: {turn.get('question', '')}")
                if turn.get('filters_used'):
                    sections.append(f"  Filters used: {turn['filters_used']}")
                if turn.get('answer'):
                    sections.append(f"  Answer: {turn['answer']}")
            sections.append("")

        return "\n".join(sections)

    async def _generate_dax_with_llm(
        self, user_question: str, model_context: Dict[str, Any], config: Dict[str, Any]
    ) -> Optional[str]:
        """Generate DAX query using LLM with enriched context."""
        llm_workspace_url = config.get("llm_workspace_url")
        llm_token = config.get("llm_token")
        llm_model = config.get("llm_model", "databricks-claude-sonnet-4")

        if not llm_workspace_url or not llm_token:
            return self._generate_simple_dax(user_question, model_context)

        measures = model_context.get("measures", [])
        if not measures:
            logger.warning("[DAX Generation] No measures available")
            return None

        enriched_context = self._build_enriched_semantic_context(model_context, config)

        prompt = f"""{enriched_context}

## USER QUESTION
{user_question}

## DAX GENERATION INSTRUCTIONS

### Context Understanding
1. **Interpret natural language**: Use business term mappings and synonyms
2. **Apply implicit filters**: Auto-apply filters from "CURRENT VIEW STATE"
3. **Use conversation history**: Consider previous questions for context
4. **Leverage sample values**: Use sample data to understand value formats

### Query Generation Rules
1. **ONLY use measure names from "Available Measures"** - DO NOT invent names
2. **ONLY use table/column names from "SEMANTIC MODEL SCHEMA"** - DO NOT guess
3. **Use exact syntax**: Measure references must match exactly
4. **Apply active filters**: Include filters from "CURRENT VIEW STATE" automatically
5. **Natural language translation**: Use "Business Term Mappings" to convert phrases

### DAX Syntax Requirements

#### Basic Structure
- Use `EVALUATE` with `SUMMARIZECOLUMNS`, `ADDCOLUMNS`, or other table functions
- **Filter order**: In SUMMARIZECOLUMNS, filters MUST come BEFORE measure name/expression pairs
- Return ONLY the DAX query - no explanations, no markdown code blocks

#### String Prefix Filtering (CRITICAL)
**NEVER use STARTSWITH()** - not supported by Power BI API.
Use LEFT() instead: `LEFT(Column, 1) = "7"`

#### Multi-Table Filtering (CRITICAL)
**NEVER use ALL() with columns from different tables**
Use TREATAS or CALCULATETABLE instead.

#### IN Operator for Multiple Values
Use curly braces: `Column IN {{"Value1", "Value2"}}`

## YOUR GENERATED DAX QUERY

**CRITICAL**: Return ONLY the DAX query. NO explanations. Start with EVALUATE.
"""

        logger.info(f"[DAX Generation] Enriched context size: {len(enriched_context)} chars")

        self._emit_llm_trace(event_context="DAX Generation - Prompt", prompt=prompt, model=llm_model, operation="generate_dax")

        url = f"{llm_workspace_url.rstrip('/')}/serving-endpoints/{llm_model}/invocations"
        headers = {"Authorization": f"Bearer {llm_token}", "Content-Type": "application/json"}
        payload = {"messages": [{"role": "user", "content": prompt}], "max_tokens": 1000, "temperature": 0.1}

        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                result = response.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")

                self._emit_llm_trace(
                    event_context="DAX Generation - Response", prompt=prompt,
                    response=content, model=llm_model, operation="generate_dax"
                )

                dax = self._extract_dax_from_llm_response(content)

                # Validate measures
                available_measure_names = [m["name"] for m in measures]
                all_references = re.findall(r'\[([^\]]+)\]', dax)
                potential_measures = []
                for ref in all_references:
                    is_table_column = any(f"{t['name']}[{ref}]" in dax for t in model_context.get("tables", []))
                    if not is_table_column:
                        potential_measures.append(ref)
                hallucinated = [m for m in potential_measures if m not in available_measure_names]
                if hallucinated:
                    logger.warning(f"[DAX Generation] Possible hallucinated measures: {hallucinated}")

                dax = self._auto_wrap_with_report_filters(dax, config)
                return dax

            except Exception as e:
                logger.error(f"LLM DAX generation error: {e}")
                return self._generate_simple_dax(user_question, model_context)

    async def _generate_dax_with_self_correction(
        self, user_question: str, model_context: Dict[str, Any],
        config: Dict[str, Any], previous_attempts: List[Dict[str, Any]]
    ) -> Optional[str]:
        """Generate DAX with self-correction based on previous failed attempts."""
        llm_workspace_url = config.get("llm_workspace_url")
        llm_token = config.get("llm_token")
        llm_model = config.get("llm_model", "databricks-claude-sonnet-4")

        if not llm_workspace_url or not llm_token:
            return None

        attempts_text = "\n\n".join([
            f"### Attempt {att['attempt']}\n"
            f"**DAX Query:**\n```dax\n{att['dax']}\n```\n"
            f"**Result:** {'SUCCESS' if att['success'] else 'FAILED'}\n"
            f"**Error:** {att['error']}" if not att['success'] else ""
            for att in previous_attempts
        ])

        enriched_context = self._build_enriched_semantic_context(model_context, config)

        prompt = f"""{enriched_context}

## SELF-CORRECTION MODE
Previous attempt(s) failed. Analyze the errors and generate a CORRECTED query.

## USER QUESTION
{user_question}

## PREVIOUS FAILED ATTEMPTS
{attempts_text}

## ERROR ANALYSIS & CORRECTION INSTRUCTIONS

1. **"Table/Column doesn't exist"**: Check spelling against schema, use exact names
2. **"All column arguments must be from the same table"**: Use TREATAS() instead of ALL() across tables
3. **"Syntax error"**: Check SUMMARIZECOLUMNS filter order (filters BEFORE measures)
4. **"Failed to resolve name 'STARTSWITH'"**: Use LEFT() instead
5. **"Type mismatch"**: Check sample values for correct types

**DO NOT repeat the same query** — try a different approach.

## YOUR CORRECTED DAX QUERY
"""

        url = f"{llm_workspace_url.rstrip('/')}/serving-endpoints/{llm_model}/invocations"
        headers = {"Authorization": f"Bearer {llm_token}", "Content-Type": "application/json"}
        payload = {"messages": [{"role": "user", "content": prompt}], "max_tokens": 1000, "temperature": 0.1}

        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                result = response.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                dax = self._extract_dax_from_llm_response(content)
                return dax
            except Exception as e:
                logger.error(f"LLM self-correction error: {e}")
                return None

    def _generate_simple_dax(self, user_question: str, model_context: Dict[str, Any]) -> Optional[str]:
        """Generate a simple DAX query without LLM."""
        measures = model_context.get("measures", [])
        if not measures:
            return None

        question_lower = user_question.lower()
        best_measure = None
        for measure in measures:
            if any(word in question_lower for word in measure["name"].lower().split()):
                best_measure = measure
                break
        if not best_measure:
            best_measure = measures[0]

        return f'EVALUATE\nSUMMARIZECOLUMNS(\n    "Result", [{best_measure["name"]}]\n)'

    def _extract_dax_from_llm_response(self, content: str) -> str:
        """Extract clean DAX query from LLM response."""
        content = re.sub(r'```dax\s*', '', content)
        content = re.sub(r'```\s*', '', content)

        evaluate_match = re.search(r'(EVALUATE[\s\S]+?)(?:\n\n|$)', content, re.IGNORECASE)
        if evaluate_match:
            dax_query = evaluate_match.group(1).strip()
        else:
            dax_query = content.strip()

        lines = dax_query.split('\n')
        clean_lines = []
        paren_depth = 0

        for line in lines:
            paren_depth += line.count('(') - line.count(')')
            clean_lines.append(line)
            if paren_depth == 0 and clean_lines:
                break

        dax_query = '\n'.join(clean_lines).strip()

        last_paren = dax_query.rfind(')')
        if last_paren != -1:
            after_paren = dax_query[last_paren + 1:].strip()
            if after_paren and (after_paren.startswith('**') or after_paren.startswith('#') or after_paren.startswith('-')):
                dax_query = dax_query[:last_paren + 1]

        return dax_query.strip()

    def _auto_wrap_with_report_filters(self, dax_query: str, config: Dict[str, Any]) -> str:
        """Wrap DAX with report-level filters if present."""
        active_filters = config.get("active_filters", {})
        if not active_filters:
            return dax_query

        filter_conditions = []
        for filter_name, filter_description in active_filters.items():
            if filter_name in dax_query:
                continue
            dax_condition = self._generate_dax_filter_condition(filter_name, filter_description)
            if dax_condition and not dax_condition.startswith("//"):
                filter_conditions.append(dax_condition)

        if not filter_conditions:
            return dax_query

        inner_dax = dax_query.strip()
        if inner_dax.upper().startswith("EVALUATE"):
            inner_dax = inner_dax[8:].strip()

        if inner_dax.upper().startswith("CALCULATETABLE"):
            paren_count = 0
            start_idx = inner_dax.find("(")
            end_idx = -1
            for i, char in enumerate(inner_dax[start_idx:], start=start_idx):
                if char == "(":
                    paren_count += 1
                elif char == ")":
                    paren_count -= 1
                    if paren_count == 0:
                        end_idx = i
                        break
            if end_idx > start_idx:
                inner_content = inner_dax[start_idx + 1:end_idx]
                wrapped_dax = f"EVALUATE\nCALCULATETABLE(\n    {inner_content},\n"
                for condition in filter_conditions:
                    wrapped_dax += f"    {condition},\n"
                wrapped_dax = wrapped_dax.rstrip(',\n') + "\n)"
            else:
                wrapped_dax = f"EVALUATE\nCALCULATETABLE(\n    {inner_dax},\n"
                for condition in filter_conditions:
                    wrapped_dax += f"    {condition},\n"
                wrapped_dax = wrapped_dax.rstrip(',\n') + "\n)"
        else:
            wrapped_dax = f"EVALUATE\nCALCULATETABLE(\n    {inner_dax},\n"
            for condition in filter_conditions:
                wrapped_dax += f"    {condition},\n"
            wrapped_dax = wrapped_dax.rstrip(',\n') + "\n)"

        return wrapped_dax

    def _generate_dax_filter_condition(self, filter_name: str, filter_description: str) -> str:
        """Generate a DAX filter condition from a filter name and description."""
        try:
            filter_desc = str(filter_description).strip()
            if filter_desc == "NOT NULL":
                return f"ISBLANK({filter_name}) = FALSE"
            if filter_desc.startswith("NOT STARTS WITH"):
                value = filter_desc.replace("NOT STARTS WITH", "").strip().strip("'\"")
                prefix_length = len(value)
                return f'FILTER(VALUES({filter_name}), NOT(LEFT({filter_name}, {prefix_length}) = "{value}"))'
            if filter_desc.startswith("= "):
                value = filter_desc[2:].strip().strip("'\"")
                return f'{filter_name} = "{value}"'
            if filter_desc.startswith("IN ("):
                values_str = filter_desc[4:-1]
                values = [v.strip().strip("'\"") for v in values_str.split(",")]
                values_list = ', '.join([f'"{v}"' for v in values])
                return f'{filter_name} IN {{{values_list}}}'
            if not filter_desc.startswith("NOT") and not filter_desc.startswith("IN"):
                value = filter_desc.strip("'\"")
                return f'{filter_name} = "{value}"'
            return f'// TODO: Apply filter {filter_name} {filter_desc}'
        except Exception:
            return f'// Error: Could not apply filter {filter_name}'

    # =====================================================================
    # DAX Execution
    # =====================================================================

    async def _execute_dax_query(
        self, workspace_id: str, dataset_id: str, access_token: str, dax_query: str
    ) -> Dict[str, Any]:
        """Execute DAX query via Power BI Execute Queries API."""
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

    def _extract_measures_from_dax(self, dax_query: str, available_measures: List[str]) -> List[str]:
        """Extract measure names used in a DAX query."""
        return [m for m in available_measures if f"[{m}]" in dax_query]

    # =====================================================================
    # Visual References
    # =====================================================================

    async def _find_visual_references(
        self, workspace_id: str, dataset_id: str, access_token: str, measures: List[str]
    ) -> List[Dict[str, Any]]:
        """Find reports, pages, and visuals that use the specified measures."""
        visual_refs: List[Dict[str, Any]] = []
        reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                reports_response = await client.get(reports_url, headers=headers)
                reports_response.raise_for_status()
                reports = [
                    r for r in reports_response.json().get("value", [])
                    if r.get("datasetId") == dataset_id
                ]
                for report in reports[:5]:
                    report_id = report.get("id")
                    report_name = report.get("name")
                    report_url = report.get("webUrl", "")
                    try:
                        page_refs = await self._get_measure_page_references(
                            workspace_id, report_id, report_name, report_url,
                            measures, access_token, client
                        )
                        if page_refs:
                            visual_refs.extend(page_refs)
                        else:
                            for measure in measures:
                                visual_refs.append({
                                    "report_name": report_name, "report_url": report_url,
                                    "page_name": None, "page_url": None, "measure": measure,
                                    "visual_type": None, "note": "Report uses same dataset",
                                })
                    except Exception:
                        for measure in measures:
                            visual_refs.append({
                                "report_name": report_name, "report_url": report_url,
                                "page_name": None, "page_url": None, "measure": measure,
                                "visual_type": None, "note": "Report uses same dataset",
                            })
            except Exception as e:
                logger.error(f"Visual reference search error: {e}")

        return visual_refs

    async def _get_measure_page_references(
        self, workspace_id: str, report_id: str, report_name: str, report_url: str,
        measures: List[str], access_token: str, client: httpx.AsyncClient
    ) -> List[Dict[str, Any]]:
        """Get page-level references for measures by parsing report definition."""
        refs: List[Dict[str, Any]] = []
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/reports/{report_id}/getDefinition"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

        try:
            response = await client.post(url, headers=headers, timeout=60.0)
            report_parts = None

            if response.status_code == 202:
                location = response.headers.get("Location")
                if not location:
                    return []
                for _ in range(30):
                    await asyncio.sleep(2)
                    poll_response = await client.get(location, headers=headers)
                    poll_data = poll_response.json()
                    if poll_data.get("status") == "Succeeded":
                        result_url = location + "/result"
                        result_response = await client.get(result_url, headers=headers)
                        result_response.raise_for_status()
                        report_parts = result_response.json().get("definition", {}).get("parts", [])
                        break
                    elif poll_data.get("status") == "Failed":
                        return []
                else:
                    return []
            elif response.status_code == 200:
                report_parts = response.json().get("definition", {}).get("parts", [])
            else:
                return []

            if not report_parts:
                return []

            pages = self._parse_report_pages(report_parts)
            visuals = self._parse_report_visuals(report_parts)
            page_lookup = {p.get("id"): p for p in pages}

            measure_locations: Dict[str, List[Dict[str, Any]]] = {}
            for visual in visuals:
                visual_measures = self._extract_measures_from_visual(visual)
                page_id = visual.get("page_id")
                visual_type = visual.get("type", "unknown")
                for measure in measures:
                    if measure in visual_measures:
                        measure_locations.setdefault(measure, []).append({"page_id": page_id, "visual_type": visual_type})

            for measure in measures:
                if measure in measure_locations:
                    seen_pages: set = set()
                    for loc in measure_locations[measure]:
                        page_id = loc["page_id"]
                        if page_id in seen_pages:
                            continue
                        seen_pages.add(page_id)
                        page_info = page_lookup.get(page_id, {})
                        page_name = page_info.get("displayName") or page_info.get("name") or page_id
                        page_url = self._build_page_url(workspace_id, report_id, page_id)
                        refs.append({
                            "report_name": report_name, "report_url": report_url,
                            "page_name": page_name, "page_url": page_url,
                            "measure": measure, "visual_type": loc["visual_type"],
                            "note": f"Measure found on page '{page_name}'",
                        })
                else:
                    refs.append({
                        "report_name": report_name, "report_url": report_url,
                        "page_name": None, "page_url": None,
                        "measure": measure, "visual_type": None,
                        "note": "Measure in dataset but not detected in visuals",
                    })
            return refs
        except Exception as e:
            logger.warning(f"Error fetching report definition: {e}")
            return []

    def _build_page_url(self, workspace_id: str, report_id: str, page_id: str) -> str:
        if page_id:
            return f"https://app.powerbi.com/groups/{workspace_id}/reports/{report_id}/ReportSection{page_id}"
        return f"https://app.powerbi.com/groups/{workspace_id}/reports/{report_id}"

    def _parse_report_pages(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse page definitions from PBIR report structure."""
        pages = []
        for part in report_parts:
            path = part.get("path", "")
            path_lower = path.lower()
            is_page_file = (
                ("/pages/" in path_lower and path_lower.endswith("/page.json")) or
                path_lower.endswith("/page.json")
            )
            if is_page_file:
                try:
                    payload = part.get("payload", "")
                    content = base64.b64decode(payload).decode("utf-8")
                    page_data = json.loads(content)
                    path_parts = path.split("/")
                    page_id = None
                    for tag in ("pages", "Pages"):
                        if tag in path_parts:
                            idx = path_parts.index(tag) + 1
                            page_id = path_parts[idx] if idx < len(path_parts) else None
                            break
                    if not page_id:
                        page_id = path_parts[-2] if len(path_parts) >= 2 else "unknown"
                    pages.append({
                        "id": page_id,
                        "name": page_data.get("name", page_id),
                        "displayName": page_data.get("displayName", page_data.get("name", page_id)),
                        "ordinal": page_data.get("ordinal", 0),
                    })
                except Exception:
                    pass

        if not pages:
            pages = self._parse_pages_from_report_json(report_parts)
        pages.sort(key=lambda p: p.get("ordinal", 0))
        return pages

    def _parse_pages_from_report_json(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pages = []
        for part in report_parts:
            path = part.get("path", "")
            if path.lower() == "report.json" or path.lower().endswith("/report.json"):
                try:
                    content = base64.b64decode(part.get("payload", "")).decode("utf-8")
                    report_data = json.loads(content)
                    pages_data = report_data.get("pages") or report_data.get("sections") or report_data.get("reportPages")
                    if pages_data and isinstance(pages_data, list):
                        for idx, page_data in enumerate(pages_data):
                            if isinstance(page_data, dict):
                                page_id = page_data.get("name") or page_data.get("id") or f"page_{idx}"
                                pages.append({
                                    "id": page_id,
                                    "name": page_data.get("name", page_id),
                                    "displayName": page_data.get("displayName", page_data.get("name", page_id)),
                                    "ordinal": page_data.get("ordinal", idx),
                                })
                except Exception:
                    pass
        return pages

    def _parse_report_visuals(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse visual definitions from PBIR report structure."""
        visuals = []
        for part in report_parts:
            path = part.get("path", "")
            path_lower = path.lower()
            is_visual_file = (
                ("/visuals/" in path_lower and path_lower.endswith("/visual.json")) or
                ("/visuals/" in path_lower and path_lower.endswith(".json"))
            )
            if is_visual_file:
                try:
                    content = base64.b64decode(part.get("payload", "")).decode("utf-8")
                    visual_data = json.loads(content)
                    path_parts = path.split("/")
                    page_id = None
                    for tag in ("pages", "Pages"):
                        if tag in path_parts:
                            idx = path_parts.index(tag) + 1
                            page_id = path_parts[idx] if idx < len(path_parts) else None
                            break
                    visual_id = None
                    for tag in ("visuals", "Visuals"):
                        if tag in path_parts:
                            idx = path_parts.index(tag) + 1
                            visual_id = path_parts[idx] if idx < len(path_parts) else None
                            break
                    if not visual_id:
                        visual_id = path_parts[-2] if len(path_parts) >= 2 else "unknown"
                    visuals.append({
                        "id": visual_id, "page_id": page_id,
                        "type": visual_data.get("visual", {}).get("visualType", "unknown"),
                        "config": visual_data,
                    })
                except Exception:
                    pass

        if not visuals:
            visuals = self._parse_visuals_from_report_json(report_parts)
        return visuals

    def _parse_visuals_from_report_json(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        visuals = []
        for part in report_parts:
            path = part.get("path", "")
            if path.lower() == "report.json" or path.lower().endswith("/report.json"):
                try:
                    content = base64.b64decode(part.get("payload", "")).decode("utf-8")
                    report_data = json.loads(content)
                    pages_data = report_data.get("pages") or report_data.get("sections") or report_data.get("reportPages")
                    if pages_data and isinstance(pages_data, list):
                        for page_data in pages_data:
                            if not isinstance(page_data, dict):
                                continue
                            page_id = page_data.get("name") or page_data.get("id")
                            visuals_data = page_data.get("visualContainers") or page_data.get("visuals")
                            if visuals_data and isinstance(visuals_data, list):
                                for vis_idx, vis_data in enumerate(visuals_data):
                                    if not isinstance(vis_data, dict):
                                        continue
                                    visual_id = vis_data.get("name") or vis_data.get("id") or f"visual_{vis_idx}"
                                    visual_type = "unknown"
                                    parsed_config = {}
                                    if "config" in vis_data:
                                        config_str = vis_data.get("config", "")
                                        if isinstance(config_str, str):
                                            try:
                                                parsed_config = json.loads(config_str)
                                                visual_type = parsed_config.get("singleVisual", {}).get("visualType", "unknown")
                                            except json.JSONDecodeError:
                                                pass
                                        elif isinstance(config_str, dict):
                                            parsed_config = config_str
                                            visual_type = parsed_config.get("singleVisual", {}).get("visualType", "unknown")
                                    elif "visualType" in vis_data:
                                        visual_type = vis_data.get("visualType", "unknown")
                                        parsed_config = vis_data
                                    visuals.append({
                                        "id": visual_id, "page_id": page_id,
                                        "type": visual_type, "config": parsed_config,
                                    })
                except Exception:
                    pass
        return visuals

    def _extract_measures_from_visual(self, visual: Dict[str, Any]) -> List[str]:
        """Extract measure names referenced in a visual configuration."""
        measures: set = set()
        config = visual.get("config", {})
        if isinstance(config, str):
            try:
                config = json.loads(config)
            except json.JSONDecodeError:
                return []
        self._find_measures_in_dict(config, measures)
        return list(measures)

    def _find_measures_in_dict(self, obj: Any, measures: set) -> None:
        """Recursively search for measure references in a dictionary."""
        if isinstance(obj, dict):
            if "measure" in obj:
                measure_ref = obj["measure"]
                if isinstance(measure_ref, dict):
                    name = measure_ref.get("property") or measure_ref.get("name")
                    if name:
                        measures.add(name)
                elif isinstance(measure_ref, str):
                    measures.add(measure_ref)
            if "Measure" in obj:
                measure_ref = obj["Measure"]
                if isinstance(measure_ref, dict):
                    name = (measure_ref.get("Property") or measure_ref.get("property")
                            or measure_ref.get("Name") or measure_ref.get("name"))
                    if name:
                        measures.add(name)
            if obj.get("aggregation") and "property" in obj:
                prop = obj["property"]
                if prop:
                    measures.add(prop)
            for value in obj.values():
                self._find_measures_in_dict(value, measures)
        elif isinstance(obj, list):
            for item in obj:
                self._find_measures_in_dict(item, measures)

    # =====================================================================
    # Trace
    # =====================================================================

    def _emit_llm_trace(
        self, event_context: str, prompt: str, model: str, operation: str, response: Optional[str] = None
    ) -> None:
        """Emit a trace event for LLM operations."""
        try:
            trace_ctx = getattr(self, 'trace_context', None)
            if not trace_ctx or not trace_ctx.get('job_id'):
                return

            from src.services.trace_queue import get_trace_queue
            queue = get_trace_queue()
            max_len = 3000
            trace_output: Dict[str, Any] = {
                'operation': operation, 'model': model,
                'prompt_length': len(prompt),
                'prompt': prompt[:max_len] + ('...[truncated]' if len(prompt) > max_len else ''),
            }
            if response:
                trace_output['response_length'] = len(response)
                trace_output['response'] = response[:max_len] + ('...[truncated]' if len(response) > max_len else '')

            queue.put_nowait({
                'job_id': trace_ctx.get('job_id'),
                'event_type': 'llm_call',
                'event_source': 'PowerBI DAX Generator',
                'event_context': event_context,
                'output': trace_output,
                'extra_data': {'agent_role': 'PowerBI DAX Generator', 'model': model},
                'trace_metadata': {'tool_name': 'PowerBISemanticModelDaxTool', 'operation': operation, 'model': model},
                'group_context': trace_ctx.get('group_context'),
            })
        except Exception as e:
            logger.error(f"[DaxTool] Failed to emit trace: {e}")

    # =====================================================================
    # Output Formatting
    # =====================================================================

    def _format_output(self, results: Dict[str, Any], output_format: str) -> str:
        """Format the results for output."""
        if output_format == "json":
            return json.dumps(results, indent=2, default=str)

        output = []
        output.append("# Power BI Analysis Results\n")
        output.append(f"**Question**: {results['user_question']}")
        output.append(f"**Workspace**: `{results['workspace_id']}`")
        output.append(f"**Dataset**: `{results['dataset_id']}`\n")

        if results.get("errors"):
            output.append("## Errors\n")
            for error in results["errors"]:
                output.append(f"- {error}")
            output.append("")

        ctx = results.get("model_context", {})
        output.append("## Model Context\n")
        output.append(f"- **Measures**: {len(ctx.get('measures', []))}")
        output.append(f"- **Tables**: {len(ctx.get('tables', []))}")
        output.append(f"- **Relationships**: {len(ctx.get('relationships', []))}\n")

        if results.get("generated_dax"):
            output.append("## Generated DAX Query\n")
            dax_attempts = results.get("dax_attempts", [])
            if len(dax_attempts) > 1:
                output.append(f"**Attempts**: {len(dax_attempts)}\n")
                for att in dax_attempts[:-1]:
                    output.append(f"**Attempt {att['attempt']}**: FAILED")
                    if att.get('error'):
                        output.append(f"  - Error: {att['error'][:100]}...")
                output.append(f"**Attempt {dax_attempts[-1]['attempt']}**: SUCCESS\n")
            output.append("```dax")
            output.append(results["generated_dax"])
            output.append("```\n")

        exec_result = results.get("dax_execution", {})
        output.append("## Execution Results\n")
        if exec_result.get("success"):
            output.append(f"**Success** - {exec_result.get('row_count', 0)} rows returned\n")
            data = exec_result.get("data", [])
            if data:
                columns = exec_result.get("columns", list(data[0].keys()) if data else [])
                output.append("| " + " | ".join(str(c).replace("[", "").replace("]", "") for c in columns) + " |")
                output.append("| " + " | ".join(["---"] * len(columns)) + " |")
                for row in data[:20]:
                    values = [str(row.get(c, ""))[:50] for c in columns]
                    output.append("| " + " | ".join(values) + " |")
                if len(data) > 20:
                    output.append(f"\n*... and {len(data) - 20} more rows*")
        else:
            output.append(f"**Failed**: {exec_result.get('error', 'Unknown error')}")
        output.append("")

        if results.get("visual_references"):
            output.append("## Visual References\n")
            report_refs: Dict[str, Any] = {}
            for ref in results["visual_references"]:
                rn = ref.get("report_name", "Unknown")
                if rn not in report_refs:
                    report_refs[rn] = {"report_url": ref.get("report_url", ""), "pages": {}}
                page_name = ref.get("page_name")
                measure = ref.get("measure", "Unknown")
                visual_type = ref.get("visual_type")
                if page_name:
                    if page_name not in report_refs[rn]["pages"]:
                        report_refs[rn]["pages"][page_name] = {
                            "page_url": ref.get("page_url"), "measures": [], "visual_types": set()
                        }
                    report_refs[rn]["pages"][page_name]["measures"].append(measure)
                    if visual_type:
                        report_refs[rn]["pages"][page_name]["visual_types"].add(visual_type)
                else:
                    if "_no_page_" not in report_refs[rn]["pages"]:
                        report_refs[rn]["pages"]["_no_page_"] = {"page_url": None, "measures": [], "visual_types": set()}
                    report_refs[rn]["pages"]["_no_page_"]["measures"].append(measure)

            for rn, rd in report_refs.items():
                output.append(f"\n### {rn}")
                output.append(f"[Open Report]({rd['report_url']})\n")
                for pn, pd in rd["pages"].items():
                    if pn == "_no_page_":
                        output.append(f"- Measures in report: {', '.join(set(pd['measures']))}")
                    else:
                        output.append(f"- **{pn}**: [Open Page]({pd['page_url']})")
                        output.append(f"  - Measures: {', '.join(set(pd['measures']))}")
                        if pd["visual_types"]:
                            output.append(f"  - Visual types: {', '.join(pd['visual_types'])}")

        return "\n".join(output)
