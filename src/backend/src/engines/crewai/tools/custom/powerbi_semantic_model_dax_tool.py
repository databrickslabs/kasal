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

    # ===== MODEL CONTEXT (from Reducer or Fetcher output) =====
    model_context_json: Optional[str] = Field(
        None,
        description="[CRITICAL — REQUIRED] The JSON output from the previous task (Metadata Reducer or Fetcher). "
                    "You MUST pass the full JSON output from the previous task here. "
                    "If not provided, falls back to cached full metadata which may be too large."
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
    context_knowledge: Optional[str] = Field(
        None,
        description="[Context] Business context or domain knowledge to guide DAX generation. "
                    "E.g., 'Complete CGR means the customer completed the full CGR process'."
    )
    reference_dax: Optional[str] = Field(
        None,
        description="[Context] Reference working DAX queries as examples. "
                    "Provide one or more EVALUATE statements that work against this model."
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
        "CRITICAL: You MUST pass the full JSON output from the previous task as 'model_context_json'. "
        "This is the reduced/enriched metadata from the Fetcher or Reducer — copy the entire JSON string. "
        "If model_context_json is not provided, falls back to cached full metadata (not reduced). "
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
            "context_knowledge": kwargs.get("context_knowledge", ""),
            "reference_dax": kwargs.get("reference_dax", ""),
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

            # Log what the agent actually passed — critical for debugging context flow
            kwarg_keys = list(kwargs.keys())
            has_mcj = "model_context_json" in kwargs and kwargs["model_context_json"] is not None
            mcj_len = len(str(kwargs.get("model_context_json", ""))) if has_mcj else 0
            kwarg_question = kwargs.get("user_question")
            default_question = self._default_config.get("user_question")
            logger.info(
                f"[DaxTool] ═══ Agent kwargs ═══ "
                f"keys={kwarg_keys} | model_context_json={'YES (' + str(mcj_len) + ' chars)' if has_mcj else 'NOT PROVIDED'} | "
                f"agent_user_question={'YES' if kwarg_question else 'NULL'} | "
                f"default_user_question={'YES' if default_question else 'NULL'}"
            )
            if has_mcj:
                logger.info(f"[DaxTool] model_context_json preview: {str(kwargs['model_context_json'])[:300]}...")

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
                            "session_id", "visible_tables", "conversation_history",
                            "context_knowledge", "reference_dax"]
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

            auth_method = "SP" if has_sp else "SA" if has_sa else "OAuth" if has_oauth else "NONE"
            has_model_ctx = merged_config.get("model_context_json") is not None
            ctx_len = len(str(merged_config.get("model_context_json", ""))) if has_model_ctx else 0
            logger.info(
                f"[DaxTool] ═══ Config summary ═══ "
                f"auth={auth_method} | question='{(merged_config.get('user_question') or '')[:80]}' | "
                f"model_context_json={'yes (' + str(ctx_len) + ' chars)' if has_model_ctx else 'no'} | "
                f"workspace={merged_config.get('workspace_id', '')[:12]}... | "
                f"dataset={merged_config.get('dataset_id', '')[:12]}... | "
                f"llm={merged_config.get('llm_model')} | "
                f"context_knowledge={'yes' if merged_config.get('context_knowledge') else 'no'} | "
                f"reference_dax={'yes' if merged_config.get('reference_dax') else 'no'}"
            )

            if not has_sp and not has_sa and not has_oauth:
                logger.warning("[DaxTool] ✗ No authentication credentials found")
                return (
                    "Error: Authentication required.\n"
                    "Provide one of:\n"
                    "- Service Principal: tenant_id, client_id, client_secret\n"
                    "- Service Account: tenant_id, client_id, username, password\n"
                    "- User OAuth: access_token"
                )

            logger.info(f"[DaxTool] ═══ Starting pipeline (auth={auth_method}) ═══")
            result = _run_async_in_sync_context(self._execute_dax_pipeline(merged_config))
            logger.info(f"[DaxTool] ═══ Pipeline complete (output={len(result)} chars) ═══")
            return result

        except Exception as e:
            logger.error(f"[DaxTool] ✗ Pipeline exception: {str(e)}", exc_info=True)
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
        logger.info("[DaxTool] Step 1/4: Acquiring access token...")
        try:
            access_token = await self._get_access_token(config)
            logger.info(f"[DaxTool] Step 1/4: ✓ Access token acquired (length={len(access_token)})")
        except Exception as e:
            logger.error(f"[DaxTool] Step 1/4: ✗ Authentication failed: {e}")
            results["errors"].append(f"Authentication error: {str(e)}")
            return self._format_output(results, output_format)

        # Step 2: Resolve model context
        logger.info("[DaxTool] Step 2/4: Resolving model context...")
        model_context = await self._resolve_model_context(config)
        if model_context is None:
            logger.error("[DaxTool] Step 2/4: ✗ No model context — neither model_context_json nor cache hit")
            results["errors"].append(
                "No model context available. Run the 'Power BI Semantic Model Fetcher' tool first, "
                "or provide model_context_json."
            )
            return self._format_output(results, output_format)

        n_tables = len(model_context.get("tables", []))
        n_measures = len(model_context.get("measures", []))
        n_rels = len(model_context.get("relationships", []))
        n_slicers = len(model_context.get("slicers", []))
        n_samples = len(model_context.get("sample_data", {}))
        context_source = model_context.get("_source", "AGENT_JSON" if config.get("model_context_json") else "CACHE")
        logger.info(
            f"[DaxTool] Step 2/4: ✓ Model context resolved (SOURCE={context_source}) — "
            f"tables={n_tables}, measures={n_measures}, relationships={n_rels}, "
            f"slicers={n_slicers}, sample_data_keys={n_samples}"
        )
        results["model_context"] = model_context

        # Merge default filters from context into active_filters
        default_filters = model_context.get("default_filters") if isinstance(model_context.get("default_filters"), dict) else {}

        # Normalize active_filters: UI may send list format
        # [{"table": "T", "column": "C", "value": "V"}] → {"T[C]": "V"}
        existing = config.get("active_filters") or {}
        if isinstance(existing, list):
            normalized = {}
            for f in existing:
                if isinstance(f, dict) and "table" in f and "column" in f:
                    key = f"{f['table']}[{f['column']}]"
                    normalized[key] = f.get("value", "NOT NULL")
            logger.info(f"[DaxTool] Normalized {len(existing)} list-format active_filters → {normalized}")
            existing = normalized
        elif not isinstance(existing, dict):
            existing = {}

        if default_filters or existing:
            config["active_filters"] = {**default_filters, **existing}
            logger.info(f"[DaxTool] Merged filters: {len(default_filters)} default + {len(existing)} active = {len(config['active_filters'])} total")

        # Ensure all filter tables are present in model_context
        # UI active_filters may reference tables not in the reduced cache.
        # Pull missing tables from the full cache so schema validation passes.
        all_filters = config.get("active_filters", {})
        if all_filters and model_context:
            existing_tables = {t["name"] for t in model_context.get("tables", [])}
            missing_tables = set()
            for filter_key in all_filters:
                filter_table = filter_key.split("[")[0] if "[" in filter_key else ""
                if filter_table and filter_table not in existing_tables:
                    missing_tables.add(filter_table)

            if missing_tables:
                logger.info(f"[DaxTool] Filter tables missing from model context: {missing_tables}. Fetching from full cache...")
                try:
                    full_cache = await self._fetch_full_cache_tables(config, missing_tables)
                    if full_cache:
                        for table_data in full_cache.get("tables", []):
                            model_context["tables"].append(table_data)
                            existing_tables.add(table_data["name"])
                        for rel in full_cache.get("relationships", []):
                            model_context.setdefault("relationships", []).append(rel)
                        for k, v in full_cache.get("sample_data", {}).items():
                            model_context.setdefault("sample_data", {})[k] = v
                        logger.info(
                            f"[DaxTool] Added {len(full_cache['tables'])} tables from full cache: "
                            f"{[t['name'] for t in full_cache['tables']]}"
                        )
                except Exception as e:
                    logger.warning(f"[DaxTool] Failed to fetch missing filter tables from full cache: {e}")

        # Step 3: Generate + Execute DAX with retry
        # Strategy: attempt 1 = LLM, attempt 2 = LLM self-correction,
        # attempt 3 = deterministic fallback (no LLM), then 2 more LLM retries
        max_retries = config.get("max_dax_retries", 5)
        dax_attempts: List[Dict[str, Any]] = []

        # Initialize prompt tracker — will be set by _generate_dax_with_llm / _generate_dax_with_self_correction
        self._last_llm_prompt = None

        if model_context.get("measures") or model_context.get("tables"):
            logger.info(f"[DaxTool] Step 3/4: DAX generation + execution (max_retries={max_retries})")
            # Count consecutive LLM failures to trigger deterministic fallback
            consecutive_llm_failures = 0

            for attempt in range(max_retries):
                try:
                    generated_dax = None

                    # After 2 consecutive LLM failures, try deterministic approach
                    if consecutive_llm_failures >= 2 and attempt >= 2:
                        logger.info(f"[DaxTool] Step 3/4: Attempt {attempt + 1}/{max_retries} — DETERMINISTIC fallback (LLM failed {consecutive_llm_failures}x)")
                        generated_dax = self._generate_deterministic_dax(user_question, model_context)
                        consecutive_llm_failures = 0  # Reset so next attempt tries LLM again
                    elif attempt == 0:
                        logger.info(f"[DaxTool] Step 3/4: Attempt {attempt + 1}/{max_retries} — generating DAX via LLM...")
                        generated_dax = await self._generate_dax_with_llm(user_question, model_context, config)
                    else:
                        logger.info(f"[DaxTool] Step 3/4: Attempt {attempt + 1}/{max_retries} — self-correction retry...")
                        generated_dax = await self._generate_dax_with_self_correction(
                            user_question, model_context, config, dax_attempts
                        )

                    results["generated_dax"] = generated_dax
                    # Capture the full LLM prompt used for this attempt
                    if self._last_llm_prompt:
                        results["llm_prompt"] = self._last_llm_prompt

                    if generated_dax:
                        logger.info(f"[DaxTool] Step 3/4: Generated DAX ({len(generated_dax)} chars): {generated_dax[:200]}...")

                        # Pre-execution validation: check all table references exist in schema
                        validation_error = self._validate_dax_references(generated_dax, model_context)
                        if validation_error:
                            logger.warning(f"[DaxTool] Step 3/4: ✗ DAX VALIDATION FAILED attempt {attempt + 1} — {validation_error}")
                            execution_result = {"success": False, "error": validation_error, "row_count": 0}
                            consecutive_llm_failures += 1
                        else:
                            logger.info("[DaxTool] Step 3/4: Executing DAX against Power BI API...")
                            execution_result = await self._execute_dax_query(
                                workspace_id, dataset_id, access_token, generated_dax
                            )
                        if not isinstance(execution_result, dict):
                            execution_result = {"success": False, "error": f"Invalid result type: {type(execution_result).__name__}", "row_count": 0}

                        if execution_result.get("success", False):
                            # Post-execution: check if DAX actually addresses the user question
                            completeness_error = self._validate_dax_completeness(
                                generated_dax, user_question, model_context
                            )
                            if completeness_error and attempt < max_retries - 1:
                                logger.warning(
                                    f"[DaxTool] Step 3/4: ✗ DAX executed but is INCOMPLETE on attempt {attempt + 1} — {completeness_error}"
                                )
                                dax_attempts.append({
                                    "attempt": attempt + 1,
                                    "dax": generated_dax,
                                    "success": False,
                                    "error": completeness_error,
                                    "row_count": execution_result.get("row_count", 0),
                                })
                                consecutive_llm_failures += 1
                                continue
                            # DAX is complete and valid
                            dax_attempts.append({
                                "attempt": attempt + 1,
                                "dax": generated_dax,
                                "success": True,
                                "error": None,
                                "row_count": execution_result.get("row_count", 0),
                            })
                            results["dax_execution"] = execution_result
                            logger.info(f"[DaxTool] Step 3/4: ✓ DAX SUCCESS on attempt {attempt + 1} — rows={execution_result.get('row_count', 0)}")
                            break
                        else:
                            dax_attempts.append({
                                "attempt": attempt + 1,
                                "dax": generated_dax,
                                "success": False,
                                "error": execution_result.get("error"),
                                "row_count": execution_result.get("row_count", 0),
                            })
                            results["dax_execution"] = execution_result
                            logger.warning(f"[DaxTool] Step 3/4: ✗ DAX FAILED attempt {attempt + 1} — {execution_result.get('error', 'unknown')[:200]}")
                            consecutive_llm_failures += 1
                            if attempt == max_retries - 1:
                                results["errors"].append(f"DAX failed after {max_retries} attempts: {execution_result.get('error')}")
                    else:
                        logger.warning(f"[DaxTool] Step 3/4: ✗ LLM returned empty/garbage DAX on attempt {attempt + 1}")
                        dax_attempts.append({
                            "attempt": attempt + 1, "dax": "(empty)",
                            "success": False, "error": "LLM returned empty or invalid response", "row_count": 0,
                        })
                        consecutive_llm_failures += 1
                        if attempt == max_retries - 1:
                            results["errors"].append("Failed to generate valid DAX query")

                except Exception as e:
                    logger.error(f"[DaxTool] Step 3/4: ✗ Exception on attempt {attempt + 1}: {e}", exc_info=True)
                    dax_attempts.append({
                        "attempt": attempt + 1, "dax": results.get("generated_dax"),
                        "success": False, "error": str(e), "row_count": 0,
                    })
                    consecutive_llm_failures += 1
                    if attempt == max_retries - 1:
                        results["errors"].append(f"DAX error after {max_retries} attempts: {str(e)}")
        else:
            logger.warning("[DaxTool] Step 3/4: SKIPPED — no measures and no tables in model context")

        results["dax_attempts"] = dax_attempts

        # Step 4: Visual references
        logger.info(f"[DaxTool] Step 4/4: Visual references (include={config.get('include_visual_references', True)})")
        if config.get("include_visual_references", True) and model_context.get("measures"):
            try:
                used_measures = self._extract_measures_from_dax(
                    results["generated_dax"] or "",
                    [m["name"] for m in model_context["measures"]],
                )
                if used_measures:
                    logger.info(f"[DaxTool] Step 4/4: Looking up visual refs for measures: {used_measures}")
                    visual_refs = await self._find_visual_references(
                        workspace_id, dataset_id, access_token, used_measures
                    )
                    results["visual_references"] = visual_refs
                    logger.info(f"[DaxTool] Step 4/4: ✓ Found {len(visual_refs)} visual references")
                else:
                    logger.info("[DaxTool] Step 4/4: No measures found in generated DAX, skipping visual refs")
            except Exception as e:
                logger.warning(f"[DaxTool] Step 4/4: ✗ Visual reference error: {e}")
                results["errors"].append(f"Visual reference error: {str(e)}")

        n_errors = len(results.get("errors", []))
        n_attempts = len(dax_attempts)
        success = results.get("dax_execution", {}).get("success", False)
        logger.info(
            f"[DaxTool] ═══ Pipeline result: success={success}, attempts={n_attempts}, "
            f"errors={n_errors}, rows={results.get('dax_execution', {}).get('row_count', 0)} ═══"
        )
        return self._format_output(results, output_format)

    async def _resolve_model_context(self, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Resolve model context with 3-tier priority.

        Priority 1: model_context_json (agent passes reduced JSON directly)
        Priority 2: Reduced cache (Reducer saved with report_id='reduced')
        Priority 3: Full cache (Fetcher's original full metadata)
        """
        dataset_id = config.get("dataset_id")
        workspace_id = config.get("workspace_id")
        report_id = config.get("report_id")
        group_id = config.get("group_id", "default")

        # ── Priority 1: model_context_json provided directly ──
        model_context_json = config.get("model_context_json")
        if model_context_json:
            logger.info(f"[DaxTool] _resolve_model_context: Trying model_context_json ({len(str(model_context_json))} chars)")
            try:
                parsed = json.loads(model_context_json) if isinstance(model_context_json, str) else model_context_json
                # Skip if this is a Reducer summary (no tables) — fall through to cache
                if not parsed.get("tables") and not parsed.get("schema", {}).get("tables"):
                    logger.info(
                        f"[DaxTool] _resolve_model_context: model_context_json has no tables "
                        f"(likely Reducer summary), falling through to cache. "
                        f"Keys: {list(parsed.keys())}"
                    )
                else:
                    result = self._parse_context_dict(parsed)
                    result["_source"] = "AGENT_JSON"
                    logger.info(
                        f"[DaxTool] _resolve_model_context: ✓ SOURCE=AGENT_JSON (model_context_json) — "
                        f"tables={len(result['tables'])}, measures={len(result['measures'])}, "
                        f"relationships={len(result['relationships'])}, slicers={len(result['slicers'])}"
                    )
                    return result
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"[DaxTool] _resolve_model_context: ✗ Failed to parse model_context_json: {e}")
        else:
            logger.info("[DaxTool] _resolve_model_context: No model_context_json from agent, trying reduced cache...")

        if not dataset_id or not workspace_id:
            logger.warning(f"[DaxTool] _resolve_model_context: ✗ Cannot do cache lookup — dataset_id={dataset_id}, workspace_id={workspace_id}")
            return None

        # ── Priority 2: Reduced cache (saved by Metadata Reducer with report_id='reduced') ──
        try:
            async with async_session_factory() as session:
                cache_service = PowerBISemanticModelCacheService(session)
                reduced = await cache_service.get_cached_metadata(
                    group_id=group_id, dataset_id=dataset_id,
                    workspace_id=workspace_id, report_id="reduced",
                )
            if reduced:
                # Reduced output has tables/measures at top level (not nested under schema)
                result = self._parse_context_dict(reduced)
                result["_source"] = "REDUCED_CACHE"
                logger.info(
                    f"[DaxTool] _resolve_model_context: ✓ SOURCE=REDUCED_CACHE (report_id='reduced') — "
                    f"tables={len(result['tables'])}, measures={len(result['measures'])}, "
                    f"relationships={len(result['relationships'])}, slicers={len(result['slicers'])}"
                )
                return result
            else:
                logger.info("[DaxTool] _resolve_model_context: No reduced cache found, trying full cache...")
        except Exception as e:
            logger.warning(f"[DaxTool] _resolve_model_context: Reduced cache lookup failed: {e}")

        # ── Priority 3: Full cache (Fetcher's original metadata) ──
        use_any_report = not report_id
        logger.info(f"[DaxTool] _resolve_model_context: Full cache lookup — group={group_id}, dataset={dataset_id}, workspace={workspace_id[:12]}..., report_id={report_id or 'ANY'}")
        try:
            async with async_session_factory() as session:
                cache_service = PowerBISemanticModelCacheService(session)
                cached = await cache_service.get_cached_metadata(
                    group_id=group_id, dataset_id=dataset_id,
                    workspace_id=workspace_id, report_id=report_id,
                    any_report_id=use_any_report,
                )
            if cached:
                result = {
                    "measures": cached.get("measures", []),
                    "relationships": cached.get("relationships", []),
                    "tables": cached.get("schema", {}).get("tables", []),
                    "columns": cached.get("schema", {}).get("columns", []),
                    "sample_data": cached.get("sample_data", {}),
                    "default_filters": cached.get("default_filters", {}),
                    "slicers": cached.get("slicers", []),
                    "_source": "FULL_CACHE",
                }
                logger.info(
                    f"[DaxTool] _resolve_model_context: ✓ SOURCE=FULL_CACHE (NOT reduced!) — "
                    f"tables={len(result['tables'])}, measures={len(result['measures'])}"
                )
                logger.warning(
                    "[DaxTool] ⚠ Using FULL cache. Reducer either didn't run or didn't save reduced cache."
                )
                return result
            else:
                logger.warning("[DaxTool] _resolve_model_context: ✗ No cache found at all")
        except Exception as e:
            logger.warning(f"[DaxTool] _resolve_model_context: Full cache lookup exception: {e}")

        return None

    async def _fetch_full_cache_tables(
        self, config: Dict[str, Any], needed_tables: set
    ) -> Optional[Dict[str, Any]]:
        """Fetch specific tables from the full (non-reduced) cache.

        Used when active_filters reference tables not in the reduced context.
        Returns dict with 'tables', 'relationships', 'sample_data' for the
        requested tables only.
        """
        group_id = config.get("group_id", "default")
        dataset_id = config.get("dataset_id")
        workspace_id = config.get("workspace_id")
        if not dataset_id or not workspace_id:
            return None

        async with async_session_factory() as session:
            cache_service = PowerBISemanticModelCacheService(session)
            cached = await cache_service.get_cached_metadata(
                group_id=group_id, dataset_id=dataset_id,
                workspace_id=workspace_id, any_report_id=True,
            )
        if not cached:
            return None

        all_tables = cached.get("schema", {}).get("tables", [])
        all_relationships = cached.get("relationships", [])
        all_sample_data = cached.get("sample_data", {})

        found_tables = [t for t in all_tables if t.get("name") in needed_tables]
        found_names = {t["name"] for t in found_tables}
        found_rels = [
            r for r in all_relationships
            if r.get("from_table") in found_names or r.get("to_table") in found_names
        ]
        found_samples = {}
        for k, v in all_sample_data.items():
            table_name = k.split("[")[0] if "[" in k else k
            if table_name in found_names:
                found_samples[k] = v

        return {"tables": found_tables, "relationships": found_rels, "sample_data": found_samples}

    @staticmethod
    def _parse_context_dict(parsed: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a context dict — handles both Reducer format (top-level)
        and Fetcher cache format (nested under 'schema')."""
        tables = parsed.get("tables", [])
        columns = parsed.get("columns", [])
        # Fetcher cache nests under 'schema'
        if not tables and "schema" in parsed:
            tables = parsed["schema"].get("tables", [])
            columns = parsed["schema"].get("columns", [])
        return {
            "measures": parsed.get("measures", []),
            "relationships": parsed.get("relationships", []),
            "tables": tables,
            "columns": columns,
            "sample_data": parsed.get("sample_data", {}),
            "default_filters": parsed.get("default_filters", {}),
            "slicers": parsed.get("slicers", []),
        }

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
        """Build enriched semantic context for LLM prompt.

        Designed for strictness: starts with an explicit ALLOWED TABLES whitelist,
        then schema, relationships, sample data. Keeps instructions minimal so the
        LLM cannot hallucinate tables not in the reduced context.
        """
        sections = []
        tables = model_context.get("tables", [])
        measures = model_context.get("measures", [])
        relationships = model_context.get("relationships", [])

        # Collect all table names — this is the hard whitelist
        tables_seen = set()
        for table in tables:
            tables_seen.add(table["name"])

        # ── HARD WHITELIST — must come first ──
        table_list = ", ".join(sorted(tables_seen))
        sections.append(f"ALLOWED TABLES (use ONLY these): {table_list}")
        sections.append("ANY table not in this list will cause an error.\n")

        # ── Schema ──
        for table in tables:
            table_name = table["name"]
            columns = table.get("columns", [])
            column_types = table.get("column_types", {})
            col_parts = []
            for col in columns:
                col_type = column_types.get(col, "")
                col_parts.append(f"{col} ({col_type})" if col_type else col)
            sections.append(f"TABLE {table_name}: {', '.join(col_parts)}")

        sections.append("")

        # ── Measures ──
        if measures:
            for measure in measures:
                expr = measure.get("expression", "")[:120]
                sections.append(f"MEASURE [{measure['name']}] on {measure.get('table', '?')}: {expr}")
            sections.append("")

        # ── Relationships ──
        if relationships:
            relevant = [r for r in relationships if r['from_table'] in tables_seen or r['to_table'] in tables_seen]
            if relevant:
                sections.append("RELATIONSHIPS:")
                for rel in relevant:
                    sections.append(f"  {rel['from_table']}[{rel['from_column']}] -> {rel['to_table']}[{rel['to_column']}]")
                sections.append("")

        # ── Sample data (ALL from reduced context — critical for filter matching) ──
        sample_values = model_context.get("sample_data", {}) or model_context.get("sample_values", {})
        if sample_values:
            sections.append("SAMPLE VALUES:")
            for column, value_info in sample_values.items():
                if value_info.get("type") in ("categorical", "slicer_values"):
                    values = value_info.get("sample_values", [])
                    # Show up to 15 values per column (enough for filter matching)
                    sections.append(f"  {column}: {', '.join([str(v) for v in values[:15]])}")
            sections.append("")

        # ── Active filters (only for tables in the model) ──
        active_filters = config.get("active_filters", {})
        if active_filters:
            relevant_filters = {}
            skipped_filters = []
            for filter_name, filter_value in active_filters.items():
                filter_table = filter_name.split("[")[0] if "[" in filter_name else None
                if filter_table and filter_table not in tables_seen:
                    skipped_filters.append(filter_name)
                    continue
                relevant_filters[filter_name] = filter_value

            if skipped_filters:
                logger.info(
                    f"[DaxTool] Skipped {len(skipped_filters)} active filters "
                    f"referencing tables not in model context: {skipped_filters}"
                )

            if relevant_filters:
                sections.append("ACTIVE FILTERS (must apply):")
                for filter_name, filter_value in relevant_filters.items():
                    if isinstance(filter_value, list):
                        quoted = ', '.join([f'"{v}"' for v in filter_value])
                        sections.append(f"  {filter_name} IN ({quoted})")
                    elif isinstance(filter_value, str):
                        val = filter_value.strip()
                        if val.upper() == "NOT NULL":
                            sections.append(f"  {filter_name} is not blank")
                        elif val.upper().startswith("NOT STARTS WITH"):
                            prefix = val[len("NOT STARTS WITH"):].strip().strip("'\"")
                            sections.append(f'  {filter_name} does NOT start with "{prefix}"')
                        elif val.upper().startswith("STARTS WITH"):
                            prefix = val[len("STARTS WITH"):].strip().strip("'\"")
                            sections.append(f'  {filter_name} starts with "{prefix}"')
                        else:
                            sections.append(f'  {filter_name} = "{val}"')
                    else:
                        sections.append(f"  {filter_name} = {filter_value}")
                sections.append("")

        # ── Business terminology ──
        business_mappings = config.get("business_mappings", {})
        if business_mappings:
            sections.append("BUSINESS TERMS:")
            for term, expression in business_mappings.items():
                sections.append(f'  "{term}" -> {expression}')
            sections.append("")

        field_synonyms = config.get("field_synonyms", {})
        if field_synonyms:
            for field, synonyms in field_synonyms.items():
                sections.append(f"SYNONYM {field}: {', '.join(synonyms)}")
            sections.append("")

        # ── Context knowledge ──
        context_knowledge = config.get("context_knowledge", "")
        if context_knowledge:
            sections.append(f"BUSINESS CONTEXT: {context_knowledge}")
            sections.append("")

        # ── Reference DAX ──
        reference_dax = config.get("reference_dax", "")
        if reference_dax:
            sections.append(f"REFERENCE DAX:\n{reference_dax}")
            sections.append("")

        # ── DAX Skeleton (from Metadata Reducer) ──
        dax_skeleton = model_context.get("dax_skeleton", {})
        if dax_skeleton and dax_skeleton.get("skeleton"):
            sections.append("DAX SKELETON (use as starting point — fill in placeholders):")
            sections.append(dax_skeleton["skeleton"])
            if dax_skeleton.get("strategy_notes"):
                for note in dax_skeleton["strategy_notes"]:
                    sections.append(f"  NOTE: {note}")
            if dax_skeleton.get("open_placeholders"):
                sections.append(f"  PLACEHOLDERS TO FILL: {', '.join(dax_skeleton['open_placeholders'])}")
            sections.append("")

        # ── Measure Resolution Flags ──
        resolution_warnings = []
        for m in measures:
            resolution = m.get("_resolution", {})
            flags = resolution.get("expression_flags", {})
            if flags.get("has_removefilters"):
                resolution_warnings.append(
                    f"  WARNING: [{m.get('name', '')}] uses REMOVEFILTERS — "
                    f"do NOT call directly in SUMMARIZECOLUMNS. Decompose or use explicit CALCULATE."
                )
            if flags.get("handles_date_internally"):
                resolution_warnings.append(
                    f"  INFO: [{m.get('name', '')}] handles date filtering internally — "
                    f"do NOT add additional date filters for this measure."
                )
        if resolution_warnings:
            sections.append("MEASURE RESOLUTION WARNINGS:")
            sections.extend(resolution_warnings)
            sections.append("")

        # ── Conversation history ──
        conversation_history = config.get("conversation_history", [])
        if conversation_history:
            sections.append("RECENT HISTORY:")
            for i, turn in enumerate(conversation_history[-3:], 1):
                sections.append(f"  Q{i}: {turn.get('question', '')}")
                if turn.get('answer'):
                    sections.append(f"  A{i}: {turn['answer']}")
            sections.append("")

        result = "\n".join(sections)
        logger.info(
            f"[DaxTool] _build_enriched_semantic_context: {len(result)} chars — "
            f"tables={len(tables)}, measures={len(measures)}, "
            f"has_filters={bool(active_filters)}, "
            f"has_context_knowledge={bool(config.get('context_knowledge'))}, "
            f"has_reference_dax={bool(config.get('reference_dax'))}"
        )
        return result

    def _build_example_dax(self, model_context: Dict[str, Any]) -> str:
        """Build a concrete example DAX query from the actual model tables/measures.

        This gives the LLM a working pattern using REAL table/column names so it
        doesn't need to guess or hallucinate.
        """
        tables = model_context.get("tables", [])
        measures = model_context.get("measures", [])
        relationships = model_context.get("relationships", [])

        if not measures or not tables:
            return ""

        measure = measures[0]
        measure_table = measure.get("table", "")

        # Find a dimension table connected to the measure's fact table via relationship
        dim_table = None
        dim_col = None
        for rel in relationships:
            if rel["from_table"] == measure_table and rel["to_table"] != measure_table:
                # Find the dim table object to get a non-key column
                for t in tables:
                    if t["name"] == rel["to_table"]:
                        non_key_cols = [c for c in t.get("columns", []) if c != rel["to_column"]]
                        if non_key_cols:
                            dim_table = t["name"]
                            dim_col = non_key_cols[0]
                            break
                if dim_table:
                    break

        if dim_table and dim_col:
            return (
                f"EVALUATE\n"
                f"SUMMARIZECOLUMNS(\n"
                f'    TREATAS({{"SomeValue"}}, {dim_table}[{dim_col}]),\n'
                f'    "Result", [{measure["name"]}]\n'
                f")"
            )
        else:
            return (
                f"EVALUATE\n"
                f"SUMMARIZECOLUMNS(\n"
                f'    "Result", [{measure["name"]}]\n'
                f")"
            )

    async def _generate_dax_with_llm(
        self, user_question: str, model_context: Dict[str, Any], config: Dict[str, Any]
    ) -> Optional[str]:
        """Generate DAX query using LLM with enriched context."""
        llm_workspace_url = config.get("llm_workspace_url")
        llm_token = config.get("llm_token")
        llm_model = config.get("llm_model", "databricks-claude-sonnet-4")

        if not llm_workspace_url or not llm_token:
            return self._generate_deterministic_dax(user_question, model_context)

        measures = model_context.get("measures", [])
        if not measures:
            logger.warning("[DAX Generation] No measures available")
            return None

        enriched_context = self._build_enriched_semantic_context(model_context, config)
        example_dax = self._build_example_dax(model_context)

        has_filters = "ACTIVE FILTERS" in enriched_context

        # Build a short, strict prompt — no verbose markdown, no flowery instructions
        system_prompt = f"""{enriched_context}
RULES:
1. ONLY use tables from ALLOWED TABLES list. Any other table = error.
2. ONLY use columns listed under each TABLE. Any other column = error.
3. ONLY use measures listed under MEASURE. Do not invent measure names.
4. Use EVALUATE + SUMMARIZECOLUMNS for all queries.
5. For cross-table filters use TREATAS: TREATAS({{"value"}}, table[column])
6. Filters go BEFORE measure expressions in SUMMARIZECOLUMNS.
7. NEVER use CALCULATETABLE with empty first argument.
8. Use LEFT() instead of STARTSWITH().
{"9. Apply the ACTIVE FILTERS listed above." if has_filters else "9. No active filters — only filter if the question asks for it."}

EXAMPLE using this model:
{example_dax}

OUTPUT: Return ONLY the DAX query starting with EVALUATE. No text, no explanation, no markdown."""

        user_prompt = user_question

        # Store full prompt for output/logging
        prompt = f"[SYSTEM]\n{system_prompt}\n\n[USER]\n{user_prompt}"
        self._last_llm_prompt = prompt

        logger.info(f"[DaxTool] ═══ LLM PROMPT (system={len(system_prompt)} chars, user={len(user_prompt)} chars) ═══")
        logger.info(f"[DaxTool] PROMPT START ═══\n{prompt}\n═══ PROMPT END")

        self._emit_llm_trace(event_context="DAX Generation - Prompt", prompt=prompt, model=llm_model, operation="generate_dax")

        url = f"{llm_workspace_url.rstrip('/')}/serving-endpoints/{llm_model}/invocations"
        headers = {"Authorization": f"Bearer {llm_token}", "Content-Type": "application/json"}

        # Try system+user first; fall back to single user message if endpoint rejects system role
        payloads = [
            {
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "max_tokens": 1000,
                "temperature": 0,
            },
            {
                "messages": [
                    {"role": "user", "content": f"{system_prompt}\n\nQUESTION: {user_prompt}"},
                ],
                "max_tokens": 1000,
                "temperature": 0,
            },
        ]

        async with httpx.AsyncClient(timeout=60.0) as client:
            for i, payload in enumerate(payloads):
                try:
                    response = await client.post(url, headers=headers, json=payload)
                    response.raise_for_status()
                    result = response.json()
                    content = result.get("choices", [{}])[0].get("message", {}).get("content", "")

                    if i > 0:
                        logger.info(f"[DaxTool] system role not supported — fell back to single user message")

                    logger.info(f"[DaxTool] RAW LLM RESPONSE ({len(content)} chars): {content[:500]}")

                    self._emit_llm_trace(
                        event_context="DAX Generation - Response", prompt=prompt,
                        response=content, model=llm_model, operation="generate_dax"
                    )

                    dax = self._extract_dax_from_llm_response(content)
                    if not dax:
                        logger.warning(f"[DaxTool] LLM returned no extractable DAX, trying deterministic fallback")
                        return self._generate_deterministic_dax(user_question, model_context)
                    dax = self._auto_wrap_with_report_filters(dax, config)
                    return dax

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400 and i == 0:
                        logger.warning(f"[DaxTool] system+user payload got 400, retrying with single user message...")
                        continue
                    logger.error(f"LLM DAX generation error: {e}")
                    return self._generate_deterministic_dax(user_question, model_context)
                except Exception as e:
                    logger.error(f"LLM DAX generation error: {e}")
                    return self._generate_deterministic_dax(user_question, model_context)

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

        # Build compact error summary from previous attempts
        attempts_parts = []
        for att in previous_attempts:
            error_msg = str(att.get('error', ''))[:200] if not att['success'] else ""
            part = f"Attempt {att['attempt']}: {att['dax']}"
            if error_msg:
                part += f"\nERROR: {error_msg}"
            attempts_parts.append(part)
        attempts_text = "\n".join(attempts_parts)

        enriched_context = self._build_enriched_semantic_context(model_context, config)
        example_dax = self._build_example_dax(model_context)

        system_prompt = f"""{enriched_context}
RULES:
1. ONLY use tables from ALLOWED TABLES list. Any other table = error.
2. Use EVALUATE + SUMMARIZECOLUMNS. Use TREATAS for cross-table filters.
3. NEVER leave CALCULATETABLE first argument empty.
4. Use LEFT() instead of STARTSWITH().

EXAMPLE using this model:
{example_dax}

OUTPUT: Return ONLY the DAX query starting with EVALUATE. No text."""

        user_prompt = f"""SELF-CORRECTION: Previous attempts failed. Generate a DIFFERENT query.

Question: {user_question}

Failed attempts:
{attempts_text}

Use ONLY the ALLOWED TABLES. Use SUMMARIZECOLUMNS with TREATAS. Return ONLY the DAX."""

        # Store full prompt for output/logging
        prompt = f"[SYSTEM]\n{system_prompt}\n\n[USER]\n{user_prompt}"
        self._last_llm_prompt = prompt

        logger.info(f"[DaxTool] ═══ SELF-CORRECTION PROMPT (system={len(system_prompt)} chars, user={len(user_prompt)} chars) ═══")
        logger.info(f"[DaxTool] PROMPT START ═══\n{prompt}\n═══ PROMPT END")

        url = f"{llm_workspace_url.rstrip('/')}/serving-endpoints/{llm_model}/invocations"
        headers = {"Authorization": f"Bearer {llm_token}", "Content-Type": "application/json"}

        # Try system+user first; fall back to single user message if endpoint rejects system role
        payloads = [
            {
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "max_tokens": 1000,
                "temperature": 0,
            },
            {
                "messages": [
                    {"role": "user", "content": f"{system_prompt}\n\n{user_prompt}"},
                ],
                "max_tokens": 1000,
                "temperature": 0,
            },
        ]

        async with httpx.AsyncClient(timeout=60.0) as client:
            for i, payload in enumerate(payloads):
                try:
                    response = await client.post(url, headers=headers, json=payload)
                    response.raise_for_status()
                    result = response.json()
                    content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                    if i > 0:
                        logger.info(f"[DaxTool] self-correction: system role not supported — fell back to single user message")
                    logger.info(f"[DaxTool] RAW SELF-CORRECTION RESPONSE ({len(content)} chars): {content[:500]}")
                    dax = self._extract_dax_from_llm_response(content)
                    if dax:
                        return dax
                    # Empty extraction — LLM returned garbage, return None to trigger retry
                    logger.warning(f"[DaxTool] Self-correction returned no extractable DAX")
                    return None
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 400 and i == 0:
                        logger.warning(f"[DaxTool] self-correction: system+user payload got 400, retrying with single user message...")
                        continue
                    logger.error(f"LLM self-correction error: {e}")
                    return None
                except Exception as e:
                    logger.error(f"LLM self-correction error: {e}")
                    return None

    def _generate_deterministic_dax(self, user_question: str, model_context: Dict[str, Any]) -> Optional[str]:
        """Build DAX deterministically by matching question terms to sample data.

        This is the fallback when the LLM keeps hallucinating. It parses the user
        question, finds matching values in sample_data, and builds TREATAS filters.
        """
        measures = model_context.get("measures", [])
        if not measures:
            return None

        question_lower = user_question.lower()

        # Pick best measure
        best_measure = None
        for measure in measures:
            if any(word in question_lower for word in measure["name"].lower().split()):
                best_measure = measure
                break
        if not best_measure:
            best_measure = measures[0]

        # Build value → column mapping from sample_data
        sample_data = model_context.get("sample_data", {})
        value_to_col: Dict[str, str] = {}  # "italy" → "dim_country[Region]"
        for col_ref, value_info in sample_data.items():
            for val in value_info.get("sample_values", []):
                val_lower = str(val).lower()
                if len(val_lower) > 1:
                    value_to_col[val_lower] = col_ref

        # Also build column name → table[column] mapping for direct column matches
        col_to_ref: Dict[str, str] = {}
        for table in model_context.get("tables", []):
            for col in table.get("columns", []):
                col_to_ref[col.lower()] = f"{table['name']}[{col}]"

        # Find filters from question
        treatas_parts = []
        filter_parts = []
        matched_values = set()

        # Match sample data values against question
        for val_lower, col_ref in sorted(value_to_col.items(), key=lambda x: -len(x[0])):
            if val_lower in question_lower and val_lower not in matched_values:
                # Find original case value
                original_val = val_lower
                for v_info in sample_data.get(col_ref, {}).get("sample_values", []):
                    if str(v_info).lower() == val_lower:
                        original_val = str(v_info)
                        break
                treatas_parts.append(f'    TREATAS({{"{original_val}"}}, {col_ref})')
                matched_values.add(val_lower)

        # Check for numeric patterns like "Week 3", "Month 5"
        number_patterns = re.findall(r'(?:week|month|year|quarter)\s+(\d+)', question_lower)
        if number_patterns:
            for num in number_patterns:
                # Find a column that matches this concept
                for col_lower, col_ref in col_to_ref.items():
                    if col_lower in ("week", "month", "year", "quarter") and col_lower in question_lower:
                        treatas_parts.append(f'    TREATAS({{{num}}}, {col_ref})')
                        break

        # Check for direct column value matches (e.g., "description" = "Complete CGR")
        for table in model_context.get("tables", []):
            for col in table.get("columns", []):
                col_lower = col.lower()
                if col_lower in question_lower and len(col_lower) > 3:
                    # Look for a quoted or named value after the column mention
                    col_ref = f"{table['name']}[{col}]"
                    # Check if any sample value for this column appears in the question
                    for sv_col_ref, sv_info in sample_data.items():
                        if sv_col_ref == col_ref:
                            for sv in sv_info.get("sample_values", []):
                                sv_lower = str(sv).lower()
                                if sv_lower in question_lower and sv_lower not in matched_values and len(sv_lower) > 2:
                                    filter_parts.append(
                                        f'    FILTER(VALUES({col_ref}), {col_ref} = "{sv}")'
                                    )
                                    matched_values.add(sv_lower)

        # Assemble DAX
        parts = treatas_parts + filter_parts
        if parts:
            filters_str = ",\n".join(parts)
            dax = (
                f"EVALUATE\n"
                f"SUMMARIZECOLUMNS(\n"
                f"{filters_str},\n"
                f'    "Result", [{best_measure["name"]}]\n'
                f")"
            )
        else:
            dax = f'EVALUATE\nSUMMARIZECOLUMNS(\n    "Result", [{best_measure["name"]}]\n)'

        logger.info(f"[DaxTool] Deterministic DAX fallback ({len(parts)} filters): {dax}")
        return dax

    @staticmethod
    def _validate_dax_references(dax: str, model_context: Dict[str, Any]) -> Optional[str]:
        """Validate that all table references in DAX exist in the model schema.

        Returns an error string if invalid references found, None if valid.
        """
        # Build set of known table names from model context
        known_tables = set()
        for table in model_context.get("tables", []):
            known_tables.add(table["name"])

        # Extract all table[column] references from DAX
        refs = re.findall(r'(\w+)\[([^\]]+)\]', dax)
        unknown_tables = set()
        for table_name, _ in refs:
            # Skip DAX functions that use bracket syntax (VALUES, FILTER, etc.)
            if table_name.upper() in (
                "EVALUATE", "SUMMARIZECOLUMNS", "CALCULATETABLE", "CALCULATE",
                "FILTER", "VALUES", "ALL", "ALLEXCEPT", "TREATAS", "ADDCOLUMNS",
                "SELECTCOLUMNS", "TOPN", "RELATED", "RELATEDTABLE", "REMOVEFILTERS",
                "KEEPFILTERS", "USERELATIONSHIP", "CROSSFILTER", "DISTINCT",
                "DATATABLE", "ROW", "UNION", "INTERSECT", "EXCEPT", "GENERATE",
                "GENERATESERIES", "NATURALINNERJOIN", "NATURALLEFTOUTERJOIN",
            ):
                continue
            if table_name not in known_tables:
                unknown_tables.add(table_name)

        if unknown_tables:
            known_list = ", ".join(sorted(known_tables))
            unknown_list = ", ".join(sorted(unknown_tables))
            return (
                f"SCHEMA VALIDATION ERROR: DAX references tables not in the model schema. "
                f"Unknown tables: [{unknown_list}]. "
                f"Available tables: [{known_list}]. "
                f"ONLY use tables from the SEMANTIC MODEL SCHEMA section."
            )

        # Check for empty CALCULATETABLE first argument
        if re.search(r'CALCULATETABLE\s*\(\s*,', dax, re.IGNORECASE):
            return (
                "SYNTAX VALIDATION ERROR: CALCULATETABLE has an empty first argument. "
                "CALCULATETABLE requires a table expression as its first argument, e.g. "
                "CALCULATETABLE(SUMMARIZECOLUMNS(...), filter1, filter2)."
            )

        return None

    @staticmethod
    def _validate_dax_completeness(
        dax: str, user_question: str, model_context: Dict[str, Any]
    ) -> Optional[str]:
        """Check if the generated DAX actually addresses the user question.

        Extracts key filter terms from the question and checks if the DAX
        contains any filter expressions. Returns error string if the DAX
        appears to be missing filters that the question requires.
        """
        dax_upper = dax.upper()
        question_lower = user_question.lower()

        # Build a map of sample values → their column references
        # e.g., "italy" → "dim_country[BU]" or "dim_country[Region]"
        value_to_columns: Dict[str, List[str]] = {}
        sample_data = model_context.get("sample_data", {})
        for col_ref, value_info in sample_data.items():
            for val in value_info.get("sample_values", []):
                val_lower = str(val).lower()
                if val_lower not in value_to_columns:
                    value_to_columns[val_lower] = []
                value_to_columns[val_lower].append(col_ref)

        # Also check column values from tables for common filter patterns
        for table in model_context.get("tables", []):
            for col in table.get("columns", []):
                col_lower = col.lower()
                # If the question mentions a column name as a whole word, DAX should reference it
                if len(col_lower) > 3 and re.search(r'\b' + re.escape(col_lower) + r'\b', question_lower):
                    col_ref = f"{table['name']}[{col}]"
                    if col_ref not in value_to_columns.get(col_lower, []):
                        if col_lower not in value_to_columns:
                            value_to_columns[col_lower] = []
                        value_to_columns[col_lower].append(col_ref)

        # Find question terms that match sample data values
        # Use word-boundary matching to avoid false positives from substring hits
        # (e.g. country code "it" matching inside "Italy", "pl" inside "complete")
        question_words = set(re.findall(r'\b\w+\b', question_lower))
        missing_filters = []
        for val_lower, col_refs in value_to_columns.items():
            # Skip very short values (2-letter codes cause too many false positives)
            if len(val_lower) < 3:
                continue
            # Require whole-word match for short values (3-5 chars),
            # substring match OK for longer values (6+ chars)
            if len(val_lower) <= 5:
                if val_lower not in question_words:
                    continue
            else:
                if val_lower not in question_lower:
                    continue
            # Check if DAX references any of the associated columns
            found_in_dax = False
            for col_ref in col_refs:
                # Check for table[column] pattern in DAX
                table_name = col_ref.split("[")[0]
                if table_name.upper() in dax_upper:
                    found_in_dax = True
                    break
            if not found_in_dax:
                missing_filters.append(f'"{val_lower}" (from {col_refs[0]})')

        # Check for numeric filters (e.g., "Week 3" → dim_weeks[Week])
        number_patterns = re.findall(r'(?:week|month|year|quarter)\s+(\d+)', question_lower)
        if number_patterns:
            has_any_filter = any(kw in dax_upper for kw in [
                "TREATAS", "FILTER", "CALCULATETABLE", "WHERE",
            ])
            if not has_any_filter:
                missing_filters.append("numeric filter (e.g., Week number)")

        if len(missing_filters) >= 2:
            return (
                f"COMPLETENESS ERROR: The DAX query executed successfully but appears to be "
                f"missing filters that the user question requires. The question mentions "
                f"{', '.join(missing_filters[:5])} but the DAX has no corresponding "
                f"FILTER, TREATAS, or WHERE expressions. Re-generate with proper filters "
                f"using TREATAS for cross-table filtering."
            )

        return None

    def _extract_dax_from_llm_response(self, content: str) -> str:
        """Extract clean DAX query from LLM response.

        Returns empty string if no valid DAX found (must contain EVALUATE + parens).
        """
        logger.info(f"[DaxTool] Raw LLM response ({len(content)} chars): {content[:300]}...")

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
            if paren_depth == 0 and len(clean_lines) > 1:
                break

        dax_query = '\n'.join(clean_lines).strip()

        last_paren = dax_query.rfind(')')
        if last_paren != -1:
            after_paren = dax_query[last_paren + 1:].strip()
            if after_paren and (after_paren.startswith('**') or after_paren.startswith('#') or after_paren.startswith('-')):
                dax_query = dax_query[:last_paren + 1]

        # Reject garbage responses — valid DAX must have EVALUATE + at least one function call
        if len(dax_query) < 30 or '(' not in dax_query:
            logger.warning(f"[DaxTool] Extracted DAX too short or missing parens ({len(dax_query)} chars): {dax_query}")
            return ""

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

        if results.get("llm_prompt"):
            output.append("## Full LLM Prompt Used for DAX Generation\n")
            output.append("<details>\n<summary>Click to expand full prompt sent to LLM</summary>\n")
            output.append("```")
            output.append(results["llm_prompt"])
            output.append("```\n")
            output.append("</details>\n")

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
