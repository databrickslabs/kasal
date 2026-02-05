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

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr
import httpx

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

    # ===== POWER BI CONFIGURATION =====
    workspace_id: Optional[str] = Field(
        None,
        description="[Power BI] Workspace ID (GUID) containing the semantic model."
    )
    dataset_id: Optional[str] = Field(
        None,
        description="[Power BI] Dataset/Semantic Model ID (GUID) to query."
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

    # ===== LLM CONFIGURATION =====
    llm_workspace_url: Optional[str] = Field(
        None,
        description="[LLM] Databricks workspace URL for LLM-based DAX generation."
    )
    llm_token: Optional[str] = Field(
        None,
        description="[LLM] Databricks token for LLM access."
    )
    llm_model: str = Field(
        "databricks-claude-sonnet-4",
        description="[LLM] Model to use for DAX generation."
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


class PowerBIAnalysisTool(BaseTool):
    """
    Power BI Analysis Tool - Question-to-DAX-to-Results Pipeline.

    **Flow**:
    1. **Extract Model Context**: Fetches measures, relationships from semantic model
    2. **Generate DAX**: Uses LLM to convert user question into DAX query
    3. **Execute DAX**: Runs the query via Power BI Execute Queries API
    4. **Find Visual References**: Identifies which reports/visuals use the queried measures

    **Authentication** (choose one):
    - **Service Principal**: client_id + client_secret + tenant_id (App Owns Data)
    - **Service Account**: username + password + client_id + tenant_id (User credentials)
    - **User OAuth**: access_token (pre-obtained token)

    **Use Cases**:
    - Answer business questions using Power BI data
    - Generate and validate DAX queries
    - Understand measure usage across reports
    """

    name: str = "Power BI Comprehensive Analysis"
    description: str = (
        "Analyzes Power BI data by converting business questions into DAX queries. "
        "IMPORTANT: Extract the user's business question from the task description and pass it as 'user_question' parameter. "
        "The tool will: 1) Extract Power BI model context, 2) Generate DAX query using LLM, 3) Execute the query, 4) Return results. "
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
            "skip_system_tables": kwargs.get("skip_system_tables", True),
            "max_dax_retries": kwargs.get("max_dax_retries", 5),
            "output_format": kwargs.get("output_format", "markdown"),
            "user_question": kwargs.get("user_question"),  # Pre-configured question from frontend
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
            config_params = ["workspace_id", "dataset_id", "tenant_id", "client_id",
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
            for key in ["include_visual_references", "skip_system_tables", "max_dax_retries", "output_format"]:
                kwarg_val = filtered_kwargs.get(key)
                default_val = self._default_config.get(key)
                merged_config[key] = kwarg_val if kwarg_val is not None else default_val

            logger.info(f"[PowerBIAnalysisTool] DEFAULT CONFIG user_question: {self._default_config.get('user_question', 'NOT SET')}")
            logger.info(f"[PowerBIAnalysisTool] KWARGS user_question: {filtered_kwargs.get('user_question', 'NOT SET')}")
            logger.info(f"[PowerBIAnalysisTool] MERGED user_question: {merged_config.get('user_question', 'NOT SET')}")
            logger.info(f"[PowerBIAnalysisTool] Merged config - workspace_id: {merged_config.get('workspace_id')}, "
                       f"question: {merged_config.get('user_question', '')[:50] if merged_config.get('user_question') else 'None'}...")

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

        # Step 2: Extract model context (measures, relationships)
        try:
            model_context = await self._extract_model_context(
                workspace_id, dataset_id, access_token, config
            )
            results["model_context"] = model_context
            logger.info(f"Model context extracted: {len(model_context['measures'])} measures, {len(model_context['relationships'])} relationships")
        except Exception as e:
            results["errors"].append(f"Model extraction error: {str(e)}")
            logger.error(f"Model extraction failed: {e}")

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

                        # Store attempt info
                        dax_attempts.append({
                            "attempt": attempt + 1,
                            "dax": generated_dax,
                            "success": execution_result["success"],
                            "error": execution_result.get("error"),
                            "row_count": execution_result.get("row_count", 0)
                        })

                        # If successful, break out of retry loop
                        if execution_result["success"]:
                            results["dax_execution"] = execution_result
                            logger.info(f"✅ DAX execution successful on attempt {attempt + 1}: rows={execution_result['row_count']}")
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

    async def _get_access_token(self, config: Dict[str, Any]) -> str:
        """
        Get OAuth access token using centralized AadService.

        Supports three authentication methods:
        1. Pre-obtained access_token (User OAuth)
        2. Service Principal (client credentials)
        3. Service Account (username/password ROPC flow)

        Uses the shared powerbi_auth_utils module for consistent authentication
        across all Power BI tools.
        """
        from src.engines.crewai.tools.custom.powerbi_auth_utils import get_powerbi_access_token_from_config
        return await get_powerbi_access_token_from_config(config)

    async def _extract_model_context(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Extract measures, relationships, and tables from the semantic model."""
        model_context = {
            "measures": [],
            "relationships": [],
            "tables": []
        }

        # Get Fabric token for TMDL (may need different scope)
        fabric_token = access_token
        try:
            if config.get("tenant_id") and config.get("client_id") and config.get("client_secret"):
                fabric_token = await self._get_fabric_token(config)
        except Exception as e:
            logger.warning(f"Could not get Fabric token, using Power BI token: {e}")

        # Fetch TMDL for measures and tables
        tmdl_parts = await self._fetch_tmdl_definition(workspace_id, dataset_id, fabric_token)
        if tmdl_parts:
            measures, tables = self._parse_tmdl_for_measures_and_tables(tmdl_parts, config)
            model_context["measures"] = measures
            model_context["tables"] = tables

        # Fetch relationships via DAX
        relationships = await self._fetch_relationships(workspace_id, dataset_id, access_token, config)
        model_context["relationships"] = relationships

        return model_context

    async def _get_fabric_token(self, config: Dict[str, Any]) -> str:
        """
        Get Fabric API token for TMDL access.

        Uses the shared powerbi_auth_utils module for consistent authentication.
        Supports Service Principal and Service Account authentication.
        """
        from src.engines.crewai.tools.custom.powerbi_auth_utils import get_fabric_access_token_from_config
        return await get_fabric_access_token_from_config(config)

    async def _fetch_tmdl_definition(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str
    ) -> List[Dict[str, Any]]:
        """Fetch TMDL definition from Fabric API."""
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/getDefinition?format=TMDL"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient(timeout=180.0) as client:
            try:
                response = await client.post(url, headers=headers)

                if response.status_code == 202:
                    # Long-running operation - poll for completion
                    location = response.headers.get("Location")
                    if not location:
                        return []

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
                            logger.error(f"TMDL fetch failed: {poll_data}")
                            return []

                    return []

                elif response.status_code == 200:
                    return response.json().get("definition", {}).get("parts", [])
                else:
                    logger.error(f"TMDL fetch error: {response.status_code}")
                    return []

            except Exception as e:
                logger.error(f"TMDL fetch exception: {e}")
                return []

    def _parse_tmdl_for_measures_and_tables(
        self,
        tmdl_parts: List[Dict[str, Any]],
        config: Dict[str, Any]
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

                # Extract table name
                table_match = re.match(r"table\s+(?:'([^']+)'|(\w+))", tmdl_content.strip())
                if not table_match:
                    continue
                table_name = table_match.group(1) or table_match.group(2)

                # Skip system tables
                if config.get("skip_system_tables", True):
                    if "LocalDateTable" in table_name or "DateTableTemplate" in table_name:
                        continue

                # Add table
                tables.append({"name": table_name})

                # Extract columns
                column_pattern = re.compile(
                    r"column\s+(?:'([^']+)'|(\w+))",
                    re.MULTILINE
                )
                columns = []
                for col_match in column_pattern.finditer(tmdl_content):
                    col_name = col_match.group(1) or col_match.group(2)
                    columns.append(col_name)

                if columns:
                    tables[-1]["columns"] = columns

                # Extract measures
                measure_pattern = re.compile(
                    r"measure\s+(?:'([^']+)'|(\w+))\s*=\s*([\s\S]*?)(?=\n\s*measure|\n\s*column|\n\t[^\t]|\Z)",
                    re.MULTILINE
                )

                for match in measure_pattern.finditer(tmdl_content):
                    measure_name = match.group(1) or match.group(2)
                    expression = match.group(3).strip()

                    # Clean expression (remove metadata lines)
                    clean_lines = []
                    for line in expression.split('\n'):
                        stripped = line.strip()
                        if stripped.startswith(('lineageTag:', 'formatString:', 'annotation', 'isHidden')):
                            break
                        clean_lines.append(line)

                    measures.append({
                        "name": measure_name,
                        "table": table_name,
                        "expression": '\n'.join(clean_lines).strip()
                    })

            except Exception as e:
                logger.warning(f"Error parsing TMDL from {path}: {e}")

        logger.info(f"Parsed {len(measures)} measure(s), {len(tables)} table(s)")
        return measures, tables

    async def _fetch_relationships(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Extract relationships using INFO.VIEW.RELATIONSHIPS() DAX function."""
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "queries": [{"query": "EVALUATE INFO.VIEW.RELATIONSHIPS()"}],
            "serializerSettings": {"includeNulls": True}
        }

        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()

                rows = data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
                relationships = []
                seen_ids = set()

                for row in rows:
                    rel_id = row.get("[ID]")
                    if rel_id in seen_ids:
                        continue
                    seen_ids.add(rel_id)

                    from_table = row.get("[FromTable]", "")
                    to_table = row.get("[ToTable]", "")

                    # Skip system tables
                    if config.get("skip_system_tables", True):
                        if "LocalDateTable" in from_table or "LocalDateTable" in to_table:
                            continue

                    relationships.append({
                        "from_table": from_table,
                        "from_column": row.get("[FromColumn]", ""),
                        "to_table": to_table,
                        "to_column": row.get("[ToColumn]", ""),
                        "is_active": row.get("[IsActive]", True)
                    })

                return relationships

            except Exception as e:
                logger.error(f"Relationships extraction error: {e}")
                return []

    async def _generate_dax_with_llm(
        self,
        user_question: str,
        model_context: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Optional[str]:
        """Generate DAX query using LLM based on user question and model context."""
        llm_workspace_url = config.get("llm_workspace_url")
        llm_token = config.get("llm_token")
        llm_model = config.get("llm_model", "databricks-claude-sonnet-4")

        if not llm_workspace_url or not llm_token:
            # Fallback: Generate simple DAX without LLM
            return self._generate_simple_dax(user_question, model_context)

        # Build context for LLM
        measures = model_context.get("measures", [])
        measures_text = "\n".join([
            f"- {m['name']} (Table: {m['table']}): {m['expression'][:100]}..."
            for m in measures[:20]
        ])

        # Log extracted measures for debugging
        logger.info(f"[DAX Generation] Extracted {len(measures)} measures from model")
        if measures:
            logger.info(f"[DAX Generation] Sample measures: {[m['name'] for m in measures[:5]]}")
        else:
            logger.warning("[DAX Generation] No measures found in model context!")

        tables_text = "\n".join([
            f"- {t['name']}: columns={t.get('columns', [])[:5]}"
            for t in model_context.get("tables", [])[:15]
        ])

        relationships_text = "\n".join([
            f"- {r['from_table']}[{r['from_column']}] -> {r['to_table']}[{r['to_column']}]"
            for r in model_context.get("relationships", [])[:15]
        ])

        # Check if we have measures to work with
        if not measures:
            logger.warning("[DAX Generation] No measures available - cannot generate meaningful query")
            return None

        prompt = f"""You are a DAX query expert. Generate a DAX query to answer the user's question.

## User Question
{user_question}

## Available Measures
{measures_text}

## Available Tables
{tables_text if tables_text else "No tables found"}

## Relationships
{relationships_text if relationships_text else "No relationships found"}

## CRITICAL INSTRUCTIONS
1. **ONLY use measure names from the "Available Measures" list above**
2. **DO NOT invent, modify, or guess measure names**
3. If the question asks for "average", use AVERAGEX or include averaging logic
4. Use EVALUATE with SUMMARIZECOLUMNS or other appropriate DAX functions
5. Return ONLY the DAX query without explanations or markdown formatting
6. The query must be executable via Power BI Execute Queries API

## Examples of CORRECT measure usage:
- If measure is listed as "Total Sales", use [Total Sales]
- If measure is listed as "Revenue Amount", use [Revenue Amount]

## Examples of INCORRECT usage (DO NOT DO THIS):
- DO NOT use [Total_Sales] if only [Total Sales] exists
- DO NOT add suffixes like [measure_doc] or [measure_calc]
- DO NOT modify measure names in any way

## DAX Query:
"""

        # Log the full prompt for debugging
        logger.info("=" * 80)
        logger.info("[DAX Generation] LLM Prompt:")
        logger.info(prompt)
        logger.info("=" * 80)

        # Call Databricks LLM
        url = f"{llm_workspace_url.rstrip('/')}/serving-endpoints/{llm_model}/invocations"
        headers = {
            "Authorization": f"Bearer {llm_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 1000,
            "temperature": 0.1
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                result = response.json()

                # Extract DAX from response
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")

                # Log the raw LLM response
                logger.info("=" * 80)
                logger.info("[DAX Generation] LLM Raw Response:")
                logger.info(content)
                logger.info("=" * 80)

                # Clean up: extract just the DAX query
                dax = self._extract_dax_from_llm_response(content)

                # Log the extracted DAX
                logger.info(f"[DAX Generation] Extracted DAX Query:")
                logger.info(dax)
                logger.info("=" * 80)

                # Validate that generated DAX uses only available measures
                available_measure_names = [m["name"] for m in model_context.get("measures", [])]

                # Check for hallucinated measures - extract all [measure] references
                dax_pattern = r'\[([^\]]+)\]'
                all_references = re.findall(dax_pattern, dax)

                # Filter to get only measure references (not table[column] references)
                table_columns = set()
                for table in model_context.get("tables", []):
                    table_name = table["name"]
                    for col in table.get("columns", []):
                        table_columns.add(f"{table_name}[{col}]")

                # Find references that look like measures (not part of table[column])
                potential_measures = []
                for ref in all_references:
                    # Check if this is part of a table[column] pattern
                    is_table_column = any(f"{table['name']}[{ref}]" in dax for table in model_context.get("tables", []))
                    if not is_table_column:
                        potential_measures.append(ref)

                # Check for measures not in available list
                hallucinated = [m for m in potential_measures if m not in available_measure_names]
                if hallucinated:
                    logger.warning(f"[DAX Generation] LLM may have used non-existent measures: {hallucinated}")
                    logger.warning(f"[DAX Generation] Available measures are: {available_measure_names[:10]}")

                return dax

            except Exception as e:
                logger.error(f"LLM DAX generation error: {e}")
                # Fallback to simple generation
                return self._generate_simple_dax(user_question, model_context)

    def _extract_dax_from_llm_response(self, content: str) -> str:
        """Extract clean DAX query from LLM response."""
        # Remove markdown code blocks
        content = re.sub(r'```dax\s*', '', content)
        content = re.sub(r'```\s*', '', content)

        # Find EVALUATE statement
        evaluate_match = re.search(r'(EVALUATE[\s\S]+?)(?:\n\n|$)', content, re.IGNORECASE)
        if evaluate_match:
            return evaluate_match.group(1).strip()

        return content.strip()

    async def _generate_dax_with_self_correction(
        self,
        user_question: str,
        model_context: Dict[str, Any],
        config: Dict[str, Any],
        previous_attempts: List[Dict[str, Any]]
    ) -> Optional[str]:
        """Generate DAX with self-correction based on previous failed attempts."""
        llm_workspace_url = config.get("llm_workspace_url")
        llm_token = config.get("llm_token")
        llm_model = config.get("llm_model", "databricks-claude-sonnet-4")

        if not llm_workspace_url or not llm_token:
            return None

        # Build context about previous attempts
        attempts_text = "\n\n".join([
            f"### Attempt {att['attempt']}\n"
            f"**DAX Query:**\n```dax\n{att['dax']}\n```\n"
            f"**Result:** {'✅ SUCCESS' if att['success'] else '❌ FAILED'}\n"
            f"**Error:** {att['error']}" if not att['success'] else ""
            for att in previous_attempts
        ])

        # Build measures and tables context
        measures = model_context.get("measures", [])
        measures_text = "\n".join([
            f"- {m['name']} (Table: {m['table']}): {m['expression'][:100]}..."
            for m in measures[:20]
        ])

        tables_text = "\n".join([
            f"- {t['name']}: columns={t.get('columns', [])[:5]}"
            for t in model_context.get("tables", [])[:15]
        ])

        relationships_text = "\n".join([
            f"- {r['from_table']}[{r['from_column']}] -> {r['to_table']}[{r['to_column']}]"
            for r in model_context.get("relationships", [])[:15]
        ])

        # Create self-correction prompt
        prompt = f"""You are a DAX query expert. Your previous attempt(s) to generate a DAX query failed.
Analyze the error(s) and generate a CORRECTED query.

## User Question
{user_question}

## Previous Failed Attempts
{attempts_text}

## Available Measures
{measures_text}

## Available Tables
{tables_text if tables_text else "No tables found"}

## Relationships
{relationships_text if relationships_text else "No relationships found"}

## CRITICAL INSTRUCTIONS FOR CORRECTION
1. **Analyze the error message** from the previous attempt(s)
2. **ONLY use measure names from the "Available Measures" list above**
3. **ONLY use table and column names from the "Available Tables" list above**
4. **DO NOT repeat the same mistake** - generate a DIFFERENT query than before
5. Common DAX errors to avoid:
   - Table or column doesn't exist → Check spelling and use exact names from lists above
   - Invalid relationship → Use only relationships listed above
   - Syntax error → Ensure proper DAX syntax (EVALUATE, SUMMARIZECOLUMNS, etc.)
   - Type mismatch → Ensure columns and measures are used correctly
6. Return ONLY the corrected DAX query without explanations

## Corrected DAX Query:
"""

        # Log the self-correction prompt
        logger.info("=" * 80)
        logger.info(f"[DAX Self-Correction] Attempt {len(previous_attempts) + 1} Prompt:")
        logger.info(prompt)
        logger.info("=" * 80)

        # Call LLM
        url = f"{llm_workspace_url.rstrip('/')}/serving-endpoints/{llm_model}/invocations"
        headers = {
            "Authorization": f"Bearer {llm_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 1000,
            "temperature": 0.1
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                result = response.json()

                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")

                # Log response
                logger.info("=" * 80)
                logger.info("[DAX Self-Correction] LLM Response:")
                logger.info(content)
                logger.info("=" * 80)

                # Extract DAX
                dax = self._extract_dax_from_llm_response(content)

                # Validate against available measures
                available_measure_names = [m["name"] for m in model_context.get("measures", [])]

                # Check for hallucinated measures - extract all [measure] references
                dax_pattern = r'\[([^\]]+)\]'
                all_references = re.findall(dax_pattern, dax)

                # Find references that look like measures (not part of table[column])
                potential_measures = []
                for ref in all_references:
                    is_table_column = any(f"{table['name']}[{ref}]" in dax for table in model_context.get("tables", []))
                    if not is_table_column:
                        potential_measures.append(ref)

                hallucinated = [m for m in potential_measures if m not in available_measure_names]
                if hallucinated:
                    logger.warning(f"[DAX Self-Correction] LLM may have used non-existent measures: {hallucinated}")

                logger.info(f"[DAX Self-Correction] Extracted DAX: {dax[:100]}...")
                return dax

            except Exception as e:
                logger.error(f"LLM self-correction error: {e}")
                return None

    def _generate_simple_dax(self, user_question: str, model_context: Dict[str, Any]) -> Optional[str]:
        """Generate a simple DAX query without LLM."""
        measures = model_context.get("measures", [])
        if not measures:
            return None

        # Find best matching measure based on question keywords
        question_lower = user_question.lower()
        best_measure = None

        for measure in measures:
            measure_name_lower = measure["name"].lower()
            if any(word in question_lower for word in measure_name_lower.split()):
                best_measure = measure
                break

        if not best_measure:
            best_measure = measures[0]

        # Generate simple EVALUATE query
        return f"""EVALUATE
SUMMARIZECOLUMNS(
    "Result", [{best_measure['name']}]
)"""

    async def _execute_dax_query(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        dax_query: str
    ) -> Dict[str, Any]:
        """Execute DAX query via Power BI Execute Queries API."""
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "queries": [{"query": dax_query}],
            "serializerSettings": {"includeNulls": True}
        }

        result = {
            "success": False,
            "data": [],
            "row_count": 0,
            "columns": [],
            "error": None
        }

        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()

                # Check for errors in response
                if "error" in data:
                    result["error"] = data["error"].get("message", str(data["error"]))
                    return result

                # Extract results
                tables = data.get("results", [{}])[0].get("tables", [])
                if tables:
                    rows = tables[0].get("rows", [])
                    result["data"] = rows
                    result["row_count"] = len(rows)
                    result["success"] = True

                    # Extract column names
                    if rows:
                        result["columns"] = list(rows[0].keys())

                return result

            except httpx.HTTPStatusError as e:
                error_text = e.response.text if hasattr(e.response, 'text') else str(e)
                result["error"] = f"HTTP {e.response.status_code}: {error_text}"
                return result
            except Exception as e:
                result["error"] = str(e)
                return result

    def _extract_measures_from_dax(self, dax_query: str, available_measures: List[str]) -> List[str]:
        """Extract measure names used in a DAX query."""
        used_measures = []
        for measure in available_measures:
            # Check for [MeasureName] pattern
            if f"[{measure}]" in dax_query:
                used_measures.append(measure)
        return used_measures

    async def _find_visual_references(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        measures: List[str]
    ) -> List[Dict[str, Any]]:
        """Find reports, pages, and visuals that use the specified measures."""
        visual_refs = []

        # Get reports using this dataset
        reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                reports_response = await client.get(reports_url, headers=headers)
                reports_response.raise_for_status()
                reports_data = reports_response.json()

                # Filter reports using our dataset
                reports = [
                    r for r in reports_data.get("value", [])
                    if r.get("datasetId") == dataset_id
                ]

                for report in reports[:5]:  # Limit to 5 reports
                    report_id = report.get("id")
                    report_name = report.get("name")
                    report_url = report.get("webUrl", "")

                    # Try to fetch detailed page/visual info from report definition
                    try:
                        page_refs = await self._get_measure_page_references(
                            workspace_id, report_id, report_name, report_url,
                            measures, access_token, client
                        )
                        if page_refs:
                            visual_refs.extend(page_refs)
                        else:
                            # Fallback to report-level reference if page parsing fails
                            for measure in measures:
                                visual_refs.append({
                                    "report_name": report_name,
                                    "report_url": report_url,
                                    "page_name": None,
                                    "page_url": None,
                                    "measure": measure,
                                    "visual_type": None,
                                    "note": "Report uses the same dataset - measure likely present"
                                })
                    except Exception as e:
                        logger.warning(f"Could not get page details for report {report_name}: {e}")
                        # Fallback to report-level reference
                        for measure in measures:
                            visual_refs.append({
                                "report_name": report_name,
                                "report_url": report_url,
                                "page_name": None,
                                "page_url": None,
                                "measure": measure,
                                "visual_type": None,
                                "note": "Report uses the same dataset - measure likely present"
                            })

            except Exception as e:
                logger.error(f"Visual reference search error: {e}")

        return visual_refs

    async def _get_measure_page_references(
        self,
        workspace_id: str,
        report_id: str,
        report_name: str,
        report_url: str,
        measures: List[str],
        access_token: str,
        client: httpx.AsyncClient
    ) -> List[Dict[str, Any]]:
        """
        Get page-level references for measures by parsing the report definition.
        Returns list of references with page names and URLs where measures are used.
        """
        refs = []

        # Fetch report definition from Fabric API
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/reports/{report_id}/getDefinition"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        try:
            response = await client.post(url, headers=headers, timeout=60.0)

            if response.status_code == 202:
                # Long-running operation - poll for completion
                location = response.headers.get("Location")
                if not location:
                    return []

                for _ in range(30):  # Max 60 seconds
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
                    return []  # Timeout

            elif response.status_code == 200:
                report_parts = response.json().get("definition", {}).get("parts", [])
            else:
                return []

            if not report_parts:
                return []

            # Parse pages and visuals from report definition
            pages = self._parse_report_pages(report_parts)
            visuals = self._parse_report_visuals(report_parts)

            # Build page lookup
            page_lookup = {p.get("id"): p for p in pages}

            # Find which measures are used in which visuals/pages
            measure_locations = {}  # measure_name -> list of (page_id, visual_type)

            for visual in visuals:
                visual_measures = self._extract_measures_from_visual(visual)
                page_id = visual.get("page_id")
                visual_type = visual.get("type", "unknown")

                for measure in measures:
                    if measure in visual_measures:
                        if measure not in measure_locations:
                            measure_locations[measure] = []
                        measure_locations[measure].append({
                            "page_id": page_id,
                            "visual_type": visual_type
                        })

            # Build references with page info
            for measure in measures:
                if measure in measure_locations:
                    # Measure found in specific pages/visuals
                    seen_pages = set()
                    for loc in measure_locations[measure]:
                        page_id = loc["page_id"]
                        if page_id in seen_pages:
                            continue
                        seen_pages.add(page_id)

                        page_info = page_lookup.get(page_id, {})
                        page_name = page_info.get("displayName") or page_info.get("name") or page_id
                        page_url = self._build_page_url(workspace_id, report_id, page_id)

                        refs.append({
                            "report_name": report_name,
                            "report_url": report_url,
                            "page_name": page_name,
                            "page_url": page_url,
                            "measure": measure,
                            "visual_type": loc["visual_type"],
                            "note": f"Measure found in visual on page '{page_name}'"
                        })
                else:
                    # Measure not found in visuals - add report-level reference
                    refs.append({
                        "report_name": report_name,
                        "report_url": report_url,
                        "page_name": None,
                        "page_url": None,
                        "measure": measure,
                        "visual_type": None,
                        "note": "Measure in dataset but not detected in report visuals"
                    })

            return refs

        except Exception as e:
            logger.warning(f"Error fetching report definition: {e}")
            return []

    def _build_page_url(self, workspace_id: str, report_id: str, page_id: str) -> str:
        """Build the Power BI report page URL."""
        if page_id:
            return f"https://app.powerbi.com/groups/{workspace_id}/reports/{report_id}/ReportSection{page_id}"
        return f"https://app.powerbi.com/groups/{workspace_id}/reports/{report_id}"

    def _parse_report_pages(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse page definitions from PBIR report structure."""
        pages = []

        for part in report_parts:
            path = part.get("path", "")
            path_lower = path.lower()

            # Look for page.json files
            is_page_file = (
                ("/pages/" in path_lower and path_lower.endswith("/page.json")) or
                (path_lower.endswith("/page.json"))
            )

            if is_page_file:
                try:
                    payload = part.get("payload", "")
                    content = base64.b64decode(payload).decode("utf-8")
                    page_data = json.loads(content)

                    # Extract page ID from path
                    path_parts = path.split("/")
                    page_id = None

                    if "pages" in path_parts:
                        idx = path_parts.index("pages") + 1
                        page_id = path_parts[idx] if idx < len(path_parts) else None
                    elif "Pages" in path_parts:
                        idx = path_parts.index("Pages") + 1
                        page_id = path_parts[idx] if idx < len(path_parts) else None
                    else:
                        page_id = path_parts[-2] if len(path_parts) >= 2 else "unknown"

                    pages.append({
                        "id": page_id,
                        "name": page_data.get("name", page_id),
                        "displayName": page_data.get("displayName", page_data.get("name", page_id)),
                        "ordinal": page_data.get("ordinal", 0),
                    })
                except Exception as e:
                    logger.warning(f"Error parsing page from {path}: {e}")

        # Try embedded format if no pages found
        if not pages:
            pages = self._parse_pages_from_report_json(report_parts)

        pages.sort(key=lambda p: p.get("ordinal", 0))
        return pages

    def _parse_pages_from_report_json(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse pages from report.json (embedded format)."""
        pages = []

        for part in report_parts:
            path = part.get("path", "")
            if path.lower() == "report.json" or path.lower().endswith("/report.json"):
                try:
                    payload = part.get("payload", "")
                    content = base64.b64decode(payload).decode("utf-8")
                    report_data = json.loads(content)

                    pages_data = (
                        report_data.get("pages") or
                        report_data.get("sections") or
                        report_data.get("reportPages")
                    )

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
                except Exception as e:
                    logger.warning(f"Error parsing report.json for pages: {e}")

        return pages

    def _parse_report_visuals(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse visual definitions from PBIR report structure."""
        visuals = []

        for part in report_parts:
            path = part.get("path", "")
            path_lower = path.lower()

            # Look for visual.json files
            is_visual_file = (
                ("/visuals/" in path_lower and path_lower.endswith("/visual.json")) or
                ("/visuals/" in path_lower and path_lower.endswith(".json"))
            )

            if is_visual_file:
                try:
                    payload = part.get("payload", "")
                    content = base64.b64decode(payload).decode("utf-8")
                    visual_data = json.loads(content)

                    # Extract page ID and visual ID from path
                    path_parts = path.split("/")

                    page_id = None
                    if "pages" in path_parts:
                        idx = path_parts.index("pages") + 1
                        page_id = path_parts[idx] if idx < len(path_parts) else None
                    elif "Pages" in path_parts:
                        idx = path_parts.index("Pages") + 1
                        page_id = path_parts[idx] if idx < len(path_parts) else None

                    visual_id = None
                    if "visuals" in path_parts:
                        idx = path_parts.index("visuals") + 1
                        visual_id = path_parts[idx] if idx < len(path_parts) else None
                    elif "Visuals" in path_parts:
                        idx = path_parts.index("Visuals") + 1
                        visual_id = path_parts[idx] if idx < len(path_parts) else None
                    else:
                        visual_id = path_parts[-2] if len(path_parts) >= 2 else "unknown"

                    visuals.append({
                        "id": visual_id,
                        "page_id": page_id,
                        "type": visual_data.get("visual", {}).get("visualType", "unknown"),
                        "config": visual_data
                    })
                except Exception as e:
                    logger.warning(f"Error parsing visual from {path}: {e}")

        # Try embedded format if no visuals found
        if not visuals:
            visuals = self._parse_visuals_from_report_json(report_parts)

        return visuals

    def _parse_visuals_from_report_json(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse visuals from report.json (embedded format)."""
        visuals = []

        for part in report_parts:
            path = part.get("path", "")
            if path.lower() == "report.json" or path.lower().endswith("/report.json"):
                try:
                    payload = part.get("payload", "")
                    content = base64.b64decode(payload).decode("utf-8")
                    report_data = json.loads(content)

                    pages_data = (
                        report_data.get("pages") or
                        report_data.get("sections") or
                        report_data.get("reportPages")
                    )

                    if pages_data and isinstance(pages_data, list):
                        for page_data in pages_data:
                            if not isinstance(page_data, dict):
                                continue

                            page_id = page_data.get("name") or page_data.get("id")
                            visuals_data = (
                                page_data.get("visualContainers") or
                                page_data.get("visuals")
                            )

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
                                        "id": visual_id,
                                        "page_id": page_id,
                                        "type": visual_type,
                                        "config": parsed_config
                                    })
                except Exception as e:
                    logger.warning(f"Error parsing report.json for visuals: {e}")

        return visuals

    def _extract_measures_from_visual(self, visual: Dict[str, Any]) -> List[str]:
        """Extract measure names referenced in a visual configuration."""
        measures = set()
        config = visual.get("config", {})

        # Handle config as JSON string
        if isinstance(config, str):
            try:
                config = json.loads(config)
            except json.JSONDecodeError:
                return []

        # Deep search for measure references
        self._find_measures_in_dict(config, measures)

        return list(measures)

    def _find_measures_in_dict(self, obj: Any, measures: set) -> None:
        """Recursively search for measure references in a dictionary."""
        if isinstance(obj, dict):
            # Check for measure patterns
            if "measure" in obj:
                measure_ref = obj["measure"]
                if isinstance(measure_ref, dict):
                    name = measure_ref.get("property") or measure_ref.get("name")
                    if name:
                        measures.add(name)
                elif isinstance(measure_ref, str):
                    measures.add(measure_ref)

            # Check for Measure key (used in some formats)
            if "Measure" in obj:
                measure_ref = obj["Measure"]
                if isinstance(measure_ref, dict):
                    name = measure_ref.get("Property") or measure_ref.get("property") or measure_ref.get("Name") or measure_ref.get("name")
                    if name:
                        measures.add(name)

            # Check for property in aggregation context
            if obj.get("aggregation") and "property" in obj:
                prop = obj["property"]
                if prop:
                    measures.add(prop)

            # Recurse into nested dicts and lists
            for value in obj.values():
                self._find_measures_in_dict(value, measures)

        elif isinstance(obj, list):
            for item in obj:
                self._find_measures_in_dict(item, measures)

    def _format_output(self, results: Dict[str, Any], output_format: str) -> str:
        """Format the results for output."""
        if output_format == "json":
            return json.dumps(results, indent=2, default=str)

        # Markdown format
        output = []

        output.append("# Power BI Analysis Results\n")
        output.append(f"**Question**: {results['user_question']}")
        output.append(f"**Workspace**: `{results['workspace_id']}`")
        output.append(f"**Dataset**: `{results['dataset_id']}`\n")

        # Errors
        if results.get("errors"):
            output.append("## ⚠️ Errors\n")
            for error in results["errors"]:
                output.append(f"- {error}")
            output.append("")

        # Model Context Summary
        ctx = results.get("model_context", {})
        output.append("## Model Context\n")
        output.append(f"- **Measures**: {len(ctx.get('measures', []))}")
        output.append(f"- **Tables**: {len(ctx.get('tables', []))}")
        output.append(f"- **Relationships**: {len(ctx.get('relationships', []))}\n")

        # Generated DAX
        if results.get("generated_dax"):
            output.append("## Generated DAX Query\n")

            # Show retry attempts if there were multiple
            dax_attempts = results.get("dax_attempts", [])
            if len(dax_attempts) > 1:
                output.append(f"**Attempts**: {len(dax_attempts)} (successful on attempt {len(dax_attempts)})\n")
                output.append("\n### Retry History\n")
                for att in dax_attempts[:-1]:  # Show all failed attempts
                    output.append(f"**Attempt {att['attempt']}**: ❌ Failed")
                    if att.get('error'):
                        output.append(f"  - Error: {att['error'][:100]}...")
                output.append(f"**Attempt {dax_attempts[-1]['attempt']}**: ✅ Success\n")

            output.append("```dax")
            output.append(results["generated_dax"])
            output.append("```\n")

        # Execution Results
        exec_result = results.get("dax_execution", {})
        output.append("## Execution Results\n")

        if exec_result.get("success"):
            output.append(f"✅ **Success** - {exec_result.get('row_count', 0)} rows returned\n")

            # Show data as table
            data = exec_result.get("data", [])
            if data:
                columns = exec_result.get("columns", list(data[0].keys()) if data else [])

                # Table header
                output.append("| " + " | ".join(str(c).replace("[", "").replace("]", "") for c in columns) + " |")
                output.append("| " + " | ".join(["---"] * len(columns)) + " |")

                # Table rows (limit to 20)
                for row in data[:20]:
                    values = [str(row.get(c, ""))[:50] for c in columns]
                    output.append("| " + " | ".join(values) + " |")

                if len(data) > 20:
                    output.append(f"\n*... and {len(data) - 20} more rows*")
        else:
            output.append(f"❌ **Failed**: {exec_result.get('error', 'Unknown error')}")
        output.append("")

        # Visual References
        if results.get("visual_references"):
            output.append("## Visual References\n")
            output.append("Reports and pages using the queried measures:\n")

            # Group by report, then by page
            report_refs = {}
            for ref in results["visual_references"]:
                report_name = ref.get("report_name", "Unknown")
                if report_name not in report_refs:
                    report_refs[report_name] = {
                        "report_url": ref.get("report_url", ""),
                        "pages": {}
                    }

                page_name = ref.get("page_name")
                page_url = ref.get("page_url")
                measure = ref.get("measure", "Unknown")
                visual_type = ref.get("visual_type")

                if page_name:
                    if page_name not in report_refs[report_name]["pages"]:
                        report_refs[report_name]["pages"][page_name] = {
                            "page_url": page_url,
                            "measures": [],
                            "visual_types": set()
                        }
                    report_refs[report_name]["pages"][page_name]["measures"].append(measure)
                    if visual_type:
                        report_refs[report_name]["pages"][page_name]["visual_types"].add(visual_type)
                else:
                    # No page info - store at report level
                    if "_no_page_" not in report_refs[report_name]["pages"]:
                        report_refs[report_name]["pages"]["_no_page_"] = {
                            "page_url": None,
                            "measures": [],
                            "visual_types": set()
                        }
                    report_refs[report_name]["pages"]["_no_page_"]["measures"].append(measure)

            # Format output
            for report_name, report_data in report_refs.items():
                report_url = report_data["report_url"]
                output.append(f"\n### 📊 {report_name}")
                output.append(f"[Open Report]({report_url})\n")

                for page_name, page_data in report_data["pages"].items():
                    if page_name == "_no_page_":
                        # Measures without page-level detail
                        unique_measures = list(set(page_data["measures"]))
                        output.append(f"- Measures in report: {', '.join(unique_measures)}")
                    else:
                        page_url = page_data["page_url"]
                        unique_measures = list(set(page_data["measures"]))
                        visual_types = list(page_data["visual_types"])

                        output.append(f"- **📄 {page_name}**: [Open Page]({page_url})")
                        output.append(f"  - Measures: {', '.join(unique_measures)}")
                        if visual_types:
                            output.append(f"  - Visual types: {', '.join(visual_types)}")

        return "\n".join(output)
