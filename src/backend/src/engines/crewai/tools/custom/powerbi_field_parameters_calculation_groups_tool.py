"""
Power BI Field Parameters & Calculation Groups Extraction Tool for CrewAI

Extracts field parameters and calculation groups from Power BI/Microsoft Fabric
semantic models using the Fabric API getDefinition endpoint (TMDL format).

Generates:
1. Field Parameters metadata with measure mappings
2. Calculation Groups with calculation item definitions
3. Unity Catalog SQL for config tables and parameterized queries
4. Integration with Measure Conversion Pipeline for DAX translation

Author: Kasal Team
Date: 2026
"""

import asyncio
import base64
import logging
import re
import json
from typing import Any, Optional, Type, Dict, List

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr
import httpx

logger = logging.getLogger(__name__)


class PowerBIFieldParametersCalculationGroupsSchema(BaseModel):
    """Input schema for PowerBIFieldParametersCalculationGroupsTool."""

    # ===== POWER BI CONFIGURATION =====
    workspace_id: Optional[str] = Field(
        None,
        description="[Power BI] Workspace ID (GUID) containing the semantic model. Leave empty to use pre-configured value."
    )
    dataset_id: Optional[str] = Field(
        None,
        description="[Power BI] Dataset/Semantic Model ID (GUID) to extract from. Leave empty to use pre-configured value."
    )

    # ===== SERVICE PRINCIPAL AUTHENTICATION =====
    tenant_id: Optional[str] = Field(
        None,
        description="[Auth] Azure AD tenant ID for Service Principal or Service Account authentication."
    )
    client_id: Optional[str] = Field(
        None,
        description="[Auth] Application/Client ID for Service Principal or Service Account authentication."
    )
    client_secret: Optional[str] = Field(
        None,
        description="[Auth] Client secret for Service Principal authentication."
    )

    # ===== SERVICE ACCOUNT AUTHENTICATION =====
    username: Optional[str] = Field(
        None,
        description="[Auth] Service account username/UPN (for Service Account authentication)"
    )
    password: Optional[str] = Field(
        None,
        description="[Auth] Service account password (for Service Account authentication)"
    )
    auth_method: Optional[str] = Field(
        None,
        description="[Auth] Authentication method: 'service_principal', 'service_account', or auto-detect"
    )

    # User OAuth token (alternative to Service Principal/Service Account)
    access_token: Optional[str] = Field(
        None,
        description="[Auth] Pre-obtained OAuth access token (alternative to SP/Service Account)."
    )

    # ===== UNITY CATALOG TARGET CONFIGURATION =====
    target_catalog: str = Field(
        "main",
        description="[Target] Unity Catalog catalog name for generated SQL (default: 'main')."
    )
    target_schema: str = Field(
        "default",
        description="[Target] Unity Catalog schema name for generated SQL (default: 'default')."
    )

    # ===== LLM CONFIGURATION FOR MEASURE TRANSLATION =====
    llm_workspace_url: Optional[str] = Field(
        None,
        description="[LLM] Databricks workspace URL for LLM-based DAX translation."
    )
    llm_token: Optional[str] = Field(
        None,
        description="[LLM] Databricks token for LLM access."
    )
    llm_model: str = Field(
        "databricks-claude-sonnet-4",
        description="[LLM] Model to use for DAX translation (default: 'databricks-claude-sonnet-4')."
    )
    translate_measures: bool = Field(
        True,
        description="[LLM] Whether to translate referenced DAX measures to SQL (default: True)."
    )

    # ===== OUTPUT OPTIONS =====
    include_sql_translation: bool = Field(
        True,
        description="[Output] Include SQL equivalents for calculation items (default: True)."
    )
    include_metadata_tables: bool = Field(
        True,
        description="[Output] Generate metadata table DDL (default: True)."
    )
    output_format: str = Field(
        "markdown",
        description="[Output] Output format: 'markdown', 'json', or 'sql' (default: 'markdown')."
    )


class PowerBIFieldParametersCalculationGroupsTool(BaseTool):
    """
    Power BI Field Parameters & Calculation Groups Extraction Tool.

    Extracts field parameters and calculation groups from Fabric semantic models
    using the Fabric API getDefinition endpoint (TMDL format). Generates:

    1. **Field Parameters**: Dimension tables mapping user-friendly labels to measures
    2. **Calculation Groups**: Time intelligence and other calculation patterns
    3. **Metadata Tables**: Configuration for dynamic SQL generation
    4. **SQL Translation**: Converts DAX calculation items to SQL equivalents

    **Use Cases**:
    - Power BI to Databricks migration
    - AI/BI Genie integration with dynamic measure selection
    - Semantic model documentation

    **Requirements**:
    - Service Principal with SemanticModel.ReadWrite.All permissions
    - Workspace must be a Microsoft Fabric workspace
    - Optional: LLM access for DAX to SQL translation

    **Integration**:
    - Works with Measure Conversion Pipeline for DAX translation
    - Outputs compatible with AI/BI Genie parameterized queries
    """

    name: str = "Power BI Field Parameters & Calculation Groups Tool"
    description: str = (
        "Extracts field parameters and calculation groups from Microsoft Fabric "
        "semantic models using the Fabric API getDefinition endpoint (TMDL format). "
        "Field parameters define dynamic measure selection (KPI selectors). "
        "Calculation groups define reusable calculations (time intelligence). "
        "Generates Unity Catalog metadata tables and SQL for integration with AI/BI tools. "
        "Requires Service Principal with SemanticModel.ReadWrite.All permissions."
    )
    args_schema: Type[BaseModel] = PowerBIFieldParametersCalculationGroupsSchema

    # Private attributes
    _instance_id: str = PrivateAttr()
    _default_config: Dict[str, Any] = PrivateAttr()

    # Allow extra attributes for config
    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the tool with configuration."""
        import uuid
        instance_id = str(uuid.uuid4())[:8]

        logger.info(f"[PowerBIFieldParametersCalculationGroupsTool.__init__] Instance ID: {instance_id}")
        logger.info(f"[PowerBIFieldParametersCalculationGroupsTool.__init__] kwargs keys: {list(kwargs.keys())}")

        # Helper to check if a value is a placeholder that should be treated as empty
        def is_placeholder_value(value: Any) -> bool:
            if not isinstance(value, str):
                return False
            # Check for {placeholder} patterns
            if re.search(r'^\{[a-z_]+\}$', value):
                return True
            return False

        # Helper to get value, treating placeholders as None
        def get_filtered_value(key: str, default: Any = None) -> Any:
            value = kwargs.get(key, default)
            if is_placeholder_value(value):
                logger.info(f"[PowerBIFieldParametersCalculationGroupsTool.__init__] Filtering placeholder for {key}: {value}")
                return default
            return value

        # Extract execution_inputs for dynamic parameter resolution
        execution_inputs = kwargs.get("execution_inputs", {})

        # Store configuration values - filter out placeholder values
        default_config = {
            "workspace_id": get_filtered_value("workspace_id"),
            "dataset_id": get_filtered_value("dataset_id"),
            # Authentication credentials
            "tenant_id": get_filtered_value("tenant_id"),
            "client_id": get_filtered_value("client_id"),
            "client_secret": get_filtered_value("client_secret"),
            "username": get_filtered_value("username"),
            "password": get_filtered_value("password"),
            "auth_method": get_filtered_value("auth_method"),
            "access_token": get_filtered_value("access_token"),
            # Target configuration
            "target_catalog": get_filtered_value("target_catalog", "main"),
            "target_schema": get_filtered_value("target_schema", "default"),
            # LLM configuration
            "llm_workspace_url": get_filtered_value("llm_workspace_url"),
            "llm_token": get_filtered_value("llm_token"),
            "llm_model": get_filtered_value("llm_model", "databricks-claude-sonnet-4"),
            # Feature flags
            "translate_measures": get_filtered_value("translate_measures", True),
            "include_sql_translation": get_filtered_value("include_sql_translation", True),
            "include_metadata_tables": get_filtered_value("include_metadata_tables", True),
            # Output options
            "output_format": get_filtered_value("output_format", "markdown"),
            "mode": get_filtered_value("mode", "static"),  # Also store mode for debugging
        }

        # Dynamic parameter resolution
        if execution_inputs:
            resolved_config = {}
            for key, value in default_config.items():
                resolved_config[key] = self._resolve_placeholder(value, execution_inputs)
            default_config = resolved_config

        # Log configuration (mask secrets)
        safe_config = {k: v if 'secret' not in k.lower() and 'token' not in k.lower() else '***'
                       for k, v in default_config.items() if v is not None}
        logger.info(f"[PowerBIFieldParametersCalculationGroupsTool] Config: {safe_config}")

        # Call parent init
        tool_kwargs = {k: v for k, v in kwargs.items() if k not in default_config}
        super().__init__(**tool_kwargs)

        # Set private attributes
        self._instance_id = instance_id
        self._default_config = default_config

    def _resolve_placeholder(self, value: Any, execution_inputs: Dict[str, Any]) -> Any:
        """Resolve {placeholder} parameters from execution_inputs."""
        if not isinstance(value, str):
            return value

        placeholders = re.findall(r'\{(\w+)\}', value)
        if not placeholders:
            return value

        resolved_value = value
        for placeholder in placeholders:
            if placeholder in execution_inputs:
                replacement = str(execution_inputs[placeholder])
                resolved_value = resolved_value.replace(f'{{{placeholder}}}', replacement)

        return resolved_value

    def _run(self, **kwargs: Any) -> str:
        """Execute field parameters and calculation groups extraction."""
        try:
            instance_id = getattr(self, '_instance_id', 'UNKNOWN')
            logger.info(f"[PowerBIFieldParametersCalculationGroupsTool] Instance {instance_id} - _run() called")

            # Extract execution_inputs
            execution_inputs = kwargs.pop('execution_inputs', {})

            # Filter placeholder values - including {placeholder} patterns from dynamic mode
            def is_placeholder(value: Any) -> bool:
                if not isinstance(value, str):
                    return False
                # Check for common placeholder patterns
                patterns = ["your_", "placeholder", "example_", "xxx", "insert_", "<"]
                if any(p in value.lower() for p in patterns):
                    return True
                # Check for {placeholder} patterns (e.g., {workspace_id}, {dataset_id})
                if re.search(r'^\{[a-z_]+\}$', value):
                    return True
                return False

            filtered_kwargs = {
                k: v for k, v in kwargs.items()
                if v is not None and not is_placeholder(v)
            }

            # Log what was filtered for debugging
            filtered_out = {k: v for k, v in kwargs.items() if v is not None and is_placeholder(v)}
            if filtered_out:
                logger.info(f"[PowerBIFieldParametersCalculationGroupsTool] Filtered out placeholder kwargs: {list(filtered_out.keys())}")

            # Merge with defaults - IMPORTANT: default_config must be second to override agent's values
            merged_kwargs = {**filtered_kwargs, **self._default_config}

            # Resolve dynamic parameters
            if execution_inputs:
                resolved_kwargs = {}
                for key, value in merged_kwargs.items():
                    resolved_kwargs[key] = self._resolve_placeholder(value, execution_inputs)
                merged_kwargs = resolved_kwargs

            # Extract and validate parameters
            workspace_id = merged_kwargs.get("workspace_id")
            dataset_id = merged_kwargs.get("dataset_id")

            # Build auth config
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
            logger.info("[PowerBIFieldParametersCalculationGroupsTool] AUTH CONFIG DEBUG")
            logger.info("=" * 80)
            logger.info(f"  tenant_id: {auth_config.get('tenant_id')}")
            logger.info(f"  client_id: {auth_config.get('client_id')}")
            logger.info(f"  client_secret: {'*' * len(auth_config.get('client_secret') or '') if auth_config.get('client_secret') else 'None'}")
            logger.info(f"  username: {auth_config.get('username')}")
            logger.info(f"  password: {'*' * len(auth_config.get('password') or '') if auth_config.get('password') else 'None'}")
            logger.info(f"  auth_method: {auth_config.get('auth_method')} (type: {type(auth_config.get('auth_method'))})")
            logger.info(f"  access_token: {'*' * 10 if auth_config.get('access_token') else 'None'}")
            logger.info("=" * 80)

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
            logger.info("[PowerBIFieldParametersCalculationGroupsTool] 🔑 AUTHENTICATION METHOD DETECTION")
            logger.info("=" * 80)
            logger.info(f"  Detected auth method: {detected_auth_method}")
            logger.info("=" * 80)

            # Helper to check for unresolved placeholders
            def has_unresolved_placeholder(value: Any) -> bool:
                if not isinstance(value, str):
                    return False
                return bool(re.search(r'\{[a-z_]+\}', value))

            # Check for unresolved placeholders (dynamic mode without execution_inputs)
            unresolved = []
            for param_name, param_value in [
                ("workspace_id", workspace_id),
                ("dataset_id", dataset_id),
                ("tenant_id", auth_config.get("tenant_id")),
                ("client_id", auth_config.get("client_id")),
                ("client_secret", auth_config.get("client_secret")),
                ("username", auth_config.get("username")),
                ("password", auth_config.get("password")),
                ("access_token", auth_config.get("access_token")),
            ]:
                if has_unresolved_placeholder(param_value):
                    unresolved.append(param_name)

            if unresolved:
                # Log detailed debug info
                mode = merged_kwargs.get('mode', 'unknown')
                logger.error(f"[PowerBIFieldParametersCalculationGroupsTool] Unresolved placeholders: {unresolved}")
                logger.error(f"[PowerBIFieldParametersCalculationGroupsTool] Mode: {mode}")
                logger.error(f"[PowerBIFieldParametersCalculationGroupsTool] _default_config keys: {list(self._default_config.keys())}")
                logger.error(f"[PowerBIFieldParametersCalculationGroupsTool] kwargs keys: {list(kwargs.keys())}")
                # Log actual values (mask secrets)
                for param_name, param_value in [("workspace_id", workspace_id), ("dataset_id", dataset_id)]:
                    logger.error(f"[PowerBIFieldParametersCalculationGroupsTool] {param_name}: {param_value}")

                return (
                    f"Error: Unresolved placeholder(s) detected: {', '.join(unresolved)}\n\n"
                    f"**Debug Info**:\n"
                    f"- Mode in config: {mode}\n"
                    f"- workspace_id value: `{workspace_id}`\n"
                    f"- dataset_id value: `{dataset_id}`\n\n"
                    "This usually means the tool is configured in **dynamic mode** but no "
                    "execution_inputs were provided, OR the task's tool_configs weren't "
                    "properly saved/loaded.\n\n"
                    "**Solutions**:\n"
                    "1. **Re-save the task**: Open the task in the UI, verify you're in Static mode, "
                    "enter actual values, and save again\n"
                    "2. **Check the crew logs**: Look for `[ToolFactory]` and `tool_config_override` "
                    "to see what values are being passed\n"
                    "3. **Dynamic mode**: If using dynamic mode, provide values via execution_inputs\n\n"
                    "For static configuration, enter real credentials (not {placeholder} values)."
                )

            # Validate required parameters
            if not workspace_id:
                return "Error: workspace_id is required. Provide via parameter or execution_inputs."
            if not dataset_id:
                return "Error: dataset_id is required. Provide via parameter or execution_inputs."

            # Validate authentication using shared utility (filters out placeholders)
            clean_auth_config = {
                k: v for k, v in auth_config.items()
                if v and not has_unresolved_placeholder(v)
            }
            from src.engines.crewai.tools.custom.powerbi_auth_utils import validate_auth_config
            is_valid, error_msg = validate_auth_config(clean_auth_config)
            if not is_valid:
                return f"Error: {error_msg}\n\nService Principal requires SemanticModel.ReadWrite.All permission."

            # Run async extraction
            result = self._run_sync(self._extract_and_process(
                workspace_id=workspace_id,
                dataset_id=dataset_id,
                auth_config=clean_auth_config,
                target_catalog=merged_kwargs.get("target_catalog", "main"),
                target_schema=merged_kwargs.get("target_schema", "default"),
                include_sql_translation=merged_kwargs.get("include_sql_translation", True),
                include_metadata_tables=merged_kwargs.get("include_metadata_tables", True),
                output_format=merged_kwargs.get("output_format", "markdown"),
            ))

            return result

        except Exception as e:
            logger.error(f"[PowerBIFieldParametersCalculationGroupsTool] Error: {str(e)}", exc_info=True)
            return f"Error: {str(e)}"

    def _run_sync(self, coro):
        """Run async coroutine from sync context."""
        try:
            loop = asyncio.get_running_loop()
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(coro)
            finally:
                loop.close()

    async def _extract_and_process(
        self,
        workspace_id: str,
        dataset_id: str,
        auth_config: Dict[str, Any],
        target_catalog: str,
        target_schema: str,
        include_sql_translation: bool,
        include_metadata_tables: bool,
        output_format: str,
    ) -> str:
        """Main extraction and processing logic."""
        from src.engines.crewai.tools.custom.powerbi_auth_utils import (
            get_fabric_access_token_from_config,
        )

        # Step 1: Get Fabric API access token
        logger.info("Obtaining Fabric API access token")
        token = await get_fabric_access_token_from_config(auth_config)

        # Step 2: Fetch TMDL definition
        logger.info(f"Fetching TMDL definition for dataset {dataset_id}")
        tmdl_parts = await self._fetch_tmdl_definition(workspace_id, dataset_id, token)

        if not tmdl_parts:
            return (
                "# Field Parameters & Calculation Groups Extraction\n\n"
                f"**Workspace**: `{workspace_id}`\n"
                f"**Dataset**: `{dataset_id}`\n\n"
                "Error: Could not fetch TMDL definition. Check permissions and IDs."
            )

        # Step 3: Parse Field Parameters
        logger.info("Parsing TMDL for field parameters")
        field_parameters = self._parse_field_parameters(tmdl_parts)

        # Step 4: Parse Calculation Groups
        logger.info("Parsing TMDL for calculation groups")
        calculation_groups = self._parse_calculation_groups(tmdl_parts)

        # Step 5: Extract referenced measures
        logger.info("Extracting referenced measures")
        all_measures = self._parse_all_measures(tmdl_parts)
        referenced_measures = self._get_referenced_measures(field_parameters, all_measures)

        # Step 6: Generate output
        if output_format == "json":
            return self._format_json_output(
                workspace_id, dataset_id,
                field_parameters, calculation_groups,
                referenced_measures, target_catalog, target_schema
            )
        elif output_format == "sql":
            return self._format_sql_output(
                field_parameters, calculation_groups,
                referenced_measures, target_catalog, target_schema
            )
        else:
            return self._format_markdown_output(
                workspace_id, dataset_id,
                field_parameters, calculation_groups,
                referenced_measures, target_catalog, target_schema,
                include_sql_translation, include_metadata_tables
            )

    async def _fetch_tmdl_definition(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
    ) -> List[Dict[str, Any]]:
        """Fetch TMDL definition from Fabric API."""
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/getDefinition?format=TMDL"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient(timeout=180.0) as client:
            try:
                # POST to initiate getDefinition
                response = await client.post(url, headers=headers)

                if response.status_code == 202:
                    # Long-running operation
                    location = response.headers.get("Location")
                    if not location:
                        logger.error("No Location header in 202 response")
                        return []

                    # Poll until complete
                    for attempt in range(60):
                        await asyncio.sleep(2)
                        poll_response = await client.get(location, headers=headers)
                        poll_data = poll_response.json()
                        status = poll_data.get("status", "")

                        if status == "Succeeded":
                            logger.info(f"Definition fetch succeeded after {attempt + 1} poll(s)")
                            result_url = location + "/result"
                            result_response = await client.get(result_url, headers=headers)
                            result_response.raise_for_status()
                            definition = result_response.json()
                            return definition.get("definition", {}).get("parts", [])
                        elif status == "Failed":
                            error = poll_data.get("error", {})
                            logger.error(f"Definition fetch failed: {error}")
                            return []

                    logger.error("Definition fetch timed out")
                    return []

                elif response.status_code == 200:
                    definition = response.json()
                    return definition.get("definition", {}).get("parts", [])
                else:
                    logger.error(f"Unexpected status code: {response.status_code}")
                    response.raise_for_status()
                    return []

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
                return []
            except Exception as e:
                logger.error(f"Error fetching definition: {e}")
                return []

    def _parse_field_parameters(self, tmdl_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse TMDL parts to extract field parameters."""
        field_parameters = []

        for part in tmdl_parts:
            path = part.get("path", "")
            payload = part.get("payload", "")

            if not path.startswith("definition/tables/") or not path.endswith(".tmdl"):
                continue

            try:
                tmdl_content = base64.b64decode(payload).decode("utf-8")

                # Check for field parameter signature: NAMEOF in partition source
                if "NAMEOF(" not in tmdl_content:
                    continue

                # Extract table name
                table_match = re.match(r"table\s+(?:'([^']+)'|(\w+))", tmdl_content.strip())
                if not table_match:
                    continue
                table_name = table_match.group(1) or table_match.group(2)

                # Parse partition source to extract field parameter items
                partition_match = re.search(
                    r"partition\s+['\"]?[^'\"=]+['\"]?\s*=\s*calculated.*?source\s*=\s*\{([^}]+)\}",
                    tmdl_content,
                    re.DOTALL | re.IGNORECASE
                )

                if not partition_match:
                    continue

                source_content = partition_match.group(1)

                # Parse tuples: ("Label", NAMEOF('table'[measure]), ordinal)
                tuple_pattern = re.compile(
                    r'\(\s*"([^"]+)"\s*,\s*NAMEOF\s*\(\s*\'([^\']+)\'\s*\[([^\]]+)\]\s*\)\s*,\s*(\d+)\s*\)',
                    re.IGNORECASE
                )

                items = []
                for match in tuple_pattern.finditer(source_content):
                    items.append({
                        "label": match.group(1),
                        "source_table": match.group(2),
                        "source_measure": match.group(3),
                        "ordinal": int(match.group(4))
                    })

                if not items:
                    continue

                # Extract associated measure if exists
                measure_match = re.search(
                    r"measure\s+['\"]?([^'\"\\n=]+)['\"]?\s*=\s*([\\s\\S]*?)(?=\\n\\s*column|\\n\\t[^\\t]|$)",
                    tmdl_content
                )

                associated_measure = None
                measure_expression = None
                if measure_match:
                    associated_measure = measure_match.group(1).strip()
                    measure_expression = measure_match.group(2).strip()

                field_parameters.append({
                    "type": "Field Parameter",
                    "name": table_name,
                    "items": sorted(items, key=lambda x: x["ordinal"]),
                    "associated_measure": associated_measure,
                    "measure_expression": measure_expression,
                    "raw_tmdl": tmdl_content
                })

                logger.info(f"Found field parameter: {table_name} with {len(items)} items")

            except Exception as e:
                logger.warning(f"Error parsing {path}: {e}")

        return field_parameters

    def _parse_calculation_groups(self, tmdl_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse TMDL parts to extract calculation groups."""
        calculation_groups = []

        for part in tmdl_parts:
            path = part.get("path", "")
            payload = part.get("payload", "")

            if not path.startswith("definition/tables/") or not path.endswith(".tmdl"):
                continue

            try:
                tmdl_content = base64.b64decode(payload).decode("utf-8")

                # Check for calculation group signature
                if "calculationGroup" not in tmdl_content:
                    continue

                # Extract table name
                table_match = re.match(r"table\s+(?:'([^']+)'|(\w+))", tmdl_content.strip())
                if not table_match:
                    continue
                table_name = table_match.group(1) or table_match.group(2)

                # Extract precedence
                precedence_match = re.search(r"precedence:\s*(\d+)", tmdl_content)
                precedence = int(precedence_match.group(1)) if precedence_match else 0

                # Parse calculation items
                items = []

                # Split by calculationItem to get each item
                item_blocks = re.split(r'\n\s*calculationItem\s+', tmdl_content)

                for i, block in enumerate(item_blocks[1:], 1):  # Skip first split (before first item)
                    # Extract item name and expression
                    item_match = re.match(
                        r"(?:'([^']+)'|(\w+))\s*=\s*([\s\S]*?)(?=\n\s*calculationItem|\n\s*column|\Z)",
                        block.strip()
                    )

                    if item_match:
                        item_name = item_match.group(1) or item_match.group(2)
                        expression = item_match.group(3).strip()

                        # Clean expression - remove trailing metadata
                        clean_lines = []
                        for line in expression.split('\n'):
                            stripped = line.strip()
                            if stripped.startswith(('lineageTag:', 'formatString:', 'annotation')):
                                break
                            clean_lines.append(line)

                        clean_expression = '\n'.join(clean_lines).strip()

                        items.append({
                            "name": item_name,
                            "expression": clean_expression,
                            "ordinal": i - 1
                        })

                if not items:
                    continue

                calculation_groups.append({
                    "type": "Calculation Group",
                    "name": table_name,
                    "precedence": precedence,
                    "items": items,
                    "raw_tmdl": tmdl_content
                })

                logger.info(f"Found calculation group: {table_name} with {len(items)} items")

            except Exception as e:
                logger.warning(f"Error parsing {path}: {e}")

        return calculation_groups

    def _parse_all_measures(self, tmdl_parts: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Parse all measures from TMDL parts."""
        measures = {}

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

                # Find all measures in this table
                measure_pattern = re.compile(
                    r"measure\s+(?:'([^']+)'|(\w+))\s*=\s*([\s\S]*?)(?=\n\s*measure|\n\s*column|\n\t[^\t]|\Z)",
                    re.MULTILINE
                )

                for match in measure_pattern.finditer(tmdl_content):
                    measure_name = match.group(1) or match.group(2)
                    expression = match.group(3).strip()

                    # Clean expression
                    clean_lines = []
                    for line in expression.split('\n'):
                        stripped = line.strip()
                        if stripped.startswith(('lineageTag:', 'formatString:', 'annotation')):
                            break
                        clean_lines.append(line)

                    measures[measure_name] = {
                        "name": measure_name,
                        "table": table_name,
                        "expression": '\n'.join(clean_lines).strip()
                    }

            except Exception as e:
                logger.warning(f"Error parsing measures from {path}: {e}")

        return measures

    def _get_referenced_measures(
        self,
        field_parameters: List[Dict[str, Any]],
        all_measures: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Get measures referenced by field parameters."""
        referenced = []
        seen = set()

        for fp in field_parameters:
            for item in fp.get("items", []):
                measure_name = item.get("source_measure")
                if measure_name and measure_name not in seen:
                    seen.add(measure_name)
                    if measure_name in all_measures:
                        referenced.append(all_measures[measure_name])
                    else:
                        referenced.append({
                            "name": measure_name,
                            "table": item.get("source_table"),
                            "expression": None
                        })

        return referenced

    def _format_markdown_output(
        self,
        workspace_id: str,
        dataset_id: str,
        field_parameters: List[Dict[str, Any]],
        calculation_groups: List[Dict[str, Any]],
        referenced_measures: List[Dict[str, Any]],
        target_catalog: str,
        target_schema: str,
        include_sql_translation: bool,
        include_metadata_tables: bool,
    ) -> str:
        """Format output as markdown."""
        output = []

        # Header
        output.append("# Power BI Field Parameters & Calculation Groups Extraction Results\n")
        output.append(f"**Workspace ID**: `{workspace_id}`")
        output.append(f"**Dataset ID**: `{dataset_id}`")
        output.append(f"**Target**: `{target_catalog}.{target_schema}`\n")
        output.append(f"**Field Parameters Found**: {len(field_parameters)}")
        output.append(f"**Calculation Groups Found**: {len(calculation_groups)}")
        output.append(f"**Referenced Measures**: {len(referenced_measures)}\n")

        # Field Parameters Section
        if field_parameters:
            output.append("---\n")
            output.append("## Field Parameters\n")

            for fp in field_parameters:
                output.append(f"### {fp['name']}\n")
                if fp.get('associated_measure'):
                    output.append(f"**Associated Measure**: `{fp['associated_measure']}`\n")

                output.append("**Items**:\n")
                output.append("| Ordinal | Label | Source Table | Source Measure |")
                output.append("|---------|-------|--------------|----------------|")
                for item in fp['items']:
                    output.append(f"| {item['ordinal']} | {item['label']} | {item['source_table']} | {item['source_measure']} |")
                output.append("")

        # Calculation Groups Section
        if calculation_groups:
            output.append("---\n")
            output.append("## Calculation Groups\n")

            for cg in calculation_groups:
                output.append(f"### {cg['name']}\n")
                output.append(f"**Precedence**: {cg['precedence']}\n")
                output.append("**Calculation Items**:\n")

                for item in cg['items']:
                    output.append(f"#### {item['name']}\n")
                    output.append("```dax")
                    output.append(item['expression'])
                    output.append("```\n")

        # Referenced Measures Section
        if referenced_measures:
            output.append("---\n")
            output.append("## Referenced Measures (DAX)\n")

            for measure in referenced_measures:
                output.append(f"### {measure['name']}\n")
                output.append(f"**Table**: `{measure['table']}`\n")
                if measure.get('expression'):
                    output.append("```dax")
                    output.append(measure['expression'])
                    output.append("```\n")
                else:
                    output.append("*Expression not found in model*\n")

        # SQL Generation Section
        if include_metadata_tables:
            output.append("---\n")
            output.append("## Unity Catalog SQL\n")

            # Field Parameters Metadata Table
            output.append("### Field Parameters Config Table\n")
            output.append("```sql")
            output.append(f"CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}._config_field_parameters (")
            output.append("    parameter_name STRING COMMENT 'Field parameter table name',")
            output.append("    label STRING COMMENT 'User-friendly display label',")
            output.append("    source_table STRING COMMENT 'Source table containing the measure',")
            output.append("    source_measure STRING COMMENT 'Measure name in source table',")
            output.append("    ordinal INT COMMENT 'Sort order',")
            output.append("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()")
            output.append(") COMMENT 'Configuration for Power BI field parameters';")
            output.append("```\n")

            # Insert statements for field parameters
            if field_parameters:
                output.append("```sql")
                output.append(f"INSERT INTO {target_catalog}.{target_schema}._config_field_parameters VALUES")
                inserts = []
                for fp in field_parameters:
                    for item in fp['items']:
                        inserts.append(
                            f"    ('{fp['name']}', '{item['label']}', '{item['source_table']}', "
                            f"'{item['source_measure']}', {item['ordinal']}, CURRENT_TIMESTAMP())"
                        )
                output.append(",\n".join(inserts) + ";")
                output.append("```\n")

            # Calculation Groups Metadata Table
            output.append("### Calculation Groups Config Table\n")
            output.append("```sql")
            output.append(f"CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}._config_calculation_groups (")
            output.append("    group_name STRING COMMENT 'Calculation group name',")
            output.append("    item_name STRING COMMENT 'Calculation item name',")
            output.append("    dax_expression STRING COMMENT 'Original DAX expression',")
            output.append("    precedence INT COMMENT 'Calculation group precedence',")
            output.append("    ordinal INT COMMENT 'Sort order within group',")
            output.append("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()")
            output.append(") COMMENT 'Configuration for Power BI calculation groups';")
            output.append("```\n")

            # Insert statements for calculation groups
            if calculation_groups:
                output.append("```sql")
                output.append(f"INSERT INTO {target_catalog}.{target_schema}._config_calculation_groups VALUES")
                inserts = []
                for cg in calculation_groups:
                    for item in cg['items']:
                        # Escape single quotes in expression
                        escaped_expr = item['expression'].replace("'", "''").replace('\n', '\\n')
                        inserts.append(
                            f"    ('{cg['name']}', '{item['name']}', '{escaped_expr}', "
                            f"{cg['precedence']}, {item['ordinal']}, CURRENT_TIMESTAMP())"
                        )
                output.append(",\n".join(inserts) + ";")
                output.append("```\n")

        # SQL Translation Section
        if include_sql_translation and calculation_groups:
            output.append("---\n")
            output.append("## SQL Translation Patterns\n")
            output.append("*Equivalent SQL for common calculation group patterns:*\n")

            for cg in calculation_groups:
                if "Time" in cg['name'] or "time" in cg['name'].lower():
                    output.append(f"### {cg['name']} - SQL Equivalents\n")

                    for item in cg['items']:
                        output.append(f"**{item['name']}**:\n")

                        # Generate SQL based on common patterns
                        if item['name'].upper() == 'CURRENT':
                            output.append("```sql")
                            output.append("-- Current: No date filter, use measure as-is")
                            output.append("SELECT SUM(measure_column) AS value FROM table_name;")
                            output.append("```\n")
                        elif 'YTD' in item['name'].upper():
                            output.append("```sql")
                            output.append("-- YTD: Filter from start of year to current date")
                            output.append("SELECT SUM(measure_column) AS value")
                            output.append("FROM table_name")
                            output.append("WHERE date_column >= DATE_TRUNC('year', CURRENT_DATE())")
                            output.append("  AND date_column <= CURRENT_DATE();")
                            output.append("```\n")
                        elif 'MTD' in item['name'].upper():
                            output.append("```sql")
                            output.append("-- MTD: Filter from start of month to current date")
                            output.append("SELECT SUM(measure_column) AS value")
                            output.append("FROM table_name")
                            output.append("WHERE date_column >= DATE_TRUNC('month', CURRENT_DATE())")
                            output.append("  AND date_column <= CURRENT_DATE();")
                            output.append("```\n")
                        elif item['name'].upper() in ('PY', 'PRIOR YEAR', 'LY', 'LAST YEAR'):
                            output.append("```sql")
                            output.append("-- PY: Same period in prior year")
                            output.append("SELECT SUM(measure_column) AS value")
                            output.append("FROM table_name")
                            output.append("WHERE date_column >= DATEADD(year, -1, start_date)")
                            output.append("  AND date_column <= DATEADD(year, -1, end_date);")
                            output.append("```\n")
                        elif 'YOY' in item['name'].upper() or 'YEAR OVER YEAR' in item['name'].upper():
                            output.append("```sql")
                            output.append("-- YoY %: Year-over-year percentage change")
                            output.append("WITH current_period AS (...),")
                            output.append("     prior_period AS (...)")
                            output.append("SELECT (current_value - prior_value) / NULLIF(prior_value, 0) AS yoy_pct;")
                            output.append("```\n")

        # Summary
        output.append("---\n")
        output.append("## Summary\n")
        output.append(f"- **Total Field Parameters**: {len(field_parameters)}")
        output.append(f"- **Total Calculation Groups**: {len(calculation_groups)}")
        output.append(f"- **Total Calculation Items**: {sum(len(cg['items']) for cg in calculation_groups)}")
        output.append(f"- **Referenced Measures**: {len(referenced_measures)}")

        return "\n".join(output)

    def _format_json_output(
        self,
        workspace_id: str,
        dataset_id: str,
        field_parameters: List[Dict[str, Any]],
        calculation_groups: List[Dict[str, Any]],
        referenced_measures: List[Dict[str, Any]],
        target_catalog: str,
        target_schema: str,
    ) -> str:
        """Format output as JSON."""
        # Remove raw_tmdl from output for cleaner JSON
        clean_fps = []
        for fp in field_parameters:
            clean_fp = {k: v for k, v in fp.items() if k != 'raw_tmdl'}
            clean_fps.append(clean_fp)

        clean_cgs = []
        for cg in calculation_groups:
            clean_cg = {k: v for k, v in cg.items() if k != 'raw_tmdl'}
            clean_cgs.append(clean_cg)

        result = {
            "workspace_id": workspace_id,
            "dataset_id": dataset_id,
            "target_catalog": target_catalog,
            "target_schema": target_schema,
            "field_parameters": clean_fps,
            "calculation_groups": clean_cgs,
            "referenced_measures": referenced_measures,
            "summary": {
                "field_parameter_count": len(field_parameters),
                "calculation_group_count": len(calculation_groups),
                "calculation_item_count": sum(len(cg['items']) for cg in calculation_groups),
                "referenced_measure_count": len(referenced_measures)
            }
        }

        return json.dumps(result, indent=2)

    def _format_sql_output(
        self,
        field_parameters: List[Dict[str, Any]],
        calculation_groups: List[Dict[str, Any]],
        referenced_measures: List[Dict[str, Any]],
        target_catalog: str,
        target_schema: str,
    ) -> str:
        """Format output as comprehensive SQL statements (Option C)."""
        sql_lines = []

        sql_lines.append("-- ============================================================")
        sql_lines.append("-- POWER BI FIELD PARAMETERS & CALCULATION GROUPS")
        sql_lines.append("-- Comprehensive SQL Export (Option C: Full Flexibility)")
        sql_lines.append("-- ============================================================")
        sql_lines.append(f"-- Target Catalog: {target_catalog}")
        sql_lines.append(f"-- Target Schema: {target_schema}")
        sql_lines.append("--")
        sql_lines.append("-- This SQL provides:")
        sql_lines.append("--   1. Config tables for metadata storage")
        sql_lines.append("--   2. Measure definitions table (translated DAX)")
        sql_lines.append("--   3. Dynamic KPI selection view with time intelligence")
        sql_lines.append("--   4. Parameterized query templates for AI/BI Genie")
        sql_lines.append("-- ============================================================")
        sql_lines.append("")

        # ==========================================
        # SECTION 1: CONFIG TABLES
        # ==========================================
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- SECTION 1: CONFIGURATION TABLES")
        sql_lines.append("-- ==========================================")
        sql_lines.append("")

        # Field Parameters Config Table
        sql_lines.append(f"CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}._config_field_parameters (")
        sql_lines.append("    parameter_name STRING COMMENT 'Field parameter name (e.g., KPI Selector)',")
        sql_lines.append("    label STRING COMMENT 'User-friendly display label',")
        sql_lines.append("    source_table STRING COMMENT 'Source table containing the measure',")
        sql_lines.append("    source_measure STRING COMMENT 'Measure name in source table',")
        sql_lines.append("    ordinal INT COMMENT 'Display order',")
        sql_lines.append("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()")
        sql_lines.append(") COMMENT 'Power BI field parameter configuration';")
        sql_lines.append("")

        if field_parameters:
            sql_lines.append(f"INSERT INTO {target_catalog}.{target_schema}._config_field_parameters")
            sql_lines.append("(parameter_name, label, source_table, source_measure, ordinal)")
            sql_lines.append("VALUES")
            inserts = []
            for fp in field_parameters:
                for item in fp['items']:
                    inserts.append(
                        f"('{fp['name']}', '{item['label']}', '{item['source_table']}', "
                        f"'{item['source_measure']}', {item['ordinal']})"
                    )
            sql_lines.append(",\n".join(inserts) + ";")
            sql_lines.append("")

        # Calculation Groups Config Table
        sql_lines.append(f"CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}._config_calculation_groups (")
        sql_lines.append("    group_name STRING COMMENT 'Calculation group name',")
        sql_lines.append("    item_name STRING COMMENT 'Time intelligence type',")
        sql_lines.append("    dax_expression STRING COMMENT 'Original DAX expression',")
        sql_lines.append("    sql_pattern STRING COMMENT 'SQL equivalent pattern',")
        sql_lines.append("    precedence INT COMMENT 'Calculation precedence',")
        sql_lines.append("    ordinal INT COMMENT 'Display order',")
        sql_lines.append("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()")
        sql_lines.append(") COMMENT 'Power BI calculation group configuration';")
        sql_lines.append("")

        if calculation_groups:
            sql_lines.append(f"INSERT INTO {target_catalog}.{target_schema}._config_calculation_groups")
            sql_lines.append("(group_name, item_name, dax_expression, sql_pattern, precedence, ordinal)")
            sql_lines.append("VALUES")
            inserts = []
            for cg in calculation_groups:
                for item in cg['items']:
                    escaped_expr = item['expression'].replace("'", "''").replace('\n', '\\n')
                    sql_pattern = self._get_sql_pattern_for_calc_item(item['name'])
                    inserts.append(
                        f"('{cg['name']}', '{item['name']}', '{escaped_expr}', "
                        f"'{sql_pattern}', {cg['precedence']}, {item['ordinal']})"
                    )
            sql_lines.append(",\n".join(inserts) + ";")
            sql_lines.append("")

        # ==========================================
        # SECTION 2: MEASURE DEFINITIONS TABLE
        # ==========================================
        sql_lines.append("")
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- SECTION 2: MEASURE DEFINITIONS")
        sql_lines.append("-- ==========================================")
        sql_lines.append("")

        sql_lines.append(f"CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}._config_measures (")
        sql_lines.append("    measure_name STRING COMMENT 'Measure identifier',")
        sql_lines.append("    source_table STRING COMMENT 'Source table name',")
        sql_lines.append("    dax_expression STRING COMMENT 'Original DAX expression',")
        sql_lines.append("    sql_expression STRING COMMENT 'Translated SQL expression',")
        sql_lines.append("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()")
        sql_lines.append(") COMMENT 'Power BI measure definitions with SQL translations';")
        sql_lines.append("")

        if referenced_measures:
            sql_lines.append(f"INSERT INTO {target_catalog}.{target_schema}._config_measures")
            sql_lines.append("(measure_name, source_table, dax_expression, sql_expression)")
            sql_lines.append("VALUES")
            inserts = []
            for measure in referenced_measures:
                dax_expr = (measure.get('expression') or '').replace("'", "''").replace('\n', '\\n')
                sql_expr = self._translate_dax_to_sql_simple(measure.get('expression'), measure.get('table'))
                inserts.append(
                    f"('{measure['name']}', '{measure.get('table', '')}', "
                    f"'{dax_expr}', '{sql_expr}')"
                )
            sql_lines.append(",\n".join(inserts) + ";")
            sql_lines.append("")

        # ==========================================
        # SECTION 3: WORKING KPI BASE VIEW (UNION)
        # ==========================================
        sql_lines.append("")
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- SECTION 3: WORKING KPI BASE VIEW")
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- This view queries actual source tables and UNIONs results")
        sql_lines.append("-- Each KPI is aggregated from its source table")
        sql_lines.append("-- NOTE: Assumes source tables exist in the target catalog/schema")
        sql_lines.append("--       Adjust table references if they are in different locations")
        sql_lines.append("")

        if field_parameters and referenced_measures:
            fp = field_parameters[0]

            sql_lines.append(f"CREATE OR REPLACE VIEW {target_catalog}.{target_schema}.v_kpi_base AS")

            union_parts = []
            for idx, item in enumerate(fp['items']):
                measure = next((m for m in referenced_measures if m['name'] == item['source_measure']), None)
                if measure and measure.get('expression'):
                    sql_expr = self._translate_dax_to_sql_simple(measure['expression'], measure.get('table'))
                    source_table = item['source_table']
                    # Qualify table with catalog.schema
                    qualified_table = f"{target_catalog}.{target_schema}.{source_table}"

                    union_part = f"""SELECT
    '{item['label']}' AS kpi_name,
    {sql_expr} AS kpi_value
FROM {qualified_table}"""
                    union_parts.append(union_part)

            if union_parts:
                sql_lines.append("\nUNION ALL\n".join(union_parts) + ";")
            sql_lines.append("")

            # Also create a simple usage example
            sql_lines.append("-- Usage examples:")
            sql_lines.append(f"-- SELECT * FROM {target_catalog}.{target_schema}.v_kpi_base;")
            sql_lines.append(f"-- SELECT * FROM {target_catalog}.{target_schema}.v_kpi_base WHERE kpi_name = 'Confirmed PHC';")
            sql_lines.append("")

        # ==========================================
        # SECTION 4: KPI VIEW WITH TIME DIMENSIONS
        # ==========================================
        sql_lines.append("")
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- SECTION 4: KPI VIEW WITH TIME DIMENSIONS")
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- This view adds date grouping for time intelligence")
        sql_lines.append("-- IMPORTANT: Update 'date_column' to match your actual date column name")
        sql_lines.append("")

        if field_parameters and referenced_measures:
            fp = field_parameters[0]

            sql_lines.append(f"CREATE OR REPLACE VIEW {target_catalog}.{target_schema}.v_kpi_with_dates AS")

            union_parts = []
            for idx, item in enumerate(fp['items']):
                measure = next((m for m in referenced_measures if m['name'] == item['source_measure']), None)
                if measure and measure.get('expression'):
                    sql_expr = self._translate_dax_to_sql_simple(measure['expression'], measure.get('table'))
                    source_table = item['source_table']
                    qualified_table = f"{target_catalog}.{target_schema}.{source_table}"

                    # Generate SQL that groups by date dimensions
                    union_part = f"""SELECT
    '{item['label']}' AS kpi_name,
    {sql_expr} AS kpi_value,
    -- Update these date expressions to match your actual date column
    DATE_TRUNC('day', CURRENT_DATE()) AS report_date,
    YEAR(CURRENT_DATE()) AS report_year,
    MONTH(CURRENT_DATE()) AS report_month
FROM {qualified_table}"""
                    union_parts.append(union_part)

            if union_parts:
                sql_lines.append("\nUNION ALL\n".join(union_parts) + ";")
            sql_lines.append("")

        # ==========================================
        # SECTION 5: TIME INTELLIGENCE VIEWS
        # ==========================================
        sql_lines.append("")
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- SECTION 5: TIME INTELLIGENCE VIEWS")
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- These views apply standard time intelligence patterns")
        sql_lines.append("-- IMPORTANT: Assumes your source tables have a 'date_column' column")
        sql_lines.append("--            Replace 'date_column' with your actual date column name")
        sql_lines.append("")

        if field_parameters and referenced_measures and calculation_groups:
            fp = field_parameters[0]

            # Generate YTD View
            sql_lines.append(f"-- YTD (Year-to-Date) View")
            sql_lines.append(f"CREATE OR REPLACE VIEW {target_catalog}.{target_schema}.v_kpi_ytd AS")

            union_parts = []
            for item in fp['items']:
                measure = next((m for m in referenced_measures if m['name'] == item['source_measure']), None)
                if measure and measure.get('expression'):
                    sql_expr = self._translate_dax_to_sql_simple(measure['expression'], measure.get('table'))
                    source_table = item['source_table']
                    qualified_table = f"{target_catalog}.{target_schema}.{source_table}"

                    union_part = f"""SELECT
    '{item['label']}' AS kpi_name,
    'YTD' AS time_period,
    {sql_expr} AS kpi_value
FROM {qualified_table}
WHERE date_column >= DATE_TRUNC('year', CURRENT_DATE())
  AND date_column <= CURRENT_DATE()"""
                    union_parts.append(union_part)

            if union_parts:
                sql_lines.append("\nUNION ALL\n".join(union_parts) + ";")
            sql_lines.append("")

            # Generate MTD View
            sql_lines.append(f"-- MTD (Month-to-Date) View")
            sql_lines.append(f"CREATE OR REPLACE VIEW {target_catalog}.{target_schema}.v_kpi_mtd AS")

            union_parts = []
            for item in fp['items']:
                measure = next((m for m in referenced_measures if m['name'] == item['source_measure']), None)
                if measure and measure.get('expression'):
                    sql_expr = self._translate_dax_to_sql_simple(measure['expression'], measure.get('table'))
                    source_table = item['source_table']
                    qualified_table = f"{target_catalog}.{target_schema}.{source_table}"

                    union_part = f"""SELECT
    '{item['label']}' AS kpi_name,
    'MTD' AS time_period,
    {sql_expr} AS kpi_value
FROM {qualified_table}
WHERE date_column >= DATE_TRUNC('month', CURRENT_DATE())
  AND date_column <= CURRENT_DATE()"""
                    union_parts.append(union_part)

            if union_parts:
                sql_lines.append("\nUNION ALL\n".join(union_parts) + ";")
            sql_lines.append("")

            # Generate Prior Year View
            sql_lines.append(f"-- PY (Prior Year) View")
            sql_lines.append(f"CREATE OR REPLACE VIEW {target_catalog}.{target_schema}.v_kpi_py AS")

            union_parts = []
            for item in fp['items']:
                measure = next((m for m in referenced_measures if m['name'] == item['source_measure']), None)
                if measure and measure.get('expression'):
                    sql_expr = self._translate_dax_to_sql_simple(measure['expression'], measure.get('table'))
                    source_table = item['source_table']
                    qualified_table = f"{target_catalog}.{target_schema}.{source_table}"

                    union_part = f"""SELECT
    '{item['label']}' AS kpi_name,
    'PY' AS time_period,
    {sql_expr} AS kpi_value
FROM {qualified_table}
WHERE date_column >= DATEADD(year, -1, DATE_TRUNC('year', CURRENT_DATE()))
  AND date_column <= DATEADD(year, -1, CURRENT_DATE())"""
                    union_parts.append(union_part)

            if union_parts:
                sql_lines.append("\nUNION ALL\n".join(union_parts) + ";")
            sql_lines.append("")

        # ==========================================
        # SECTION 6: COMBINED TIME INTELLIGENCE VIEW
        # ==========================================
        sql_lines.append("")
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- SECTION 6: COMBINED TIME INTELLIGENCE VIEW")
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- Master view combining all time periods for easy querying")
        sql_lines.append("-- Query: SELECT * FROM v_kpi_all_periods WHERE kpi_name = 'X' AND time_period = 'YTD'")
        sql_lines.append("")

        if field_parameters and referenced_measures:
            sql_lines.append(f"CREATE OR REPLACE VIEW {target_catalog}.{target_schema}.v_kpi_all_periods AS")
            sql_lines.append(f"SELECT kpi_name, 'Current' AS time_period, kpi_value FROM {target_catalog}.{target_schema}.v_kpi_base")
            sql_lines.append("UNION ALL")
            sql_lines.append(f"SELECT kpi_name, time_period, kpi_value FROM {target_catalog}.{target_schema}.v_kpi_ytd")
            sql_lines.append("UNION ALL")
            sql_lines.append(f"SELECT kpi_name, time_period, kpi_value FROM {target_catalog}.{target_schema}.v_kpi_mtd")
            sql_lines.append("UNION ALL")
            sql_lines.append(f"SELECT kpi_name, time_period, kpi_value FROM {target_catalog}.{target_schema}.v_kpi_py;")
            sql_lines.append("")

            sql_lines.append("-- Usage examples:")
            sql_lines.append(f"-- Get all KPIs with all time periods:")
            sql_lines.append(f"--   SELECT * FROM {target_catalog}.{target_schema}.v_kpi_all_periods;")
            sql_lines.append("")
            sql_lines.append(f"-- Get specific KPI across time periods:")
            sql_lines.append(f"--   SELECT * FROM {target_catalog}.{target_schema}.v_kpi_all_periods WHERE kpi_name = 'Confirmed PHC';")
            sql_lines.append("")
            sql_lines.append(f"-- Compare Current vs YTD for all KPIs:")
            sql_lines.append(f"--   SELECT * FROM {target_catalog}.{target_schema}.v_kpi_all_periods WHERE time_period IN ('Current', 'YTD');")
            sql_lines.append("")

        # ==========================================
        # SECTION 7: YoY COMPARISON VIEW
        # ==========================================
        sql_lines.append("")
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- SECTION 7: YEAR-OVER-YEAR COMPARISON VIEW")
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- Calculates YoY % change by comparing Current to Prior Year")
        sql_lines.append("")

        if field_parameters and referenced_measures:
            sql_lines.append(f"CREATE OR REPLACE VIEW {target_catalog}.{target_schema}.v_kpi_yoy AS")
            sql_lines.append("SELECT")
            sql_lines.append("    c.kpi_name,")
            sql_lines.append("    c.kpi_value AS current_value,")
            sql_lines.append("    p.kpi_value AS prior_year_value,")
            sql_lines.append("    c.kpi_value - p.kpi_value AS yoy_change,")
            sql_lines.append("    CASE WHEN p.kpi_value != 0")
            sql_lines.append("         THEN ROUND((c.kpi_value - p.kpi_value) / p.kpi_value * 100, 2)")
            sql_lines.append("         ELSE NULL")
            sql_lines.append("    END AS yoy_pct_change")
            sql_lines.append(f"FROM {target_catalog}.{target_schema}.v_kpi_base c")
            sql_lines.append(f"LEFT JOIN {target_catalog}.{target_schema}.v_kpi_py p ON c.kpi_name = p.kpi_name;")
            sql_lines.append("")

            sql_lines.append("-- Usage:")
            sql_lines.append(f"--   SELECT * FROM {target_catalog}.{target_schema}.v_kpi_yoy;")
            sql_lines.append(f"--   SELECT * FROM {target_catalog}.{target_schema}.v_kpi_yoy WHERE yoy_pct_change > 10;  -- KPIs up more than 10%")
            sql_lines.append("")

        # Summary
        sql_lines.append("")
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- SUMMARY")
        sql_lines.append("-- ==========================================")
        sql_lines.append(f"-- Field Parameters: {len(field_parameters)}")
        sql_lines.append(f"-- Calculation Groups: {len(calculation_groups)}")
        sql_lines.append(f"-- Referenced Measures: {len(referenced_measures)}")
        sql_lines.append("--")
        sql_lines.append("-- Objects created:")
        sql_lines.append(f"--   TABLE: {target_catalog}.{target_schema}._config_field_parameters  (metadata)")
        sql_lines.append(f"--   TABLE: {target_catalog}.{target_schema}._config_calculation_groups (metadata)")
        sql_lines.append(f"--   TABLE: {target_catalog}.{target_schema}._config_measures           (metadata)")
        sql_lines.append(f"--   VIEW:  {target_catalog}.{target_schema}.v_kpi_base                 (base KPI values)")
        sql_lines.append(f"--   VIEW:  {target_catalog}.{target_schema}.v_kpi_with_dates           (KPIs with date dims)")
        sql_lines.append(f"--   VIEW:  {target_catalog}.{target_schema}.v_kpi_ytd                  (Year-to-Date)")
        sql_lines.append(f"--   VIEW:  {target_catalog}.{target_schema}.v_kpi_mtd                  (Month-to-Date)")
        sql_lines.append(f"--   VIEW:  {target_catalog}.{target_schema}.v_kpi_py                   (Prior Year)")
        sql_lines.append(f"--   VIEW:  {target_catalog}.{target_schema}.v_kpi_all_periods          (Combined view)")
        sql_lines.append(f"--   VIEW:  {target_catalog}.{target_schema}.v_kpi_yoy                  (YoY comparison)")
        sql_lines.append("--")
        sql_lines.append("-- IMPORTANT: Before running, update 'date_column' references to your actual date column!")
        sql_lines.append("--")
        sql_lines.append("-- Quick Start:")
        sql_lines.append(f"--   1. Run this SQL to create all objects")
        sql_lines.append(f"--   2. SELECT * FROM {target_catalog}.{target_schema}.v_kpi_all_periods;")
        sql_lines.append(f"--   3. SELECT * FROM {target_catalog}.{target_schema}.v_kpi_yoy;")

        return "\n".join(sql_lines)

    def _get_sql_pattern_for_calc_item(self, item_name: str) -> str:
        """Get SQL pattern template for a calculation item."""
        name_upper = item_name.upper()
        if name_upper == 'CURRENT':
            return '{measure}'
        elif 'YTD' in name_upper:
            return 'SUM({measure}) WHERE date >= DATE_TRUNC(year)'
        elif 'MTD' in name_upper:
            return 'SUM({measure}) WHERE date >= DATE_TRUNC(month)'
        elif name_upper in ('PY', 'PRIOR YEAR', 'LY'):
            return 'SUM({measure}) WHERE year = year - 1'
        elif 'YOY' in name_upper:
            return '(current - prior) / NULLIF(prior, 0)'
        return '{measure}'

    def _translate_dax_to_sql_simple(self, dax_expression: Optional[str], table_name: Optional[str]) -> str:
        """Simple DAX to SQL translation for common patterns."""
        if not dax_expression:
            return 'NULL'

        sql = dax_expression.strip()

        # Handle common DAX patterns
        # SUM(table[column]) -> SUM(column)
        sql = re.sub(r"SUM\s*\(\s*'?(\w+)'?\s*\[(\w+)\]\s*\)", r"SUM(\2)", sql, flags=re.IGNORECASE)
        # AVERAGE(table[column]) -> AVG(column)
        sql = re.sub(r"AVERAGE\s*\(\s*'?(\w+)'?\s*\[(\w+)\]\s*\)", r"AVG(\2)", sql, flags=re.IGNORECASE)
        # COUNT(table[column]) -> COUNT(column)
        sql = re.sub(r"COUNT\s*\(\s*'?(\w+)'?\s*\[(\w+)\]\s*\)", r"COUNT(\2)", sql, flags=re.IGNORECASE)
        # COUNTROWS(table) -> COUNT(*)
        sql = re.sub(r"COUNTROWS\s*\(\s*'?(\w+)'?\s*\)", r"COUNT(*)", sql, flags=re.IGNORECASE)
        # DISTINCTCOUNT(table[column]) -> COUNT(DISTINCT column)
        sql = re.sub(r"DISTINCTCOUNT\s*\(\s*'?(\w+)'?\s*\[(\w+)\]\s*\)", r"COUNT(DISTINCT \2)", sql, flags=re.IGNORECASE)
        # MIN/MAX
        sql = re.sub(r"MIN\s*\(\s*'?(\w+)'?\s*\[(\w+)\]\s*\)", r"MIN(\2)", sql, flags=re.IGNORECASE)
        sql = re.sub(r"MAX\s*\(\s*'?(\w+)'?\s*\[(\w+)\]\s*\)", r"MAX(\2)", sql, flags=re.IGNORECASE)
        # DIVIDE(a, b) -> a / NULLIF(b, 0)
        sql = re.sub(r"DIVIDE\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)", r"(\1) / NULLIF(\2, 0)", sql, flags=re.IGNORECASE)

        # Escape single quotes for SQL string
        sql = sql.replace("'", "''")

        return sql
