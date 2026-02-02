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
        description="[Power BI] Workspace ID containing the semantic model. Supports {workspace_id} placeholder."
    )
    dataset_id: Optional[str] = Field(
        None,
        description="[Power BI] Dataset/Semantic Model ID to extract from. Supports {dataset_id} placeholder."
    )

    # ===== SERVICE PRINCIPAL AUTHENTICATION =====
    tenant_id: Optional[str] = Field(
        None,
        description="[Auth] Azure AD tenant ID for Service Principal authentication."
    )
    client_id: Optional[str] = Field(
        None,
        description="[Auth] Application/Client ID for Service Principal authentication."
    )
    client_secret: Optional[str] = Field(
        None,
        description="[Auth] Client secret for Service Principal authentication."
    )

    # User OAuth token (alternative to Service Principal)
    access_token: Optional[str] = Field(
        None,
        description="[Auth] Pre-obtained OAuth access token (alternative to Service Principal)."
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

        # Extract execution_inputs for dynamic parameter resolution
        execution_inputs = kwargs.get("execution_inputs", {})

        # Store configuration values
        default_config = {
            "workspace_id": kwargs.get("workspace_id"),
            "dataset_id": kwargs.get("dataset_id"),
            "tenant_id": kwargs.get("tenant_id"),
            "client_id": kwargs.get("client_id"),
            "client_secret": kwargs.get("client_secret"),
            "access_token": kwargs.get("access_token"),
            "target_catalog": kwargs.get("target_catalog", "main"),
            "target_schema": kwargs.get("target_schema", "default"),
            "llm_workspace_url": kwargs.get("llm_workspace_url"),
            "llm_token": kwargs.get("llm_token"),
            "llm_model": kwargs.get("llm_model", "databricks-claude-sonnet-4"),
            "translate_measures": kwargs.get("translate_measures", True),
            "include_sql_translation": kwargs.get("include_sql_translation", True),
            "include_metadata_tables": kwargs.get("include_metadata_tables", True),
            "output_format": kwargs.get("output_format", "markdown"),
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

            # Filter placeholder values
            def is_placeholder(value: Any) -> bool:
                if not isinstance(value, str):
                    return False
                patterns = ["your_", "placeholder", "example_", "xxx", "insert_", "<"]
                return any(p in value.lower() for p in patterns)

            filtered_kwargs = {
                k: v for k, v in kwargs.items()
                if v is not None and not is_placeholder(v)
            }

            # Merge with defaults
            merged_kwargs = {**self._default_config, **filtered_kwargs}

            # Resolve dynamic parameters
            if execution_inputs:
                resolved_kwargs = {}
                for key, value in merged_kwargs.items():
                    resolved_kwargs[key] = self._resolve_placeholder(value, execution_inputs)
                merged_kwargs = resolved_kwargs

            # Extract and validate parameters
            workspace_id = merged_kwargs.get("workspace_id")
            dataset_id = merged_kwargs.get("dataset_id")
            tenant_id = merged_kwargs.get("tenant_id")
            client_id = merged_kwargs.get("client_id")
            client_secret = merged_kwargs.get("client_secret")
            access_token = merged_kwargs.get("access_token")

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
                ("tenant_id", tenant_id),
                ("client_id", client_id),
                ("client_secret", client_secret),
                ("access_token", access_token),
            ]:
                if has_unresolved_placeholder(param_value):
                    unresolved.append(param_name)

            if unresolved:
                return (
                    f"Error: Unresolved placeholder(s) detected: {', '.join(unresolved)}\n\n"
                    "This usually means the tool is configured in **dynamic mode** but no "
                    "execution_inputs were provided.\n\n"
                    "**Solutions**:\n"
                    "1. Switch to **Static** mode in the UI and enter actual values\n"
                    "2. Or provide values via execution_inputs when calling the crew\n\n"
                    "For static configuration, enter real credentials (not {placeholder} values)."
                )

            # Validate required parameters
            if not workspace_id:
                return "Error: workspace_id is required. Provide via parameter or execution_inputs."
            if not dataset_id:
                return "Error: dataset_id is required. Provide via parameter or execution_inputs."

            # Check authentication - ensure values are not empty placeholders
            has_spn_auth = all([
                tenant_id and not has_unresolved_placeholder(tenant_id),
                client_id and not has_unresolved_placeholder(client_id),
                client_secret and not has_unresolved_placeholder(client_secret)
            ])
            has_token_auth = bool(access_token and not has_unresolved_placeholder(access_token))

            if not has_spn_auth and not has_token_auth:
                return (
                    "Error: Authentication required.\n\n"
                    "Provide either:\n"
                    "- **Service Principal**: tenant_id + client_id + client_secret\n"
                    "- **User OAuth**: access_token\n\n"
                    "Service Principal requires SemanticModel.ReadWrite.All permission."
                )

            # Run async extraction
            result = self._run_sync(self._extract_and_process(
                workspace_id=workspace_id,
                dataset_id=dataset_id,
                tenant_id=tenant_id if has_spn_auth else None,
                client_id=client_id if has_spn_auth else None,
                client_secret=client_secret if has_spn_auth else None,
                access_token=access_token if has_token_auth else None,
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
        tenant_id: Optional[str],
        client_id: Optional[str],
        client_secret: Optional[str],
        access_token: Optional[str],
        target_catalog: str,
        target_schema: str,
        include_sql_translation: bool,
        include_metadata_tables: bool,
        output_format: str,
    ) -> str:
        """Main extraction and processing logic."""

        # Step 1: Get access token
        if access_token:
            token = access_token
            logger.info("Using provided access token")
        else:
            logger.info("Obtaining access token via Service Principal")
            # Type assertion: We know these are not None because we validated has_spn_auth before calling
            assert tenant_id is not None, "tenant_id is required for Service Principal auth"
            assert client_id is not None, "client_id is required for Service Principal auth"
            assert client_secret is not None, "client_secret is required for Service Principal auth"
            token = await self._get_access_token(tenant_id, client_id, client_secret)

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

    async def _get_access_token(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
    ) -> str:
        """Get OAuth access token using Service Principal."""
        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://api.fabric.microsoft.com/.default"
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, data=data)
            response.raise_for_status()
            return response.json()["access_token"]

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
        _referenced_measures: List[Dict[str, Any]],  # Reserved for future use
        target_catalog: str,
        target_schema: str,
    ) -> str:
        """Format output as SQL statements only."""
        sql_lines = []

        sql_lines.append("-- Power BI Field Parameters & Calculation Groups SQL Export")
        sql_lines.append(f"-- Target: {target_catalog}.{target_schema}")
        sql_lines.append("")

        # Field Parameters Config Table
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- FIELD PARAMETERS CONFIGURATION")
        sql_lines.append("-- ==========================================")
        sql_lines.append("")
        sql_lines.append(f"CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}._config_field_parameters (")
        sql_lines.append("    parameter_name STRING,")
        sql_lines.append("    label STRING,")
        sql_lines.append("    source_table STRING,")
        sql_lines.append("    source_measure STRING,")
        sql_lines.append("    ordinal INT,")
        sql_lines.append("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()")
        sql_lines.append(");")
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
        sql_lines.append("-- ==========================================")
        sql_lines.append("-- CALCULATION GROUPS CONFIGURATION")
        sql_lines.append("-- ==========================================")
        sql_lines.append("")
        sql_lines.append(f"CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}._config_calculation_groups (")
        sql_lines.append("    group_name STRING,")
        sql_lines.append("    item_name STRING,")
        sql_lines.append("    dax_expression STRING,")
        sql_lines.append("    precedence INT,")
        sql_lines.append("    ordinal INT,")
        sql_lines.append("    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()")
        sql_lines.append(");")
        sql_lines.append("")

        if calculation_groups:
            sql_lines.append(f"INSERT INTO {target_catalog}.{target_schema}._config_calculation_groups")
            sql_lines.append("(group_name, item_name, dax_expression, precedence, ordinal)")
            sql_lines.append("VALUES")
            inserts = []
            for cg in calculation_groups:
                for item in cg['items']:
                    escaped_expr = item['expression'].replace("'", "''").replace('\n', '\\n')
                    inserts.append(
                        f"('{cg['name']}', '{item['name']}', '{escaped_expr}', "
                        f"{cg['precedence']}, {item['ordinal']})"
                    )
            sql_lines.append(",\n".join(inserts) + ";")

        return "\n".join(sql_lines)
