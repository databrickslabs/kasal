"""
Power BI Hierarchies Extraction Tool for CrewAI

Extracts hierarchies from Power BI/Microsoft Fabric semantic models using the
Fabric API getDefinition endpoint which returns TMDL (Tabular Model Definition Language)
format containing hierarchy definitions.

Requires a Service Principal with SemanticModel.ReadWrite.All permissions on the workspace.
Works with Microsoft Fabric workspaces (not legacy Power BI Service without Fabric).

The tool parses TMDL files to extract hierarchy definitions and generates Unity Catalog
dimension view SQL statements.

Author: Kasal Team
Date: 2025
"""

import asyncio
import base64
import logging
import re
from typing import Any, Optional, Type, Dict, List

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

import httpx

logger = logging.getLogger(__name__)


class PowerBIHierarchiesSchema(BaseModel):
    """Input schema for PowerBIHierarchiesTool."""

    # ===== POWER BI CONFIGURATION =====
    workspace_id: Optional[str] = Field(
        None,
        description="[Power BI] Workspace ID containing the semantic model (required). Supports {placeholder} for dynamic mode."
    )
    dataset_id: Optional[str] = Field(
        None,
        description="[Power BI] Dataset/Semantic Model ID to extract hierarchies from (required). Supports {placeholder} for dynamic mode."
    )

    # ===== SERVICE PRINCIPAL AUTHENTICATION (must be workspace member) =====
    tenant_id: Optional[str] = Field(
        None,
        description="[Auth] Azure AD tenant ID (required for SP or Service Account)"
    )
    client_id: Optional[str] = Field(
        None,
        description="[Auth] Application/Client ID - must be a workspace member (required for SP or Service Account)"
    )
    client_secret: Optional[str] = Field(
        None,
        description="[Auth] Client secret (required for Service Principal)"
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
        description="[Auth] Pre-obtained OAuth access token (alternative to SP/Service Account). Use this when authenticating as a user."
    )

    # ===== UNITY CATALOG TARGET CONFIGURATION =====
    target_catalog: str = Field(
        "main",
        description="[Target] Unity Catalog catalog name for dimension views (default: 'main'). Supports {placeholder} for dynamic mode."
    )
    target_schema: str = Field(
        "default",
        description="[Target] Unity Catalog schema name for dimension views (default: 'default'). Supports {placeholder} for dynamic mode."
    )

    # ===== OUTPUT OPTIONS =====
    skip_system_tables: bool = Field(
        True,
        description="[Output] Skip system hierarchies like LocalDateTable (default: True)"
    )
    include_hidden: bool = Field(
        False,
        description="[Output] Include hidden hierarchies (default: False)"
    )


class PowerBIHierarchiesTool(BaseTool):
    """
    Power BI / Microsoft Fabric Hierarchies Extraction Tool.

    Extracts hierarchies from Fabric semantic models using the Fabric API getDefinition
    endpoint which returns TMDL (Tabular Model Definition Language) format. Parses the
    TMDL to extract hierarchy definitions and generates Unity Catalog dimension views.

    **IMPORTANT**: This tool works with Microsoft Fabric workspaces. For legacy Power BI
    Service workspaces (without Fabric), hierarchies require XMLA endpoint access.

    **Requirements**:
    - Service Principal with SemanticModel.ReadWrite.All or Item.ReadWrite.All permissions
    - Workspace must be a Microsoft Fabric workspace
    - Semantic model must not have encrypted sensitivity labels

    **Capabilities**:
    - Extract all hierarchies from a Fabric semantic model via TMDL
    - Extract hierarchy levels with ordinal positions
    - Generate Unity Catalog dimension view SQL
    - Document drill-down paths for BI tools

    **Example Use Cases**:
    1. Migrate Power BI hierarchies to Databricks as dimension views
    2. Document Power BI data model drill-down structures
    3. Generate DDL for dimension tables based on Power BI model
    4. Analyze hierarchy patterns for migration planning

    **Output Format**:
    Returns a formatted report with:
    - List of all hierarchies with their levels
    - Drill-down paths (e.g., Country → City → PostalCode)
    - Unity Catalog CREATE VIEW statements for dimensions
    - Summary statistics
    """

    name: str = "Power BI Hierarchies Tool"
    description: str = (
        "Extracts hierarchies from Microsoft Fabric semantic models using the Fabric API "
        "getDefinition endpoint (TMDL format). Generates Unity Catalog dimension views. "
        "Requires a Service Principal with SemanticModel.ReadWrite.All permissions. "
        "Works with Fabric workspaces only. Configure workspace_id, dataset_id, and credentials."
    )
    args_schema: Type[BaseModel] = PowerBIHierarchiesSchema

    # Private attributes
    _instance_id: str = PrivateAttr()
    _default_config: Dict[str, Any] = PrivateAttr()

    # Allow extra attributes for config
    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the Power BI Hierarchies tool."""
        import uuid
        instance_id = str(uuid.uuid4())[:8]

        logger.info(f"[PowerBIHierarchiesTool.__init__] Instance ID: {instance_id}")
        logger.info(f"[PowerBIHierarchiesTool.__init__] Received kwargs keys: {list(kwargs.keys())}")

        # Extract execution_inputs if provided (for dynamic parameter resolution)
        execution_inputs = kwargs.get("execution_inputs", {})

        # Store config values
        default_config = {
            # Power BI Configuration
            "workspace_id": kwargs.get("workspace_id"),
            "dataset_id": kwargs.get("dataset_id"),
            # Service Principal Authentication
            "tenant_id": kwargs.get("tenant_id"),
            "client_id": kwargs.get("client_id"),
            "client_secret": kwargs.get("client_secret"),
            # Service Account Authentication
            "username": kwargs.get("username"),
            "password": kwargs.get("password"),
            "auth_method": kwargs.get("auth_method"),
            # User OAuth token (alternative to Service Principal)
            "access_token": kwargs.get("access_token"),
            # Unity Catalog Target
            "target_catalog": kwargs.get("target_catalog", "main"),
            "target_schema": kwargs.get("target_schema", "default"),
            # Output Options
            "skip_system_tables": kwargs.get("skip_system_tables", True),
            "include_hidden": kwargs.get("include_hidden", False),
        }

        # DYNAMIC PARAMETER RESOLUTION
        if execution_inputs:
            logger.info(f"[PowerBIHierarchiesTool.__init__] Instance {instance_id} - Resolving parameters from execution_inputs")
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

        # Call parent __init__
        tool_kwargs = {k: v for k, v in kwargs.items() if k not in default_config}
        super().__init__(**tool_kwargs)

        # Set private attributes
        self._instance_id = instance_id
        self._default_config = default_config

        logger.info(f"[PowerBIHierarchiesTool.__init__] Instance {instance_id} initialized")

    def _resolve_parameter(self, value: Any, execution_inputs: Dict[str, Any]) -> Any:
        """Resolve parameter placeholders in configuration values."""
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

        return resolved_value

    def _run(self, **kwargs: Any) -> str:
        """Execute hierarchy extraction."""
        try:
            instance_id = getattr(self, '_instance_id', 'UNKNOWN')
            logger.info(f"[PowerBIHierarchiesTool] Instance {instance_id} - _run() called")

            # Extract execution_inputs if provided
            execution_inputs = kwargs.pop('execution_inputs', {})

            # Filter out placeholder values
            def is_placeholder(value: Any) -> bool:
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

            # CRITICAL: Merge strategy for deterministic authentication
            # - CREDENTIALS: Use pre-configured values (prevent agent placeholder overrides)
            # - AUTH_METHOD: Use UI selection (deterministic, not auto-detected)
            # - OTHER: Agent can override
            credential_fields = ['workspace_id', 'dataset_id', 'tenant_id', 'client_id', 'client_secret', 'username', 'password', 'access_token']
            selection_fields = ['auth_method']  # User selection - must be deterministic

            merged_kwargs = {}
            for key in set(list(self._default_config.keys()) + list(filtered_kwargs.keys())):
                if key in credential_fields:
                    # Credentials: use pre-configured value (protected from agent)
                    merged_kwargs[key] = self._default_config.get(key, filtered_kwargs.get(key))
                elif key in selection_fields:
                    # User selections: UI value takes precedence for deterministic behavior
                    merged_kwargs[key] = self._default_config.get(key, filtered_kwargs.get(key))
                else:
                    # Other fields: agent can override (filtered_kwargs takes precedence)
                    merged_kwargs[key] = filtered_kwargs.get(key, self._default_config.get(key))

            # Dynamic parameter resolution
            if execution_inputs:
                resolved_kwargs = {}
                for key, value in merged_kwargs.items():
                    resolved_kwargs[key] = self._resolve_parameter(value, execution_inputs)
                merged_kwargs = resolved_kwargs

            # Extract parameters
            workspace_id = merged_kwargs.get("workspace_id")
            dataset_id = merged_kwargs.get("dataset_id")

            # DEBUG: Log merged_kwargs to see what we're working with
            logger.info("[PowerBIHierarchiesTool] MERGED KWARGS DEBUG:")
            logger.info(f"  workspace_id: {workspace_id}")
            logger.info(f"  dataset_id: {dataset_id}")
            logger.info(f"  auth_method in merged_kwargs: {merged_kwargs.get('auth_method')}")
            logger.info(f"  username in merged_kwargs: {merged_kwargs.get('username')}")
            logger.info(f"  Has client_secret: {bool(merged_kwargs.get('client_secret'))}")

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

            # DEBUG: Log what auth config is being used
            logger.info("=" * 80)
            logger.info("[PowerBIHierarchiesTool] AUTH CONFIG DEBUG")
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
            if not dataset_id:
                return "Error: dataset_id is required"

            # Validate authentication using shared utility
            from src.engines.crewai.tools.custom.powerbi_auth_utils import validate_auth_config
            is_valid, error_msg = validate_auth_config(auth_config)
            if not is_valid:
                return f"Error: {error_msg}"

            logger.info(f"[PowerBIHierarchiesTool] Extracting hierarchies from dataset {dataset_id}")

            # Run async extraction
            result = self._run_sync(self._extract_hierarchies(
                workspace_id=workspace_id,
                dataset_id=dataset_id,
                auth_config=auth_config,
                target_catalog=merged_kwargs.get("target_catalog", "main"),
                target_schema=merged_kwargs.get("target_schema", "default"),
                skip_system_tables=merged_kwargs.get("skip_system_tables", True),
                include_hidden=merged_kwargs.get("include_hidden", False),
            ))

            return result

        except Exception as e:
            logger.error(f"PowerBIHierarchiesTool error: {str(e)}", exc_info=True)
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

    async def _extract_hierarchies(
        self,
        workspace_id: str,
        dataset_id: str,
        auth_config: Dict[str, Any],
        target_catalog: str,
        target_schema: str,
        skip_system_tables: bool,
        include_hidden: bool,
    ) -> str:
        """Extract hierarchies and format output."""

        # Get access token using shared auth utility
        from src.engines.crewai.tools.custom.powerbi_auth_utils import get_powerbi_access_token_from_config
        token = await get_powerbi_access_token_from_config(auth_config)

        # Fetch hierarchies and levels
        hierarchies = await self._fetch_hierarchies(
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            access_token=token,
            skip_system_tables=skip_system_tables,
            include_hidden=include_hidden,
        )

        # Check if we got an API error
        if hierarchies and len(hierarchies) == 1 and "_error" in hierarchies[0]:
            error_type = hierarchies[0].get("_error")

            if error_type == "not_fabric_workspace":
                return (
                    "# Power BI Hierarchies Extraction\n\n"
                    f"**Workspace**: {workspace_id}\n"
                    f"**Dataset**: {dataset_id}\n\n"
                    "## ⚠️ Not a Fabric Workspace\n\n"
                    "The Fabric API returned 404, indicating this workspace is not a Microsoft Fabric "
                    "workspace or the semantic model ID is incorrect.\n\n"
                    "**Alternative Approaches for Legacy Power BI Workspaces:**\n\n"
                    "1. **XMLA Endpoint** (Premium/PPU):\n"
                    "   - Connect via XMLA endpoint\n"
                    "   - Use tools like DAX Studio or Tabular Editor\n"
                    "   - Query: `SELECT * FROM $SYSTEM.TMSCHEMA_HIERARCHIES`\n\n"
                    "2. **Power BI Desktop Export**:\n"
                    "   - Download the PBIX file\n"
                    "   - Open in Power BI Desktop\n"
                    "   - Export to PBIP format (contains TMDL with hierarchies)\n\n"
                    "3. **Migrate to Microsoft Fabric**:\n"
                    "   - Upgrade workspace to Fabric capacity\n"
                    "   - Re-run this tool with Fabric API access"
                )
            elif error_type == "unauthorized":
                return (
                    "# Power BI Hierarchies Extraction\n\n"
                    f"**Workspace**: {workspace_id}\n"
                    f"**Dataset**: {dataset_id}\n\n"
                    "## ⚠️ Authentication Failed\n\n"
                    "The Fabric API returned 401 Unauthorized.\n\n"
                    "**Please ensure:**\n\n"
                    "1. The Service Principal is registered in Azure AD\n"
                    "2. The Service Principal has `SemanticModel.ReadWrite.All` API permission\n"
                    "3. Admin consent has been granted for the permission\n"
                    "4. The credentials (tenant_id, client_id, client_secret) are correct"
                )
            elif error_type == "forbidden":
                return (
                    "# Power BI Hierarchies Extraction\n\n"
                    f"**Workspace**: {workspace_id}\n"
                    f"**Dataset**: {dataset_id}\n\n"
                    "## ⚠️ Access Forbidden\n\n"
                    "The Fabric API returned 403 Forbidden.\n\n"
                    "**Possible causes:**\n\n"
                    "1. The Service Principal lacks workspace access\n"
                    "2. The semantic model has encrypted sensitivity labels\n"
                    "3. The workspace capacity doesn't allow API access\n\n"
                    "**Resolution:**\n"
                    "- Add the Service Principal as a workspace member/contributor\n"
                    "- Check sensitivity label settings on the semantic model"
                )
            else:
                message = hierarchies[0].get("_message", "Unknown error")
                status = hierarchies[0].get("_status", "N/A")
                return (
                    "# Power BI Hierarchies Extraction\n\n"
                    f"**Workspace**: {workspace_id}\n"
                    f"**Dataset**: {dataset_id}\n\n"
                    "## ⚠️ API Error\n\n"
                    f"The Fabric API returned an error.\n\n"
                    f"**Status Code**: {status}\n"
                    f"**Message**: {message}\n\n"
                    "**Troubleshooting:**\n"
                    "1. Verify the workspace_id and dataset_id are correct\n"
                    "2. Check Service Principal permissions\n"
                    "3. Ensure the workspace is a Microsoft Fabric workspace"
                )

        if not hierarchies:
            return (
                "# Power BI Hierarchies Extraction\n\n"
                f"**Workspace**: {workspace_id}\n"
                f"**Dataset**: {dataset_id}\n\n"
                "No hierarchies found in the semantic model.\n\n"
                "This could mean:\n"
                "- The model has no user-defined hierarchies\n"
                "- All hierarchies involve system tables (set skip_system_tables=False to include them)\n"
                "- All hierarchies are hidden (set include_hidden=True to include them)"
            )

        # Generate dimension SQL
        sql_output = self._generate_dimension_sql(
            hierarchies=hierarchies,
            target_catalog=target_catalog,
            target_schema=target_schema,
        )

        # Format output
        return self._format_output(
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            hierarchies=hierarchies,
            sql_output=sql_output,
            target_catalog=target_catalog,
            target_schema=target_schema,
        )

    async def _fetch_hierarchies(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        skip_system_tables: bool,
        include_hidden: bool,
    ) -> List[Dict[str, Any]]:
        """
        Fetch hierarchies using the Microsoft Fabric API getDefinition endpoint.

        The Fabric API returns the semantic model definition in TMDL format,
        which includes hierarchy definitions. This method parses the TMDL to
        extract hierarchy information.

        Flow:
        1. POST to getDefinition → returns 202 with Location header
        2. Poll the Location URL until status is "Succeeded"
        3. GET location + "/result" to get the actual TMDL definition
        4. Parse TMDL files to extract hierarchies

        If the Fabric API is not available (non-Fabric workspace), returns an
        error marker with guidance on alternative approaches.
        """
        # Fabric API endpoint for getting semantic model definition
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels/{dataset_id}/getDefinition?format=TMDL"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        logger.info("Fetching semantic model definition from Fabric API...")

        async with httpx.AsyncClient(timeout=180.0) as client:
            try:
                # Step 1: POST to getDefinition endpoint (initiates long-running operation)
                response = await client.post(url, headers=headers)

                definition_data = None

                # Handle 202 Accepted (long running operation)
                if response.status_code == 202:
                    location = response.headers.get("Location")
                    if not location:
                        return [{"_error": "api_error", "_message": "No Location header in 202 response"}]

                    retry_after = int(response.headers.get("Retry-After", "5"))
                    logger.info(f"Long-running operation started. Polling every {retry_after}s...")

                    # Step 2: Poll until status is "Succeeded"
                    for attempt in range(60):  # Max 5 minutes of polling
                        await asyncio.sleep(retry_after)
                        poll_response = await client.get(location, headers=headers)

                        if poll_response.status_code == 200:
                            poll_data = poll_response.json()
                            status = poll_data.get("status", "")

                            if status == "Succeeded":
                                logger.info(f"Operation succeeded after {attempt + 1} poll(s)")

                                # Step 3: GET the result from location + "/result"
                                result_url = location + "/result" if not location.endswith("/result") else location
                                result_response = await client.get(result_url, headers=headers)
                                result_response.raise_for_status()
                                definition_data = result_response.json()
                                break
                            elif status == "Failed":
                                error_msg = poll_data.get("error", "Unknown error")
                                logger.error(f"Operation failed: {error_msg}")
                                return [{"_error": "api_error", "_message": f"Operation failed: {error_msg}"}]
                            else:
                                # Still running, continue polling
                                logger.debug(f"Status: {status}, continuing to poll...")
                                continue

                        elif poll_response.status_code == 202:
                            # Still processing
                            retry_after = int(poll_response.headers.get("Retry-After", "5"))
                            continue
                        else:
                            poll_response.raise_for_status()
                    else:
                        return [{"_error": "timeout", "_message": "Fabric API request timed out after 5 minutes"}]

                elif response.status_code == 200:
                    # Direct response (no long-running operation)
                    definition_data = response.json()
                else:
                    response.raise_for_status()

                if not definition_data:
                    return [{"_error": "api_error", "_message": "No definition data received"}]

                # Step 4: Parse the TMDL parts
                parts = definition_data.get("definition", {}).get("parts", [])
                if not parts:
                    logger.warning("No TMDL parts found in definition response")
                    return []

                # Find and parse table TMDL files for hierarchies
                hierarchies = []
                for part in parts:
                    path = part.get("path", "")
                    payload = part.get("payload", "")

                    # Only process table definition files
                    if "definition/tables/" in path and path.endswith(".tmdl"):
                        try:
                            # Decode base64 content
                            tmdl_content = base64.b64decode(payload).decode("utf-8")

                            # Parse hierarchies from TMDL
                            table_hierarchies = self._parse_tmdl_hierarchies(
                                tmdl_content,
                                skip_system_tables,
                                include_hidden
                            )
                            hierarchies.extend(table_hierarchies)

                        except Exception as e:
                            logger.warning(f"Failed to parse TMDL from {path}: {e}")

                logger.info(f"Extracted {len(hierarchies)} hierarchy(ies) from TMDL")
                return hierarchies

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.warning(
                        "Fabric API returned 404. This may indicate the workspace is not a "
                        "Microsoft Fabric workspace or the semantic model ID is incorrect."
                    )
                    return [{"_error": "not_fabric_workspace"}]
                elif e.response.status_code == 401:
                    logger.warning(
                        "Fabric API returned 401 Unauthorized. Ensure the Service Principal "
                        "has SemanticModel.ReadWrite.All permissions."
                    )
                    return [{"_error": "unauthorized"}]
                elif e.response.status_code == 403:
                    logger.warning(
                        "Fabric API returned 403 Forbidden. The Service Principal may lack "
                        "required permissions or the semantic model has encrypted sensitivity labels."
                    )
                    return [{"_error": "forbidden"}]
                else:
                    logger.error(f"Fabric API error: {e.response.status_code} - {e.response.text}")
                    return [{"_error": "api_error", "_status": e.response.status_code}]
            except Exception as e:
                logger.error(f"Failed to fetch hierarchies from Fabric API: {e}")
                return [{"_error": "api_error", "_message": str(e)}]

    def _parse_tmdl_hierarchies(
        self,
        tmdl_content: str,
        skip_system_tables: bool,
        include_hidden: bool
    ) -> List[Dict[str, Any]]:
        """
        Parse hierarchy definitions from TMDL content.

        TMDL hierarchy format:
        ```
        table 'Table Name'

            hierarchy 'Hierarchy Name'
                isHidden
                description: Some description
                level 'Level Name'
                    column: ColumnName
                level 'Another Level'
                    column: AnotherColumn
        ```
        """
        hierarchies = []

        # Extract table name from the first line
        table_match = re.match(r"table\s+(?:'([^']+)'|(\w+))", tmdl_content.strip())
        if not table_match:
            return []

        table_name = table_match.group(1) or table_match.group(2)

        # Skip system tables if requested
        if skip_system_tables:
            if "LocalDateTable" in table_name or "DateTableTemplate" in table_name:
                return []

        # Find all hierarchy blocks
        # Pattern matches: hierarchy 'Name' or hierarchy Name followed by indented content
        hierarchy_pattern = re.compile(
            r"^\s*hierarchy\s+(?:'([^']+)'|(\w+))\s*\n((?:\s+.*\n)*)",
            re.MULTILINE
        )

        for match in hierarchy_pattern.finditer(tmdl_content):
            hier_name = match.group(1) or match.group(2)
            hier_content = match.group(3)

            # Parse hierarchy properties
            is_hidden = "isHidden" in hier_content

            # Skip hidden if not requested
            if not include_hidden and is_hidden:
                continue

            # Extract description
            description_match = re.search(r"description:\s*(.+)", hier_content)
            description = description_match.group(1).strip() if description_match else None

            # Parse levels
            levels = []
            level_pattern = re.compile(
                r"level\s+(?:'([^']+)'|(\w+))\s*\n((?:\s+.*\n)*?(?=\s*level\s|\Z))",
                re.MULTILINE
            )

            for ordinal, level_match in enumerate(level_pattern.finditer(hier_content)):
                level_name = level_match.group(1) or level_match.group(2)
                level_content = level_match.group(3)

                # Extract column reference
                column_match = re.search(r"column:\s*(?:'([^']+)'|(\w+))", level_content)
                column_name = ""
                if column_match:
                    column_name = column_match.group(1) or column_match.group(2)

                levels.append({
                    "name": level_name,
                    "ordinal": ordinal,
                    "column": column_name,
                    "description": None,
                })

            if levels:  # Only add hierarchies that have levels
                hierarchies.append({
                    "id": f"{table_name}_{hier_name}",
                    "name": hier_name,
                    "table": table_name,
                    "is_hidden": is_hidden,
                    "description": description,
                    "levels": levels,
                    "columns_ordered": [lvl["column"] for lvl in levels],
                })

        return hierarchies

    def _generate_dimension_sql(
        self,
        hierarchies: List[Dict[str, Any]],
        target_catalog: str,
        target_schema: str,
    ) -> Dict[str, Any]:
        """
        Generate Unity Catalog SQL statements for hierarchies.

        Returns:
            Dict with:
            - dimension_views: List of CREATE VIEW statements with hierarchy_path
            - metadata_table_ddl: CREATE TABLE statement for _metadata_hierarchies
            - metadata_inserts: INSERT statements for hierarchy metadata
        """
        dimension_views = []
        metadata_inserts = []

        for hier in hierarchies:
            table_clean = hier["table"].replace(" ", "_").replace("-", "_")
            hier_name_clean = hier["name"].lower().replace(" ", "_").replace("-", "_")

            # Build column list with level comments
            column_defs = []
            columns_ordered = hier["columns_ordered"]

            for level in hier["levels"]:
                column_defs.append(
                    f"    {level['column']}  -- Level {level['ordinal']}: {level['name']}"
                )

            columns_sql = ",\n".join(column_defs)

            # Build hierarchy_path CONCAT expression
            if len(columns_ordered) > 1:
                path_parts = [f"CAST({col} AS STRING)" for col in columns_ordered[:-1]]
                hierarchy_path_expr = "CONCAT(" + ", ' > ', ".join(path_parts) + ")"
            elif len(columns_ordered) == 1:
                hierarchy_path_expr = f"CAST({columns_ordered[0]} AS STRING)"
            else:
                hierarchy_path_expr = "''"

            # Build drill-down path string for comments
            drill_path = " → ".join([lvl["name"] for lvl in hier["levels"]])

            # Generate dimension view SQL (Option B style)
            view_sql = f"""-- ============================================================
-- Hierarchy: {hier['name']}
-- Table: {hier['table']}
-- Drill-down path: {drill_path}
-- Hidden: {hier['is_hidden']}
-- ============================================================
CREATE OR REPLACE VIEW {target_catalog}.{target_schema}.dim_{table_clean}_{hier_name_clean} AS
SELECT DISTINCT
{columns_sql},
    -- Hierarchy path for BI tools (drill-down navigation)
    {hierarchy_path_expr} AS hierarchy_path
FROM {target_catalog}.{target_schema}.{table_clean}
ORDER BY {', '.join(columns_ordered)};"""

            dimension_views.append(view_sql)

            # Generate metadata INSERT statements (Option C style)
            for level in hier["levels"]:
                metadata_inserts.append(
                    f"('{hier['name']}', '{hier['table']}', {level['ordinal']}, "
                    f"'{level['column']}', '{level['name']}')"
                )

        # Generate metadata table DDL
        metadata_table_ddl = f"""-- ============================================================
-- Hierarchy Metadata Table
-- Stores Power BI hierarchy definitions for documentation and tooling
-- ============================================================
CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}._metadata_hierarchies (
    hierarchy_name STRING COMMENT 'Name of the hierarchy',
    table_name STRING COMMENT 'Source table containing the hierarchy',
    level_order INT COMMENT 'Position in hierarchy (0 = top level)',
    column_name STRING COMMENT 'Column name in the source table',
    display_name STRING COMMENT 'Display name for the level',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'When this record was created'
)
COMMENT 'Metadata table documenting Power BI hierarchies migrated to Unity Catalog';"""

        # Generate bulk INSERT statement
        if metadata_inserts:
            values_str = ",\n    ".join(metadata_inserts)
            metadata_insert_sql = f"""-- Insert hierarchy metadata
INSERT INTO {target_catalog}.{target_schema}._metadata_hierarchies
    (hierarchy_name, table_name, level_order, column_name, display_name)
VALUES
    {values_str};"""
        else:
            metadata_insert_sql = "-- No hierarchies to insert"

        return {
            "dimension_views": dimension_views,
            "metadata_table_ddl": metadata_table_ddl,
            "metadata_insert_sql": metadata_insert_sql,
        }

    def _format_output(
        self,
        workspace_id: str,
        dataset_id: str,
        hierarchies: List[Dict[str, Any]],
        sql_output: Dict[str, Any],
        target_catalog: str,
        target_schema: str,
    ) -> str:
        """Format the extraction output."""
        output = []

        output.append("# Power BI Hierarchies Extraction Results\n")
        output.append(f"**Workspace ID**: {workspace_id}")
        output.append(f"**Dataset ID**: {dataset_id}")
        output.append(f"**Target Location**: {target_catalog}.{target_schema}")
        output.append(f"**Hierarchies Found**: {len(hierarchies)}\n")

        # List hierarchies
        output.append("## Hierarchies\n")
        for hier in hierarchies:
            drill_path = " → ".join([lvl["name"] for lvl in hier["levels"]])
            hidden_str = " (HIDDEN)" if hier["is_hidden"] else ""

            output.append(f"### {hier['name']}{hidden_str}")
            output.append(f"- **Table**: {hier['table']}")
            output.append(f"- **Levels**: {len(hier['levels'])}")
            output.append(f"- **Drill-down**: {drill_path}")
            if hier.get("description"):
                output.append(f"- **Description**: {hier['description']}")
            output.append("")

            # List levels
            output.append("**Level Details:**")
            for level in hier["levels"]:
                output.append(f"  {level['ordinal']}. **{level['name']}** → Column: `{level['column']}`")
            output.append("")

        # Option B: Dimension View SQL statements
        output.append("## Option B: Dimension Views with Hierarchy Path\n")
        output.append("```sql")
        output.append("\n\n".join(sql_output["dimension_views"]))
        output.append("```\n")

        # Option C: Metadata Table
        output.append("## Option C: Hierarchy Metadata Table\n")
        output.append("### Table DDL\n")
        output.append("```sql")
        output.append(sql_output["metadata_table_ddl"])
        output.append("```\n")

        output.append("### Insert Statements\n")
        output.append("```sql")
        output.append(sql_output["metadata_insert_sql"])
        output.append("```\n")

        # Summary
        output.append("## Summary\n")
        total_levels = sum(len(h["levels"]) for h in hierarchies)
        hidden_count = sum(1 for h in hierarchies if h["is_hidden"])

        output.append(f"- **Total Hierarchies**: {len(hierarchies)}")
        output.append(f"- **Total Levels**: {total_levels}")
        output.append(f"- **Hidden Hierarchies**: {hidden_count}")

        # Tables with hierarchies
        tables = set(h["table"] for h in hierarchies)
        output.append(f"- **Tables with Hierarchies**: {len(tables)}")
        for table in sorted(tables):
            hier_names = [h["name"] for h in hierarchies if h["table"] == table]
            output.append(f"  - {table}: {', '.join(hier_names)}")

        return "\n".join(output)
