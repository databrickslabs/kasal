"""
Power BI Relationships Extraction Tool for CrewAI

Extracts relationships from Power BI semantic models using the Execute Queries API
with INFO.VIEW.RELATIONSHIPS() DAX function and converts them to Unity Catalog
Foreign Key constraints.

Requires a Service Principal that is a WORKSPACE MEMBER with dataset read permissions.
This is different from the Admin API which requires admin-level permissions.

Author: Kasal Team
Date: 2025
"""

import asyncio
import logging
from typing import Any, Optional, Type, Dict, List

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

import httpx

logger = logging.getLogger(__name__)


class PowerBIRelationshipsSchema(BaseModel):
    """Input schema for PowerBIRelationshipsTool."""

    # ===== POWER BI CONFIGURATION =====
    workspace_id: Optional[str] = Field(
        None,
        description="[Power BI] Workspace ID containing the semantic model (required). Supports {placeholder} for dynamic mode."
    )
    dataset_id: Optional[str] = Field(
        None,
        description="[Power BI] Dataset/Semantic Model ID to extract relationships from (required). Supports {placeholder} for dynamic mode."
    )

    # ===== SERVICE PRINCIPAL AUTHENTICATION (must be workspace member) =====
    tenant_id: Optional[str] = Field(
        None,
        description="[Auth] Azure AD tenant ID (required)"
    )
    client_id: Optional[str] = Field(
        None,
        description="[Auth] Application/Client ID - must be a workspace member (required)"
    )
    client_secret: Optional[str] = Field(
        None,
        description="[Auth] Client secret (required)"
    )

    # ===== UNITY CATALOG TARGET CONFIGURATION =====
    target_catalog: str = Field(
        "main",
        description="[Target] Unity Catalog catalog name for FK statements (default: 'main'). Supports {placeholder} for dynamic mode."
    )
    target_schema: str = Field(
        "default",
        description="[Target] Unity Catalog schema name for FK statements (default: 'default'). Supports {placeholder} for dynamic mode."
    )

    # ===== OUTPUT OPTIONS =====
    include_inactive: bool = Field(
        False,
        description="[Output] Include inactive relationships (default: False)"
    )
    skip_system_tables: bool = Field(
        True,
        description="[Output] Skip system tables like LocalDateTable (default: True)"
    )


class PowerBIRelationshipsTool(BaseTool):
    """
    Power BI Relationships Extraction Tool.

    Extracts relationships from Power BI semantic models using the Execute Queries API
    with INFO.VIEW.RELATIONSHIPS() DAX function. Generates Unity Catalog Foreign Key
    constraint statements (NOT ENFORCED).

    **IMPORTANT**: This tool requires a Service Principal that is a MEMBER of the
    Power BI workspace with at least read permissions on the dataset. This is different
    from the Admin API which requires admin-level permissions.

    **Capabilities**:
    - Extract all relationships from a Power BI semantic model
    - Generate Unity Catalog FK constraint SQL (NOT ENFORCED)
    - Filter inactive relationships and system tables
    - Support for all cardinality types (1:1, 1:*, *:1, *:*)

    **Example Use Cases**:
    1. Migrate Power BI relationships to Unity Catalog as informational FKs
    2. Document Power BI data model relationships
    3. Generate DDL for Databricks tables based on Power BI model
    4. Analyze relationship patterns for migration planning

    **Output Format**:
    Returns a formatted report with:
    - List of all relationships with cardinality
    - Unity Catalog ALTER TABLE statements for each relationship
    - Summary statistics
    """

    name: str = "Power BI Relationships Tool"
    description: str = (
        "Extracts relationships from Power BI semantic models and generates Unity Catalog "
        "Foreign Key constraints. Uses INFO.VIEW.RELATIONSHIPS() via Execute Queries API. "
        "IMPORTANT: Requires a Service Principal that is a WORKSPACE MEMBER (not Admin API). "
        "Configure workspace_id, dataset_id, and Service Principal credentials."
    )
    args_schema: Type[BaseModel] = PowerBIRelationshipsSchema

    # Private attributes
    _instance_id: str = PrivateAttr()
    _default_config: Dict[str, Any] = PrivateAttr()

    # Allow extra attributes for config
    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the Power BI Relationships tool."""
        import uuid
        instance_id = str(uuid.uuid4())[:8]

        logger.info(f"[PowerBIRelationshipsTool.__init__] Instance ID: {instance_id}")
        logger.info(f"[PowerBIRelationshipsTool.__init__] Received kwargs keys: {list(kwargs.keys())}")
        logger.info(f"[PowerBIRelationshipsTool.__init__] workspace_id: {kwargs.get('workspace_id', 'NOT PROVIDED')}")

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
            # Unity Catalog Target
            "target_catalog": kwargs.get("target_catalog", "main"),
            "target_schema": kwargs.get("target_schema", "default"),
            # Output Options
            "include_inactive": kwargs.get("include_inactive", False),
            "skip_system_tables": kwargs.get("skip_system_tables", True),
        }

        # DYNAMIC PARAMETER RESOLUTION: If execution_inputs provided, resolve placeholders during init
        if execution_inputs:
            logger.info(f"[PowerBIRelationshipsTool.__init__] Instance {instance_id} - Resolving parameters from execution_inputs: {list(execution_inputs.keys())}")
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

        logger.info(f"[PowerBIRelationshipsTool.__init__] Instance {instance_id} initialized with config keys: {list(default_config.keys())}")

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
        Execute relationship extraction.

        Returns:
            Formatted output with relationships and FK statements
        """
        try:
            instance_id = getattr(self, '_instance_id', 'UNKNOWN')
            logger.info(f"[PowerBIRelationshipsTool] Instance {instance_id} - _run() called")
            logger.info(f"[PowerBIRelationshipsTool] Instance {instance_id} - Received kwargs: {list(kwargs.keys())}")

            # Extract execution_inputs if provided
            execution_inputs = kwargs.pop('execution_inputs', {})
            logger.info(f"[PowerBIRelationshipsTool] Instance {instance_id} - Execution inputs: {list(execution_inputs.keys())}")

            # Merge agent-provided kwargs with pre-configured defaults
            # Filter out None values and placeholder-like strings
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
            logger.info(f"[PowerBIRelationshipsTool] Instance {instance_id} - Filtered kwargs: {list(filtered_kwargs.keys())}")

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

            # Validate required parameters
            if not workspace_id:
                return "Error: workspace_id is required"
            if not dataset_id:
                return "Error: dataset_id is required"
            if not all([tenant_id, client_id, client_secret]):
                return "Error: Service Principal credentials required (tenant_id, client_id, client_secret)"

            logger.info(f"[PowerBIRelationshipsTool] Extracting relationships from dataset {dataset_id}")

            # Run async extraction
            result = self._run_sync(self._extract_relationships(
                workspace_id=workspace_id,
                dataset_id=dataset_id,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
                target_catalog=merged_kwargs.get("target_catalog", "main"),
                target_schema=merged_kwargs.get("target_schema", "default"),
                include_inactive=merged_kwargs.get("include_inactive", False),
                skip_system_tables=merged_kwargs.get("skip_system_tables", True),
            ))

            return result

        except Exception as e:
            logger.error(f"PowerBIRelationshipsTool error: {str(e)}", exc_info=True)
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

    async def _extract_relationships(
        self,
        workspace_id: str,
        dataset_id: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        target_catalog: str,
        target_schema: str,
        include_inactive: bool,
        skip_system_tables: bool,
    ) -> str:
        """Extract relationships and format output."""

        # Get access token
        token = await self._get_access_token(tenant_id, client_id, client_secret)

        # Execute INFO.VIEW.RELATIONSHIPS() query
        relationships = await self._fetch_relationships(
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            access_token=token,
            include_inactive=include_inactive,
            skip_system_tables=skip_system_tables,
        )

        if not relationships:
            return (
                "# Power BI Relationships Extraction\n\n"
                f"**Workspace**: {workspace_id}\n"
                f"**Dataset**: {dataset_id}\n\n"
                "No relationships found in the semantic model.\n\n"
                "This could mean:\n"
                "- The model has no defined relationships\n"
                "- All relationships are inactive (set include_inactive=True to include them)\n"
                "- All relationships involve system tables (set skip_system_tables=False to include them)"
            )

        # Generate FK statements
        fk_statements = self._generate_fk_statements(
            relationships=relationships,
            target_catalog=target_catalog,
            target_schema=target_schema,
        )

        # Format output
        return self._format_output(
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            relationships=relationships,
            fk_statements=fk_statements,
            target_catalog=target_catalog,
            target_schema=target_schema,
        )

    async def _get_access_token(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str
    ) -> str:
        """Get OAuth access token using Service Principal."""
        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://analysis.windows.net/powerbi/api/.default"
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, data=data)
            response.raise_for_status()
            return response.json()["access_token"]

    async def _fetch_relationships(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        include_inactive: bool,
        skip_system_tables: bool,
    ) -> List[Dict[str, Any]]:
        """Fetch relationships using INFO.VIEW.RELATIONSHIPS() DAX function."""
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
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()

        # Extract rows from response
        rows = data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])

        # Parse and deduplicate relationships
        relationships = []
        seen_ids = set()

        for row in rows:
            rel_id = row.get("[ID]")

            # Skip duplicates (bidirectional relationships appear twice)
            if rel_id in seen_ids:
                continue
            seen_ids.add(rel_id)

            from_table = row.get("[FromTable]", "")
            to_table = row.get("[ToTable]", "")
            is_active = row.get("[IsActive]", True)

            # Skip inactive if not requested
            if not include_inactive and not is_active:
                continue

            # Skip system tables if requested
            if skip_system_tables:
                if "LocalDateTable" in from_table or "LocalDateTable" in to_table:
                    continue
                if "DateTableTemplate" in from_table or "DateTableTemplate" in to_table:
                    continue

            # Map cardinality
            from_card = row.get("[FromCardinality]", "")
            to_card = row.get("[ToCardinality]", "")

            relationships.append({
                "id": rel_id,
                "name": row.get("[Name]", ""),
                "from_table": from_table,
                "from_column": row.get("[FromColumn]", ""),
                "from_cardinality": from_card,
                "to_table": to_table,
                "to_column": row.get("[ToColumn]", ""),
                "to_cardinality": to_card,
                "is_active": is_active,
                "cross_filtering": row.get("[CrossFilteringBehavior]", ""),
                "security_filtering": row.get("[SecurityFilteringBehavior]", ""),
            })

        logger.info(f"Extracted {len(relationships)} relationship(s)")
        return relationships

    def _generate_fk_statements(
        self,
        relationships: List[Dict[str, Any]],
        target_catalog: str,
        target_schema: str,
    ) -> List[str]:
        """Generate Unity Catalog FK constraint statements."""
        statements = []

        for rel in relationships:
            # Clean table/column names
            from_table = rel["from_table"].replace(" ", "_").replace("-", "_")
            to_table = rel["to_table"].replace(" ", "_").replace("-", "_")
            from_column = rel["from_column"]
            to_column = rel["to_column"]

            # Generate constraint name
            constraint_name = f"fk_{from_table}_{from_column}_{to_table}"
            if len(constraint_name) > 128:
                constraint_name = constraint_name[:128]

            # Format cardinality for comment
            from_card = "*" if rel["from_cardinality"] == "Many" else "1"
            to_card = "*" if rel["to_cardinality"] == "Many" else "1"
            cardinality_str = f"{from_card} to {to_card}"

            sql = (
                f"-- Relationship: {rel['name']}\n"
                f"-- Cardinality: {cardinality_str}\n"
                f"-- Cross-filtering: {rel['cross_filtering']}\n"
                f"-- Active: {rel['is_active']}\n"
                f"ALTER TABLE {target_catalog}.{target_schema}.{from_table}\n"
                f"ADD CONSTRAINT {constraint_name}\n"
                f"FOREIGN KEY ({from_column})\n"
                f"REFERENCES {target_catalog}.{target_schema}.{to_table}({to_column})\n"
                f"NOT ENFORCED;"
            )
            statements.append(sql)

        return statements

    def _format_output(
        self,
        workspace_id: str,
        dataset_id: str,
        relationships: List[Dict[str, Any]],
        fk_statements: List[str],
        target_catalog: str,
        target_schema: str,
    ) -> str:
        """Format the extraction output."""
        output = []

        output.append("# Power BI Relationships Extraction Results\n")
        output.append(f"**Workspace ID**: {workspace_id}")
        output.append(f"**Dataset ID**: {dataset_id}")
        output.append(f"**Target Location**: {target_catalog}.{target_schema}")
        output.append(f"**Relationships Found**: {len(relationships)}\n")

        # List relationships
        output.append("## Relationships\n")
        for rel in relationships:
            from_card = "*" if rel["from_cardinality"] == "Many" else "1"
            to_card = "*" if rel["to_cardinality"] == "Many" else "1"
            active_str = "ACTIVE" if rel["is_active"] else "INACTIVE"

            output.append(f"### {rel['name']}")
            output.append(f"- **From**: {rel['from_table']}.{rel['from_column']} ({from_card})")
            output.append(f"- **To**: {rel['to_table']}.{rel['to_column']} ({to_card})")
            output.append(f"- **Cross-filtering**: {rel['cross_filtering']}")
            output.append(f"- **Status**: {active_str}\n")

        # FK statements
        output.append("## Unity Catalog Foreign Key Statements\n")
        output.append("```sql")
        output.append("\n\n".join(fk_statements))
        output.append("```\n")

        # Summary
        output.append("## Summary\n")
        active_count = sum(1 for r in relationships if r["is_active"])
        inactive_count = len(relationships) - active_count

        output.append(f"- **Total Relationships**: {len(relationships)}")
        output.append(f"- **Active**: {active_count}")
        output.append(f"- **Inactive**: {inactive_count}")

        # Cardinality breakdown
        cardinality_counts = {}
        for rel in relationships:
            key = f"{rel['from_cardinality']} to {rel['to_cardinality']}"
            cardinality_counts[key] = cardinality_counts.get(key, 0) + 1

        output.append("\n**Cardinality Breakdown**:")
        for card, count in cardinality_counts.items():
            output.append(f"- {card}: {count}")

        return "\n".join(output)
