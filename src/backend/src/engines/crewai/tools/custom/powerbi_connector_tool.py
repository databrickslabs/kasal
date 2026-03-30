"""Power BI Connector Tool for CrewAI"""

import logging
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

# Import converters
from src.converters.pipeline import ConversionPipeline, OutboundFormat
from src.converters.base.connectors import ConnectorType

logger = logging.getLogger(__name__)


class PowerBIConnectorToolSchema(BaseModel):
    """Input schema for PowerBIConnectorTool."""

    semantic_model_id: str = Field(
        ...,
        description="Power BI dataset/semantic model ID (required)"
    )
    group_id: str = Field(
        ...,
        description="Power BI workspace ID (required)"
    )
    tenant_id: Optional[str] = Field(
        None,
        description="Azure AD tenant ID (required for service_principal or service_account auth)"
    )
    client_id: Optional[str] = Field(
        None,
        description="Azure AD application (client) ID (required for service_principal or service_account auth)"
    )
    client_secret: Optional[str] = Field(
        None,
        description="Azure AD application client secret (required for service_principal auth)"
    )
    access_token: Optional[str] = Field(
        None,
        description="OAuth access token for Power BI authentication (alternative to service_principal/service_account)"
    )
    auth_method: Optional[str] = Field(
        None,
        description="Authentication method: 'service_principal', 'service_account', or auto-detect. "
                    "service_principal uses client_id+client_secret+tenant_id. "
                    "service_account uses username+password+client_id+tenant_id."
    )
    username: Optional[str] = Field(
        None,
        description="Service account username/UPN (for service_account auth)"
    )
    password: Optional[str] = Field(
        None,
        description="Service account password (for service_account auth)"
    )
    username_env: Optional[str] = Field(
        None,
        description="Environment variable name containing service account username (alternative to direct username)"
    )
    password_env: Optional[str] = Field(
        None,
        description="Environment variable name containing service account password (alternative to direct password)"
    )
    outbound_format: str = Field(
        "dax",
        description="Target output format: 'dax', 'sql', 'uc_metrics', or 'yaml' (default: 'dax')"
    )
    include_hidden: bool = Field(
        False,
        description="Include hidden measures in extraction (default: False)"
    )
    filter_pattern: Optional[str] = Field(
        None,
        description="Regex pattern to filter measure names (optional)"
    )
    sql_dialect: str = Field(
        "databricks",
        description="SQL dialect for SQL output: 'databricks', 'postgresql', 'mysql', 'sqlserver', 'snowflake', 'bigquery', 'standard' (default: 'databricks')"
    )
    uc_catalog: str = Field(
        "main",
        description="Unity Catalog catalog name for UC Metrics output (default: 'main')"
    )
    uc_schema: str = Field(
        "default",
        description="Unity Catalog schema name for UC Metrics output (default: 'default')"
    )
    info_table_name: str = Field(
        "Info Measures",
        description="Name of the Power BI Info Measures table (default: 'Info Measures')"
    )


class PowerBIConnectorTool(BaseTool):
    """
    Extract measures from Power BI datasets and convert to target format.

    This tool connects to Power BI via REST API, extracts measure definitions
    from the Info Measures table, parses DAX expressions, and converts them
    to the target format (DAX, SQL, UC Metrics, or YAML).

    Features:
    - Connects to Power BI semantic models via REST API
    - Extracts measure metadata and DAX expressions
    - Parses DAX formulas to extract aggregations, filters, and source tables
    - Converts to multiple target formats (DAX, SQL, UC Metrics, YAML)
    - Supports multiple authentication methods:
      * OAuth access token (from frontend)
      * Service Principal (client_id + client_secret + tenant_id)
      * Service Account (username + password + client_id + tenant_id)
    - Configurable measure filtering and output formats

    Example usage with access token:
    ```
    result = powerbi_connector_tool._run(
        semantic_model_id="abc123",
        group_id="workspace456",
        access_token="eyJ...",
        outbound_format="sql",
        sql_dialect="databricks"
    )
    ```

    Example usage with service principal:
    ```
    result = powerbi_connector_tool._run(
        semantic_model_id="abc123",
        group_id="workspace456",
        tenant_id="tenant789",
        client_id="client123",
        client_secret="secret456",
        auth_method="service_principal",
        outbound_format="sql"
    )
    ```

    Example usage with service account:
    ```
    result = powerbi_connector_tool._run(
        semantic_model_id="abc123",
        group_id="workspace456",
        tenant_id="tenant789",
        client_id="client123",
        username="user@domain.com",
        password="password",
        auth_method="service_account",
        outbound_format="sql"
    )
    ```

    Example usage with service account (env vars):
    ```
    result = powerbi_connector_tool._run(
        semantic_model_id="abc123",
        group_id="workspace456",
        tenant_id="tenant789",
        client_id="client123",
        username_env="POWERBI_USERNAME",
        password_env="POWERBI_PASSWORD",
        auth_method="service_account",
        outbound_format="sql"
    )
    ```

    Output formats:
    - **dax**: List of DAX measures with names and expressions
    - **sql**: SQL query for the target dialect
    - **uc_metrics**: Unity Catalog Metrics YAML definition
    - **yaml**: YAML KPI definition format
    """

    name: str = "Power BI Connector"
    description: str = (
        "Extract measures from Power BI datasets and convert them to DAX, SQL, UC Metrics, or YAML format. "
        "Connects to Power BI via REST API and supports multiple authentication methods: "
        "(1) OAuth access_token, (2) Service Principal (client_id, client_secret, tenant_id), "
        "(3) Service Account (username, password, client_id, tenant_id). "
        "Required parameters: semantic_model_id, group_id, plus one authentication method. "
        "Optional: outbound_format (dax/sql/uc_metrics/yaml), include_hidden, filter_pattern, sql_dialect, uc_catalog, uc_schema."
    )
    args_schema: Type[BaseModel] = PowerBIConnectorToolSchema

    # Private attributes (not part of schema)
    _pipeline: Any = PrivateAttr()

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the Power BI Connector tool."""
        super().__init__(**kwargs)
        self._pipeline = ConversionPipeline()

    def _run(self, **kwargs: Any) -> str:
        """
        Execute Power BI extraction and conversion.

        Args:
            semantic_model_id: Power BI dataset ID
            group_id: Power BI workspace ID
            tenant_id: Azure AD tenant ID (for service_principal/service_account auth)
            client_id: Azure AD application ID (for service_principal/service_account auth)
            client_secret: Azure AD client secret (for service_principal auth)
            access_token: OAuth access token (alternative to credential-based auth)
            auth_method: Authentication method: 'service_principal', 'service_account', or auto
            username: Service account username (for service_account auth)
            password: Service account password (for service_account auth)
            username_env: Env var name for username (for service_account auth)
            password_env: Env var name for password (for service_account auth)
            outbound_format: Target format (dax/sql/uc_metrics/yaml)
            include_hidden: Include hidden measures
            filter_pattern: Regex to filter measure names
            sql_dialect: SQL dialect (for SQL output)
            uc_catalog: Unity Catalog catalog (for UC Metrics output)
            uc_schema: Unity Catalog schema (for UC Metrics output)
            info_table_name: Name of Info Measures table

        Returns:
            Formatted output in the target format
        """
        try:
            # Extract parameters
            semantic_model_id = kwargs.get("semantic_model_id")
            group_id = kwargs.get("group_id")
            tenant_id = kwargs.get("tenant_id")
            client_id = kwargs.get("client_id")
            client_secret = kwargs.get("client_secret")
            access_token = kwargs.get("access_token")
            auth_method = kwargs.get("auth_method")
            username = kwargs.get("username")
            password = kwargs.get("password")
            username_env = kwargs.get("username_env")
            password_env = kwargs.get("password_env")
            outbound_format = kwargs.get("outbound_format", "dax")
            include_hidden = kwargs.get("include_hidden", False)
            filter_pattern = kwargs.get("filter_pattern")
            sql_dialect = kwargs.get("sql_dialect", "databricks")
            uc_catalog = kwargs.get("uc_catalog", "main")
            uc_schema = kwargs.get("uc_schema", "default")
            info_table_name = kwargs.get("info_table_name", "Info Measures")

            # Validate required parameters
            if not semantic_model_id or not group_id:
                return "Error: Missing required parameters. semantic_model_id and group_id are required."

            # Validate authentication - need either access_token OR service_principal/service_account credentials
            has_access_token = bool(access_token)
            has_service_principal = bool(client_id and client_secret and tenant_id)
            has_service_account = bool(client_id and tenant_id and (
                (username and password) or (username_env and password_env)
            ))

            if not any([has_access_token, has_service_principal, has_service_account]):
                return (
                    "Error: Missing authentication. Provide one of:\n"
                    "1. access_token (OAuth token)\n"
                    "2. Service Principal: client_id, client_secret, tenant_id\n"
                    "3. Service Account: client_id, tenant_id, and (username+password or username_env+password_env)"
                )

            # Map outbound format string to enum
            format_map = {
                "dax": OutboundFormat.DAX,
                "sql": OutboundFormat.SQL,
                "uc_metrics": OutboundFormat.UC_METRICS,
                "yaml": OutboundFormat.YAML
            }
            outbound_format_enum = format_map.get(outbound_format.lower())
            if not outbound_format_enum:
                return f"Error: Invalid outbound_format '{outbound_format}'. Must be one of: dax, sql, uc_metrics, yaml"

            # Determine auth method for logging
            auth_type = "access_token" if has_access_token else (
                "service_principal" if has_service_principal else "service_account"
            )
            logger.info(
                f"Executing Power BI conversion: dataset={semantic_model_id}, "
                f"workspace={group_id}, format={outbound_format}, auth={auth_type}"
            )

            # Build inbound parameters with all authentication options
            inbound_params = {
                "semantic_model_id": semantic_model_id,
                "group_id": group_id,
                "tenant_id": tenant_id,
                "client_id": client_id,
                "client_secret": client_secret,
                "access_token": access_token,
                "auth_method": auth_method,
                "username": username,
                "password": password,
                "username_env": username_env,
                "password_env": password_env,
                "info_table_name": info_table_name
            }

            # Build extract parameters
            extract_params = {
                "include_hidden": include_hidden
            }
            if filter_pattern:
                extract_params["filter_pattern"] = filter_pattern

            # Build outbound parameters
            outbound_params = {}
            if outbound_format_enum == OutboundFormat.SQL:
                outbound_params["dialect"] = sql_dialect
            elif outbound_format_enum == OutboundFormat.UC_METRICS:
                outbound_params["catalog"] = uc_catalog
                outbound_params["schema"] = uc_schema

            # Execute pipeline
            result = self._pipeline.execute(
                inbound_type=ConnectorType.POWERBI,
                inbound_params=inbound_params,
                outbound_format=outbound_format_enum,
                outbound_params=outbound_params,
                extract_params=extract_params,
                definition_name=f"powerbi_{semantic_model_id}"
            )

            if not result["success"]:
                error_msgs = ", ".join(result["errors"])
                return f"Error: Conversion failed - {error_msgs}"

            # Format output based on target format
            output = result["output"]
            measure_count = result["measure_count"]

            if outbound_format_enum == OutboundFormat.DAX:
                # Format DAX measures
                formatted = f"# Power BI Measures Converted to DAX\n\n"
                formatted += f"Extracted {measure_count} measures from Power BI dataset '{semantic_model_id}'\n\n"
                for measure in output:
                    formatted += f"## {measure['name']}\n\n"
                    formatted += f"```dax\n{measure['name']} = \n{measure['expression']}\n```\n\n"
                    if measure.get('description'):
                        formatted += f"*Description: {measure['description']}*\n\n"
                return formatted

            elif outbound_format_enum == OutboundFormat.SQL:
                # Format SQL query
                formatted = f"# Power BI Measures Converted to SQL\n\n"
                formatted += f"Extracted {measure_count} measures from Power BI dataset '{semantic_model_id}'\n\n"
                formatted += f"```sql\n{output}\n```\n"
                return formatted

            elif outbound_format_enum == OutboundFormat.UC_METRICS:
                # Format UC Metrics YAML
                formatted = f"# Power BI Measures Converted to UC Metrics\n\n"
                formatted += f"Extracted {measure_count} measures from Power BI dataset '{semantic_model_id}'\n\n"
                formatted += f"```yaml\n{output}\n```\n"
                return formatted

            elif outbound_format_enum == OutboundFormat.YAML:
                # Format YAML output
                formatted = f"# Power BI Measures Exported as YAML\n\n"
                formatted += f"Extracted {measure_count} measures from Power BI dataset '{semantic_model_id}'\n\n"
                formatted += f"```yaml\n{output}\n```\n"
                return formatted

            return str(output)

        except Exception as e:
            logger.error(f"Power BI Connector error: {str(e)}", exc_info=True)
            return f"Error: {str(e)}"
