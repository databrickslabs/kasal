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
    access_token: str = Field(
        ...,
        description="OAuth access token for Power BI authentication (required)"
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
    - Supports OAuth access token authentication
    - Configurable measure filtering and output formats

    Example usage:
    ```
    result = powerbi_connector_tool._run(
        semantic_model_id="abc123",
        group_id="workspace456",
        access_token="eyJ...",
        outbound_format="sql",
        sql_dialect="databricks",
        include_hidden=False
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
        "Connects to Power BI via REST API using an OAuth access token, queries the Info Measures table, "
        "parses DAX expressions, and converts to the target format. "
        "Required parameters: semantic_model_id, group_id, access_token. "
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
            access_token: OAuth access token
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
            access_token = kwargs.get("access_token")
            outbound_format = kwargs.get("outbound_format", "dax")
            include_hidden = kwargs.get("include_hidden", False)
            filter_pattern = kwargs.get("filter_pattern")
            sql_dialect = kwargs.get("sql_dialect", "databricks")
            uc_catalog = kwargs.get("uc_catalog", "main")
            uc_schema = kwargs.get("uc_schema", "default")
            info_table_name = kwargs.get("info_table_name", "Info Measures")

            # Validate required parameters
            if not all([semantic_model_id, group_id, access_token]):
                return "Error: Missing required parameters. semantic_model_id, group_id, and access_token are required."

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

            logger.info(
                f"Executing Power BI conversion: dataset={semantic_model_id}, "
                f"workspace={group_id}, format={outbound_format}"
            )

            # Build inbound parameters
            inbound_params = {
                "semantic_model_id": semantic_model_id,
                "group_id": group_id,
                "access_token": access_token,
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
