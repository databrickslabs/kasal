"""
Measure Conversion Pipeline Tool for CrewAI
Universal converter: Any source (Power BI, Tableau, YAML) → Any target (DAX, SQL, UC Metrics)
"""

import logging
from typing import Any, Optional, Type, Dict, Literal

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

# Import converters
from src.converters.pipeline import ConversionPipeline, OutboundFormat
from src.converters.base.connectors import ConnectorType

logger = logging.getLogger(__name__)


class MeasureConversionPipelineSchema(BaseModel):
    """Input schema for MeasureConversionPipelineTool."""

    # ===== INBOUND CONNECTOR SELECTION =====
    inbound_connector: Optional[Literal["powerbi", "yaml"]] = Field(
        None,
        description="[OPTIONAL - Pre-configured] Source connector type: 'powerbi' (Power BI dataset), 'yaml' (YAML file). Leave empty to use pre-configured value."
    )

    # ===== INBOUND: POWER BI CONFIGURATION =====
    powerbi_semantic_model_id: Optional[str] = Field(
        None,
        description="[Power BI] Dataset/semantic model ID (required if inbound_connector='powerbi')"
    )
    powerbi_group_id: Optional[str] = Field(
        None,
        description="[Power BI] Workspace ID (required if inbound_connector='powerbi')"
    )

    # Service Principal authentication (required for Power BI)
    powerbi_tenant_id: Optional[str] = Field(
        None,
        description="[Power BI Auth] Azure AD tenant ID (required for Service Principal auth)"
    )
    powerbi_client_id: Optional[str] = Field(
        None,
        description="[Power BI Auth] Application/Client ID (required for Service Principal auth)"
    )
    powerbi_client_secret: Optional[str] = Field(
        None,
        description="[Power BI Auth] Client secret (required for Service Principal auth)"
    )

    # Other Power BI settings
    powerbi_info_table_name: str = Field(
        "Info Measures",
        description="[Power BI] Name of the Info Measures table (default: 'Info Measures')"
    )
    powerbi_include_hidden: bool = Field(
        False,
        description="[Power BI] Include hidden measures in extraction (default: False)"
    )
    powerbi_filter_pattern: Optional[str] = Field(
        None,
        description="[Power BI] Regex pattern to filter measure names (optional)"
    )

    # ===== INBOUND: YAML CONFIGURATION =====
    yaml_content: Optional[str] = Field(
        None,
        description="[YAML] YAML content as string (required if inbound_connector='yaml')"
    )
    yaml_file_path: Optional[str] = Field(
        None,
        description="[YAML] Path to YAML file (alternative to yaml_content)"
    )

    # ===== OUTBOUND FORMAT SELECTION =====
    outbound_format: Optional[Literal["dax", "sql", "uc_metrics", "yaml"]] = Field(
        None,
        description="[OPTIONAL - Pre-configured] Target output format: 'dax' (Power BI), 'sql' (SQL dialects), 'uc_metrics' (Databricks UC Metrics), 'yaml' (YAML definition). Leave empty to use pre-configured value."
    )

    # ===== OUTBOUND: SQL CONFIGURATION =====
    sql_dialect: str = Field(
        "databricks",
        description="[SQL] SQL dialect: 'databricks', 'postgresql', 'mysql', 'sqlserver', 'snowflake', 'bigquery', 'standard' (default: 'databricks')"
    )
    sql_include_comments: bool = Field(
        True,
        description="[SQL] Include descriptive comments in SQL output (default: True)"
    )
    sql_process_structures: bool = Field(
        True,
        description="[SQL] Process time intelligence structures (default: True)"
    )

    # ===== OUTBOUND: UC METRICS CONFIGURATION =====
    uc_catalog: str = Field(
        "main",
        description="[UC Metrics] Unity Catalog catalog name (default: 'main')"
    )
    uc_schema: str = Field(
        "default",
        description="[UC Metrics] Unity Catalog schema name (default: 'default')"
    )
    uc_process_structures: bool = Field(
        True,
        description="[UC Metrics] Process time intelligence structures (default: True)"
    )

    # ===== OUTBOUND: DAX CONFIGURATION =====
    dax_process_structures: bool = Field(
        True,
        description="[DAX] Process time intelligence structures (default: True)"
    )

    # ===== GENERAL CONFIGURATION =====
    definition_name: Optional[str] = Field(
        None,
        description="Name for the generated KPI definition (default: auto-generated)"
    )


class MeasureConversionPipelineTool(BaseTool):
    """
    Universal Measure Conversion Pipeline.

    Convert measures between different BI platforms and formats:

    **Inbound Connectors** (Sources):
    - **Power BI**: Extract measures from Power BI datasets via REST API
    - **YAML**: Load measures from YAML definition files
    - Coming Soon: Tableau, Excel, Looker

    **Outbound Formats** (Targets):
    - **DAX**: Power BI / Analysis Services measures
    - **SQL**: Multiple SQL dialects (Databricks, PostgreSQL, MySQL, etc.)
    - **UC Metrics**: Databricks Unity Catalog Metrics Store
    - **YAML**: Portable YAML definition format

    **Example Workflows**:
    1. Power BI → SQL: Migrate Power BI measures to Databricks
    2. YAML → DAX: Generate Power BI measures from YAML specs
    3. Power BI → UC Metrics: Export to Databricks Metrics Store
    4. YAML → SQL: Create SQL views from business logic definitions

    **Configuration**:
    - Select inbound_connector ('powerbi' or 'yaml')
    - Configure source-specific parameters
    - Select outbound_format ('dax', 'sql', 'uc_metrics')
    - Configure target-specific parameters
    - Execute conversion
    """

    name: str = "Measure Conversion Pipeline"
    description: str = (
        "Universal measure conversion pipeline - PRE-CONFIGURED and ready to use. "
        "This tool has been configured with source and target formats. "
        "Simply call this tool WITHOUT any parameters to execute the conversion. "
        "The tool will convert measures from the configured source format to the configured target format. "
        "Returns formatted output in the target format (DAX, SQL, UC Metrics, or YAML)."
    )
    args_schema: Type[BaseModel] = MeasureConversionPipelineSchema

    # Private attributes (not part of schema)
    _pipeline: Any = PrivateAttr()
    _instance_id: str = PrivateAttr()
    _default_config: Dict[str, Any] = PrivateAttr()

    # Allow extra attributes for pipeline and config
    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the Measure Conversion Pipeline tool."""
        # ===== DEBUG: Generate unique instance ID =====
        import uuid
        instance_id = str(uuid.uuid4())[:8]

        # ===== DEBUG: Log what kwargs we received =====
        logger.info(f"[MeasureConversionPipelineTool.__init__] Instance ID: {instance_id}")
        logger.info(f"[MeasureConversionPipelineTool.__init__] Received kwargs keys: {list(kwargs.keys())}")
        logger.info(f"[MeasureConversionPipelineTool.__init__] inbound_connector: {kwargs.get('inbound_connector', 'NOT PROVIDED')}")
        logger.info(f"[MeasureConversionPipelineTool.__init__] powerbi_semantic_model_id: {kwargs.get('powerbi_semantic_model_id', 'NOT PROVIDED')}")
        logger.info(f"[MeasureConversionPipelineTool.__init__] outbound_format: {kwargs.get('outbound_format', 'NOT PROVIDED')}")

        # Extract execution_inputs if provided (for dynamic parameter resolution)
        execution_inputs = kwargs.get("execution_inputs", {})

        # Store config values temporarily
        default_config = {
            # Inbound connector
            "inbound_connector": kwargs.get("inbound_connector"),
            # Power BI params
            "powerbi_semantic_model_id": kwargs.get("powerbi_semantic_model_id"),
            "powerbi_group_id": kwargs.get("powerbi_group_id"),
            # Power BI Service Principal authentication
            "powerbi_tenant_id": kwargs.get("powerbi_tenant_id"),
            "powerbi_client_id": kwargs.get("powerbi_client_id"),
            "powerbi_client_secret": kwargs.get("powerbi_client_secret"),
            # Power BI other settings
            "powerbi_info_table_name": kwargs.get("powerbi_info_table_name", "Info Measures"),
            "powerbi_include_hidden": kwargs.get("powerbi_include_hidden", False),
            "powerbi_filter_pattern": kwargs.get("powerbi_filter_pattern"),
            # YAML params
            "yaml_content": kwargs.get("yaml_content"),
            "yaml_file_path": kwargs.get("yaml_file_path"),
            # Outbound format
            "outbound_format": kwargs.get("outbound_format"),
            # SQL params
            "sql_dialect": kwargs.get("sql_dialect", "databricks"),
            "sql_include_comments": kwargs.get("sql_include_comments", True),
            "sql_process_structures": kwargs.get("sql_process_structures", True),
            # UC Metrics params
            "uc_catalog": kwargs.get("uc_catalog", "main"),
            "uc_schema": kwargs.get("uc_schema", "default"),
            "uc_process_structures": kwargs.get("uc_process_structures", True),
            # DAX params
            "dax_process_structures": kwargs.get("dax_process_structures", True),
            # General
            "definition_name": kwargs.get("definition_name"),
        }

        # DYNAMIC PARAMETER RESOLUTION: If execution_inputs provided, resolve placeholders during init
        if execution_inputs:
            logger.info(f"[MeasureConversionPipelineTool.__init__] Instance {instance_id} - Resolving parameters from execution_inputs: {list(execution_inputs.keys())}")
            resolved_config = {}
            for key, value in default_config.items():
                if isinstance(value, str) and '{' in value:
                    # Resolve placeholder
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

        # Call parent __init__ with filtered kwargs (remove our custom config params)
        # BaseTool expects certain parameters like 'result_as_answer'
        tool_kwargs = {k: v for k, v in kwargs.items() if k not in default_config}
        super().__init__(**tool_kwargs)

        # Set private attributes AFTER super().__init__()
        self._instance_id = instance_id
        self._default_config = default_config
        self._pipeline = ConversionPipeline()

        logger.info(f"[MeasureConversionPipelineTool.__init__] Instance {instance_id} initialized with config: {default_config}")

    def _resolve_parameter(self, value: Any, execution_inputs: Dict[str, Any]) -> Any:
        """
        Resolve parameter placeholders in configuration values.

        Supports dynamic parameter injection for external app integration:
        - Static value: "powerbi" → returns "powerbi"
        - Placeholder: "{dataset_id}" → resolves from execution_inputs["dataset_id"]
        - Nested: "{source}_config" → resolves {source} from inputs

        Args:
            value: Configuration value (may contain {placeholders})
            execution_inputs: Dictionary of execution input values

        Returns:
            Resolved value with placeholders replaced
        """
        if not isinstance(value, str):
            return value

        # Check if value contains placeholders in format {param_name}
        import re
        placeholders = re.findall(r'\{(\w+)\}', value)

        if not placeholders:
            return value  # No placeholders, return as-is

        # Resolve each placeholder
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
        Execute measure conversion pipeline.

        Args:
            inbound_connector: Source type ('powerbi', 'yaml')
            outbound_format: Target format ('dax', 'sql', 'uc_metrics', 'yaml')
            execution_inputs: Dictionary of dynamic parameter values (for external app integration)
            [source-specific parameters]
            [target-specific parameters]

        Returns:
            Formatted output in the target format
        """
        try:
            instance_id = getattr(self, '_instance_id', 'UNKNOWN')
            logger.info(f"[TOOL CALL] Instance {instance_id} - _run() called")
            logger.info(f"[TOOL CALL] Instance {instance_id} - Received kwargs: {list(kwargs.keys())}")

            # Extract execution_inputs if provided (for dynamic parameter resolution)
            execution_inputs = kwargs.pop('execution_inputs', {})
            logger.info(f"[TOOL CALL] Instance {instance_id} - Execution inputs: {list(execution_inputs.keys())}")

            logger.info(f"[TOOL CALL] Instance {instance_id} - _default_config inbound_connector: {self._default_config.get('inbound_connector', 'NOT SET')}")
            logger.info(f"[TOOL CALL] Instance {instance_id} - _default_config powerbi_semantic_model_id: {self._default_config.get('powerbi_semantic_model_id', 'NOT SET')}")
            logger.info(f"[TOOL CALL] Instance {instance_id} - _default_config outbound_format: {self._default_config.get('outbound_format', 'NOT SET')}")

            # Merge agent-provided kwargs with pre-configured defaults
            # Filter out None values AND placeholder-like strings that agents often generate
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
            logger.info(f"[TOOL CALL] Instance {instance_id} - Filtered kwargs (removed None/placeholders): {list(filtered_kwargs.keys())}")

            # Pre-configured values take precedence over agent-provided placeholders
            merged_kwargs = {**self._default_config, **filtered_kwargs}

            # DYNAMIC PARAMETER RESOLUTION: Resolve placeholders from execution inputs
            if execution_inputs:
                logger.info(f"[PARAM RESOLUTION] Resolving parameters with execution_inputs")
                resolved_kwargs = {}
                for key, value in merged_kwargs.items():
                    resolved_kwargs[key] = self._resolve_parameter(value, execution_inputs)
                merged_kwargs = resolved_kwargs
            else:
                logger.info(f"[PARAM RESOLUTION] No execution_inputs provided, using static configuration")

            # Extract common parameters from merged config
            inbound_connector = merged_kwargs.get("inbound_connector", "powerbi")
            outbound_format = merged_kwargs.get("outbound_format", "dax")
            definition_name = merged_kwargs.get("definition_name")

            logger.info(f"[TOOL CALL] Instance {instance_id} - Executing conversion: inbound={inbound_connector}, outbound={outbound_format}")

            # Validate inbound connector
            if inbound_connector not in ["powerbi", "yaml"]:
                return f"Error: Unsupported inbound_connector '{inbound_connector}'. Must be: powerbi, yaml"

            # Validate outbound format
            if outbound_format not in ["dax", "sql", "uc_metrics", "yaml"]:
                return f"Error: Unsupported outbound_format '{outbound_format}'. Must be: dax, sql, uc_metrics, yaml"

            logger.info(f"Executing conversion: {inbound_connector} → {outbound_format}")

            # ===== INBOUND: Build connector parameters =====
            inbound_params = {}
            extract_params = {}

            if inbound_connector == "powerbi":
                # Power BI configuration
                semantic_model_id = merged_kwargs.get("powerbi_semantic_model_id")
                group_id = merged_kwargs.get("powerbi_group_id")

                # Service Principal authentication (required)
                tenant_id = merged_kwargs.get("powerbi_tenant_id")
                client_id = merged_kwargs.get("powerbi_client_id")
                client_secret = merged_kwargs.get("powerbi_client_secret")

                # Validate required parameters
                if not semantic_model_id or not group_id:
                    return "Error: Power BI requires powerbi_semantic_model_id and powerbi_group_id"

                # Check Service Principal authentication
                if not all([tenant_id, client_id, client_secret]):
                    return ("Error: Power BI requires Service Principal authentication.\n"
                            "Provide: powerbi_tenant_id + powerbi_client_id + powerbi_client_secret")

                # DEBUG: Log the actual credential values being used
                logger.info(f"[TOOL DEBUG] PowerBI credentials being used:")
                logger.info(f"[TOOL DEBUG]   tenant_id: '{tenant_id}'")
                logger.info(f"[TOOL DEBUG]   client_id: '{client_id}'")
                logger.info(f"[TOOL DEBUG]   client_secret length: {len(client_secret) if client_secret else 0}")
                logger.info(f"[TOOL DEBUG]   semantic_model_id: '{semantic_model_id}'")
                logger.info(f"[TOOL DEBUG]   group_id: '{group_id}'")

                inbound_params = {
                    "semantic_model_id": semantic_model_id,
                    "group_id": group_id,
                    "tenant_id": tenant_id,
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "info_table_name": merged_kwargs.get("powerbi_info_table_name", "Info Measures")
                }

                extract_params = {
                    "include_hidden": merged_kwargs.get("powerbi_include_hidden", False)
                }
                if merged_kwargs.get("powerbi_filter_pattern"):
                    extract_params["filter_pattern"] = merged_kwargs["powerbi_filter_pattern"]

                connector_type = ConnectorType.POWERBI
                if not definition_name:
                    definition_name = f"powerbi_{semantic_model_id}"

            elif inbound_connector == "yaml":
                # YAML configuration
                yaml_content = merged_kwargs.get("yaml_content")
                yaml_file_path = merged_kwargs.get("yaml_file_path")

                if not yaml_content and not yaml_file_path:
                    return "Error: YAML requires either yaml_content or yaml_file_path"

                # For YAML, we'll need to handle it differently
                # since it doesn't use the connector pattern
                return self._handle_yaml_conversion(
                    yaml_content=yaml_content,
                    yaml_file_path=yaml_file_path,
                    outbound_format=outbound_format,
                    definition_name=definition_name,
                    kwargs=merged_kwargs
                )

            # ===== OUTBOUND: Build format parameters =====
            outbound_params = {}
            format_map = {
                "dax": OutboundFormat.DAX,
                "sql": OutboundFormat.SQL,
                "uc_metrics": OutboundFormat.UC_METRICS,
                "yaml": OutboundFormat.YAML
            }
            outbound_format_enum = format_map[outbound_format]

            if outbound_format == "sql":
                outbound_params = {
                    "dialect": merged_kwargs.get("sql_dialect", "databricks"),
                    "include_comments": merged_kwargs.get("sql_include_comments", True),
                    "process_structures": merged_kwargs.get("sql_process_structures", True)
                }
            elif outbound_format == "uc_metrics":
                outbound_params = {
                    "catalog": merged_kwargs.get("uc_catalog", "main"),
                    "schema": merged_kwargs.get("uc_schema", "default"),
                    "process_structures": merged_kwargs.get("uc_process_structures", True)
                }
            elif outbound_format == "dax":
                outbound_params = {
                    "process_structures": merged_kwargs.get("dax_process_structures", True)
                }

            # ===== EXECUTE PIPELINE =====
            result = self._pipeline.execute(
                inbound_type=connector_type,
                inbound_params=inbound_params,
                outbound_format=outbound_format_enum,
                outbound_params=outbound_params,
                extract_params=extract_params,
                definition_name=definition_name
            )

            if not result["success"]:
                error_msgs = ", ".join(result["errors"])
                return f"Error: Conversion failed - {error_msgs}"

            # ===== FORMAT OUTPUT =====
            return self._format_output(
                output=result["output"],
                inbound_connector=inbound_connector,
                outbound_format=outbound_format,
                measure_count=result["measure_count"],
                source_id=inbound_params.get("semantic_model_id", "source")
            )

        except Exception as e:
            logger.error(f"Measure Conversion Pipeline error: {str(e)}", exc_info=True)
            return f"Error: {str(e)}"

    def _handle_yaml_conversion(
        self,
        yaml_content: Optional[str],
        yaml_file_path: Optional[str],
        outbound_format: str,
        definition_name: Optional[str],
        kwargs: Dict[str, Any]
    ) -> str:
        """Handle YAML to outbound format conversion."""
        print(f"[YAML DEBUG] _handle_yaml_conversion called: outbound_format={outbound_format}")
        print(f"[YAML DEBUG] yaml_content length={len(yaml_content) if yaml_content else 0}, yaml_file_path={yaml_file_path}")

        try:
            from src.converters.common.transformers.yaml import YAMLKPIParser
            from src.converters.services.powerbi import DAXGenerator
            from src.converters.services.sql import SQLGenerator
            from src.converters.services.sql import SQLDialect
            from src.converters.services.uc_metrics import UCMetricsGenerator

            print(f"[YAML DEBUG] Imports successful, creating parser")

            # Parse YAML
            parser = YAMLKPIParser()
            if yaml_file_path:
                print(f"[YAML DEBUG] Parsing YAML from file: {yaml_file_path}")
                definition = parser.parse_file(yaml_file_path)
            else:
                print(f"[YAML DEBUG] Parsing YAML from content")
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                    f.write(yaml_content)
                    temp_path = f.name
                print(f"[YAML DEBUG] Wrote YAML to temp file: {temp_path}")
                definition = parser.parse_file(temp_path)
                import os
                os.unlink(temp_path)

            measure_count = len(definition.kpis)
            print(f"[YAML DEBUG] Parsed {measure_count} KPIs from YAML")
            print(f"[YAML DEBUG] Converting to format: {outbound_format}")

            # Convert to target format
            if outbound_format == "dax":
                generator = DAXGenerator()
                measures = []
                for kpi in definition.kpis:
                    dax_measure = generator.generate_dax_measure(definition, kpi)
                    measures.append({
                        "name": dax_measure.name,
                        "expression": dax_measure.dax_formula,
                        "description": dax_measure.description
                    })
                output = measures

            elif outbound_format == "sql":
                # Use the same pattern as DAX - process each KPI individually
                from src.converters.services.sql import SQLTranslationOptions

                dialect = kwargs.get("sql_dialect", "databricks")
                sql_dialect = SQLDialect[dialect.upper()]
                generator = SQLGenerator(dialect=sql_dialect)

                print(f"[SQL DEBUG] Starting SQL generation for {len(definition.kpis)} KPIs with dialect {dialect}")
                logger.info(f"Starting SQL generation for {len(definition.kpis)} KPIs with dialect {dialect}")

                try:
                    # Create options with separate_measures=True to generate individual queries
                    options = SQLTranslationOptions(target_dialect=sql_dialect, separate_measures=True)

                    result = generator.generate_sql_from_kbi_definition(definition, options)
                    print(f"[SQL DEBUG] SQL generation completed: measures_count={result.measures_count}, queries_count={result.queries_count}")
                    print(f"[SQL DEBUG] Result has sql_queries={hasattr(result, 'sql_queries')}, type={type(result.sql_queries) if hasattr(result, 'sql_queries') else 'N/A'}")

                    logger.info(f"SQL generation completed: measures_count={result.measures_count if hasattr(result, 'measures_count') else 'N/A'}, queries_count={result.queries_count if hasattr(result, 'queries_count') else 'N/A'}")

                    # Return all SQL queries
                    output = result.sql_queries if result.sql_queries else []

                    print(f"[SQL DEBUG] output is list={isinstance(output, list)}, length={len(output) if output else 0}")
                    if output:
                        print(f"[SQL DEBUG] First query type={type(output[0])}")
                        print(f"[SQL DEBUG] First query has to_sql method={hasattr(output[0], 'to_sql')}")

                    if not output:
                        print(f"[SQL DEBUG] NO OUTPUT! Result object: measures_count={result.measures_count}, queries_count={result.queries_count}")
                        print(f"[SQL DEBUG] sql_queries value: {result.sql_queries}")
                        print(f"[SQL DEBUG] sql_measures count: {len(result.sql_measures) if hasattr(result, 'sql_measures') else 'N/A'}")
                        logger.warning(f"SQL generator returned no queries! Result: {result}")
                        logger.warning(f"Definition had {len(definition.kpis)} KPIs: {[kpi.technical_name for kpi in definition.kpis]}")

                    logger.info(f"SQL conversion: result type={type(result)}, sql_queries type={type(result.sql_queries) if hasattr(result, 'sql_queries') else 'N/A'}, count={len(output) if output else 0}")
                except Exception as e:
                    print(f"[SQL DEBUG] EXCEPTION during SQL generation: {e}")
                    logger.error(f"Exception during SQL generation: {e}", exc_info=True)
                    import traceback
                    traceback.print_exc()
                    output = []

            elif outbound_format == "uc_metrics":
                generator = UCMetricsGenerator()
                metadata = {
                    "name": definition_name or "yaml_measures",
                    "catalog": kwargs.get("uc_catalog", "main"),
                    "schema": kwargs.get("uc_schema", "default")
                }
                uc_metrics = generator.generate_consolidated_uc_metrics(definition.kpis, metadata)
                output = generator.format_consolidated_uc_metrics_yaml(uc_metrics)

            elif outbound_format == "yaml":
                output = parser.export_to_yaml(definition)

            return self._format_output(
                output=output,
                inbound_connector="yaml",
                outbound_format=outbound_format,
                measure_count=measure_count,
                source_id="yaml_file"
            )

        except Exception as e:
            logger.error(f"YAML conversion error: {str(e)}", exc_info=True)
            return f"Error: YAML conversion failed - {str(e)}"

    def _format_output(
        self,
        output: Any,
        inbound_connector: str,
        outbound_format: str,
        measure_count: int,
        source_id: str
    ) -> str:
        """Format output based on target format."""
        source_name = {
            "powerbi": "Power BI",
            "yaml": "YAML",
            "tableau": "Tableau",
            "excel": "Excel"
        }.get(inbound_connector, inbound_connector)

        format_name = {
            "dax": "DAX",
            "sql": "SQL",
            "uc_metrics": "UC Metrics",
            "yaml": "YAML"
        }.get(outbound_format, outbound_format)

        header = f"# {source_name} Measures → {format_name}\n\n"
        header += f"Converted {measure_count} measures from {source_name} source '{source_id}'\n\n"

        if outbound_format == "dax":
            formatted = header
            for measure in output:
                formatted += f"## {measure['name']}\n\n"
                formatted += f"```dax\n{measure['name']} = \n{measure['expression']}\n```\n\n"
                if measure.get('description'):
                    formatted += f"*{measure['description']}*\n\n"
            return formatted

        elif outbound_format == "sql":
            # FIXED: Format each SQL measure separately like DAX
            logger.info(f"Formatting SQL output: output type={type(output)}, is list={isinstance(output, list)}, length={len(output) if hasattr(output, '__len__') else 'N/A'}")
            formatted = header

            if not output:
                logger.warning("SQL output is empty!")
                return formatted + "\n*No SQL queries generated*\n"

            # Ensure output is a list (pipeline might return a single string)
            if isinstance(output, str):
                output = [output]

            for i, sql_query in enumerate(output):
                logger.info(f"Processing SQL query {i+1}: type={type(sql_query)}, has original_kbi={hasattr(sql_query, 'original_kbi')}")

                # Handle string, dict, and SQLQuery object types
                if isinstance(sql_query, str):
                    # SQL query is already a string (from pipeline)
                    measure_name = f'SQL Measure {i+1}'
                    sql_text = sql_query
                elif isinstance(sql_query, dict) or (hasattr(sql_query, 'get') and not hasattr(sql_query, 'to_sql')):
                    # Dictionary with transpiled SQL (from PowerBI → SQL transpilation path)
                    # Check for dict type OR dict-like behavior (has 'get' but not 'to_sql')
                    measure_name = sql_query.get('name', f'SQL Measure {i+1}')
                    sql_text = sql_query.get('sql', '')
                    # Add transpilability info as a comment if available
                    if not sql_query.get('is_transpilable', True):
                        sql_text = f"-- WARNING: Not fully transpilable\n{sql_text}"
                elif hasattr(sql_query, 'to_sql'):
                    # SQLQuery object with metadata (from direct generator)
                    # Get measure name from the original KBI
                    if hasattr(sql_query, 'original_kbi') and sql_query.original_kbi:
                        measure_name = sql_query.original_kbi.technical_name or sql_query.original_kbi.description or 'SQL Measure'
                    else:
                        measure_name = 'SQL Measure'

                    try:
                        sql_text = sql_query.to_sql()
                    except Exception as e:
                        logger.error(f"Error calling to_sql() on query {i+1}: {e}")
                        sql_text = f"Error formatting SQL: {e}"
                else:
                    # Fallback: try to convert to string
                    logger.warning(f"Unknown SQL query type {type(sql_query)} for query {i+1}, converting to string")
                    measure_name = f'SQL Measure {i+1}'
                    sql_text = str(sql_query)

                formatted += f"## {measure_name}\n\n"
                formatted += f"```sql\n{sql_text}\n```\n\n"
                logger.info(f"Successfully formatted SQL query {i+1}, length={len(sql_text)}")

                # Add description if available (only for SQLQuery objects)
                if hasattr(sql_query, 'description') and sql_query.description:
                    formatted += f"*{sql_query.description}*\n\n"

            logger.info(f"Final formatted SQL output length: {len(formatted)}")
            return formatted

        elif outbound_format in ["uc_metrics", "yaml"]:
            return header + f"```yaml\n{output}\n```"

        return str(output)
