"""
Conversion Pipeline Orchestrator
Connects inbound connectors to outbound converters for end-to-end conversion
"""

from typing import Dict, Any, List, Optional, Union
from enum import Enum
import logging

from .base.models import KPIDefinition
from .base.connectors import BaseInboundConnector, ConnectorType
from .services.powerbi import PowerBIConnector
from .services.powerbi.helpers.dax_smart import SmartDAXGenerator
from .services.sql import SQLGenerator, SQLDialect
from .services.uc_metrics import UCMetricsGenerator
from .services.uc_metrics.helpers.uc_metrics_smart import SmartUCMetricsGenerator


class OutboundFormat(str, Enum):
    """Supported outbound conversion formats"""
    DAX = "dax"
    SQL = "sql"
    UC_METRICS = "uc_metrics"
    YAML = "yaml"


class ConversionPipeline:
    """
    End-to-end conversion pipeline from source system to target format.

    Flow:
    1. Create inbound connector (Power BI, etc.)
    2. Extract measures → KPIDefinition
    3. Select outbound converter (DAX, SQL, UC Metrics)
    4. Generate output in target format

    Example:
        pipeline = ConversionPipeline()

        # Step 1: Connect to Power BI
        result = pipeline.execute(
            inbound_type=ConnectorType.POWERBI,
            inbound_params={
                "semantic_model_id": "abc123",
                "group_id": "workspace456",
                "access_token": "eyJ...",
            },
            outbound_format=OutboundFormat.DAX,
            outbound_params={}
        )

        print(result["output"])  # DAX measures
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def create_inbound_connector(
        self,
        connector_type: ConnectorType,
        connection_params: Dict[str, Any]
    ) -> BaseInboundConnector:
        """
        Factory method to create inbound connector.

        Args:
            connector_type: Type of connector (POWERBI, TABLEAU, etc.)
            connection_params: Connector-specific parameters

        Returns:
            Initialized inbound connector

        Raises:
            ValueError: If connector type not supported
        """
        if connector_type == ConnectorType.POWERBI:
            return PowerBIConnector(**connection_params)
        # Add more connector types here as implemented
        # elif connector_type == ConnectorType.TABLEAU:
        #     return TableauConnector(**connection_params)
        else:
            raise ValueError(f"Unsupported connector type: {connector_type}")

    def execute(
        self,
        inbound_type: ConnectorType,
        inbound_params: Dict[str, Any],
        outbound_format: OutboundFormat,
        outbound_params: Optional[Dict[str, Any]] = None,
        extract_params: Optional[Dict[str, Any]] = None,
        definition_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute full conversion pipeline.

        Args:
            inbound_type: Type of inbound connector
            inbound_params: Parameters for inbound connector
            outbound_format: Target output format
            outbound_params: Parameters for outbound converter
            extract_params: Parameters for extraction (include_hidden, filter_pattern, etc.)
            definition_name: Name for the KPI definition

        Returns:
            {
                "success": bool,
                "definition": KPIDefinition,
                "output": str | dict,  # Generated output
                "metadata": dict,
                "errors": List[str]
            }
        """
        outbound_params = outbound_params or {}
        extract_params = extract_params or {}
        definition_name = definition_name or "converted_measures"

        errors = []
        definition = None
        output = None

        try:
            # Step 1: Create and connect inbound connector
            self.logger.info(f"Creating {inbound_type} connector")
            connector = self.create_inbound_connector(inbound_type, inbound_params)

            with connector:
                # Step 2: Extract measures to KPIDefinition
                self.logger.info("Extracting measures")
                definition = connector.extract_to_definition(
                    definition_name=definition_name,
                    **extract_params
                )

                self.logger.info(f"Extracted {len(definition.kpis)} measures")

                # Step 3: Convert to target format
                self.logger.info(f"Converting to {outbound_format}")
                output = self._convert_to_format(
                    definition,
                    outbound_format,
                    outbound_params,
                    inbound_type=inbound_type  # Pass inbound type for path detection
                )

            metadata = connector.get_metadata()

            return {
                "success": True,
                "definition": definition,
                "output": output,
                "metadata": metadata.__dict__,
                "errors": errors,
                "measure_count": len(definition.kpis)
            }

        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
            errors.append(str(e))
            return {
                "success": False,
                "definition": definition,
                "output": output,
                "metadata": {},
                "errors": errors,
                "measure_count": len(definition.kpis) if definition else 0
            }

    def _convert_to_format(
        self,
        definition: KPIDefinition,
        format: OutboundFormat,
        params: Dict[str, Any],
        inbound_type: ConnectorType = None
    ) -> Union[str, Dict[str, Any], List[Any]]:
        """
        Convert KPIDefinition to target format.

        Uses different conversion paths based on inbound source:
        - PowerBI → SQL/UC_METRICS: Use transpilation (parse_advanced)
        - YAML → SQL/UC_METRICS: Use generation (SQLGenerator/UCMetricsGenerator)

        Args:
            definition: KPIDefinition to convert
            format: Target format
            params: Format-specific parameters
            inbound_type: Source connector type (for path detection)

        Returns:
            Converted output (format depends on target)
        """
        # Detect if we should use transpilation path (PowerBI source)
        use_transpilation = (inbound_type == ConnectorType.POWERBI and
                            format in [OutboundFormat.SQL, OutboundFormat.UC_METRICS])

        if use_transpilation:
            self.logger.info(f"🔄 Using TRANSPILATION path: PowerBI DAX → {format.value.upper()} (via dax_parser.parse_advanced)")
        else:
            self.logger.info(f"🔨 Using GENERATION path: YAML → {format.value.upper()} (via generators)")

        if format == OutboundFormat.DAX:
            return self._convert_to_dax(definition, params)
        elif format == OutboundFormat.SQL:
            return self._convert_to_sql(definition, params, use_transpilation=use_transpilation)
        elif format == OutboundFormat.UC_METRICS:
            return self._convert_to_uc_metrics(definition, params, use_transpilation=use_transpilation)
        else:
            raise ValueError(f"Unsupported output format: {format}")

    def _convert_to_dax(self, definition: KPIDefinition, params: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Convert to DAX measures with automatic dependency resolution.

        Uses SmartDAXGenerator which automatically:
        - Detects if measures have dependencies
        - Routes to basic DAXGenerator for simple cases
        - Routes to TreeParsingDAXGenerator for CALCULATED measures with dependencies
        - Handles circular dependency detection and topological sorting
        """
        generator = SmartDAXGenerator()

        try:
            # Generate all measures with dependency resolution
            dax_measures = generator.generate_all_measures(definition)

            # Convert to output format
            measures = []
            for dax_measure in dax_measures:
                measures.append({
                    "name": dax_measure.name,
                    "expression": dax_measure.dax_formula,
                    "description": dax_measure.description,
                    "table": dax_measure.table,
                })

            self.logger.info(f"Generated {len(measures)} DAX measures with dependency resolution")
            return measures

        except Exception as e:
            self.logger.error(f"Failed to generate DAX measures: {e}")
            raise

    def _format_transpiled_sql(self, definition: KPIDefinition, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract transpiled SQL from KPI definitions (populated by parse_advanced in connector)
        and format as output.

        PowerBI measures are transpiled to SQL during extraction and stored in
        _advanced_parsing attribute on each KPI.

        Args:
            definition: KPIDefinition with PowerBI measures
            params: Format-specific parameters

        Returns:
            List of dicts with transpiled SQL for each measure
        """
        measures = []

        for kpi in definition.kpis:
            # Check if this KPI has advanced parsing metadata (from PowerBI connector)
            if not hasattr(kpi, '_advanced_parsing'):
                self.logger.warning(f"KPI {kpi.technical_name} missing _advanced_parsing metadata")
                continue

            advanced = kpi._advanced_parsing

            # Extract transpiled SQL
            transpiled_sql = advanced.get('transpiled_sql', '')
            is_transpilable = advanced.get('is_transpilable', False)

            measures.append({
                "name": kpi.technical_name,
                "sql": transpiled_sql if is_transpilable else kpi.formula,
                "description": kpi.description,
                "is_transpilable": is_transpilable,
                "signature": advanced.get('signature', ''),
                "source_table": kpi.source_table,
            })

        return measures

    def _format_transpiled_uc_metrics(self, definition: KPIDefinition, params: Dict[str, Any]) -> str:
        """
        Wrap transpiled SQL in UC Metrics YAML format.

        PowerBI measures are transpiled to SQL during extraction. This method
        wraps them in Unity Catalog Metrics YAML structure.

        Args:
            definition: KPIDefinition with PowerBI measures
            params: Format-specific parameters (catalog, schema, name)

        Returns:
            UC Metrics YAML string
        """
        self.logger.info(f"✨ Formatting {len(definition.kpis)} transpiled SQL measures as UC Metrics YAML")

        # Extract metadata
        catalog = params.get("catalog", "main")
        schema = params.get("schema", "default")
        name = params.get("name", definition.technical_name)

        # Build UC Metrics structure
        lines = []
        lines.append("version: '0.1'")
        lines.append("")
        lines.append(f"# --- UC Metrics for PowerBI measures from {name} ---")
        lines.append("")

        # Determine source table (use first KPI's source table or default)
        source_table = None
        for kpi in definition.kpis:
            if kpi.source_table:
                source_table = kpi.source_table
                break

        if source_table:
            # Build fully qualified source reference
            if '.' in source_table:
                source = source_table
            else:
                source = f"{catalog}.{schema}.{source_table}"
            lines.append(f"source: {source}")
            lines.append("")

        # Add measures section
        lines.append("measures:")

        for kpi in definition.kpis:
            # Check if this KPI has advanced parsing metadata
            if not hasattr(kpi, '_advanced_parsing'):
                self.logger.warning(f"KPI {kpi.technical_name} missing _advanced_parsing metadata")
                continue

            advanced = kpi._advanced_parsing
            transpiled_sql = advanced.get('transpiled_sql', '')
            is_transpilable = advanced.get('is_transpilable', False)

            # Use transpiled SQL if available, otherwise use original formula
            measure_expr = transpiled_sql if is_transpilable else kpi.formula

            # Add measure
            lines.append(f"  - name: {kpi.technical_name}")
            lines.append(f"    expr: {measure_expr}")
            if kpi.description:
                lines.append(f"    # {kpi.description}")
            if not is_transpilable:
                lines.append(f"    # WARNING: Not transpilable - using original DAX formula")
            lines.append("")

        return "\n".join(lines)

    def _convert_to_sql(self, definition: KPIDefinition, params: Dict[str, Any], use_transpilation: bool = False) -> Union[str, List[Dict[str, Any]]]:
        """
        Convert to SQL queries.

        Two paths:
        1. Transpilation (PowerBI source): Use transpiled SQL from parse_advanced
        2. Generation (YAML source): Generate SQL from KPI definitions
        """
        if use_transpilation:
            # PowerBI → SQL: Use transpiled SQL from parse_advanced
            return self._format_transpiled_sql(definition, params)
        else:
            # YAML → SQL: Generate SQL from KPI definitions
            dialect = params.get("dialect", "databricks")
            sql_dialect = SQLDialect[dialect.upper()]

            generator = SQLGenerator(dialect=sql_dialect)
            result = generator.generate_sql_from_kbi_definition(definition)

            # Return all SQL queries formatted
            if result.sql_queries:
                # Use to_output_string() to get all queries with formatting
                return result.to_output_string(formatted=True)
            return ""

    def _convert_to_uc_metrics(self, definition: KPIDefinition, params: Dict[str, Any], use_transpilation: bool = False) -> str:
        """
        Convert to UC Metrics YAML with automatic dependency resolution.

        Two paths:
        1. Transpilation (PowerBI source): Wrap transpiled SQL in UC Metrics format
        2. Generation (YAML source): Generate UC Metrics with dependency resolution

        Uses SmartUCMetricsGenerator which automatically:
        - Detects if measures have dependencies
        - Routes to basic UCMetricsGenerator for simple cases
        - Routes to UCMetricsTreeParsingGenerator for CALCULATED measures with dependencies
        - Handles circular dependency detection and topological sorting
        """
        if use_transpilation:
            # PowerBI → UC Metrics: Wrap transpiled SQL in UC Metrics format
            return self._format_transpiled_uc_metrics(definition, params)
        else:
            # YAML → UC Metrics: Generate from KPI definitions with smart routing
            generator = SmartUCMetricsGenerator()

            metadata = {
                "name": params.get("name", definition.technical_name),
                "catalog": params.get("catalog", "main"),
                "schema": params.get("schema", "default"),
            }

            try:
                # Generate with automatic dependency resolution
                uc_metrics = generator.generate_consolidated_uc_metrics(definition, metadata)

                # Format as YAML string
                # Note: SmartUCMetricsGenerator returns dict, need to format it
                # Reuse the formatting method from basic generator
                basic_gen = UCMetricsGenerator()
                yaml_output = basic_gen.format_consolidated_uc_metrics_yaml(uc_metrics)

                self.logger.info(f"Generated UC Metrics with dependency resolution: {len(uc_metrics.get('measures', []))} measures")
                return yaml_output

            except Exception as e:
                self.logger.error(f"Failed to generate UC Metrics: {e}")
                raise

    # YAML export removed - use only DAX, SQL, or UC Metrics as output formats
    # def _convert_to_yaml(self, definition: KPIDefinition, params: Dict[str, Any]) -> str:
    #     """Convert to YAML format."""
    #     from .common.transformers.yaml import YAMLKPIParser
    #     parser = YAMLKPIParser()
    #     return parser.export_to_yaml(definition)


# Convenience functions for direct usage

def convert_powerbi_to_dax(
    semantic_model_id: str,
    group_id: str,
    access_token: str,
    **kwargs
) -> Dict[str, Any]:
    """
    Convert Power BI measures to DAX.

    Args:
        semantic_model_id: Power BI dataset ID
        group_id: Workspace ID
        access_token: Access token for authentication
        **kwargs: Additional parameters (include_hidden, filter_pattern, etc.)

    Returns:
        Pipeline execution result
    """
    pipeline = ConversionPipeline()
    return pipeline.execute(
        inbound_type=ConnectorType.POWERBI,
        inbound_params={
            "semantic_model_id": semantic_model_id,
            "group_id": group_id,
            "access_token": access_token,
        },
        outbound_format=OutboundFormat.DAX,
        extract_params=kwargs
    )


def convert_powerbi_to_sql(
    semantic_model_id: str,
    group_id: str,
    access_token: str,
    dialect: str = "databricks",
    **kwargs
) -> Dict[str, Any]:
    """Convert Power BI measures to SQL."""
    pipeline = ConversionPipeline()
    return pipeline.execute(
        inbound_type=ConnectorType.POWERBI,
        inbound_params={
            "semantic_model_id": semantic_model_id,
            "group_id": group_id,
            "access_token": access_token,
        },
        outbound_format=OutboundFormat.SQL,
        outbound_params={"dialect": dialect},
        extract_params=kwargs
    )


def convert_powerbi_to_uc_metrics(
    semantic_model_id: str,
    group_id: str,
    access_token: str,
    catalog: str = "main",
    schema: str = "default",
    **kwargs
) -> Dict[str, Any]:
    """Convert Power BI measures to UC Metrics."""
    pipeline = ConversionPipeline()
    return pipeline.execute(
        inbound_type=ConnectorType.POWERBI,
        inbound_params={
            "semantic_model_id": semantic_model_id,
            "group_id": group_id,
            "access_token": access_token,
        },
        outbound_format=OutboundFormat.UC_METRICS,
        outbound_params={"catalog": catalog, "schema": schema},
        extract_params=kwargs
    )
