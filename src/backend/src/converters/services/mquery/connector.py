"""
M-Query Connector

This connector extracts M-Query expressions from Power BI semantic models
using the Admin API and converts them to Databricks SQL.

It follows the BaseInboundConnector pattern for consistency with other connectors.

Author: Kasal Team
Date: 2025
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any

from src.converters.base.connectors import (
    BaseInboundConnector,
    ConnectorType,
    InboundConnectorMetadata
)
from src.converters.base.models import KPI, KPIDefinition

from ..powerbi.authentication import AadService

from .models import (
    SemanticModel,
    PowerBITable,
    ConversionResult,
    MQueryConversionConfig,
    ExpressionType
)
from .scanner import PowerBIAdminScanner
from .parser import MQueryParser
from .llm_converter import MQueryLLMConverter

logger = logging.getLogger(__name__)


class MQueryConnector(BaseInboundConnector):
    """
    Connector for extracting and converting Power BI M-Query expressions.

    This connector:
    1. Authenticates with Power BI Admin API using Service Principal
    2. Scans workspaces to extract M-Query expressions
    3. Parses expressions to extract SQL and metadata
    4. Converts complex expressions using LLM
    5. Returns results as ConversionResult objects

    Usage:
        config = MQueryConversionConfig(
            tenant_id="...",
            client_id="...",
            client_secret="...",
            workspace_id="..."
        )
        connector = MQueryConnector(config)

        async with connector:
            # Get semantic models
            models = await connector.scan_workspace()

            # Convert tables to SQL
            results = await connector.convert_all_tables(models[0])
    """

    def __init__(
        self,
        config: MQueryConversionConfig,
        access_token: Optional[str] = None
    ):
        """
        Initialize the M-Query connector.

        Args:
            config: Configuration with Power BI and LLM credentials
            access_token: Optional pre-obtained access token
        """
        self.config = config
        self._access_token = access_token
        self._connected = False

        # Initialize services
        self._auth_service: Optional[AadService] = None
        self._scanner: Optional[PowerBIAdminScanner] = None
        self._parser = MQueryParser()
        self._llm_converter: Optional[MQueryLLMConverter] = None

        # Cached data
        self._semantic_models: List[SemanticModel] = []
        self._raw_scan_data: Optional[Dict[str, Any]] = None

    # ========== BaseInboundConnector Implementation ==========

    def connect(self) -> None:
        """
        Establish connection to Power BI Admin API.

        Obtains access token using Service Principal credentials.
        """
        if self._connected:
            return

        logger.info("Connecting to Power BI Admin API...")

        # Initialize auth service
        self._auth_service = AadService(
            tenant_id=self.config.tenant_id,
            client_id=self.config.client_id,
            client_secret=self.config.client_secret,
            access_token=self._access_token
        )

        # Get access token
        if not self._access_token:
            self._access_token = self._auth_service.get_access_token()

        # Initialize scanner with token
        self._scanner = PowerBIAdminScanner(
            access_token=self._access_token,
            config=self.config
        )

        # Initialize LLM converter if credentials provided
        if self.config.llm_workspace_url and self.config.llm_token:
            self._llm_converter = MQueryLLMConverter(
                workspace_url=self.config.llm_workspace_url,
                token=self.config.llm_token,
                model=self.config.llm_model
            )
        else:
            logger.info("LLM credentials not provided, will use rule-based conversion only")
            self._llm_converter = MQueryLLMConverter()  # No LLM, rule-based only

        self._connected = True
        logger.info("Successfully connected to Power BI Admin API")

    def disconnect(self) -> None:
        """Clean up resources"""
        self._connected = False
        self._access_token = None
        self._scanner = None
        self._semantic_models = []
        self._raw_scan_data = None
        logger.info("Disconnected from Power BI Admin API")

    def extract_measures(self, **kwargs) -> List[KPI]:
        """
        Extract measures from the semantic model.

        Note: This connector primarily deals with M-Query table expressions,
        not DAX measures. Use PowerBIConnector for DAX measure extraction.

        Returns:
            List of KPI objects (from DAX measures in tables)
        """
        kpis = []

        for model in self._semantic_models:
            for table in model.tables:
                for measure in table.measures:
                    kpis.append(KPI(
                        description=measure.description or measure.name,
                        formula=measure.expression,
                        filters=[],
                        technical_name=f"{table.name}.{measure.name}",
                        source_table=table.name
                    ))

        return kpis

    def get_metadata(self) -> InboundConnectorMetadata:
        """Get connector metadata"""
        table_count = sum(
            len(model.tables) for model in self._semantic_models
        )
        measure_count = sum(
            len(table.measures)
            for model in self._semantic_models
            for table in model.tables
        )

        return InboundConnectorMetadata(
            connector_type=ConnectorType.POWERBI,  # Using POWERBI since we're accessing PBI
            source_id=self.config.workspace_id,
            source_name=self._semantic_models[0].workspace_name if self._semantic_models else None,
            description="Power BI Admin API - M-Query Extractor",
            connected=self._connected,
            measure_count=measure_count,
            additional_info={
                "model_count": len(self._semantic_models),
                "table_count": table_count,
                "dataset_id": self.config.dataset_id
            }
        )

    @property
    def is_connected(self) -> bool:
        """Check if connected"""
        return self._connected

    # ========== Async Context Manager ==========

    async def __aenter__(self):
        """Async context manager entry"""
        self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        self.disconnect()

    # ========== M-Query Specific Methods ==========

    async def scan_workspace(
        self,
        workspace_id: Optional[str] = None,
        dataset_id: Optional[str] = None
    ) -> List[SemanticModel]:
        """
        Scan a workspace and extract semantic models.

        Args:
            workspace_id: Override workspace ID from config
            dataset_id: Optional specific dataset ID to filter

        Returns:
            List of SemanticModel objects
        """
        if not self._connected:
            self.connect()

        ws_id = workspace_id or self.config.workspace_id
        ds_id = dataset_id or self.config.dataset_id

        logger.info(f"Scanning workspace {ws_id}...")

        self._semantic_models, self._raw_scan_data = await self._scanner.scan_workspace(
            workspace_id=ws_id,
            dataset_id=ds_id
        )

        # Parse M-Query expressions in all tables
        for model in self._semantic_models:
            for table in model.tables:
                self._parser.parse_table(table)

        logger.info(f"Found {len(self._semantic_models)} semantic model(s)")
        return self._semantic_models

    def get_tables_with_mquery(
        self,
        model: Optional[SemanticModel] = None,
        include_hidden: bool = False
    ) -> List[PowerBITable]:
        """
        Get tables that have M-Query source expressions.

        Args:
            model: Specific model to get tables from (uses first if not specified)
            include_hidden: Whether to include hidden tables

        Returns:
            List of PowerBITable objects with M-Query expressions
        """
        target_model = model or (self._semantic_models[0] if self._semantic_models else None)
        if not target_model:
            return []

        return self._scanner.extract_tables_with_mquery(
            target_model,
            include_hidden=include_hidden or self.config.include_hidden_tables
        )

    async def convert_table(
        self,
        table: PowerBITable,
        use_llm: bool = True
    ) -> List[ConversionResult]:
        """
        Convert a single table's M-Query expressions to SQL.

        Args:
            table: PowerBITable with parsed expressions
            use_llm: Whether to use LLM for complex conversions

        Returns:
            List of ConversionResult objects
        """
        if not self._llm_converter:
            self._llm_converter = MQueryLLMConverter()

        return await self._llm_converter.convert_table(
            table=table,
            target_catalog=self.config.target_catalog,
            target_schema=self.config.target_schema,
            use_llm=use_llm and bool(self.config.llm_workspace_url)
        )

    async def convert_all_tables(
        self,
        model: Optional[SemanticModel] = None,
        use_llm: bool = True
    ) -> Dict[str, List[ConversionResult]]:
        """
        Convert all tables in a semantic model.

        Args:
            model: Specific model to convert (uses first if not specified)
            use_llm: Whether to use LLM for complex conversions

        Returns:
            Dict mapping table names to ConversionResult lists
        """
        tables = self.get_tables_with_mquery(model)
        results = {}

        for table in tables:
            # Skip static tables if configured
            if self.config.skip_static_tables:
                has_static = any(
                    expr.expression_type == ExpressionType.TABLE_FROM_ROWS
                    for expr in table.source_expressions
                )
                if has_static:
                    logger.info(f"Skipping static table '{table.name}'")
                    continue

            table_results = await self.convert_table(table, use_llm=use_llm)
            results[table.name] = table_results

        return results

    def get_relationships(
        self,
        model: Optional[SemanticModel] = None
    ) -> List[Dict[str, Any]]:
        """
        Get relationships from a semantic model.

        Args:
            model: Specific model (uses first if not specified)

        Returns:
            List of relationship dicts with FK constraint info
        """
        target_model = model or (self._semantic_models[0] if self._semantic_models else None)
        if not target_model:
            return []

        relationships = []
        for rel in target_model.relationships:
            relationships.append({
                "name": rel.name,
                "from_table": rel.from_table,
                "from_column": rel.from_column,
                "to_table": rel.to_table,
                "to_column": rel.to_column,
                "cardinality": rel.cardinality,
                "is_active": rel.is_active,
                "uc_fk_sql": (
                    f"ALTER TABLE {self.config.target_catalog or 'catalog'}."
                    f"{self.config.target_schema or 'schema'}.{rel.from_table} "
                    f"ADD CONSTRAINT fk_{rel.name} FOREIGN KEY ({rel.from_column}) "
                    f"REFERENCES {rel.to_table}({rel.to_column})"
                )
            })

        return relationships

    def generate_summary_report(self) -> Dict[str, Any]:
        """
        Generate a summary report of extracted data.

        Returns:
            Dict with summary statistics and details
        """
        if not self._semantic_models:
            return {"error": "No models scanned yet"}

        total_tables = 0
        total_measures = 0
        expression_types = {}
        tables_by_type = {}

        for model in self._semantic_models:
            for table in model.tables:
                total_tables += 1
                total_measures += len(table.measures)

                for expr in table.source_expressions:
                    expr_type = expr.expression_type.value
                    expression_types[expr_type] = expression_types.get(expr_type, 0) + 1

                    if expr_type not in tables_by_type:
                        tables_by_type[expr_type] = []
                    tables_by_type[expr_type].append(table.name)

        return {
            "workspace_id": self.config.workspace_id,
            "dataset_id": self.config.dataset_id,
            "model_count": len(self._semantic_models),
            "total_tables": total_tables,
            "total_measures": total_measures,
            "expression_types": expression_types,
            "tables_by_type": tables_by_type,
            "relationships_count": sum(
                len(m.relationships) for m in self._semantic_models
            )
        }
