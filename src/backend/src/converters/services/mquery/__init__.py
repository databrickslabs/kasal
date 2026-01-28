"""
M-Query Converter Service

This module provides functionality to extract and convert Power BI M-Query
expressions to Databricks SQL.

Components:
- MQueryConnector: Main connector for scanning and converting
- MQueryParser: Parser for M-Query expressions
- MQueryLLMConverter: LLM-powered converter for complex expressions
- PowerBIAdminScanner: Scanner for Power BI Admin API
- Models: Data models for M-Query, tables, and conversion results

Example Usage:
    from src.converters.services.mquery import (
        MQueryConnector,
        MQueryConversionConfig
    )

    config = MQueryConversionConfig(
        tenant_id="your-tenant-id",
        client_id="your-client-id",
        client_secret="your-client-secret",
        workspace_id="your-workspace-id",
        dataset_id="optional-dataset-id",
        llm_workspace_url="https://your-workspace.databricks.com",
        llm_token="your-databricks-token"
    )

    async with MQueryConnector(config) as connector:
        # Scan workspace
        models = await connector.scan_workspace()

        # Get tables with M-Query expressions
        tables = connector.get_tables_with_mquery()

        # Convert all tables to SQL
        results = await connector.convert_all_tables()

        # Get relationships for FK constraints
        relationships = connector.get_relationships()

        # Generate summary report
        report = connector.generate_summary_report()

Author: Kasal Team
Date: 2025
"""

from .models import (
    ExpressionType,
    ColumnDataType,
    StorageMode,
    TableColumn,
    TableMeasure,
    MQueryExpression,
    PowerBITable,
    TableRelationship,
    SemanticModel,
    ConversionResult,
    CalculatedColumnResult,
    ScanStatus,
    MQueryConversionConfig,
    # Hierarchy models (kept for use by external tools like PowerBIHierarchiesTool)
    Hierarchy,
    HierarchyLevel,
)

from .scanner import PowerBIAdminScanner
from .parser import MQueryParser
from .llm_converter import MQueryLLMConverter
from .connector import MQueryConnector

__all__ = [
    # Main connector
    "MQueryConnector",

    # Configuration
    "MQueryConversionConfig",

    # Supporting services
    "PowerBIAdminScanner",
    "MQueryParser",
    "MQueryLLMConverter",

    # Models - Expression types
    "ExpressionType",
    "ColumnDataType",
    "StorageMode",

    # Models - Data structures
    "TableColumn",
    "TableMeasure",
    "MQueryExpression",
    "PowerBITable",
    "TableRelationship",
    "Hierarchy",
    "HierarchyLevel",
    "SemanticModel",

    # Models - Results
    "ConversionResult",
    "CalculatedColumnResult",
    "ScanStatus",
]
