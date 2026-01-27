"""
M-Query Expression Parser (Simplified)

This module provides minimal parsing for Power BI M-Query expressions.
The actual SQL conversion is handled by the LLM - this parser just:
- Detects expression type (for categorization/UI display)
- Extracts connection metadata (for logging/display)

Author: Kasal Team
Date: 2025
"""

import re
import logging
from typing import Dict, Optional, Any

from .models import (
    MQueryExpression,
    ExpressionType,
    PowerBITable
)

logger = logging.getLogger(__name__)


class MQueryParser:
    """
    Simplified parser for Power BI M-Query expressions.

    NOTE: The actual M-Query to SQL conversion is handled by the LLM.
    This parser just provides metadata extraction for UI display and logging.
    """

    # Regex patterns for expression type detection
    PATTERNS = {
        ExpressionType.NATIVE_QUERY: r"Value\.NativeQuery\s*\(",
        ExpressionType.DATABRICKS_CATALOG: r"(?:DatabricksMultiCloud\.Catalogs|Databricks\.Catalogs)\s*\(",
        ExpressionType.SQL_DATABASE: r"Sql\.Database\s*\(",
        ExpressionType.TABLE_FROM_ROWS: r"Table\.FromRows\s*\(",
        ExpressionType.ODBC: r"Odbc\.(?:Query|DataSource)\s*\(",
        ExpressionType.ORACLE: r"Oracle\.Database\s*\(",
        ExpressionType.SNOWFLAKE: r"Snowflake\.Databases\s*\(",
    }

    def detect_expression_type(self, expression: str) -> ExpressionType:
        """
        Detect the type of M-Query expression.

        Args:
            expression: Raw M-Query expression

        Returns:
            ExpressionType enum value
        """
        for expr_type, pattern in self.PATTERNS.items():
            if re.search(pattern, expression, re.IGNORECASE):
                return expr_type
        return ExpressionType.OTHER

    def extract_databricks_catalog_info(self, expression: str) -> Dict[str, Optional[str]]:
        """
        Extract connection info from DatabricksMultiCloud.Catalogs.
        Used for display/logging purposes only.

        Args:
            expression: M-Query expression

        Returns:
            Dict with workspace_url, warehouse_path, catalog, database
        """
        result: Dict[str, Optional[str]] = {
            "workspace_url": None,
            "warehouse_path": None,
            "catalog": None,
            "database": None
        }

        # Extract workspace URL
        url_match = re.search(
            r'Databricks(?:MultiCloud)?\.Catalogs\s*\(\s*"([^"]+)"',
            expression
        )
        if url_match:
            result["workspace_url"] = url_match.group(1)

        # Extract warehouse path
        warehouse_match = re.search(r'"(/sql/[^"]+)"', expression)
        if warehouse_match:
            result["warehouse_path"] = warehouse_match.group(1)

        # Extract catalog from options
        catalog_match = re.search(r'\[Catalog\s*=\s*"([^"]+)"', expression)
        if catalog_match:
            result["catalog"] = catalog_match.group(1)

        # Extract database/schema
        db_match = re.search(r'Database\s*=\s*"?([^",\]]+)"?', expression)
        if db_match and db_match.group(1) != "null":
            result["database"] = db_match.group(1)

        # Try to extract from Name in the path
        name_match = re.search(r'\{?\[Name\s*=\s*"([^"]+)"', expression)
        if name_match and not result["catalog"]:
            result["catalog"] = name_match.group(1)

        return result

    def extract_sql_database_info(self, expression: str) -> Dict[str, Optional[str]]:
        """
        Extract connection info from Sql.Database.
        Used for display/logging purposes only.

        Args:
            expression: M-Query expression

        Returns:
            Dict with server, database
        """
        result: Dict[str, Optional[str]] = {
            "server": None,
            "database": None
        }

        match = re.search(
            r'Sql\.Database\s*\(\s*"([^"]+)"\s*,\s*"([^"]+)"',
            expression
        )
        if match:
            result["server"] = match.group(1)
            result["database"] = match.group(2)

        return result

    def parse_expression(self, raw_expression: str) -> MQueryExpression:
        """
        Parse a raw M-Query expression into a structured object.

        NOTE: This is a simplified parser that only extracts metadata.
        The actual SQL conversion is handled by the LLM.

        Args:
            raw_expression: The raw M-Query expression string

        Returns:
            MQueryExpression with expression type and connection metadata
        """
        expr_type = self.detect_expression_type(raw_expression)
        logger.info(f"[PARSER] Detected expression type: {expr_type.value}")

        result = MQueryExpression(
            raw_expression=raw_expression,
            expression_type=expr_type
        )

        # Extract connection metadata based on expression type
        if expr_type in (ExpressionType.NATIVE_QUERY, ExpressionType.DATABRICKS_CATALOG):
            dbx_info = self.extract_databricks_catalog_info(raw_expression)
            if dbx_info["workspace_url"]:
                result.server = dbx_info["workspace_url"]
                result.warehouse_path = dbx_info["warehouse_path"]
                result.catalog = dbx_info["catalog"]
                result.database = dbx_info["database"]
                logger.info(f"[PARSER] Databricks source: workspace={dbx_info['workspace_url']}")
            elif expr_type == ExpressionType.NATIVE_QUERY:
                # Check for SQL Server source
                sql_info = self.extract_sql_database_info(raw_expression)
                if sql_info["server"]:
                    result.server = sql_info["server"]
                    result.database = sql_info["database"]
                    logger.info(f"[PARSER] SQL Server source: server={sql_info['server']}")

        elif expr_type == ExpressionType.SQL_DATABASE:
            sql_info = self.extract_sql_database_info(raw_expression)
            result.server = sql_info["server"]
            result.database = sql_info["database"]

        # Check for EnableFolding option
        result.enable_folding = "enablefolding=true" in raw_expression.lower()

        return result

    def parse_table(self, table: PowerBITable) -> PowerBITable:
        """
        Parse all M-Query expressions in a table.

        Args:
            table: PowerBITable with raw expressions

        Returns:
            PowerBITable with parsed expressions
        """
        parsed_expressions = []

        for expr in table.source_expressions:
            parsed = self.parse_expression(expr.raw_expression)
            parsed_expressions.append(parsed)

        table.source_expressions = parsed_expressions
        return table

    def get_expression_summary(self, expression: MQueryExpression) -> Dict[str, Any]:
        """
        Get a summary of a parsed expression for logging/display.

        Args:
            expression: Parsed MQueryExpression

        Returns:
            Summary dict
        """
        return {
            "type": expression.expression_type.value,
            "server": expression.server,
            "database": expression.database,
            "catalog": expression.catalog,
            "warehouse_path": expression.warehouse_path,
            "enable_folding": expression.enable_folding
        }
