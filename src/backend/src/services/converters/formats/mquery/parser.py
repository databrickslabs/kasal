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


class TableFromRowsConverter:
    """
    Separate converter for Table.FromRows M-Query expressions.

    Converts static data tables defined in Power BI M-Query to SQL VALUES statements.
    This is a standalone converter that doesn't affect the main LLM-based conversion flow.

    Example M-Query:
        let
          Source = Table.FromRows({
            {"value1", "value2"},
            {"value3", "value4"}
          },
          type table [Column1 = text, Column2 = text])
        in Source

    Converts to SQL:
        CREATE OR REPLACE VIEW catalog.schema.table_name AS
        SELECT * FROM VALUES
          ('value1', 'value2'),
          ('value3', 'value4')
        AS t(Column1, Column2);
    """

    def __init__(self, target_catalog: str = "main", target_schema: str = "default"):
        """
        Initialize the Table.FromRows converter.

        Args:
            target_catalog: Target Unity Catalog catalog name
            target_schema: Target Unity Catalog schema name
        """
        self.target_catalog = target_catalog
        self.target_schema = target_schema

    def is_table_from_rows(self, expression: str) -> bool:
        """Check if expression contains Table.FromRows."""
        return bool(re.search(r"Table\.FromRows\s*\(", expression, re.IGNORECASE))

    def extract_rows(self, expression: str) -> list:
        """
        Extract row data from Table.FromRows expression.

        Args:
            expression: M-Query expression containing Table.FromRows

        Returns:
            List of row tuples
        """
        rows = []

        # Find the rows section: Table.FromRows( { {row1}, {row2}, ... }, type table [...] )
        # Match the content between Table.FromRows( { and }, type table
        rows_match = re.search(
            r"Table\.FromRows\s*\(\s*\{\s*((?:\{[^}]*\}\s*,?\s*)+)\s*\}",
            expression,
            re.IGNORECASE | re.DOTALL
        )

        if not rows_match:
            logger.warning("[TableFromRows] Could not extract rows from expression")
            return rows

        rows_content = rows_match.group(1)

        # Extract individual row tuples: {"value1", "value2", ...}
        row_pattern = re.compile(r"\{([^}]+)\}")
        for row_match in row_pattern.finditer(rows_content):
            row_content = row_match.group(1)

            # Parse values - handle quoted strings and unquoted values
            values = []
            # Match quoted strings or unquoted values
            value_pattern = re.compile(r'"([^"]*)"|([^,"\s]+)')
            for value_match in value_pattern.finditer(row_content):
                if value_match.group(1) is not None:
                    values.append(value_match.group(1))
                elif value_match.group(2) is not None:
                    values.append(value_match.group(2))

            if values:
                rows.append(tuple(values))

        logger.info(f"[TableFromRows] Extracted {len(rows)} rows")
        return rows

    def extract_column_definitions(self, expression: str) -> list:
        """
        Extract column names and types from type table [...] definition.

        Args:
            expression: M-Query expression

        Returns:
            List of (column_name, column_type) tuples
        """
        columns = []

        # Find type table [ Column1 = text, Column2 = number, ... ]
        type_table_match = re.search(
            r"type\s+table\s*\[\s*((?:[^\]]+))\s*\]",
            expression,
            re.IGNORECASE | re.DOTALL
        )

        if not type_table_match:
            logger.warning("[TableFromRows] Could not find type table definition")
            return columns

        type_content = type_table_match.group(1)

        # Parse column definitions: ColumnName = type
        column_pattern = re.compile(r"(\w+)\s*=\s*(text|number|Int64\.Type|type\s+\w+|\w+)")
        for col_match in column_pattern.finditer(type_content):
            col_name = col_match.group(1)
            col_type = col_match.group(2).strip()
            columns.append((col_name, col_type))

        logger.info(f"[TableFromRows] Extracted {len(columns)} column definitions")
        return columns

    def mquery_type_to_sql(self, mquery_type: str) -> str:
        """
        Convert M-Query type to SQL type.

        Args:
            mquery_type: M-Query type string

        Returns:
            SQL type string
        """
        type_map = {
            "text": "STRING",
            "number": "DOUBLE",
            "int64.type": "BIGINT",
            "type number": "DOUBLE",
            "type text": "STRING",
            "type date": "DATE",
            "type datetime": "TIMESTAMP",
            "type datetimezone": "TIMESTAMP",
            "type time": "STRING",
            "type duration": "STRING",
            "type logical": "BOOLEAN",
            "type binary": "BINARY",
        }
        return type_map.get(mquery_type.lower(), "STRING")

    def convert_to_sql(
        self,
        expression: str,
        table_name: str,
        columns_from_schema: Optional[list] = None
    ) -> Optional[str]:
        """
        Convert Table.FromRows M-Query to CREATE VIEW with VALUES statement.

        Args:
            expression: M-Query expression containing Table.FromRows
            table_name: Name for the output view
            columns_from_schema: Optional list of column dicts from API schema
                                (fallback if type table parsing fails)

        Returns:
            SQL CREATE VIEW statement or None if conversion fails
        """
        if not self.is_table_from_rows(expression):
            logger.info(f"[TableFromRows] Expression is not Table.FromRows, skipping")
            return None

        # Extract rows
        rows = self.extract_rows(expression)
        if not rows:
            logger.warning(f"[TableFromRows] No rows extracted from {table_name}")
            return None

        # Extract column definitions from type table
        columns = self.extract_column_definitions(expression)

        # Fallback to schema columns if type table parsing fails
        if not columns and columns_from_schema:
            columns = [
                (col.get("name", f"col{i}"), col.get("dataType", "String"))
                for i, col in enumerate(columns_from_schema)
            ]
            logger.info(f"[TableFromRows] Using schema columns as fallback: {len(columns)} columns")

        if not columns:
            logger.warning(f"[TableFromRows] No column definitions found for {table_name}")
            # Try to infer column count from first row
            if rows:
                columns = [(f"col{i}", "text") for i in range(len(rows[0]))]
                logger.info(f"[TableFromRows] Inferred {len(columns)} columns from row data")

        # Build column names list
        column_names = [col[0] for col in columns]

        # Sanitize table name
        safe_table_name = re.sub(r"[^a-zA-Z0-9_]", "_", table_name.lower())

        # Build VALUES clause
        values_rows = []
        for row in rows:
            # Quote string values, handle NULLs
            formatted_values = []
            for val in row:
                if val is None or val.lower() == "null":
                    formatted_values.append("NULL")
                else:
                    # Escape single quotes in values
                    escaped_val = val.replace("'", "''")
                    formatted_values.append(f"'{escaped_val}'")
            values_rows.append(f"  ({', '.join(formatted_values)})")

        # Build the SQL - join rows with comma and newline
        values_str = ',\n'.join(values_rows)
        sql = f"""CREATE OR REPLACE VIEW {self.target_catalog}.{self.target_schema}.{safe_table_name} AS
SELECT * FROM VALUES
{values_str}
AS t({', '.join(column_names)});"""

        logger.info(f"[TableFromRows] Generated SQL for {table_name}: {len(rows)} rows, {len(columns)} columns")
        return sql

    def convert_table(
        self,
        table_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Convert a Power BI table with Table.FromRows source to SQL.

        Args:
            table_data: Table dict from Power BI API containing:
                - name: Table name
                - columns: List of column definitions
                - source: List with expression dict

        Returns:
            Dict with table_name, sql, row_count, column_count or None
        """
        table_name = table_data.get("name", "unknown_table")

        # Get source expression
        sources = table_data.get("source", [])
        if not sources:
            logger.info(f"[TableFromRows] No source expressions for {table_name}")
            return None

        expression = sources[0].get("expression", "")
        if not expression:
            logger.info(f"[TableFromRows] Empty expression for {table_name}")
            return None

        # Check if it's Table.FromRows
        if not self.is_table_from_rows(expression):
            return None

        # Get column schema as fallback
        columns_from_schema = table_data.get("columns", [])

        # Convert to SQL
        sql = self.convert_to_sql(expression, table_name, columns_from_schema)

        if not sql:
            return None

        # Count rows for reporting
        rows = self.extract_rows(expression)
        columns = self.extract_column_definitions(expression) or columns_from_schema

        return {
            "table_name": table_name,
            "sql": sql,
            "row_count": len(rows),
            "column_count": len(columns),
            "expression_type": "table_from_rows"
        }
