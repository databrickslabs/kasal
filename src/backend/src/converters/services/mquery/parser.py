"""
M-Query Expression Parser

This module parses Power BI M-Query (Power Query) expressions to extract:
- Embedded SQL queries (from Value.NativeQuery)
- Connection information (server, database, catalog)
- Parameters (variables used with & concatenation)
- Transformations (Table.SelectRows, Table.ReplaceValue, etc.)

Author: Kasal Team
Date: 2025
"""

import re
import logging
from typing import Dict, List, Optional, Any, Tuple

from .models import (
    MQueryExpression,
    ExpressionType,
    PowerBITable
)

logger = logging.getLogger(__name__)


class MQueryParser:
    """
    Parser for Power BI M-Query expressions.

    Handles various M-Query patterns including:
    - Value.NativeQuery (SQL passthrough)
    - DatabricksMultiCloud.Catalogs
    - Sql.Database
    - Table.FromRows (static data)
    - Various Table.* transformations
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

    # Transformation patterns
    TRANSFORMATION_PATTERNS = {
        "select_rows": r"Table\.SelectRows\s*\([^,]+,\s*each\s+(.+?)\)",
        "replace_value": r"Table\.ReplaceValue\s*\([^,]+,\s*([^,]+),\s*([^,]+),\s*Replacer\.[^,]+,\s*\{([^}]+)\}\)",
        "first_n": r"Table\.FirstN\s*\([^,]+,\s*(\w+)\)",
        "add_column": r"Table\.AddColumn\s*\([^,]+,\s*\"([^\"]+)\",\s*each\s+(.+?)\)",
        "rename_columns": r"Table\.RenameColumns\s*\([^,]+,\s*\{(.+?)\}\)",
        "remove_columns": r"Table\.RemoveColumns\s*\([^,]+,\s*\{(.+?)\}\)",
        "filter_rows": r"Table\.SelectRows\s*\([^,]+,\s*each\s+\[([^\]]+)\]\s*([><=!]+)\s*(.+?)\)",
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

    def extract_native_query_sql(self, expression: str) -> Optional[str]:
        """
        Extract embedded SQL from Value.NativeQuery.

        Args:
            expression: M-Query expression containing Value.NativeQuery

        Returns:
            Extracted SQL string or None
        """
        # Pattern to match SQL in Value.NativeQuery
        # Handles both single-line and multi-line strings
        patterns = [
            # Double-quoted string with #(lf) line breaks
            r'Value\.NativeQuery\s*\([^,]+,\s*"((?:[^"\\]|\\.|#\([^)]+\))*)"',
            # Multi-line string
            r'Value\.NativeQuery\s*\([^,]+,\s*"((?:[^"\\]|\\.|\n)*)"',
        ]

        for pattern in patterns:
            match = re.search(pattern, expression, re.DOTALL)
            if match:
                sql = match.group(1)
                # Clean up M-Query escape sequences
                sql = self._clean_mquery_string(sql)
                return sql

        return None

    def extract_databricks_catalog_info(self, expression: str) -> Dict[str, Optional[str]]:
        """
        Extract connection info from DatabricksMultiCloud.Catalogs.

        Args:
            expression: M-Query expression

        Returns:
            Dict with workspace_url, warehouse_path, catalog, database
        """
        result = {
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

        Args:
            expression: M-Query expression

        Returns:
            Dict with server, database
        """
        result = {
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

    def extract_parameters(self, expression: str) -> List[Dict[str, str]]:
        """
        Extract parameters from M-Query expression.

        Parameters in M-Query are typically concatenated using & operator:
        - '" & ParameterName & "'
        - '" & Text.From(ParameterValue) & "'

        Args:
            expression: M-Query expression

        Returns:
            List of parameter dicts with name and context
        """
        parameters = []
        seen = set()

        # Pattern for simple parameter concatenation
        # Matches: '" & ParamName & "' or " & ParamName & "
        simple_pattern = r'["\']?\s*&\s*(\w+)\s*&\s*["\']?'

        for match in re.finditer(simple_pattern, expression):
            param_name = match.group(1)
            # Skip common M-Query keywords
            if param_name.lower() not in {"text", "number", "date", "time", "true", "false", "null"}:
                if param_name not in seen:
                    seen.add(param_name)
                    parameters.append({
                        "name": param_name,
                        "type": "STRING",
                        "context": match.group(0).strip()
                    })

        # Pattern for Text.From() wrapped parameters
        text_from_pattern = r'Text\.From\s*\(\s*(\w+)\s*\)'
        for match in re.finditer(text_from_pattern, expression):
            param_name = match.group(1)
            if param_name not in seen:
                seen.add(param_name)
                parameters.append({
                    "name": param_name,
                    "type": "ANY",
                    "context": match.group(0)
                })

        # Pattern for common filter parameters
        filter_params = ["RangeStart", "RangeEnd", "RowLimit", "TopN"]
        for param in filter_params:
            if param in expression and param not in seen:
                seen.add(param)
                parameters.append({
                    "name": param,
                    "type": "DATETIME" if "Range" in param else "INTEGER",
                    "context": "Incremental refresh parameter"
                })

        return parameters

    def extract_transformations(self, expression: str) -> List[Dict[str, Any]]:
        """
        Extract Power Query transformations from the expression.

        Args:
            expression: M-Query expression

        Returns:
            List of transformation dicts with type, details, and SQL equivalent
        """
        transformations = []

        # Table.SelectRows - WHERE clause
        select_rows = re.findall(
            r'Table\.SelectRows\s*\([^,]+,\s*each\s+(.+?)(?:\)|,)',
            expression,
            re.DOTALL
        )
        for condition in select_rows:
            # Clean up the condition
            clean_condition = condition.strip()
            if clean_condition:
                transformations.append({
                    "type": "filter",
                    "mquery": f"Table.SelectRows(_, each {clean_condition})",
                    "condition": clean_condition,
                    "sql_hint": "WHERE clause"
                })

        # Table.ReplaceValue - COALESCE
        replace_value = re.findall(
            r'Table\.ReplaceValue\s*\([^,]+,\s*null\s*,\s*"([^"]+)"\s*,\s*Replacer\.\w+\s*,\s*\{"([^"]+)"\}',
            expression
        )
        for replacement, column in replace_value:
            transformations.append({
                "type": "replace_null",
                "mquery": f'Table.ReplaceValue(_, null, "{replacement}", _, {{"{column}"}})',
                "column": column,
                "replacement": replacement,
                "sql_hint": f"COALESCE({column}, '{replacement}')"
            })

        # Table.FirstN - LIMIT
        first_n = re.findall(r'Table\.FirstN\s*\([^,]+,\s*(\w+)\)', expression)
        for limit_var in first_n:
            transformations.append({
                "type": "limit",
                "mquery": f"Table.FirstN(_, {limit_var})",
                "limit_variable": limit_var,
                "sql_hint": f"LIMIT {limit_var}"
            })

        # Table.AddColumn - Computed column
        add_column = re.findall(
            r'Table\.AddColumn\s*\([^,]+,\s*"([^"]+)"\s*,\s*each\s+(.+?)(?:\)|,)',
            expression,
            re.DOTALL
        )
        for col_name, col_expr in add_column:
            transformations.append({
                "type": "computed_column",
                "mquery": f'Table.AddColumn(_, "{col_name}", each {col_expr})',
                "column_name": col_name,
                "expression": col_expr.strip(),
                "sql_hint": f"{col_expr.strip()} AS {col_name}"
            })

        return transformations

    def _clean_mquery_string(self, text: str) -> str:
        """
        Clean M-Query string escape sequences.

        Args:
            text: Raw string from M-Query

        Returns:
            Cleaned string
        """
        # Replace #(lf) with newline
        text = re.sub(r'#\(lf\)', '\n', text)
        # Replace #(cr) with carriage return
        text = re.sub(r'#\(cr\)', '\r', text)
        # Replace #(tab) with tab
        text = re.sub(r'#\(tab\)', '\t', text)
        # Replace escaped quotes
        text = text.replace('""', '"')
        return text

    def parse_expression(self, raw_expression: str) -> MQueryExpression:
        """
        Parse a raw M-Query expression into a structured object.

        Args:
            raw_expression: The raw M-Query expression string

        Returns:
            MQueryExpression with extracted information
        """
        expr_type = self.detect_expression_type(raw_expression)

        result = MQueryExpression(
            raw_expression=raw_expression,
            expression_type=expr_type
        )

        # Extract based on expression type
        if expr_type == ExpressionType.NATIVE_QUERY:
            result.embedded_sql = self.extract_native_query_sql(raw_expression)

            # Check if the data source is Databricks
            dbx_info = self.extract_databricks_catalog_info(raw_expression)
            if dbx_info["workspace_url"]:
                result.server = dbx_info["workspace_url"]
                result.warehouse_path = dbx_info["warehouse_path"]
                result.catalog = dbx_info["catalog"]
                result.database = dbx_info["database"]
            else:
                # Check for SQL Server
                sql_info = self.extract_sql_database_info(raw_expression)
                result.server = sql_info["server"]
                result.database = sql_info["database"]

        elif expr_type == ExpressionType.DATABRICKS_CATALOG:
            dbx_info = self.extract_databricks_catalog_info(raw_expression)
            result.server = dbx_info["workspace_url"]
            result.warehouse_path = dbx_info["warehouse_path"]
            result.catalog = dbx_info["catalog"]
            result.database = dbx_info["database"]

        elif expr_type == ExpressionType.SQL_DATABASE:
            sql_info = self.extract_sql_database_info(raw_expression)
            result.server = sql_info["server"]
            result.database = sql_info["database"]

        # Extract parameters and transformations for all types
        result.parameters = self.extract_parameters(raw_expression)
        result.transformations = self.extract_transformations(raw_expression)

        # Check for EnableFolding option
        result.enable_folding = "EnableFolding=true" in raw_expression.lower()

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
            "has_embedded_sql": bool(expression.embedded_sql),
            "server": expression.server,
            "database": expression.database,
            "catalog": expression.catalog,
            "parameter_count": len(expression.parameters),
            "transformation_count": len(expression.transformations),
            "enable_folding": expression.enable_folding
        }
