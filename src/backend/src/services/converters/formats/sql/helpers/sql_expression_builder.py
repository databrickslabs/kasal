"""
SQL Expression Builder - Centralized SQL Expression Generation

This module provides a single source of truth for all SQL expression generation,
similar to how TranspilationEngine centralizes DAX transpilation logic.

All SQL expression building, aggregations, filters, and dialect-specific
formatting are consolidated here for easy maintenance and extension.
"""

from enum import Enum
from typing import Dict, List, Optional, Any, Tuple, Callable
import re
import logging
from ..models import SQLDialect, SQLAggregationType


class SQLExpressionEngine:
    """
    Centralized SQL expression generation engine.

    Similar to TranspilationEngine for DAX, this class provides a single
    location for all SQL expression generation logic, making it easy to:
    - Add new SQL functions and expressions
    - Maintain expression templates
    - Update dialect-specific behaviors
    - Ensure consistency across all SQL generation

    Usage:
        engine = SQLExpressionEngine(dialect=SQLDialect.DATABRICKS)
        sql = engine.build_aggregation(SQLAggregationType.SUM, "revenue", "sales")
        # Returns: "SUM(`sales`.`revenue`)"
    """

    def __init__(self, dialect: SQLDialect = SQLDialect.STANDARD):
        """
        Initialize SQL expression engine.

        Args:
            dialect: SQL dialect for expression generation
        """
        self.dialect = dialect
        self.logger = logging.getLogger(__name__)

        # Dialect-specific configurations
        self.dialect_config = self._get_dialect_config()

        # Aggregation function mappings (similar to TranspilationEngine.function_mappings)
        self.aggregation_builders: Dict[SQLAggregationType, Callable] = {
            SQLAggregationType.SUM: self._build_sum,
            SQLAggregationType.COUNT: self._build_count,
            SQLAggregationType.AVG: self._build_avg,
            SQLAggregationType.MIN: self._build_min,
            SQLAggregationType.MAX: self._build_max,
            SQLAggregationType.COUNT_DISTINCT: self._build_count_distinct,
            SQLAggregationType.STDDEV: self._build_stddev,
            SQLAggregationType.VARIANCE: self._build_variance,
            SQLAggregationType.MEDIAN: self._build_median,
            SQLAggregationType.PERCENTILE: self._build_percentile,
            SQLAggregationType.WEIGHTED_AVG: self._build_weighted_avg,
            SQLAggregationType.RATIO: self._build_ratio,
            SQLAggregationType.RUNNING_SUM: self._build_running_sum,
            SQLAggregationType.COALESCE: self._build_coalesce,
            SQLAggregationType.ROW_NUMBER: self._build_row_number,
            SQLAggregationType.RANK: self._build_rank,
            SQLAggregationType.DENSE_RANK: self._build_dense_rank,
            SQLAggregationType.EXCEPTION_AGGREGATION: self._build_exception_aggregation,
        }

    # ========================================================================
    # Configuration
    # ========================================================================

    def _get_dialect_config(self) -> Dict[str, Any]:
        """
        Get dialect-specific configuration for SQL generation.

        Supports:
        - DATABRICKS (primary): Databricks SQL / Spark SQL with Unity Catalog
        - STANDARD (fallback): ANSI SQL standard for compatibility
        """
        configs = {
            SQLDialect.DATABRICKS: {
                "quote_char": "`",
                "quote_char_end": "`",
                "limit_syntax": "LIMIT",
                "supports_cte": True,
                "supports_window_functions": True,
                "date_format": "yyyy-MM-dd",
                "string_concat": "||",
                "case_sensitive": False,
                "unity_catalog": True,
            },
            SQLDialect.STANDARD: {
                "quote_char": '"',
                "quote_char_end": '"',
                "limit_syntax": "LIMIT",
                "supports_cte": True,
                "supports_window_functions": True,
                "date_format": "YYYY-MM-DD",
                "string_concat": "||",
                "case_sensitive": True,
            },
        }
        return configs.get(self.dialect, configs[SQLDialect.DATABRICKS])

    # ========================================================================
    # Public API - Expression Building
    # ========================================================================

    def build_aggregation(
        self,
        agg_type: SQLAggregationType,
        column_name: str,
        table_name: str,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Build SQL aggregation expression.

        Central method for building all types of SQL aggregations.
        Similar to TranspilationEngine.transpile() for DAX.

        Args:
            agg_type: Type of SQL aggregation
            column_name: Column to aggregate
            table_name: Source table name
            context: Additional context (filters, KPI definition, etc.)

        Returns:
            SQL aggregation expression

        Example:
            >>> engine = SQLExpressionEngine(SQLDialect.DATABRICKS)
            >>> engine.build_aggregation(SQLAggregationType.SUM, "revenue", "sales")
            "SUM(`sales`.`revenue`)"
        """
        if context is None:
            context = {}

        if agg_type in self.aggregation_builders:
            return self.aggregation_builders[agg_type](column_name, table_name, context)
        else:
            # Fallback to SUM
            self.logger.warning(f"Unknown aggregation type {agg_type}, falling back to SUM")
            return self._build_sum(column_name, table_name, context)

    def build_filter(
        self,
        filter_expr: str,
        table_name: str,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Build SQL filter expression (WHERE clause).

        Args:
            filter_expr: Filter expression to build
            table_name: Source table name
            context: Additional context

        Returns:
            SQL WHERE clause expression
        """
        if context is None:
            context = {}

        # Handle various filter formats
        if not filter_expr or filter_expr.strip() == "":
            return ""

        # If it's already a valid SQL expression, return as-is
        if self._is_valid_sql_filter(filter_expr):
            return filter_expr

        # Otherwise, build filter expression
        return self._build_filter_expression(filter_expr, table_name, context)

    def build_case_when(
        self,
        conditions: List[Tuple[str, Any]],
        else_value: Any = None
    ) -> str:
        """
        Build CASE WHEN expression.

        Args:
            conditions: List of (condition, value) tuples
            else_value: ELSE value (optional)

        Returns:
            CASE WHEN SQL expression

        Example:
            >>> engine.build_case_when([
            ...     ("status = 'active'", 1),
            ...     ("status = 'pending'", 0.5)
            ... ], 0)
            "CASE WHEN status = 'active' THEN 1 WHEN status = 'pending' THEN 0.5 ELSE 0 END"
        """
        case_parts = ["CASE"]

        for condition, value in conditions:
            case_parts.append(f"WHEN {condition} THEN {self._format_value(value)}")

        if else_value is not None:
            case_parts.append(f"ELSE {self._format_value(else_value)}")

        case_parts.append("END")
        return " ".join(case_parts)

    def build_window_function(
        self,
        function_name: str,
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[Tuple[str, str]]] = None,
        frame_clause: Optional[str] = None
    ) -> str:
        """
        Build window function expression.

        Args:
            function_name: Window function name (e.g., ROW_NUMBER(), SUM(column))
            partition_by: List of partition columns
            order_by: List of (column, direction) tuples
            frame_clause: Frame clause (e.g., "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")

        Returns:
            Window function SQL expression
        """
        parts = [function_name, "OVER ("]

        if partition_by:
            quoted_partitions = [self._quote_identifier(col) for col in partition_by]
            parts.append(f"PARTITION BY {', '.join(quoted_partitions)}")

        if order_by:
            order_parts = []
            for col, direction in order_by:
                quoted_col = self._quote_identifier(col)
                order_parts.append(f"{quoted_col} {direction.upper()}")

            if partition_by:
                parts.append(" ")
            parts.append(f"ORDER BY {', '.join(order_parts)}")

        if frame_clause:
            parts.append(f" {frame_clause}")

        parts.append(")")
        return "".join(parts)

    # ========================================================================
    # Helper Methods - Formatting & Quoting
    # ========================================================================

    def _quote_identifier(self, identifier: str) -> str:
        """Quote identifier according to SQL dialect."""
        quote_start = self.dialect_config["quote_char"]
        quote_end = self.dialect_config.get("quote_char_end", quote_start)
        return f"{quote_start}{identifier}{quote_end}"

    def _format_value(self, value: Any) -> str:
        """Format value for SQL (handles strings, numbers, etc.)."""
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # Escape single quotes
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            return str(value)

    def _is_valid_sql_filter(self, expr: str) -> bool:
        """Check if expression is already a valid SQL filter."""
        # Simple heuristic: if it contains SQL operators, assume it's valid
        sql_operators = ['=', '!=', '<>', '>', '<', '>=', '<=', 'AND', 'OR', 'IN', 'LIKE', 'BETWEEN']
        expr_upper = expr.upper()
        return any(op in expr_upper for op in sql_operators)

    def _build_filter_expression(
        self,
        filter_expr: str,
        table_name: str,
        context: Dict[str, Any]
    ) -> str:
        """Build filter expression from filter string."""
        # Parse and build filter
        # This is a simplified version - can be extended based on needs
        quoted_table = self._quote_identifier(table_name)

        # Handle simple column = value patterns
        if '=' in filter_expr:
            parts = filter_expr.split('=')
            if len(parts) == 2:
                col = parts[0].strip()
                val = parts[1].strip()
                quoted_col = self._quote_identifier(col)
                return f"{quoted_table}.{quoted_col} = {self._format_value(val)}"

        # Return as-is if we can't parse it
        return filter_expr

    # ========================================================================
    # Aggregation Builders - Core SQL Functions
    # ========================================================================

    def _build_sum(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build SUM aggregation."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)

        # Handle CASE expressions (don't quote them)
        if column_name.upper().startswith('CASE'):
            return f"SUM({column_name})"

        return f"SUM({quoted_table}.{quoted_column})"

    def _build_count(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build COUNT aggregation."""
        if column_name == "*" or column_name.upper() == "COUNT":
            return "COUNT(*)"

        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        return f"COUNT({quoted_table}.{quoted_column})"

    def _build_avg(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build AVG aggregation."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        return f"AVG({quoted_table}.{quoted_column})"

    def _build_min(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build MIN aggregation."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        return f"MIN({quoted_table}.{quoted_column})"

    def _build_max(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build MAX aggregation."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        return f"MAX({quoted_table}.{quoted_column})"

    def _build_count_distinct(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build COUNT DISTINCT aggregation."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        return f"COUNT(DISTINCT {quoted_table}.{quoted_column})"

    def _build_stddev(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build STDDEV aggregation."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)

        # DATABRICKS and STANDARD both support STDDEV
        return f"STDDEV({quoted_table}.{quoted_column})"

    def _build_variance(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build VARIANCE aggregation."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)

        # DATABRICKS and STANDARD both support VARIANCE
        return f"VARIANCE({quoted_table}.{quoted_column})"

    def _build_median(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build MEDIAN aggregation."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)

        # DATABRICKS and STANDARD both support PERCENTILE_CONT
        return f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {quoted_table}.{quoted_column})"

    def _build_percentile(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build PERCENTILE aggregation."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)

        # Get percentile value from context (default to 0.5)
        percentile = context.get('percentile', 0.5)

        if self.dialect == SQLDialect.DATABRICKS:
            return f"PERCENTILE({quoted_table}.{quoted_column}, {percentile})"
        else:
            return f"PERCENTILE_CONT({percentile}) WITHIN GROUP (ORDER BY {quoted_table}.{quoted_column})"

    def _build_weighted_avg(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build WEIGHTED_AVG aggregation."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)

        # Get weight column from context
        weight_column = context.get('weight_column', 'weight')
        quoted_weight = self._quote_identifier(weight_column)

        # Weighted average: SUM(value * weight) / SUM(weight)
        return f"SUM({quoted_table}.{quoted_column} * {quoted_table}.{quoted_weight}) / NULLIF(SUM({quoted_table}.{quoted_weight}), 0)"

    def _build_ratio(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build RATIO aggregation."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)

        # Get denominator column from context
        denominator_column = context.get('denominator_column', 'total')
        quoted_denominator = self._quote_identifier(denominator_column)

        # Ratio with NULL handling
        return f"{quoted_table}.{quoted_column} / NULLIF({quoted_table}.{quoted_denominator}, 0)"

    def _build_running_sum(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build RUNNING_SUM (cumulative sum) using window function."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)

        # Get partition and order columns from context
        partition_by = context.get('partition_by', [])
        order_by = context.get('order_by', [])

        # Build window function
        return self.build_window_function(
            function_name=f"SUM({quoted_table}.{quoted_column})",
            partition_by=partition_by,
            order_by=order_by,
            frame_clause="ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
        )

    def _build_coalesce(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build COALESCE expression."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)

        # Get default value from context
        default_value = context.get('default_value', 0)
        formatted_default = self._format_value(default_value)

        return f"COALESCE({quoted_table}.{quoted_column}, {formatted_default})"

    # ========================================================================
    # Window Functions
    # ========================================================================

    def _build_row_number(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build ROW_NUMBER window function."""
        partition_by = context.get('partition_by', [])
        order_by = context.get('order_by', [(column_name, 'ASC')])

        return self.build_window_function(
            function_name="ROW_NUMBER()",
            partition_by=partition_by,
            order_by=order_by
        )

    def _build_rank(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build RANK window function."""
        partition_by = context.get('partition_by', [])
        order_by = context.get('order_by', [(column_name, 'DESC')])

        return self.build_window_function(
            function_name="RANK()",
            partition_by=partition_by,
            order_by=order_by
        )

    def _build_dense_rank(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build DENSE_RANK window function."""
        partition_by = context.get('partition_by', [])
        order_by = context.get('order_by', [(column_name, 'DESC')])

        return self.build_window_function(
            function_name="DENSE_RANK()",
            partition_by=partition_by,
            order_by=order_by
        )

    def _build_exception_aggregation(self, column_name: str, table_name: str, context: Dict) -> str:
        """Build EXCEPTION aggregation (aggregation excluding certain values)."""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)

        # Get exception values from context
        exception_values = context.get('exception_values', [])

        if not exception_values:
            # No exceptions, just do regular aggregation
            return f"SUM({quoted_table}.{quoted_column})"

        # Build CASE WHEN to exclude exception values
        conditions = []
        for exc_val in exception_values:
            formatted_val = self._format_value(exc_val)
            conditions.append((f"{quoted_table}.{quoted_column} != {formatted_val}", f"{quoted_table}.{quoted_column}"))

        case_expr = self.build_case_when(conditions, else_value=0)
        return f"SUM({case_expr})"


# ========================================================================
# Utility Functions
# ========================================================================

def detect_aggregation_type(formula: str, aggregation_hint: str = None) -> SQLAggregationType:
    """
    Detect SQL aggregation type from formula or hint.

    Args:
        formula: SQL formula string
        aggregation_hint: Optional hint about aggregation type

    Returns:
        Detected SQL aggregation type
    """
    formula_upper = formula.upper()

    # Check hint first
    if aggregation_hint:
        hint_upper = aggregation_hint.upper()
        if "SUM" in hint_upper:
            return SQLAggregationType.SUM
        elif "COUNT" in hint_upper:
            if "DISTINCT" in hint_upper:
                return SQLAggregationType.COUNT_DISTINCT
            return SQLAggregationType.COUNT
        elif "AVG" in hint_upper or "AVERAGE" in hint_upper:
            return SQLAggregationType.AVG
        elif "MIN" in hint_upper:
            return SQLAggregationType.MIN
        elif "MAX" in hint_upper:
            return SQLAggregationType.MAX

    # Detect from formula
    if "SUM(" in formula_upper:
        return SQLAggregationType.SUM
    elif "COUNT(DISTINCT" in formula_upper:
        return SQLAggregationType.COUNT_DISTINCT
    elif "COUNT(" in formula_upper:
        return SQLAggregationType.COUNT
    elif "AVG(" in formula_upper:
        return SQLAggregationType.AVG
    elif "MIN(" in formula_upper:
        return SQLAggregationType.MIN
    elif "MAX(" in formula_upper:
        return SQLAggregationType.MAX
    elif "MEDIAN(" in formula_upper:
        return SQLAggregationType.MEDIAN
    elif "STDDEV(" in formula_upper or "STDEV(" in formula_upper:
        return SQLAggregationType.STDDEV
    elif "VARIANCE(" in formula_upper or "VAR(" in formula_upper:
        return SQLAggregationType.VARIANCE

    # Default to SUM
    return SQLAggregationType.SUM
