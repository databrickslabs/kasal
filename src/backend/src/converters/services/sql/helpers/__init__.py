"""
SQL Service Helper Modules

Supporting utilities for SQL generation:
- sql_aggregations.py: SQL aggregation builders for various dialects
- sql_context.py: Context tracking for filter chains and KBI dependencies
- sql_expression_builder.py: Centralized SQL expression generation engine
- sql_structures.py: SAP BW structure processor for time intelligence

These modules provide the core building blocks for SQL query generation
across multiple SQL dialects (Spark, Snowflake, PostgreSQL, etc.).
"""

from .sql_aggregations import SQLAggregationBuilder
from .sql_context import SQLBaseKBIContext, SQLKBIContextCache
from .sql_expression_builder import SQLExpressionEngine, detect_aggregation_type
from .sql_structures import SQLStructureExpander

__all__ = [
    'SQLAggregationBuilder',
    'SQLBaseKBIContext',
    'SQLKBIContextCache',
    'SQLExpressionEngine',
    'detect_aggregation_type',
    'SQLStructureExpander',
]
