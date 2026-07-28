"""
SQL Service

Generates SQL queries and definitions from KPI objects.
Supports multiple SQL dialects (Spark, Trino, Snowflake, etc.)

Architecture:
- yaml_to_sql.py: YAML → SQL converter (core generator)
- models.py: SQL data models and dialect definitions
- helpers/: Supporting utilities
  - sql_aggregations.py: SQL aggregation builders for various dialects
  - sql_context.py: Context tracking for filter chains
  - sql_expression_builder.py: Centralized SQL expression generation
  - sql_structures.py: SAP BW structure processor for time intelligence

Workflow:
- **YAML → SQL**: YAML KPI → yaml_to_sql → SQL queries (for various dialects)
"""

from .yaml_to_sql import SQLGenerator
from .models import SQLDialect, SQLTranslationOptions, SQLMeasure
from .helpers.sql_expression_builder import SQLExpressionEngine, detect_aggregation_type

__all__ = [
    'SQLGenerator',
    'SQLDialect',
    'SQLTranslationOptions',
    'SQLMeasure',
    'SQLExpressionEngine',
    'detect_aggregation_type',
]
