"""
Unity Catalog Metrics Service

Generates Unity Catalog Metrics Store definitions from KPI objects.

Architecture:
- yaml_to_uc_metrics.py: YAML → UC Metrics converter (core generator)
- uc_metrics_to_sql.py: UC Metrics → SQL transpilation (extraction)
- authentication.py: Databricks authentication service
- connector.py: Databricks Unity Catalog connector
- helpers/: Supporting utilities
  - uc_metrics_aggregations.py: Spark SQL aggregation builders
  - uc_metrics_context.py: KBI context tracking for filter chains
  - uc_metrics_tree_parsing.py: Tree parsing generator for dependency resolution
  - uc_metrics_smart.py: Smart generator with auto-routing

Workflows:
1. **YAML → UC Metrics**: YAML KPI → yaml_to_uc_metrics → UC Metrics YAML (for Databricks)
2. **UC Metrics → SQL**: UC Metrics YAML → uc_metrics_to_sql → Standalone SQL queries

Note: Tree parsing generators are NOT auto-imported to avoid circular dependencies.
Import them explicitly when needed:
    from .helpers.uc_metrics_tree_parsing import UCMetricsTreeParsingGenerator
    from .helpers.uc_metrics_smart import SmartUCMetricsGenerator
"""

from .yaml_to_uc_metrics import UCMetricsGenerator
from .uc_metrics_to_sql import UCMetricsToSqlTranspiler
from .authentication import DatabricksAuthService
from .connector import DatabricksConnector
from .helpers.uc_metrics_aggregations import UCMetricsAggregationBuilder
from .helpers.uc_metrics_context import UCBaseKBIContext, UCKBIContextCache

# Note: UCMetricsTreeParsingGenerator and SmartUCMetricsGenerator not auto-imported
# to avoid circular dependencies

__all__ = [
    'UCMetricsGenerator',
    'UCMetricsToSqlTranspiler',
    'DatabricksAuthService',
    'DatabricksConnector',
    'UCMetricsAggregationBuilder',
    'UCBaseKBIContext',
    'UCKBIContextCache',
]
