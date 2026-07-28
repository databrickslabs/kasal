"""
UC Metrics Helper Modules

Supporting utilities for Unity Catalog Metrics generation:
- uc_metrics_aggregations: Spark SQL aggregation builders
- uc_metrics_context: KBI context tracking for filter chains
- uc_metrics_tree_parsing: Tree parsing generator for dependency resolution
- uc_metrics_smart: Smart generator with auto-routing

Note: Tree parsing generators are NOT auto-imported to avoid circular dependencies.
Import them explicitly when needed:
    from .uc_metrics_tree_parsing import UCMetricsTreeParsingGenerator
    from .uc_metrics_smart import SmartUCMetricsGenerator
"""

from .uc_metrics_aggregations import UCMetricsAggregationBuilder
from .uc_metrics_context import UCBaseKBIContext, UCKBIContextCache

# Note: UCMetricsTreeParsingGenerator and SmartUCMetricsGenerator not auto-imported
# to avoid circular dependencies (they import from ..generator)

__all__ = [
    'UCMetricsAggregationBuilder',
    'UCBaseKBIContext',
    'UCKBIContextCache',
]
