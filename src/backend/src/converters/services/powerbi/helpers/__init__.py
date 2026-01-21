"""
PowerBI DAX Helper Modules

Supporting modules for DAX operations (used by parser.py):
- Base DAX generator (YAML to DAX)
- Tree parsing for dependency resolution
- Smart generator selection based on complexity
- Context tracking for filters and aggregations
- Syntax conversion utilities
- Aggregation expression builders

HELPER ARCHITECTURE - Why These Modules Exist
==============================================

These helpers extend the base DAX parsing capabilities with advanced features:

1. dax_generator.py - DAXGenerator (BASE)
   - Generates DAX from YAML for simple measures
   - Handles: SUM, AVG, COUNT, CALCULATED aggregations
   - Limitation: Cannot resolve measure-to-measure dependencies
   - Use case: Leaf measures that aggregate columns directly

2. dax_tree_parsing.py - TreeParsingDAXGenerator (EXTENDS DAXGenerator)
   - Resolves nested measure dependencies using DependencyResolver
   - Detects circular dependencies
   - Generates measures in topological order (dependencies first)
   - Two generation modes:
     * generate_all_measures(): Inline dependencies (single formula)
     * generate_measure_with_separate_dependencies(): Create separate measures
   - Use case: CALCULATED measures that reference other measures

3. dax_smart.py - SmartDAXGenerator (USES BOTH)
   - **RECOMMENDED: Use this for all general cases**
   - Automatically detects if measures have dependencies
   - Routes to DAXGenerator for simple cases
   - Routes to TreeParsingDAXGenerator for complex cases
   - Provides analysis: get_analysis_report(), get_generation_strategy()
   - Use case: Any YAML file (automatically handles both simple and complex)

4. dax_context.py - Context Tracking
   - Tracks base KBI contexts and their parent chains
   - Used for dependency tree building
   - Mirrors SQL pattern for filter/aggregation context

5. dax_syntax_converter.py - DaxSyntaxConverter
   - Converts SQL-style syntax to DAX syntax
   - Handles CASE WHEN → SWITCH, IN → {values}, etc.

6. dax_aggregations.py - Enhanced Aggregation System
   - detect_and_build_aggregation(): Main entry point
   - Handles: Exception aggregation, weighted avg, percentiles
   - 20+ aggregation types with special case handling

USAGE EXAMPLE
=============

For a YAML with calculated measures (aggregation_type: "CALCULATED"):

```python
from src.converters.services.powerbi.helpers import SmartDAXGenerator
from src.converters.base.models import KPIDefinition

# Load YAML
definition = KPIDefinition.from_yaml("measures.yaml")

# Use SmartDAXGenerator (recommended)
smart_gen = SmartDAXGenerator()

# Option 1: Generate all measures (auto-detects dependencies)
all_measures = smart_gen.generate_all_measures(definition)

# Option 2: Generate specific measure with its dependencies
measures = smart_gen.generate_measures_with_dependencies(
    definition,
    "total_taxes_in_gross_revenue"
)

# Option 3: Get analysis report
analysis = smart_gen.get_analysis_report(definition)
print(f"Strategy: {analysis['recommended_strategy']}")
print(f"Has dependencies: {analysis['has_dependencies']}")
```

IMPORT NOTE
===========

DAXGenerator is defined here in helpers/dax_generator.py but is also imported
and re-exported by parser.py to maintain a single entry point for external code.
This avoids circular imports while keeping the architecture clean.

External code should import from parser.py:
```python
from src.converters.services.powerbi.parser import DAXGenerator
```

Internal code (within helpers) imports directly:
```python
from .dax_generator import DAXGenerator
```
"""

# Note: DAXGenerator and DaxToSqlTranspiler have been moved to powerbi root
# Import them from parent module instead:
# from ..yaml_to_dax import DAXGenerator
# from ..dax_to_sql import DaxToSqlTranspiler

# Note: TreeParsingDAXGenerator and SmartDAXGenerator are NOT auto-imported
# to avoid circular dependencies (they depend on DAXGenerator from yaml_to_dax.py).
# Import them explicitly when needed:
# from .dax_tree_parsing import TreeParsingDAXGenerator
# from .dax_smart import SmartDAXGenerator

from .dax_context import DAXBaseKBIContext, DAXKBIContextCache
from .dax_syntax_converter import DaxSyntaxConverter
from .dax_aggregations import (
    AggregationType,
    AggregationDetector,
    DAXAggregationBuilder,
    ExceptionAggregationHandler,
    detect_and_build_aggregation,
)

__all__ = [
    # Context
    'DAXBaseKBIContext',
    'DAXKBIContextCache',

    # Syntax conversion
    'DaxSyntaxConverter',

    # Aggregations
    'AggregationType',
    'AggregationDetector',
    'DAXAggregationBuilder',
    'ExceptionAggregationHandler',
    'detect_and_build_aggregation',

    # Note: TreeParsingDAXGenerator and SmartDAXGenerator should be imported directly:
    # from .helpers.dax_tree_parsing import TreeParsingDAXGenerator
    # from .helpers.dax_smart import SmartDAXGenerator
]
