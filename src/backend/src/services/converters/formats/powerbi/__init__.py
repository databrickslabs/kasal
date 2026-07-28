"""
PowerBI Service

Centralized service for PowerBI operations with clear DAX handling:

Components:
- authentication.py: Azure AD authentication (AadService)
- connector.py: PowerBI connection and data extraction (PowerBIConnector)
- dax_parser.py: DAX expression parsing and tokenization
- dax_to_sql.py: DAX → SQL transpilation (PowerBI extraction)
- yaml_to_dax.py: YAML → DAX generation (measure creation)
- helpers/: Supporting modules for advanced DAX operations

Workflows:
1. **PowerBI → SQL**: PowerBI → Extract DAX → dax_parser → dax_to_sql → Transpiled SQL
2. **YAML → DAX**: YAML KPI → yaml_to_dax (DAXGenerator) → PowerBI DAX measures
"""

# Authentication
from .authentication import AadService

# Connection & Extraction
from .connector import PowerBIConnector

# DAX Parser - Parsing and tokenization
from .dax_parser import (
    DAXExpressionParser,
    DaxToken,
)

# DAX Transpilation - DAX → SQL
from .dax_to_sql import DaxToSqlTranspiler

# DAX Generation - YAML → DAX
from .yaml_to_dax import DAXGenerator

# Helper modules (for advanced usage)
from .helpers.dax_aggregations import (
    AggregationType,
    AggregationDetector,
    DAXAggregationBuilder,
    ExceptionAggregationHandler,
    detect_and_build_aggregation,
)
from .helpers.dax_context import DAXBaseKBIContext, DAXKBIContextCache
from .helpers.dax_syntax_converter import DaxSyntaxConverter

# Advanced generators (imported directly to avoid circular dependencies)
from .helpers.dax_smart import SmartDAXGenerator
from .helpers.dax_tree_parsing import TreeParsingDAXGenerator

__all__ = [
    # ===== AUTHENTICATION =====
    'AadService',

    # ===== CONNECTION & EXTRACTION =====
    'PowerBIConnector',

    # ===== DAX PARSER (Consolidated) =====
    # Parsing & Transpilation
    'DAXExpressionParser',
    'DaxToken',
    'DaxToSqlTranspiler',  # DAX → SQL transpilation

    # YAML to DAX Generation
    'DAXGenerator',

    # ===== HELPERS (Advanced) =====
    # Aggregations
    'AggregationType',
    'AggregationDetector',
    'DAXAggregationBuilder',
    'ExceptionAggregationHandler',
    'detect_and_build_aggregation',

    # Context
    'DAXBaseKBIContext',
    'DAXKBIContextCache',

    # Syntax & Parsing
    'DaxSyntaxConverter',
    'SmartDAXGenerator',
    'TreeParsingDAXGenerator',
]
