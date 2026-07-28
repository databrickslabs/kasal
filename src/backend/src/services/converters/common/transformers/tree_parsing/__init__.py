"""
Tree Parsing - Generic Dependency Resolution for Measure Generation

Provides base classes and utilities for generators that need to handle
nested measure dependencies with topological sorting and circular
dependency detection.

This module enables consistent dependency handling across:
- DAX generation (PowerBI)
- SQL generation
- UC Metrics generation
- Any measure generation system with calculated measures
"""

from .base_tree_generator import BaseTreeParsingGenerator

__all__ = [
    'BaseTreeParsingGenerator',
]
