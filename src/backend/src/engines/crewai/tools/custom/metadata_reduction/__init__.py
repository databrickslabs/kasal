"""
Power BI Metadata Reduction package.

Provides intelligent metadata reduction for Power BI semantic models:
- FuzzyScorer: Fuzzy matching of question tokens against model elements
- MeasureDependencyResolver: DAX expression dependency graph resolution
- ValueNormalizer: Filter value normalization against column values
"""

from .fuzzy_scorer import FuzzyScorer
from .dependency_resolver import MeasureDependencyResolver
from .value_normalizer import ValueNormalizer

__all__ = ["FuzzyScorer", "MeasureDependencyResolver", "ValueNormalizer"]
