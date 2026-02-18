"""
Tree Parsing UC Metrics Generator
Extends the generic tree parsing generator to handle UC Metrics-specific generation with dependencies
"""

import re
from typing import Dict, Any
from ....base.models import KPI, KPIDefinition
from ....common.transformers.tree_parsing import BaseTreeParsingGenerator
from ..yaml_to_uc_metrics import UCMetricsGenerator


class UCMetricsTreeParsingGenerator(BaseTreeParsingGenerator[Dict], UCMetricsGenerator):
    """
    UC Metrics Generator with tree parsing capabilities for nested measure dependencies.

    Extends both:
    - BaseTreeParsingGenerator: Provides generic dependency resolution
    - UCMetricsGenerator: Provides UC Metrics-specific generation methods

    This combination enables:
    - Dependency resolution and topological sorting (from BaseTreeParsingGenerator)
    - UC Metrics format generation (from UCMetricsGenerator)
    """

    def __init__(self, dialect: str = "spark"):
        # Initialize both parent classes
        BaseTreeParsingGenerator.__init__(self)
        UCMetricsGenerator.__init__(self, dialect=dialect)

    # Implement abstract methods from BaseTreeParsingGenerator

    def _generate_leaf_measure(self, definition: KPIDefinition, kpi: KPI) -> Dict[str, Any]:
        """
        Generate a leaf measure (no dependencies) using standard UC Metrics generation.

        Args:
            definition: KPI definition for context
            kpi: The leaf KPI to generate

        Returns:
            UC Metrics measure dict
        """
        measure_name = kpi.technical_name or "unnamed_measure"

        # Build measure expression using aggregation builder
        measure_expr = self.aggregation_builder.build_measure_expression(kpi)

        # Apply display sign if needed (SAP BW visualization property)
        if hasattr(kpi, 'display_sign') and kpi.display_sign == -1:
            measure_expr = f"(-1) * ({measure_expr})"
        elif hasattr(kpi, 'display_sign') and kpi.display_sign != 1:
            measure_expr = f"{kpi.display_sign} * ({measure_expr})"

        return {
            "name": measure_name,
            "expr": measure_expr,
            "description": kpi.description or f"Measure for {measure_name}",
        }

    def _generate_calculated_measure(self, definition: KPIDefinition, kpi: KPI) -> Dict[str, Any]:
        """
        Generate a calculated measure with dependencies inlined.

        Args:
            definition: KPI definition for context
            kpi: The calculated KPI to generate

        Returns:
            UC Metrics measure dict with dependencies resolved inline
        """
        measure_name = kpi.technical_name or "unnamed_measure"

        # Resolve dependencies inline (all dependencies expanded into single formula)
        resolved_formula = self.dependency_resolver.resolve_formula_inline(kpi.technical_name)

        # Build measure expression (UC Metrics uses SQL-like syntax)
        aggregation_type = kpi.aggregation_type.upper() if kpi.aggregation_type else "SUM"

        # For CALCULATED type, the resolved formula IS the expression
        if aggregation_type == "CALCULATED":
            measure_expr = resolved_formula
        else:
            # For other types, apply aggregation to the resolved formula
            measure_expr = self.aggregation_builder.build_measure_expression(kpi)

        # Apply display sign if needed (SAP BW visualization property)
        if hasattr(kpi, 'display_sign') and kpi.display_sign == -1:
            measure_expr = f"(-1) * ({measure_expr})"
        elif hasattr(kpi, 'display_sign') and kpi.display_sign != 1:
            measure_expr = f"{kpi.display_sign} * ({measure_expr})"

        return {
            "name": measure_name,
            "expr": measure_expr,
            "description": kpi.description or f"Calculated measure for {measure_name}",
        }

    def _generate_calculated_measure_with_references(
        self,
        definition: KPIDefinition,
        kpi: KPI
    ) -> Dict[str, Any]:
        """
        Generate a calculated measure that references other measures by name.

        Instead of inlining dependencies, this creates a measure that references
        other measures using their measure names.

        Args:
            definition: KPI definition for context
            kpi: The calculated KPI to generate

        Returns:
            UC Metrics measure dict with references to other measures
        """
        measure_name = kpi.technical_name or "unnamed_measure"

        # Get dependencies
        formula = kpi.formula
        dependencies = self.dependency_resolver.dependency_graph.get(kpi.technical_name, [])

        # Replace measure technical names with their actual measure names
        # (In UC Metrics, measures are referenced directly by name)
        resolved_formula = formula
        for dep in dependencies:
            dep_kpi = self.dependency_resolver.measure_registry[dep]
            dep_measure_name = dep_kpi.technical_name
            # Replace with measure reference (UC Metrics uses column-style references)
            resolved_formula = re.sub(
                r'\b' + re.escape(dep) + r'\b',
                dep_measure_name,
                resolved_formula
            )

        # Apply display sign if needed (SAP BW visualization property)
        if hasattr(kpi, 'display_sign') and kpi.display_sign == -1:
            resolved_formula = f"(-1) * ({resolved_formula})"
        elif hasattr(kpi, 'display_sign') and kpi.display_sign != 1:
            resolved_formula = f"{kpi.display_sign} * ({resolved_formula})"

        return {
            "name": measure_name,
            "expr": resolved_formula,
            "description": kpi.description or f"Calculated measure for {measure_name}",
        }
