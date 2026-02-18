"""
Tree Parsing DAX Generator
Extends the generic tree parsing generator to handle DAX-specific measure generation
"""

import re
from ....base.models import KPI, KPIDefinition, DAXMeasure
from ....common.transformers.tree_parsing import BaseTreeParsingGenerator
from ..yaml_to_dax import DAXGenerator


class TreeParsingDAXGenerator(BaseTreeParsingGenerator[DAXMeasure], DAXGenerator):
    """
    DAX Generator with tree parsing capabilities for nested measure dependencies.

    Extends both:
    - BaseTreeParsingGenerator: Provides generic dependency resolution
    - DAXGenerator: Provides DAX-specific generation methods

    This combination enables:
    - Dependency resolution and topological sorting (from BaseTreeParsingGenerator)
    - DAX formula generation and syntax handling (from DAXGenerator)
    """

    def __init__(self):
        # Initialize both parent classes
        BaseTreeParsingGenerator.__init__(self)
        DAXGenerator.__init__(self)

    # Implement abstract methods from BaseTreeParsingGenerator

    def _generate_leaf_measure(self, definition: KPIDefinition, kpi: KPI) -> DAXMeasure:
        """
        Generate a leaf measure (no dependencies) using standard DAX generation.

        Args:
            definition: KPI definition for context
            kpi: The leaf KPI to generate

        Returns:
            DAX measure
        """
        return self.generate_dax_measure(definition, kpi)

    def _generate_calculated_measure(self, definition: KPIDefinition, kpi: KPI) -> DAXMeasure:
        """
        Generate a calculated measure with dependencies inlined.

        Args:
            definition: KPI definition for context
            kpi: The calculated KPI to generate

        Returns:
            DAX measure with dependencies resolved inline
        """
        measure_name = self.formula_translator.create_measure_name(kpi, definition)

        # Resolve dependencies inline (all dependencies expanded into single formula)
        resolved_formula = self.dependency_resolver.resolve_formula_inline(kpi.technical_name)

        # Apply filters and constant selection if specified
        resolved_filters = self.filter_resolver.resolve_filters(definition, kpi)
        dax_formula = self._add_filters_to_dax(
            resolved_formula,
            resolved_filters,
            kpi.source_table or 'Table',
            kpi
        )

        # Apply display sign if needed (SAP BW visualization property)
        if hasattr(kpi, 'display_sign') and kpi.display_sign == -1:
            dax_formula = f"-1 * ({dax_formula})"
        elif hasattr(kpi, 'display_sign') and kpi.display_sign != 1:
            dax_formula = f"{kpi.display_sign} * ({dax_formula})"

        return DAXMeasure(
            name=measure_name,
            description=kpi.description or f"Calculated measure for {measure_name}",
            dax_formula=dax_formula,
            original_kbi=kpi
        )

    def _generate_calculated_measure_with_references(
        self,
        definition: KPIDefinition,
        kpi: KPI
    ) -> DAXMeasure:
        """
        Generate a calculated measure that references other measures by name.

        Instead of inlining dependencies, this creates a measure that references
        other measures using DAX's [Measure Name] syntax.

        Args:
            definition: KPI definition for context
            kpi: The calculated KPI to generate

        Returns:
            DAX measure with references to other measures
        """
        measure_name = self.formula_translator.create_measure_name(kpi, definition)

        # Get dependencies
        formula = kpi.formula
        dependencies = self.dependency_resolver.dependency_graph.get(kpi.technical_name, [])

        # Replace measure names with DAX measure references
        resolved_formula = formula
        for dep in dependencies:
            dep_kbi = self.dependency_resolver.measure_registry[dep]
            dep_measure_name = self.formula_translator.create_measure_name(dep_kbi, definition)
            # Replace with proper DAX measure reference
            resolved_formula = re.sub(
                r'\b' + re.escape(dep) + r'\b',
                f'[{dep_measure_name}]',
                resolved_formula
            )

        # Apply filters and constant selection if specified
        resolved_filters = self.filter_resolver.resolve_filters(definition, kpi)
        dax_formula = self._add_filters_to_dax(
            resolved_formula,
            resolved_filters,
            kpi.source_table or 'Table',
            kpi
        )

        # Apply display sign if needed (SAP BW visualization property)
        if hasattr(kpi, 'display_sign') and kpi.display_sign == -1:
            dax_formula = f"-1 * ({dax_formula})"
        elif hasattr(kpi, 'display_sign') and kpi.display_sign != 1:
            dax_formula = f"{kpi.display_sign} * ({dax_formula})"

        return DAXMeasure(
            name=measure_name,
            description=kpi.description or f"Calculated measure for {measure_name}",
            dax_formula=dax_formula,
            original_kbi=kpi
        )
