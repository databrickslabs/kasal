"""
Smart UC Metrics Generator
Auto-routes between basic and tree parsing generators based on complexity
"""

from typing import Dict, Any, List
from ....base.models import KPIDefinition
from ..yaml_to_uc_metrics import UCMetricsGenerator
from .uc_metrics_tree_parsing import UCMetricsTreeParsingGenerator


class SmartUCMetricsGenerator:
    """
    Intelligent UC Metrics generator that automatically selects the appropriate strategy.

    Analyzes the KPI definition and routes to:
    - UCMetricsGenerator: For simple measures without dependencies
    - UCMetricsTreeParsingGenerator: For CALCULATED measures with dependencies

    This provides the best of both worlds:
    - Performance: Simple measures use lightweight generation
    - Capability: Complex measures get full dependency resolution
    """

    def __init__(self, dialect: str = "spark"):
        self.dialect = dialect
        self.basic_generator = UCMetricsGenerator(dialect=dialect)
        self.tree_generator = UCMetricsTreeParsingGenerator(dialect=dialect)

    def has_dependencies(self, definition: KPIDefinition) -> bool:
        """
        Check if the definition has any CALCULATED measures with dependencies.

        Args:
            definition: KPI definition to analyze

        Returns:
            True if there are CALCULATED measures that might reference others
        """
        for kpi in definition.kpis:
            if kpi.aggregation_type and kpi.aggregation_type.upper() == "CALCULATED":
                # CALCULATED measures might have dependencies
                return True
        return False

    def get_recommended_strategy(self, definition: KPIDefinition) -> str:
        """
        Analyze the definition and recommend a generation strategy.

        Args:
            definition: KPI definition to analyze

        Returns:
            Either "basic" or "tree_parsing"
        """
        if self.has_dependencies(definition):
            return "tree_parsing"
        return "basic"

    def generate_consolidated_uc_metrics(
        self,
        definition: KPIDefinition,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate consolidated UC Metrics with automatic strategy selection.

        Args:
            definition: KPI definition
            metadata: Metadata dict with name, catalog, schema

        Returns:
            UC Metrics dict in consolidated format
        """
        strategy = self.get_recommended_strategy(definition)

        if strategy == "tree_parsing":
            # Use tree parsing for dependency resolution
            # Register measures for dependency resolution
            self.tree_generator.dependency_resolver.register_measures(definition)

            # Check for circular dependencies
            cycles = self.tree_generator.dependency_resolver.detect_circular_dependencies()
            if cycles:
                cycle_descriptions = [' -> '.join(cycle) for cycle in cycles]
                raise ValueError(f"Circular dependencies detected:\n" + '\n'.join(cycle_descriptions))

            # Generate all measures with dependencies resolved
            measures = self.tree_generator.generate_all_measures(definition)

            # Convert to consolidated UC Metrics format
            return self._build_consolidated_format(measures, definition, metadata)
        else:
            # Use basic generator for simple cases
            return self.basic_generator.generate_consolidated_uc_metrics(
                definition.kpis,
                metadata
            )

    def _build_consolidated_format(
        self,
        measures: List[Dict[str, Any]],
        definition: KPIDefinition,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Build consolidated UC Metrics format from generated measures.

        Args:
            measures: List of measure dicts from tree parsing
            definition: Original KPI definition
            metadata: Metadata dict

        Returns:
            Consolidated UC Metrics dict
        """
        # Determine source table from first KPI with source_table
        source_table = None
        for kpi in definition.kpis:
            if kpi.source_table:
                source_table = kpi.source_table
                break

        if not source_table:
            source_table = "default_table"

        # Build fully qualified source reference
        catalog = metadata.get("catalog", "main")
        schema = metadata.get("schema", "default")

        if '.' in source_table:
            source = source_table
        else:
            source = f"{catalog}.{schema}.{source_table}"

        # Build UC Metrics structure
        uc_metrics = {
            "version": "0.1",
            "description": f"UC metrics store definition for \"{metadata.get('name', 'measures')}\"",
            "source": source,
            "measures": measures
        }

        # Add common filters if present
        # (Tree parsing doesn't currently handle this - could be enhanced)

        return uc_metrics

    def get_analysis_report(self, definition: KPIDefinition) -> Dict[str, Any]:
        """
        Get analysis report for the KPI definition.

        Args:
            definition: KPI definition to analyze

        Returns:
            Analysis report dict
        """
        strategy = self.get_recommended_strategy(definition)
        has_deps = self.has_dependencies(definition)

        report = {
            "total_measures": len(definition.kpis),
            "has_dependencies": has_deps,
            "recommended_strategy": strategy,
            "calculated_measures": sum(
                1 for kpi in definition.kpis
                if kpi.aggregation_type and kpi.aggregation_type.upper() == "CALCULATED"
            ),
            "simple_measures": sum(
                1 for kpi in definition.kpis
                if not kpi.aggregation_type or kpi.aggregation_type.upper() != "CALCULATED"
            ),
        }

        # Add dependency analysis if using tree parsing
        if strategy == "tree_parsing":
            self.tree_generator.dependency_resolver.register_measures(definition)
            report["dependency_analysis"] = self.tree_generator.get_dependency_analysis(definition)

        return report
