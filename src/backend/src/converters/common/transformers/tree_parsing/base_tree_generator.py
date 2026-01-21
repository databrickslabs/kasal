"""
Base Tree Parsing Generator - Generic Dependency Resolution for Measure Generation

Provides abstract base class for generators that need to handle nested measure
dependencies with topological sorting and circular dependency detection.

This pattern can be used by:
- DAX generators (PowerBI)
- SQL generators
- UC Metrics generators
- Any measure generation system with calculated measures
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Set, Any, TypeVar, Generic
from ....base.models import KPI, KPIDefinition
from ...translators.dependencies import DependencyResolver

# Generic type for the output measure format
TMeasure = TypeVar('TMeasure')


class BaseTreeParsingGenerator(ABC, Generic[TMeasure]):
    """
    Abstract base class for tree parsing generators.

    This class provides the generic dependency resolution pattern:
    1. Register all measures
    2. Detect circular dependencies
    3. Generate measures in dependency order
    4. Support both inline and separate dependency modes

    Subclasses must implement format-specific generation methods.
    """

    def __init__(self):
        """Initialize dependency resolver."""
        self.dependency_resolver = DependencyResolver()

    def generate_all_measures(self, definition: KPIDefinition) -> List[TMeasure]:
        """
        Generate measures for all KPIs, resolving dependencies.

        Returns measures in dependency order (dependencies first).

        Args:
            definition: KPI definition with all measures

        Returns:
            List of generated measures in dependency order

        Raises:
            ValueError: If circular dependencies detected
        """
        # Register all measures for dependency resolution
        self.dependency_resolver.register_measures(definition)

        # Check for circular dependencies
        cycles = self.dependency_resolver.detect_circular_dependencies()
        if cycles:
            cycle_descriptions = []
            for cycle in cycles:
                cycle_descriptions.append(' -> '.join(cycle))
            raise ValueError(f"Circular dependencies detected:\n" + '\n'.join(cycle_descriptions))

        # Get measures in dependency order
        ordered_measures = self.dependency_resolver.get_dependency_order()

        measures = []
        for measure_name in ordered_measures:
            kpi = self.dependency_resolver.measure_registry[measure_name]

            if kpi.aggregation_type == 'CALCULATED':
                # For calculated measures, use calculated generation
                measure = self._generate_calculated_measure(definition, kpi)
            else:
                # For leaf measures, use standard generation
                measure = self._generate_leaf_measure(definition, kpi)

            measures.append(measure)

        return measures

    def generate_measure_with_separate_dependencies(
        self,
        definition: KPIDefinition,
        target_measure_name: str
    ) -> List[TMeasure]:
        """
        Generate a target measure along with all its dependencies as separate measures.

        This creates individual measures for each dependency rather than inlining everything.
        Useful when the target format supports measure references (like DAX, SQL CTEs).

        Args:
            definition: KPI definition
            target_measure_name: Name of the target measure to generate

        Returns:
            List of measures in dependency order (dependencies first, target last)

        Raises:
            ValueError: If target measure not found
        """
        self.dependency_resolver.register_measures(definition)

        if target_measure_name not in self.dependency_resolver.measure_registry:
            raise ValueError(f"Measure '{target_measure_name}' not found")

        # Get all dependencies for the target measure
        all_dependencies = self.dependency_resolver.get_all_dependencies(target_measure_name)
        all_dependencies.add(target_measure_name)  # Include the target itself

        # Get them in dependency order
        ordered_measures = self.dependency_resolver.get_dependency_order()
        required_measures = [m for m in ordered_measures if m in all_dependencies]

        measures = []
        for measure_name in required_measures:
            kpi = self.dependency_resolver.measure_registry[measure_name]

            if kpi.aggregation_type == 'CALCULATED':
                # For calculated measures, generate with references (not inline)
                measure = self._generate_calculated_measure_with_references(definition, kpi)
            else:
                # For leaf measures, use standard generation
                measure = self._generate_leaf_measure(definition, kpi)

            measures.append(measure)

        return measures

    def get_dependency_analysis(self, definition: KPIDefinition) -> Dict[str, Any]:
        """
        Get comprehensive dependency analysis for all measures.

        Args:
            definition: KPI definition

        Returns:
            Dictionary with dependency analysis including:
            - total_measures: Total number of measures
            - dependency_graph: Full dependency graph
            - dependency_order: Topologically sorted measure names
            - circular_dependencies: List of circular dependency cycles
            - measure_trees: Dependency tree for each measure
        """
        self.dependency_resolver.register_measures(definition)

        analysis = {
            "total_measures": len(definition.kpis),
            "dependency_graph": dict(self.dependency_resolver.dependency_graph),
            "dependency_order": self.dependency_resolver.get_dependency_order(),
            "circular_dependencies": self.dependency_resolver.detect_circular_dependencies(),
            "measure_trees": {}
        }

        # Generate dependency trees for all measures
        for kpi in definition.kpis:
            if kpi.technical_name:
                analysis["measure_trees"][kpi.technical_name] = \
                    self.dependency_resolver.get_dependency_tree(kpi.technical_name)

        return analysis

    def get_measure_complexity_report(self, definition: KPIDefinition) -> Dict[str, Any]:
        """
        Generate a complexity report for all measures.

        Analyzes dependency depth, counts leaf vs calculated measures, and
        identifies the most complex measure.

        Args:
            definition: KPI definition

        Returns:
            Dictionary with complexity metrics
        """
        self.dependency_resolver.register_measures(definition)

        report = {
            "measures": {},
            "summary": {
                "leaf_measures": 0,
                "calculated_measures": 0,
                "max_dependency_depth": 0,
                "most_complex_measure": None
            }
        }

        for kpi in definition.kpis:
            if kpi.technical_name:
                dependencies = self.dependency_resolver.get_all_dependencies(kpi.technical_name)
                depth = self._calculate_dependency_depth(kpi.technical_name)

                measure_info = {
                    "name": kpi.technical_name,
                    "description": kpi.description,
                    "type": kpi.aggregation_type or "SUM",
                    "direct_dependencies": len(
                        self.dependency_resolver.dependency_graph.get(kpi.technical_name, [])
                    ),
                    "total_dependencies": len(dependencies),
                    "dependency_depth": depth,
                    "is_leaf": len(dependencies) == 0
                }

                report["measures"][kpi.technical_name] = measure_info

                # Update summary
                if measure_info["is_leaf"]:
                    report["summary"]["leaf_measures"] += 1
                else:
                    report["summary"]["calculated_measures"] += 1

                if depth > report["summary"]["max_dependency_depth"]:
                    report["summary"]["max_dependency_depth"] = depth
                    report["summary"]["most_complex_measure"] = kpi.technical_name

        return report

    def _calculate_dependency_depth(self, measure_name: str, visited: Set[str] = None) -> int:
        """
        Calculate the maximum depth of dependencies for a measure.

        Args:
            measure_name: Name of the measure
            visited: Set of already visited measures (for cycle detection)

        Returns:
            Maximum dependency depth (0 for leaf measures)
        """
        if visited is None:
            visited = set()

        if measure_name in visited:
            return 0  # Circular dependency

        dependencies = self.dependency_resolver.dependency_graph.get(measure_name, [])
        if not dependencies:
            return 0  # Leaf measure

        visited.add(measure_name)
        max_depth = 0

        for dep in dependencies:
            depth = self._calculate_dependency_depth(dep, visited.copy())
            max_depth = max(max_depth, depth + 1)

        return max_depth

    # Abstract methods to be implemented by subclasses

    @abstractmethod
    def _generate_leaf_measure(self, definition: KPIDefinition, kpi: KPI) -> TMeasure:
        """
        Generate a leaf measure (no dependencies).

        Args:
            definition: KPI definition for context
            kpi: The leaf KPI to generate

        Returns:
            Generated measure in target format
        """
        pass

    @abstractmethod
    def _generate_calculated_measure(self, definition: KPIDefinition, kpi: KPI) -> TMeasure:
        """
        Generate a calculated measure with dependencies inlined.

        Args:
            definition: KPI definition for context
            kpi: The calculated KPI to generate

        Returns:
            Generated measure with dependencies resolved inline
        """
        pass

    @abstractmethod
    def _generate_calculated_measure_with_references(
        self,
        definition: KPIDefinition,
        kpi: KPI
    ) -> TMeasure:
        """
        Generate a calculated measure that references other measures by name.

        Used in separate dependency mode where each measure is generated
        independently and can reference others.

        Args:
            definition: KPI definition for context
            kpi: The calculated KPI to generate

        Returns:
            Generated measure with references to other measures
        """
        pass
