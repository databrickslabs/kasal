"""
Smart DAX Generator - Automatically chooses the right generator based on dependencies
"""

from typing import List
from ....base.models import KPI, KPIDefinition, DAXMeasure
from ..yaml_to_dax import DAXGenerator
from .dax_tree_parsing import TreeParsingDAXGenerator


class SmartDAXGenerator:
    """
    Automatically detects if measures have dependencies and uses the appropriate generator
    """
    
    def __init__(self):
        self.standard_generator = DAXGenerator()
        self.tree_generator = TreeParsingDAXGenerator()
    
    def generate_dax_measure(self, definition: KPIDefinition, kpi: KPI) -> DAXMeasure:
        """Generate a single DAX measure using the appropriate generator"""
        if self._has_dependencies(definition):
            # Use tree parsing generator for complex dependencies
            measures = self.tree_generator.generate_measure_with_separate_dependencies(definition, kpi.technical_name)
            # Return the target measure
            for measure in measures:
                if measure.original_kbi.technical_name == kpi.technical_name:
                    return measure
            # Fallback if not found
            return self.standard_generator.generate_dax_measure(definition, kpi)
        else:
            # Use standard generator for simple cases
            return self.standard_generator.generate_dax_measure(definition, kpi)
    
    def generate_all_measures(self, definition: KPIDefinition) -> List[DAXMeasure]:
        """Generate all measures using the appropriate approach"""
        if self._has_dependencies(definition):
            # Use tree parsing for dependency resolution
            return self.tree_generator.generate_all_measures(definition)
        else:
            # Use standard generation
            measures = []
            for kpi in definition.kpis:
                measures.append(self.standard_generator.generate_dax_measure(definition, kpi))
            return measures
    
    def generate_measures_with_dependencies(self, definition: KPIDefinition, target_measure_name: str) -> List[DAXMeasure]:
        """Generate a measure and all its dependencies as separate DAX measures"""
        return self.tree_generator.generate_measure_with_separate_dependencies(definition, target_measure_name)
    
    def _has_dependencies(self, definition: KPIDefinition) -> bool:
        """Check if any measures have CALCULATED aggregation type or dependencies"""
        # Quick check for CALCULATED type
        for kpi in definition.kpis:
            if kpi.aggregation_type == 'CALCULATED':
                return True
        
        # More thorough check for potential dependencies
        self.tree_generator.dependency_resolver.register_measures(definition)
        for measure_name, deps in self.tree_generator.dependency_resolver.dependency_graph.items():
            if deps:  # Has dependencies
                return True
        
        return False
    
    def get_generation_strategy(self, definition: KPIDefinition) -> str:
        """Get recommended generation strategy"""
        if not self._has_dependencies(definition):
            return "STANDARD"
        
        # Check complexity
        complexity = self.tree_generator.get_measure_complexity_report(definition)
        max_depth = complexity["summary"]["max_dependency_depth"]
        calculated_count = complexity["summary"]["calculated_measures"]
        
        if max_depth <= 1 and calculated_count <= 3:
            return "SIMPLE_TREE_PARSING"
        elif max_depth <= 3 and calculated_count <= 10:
            return "MODERATE_TREE_PARSING"  
        else:
            return "COMPLEX_TREE_PARSING"
    
    def get_analysis_report(self, definition: KPIDefinition) -> dict:
        """Get comprehensive analysis of the measures"""
        strategy = self.get_generation_strategy(definition)
        
        report = {
            "recommended_strategy": strategy,
            "has_dependencies": self._has_dependencies(definition)
        }
        
        if report["has_dependencies"]:
            report.update(self.tree_generator.get_dependency_analysis(definition))
            report.update({"complexity": self.tree_generator.get_measure_complexity_report(definition)})
        
        return report