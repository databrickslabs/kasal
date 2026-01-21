"""
Dependency Resolver for YAML2DAX - Tree Parsing for Nested Measures
Resolves measure dependencies and builds DAX formulas with proper nesting
"""

import re
from typing import Dict, List, Set, Optional, Tuple
from collections import deque, defaultdict
from ...base.models import KPI, KPIDefinition


class DependencyResolver:
    """Resolves dependencies between measures and handles tree parsing for nested formulas"""
    
    def __init__(self):
        self.measure_registry: Dict[str, KPI] = {}
        self.dependency_graph: Dict[str, List[str]] = defaultdict(list)
        self.resolved_cache: Dict[str, str] = {}
        
    def register_measures(self, definition: KPIDefinition):
        """Register all measures from a KPI definition for dependency resolution"""
        self.measure_registry.clear()
        self.dependency_graph.clear()
        self.resolved_cache.clear()
        
        # Build measure registry
        for kpi in definition.kpis:
            if kpi.technical_name:
                self.measure_registry[kpi.technical_name] = kpi

        # Build dependency graph
        for kpi in definition.kpis:
            if kpi.technical_name:
                dependencies = self._extract_measure_references(kpi.formula)
                self.dependency_graph[kpi.technical_name] = dependencies
    
    def _extract_measure_references(self, formula: str) -> List[str]:
        """
        Extract measure references from a formula
        
        Identifies measure names that are:
        1. Valid identifiers (letters, numbers, underscores)
        2. Not column names (don't contain table prefixes like bic_)
        3. Not DAX functions
        4. Present in the measure registry
        """
        if not formula:
            return []
        
        # Common DAX functions and operators to exclude
        dax_functions = {
            'SUM', 'COUNT', 'AVERAGE', 'MIN', 'MAX', 'CALCULATE', 'FILTER', 'IF', 'DIVIDE',
            'DISTINCTCOUNT', 'COUNTROWS', 'SUMX', 'AVERAGEX', 'MINX', 'MAXX', 'COUNTX',
            'SELECTEDVALUE', 'ISBLANK', 'REMOVEFILTERS', 'ALL', 'ALLEXCEPT', 'VALUES',
            'AND', 'OR', 'NOT', 'TRUE', 'FALSE', 'BLANK'
        }
        
        # Extract potential identifiers from the formula
        # Look for word patterns that could be measure names
        identifier_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
        potential_measures = re.findall(identifier_pattern, formula)
        
        dependencies = []
        for identifier in potential_measures:
            # Skip if it's a DAX function
            if identifier.upper() in dax_functions:
                continue
                
            # Skip if it looks like a column name (contains common prefixes)
            # But allow measure names even if they have underscores
            if identifier.startswith(('bic_', 'dim_', 'fact_')):
                continue
                
            # Skip numbers
            if identifier.isdigit():
                continue
                
            # Include if it's in our measure registry
            if identifier in self.measure_registry:
                dependencies.append(identifier)
        
        return list(set(dependencies))  # Remove duplicates
    
    def get_dependency_order(self) -> List[str]:
        """
        Get measures in dependency order using topological sort
        Returns measures ordered so that dependencies come before dependents
        """
        # Kahn's algorithm for topological sorting
        in_degree = defaultdict(int)
        
        # Calculate in-degrees
        for measure in self.measure_registry:
            in_degree[measure] = 0
        
        for measure, deps in self.dependency_graph.items():
            for dep in deps:
                in_degree[measure] += 1
        
        # Start with measures that have no dependencies
        queue = deque([measure for measure, degree in in_degree.items() if degree == 0])
        result = []
        
        while queue:
            measure = queue.popleft()
            result.append(measure)
            
            # Reduce in-degree for dependent measures
            for dependent, deps in self.dependency_graph.items():
                if measure in deps:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)
        
        # Check for circular dependencies
        if len(result) != len(self.measure_registry):
            remaining = set(self.measure_registry.keys()) - set(result)
            raise ValueError(f"Circular dependencies detected among measures: {remaining}")
        
        return result
    
    def detect_circular_dependencies(self) -> List[List[str]]:
        """Detect circular dependencies in the measure graph"""
        visited = set()
        rec_stack = set()
        cycles = []
        
        def dfs(measure, path):
            if measure in rec_stack:
                # Found a cycle
                cycle_start = path.index(measure)
                cycles.append(path[cycle_start:] + [measure])
                return
            
            if measure in visited:
                return
            
            visited.add(measure)
            rec_stack.add(measure)
            
            for dep in self.dependency_graph.get(measure, []):
                dfs(dep, path + [measure])
            
            rec_stack.remove(measure)
        
        for measure in self.measure_registry:
            if measure not in visited:
                dfs(measure, [])
        
        return cycles
    
    def resolve_formula_inline(self, measure_name: str, max_depth: int = 5) -> str:
        """
        Resolve a measure formula by inlining all dependencies
        
        Args:
            measure_name: Name of the measure to resolve
            max_depth: Maximum recursion depth to prevent infinite loops
            
        Returns:
            Formula with all measure references replaced by their DAX expressions
        """
        if measure_name in self.resolved_cache:
            return self.resolved_cache[measure_name]
        
        if measure_name not in self.measure_registry:
            raise ValueError(f"Measure '{measure_name}' not found in registry")
        
        return self._resolve_recursive(measure_name, set(), max_depth)
    
    def _resolve_recursive(self, measure_name: str, visited: Set[str], max_depth: int) -> str:
        """Recursively resolve measure dependencies"""
        if max_depth <= 0:
            raise ValueError(f"Maximum recursion depth reached while resolving '{measure_name}'")
        
        if measure_name in visited:
            raise ValueError(f"Circular dependency detected: {' -> '.join(visited)} -> {measure_name}")
        
        measure = self.measure_registry[measure_name]
        formula = measure.formula
        
        # Get dependencies for this measure
        dependencies = self.dependency_graph.get(measure_name, [])
        
        if not dependencies:
            # No dependencies - this is a leaf measure, return its DAX
            resolved_dax = self._generate_leaf_measure_dax(measure)
            self.resolved_cache[measure_name] = resolved_dax
            return resolved_dax
        
        # Resolve each dependency
        visited_copy = visited.copy()
        visited_copy.add(measure_name)
        
        resolved_formula = formula
        for dep in dependencies:
            dep_dax = self._resolve_recursive(dep, visited_copy, max_depth - 1)
            # Replace the dependency name with its resolved DAX (wrapped in parentheses)
            resolved_formula = re.sub(r'\b' + re.escape(dep) + r'\b', f'({dep_dax})', resolved_formula)
        
        self.resolved_cache[measure_name] = resolved_formula
        return resolved_formula
    
    def _generate_leaf_measure_dax(self, measure: KPI) -> str:
        """Generate DAX for a leaf measure (no dependencies)"""
        # For inline resolution, we need to generate a complete DAX expression
        # This is a simplified version that just returns the base aggregation
        # The full DAX generation with filters should be handled by the main generator
        from ...services.powerbi.helpers.dax_aggregations import detect_and_build_aggregation
        
        # Create KPI definition dict for the aggregation system
        kbi_dict = {
            'formula': measure.formula,
            'source_table': measure.source_table,
            'aggregation_type': measure.aggregation_type,
            'weight_column': measure.weight_column,
            'target_column': measure.target_column,
            'percentile': measure.percentile,
            'exceptions': measure.exceptions or [],
            'display_sign': measure.display_sign,
            'exception_aggregation': measure.exception_aggregation,
            'fields_for_exception_aggregation': measure.fields_for_exception_aggregation or [],
            'fields_for_constant_selection': measure.fields_for_constant_selection or []
        }
        
        # Generate the base DAX using the existing aggregation system
        return detect_and_build_aggregation(kbi_dict)
    
    def get_dependency_tree(self, measure_name: str) -> Dict:
        """Get the full dependency tree for a measure"""
        if measure_name not in self.measure_registry:
            raise ValueError(f"Measure '{measure_name}' not found in registry")
        
        def build_tree(name: str, visited: Set[str]) -> Dict:
            if name in visited:
                return {"name": name, "circular": True, "dependencies": []}
            
            visited_copy = visited.copy()
            visited_copy.add(name)
            
            dependencies = self.dependency_graph.get(name, [])
            tree = {
                "name": name,
                "description": self.measure_registry[name].description,
                "formula": self.measure_registry[name].formula,
                "dependencies": [build_tree(dep, visited_copy) for dep in dependencies]
            }
            
            return tree
        
        return build_tree(measure_name, set())
    
    def get_all_dependencies(self, measure_name: str) -> Set[str]:
        """Get all transitive dependencies for a measure"""
        if measure_name not in self.measure_registry:
            return set()
        
        all_deps = set()
        queue = deque([measure_name])
        visited = set()
        
        while queue:
            current = queue.popleft()
            if current in visited:
                continue
                
            visited.add(current)
            deps = self.dependency_graph.get(current, [])
            
            for dep in deps:
                if dep not in all_deps:
                    all_deps.add(dep)
                    queue.append(dep)
        
        return all_deps