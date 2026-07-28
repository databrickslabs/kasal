"""
Structure Expander for SAP BW Time Intelligence and Reusable Calculations

This module handles the expansion of KBIs with applied structures, creating
combined measures with names like: kbi_name + "_" + structure_name
"""

from typing import List, Dict, Tuple, Optional
import re
from ...base.models import KPI, Structure, KPIDefinition


class StructureExpander:
    """Expands KBIs with applied structures to create combined measures"""
    
    def __init__(self):
        self.processed_definitions: List[KPIDefinition] = []
    
    def process_definition(self, definition: KPIDefinition) -> KPIDefinition:
        """
        Process a KPI definition and expand KBIs with applied structures
        
        Args:
            definition: Original KPI definition with structures
            
        Returns:
            Expanded definition with combined KBI+structure measures
        """
        if not definition.structures:
            # No structures defined, return as-is
            return definition
        
        expanded_kbis = []
        
        for kpi in definition.kpis:
            if kpi.apply_structures:
                # Create combined measures for each applied structure
                combined_kbis = self._create_combined_measures(
                    kpi, definition.structures, kpi.apply_structures, definition
                )
                expanded_kbis.extend(combined_kbis)
            else:
                # No structures applied, keep original KBI
                expanded_kbis.append(kpi)

        # Create new definition with expanded KBIs
        expanded_definition = KPIDefinition(
            description=definition.description,
            technical_name=definition.technical_name,
            default_variables=definition.default_variables,
            query_filters=definition.query_filters,
            filters=definition.filters,  # Preserve filters dict for UC metrics
            structures=definition.structures,
            kpis=expanded_kbis
        )
        
        return expanded_definition
    
    def _create_combined_measures(
        self, 
        base_kbi: KPI, 
        structures: Dict[str, Structure], 
        structure_names: List[str],
        definition: KPIDefinition
    ) -> List[KPI]:
        """
        Create combined KBI+structure measures
        
        Args:
            base_kbi: Base KBI to combine with structures
            structures: Available structures dictionary
            structure_names: Names of structures to apply
            
        Returns:
            List of combined KPI measures
        """
        combined_measures = []
        
        for struct_name in structure_names:
            if struct_name not in structures:
                print(f"Warning: Structure '{struct_name}' not found, skipping")
                continue
            
            structure = structures[struct_name]
            
            # Create combined measure name: kbi_technical_name + "_" + structure_name
            base_name = base_kbi.technical_name or self._generate_technical_name(base_kbi.description)
            combined_name = f"{base_name}_{struct_name}"
            
            # Determine combined formula
            combined_formula = self._combine_formula_and_structure(base_kbi, structure, structures)
            
            # Determine aggregation type and filters based on structure formula
            if structure.formula:
                # Structure has formula - this should be a CALCULATED measure
                aggregation_type = "CALCULATED"
                # For calculated measures, only use structure filters (no base KBI data filters)
                combined_filters = list(structure.filters)
                # No source table for calculated measures
                source_table = None
            else:
                # Structure without formula - regular aggregation with combined filters
                aggregation_type = structure.aggregation_type or base_kbi.aggregation_type
                
                # Resolve structure filter variables before combining
                resolved_structure_filters = []
                if structure.filters:
                    from ..translators.filters import FilterResolver
                    filter_resolver = FilterResolver()
                    
                    # Create a temporary KPI with just structure filters to resolve them
                    temp_kpi = KPI(
                        description="temp",
                        technical_name="temp",
                        formula="temp",
                        filters=list(structure.filters)
                    )

                    # Resolve structure filters using the definition's variables
                    resolved_structure_filters = filter_resolver.resolve_filters(definition, temp_kpi)
                
                combined_filters = list(base_kbi.filters) + resolved_structure_filters
                source_table = base_kbi.source_table
            
            # Determine display sign (structure overrides KBI if specified)
            display_sign = structure.display_sign if structure.display_sign is not None else base_kbi.display_sign
            
            # Create combined measure
            combined_kpi = KPI(
                description=f"{base_kbi.description} - {structure.description}",
                formula=combined_formula,
                filters=combined_filters,
                display_sign=display_sign,
                technical_name=combined_name,
                source_table=source_table,
                aggregation_type=aggregation_type,
                weight_column=base_kbi.weight_column,
                target_column=base_kbi.target_column,
                percentile=base_kbi.percentile,
                exceptions=base_kbi.exceptions,
                exception_aggregation=base_kbi.exception_aggregation,
                fields_for_exception_aggregation=base_kbi.fields_for_exception_aggregation,
                fields_for_constant_selection=base_kbi.fields_for_constant_selection
            )

            combined_measures.append(combined_kpi)
        
        return combined_measures
    
    def _combine_formula_and_structure(
        self, 
        base_kbi: KPI, 
        structure: Structure,
        all_structures: Dict[str, Structure]
    ) -> str:
        """
        Combine base KBI formula with structure formula
        
        Args:
            base_kbi: Base KBI
            structure: Structure to apply
            all_structures: All available structures for reference resolution
            
        Returns:
            Combined formula string
        """
        if structure.formula:
            # Structure has its own formula - resolve structure references
            resolved_formula = self._resolve_structure_references(
                structure.formula, base_kbi, all_structures
            )
            return resolved_formula
        else:
            # Structure doesn't have formula - use base KBI formula
            # The structure will contribute through its filters
            return base_kbi.formula
    
    def _resolve_structure_references(
        self, 
        formula: str, 
        base_kbi: KPI,
        all_structures: Dict[str, Structure]
    ) -> str:
        """
        Resolve structure references in formula to combined measure names
        
        Example: "( act_ytd ) + ( re_ytg )" 
        becomes: "[excise_tax_actual_act_ytd] + [excise_tax_actual_re_ytg]"
        """
        base_name = base_kbi.technical_name or self._generate_technical_name(base_kbi.description)
        
        # Find structure references in parentheses
        pattern = r'\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)'
        
        def replace_reference(match):
            struct_ref = match.group(1).strip()
            if struct_ref in all_structures:
                # Convert to combined measure technical name (no brackets - let tree-parsing handle that)
                return f"{base_name}_{struct_ref}"
            else:
                # Not a structure reference, keep as-is
                return match.group(0)
        
        resolved_formula = re.sub(pattern, replace_reference, formula)
        return resolved_formula
    
    def _generate_technical_name(self, description: str) -> str:
        """Generate technical name from description"""
        # Convert to lowercase, replace spaces with underscores, remove special chars
        name = re.sub(r'[^a-zA-Z0-9\s]', '', description.lower())
        name = re.sub(r'\s+', '_', name.strip())
        return name
    
    def get_structure_dependencies(self, structures: Dict[str, Structure]) -> Dict[str, List[str]]:
        """
        Analyze structure dependencies to ensure proper processing order
        
        Returns:
            Dictionary mapping structure names to their dependencies
        """
        dependencies = {}
        
        for struct_name, structure in structures.items():
            deps = []
            if structure.formula:
                # Find structure references in the formula
                pattern = r'\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)'
                matches = re.findall(pattern, structure.formula)
                for match in matches:
                    if match in structures and match != struct_name:
                        deps.append(match)
            dependencies[struct_name] = deps
        
        return dependencies
    
    def validate_structures(self, definition: KPIDefinition) -> List[str]:
        """
        Validate structure definitions and references
        
        Returns:
            List of validation error messages
        """
        errors = []
        
        if not definition.structures:
            return errors
        
        # Check for circular dependencies
        dependencies = self.get_structure_dependencies(definition.structures)
        
        def has_circular_dependency(struct_name: str, visited: set, path: set) -> bool:
            if struct_name in path:
                return True
            if struct_name in visited:
                return False
            
            visited.add(struct_name)
            path.add(struct_name)
            
            for dep in dependencies.get(struct_name, []):
                if has_circular_dependency(dep, visited, path):
                    return True
            
            path.remove(struct_name)
            return False
        
        visited = set()
        for struct_name in definition.structures.keys():
            if struct_name not in visited:
                if has_circular_dependency(struct_name, visited, set()):
                    errors.append(f"Circular dependency detected involving structure: {struct_name}")
        
        # Check KPI structure references
        for kpi in definition.kpis:
            if kpi.apply_structures:
                for struct_name in kpi.apply_structures:
                    if struct_name not in definition.structures:
                        errors.append(f"KPI '{kpi.technical_name or kpi.description}' references undefined structure: {struct_name}")
        
        return errors


class TimeIntelligenceHelper:
    """Helper class for common SAP BW time intelligence patterns"""
    
    @staticmethod
    def create_ytd_structure() -> Structure:
        """Create Year-to-Date structure"""
        return Structure(
            description="Year to Date",
            filters=[
                "( fiscper3 < $var_current_period )",
                "( fiscyear = $var_current_year )",
                "( bic_chversion = '0000' )"  # Actuals version
            ],
            display_sign=1
        )
    
    @staticmethod
    def create_ytg_structure() -> Structure:
        """Create Year-to-Go structure"""
        return Structure(
            description="Year to Go",
            filters=[
                "( fiscper3 >= $var_current_period )",
                "( fiscyear = $var_current_year )",
                "( bic_chversion = $var_forecast_version )"
            ],
            display_sign=1
        )
    
    @staticmethod
    def create_py_structure() -> Structure:
        """Create Prior Year structure"""
        return Structure(
            description="Prior Year",
            filters=[
                "( fiscyear = $var_prior_year )",
                "( bic_chversion = '0000' )"
            ],
            display_sign=1
        )
    
    @staticmethod
    def create_act_plus_forecast_structure() -> Structure:
        """Create combined Actuals + Forecast structure"""
        return Structure(
            description="Actuals + Forecast",
            formula="( ytd_actuals ) + ( ytg_forecast )",
            display_sign=1
        )