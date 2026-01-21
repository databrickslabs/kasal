import re
from typing import Dict, List
from ...base.models import KPI, KPIDefinition


class FormulaTranslator:
    def __init__(self):
        # Common SAP BW field patterns to DAX aggregation mapping
        self.aggregation_mappings = {
            'volume': 'SUM',
            'amount': 'SUM',
            'quantity': 'SUM',
            'count': 'COUNT',
            'avg': 'AVERAGE',
            'max': 'MAX',
            'min': 'MIN',
            'kvolume': 'SUM',  # SAP BW key figure for volume
            'kamount': 'SUM',  # SAP BW key figure for amount
        }
        
        # Pattern to extract table and column information
        self.field_pattern = re.compile(r'bic_([a-zA-Z0-9_]+)')

    def translate_formula(self, kpi: KPI, definition: KPIDefinition) -> Dict[str, str]:
        """Translate a KPI formula to DAX components."""
        formula = kpi.formula.lower()
        
        # Extract the technical field name
        technical_field = kpi.formula
        if technical_field.startswith('bic_'):
            technical_field = technical_field
        else:
            technical_field = f'bic_{technical_field}'
        
        # Determine aggregation function
        aggregation = self._determine_aggregation(formula)
        
        # Use source_table if specified, otherwise generate table name
        if kpi.source_table:
            table_name = kpi.source_table
        else:
            # Generate table name from field (fallback approach)
            table_name = self._generate_table_name(technical_field)
        
        # Clean column name
        column_name = technical_field
        
        return {
            'aggregation': aggregation,
            'table_name': table_name,
            'column_name': column_name,
            'technical_field': technical_field
        }
    
    def _determine_aggregation(self, formula: str) -> str:
        """Determine the appropriate DAX aggregation function."""
        formula_lower = formula.lower()
        
        # Check for explicit aggregation hints in the formula
        for keyword, aggregation in self.aggregation_mappings.items():
            if keyword in formula_lower:
                return aggregation
        
        # Default to SUM for most business metrics
        return 'SUM'
    
    def _generate_table_name(self, field_name: str) -> str:
        """Generate a table name from the field name."""
        # Remove bic_ prefix and create a proper table name
        if field_name.startswith('bic_'):
            base_name = field_name[4:]  # Remove 'bic_' prefix
        else:
            base_name = field_name
        
        # Convert to proper case for table name
        # Example: kvolume_c -> Volume
        parts = base_name.split('_')
        if parts:
            # Take the first meaningful part
            main_part = parts[0]
            # Capitalize and clean up
            if main_part.startswith('k'):
                # SAP BW key figures often start with 'k'
                main_part = main_part[1:]
            
            return main_part.capitalize() + 'Data'
        
        return 'FactTable'

    def create_measure_name(self, kpi: KPI, definition: KPIDefinition) -> str:
        """Create a clean measure name from KPI description."""
        if kpi.description:
            # Clean up the description for use as measure name
            clean_name = re.sub(r'[^\w\s]', '', kpi.description)
            clean_name = re.sub(r'\s+', ' ', clean_name).strip()
            return clean_name
        
        # Fallback to technical name or formula
        if kpi.technical_name:
            return kpi.technical_name.replace('_', ' ').title()
        
        # Last resort: use formula
        return kpi.formula.replace('bic_', '').replace('_', ' ').title()
    
    def get_field_metadata(self, field_name: str) -> Dict[str, str]:
        """Extract metadata from SAP BW field names."""
        metadata = {
            'original_field': field_name,
            'clean_name': field_name,
            'data_type': 'DECIMAL',
            'category': 'Measure'
        }
        
        if field_name.startswith('bic_'):
            clean_name = field_name[4:]
            metadata['clean_name'] = clean_name
            
            # Determine if it's a key figure or characteristic
            if any(prefix in clean_name for prefix in ['k', 'amount', 'volume', 'qty']):
                metadata['category'] = 'Measure'
                metadata['data_type'] = 'DECIMAL'
            else:
                metadata['category'] = 'Dimension'
                metadata['data_type'] = 'STRING'
        
        return metadata