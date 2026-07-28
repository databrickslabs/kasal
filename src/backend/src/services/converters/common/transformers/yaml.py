import yaml
from pathlib import Path
from typing import List, Dict, Any, Union
from ...base.models import KPI, QueryFilter, Structure, KPIDefinition


class YAMLKPIParser:
    def __init__(self):
        self.parsed_definitions: List[KPIDefinition] = []
    
    def parse_file(self, file_path: Union[str, Path]) -> KPIDefinition:
        """Parse a single YAML file containing KPI definitions."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"YAML file not found: {file_path}")
        
        with open(path, 'r', encoding='utf-8') as file:
            data = yaml.safe_load(file)
        
        return self._parse_yaml_data(data)
    
    def parse_directory(self, directory_path: Union[str, Path]) -> List[KPIDefinition]:
        """Parse all YAML files in a directory."""
        path = Path(directory_path)
        if not path.is_dir():
            raise NotADirectoryError(f"Directory not found: {directory_path}")
        
        definitions = []
        for yaml_file in path.glob("*.yaml"):
            try:
                definition = self.parse_file(yaml_file)
                definitions.append(definition)
            except Exception as e:
                print(f"Error parsing {yaml_file}: {e}")
        
        for yaml_file in path.glob("*.yml"):
            try:
                definition = self.parse_file(yaml_file)
                definitions.append(definition)
            except Exception as e:
                print(f"Error parsing {yaml_file}: {e}")
        
        self.parsed_definitions = definitions
        return definitions
    
    def _parse_yaml_data(self, data: Dict[str, Any]) -> KPIDefinition:
        """Convert raw YAML data to KPIDefinition model."""
        # Parse query filters
        query_filters = []
        if 'filters' in data and 'query_filter' in data['filters']:
            for name, expression in data['filters']['query_filter'].items():
                query_filters.append(QueryFilter(name=name, expression=expression))
        
        # Parse structures for time intelligence
        structures = None
        if 'structures' in data:
            structures = {}
            for struct_name, struct_data in data['structures'].items():
                # Debug: check what filter data we're getting from YAML
                filter_data = struct_data.get('filter', [])
                with open('/tmp/sql_debug.log', 'a') as f:
                    f.write(f"YAML Parser - Structure {struct_name}: raw filter data = {filter_data}\n")

                # Create structure - bypass constructor to avoid Pydantic alias issues
                structure = Structure.model_validate({
                    'description': struct_data.get('description', ''),
                    'formula': struct_data.get('formula'),
                    'filter': filter_data,  # Use the alias name 'filter'
                    'display_sign': struct_data.get('display_sign', 1),
                    'technical_name': struct_data.get('technical_name'),
                    'aggregation_type': struct_data.get('aggregation_type'),
                    'variables': struct_data.get('variables')
                })

                with open('/tmp/sql_debug.log', 'a') as f:
                    f.write(f"YAML Parser - Structure {struct_name}: created with filters = {structure.filters}\n")

                structures[struct_name] = structure
        
        # Parse KBIs
        kbis = []
        if 'kbi' in data:
            for kbi_data in data['kbi']:
                kbi = KPI(
                    description=kbi_data.get('description', ''),
                    formula=kbi_data.get('formula', ''),
                    filters=kbi_data.get('filter', []),
                    display_sign=kbi_data.get('display_sign', 1),
                    technical_name=kbi_data.get('technical_name'),
                    source_table=kbi_data.get('source_table'),
                    aggregation_type=kbi_data.get('aggregation_type'),
                    weight_column=kbi_data.get('weight_column'),
                    target_column=kbi_data.get('target_column'),
                    percentile=kbi_data.get('percentile'),
                    exceptions=kbi_data.get('exceptions'),
                    exception_aggregation=kbi_data.get('exception_aggregation'),
                    fields_for_exception_aggregation=kbi_data.get('fields_for_exception_aggregation'),
                    fields_for_constant_selection=kbi_data.get('fields_for_constant_selection'),
                    apply_structures=kbi_data.get('apply_structures')
                )
                kbis.append(kbi)
        
        return KPIDefinition(
            description=data.get('description', ''),
            technical_name=data.get('technical_name', ''),
            default_variables=data.get('default_variables', {}),
            query_filters=query_filters,
            filters=data.get('filters'),  # Pass raw filters data for SQL processing
            structures=structures,
            kpis=kbis
        )
    
    def get_all_kbis(self) -> List[tuple[KPIDefinition, KPI]]:
        """Get all KBIs from all parsed definitions as (definition, kbi) tuples."""
        all_kbis = []
        for definition in self.parsed_definitions:
            for kpi in definition.kpis:
                all_kbis.append((definition, kpi))
        return all_kbis