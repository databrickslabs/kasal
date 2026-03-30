import re
from typing import Dict, Any, List
from ...base.models import KPI, QueryFilter, KPIDefinition


class FilterResolver:
    def __init__(self):
        self.variable_pattern = re.compile(r'\$var_(\w+)')
        self.query_filter_pattern = re.compile(r'\$query_filter')

    def resolve_filters(self, kpi: KPI, definition: KPIDefinition) -> List[str]:
        """Resolve all filters for a KPI, replacing variables and query filter references."""
        resolved_filters = []

        # Handle None filters (return empty list)
        if kpi.filters is None:
            return resolved_filters

        for filter_item in kpi.filters:
            if isinstance(filter_item, str):
                # Simple string filter
                resolved_filter = self._resolve_variables(filter_item, definition.default_variables)
                resolved_filter = self._resolve_query_filters(resolved_filter, definition.query_filters, definition.default_variables)
                resolved_filters.append(resolved_filter)
            elif isinstance(filter_item, dict):
                # Complex filter object
                resolved_filter = self._resolve_complex_filter(filter_item, definition)
                resolved_filters.append(resolved_filter)
        
        return resolved_filters
    
    def _resolve_variables(self, filter_text: str, variables: Dict[str, Any]) -> str:
        """Replace $var_xyz references with actual values."""
        def replace_var(match):
            var_name = match.group(1)
            if var_name in variables:
                value = variables[var_name]
                if isinstance(value, str):
                    # Check if the value is already quoted in the original filter
                    if f"'$var_{var_name}'" in filter_text:
                        return value  # Don't add extra quotes
                    else:
                        return f"'{value}'"
                elif isinstance(value, list):
                    # Format as IN clause
                    formatted_values = [f"'{v}'" if isinstance(v, str) else str(v) for v in value]
                    return f"({', '.join(formatted_values)})"
                else:
                    return str(value)
            return match.group(0)  # Return original if variable not found
        
        return self.variable_pattern.sub(replace_var, filter_text)
    
    def _resolve_query_filters(self, filter_text: str, query_filters: List[QueryFilter], variables: Dict[str, Any] = None) -> str:
        """Replace $query_filter references with full expressions."""
        if '$query_filter' in filter_text:
            # For now, combine all query filters with AND
            if query_filters:
                resolved_expressions = []
                for qf in query_filters:
                    # Resolve variables in each query filter expression
                    resolved_expr = qf.expression
                    if variables:
                        resolved_expr = self._resolve_variables(resolved_expr, variables)
                    resolved_expressions.append(resolved_expr)
                
                combined_filters = ' AND '.join(resolved_expressions)
                return filter_text.replace('$query_filter', f"({combined_filters})")
            else:
                return filter_text.replace('$query_filter', "1=1")  # No filter condition
        return filter_text
    
    def _resolve_complex_filter(self, filter_dict: Dict[str, Any], definition: KPIDefinition) -> str:
        """Convert complex filter dictionary to DAX-compatible string."""
        if 'field' in filter_dict and 'operator' in filter_dict and 'value' in filter_dict:
            field = filter_dict['field']
            operator = filter_dict['operator']
            value = filter_dict['value']
            
            # Resolve variables in value
            if isinstance(value, str):
                value = self._resolve_variables(value, definition.default_variables)
            
            # Convert to DAX format
            return self._format_dax_filter(field, operator, value)
        
        # If it's a string representation, resolve it
        if isinstance(filter_dict, str):
            resolved = self._resolve_variables(filter_dict, definition.default_variables)
            return self._resolve_query_filters(resolved, definition.query_filters, definition.default_variables)
        
        return str(filter_dict)
    
    def _format_dax_filter(self, field: str, operator: str, value: Any) -> str:
        """Format a single filter condition for DAX."""
        # Clean field name - remove bic_ prefix and handle special characters
        clean_field = field.replace('bic_', '').replace('_', ' ').title()
        
        if operator.upper() == 'IN':
            if isinstance(value, list):
                formatted_values = [f'"{v}"' if isinstance(v, str) else str(v) for v in value]
                return f"'{clean_field}'[{field}] IN {{{', '.join(formatted_values)}}}"
            else:
                return f"'{clean_field}'[{field}] IN {value}"
        elif operator == '=':
            if isinstance(value, str):
                return f"'{clean_field}'[{field}] = \"{value}\""
            else:
                return f"'{clean_field}'[{field}] = {value}"
        elif operator == '!=':
            if isinstance(value, str):
                return f"'{clean_field}'[{field}] <> \"{value}\""
            else:
                return f"'{clean_field}'[{field}] <> {value}"
        elif operator in ['>', '<', '>=', '<=']:
            return f"'{clean_field}'[{field}] {operator} {value}"
        else:
            # Default case
            return f"'{clean_field}'[{field}] {operator} {value}"
    
    def combine_filters(self, filters: List[str], logical_operator: str = "AND") -> str:
        """Combine multiple filters with logical operators."""
        if not filters:
            return ""
        if len(filters) == 1:
            return filters[0]
        
        return f" {logical_operator} ".join([f"({f})" for f in filters])