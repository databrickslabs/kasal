"""Parser for DAX expressions with hierarchical decomposition."""
import logging
import re
from typing import Any, Dict, List, Optional, Set

from .constants import AGGREGATION_FUNCTIONS

logger = logging.getLogger(__name__)

# Pre-compiled regex patterns for performance
_VAR_PATTERN = re.compile(r'var\s+(\w+)\s*=\s*(.+?)(?=var\s+\w+\s*=|RETURN|$)', re.IGNORECASE | re.DOTALL)
_RETURN_PATTERN = re.compile(r'RETURN(.*)$', re.IGNORECASE | re.DOTALL)

# Maximum iterations for variable substitution to prevent infinite loops
_MAX_VAR_SUBSTITUTION_ITERATIONS = 100
_VAR_SUBSTITUTION_WARNING_THRESHOLD = 95


class DAXExpressionParser:
    """Parser for DAX expressions with hierarchical decomposition."""
    
    def __init__(self, table_mappings: Optional[Dict] = None, column_mappings: Optional[Dict] = None):
        self.table_mappings = table_mappings or {}
        self.column_mappings = column_mappings or {}
    
    def parse(self, expr: str) -> Dict[str, Any]:
        """
        Parse a DAX expression into structured components with hierarchical tree.
        
        The hierarchical tree is the primary output, with flat extractions derived from it.
        
        Args:
            expr: DAX expression to parse
            
        Returns:
            Dict containing:
            - raw: Original expression
            - hierarchical_tree: Hierarchical parse tree (PRIMARY)
            - variables: Extracted VAR declarations with metadata
            - aggregations: List of aggregation functions (derived from tree)
            - references: Set of table.column references (derived from tree)
            - filters: List of filter conditions (derived from tree)
            - operations: List of operations (derived from tree)
            - structure: Structural analysis (derived from tree)
        """
        expr = expr.strip()
        
        # 1. Get hierarchical tree (PRIMARY OUTPUT)
        hierarchical_tree = self.decompose(expr)
        
        # 2. Extract variables and top-level components
        cleaned = self.clean_dax_comments(expr)
        toplevel = self.decompose_toplevel(cleaned)
        
        # 3. Derive flat extractions from hierarchical tree
        aggregations = self._extract_aggregations_from_tree(hierarchical_tree)
        filters = self._extract_filters_from_tree(hierarchical_tree)
        references = self._extract_references_from_tree(hierarchical_tree)
        operations = self._extract_operations_from_tree(hierarchical_tree)
        structure = self._analyze_structure_from_tree(hierarchical_tree)
        
        result = {
            "raw": expr,
            "hierarchical_tree": hierarchical_tree,
            "variables": toplevel.get("variables", []),
            "aggregations": aggregations,
            "references": references,
            "filters": filters,
            "operations": operations,
            "structure": structure,
        }
        
        return result
    
    def clean_dax_comments(self, dax_expr: str) -> str:
        """Removes comments (// and --) and strips the DAX of unnecessary whitespaces."""
        lines = [line.strip() for line in dax_expr.split('\n') if not line.strip().startswith('//')]
        lines = [line.split('//')[0].strip() for line in lines]
        lines = [line.split('--')[0].strip() for line in lines]
        return '\n'.join(lines)
    
    def _extract_variables(self, dax_expr: str) -> List[Dict[str, Any]]:
        """Extract VAR declarations from DAX expression."""
        var_matches = list(_VAR_PATTERN.finditer(dax_expr))
        return [
            {
                "variable_name": v.group(1), 
                "variable_expr": v.group(2)
            } for v in var_matches
        ]
    
    def _extract_return_expr(self, dax_expr: str) -> Optional[str]:
        """Extract RETURN expression from DAX."""
        return_matches = list(_RETURN_PATTERN.finditer(dax_expr))
        if return_matches:
            return return_matches[0].group(1)
        return None
    
    def check_variable_usage(self, variable_name: str, dax_expr: str) -> bool:
        """Check if a variable is used in an expression."""
        variable_pattern = r'[^\w](%s)[^\w]' % re.escape(variable_name)
        matches = list(re.finditer(variable_pattern, dax_expr, re.IGNORECASE | re.DOTALL))
        return bool(matches)
    
    def decompose_toplevel(self, dax_expr: str) -> Dict[str, Any]:
        """
        Decompose DAX expression into top-level components (variables and return expression).
        
        Returns:
            Dict with 'type', 'return_expr' or 'expr', and 'variables' (if any)
        """
        # Extract variables
        variables = self._extract_variables(dax_expr)
        if variables:
            return_expr = self._extract_return_expr(dax_expr)
            
            # Check variable usage
            for i, var in enumerate(variables):
                # Check if used in return expression
                if return_expr and self.check_variable_usage(var["variable_name"], return_expr):
                    variables[i]["used_in_return"] = True
                # Check if used in another variable
                for var_expr in variables:
                    if self.check_variable_usage(var["variable_name"], var_expr["variable_expr"]):
                        variables[i]["used_in_variable"] = True
            
            relevant_variables = [
                v for v in variables 
                if v.get("used_in_return", False) or v.get("used_in_variable", False)
            ]
            return {
                "type": "return",
                "return_expr": return_expr,
                "variables": relevant_variables
            }
        else:
            return {
                "type": "no_return",
                "expr": dax_expr
            }
    
    def _substitute_single_variable(self, expression: str, var_name: str, var_expr: str) -> str:
        """
        Substitute a single variable in an expression.
        
        Args:
            expression: The expression to substitute in
            var_name: The variable name to find
            var_expr: The expression to substitute with
            
        Returns:
            Expression with variable substituted
        """
        # Use word boundaries to match the variable name as a whole word.
        # re.escape() prevents metacharacters in var_name from breaking the pattern.
        variable_pattern = r'([^\w])(%s)([^\w])' % re.escape(var_name)
        
        # Check if the variable exists in the expression
        if not re.search(variable_pattern, expression, re.IGNORECASE):
            return expression
        
        replacement = r"\1%s\3" % (var_expr)
        result = re.sub(variable_pattern, replacement, expression, flags=re.IGNORECASE)
        
        return result
    
    def substitute_all_variables_recursively(self, variables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Recursively substitute all variables into each other until fully expanded.
        
        This method processes variables in dependency order, ensuring that if
        variable A references B, and B references C, then A will be fully expanded
        to reference C directly.
        
        Args:
            variables: List of variable dicts with 'variable_name' and 'variable_expr'
            
        Returns:
            List of variables with fully substituted expressions
            
        Raises:
            RuntimeError: If max iterations exceeded (potential infinite loop)
        """
        if not variables:
            return []
        
        # Create a working copy
        working_vars = [v.copy() for v in variables]

        # Perform iterative substitution until no more changes occur
        iteration = 0
        
        while iteration < _MAX_VAR_SUBSTITUTION_ITERATIONS:
            iteration += 1
            
            # Warn if approaching max iterations
            if iteration >= _VAR_SUBSTITUTION_WARNING_THRESHOLD:
                logger.warning(
                    "Variable substitution approaching max iterations (%d/%d). "
                    "Possible circular dependency in variables.",
                    iteration, _MAX_VAR_SUBSTITUTION_ITERATIONS
                )
            
            changed = False
            
            # For each variable, try to substitute all other variables into it
            for i, var in enumerate(working_vars):
                original_expr = var["variable_expr"]
                
                # Sort other variables by name length (longest first) to avoid partial matches
                other_vars = [v for v in working_vars if v["variable_name"] != var["variable_name"]]
                other_vars.sort(key=lambda x: len(x["variable_name"]), reverse=True)
                
                # Try to substitute each other variable
                for other_var in other_vars:
                    new_expr = self._substitute_single_variable(
                        var["variable_expr"],
                        other_var["variable_name"],
                        other_var["variable_expr"]
                    )
                    
                    if new_expr != var["variable_expr"]:
                        var["variable_expr"] = new_expr
                        changed = True
                
                # Update the working list
                working_vars[i] = var
            
            # If no changes were made in this iteration, we're done
            if not changed:
                break
        
        if iteration >= _MAX_VAR_SUBSTITUTION_ITERATIONS:
            logger.error(
                "Variable substitution exceeded max iterations (%d). Possible infinite loop detected.",
                _MAX_VAR_SUBSTITUTION_ITERATIONS
            )
            raise RuntimeError(
                f"Variable substitution exceeded maximum iterations ({_MAX_VAR_SUBSTITUTION_ITERATIONS}). "
                "This may indicate a circular dependency or malformed DAX expression."
            )
        
        return working_vars
    
    def parse_function_call(self, expr: str) -> Dict[str, Any]:
        """
        Parse a function call expression into a hierarchical structure.
        
        Example:
            func1(func2(a),func3(b),func4(func5(c)))
        
        Returns:
            {
                'function': 'func1',
                'arguments': [
                    {'function': 'func2', 'arguments': [{'value': 'a'}]},
                    {'function': 'func3', 'arguments': [{'value': 'b'}]},
                    {'function': 'func4', 'arguments': [
                        {'function': 'func5', 'arguments': [{'value': 'c'}]}
                    ]}
                ]
            }
        """
        expr = expr.strip()
        
        # Find the function name (everything before the first '(' or '{')
        paren_pos = expr.find('(')
        
        if paren_pos == -1:
            # No parentheses, this is a simple value/variable
            return {'value': expr}
        
        func_name = expr[:paren_pos].strip()
        
        # Extract the content within the parentheses
        content = self._extract_balanced_parens(expr, paren_pos)
        
        # Parse the arguments
        arguments = self._parse_arguments(content)
        
        return {
            'function': func_name,
            'arguments': arguments
        }
    
    def _parse_arguments(self, args_str: str) -> List[Dict[str, Any]]:
        """
        Parse comma-separated arguments, respecting nested parentheses.
        
        Args:
            args_str: String containing comma-separated arguments
            
        Returns:
            List of parsed argument structures
        """
        if not args_str.strip():
            return []
        
        arguments = []
        current_arg = []
        depth = 0
        
        for char in args_str:
            if char == '(' or char == '{':
                depth += 1
                current_arg.append(char)
            elif char == ')' or char == '}':
                depth -= 1
                current_arg.append(char)
            elif char == ',' and depth == 0:
                # This comma is at the top level, so it separates arguments
                arg_text = ''.join(current_arg).strip()
                if arg_text:
                    arguments.append(self.parse_function_call(arg_text))
                current_arg = []
            else:
                current_arg.append(char)
        
        # Don't forget the last argument
        arg_text = ''.join(current_arg).strip()
        if arg_text:
            arguments.append(self.parse_function_call(arg_text))
        
        return arguments
    
    def format_parse_tree(self, tree: Dict[str, Any], indent: int = 0) -> str:
        """
        Format a parse tree into a readable hierarchical string.
        
        Args:
            tree: The parse tree from parse_function_call
            indent: Current indentation level
            
        Returns:
            Formatted string representation
        """
        indent_str = "    " * indent
        
        if 'value' in tree:
            # This is a leaf node (simple value)
            return f"{indent_str}- {tree['value']}"
        
        # This is a function call
        result = [f"{indent_str}- {tree['function']}"]
        
        for arg in tree.get('arguments', []):
            result.append(self.format_parse_tree(arg, indent + 1))
        
        return '\n'.join(result)
    
    def decompose(self, dax_expr: str) -> Dict[str, Any]:
        """
        Main decomposition method that returns hierarchical parse tree.
        
        This is the primary parsing method that:
        1. Cleans comments
        2. Extracts and substitutes variables
        3. Returns hierarchical function call tree
        
        Args:
            dax_expr: DAX expression to decompose
            
        Returns:
            Hierarchical parse tree
        """
        cleaned = self.clean_dax_comments(dax_expr)
        toplevel_components = self.decompose_toplevel(cleaned)
        
        if toplevel_components.get("variables"):
            substituted_vars = self.substitute_all_variables_recursively(toplevel_components["variables"])
            
            resulting_return_expr = toplevel_components["return_expr"]
            for svar in substituted_vars:
                resulting_return_expr = self._substitute_single_variable(
                    resulting_return_expr, 
                    svar["variable_name"], 
                    svar["variable_expr"]
                )
        elif toplevel_components.get("return_expr"):
            resulting_return_expr = toplevel_components["return_expr"]
        else:
            resulting_return_expr = toplevel_components["expr"]
        
        return self.parse_function_call(resulting_return_expr)
    
    def _parse_condition(self, condition: str) -> Dict[str, Any]:
        """Parse a DAX filter condition."""
        # Handle IN clauses: table[column] in {...}
        in_match = re.search(r'(\w+)\[(\w+)\]\s+in\s*\{([^}]+)\}', condition, re.IGNORECASE)
        if in_match:
            table = in_match.group(1)
            column = in_match.group(2)
            values = [v.strip().strip('"\'') for v in in_match.group(3).split(',')]
            return {
                "type": "IN",
                "table": table,
                "column": column,
                "values": sorted(values)
            }
        
        # Handle equality: table[column] = value
        eq_match = re.search(r'(\w+)\[(\w+)\]\s*=\s*(.+)', condition)
        if eq_match:
            return {
                "type": "EQUALS",
                "table": eq_match.group(1),
                "column": eq_match.group(2),
                "value": eq_match.group(3).strip().strip('"\'')
            }
        
        logger.debug("Unable to parse condition: %s", condition)
        return {"type": "UNKNOWN", "raw": condition}
    
    def _extract_balanced_parens(self, text: str, start_pos: int) -> str:
        """Extract content within balanced parentheses or curly braces."""
        if start_pos >= len(text):
            return ""
        
        if text[start_pos] == '(':
            paren_open = '('
            paren_close = ')'
        elif text[start_pos] == '{':
            paren_open = '{'
            paren_close = '}'
        else:
            return ""
        
        depth = 0
        for i in range(start_pos, len(text)):
            if text[i] == paren_open:
                depth += 1
            elif text[i] == paren_close:
                depth -= 1
                if depth == 0:
                    return text[start_pos + 1:i]
        
        return text[start_pos + 1:]
    
    def _extract_aggregations_from_tree(self, tree: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract aggregation functions from hierarchical tree."""
        aggregations = []
        
        def traverse(node: Dict[str, Any], position: int = 0):
            if 'function' in node:
                func_name = node['function'].upper()
                # Check if this is an aggregation function
                if func_name in AGGREGATION_FUNCTIONS:
                    aggregations.append({
                        "type": func_name,
                        "content": str(node.get('arguments', [])),
                        "position": position,
                        "node": node
                    })
                
                # Traverse arguments
                for arg in node.get('arguments', []):
                    traverse(arg, position + 1)
        
        traverse(tree)
        return aggregations
    
    def _extract_filters_from_tree(self, tree: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract FILTER clauses from hierarchical tree."""
        filters = []
        
        def traverse(node: Dict[str, Any]):
            if 'function' in node:
                func_name = node['function'].upper()
                
                # Check if this is a FILTER function
                if func_name == 'FILTER':
                    args = node.get('arguments', [])
                    if len(args) >= 2:
                        table = args[0].get('value', '') if 'value' in args[0] else str(args[0])
                        condition = args[1].get('value', '') if 'value' in args[1] else str(args[1])
                        
                        filters.append({
                            "type": "FILTER",
                            "table": table,
                            "condition": condition,
                            "parsed_condition": self._parse_condition(condition),
                            "node": node
                        })
                
                # Traverse arguments
                for arg in node.get('arguments', []):
                    traverse(arg)
        
        traverse(tree)
        return filters
    
    def _extract_references_from_tree(self, tree: Dict[str, Any]) -> Set[str]:
        """Extract table[column] references from hierarchical tree."""
        references = set()
        
        def traverse(node: Dict[str, Any]):
            if 'value' in node:
                # Check if this value contains a table[column] reference
                value = node['value']
                table_pattern = r'(\w+)\[(\w+)\]'
                for match in re.finditer(table_pattern, str(value)):
                    table_name = match.group(1)
                    column_name = match.group(2)
                    references.add(f"{table_name}.{column_name}")
            
            if 'function' in node:
                # Check function name for references
                func_name = node['function']
                table_pattern = r'(\w+)\[(\w+)\]'
                for match in re.finditer(table_pattern, func_name):
                    table_name = match.group(1)
                    column_name = match.group(2)
                    references.add(f"{table_name}.{column_name}")
                
                # Traverse arguments
                for arg in node.get('arguments', []):
                    traverse(arg)
        
        traverse(tree)
        return references
    
    def _extract_operations_from_tree(self, tree: Dict[str, Any]) -> List[str]:
        """Extract mathematical operations from hierarchical tree."""
        operations = []
        operations_set = set()
        
        def traverse(node: Dict[str, Any]):
            if 'function' in node:
                func_name = node['function'].upper()
                
                # Check for operation functions
                if func_name == 'DIVIDE' and 'DIVISION' not in operations_set:
                    operations.append('DIVISION')
                    operations_set.add('DIVISION')
                
                # Traverse arguments
                for arg in node.get('arguments', []):
                    traverse(arg)
            
            if 'value' in node:
                value = str(node['value'])
                # Check for operators in values
                if '/' in value and 'DIVISION' not in operations_set:
                    operations.append('DIVISION')
                    operations_set.add('DIVISION')
                if '*' in value and 'MULTIPLICATION' not in operations_set:
                    operations.append('MULTIPLICATION')
                    operations_set.add('MULTIPLICATION')
                if '+' in value and 'ADDITION' not in operations_set:
                    operations.append('ADDITION')
                    operations_set.add('ADDITION')
                if '-' in value and 'SUBTRACTION' not in operations_set:
                    operations.append('SUBTRACTION')
                    operations_set.add('SUBTRACTION')
        
        traverse(tree)
        return operations
    
    def _analyze_structure_from_tree(self, tree: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze the overall structure from hierarchical tree."""
        structure = {
            "is_division": False,
            "has_filter": False,
            "has_calculate": False,
            "complexity": "simple"
        }
        
        def traverse(node: Dict[str, Any]):
            if 'function' in node:
                func_name = node['function'].upper()
                
                if func_name == 'DIVIDE':
                    structure["is_division"] = True
                if func_name == 'FILTER':
                    structure["has_filter"] = True
                if func_name == 'CALCULATE':
                    structure["has_calculate"] = True
                
                # Traverse arguments
                for arg in node.get('arguments', []):
                    traverse(arg)
        
        traverse(tree)
        
        # Determine complexity
        if structure["is_division"] and structure["has_filter"]:
            structure["complexity"] = "complex"
        elif structure["has_filter"] or structure["has_calculate"]:
            structure["complexity"] = "medium"
        
        return structure
