"""
DAX Syntax Converter
Converts SQL-style formula syntax to DAX expressions
Handles CASE WHEN → IF conversion and other SQL-to-DAX transformations
"""

import re
from typing import Dict, Any


class DaxSyntaxConverter:
    """Converts SQL-style formula expressions to DAX syntax"""
    
    def __init__(self):
        self.dax_functions = [
            'IF', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT',
            'SUM', 'COUNT', 'AVERAGE', 'MIN', 'MAX', 'SELECTEDVALUE', 'ISBLANK',
            'CALCULATE', 'FILTER', 'SUMX', 'AVERAGEX', 'DIVIDE'
        ]
    
    def parse_formula(self, formula: str, source_table: str) -> str:
        """
        Parse a formula and convert SQL-style syntax to DAX
        
        Args:
            formula: The original formula string
            source_table: The table name for column references
            
        Returns:
            DAX-compatible formula string
        """
        if not formula:
            return formula
            
        result = formula.strip()
        
        # Step 1: Handle CASE WHEN expressions
        result = self._convert_case_when_to_if(result, source_table)
        
        # Step 2: Handle column references
        result = self._convert_column_references(result, source_table)
        
        # Step 3: Clean up extra parentheses
        result = self._cleanup_parentheses(result)
        
        return result
    
    def _convert_case_when_to_if(self, formula: str, source_table: str) -> str:
        """Convert SQL-style CASE WHEN to DAX IF statements"""
        if 'CASE WHEN' not in formula.upper():
            return formula
        
        # Pattern: CASE WHEN (condition) THEN value1 ELSE value2 END
        case_pattern = r'CASE\s+WHEN\s*\(\s*([^)]+)\s*\)\s*THEN\s+([^\s]+)\s+ELSE\s+([^\s]+)\s+END'
        
        def convert_case(match):
            condition = match.group(1).strip()
            then_value = match.group(2).strip()
            else_value = match.group(3).strip()
            
            # Convert condition to proper DAX
            condition_dax = self._convert_condition_to_dax(condition, source_table)
            
            return f"IF({condition_dax}, {then_value}, {else_value})"
        
        result = re.sub(case_pattern, convert_case, formula, flags=re.IGNORECASE)
        
        # Also handle simple CASE WHEN without parentheses around condition
        simple_case_pattern = r'CASE\s+WHEN\s+([^T]+?)\s+THEN\s+([^\s]+)\s+ELSE\s+([^\s]+)\s+END'
        
        def convert_simple_case(match):
            condition = match.group(1).strip()
            then_value = match.group(2).strip() 
            else_value = match.group(3).strip()
            
            condition_dax = self._convert_condition_to_dax(condition, source_table)
            return f"IF({condition_dax}, {then_value}, {else_value})"
        
        result = re.sub(simple_case_pattern, convert_simple_case, result, flags=re.IGNORECASE)
        
        return result
    
    def _convert_condition_to_dax(self, condition: str, source_table: str) -> str:
        """Convert SQL-style conditions to DAX conditions"""
        condition = condition.strip()
        
        # Handle comparison operators
        comparison_pattern = r'([a-zA-Z_][a-zA-Z0-9_]*)\s*(<>|!=|=|>|<|>=|<=)\s*(\w+|\d+)'
        
        def convert_comparison(match):
            column = match.group(1)
            operator = match.group(2)
            value = match.group(3)
            
            # Convert != to <> for DAX
            if operator == '!=':
                operator = '<>'
            
            # Use table[column] format for regular aggregations
            # For exception aggregations, SELECTEDVALUE will be handled separately
            return f"{source_table}[{column}] {operator} {value}"
        
        return re.sub(comparison_pattern, convert_comparison, condition)
    
    def _convert_column_references(self, formula: str, source_table: str) -> str:
        """Convert column references to proper DAX format"""
        # Don't do column conversion for complex formulas - let the aggregation system handle it
        # This prevents double conversion issues like FactSales[FactSales][column]
        return formula
    
    def _cleanup_parentheses(self, formula: str) -> str:
        """Clean up extra parentheses in the formula"""
        # Remove double opening parentheses
        result = re.sub(r'\(\s*\(', '(', formula)
        # Remove double closing parentheses  
        result = re.sub(r'\)\s*\)', ')', result)
        
        return result.strip()