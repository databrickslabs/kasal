"""
Enhanced DAX Aggregation Support
Provides comprehensive aggregation types for KBI to DAX conversion
"""

from enum import Enum
from typing import Dict, List, Optional, Any
import re


class AggregationType(Enum):
    """Supported DAX aggregation types"""
    SUM = "SUM"
    COUNT = "COUNT" 
    AVERAGE = "AVERAGE"
    MIN = "MIN"
    MAX = "MAX"
    DISTINCTCOUNT = "DISTINCTCOUNT"
    COUNTROWS = "COUNTROWS"
    MEDIAN = "MEDIAN"
    PERCENTILE = "PERCENTILE"
    STDEV = "STDEV"
    VAR = "VAR"
    # Advanced aggregations
    SUMX = "SUMX"
    AVERAGEX = "AVERAGEX"
    MINX = "MINX"
    MAXX = "MAXX"
    COUNTX = "COUNTX"
    # Exception/Custom aggregations
    DIVIDE = "DIVIDE"
    RATIO = "RATIO"
    VARIANCE = "VARIANCE"
    WEIGHTED_AVERAGE = "WEIGHTED_AVERAGE"
    EXCEPTION_AGGREGATION = "EXCEPTION_AGGREGATION"
    CALCULATED = "CALCULATED"


class AggregationDetector:
    """Detects aggregation type from formula or explicit specification"""
    
    @staticmethod
    def detect_aggregation_type(formula: str, aggregation_hint: Optional[str] = None, kbi_definition: Optional[Dict] = None) -> AggregationType:
        """
        Detect the aggregation type from formula string or hint
        
        Args:
            formula: The formula field value
            aggregation_hint: Optional explicit aggregation type hint
            kbi_definition: Full KPI definition for additional context
            
        Returns:
            AggregationType enum value
        """
        # Check for exception aggregation first
        if kbi_definition and kbi_definition.get('exception_aggregation') and kbi_definition.get('fields_for_exception_aggregation'):
            return AggregationType.EXCEPTION_AGGREGATION
        
        if aggregation_hint:
            try:
                return AggregationType(aggregation_hint.upper())
            except ValueError:
                pass
        
        # Check if formula already contains DAX aggregation
        formula_upper = formula.upper()
        
        # Direct DAX function detection
        dax_patterns = {
            r'COUNT\s*\(': AggregationType.COUNT,
            r'COUNTROWS\s*\(': AggregationType.COUNTROWS,
            r'DISTINCTCOUNT\s*\(': AggregationType.DISTINCTCOUNT,
            r'SUM\s*\(': AggregationType.SUM,
            r'AVERAGE\s*\(': AggregationType.AVERAGE,
            r'MIN\s*\(': AggregationType.MIN,
            r'MAX\s*\(': AggregationType.MAX,
            r'SUMX\s*\(': AggregationType.SUMX,
            r'AVERAGEX\s*\(': AggregationType.AVERAGEX,
            r'MINX\s*\(': AggregationType.MINX,
            r'MAXX\s*\(': AggregationType.MAXX,
            r'COUNTX\s*\(': AggregationType.COUNTX,
            r'DIVIDE\s*\(': AggregationType.DIVIDE,
        }
        
        for pattern, agg_type in dax_patterns.items():
            if re.search(pattern, formula_upper):
                return agg_type
        
        # Default to SUM for backward compatibility
        return AggregationType.SUM


class DAXAggregationBuilder:
    """Builds DAX aggregation expressions"""
    
    def __init__(self):
        self.aggregation_templates = {
            AggregationType.SUM: self._build_sum,
            AggregationType.COUNT: self._build_count,
            AggregationType.COUNTROWS: self._build_countrows,
            AggregationType.DISTINCTCOUNT: self._build_distinctcount,
            AggregationType.AVERAGE: self._build_average,
            AggregationType.MIN: self._build_min,
            AggregationType.MAX: self._build_max,
            AggregationType.MEDIAN: self._build_median,
            AggregationType.PERCENTILE: self._build_percentile,
            AggregationType.STDEV: self._build_stdev,
            AggregationType.VAR: self._build_var,
            AggregationType.SUMX: self._build_sumx,
            AggregationType.AVERAGEX: self._build_averagex,
            AggregationType.MINX: self._build_minx,
            AggregationType.MAXX: self._build_maxx,
            AggregationType.COUNTX: self._build_countx,
            AggregationType.DIVIDE: self._build_divide,
            AggregationType.RATIO: self._build_ratio,
            AggregationType.VARIANCE: self._build_variance,
            AggregationType.WEIGHTED_AVERAGE: self._build_weighted_average,
            AggregationType.EXCEPTION_AGGREGATION: self._build_exception_aggregation,
            AggregationType.CALCULATED: self._build_calculated,
        }
    
    def build_aggregation(self, 
                         agg_type: AggregationType,
                         formula: str,
                         source_table: str,
                         kbi_definition: Dict[str, Any] = None) -> str:
        """
        Build DAX aggregation expression
        
        Args:
            agg_type: Type of aggregation
            formula: Formula field or expression
            source_table: Source table name
            kbi_definition: Full KPI definition for context
            
        Returns:
            DAX aggregation expression
        """
        if agg_type in self.aggregation_templates:
            return self.aggregation_templates[agg_type](formula, source_table, kbi_definition or {})
        else:
            # Fallback to SUM
            return self._build_sum(formula, source_table, kbi_definition or {})
    
    def _build_sum(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build SUM aggregation"""
        if 'SUM(' in formula.upper():
            return formula
        
        # Handle complex formulas with IF/CASE statements
        if 'IF(' in formula.upper() or 'CASE' in formula.upper():
            # For complex formulas, wrap in SUMX to handle row-by-row evaluation
            return f"SUMX({source_table}, {self._ensure_table_references(formula, source_table)})"
        
        return f"SUM({source_table}[{formula}])"
    
    def _ensure_table_references(self, formula: str, source_table: str) -> str:
        """Ensure column references have proper table prefixes"""
        import re
        
        # Skip if formula already looks properly formatted
        if f"{source_table}[" in formula and not f"{source_table}[{source_table}[" in formula:
            return formula
        
        # For column names that start with common prefixes (bic_, etc.)
        result = formula
        
        # Simple approach: find all bic_ prefixed columns and wrap them
        bic_columns = re.findall(r'\bbic_[a-zA-Z0-9_]+\b', result)
        
        for column in bic_columns:
            if f"{source_table}[{column}]" not in result:
                result = result.replace(column, f"{source_table}[{column}]")
        
        # Also handle any other column-like words that aren't numbers or DAX functions
        words = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', result)
        dax_functions = ['IF', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT']
        
        for word in set(words):  # Use set to avoid duplicate processing
            if (word.upper() not in dax_functions and 
                not word.isdigit() and 
                word not in ['0', '1'] and
                word != source_table and  # Don't convert table names
                ('_' in word or word.startswith('bic')) and  # Likely a column name
                f"{source_table}[{word}]" not in result and
                f"[{word}]" not in result):
                # Use word boundary regex to replace only whole words
                result = re.sub(r'\b' + re.escape(word) + r'\b', f"{source_table}[{word}]", result)
        
        return result
    
    def _build_count(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build COUNT aggregation"""
        if 'COUNT(' in formula.upper():
            return formula
        return f"COUNT({source_table}[{formula}])"
    
    def _build_countrows(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build COUNTROWS aggregation"""
        if 'COUNTROWS(' in formula.upper():
            return formula
        return f"COUNTROWS({source_table})"
    
    def _build_distinctcount(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build DISTINCTCOUNT aggregation"""
        if 'DISTINCTCOUNT(' in formula.upper():
            return formula
        return f"DISTINCTCOUNT({source_table}[{formula}])"
    
    def _build_average(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build AVERAGE aggregation"""
        if 'AVERAGE(' in formula.upper():
            return formula
        return f"AVERAGE({source_table}[{formula}])"
    
    def _build_min(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build MIN aggregation"""
        if 'MIN(' in formula.upper():
            return formula
        return f"MIN({source_table}[{formula}])"
    
    def _build_max(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build MAX aggregation"""
        if 'MAX(' in formula.upper():
            return formula
        return f"MAX({source_table}[{formula}])"
    
    def _build_median(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build MEDIAN aggregation"""
        if 'MEDIAN(' in formula.upper():
            return formula
        return f"MEDIAN({source_table}[{formula}])"
    
    def _build_percentile(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build PERCENTILE aggregation"""
        percentile = kbi_def.get('percentile', 0.5)  # Default to median
        if 'PERCENTILE' in formula.upper():
            return formula
        return f"PERCENTILE.INC({source_table}[{formula}], {percentile})"
    
    def _build_stdev(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build STDEV aggregation"""
        if 'STDEV' in formula.upper():
            return formula
        return f"STDEV.P({source_table}[{formula}])"
    
    def _build_var(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build VAR aggregation"""
        if 'VAR' in formula.upper():
            return formula
        return f"VAR.P({source_table}[{formula}])"
    
    def _build_sumx(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build SUMX aggregation"""
        if 'SUMX(' in formula.upper():
            return formula
        return f"SUMX({source_table}, {source_table}[{formula}])"
    
    def _build_averagex(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build AVERAGEX aggregation"""
        if 'AVERAGEX(' in formula.upper():
            return formula
        return f"AVERAGEX({source_table}, {source_table}[{formula}])"
    
    def _build_minx(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build MINX aggregation"""
        if 'MINX(' in formula.upper():
            return formula
        return f"MINX({source_table}, {source_table}[{formula}])"
    
    def _build_maxx(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build MAXX aggregation"""
        if 'MAXX(' in formula.upper():
            return formula
        return f"MAXX({source_table}, {source_table}[{formula}])"
    
    def _build_countx(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build COUNTX aggregation"""
        if 'COUNTX(' in formula.upper():
            return formula
        condition = kbi_def.get('count_condition', f"{source_table}[{formula}] <> BLANK()")
        return f"COUNTX({source_table}, IF({condition}, 1, BLANK()))"
    
    def _build_divide(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build DIVIDE aggregation for ratios"""
        if 'DIVIDE(' in formula.upper():
            return formula
        
        # Expect formula to be in format: "numerator_column/denominator_column"
        if '/' in formula:
            parts = formula.split('/')
            if len(parts) == 2:
                numerator = parts[0].strip()
                denominator = parts[1].strip()
                return f"DIVIDE(SUM({source_table}[{numerator}]), SUM({source_table}[{denominator}]), 0)"
        
        # Fallback
        return f"DIVIDE(SUM({source_table}[{formula}]), 1, 0)"
    
    def _build_ratio(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build ratio calculation"""
        base_column = kbi_def.get('base_column')
        if base_column:
            return f"DIVIDE(SUM({source_table}[{formula}]), SUM({source_table}[{base_column}]), 0)"
        return self._build_divide(formula, source_table, kbi_def)
    
    def _build_variance(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build variance calculation (actual vs target)"""
        target_column = kbi_def.get('target_column')
        if target_column:
            return f"SUM({source_table}[{formula}]) - SUM({source_table}[{target_column}])"
        return f"VAR.P({source_table}[{formula}])"
    
    def _build_weighted_average(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build weighted average calculation"""
        weight_column = kbi_def.get('weight_column')
        if weight_column:
            return f"DIVIDE(SUMX({source_table}, {source_table}[{formula}] * {source_table}[{weight_column}]), SUM({source_table}[{weight_column}]), 0)"
        return f"AVERAGE({source_table}[{formula}])"

    def _build_calculated(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build calculated measure - formula contains references to other measures"""
        # For calculated measures, the formula should be resolved by the dependency resolver
        # We return it as-is since it should already contain proper DAX expressions
        return formula

    def _build_exception_aggregation(self, formula: str, source_table: str, kbi_def: Dict) -> str:
        """Build SAP BW-style exception aggregation using SUMMARIZE and SUMX"""
        exception_agg_type = kbi_def.get('exception_aggregation', 'SUM').upper()
        fields_for_exception = kbi_def.get('fields_for_exception_aggregation', [])
        
        if not fields_for_exception:
            # Fallback to regular aggregation if no exception fields specified
            return f"{exception_agg_type}({source_table}[{formula}])"
        
        # Build SUMMARIZE columns (grouping fields)
        summarize_columns = []
        for field in fields_for_exception:
            summarize_columns.append(f"'{source_table}'[{field}]")
        
        # Parse the formula to handle complex expressions like CASE WHEN
        calculated_expression = self._parse_exception_formula(formula, source_table)
        
        # Build the SUMX with SUMMARIZE pattern
        summarize_args = f"'{source_table}', " + ", ".join(summarize_columns)
        
        if exception_agg_type == 'SUM':
            return f'''SUMX (
    SUMMARIZE (
        {summarize_args},
        "CalculatedValue", {calculated_expression}
    ),
    [CalculatedValue]
)'''
        elif exception_agg_type == 'AVERAGE':
            return f'''AVERAGEX (
    SUMMARIZE (
        {summarize_args},
        "CalculatedValue", {calculated_expression}
    ),
    [CalculatedValue]
)'''
        elif exception_agg_type == 'COUNT':
            return f'''SUMX (
    SUMMARIZE (
        {summarize_args},
        "CalculatedValue", IF(ISBLANK({calculated_expression}), 0, 1)
    ),
    [CalculatedValue]
)'''
        else:
            # Default to SUM for other aggregation types
            return f'''SUMX (
    SUMMARIZE (
        {summarize_args},
        "CalculatedValue", {calculated_expression}
    ),
    [CalculatedValue]
)'''

    def _apply_constant_selection(self, base_formula: str, source_table: str, kbi_def: Dict) -> str:
        """
        Apply SAP BW-style constant selection using REMOVEFILTERS
        
        Constant selection ensures that certain dimensions maintain their filter context
        regardless of user navigation or filtering - similar to SAP BW constant selection
        
        Args:
            base_formula: The base DAX aggregation formula
            source_table: Source table name  
            kbi_def: KPI definition containing fields_for_constant_selection
            
        Returns:
            DAX formula with REMOVEFILTERS applied for constant selection fields
        """
        constant_selection_fields = kbi_def.get('fields_for_constant_selection', [])
        
        if not constant_selection_fields:
            return base_formula
        
        # Build REMOVEFILTERS clauses for each constant selection field
        removefilters_clauses = []
        for field in constant_selection_fields:
            removefilters_clauses.append(f"REMOVEFILTERS({source_table}[{field}])")
        
        # Return the base formula unchanged - the constant selection will be handled 
        # by the main DAX generator in the CALCULATE function where other filters are added
        return base_formula

    def _parse_exception_formula(self, formula: str, source_table: str) -> str:
        """Parse complex formulas and convert them to DAX expressions"""
        import re
        
        # Handle CASE WHEN expressions
        if 'CASE WHEN' in formula.upper():
            # Convert SQL-style CASE WHEN to DAX IF statements
            # Pattern: CASE WHEN condition THEN value1 ELSE value2 END
            # The condition can span multiple parts including comparisons
            case_pattern = r'CASE\s+WHEN\s+(.+?)\s+THEN\s+(\w+|\d+)\s+ELSE\s+(\w+|\d+)\s+END'
            
            def convert_case(match):
                condition = match.group(1).strip()
                then_value = match.group(2).strip()
                else_value = match.group(3).strip()
                
                # Convert condition to use SELECTEDVALUE for proper context
                condition_dax = self._convert_condition_to_dax(condition, source_table)
                
                return f"IF({condition_dax}, {then_value}, {else_value})"
            
            formula = re.sub(case_pattern, convert_case, formula, flags=re.IGNORECASE)
        
        # Apply simple column conversion for remaining column references
        # Only convert standalone column names that aren't already in SELECTEDVALUE calls
        result = formula
        
        # Find all column names that match the bic_ pattern
        import re
        column_pattern = r'\b(bic_[a-zA-Z0-9_]+)\b'
        
        def convert_standalone_column(match):
            column_name = match.group(1)
            # Don't convert if it's already inside a SELECTEDVALUE call
            start_pos = match.start()
            text_before = result[:start_pos]
            
            # Check if this column is inside a SELECTEDVALUE call
            last_selectedvalue = text_before.rfind('SELECTEDVALUE(')
            if last_selectedvalue != -1:
                # Check if there's a closing parenthesis after this position
                text_after_sv = result[last_selectedvalue:]
                next_close_paren = text_after_sv.find(')')
                if next_close_paren > start_pos - last_selectedvalue:
                    # We're inside a SELECTEDVALUE call, don't convert
                    return column_name
            
            return f"SELECTEDVALUE('{source_table}'[{column_name}])"
        
        result = re.sub(column_pattern, convert_standalone_column, result)
        
        # Final cleanup: Replace any remaining CASE WHEN with IF
        result = result.replace('CASE WHEN', 'IF')
        result = result.replace('THEN', ',')
        result = result.replace('ELSE', ',')
        result = result.replace('END', '')
        
        # Clean up extra parentheses multiple times to handle nested cases
        for _ in range(3):  # Run cleanup multiple times
            result = re.sub(r'\(\s*\(\s*', '(', result)  # Remove double opening parentheses
            result = re.sub(r'\s*\)\s*\)', ')', result)  # Remove double closing parentheses
        
        # More aggressive cleanup for common patterns in IF statements
        result = re.sub(r'IF\(\s*(SELECTEDVALUE\([^)]+\[[^]]+\]\))\s+(<>|!=|=|>|<|>=|<=)\s+\(\s*(\w+|\d+)\s*\)', r'IF(\1 \2 \3', result)
        
        # Remove parentheses around simple values in comparisons
        result = re.sub(r'(<>|!=|=|>|<|>=|<=)\s+\(\s*(\w+|\d+)\s*\)', r'\1 \2', result)
        
        # Check if we need to add a missing closing parenthesis
        open_count = result.count('(')
        close_count = result.count(')')
        if open_count > close_count:
            result += ')' * (open_count - close_count)
        
        return result

    def _convert_condition_to_dax(self, condition: str, source_table: str) -> str:
        """Convert SQL-style conditions to DAX conditions"""
        import re
        
        # Handle column references in conditions
        condition = condition.strip()
        
        # Remove extra parentheses around column names like ( bic_order_value )
        condition = re.sub(r'\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)', r'\1', condition)
        
        # Remove extra parentheses around values like ( 0 )
        condition = re.sub(r'\(\s*(\d+)\s*\)', r'\1', condition)
        
        # Pattern for column comparisons like "confirmed_phc <> 0"
        comparison_pattern = r'(\w+)\s*(<>|!=|=|>|<|>=|<=)\s*(\w+|\d+)'
        
        def convert_comparison(match):
            column = match.group(1)
            operator = match.group(2)
            value = match.group(3)
            
            # Convert <> to <> (DAX uses <> for not equal)
            if operator == '!=':
                operator = '<>'
            
            return f"SELECTEDVALUE('{source_table}'[{column}]) {operator} {value}"
        
        result = re.sub(comparison_pattern, convert_comparison, condition)
        
        # Final cleanup - remove any remaining extra parentheses around SELECTEDVALUE calls
        result = re.sub(r'\(\s*(SELECTEDVALUE\([^)]+\[[^]]+\]\))\s*\)', r'\1', result)
        
        return result


class ExceptionAggregationHandler:
    """Handles special cases and exception aggregations"""
    
    @staticmethod
    def handle_exception_aggregation(kbi_definition: Dict[str, Any], base_dax: str) -> str:
        """
        Handle exception aggregations and post-processing
        
        Args:
            kbi_definition: Full KPI definition
            base_dax: Base DAX expression
            
        Returns:
            Enhanced DAX with exception handling
        """
        exceptions = kbi_definition.get('exceptions', [])
        display_sign = kbi_definition.get('display_sign', 1)
        
        enhanced_dax = base_dax
        
        # Apply display sign
        if display_sign == -1:
            enhanced_dax = f"-1 * ({enhanced_dax})"
        elif display_sign != 1:
            enhanced_dax = f"{display_sign} * ({enhanced_dax})"
        
        # Handle exceptions
        for exception in exceptions:
            exception_type = exception.get('type')
            
            if exception_type == 'null_to_zero':
                enhanced_dax = f"IF(ISBLANK({enhanced_dax}), 0, {enhanced_dax})"
            
            elif exception_type == 'division_by_zero':
                enhanced_dax = f"IF(ISERROR({enhanced_dax}), 0, {enhanced_dax})"
            
            elif exception_type == 'negative_to_zero':
                enhanced_dax = f"MAX(0, {enhanced_dax})"
            
            elif exception_type == 'threshold':
                threshold_value = exception.get('value', 0)
                comparison = exception.get('comparison', 'min')
                if comparison == 'min':
                    enhanced_dax = f"MAX({threshold_value}, {enhanced_dax})"
                elif comparison == 'max':
                    enhanced_dax = f"MIN({threshold_value}, {enhanced_dax})"
            
            elif exception_type == 'custom_condition':
                condition = exception.get('condition', '')
                true_value = exception.get('true_value', enhanced_dax)
                false_value = exception.get('false_value', '0')
                enhanced_dax = f"IF({condition}, {true_value}, {false_value})"
        
        return enhanced_dax


def detect_and_build_aggregation(kbi_definition: Dict[str, Any]) -> str:
    """
    Main function to detect aggregation type and build DAX
    
    Args:
        kbi_definition: Full KPI definition dictionary
        
    Returns:
        Complete DAX aggregation expression
    """
    formula = kbi_definition.get('formula', '')
    source_table = kbi_definition.get('source_table', 'Table')
    aggregation_hint = kbi_definition.get('aggregation_type')
    
    # Detect aggregation type
    detector = AggregationDetector()
    agg_type = detector.detect_aggregation_type(formula, aggregation_hint, kbi_definition)
    
    # Build base aggregation
    builder = DAXAggregationBuilder()
    base_dax = builder.build_aggregation(agg_type, formula, source_table, kbi_definition)
    
    # Handle exceptions
    exception_handler = ExceptionAggregationHandler()
    final_dax = exception_handler.handle_exception_aggregation(kbi_definition, base_dax)
    
    return final_dax