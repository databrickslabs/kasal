"""
SQL Aggregation Builders for YAML2DAX SQL Translation
Provides comprehensive SQL aggregation support for various SQL dialects
"""

from enum import Enum
from typing import Dict, List, Optional, Any, Tuple
import re
from ..models import SQLDialect, SQLAggregationType


class SQLAggregationBuilder:
    """Builds SQL aggregation expressions for different dialects"""
    
    def __init__(self, dialect: SQLDialect = SQLDialect.STANDARD):
        self.dialect = dialect
        self.aggregation_templates = {
            SQLAggregationType.SUM: self._build_sum,
            SQLAggregationType.COUNT: self._build_count,
            SQLAggregationType.AVG: self._build_avg,
            SQLAggregationType.MIN: self._build_min,
            SQLAggregationType.MAX: self._build_max,
            SQLAggregationType.COUNT_DISTINCT: self._build_count_distinct,
            SQLAggregationType.STDDEV: self._build_stddev,
            SQLAggregationType.VARIANCE: self._build_variance,
            SQLAggregationType.MEDIAN: self._build_median,
            SQLAggregationType.PERCENTILE: self._build_percentile,
            SQLAggregationType.WEIGHTED_AVG: self._build_weighted_avg,
            SQLAggregationType.RATIO: self._build_ratio,
            SQLAggregationType.RUNNING_SUM: self._build_running_sum,
            SQLAggregationType.COALESCE: self._build_coalesce,
            # Window functions
            SQLAggregationType.ROW_NUMBER: self._build_row_number,
            SQLAggregationType.RANK: self._build_rank,
            SQLAggregationType.DENSE_RANK: self._build_dense_rank,
            SQLAggregationType.EXCEPTION_AGGREGATION: self._build_exception_aggregation,
        }
    
    def build_aggregation(self,
                         agg_type: SQLAggregationType,
                         column_name: str,
                         table_name: str,
                         kbi_definition: Dict[str, Any] = None) -> str:
        """
        Build SQL aggregation expression
        
        Args:
            agg_type: Type of SQL aggregation
            column_name: Column to aggregate
            table_name: Source table name
            kbi_definition: Full KPI definition for context
            
        Returns:
            SQL aggregation expression
        """
        if kbi_definition is None:
            kbi_definition = {}
        
        if agg_type in self.aggregation_templates:
            return self.aggregation_templates[agg_type](column_name, table_name, kbi_definition)
        else:
            # Fallback to SUM
            return self._build_sum(column_name, table_name, kbi_definition)
    
    def _quote_identifier(self, identifier: str) -> str:
        """
        Quote identifier according to SQL dialect.

        - DATABRICKS: Backticks (`)
        - STANDARD: Double quotes (")
        """
        if self.dialect == SQLDialect.DATABRICKS:
            return f"`{identifier}`"
        else:  # STANDARD
            return f'"{identifier}"'
    
    def _build_sum(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build SUM aggregation"""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        
        # Handle CASE expressions
        if column_name.upper().startswith('CASE'):
            return f"SUM({column_name})"
        
        return f"SUM({quoted_table}.{quoted_column})"
    
    def _build_count(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build COUNT aggregation"""
        if column_name == "*" or column_name.upper() == "COUNT":
            return "COUNT(*)"
        
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        return f"COUNT({quoted_table}.{quoted_column})"
    
    def _build_count_distinct(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build COUNT DISTINCT aggregation"""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        return f"COUNT(DISTINCT {quoted_table}.{quoted_column})"
    
    def _build_avg(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build AVG aggregation"""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        return f"AVG({quoted_table}.{quoted_column})"
    
    def _build_min(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build MIN aggregation"""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        return f"MIN({quoted_table}.{quoted_column})"
    
    def _build_max(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build MAX aggregation"""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        return f"MAX({quoted_table}.{quoted_column})"
    
    def _build_stddev(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build STDDEV aggregation"""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        
        # DATABRICKS and STANDARD both support STDDEV_POP
        return f"STDDEV_POP({quoted_table}.{quoted_column})"
    
    def _build_variance(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build VARIANCE aggregation - for business variance calculations like actual vs budget"""
        target_column = kbi_def.get('target_column')
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)

        if target_column:
            # Business variance: SUM(actual) - SUM(budget)
            quoted_target = self._quote_identifier(target_column)
            return f"SUM({quoted_table}.{quoted_column}) - SUM({quoted_table}.{quoted_target})"
        else:
            # Fallback to statistical variance
            # DATABRICKS and STANDARD both support VAR_POP
            return f"VAR_POP({quoted_table}.{quoted_column})"
    
    def _build_median(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build MEDIAN aggregation"""
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        
        # DATABRICKS and STANDARD both support PERCENTILE_CONT
        return f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {quoted_table}.{quoted_column})"
    
    def _build_percentile(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build PERCENTILE aggregation"""
        percentile = kbi_def.get('percentile', 0.5)
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        
        # DATABRICKS and STANDARD both support PERCENTILE_CONT
        return f"PERCENTILE_CONT({percentile}) WITHIN GROUP (ORDER BY {quoted_table}.{quoted_column})"
    
    def _build_weighted_avg(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build weighted average aggregation"""
        weight_column = kbi_def.get('weight_column')
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)

        if weight_column:
            quoted_weight = self._quote_identifier(weight_column)
            numerator = f"SUM({quoted_table}.{quoted_column} * {quoted_table}.{quoted_weight})"
            denominator = f"SUM({quoted_table}.{quoted_weight})"

            # Use NULLIF for safer division by zero handling
            return f"{numerator} / NULLIF({denominator}, 0)"
        else:
            # Fallback to regular average
            return f"AVG({quoted_table}.{quoted_column})"
    
    def _build_ratio(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build ratio calculation for DIVIDE aggregation"""
        quoted_table = self._quote_identifier(table_name)

        # Check if the formula contains a division operator
        if '/' in column_name:
            # Split the formula at the division operator
            parts = column_name.split('/')
            if len(parts) == 2:
                numerator_col = parts[0].strip()
                denominator_col = parts[1].strip()

                # Quote the column names
                quoted_numerator = self._quote_identifier(numerator_col)
                quoted_denominator = self._quote_identifier(denominator_col)

                # Build: SUM(numerator) / NULLIF(SUM(denominator), 0)
                numerator = f"SUM({quoted_table}.{quoted_numerator})"
                denominator = f"SUM({quoted_table}.{quoted_denominator})"

                return f"{numerator} / NULLIF({denominator}, 0)"

        # Fallback: Check for base_column parameter (legacy support)
        base_column = kbi_def.get('base_column')
        if base_column:
            quoted_column = self._quote_identifier(column_name)
            quoted_base = self._quote_identifier(base_column)
            numerator = f"SUM({quoted_table}.{quoted_column})"
            denominator = f"SUM({quoted_table}.{quoted_base})"

            return f"{numerator} / NULLIF({denominator}, 0)"
        else:
            # Just sum the single column if no division found
            quoted_column = self._quote_identifier(column_name)
            return f"SUM({quoted_table}.{quoted_column})"
    
    def _build_running_sum(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build running sum using window functions"""
        order_column = kbi_def.get('order_column', 'id')
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        quoted_order = self._quote_identifier(order_column)
        
        if self._supports_window_functions():
            return f"SUM({quoted_table}.{quoted_column}) OVER (ORDER BY {quoted_table}.{quoted_order} ROWS UNBOUNDED PRECEDING)"
        else:
            # Fallback for databases without window function support
            return f"SUM({quoted_table}.{quoted_column})"
    
    def _build_coalesce(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build COALESCE expression for null handling"""
        default_value = kbi_def.get('default_value', 0)
        quoted_table = self._quote_identifier(table_name)
        quoted_column = self._quote_identifier(column_name)
        
        return f"COALESCE({quoted_table}.{quoted_column}, {default_value})"
    
    def _build_row_number(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build ROW_NUMBER window function"""
        order_column = kbi_def.get('order_column', column_name)
        partition_columns = kbi_def.get('partition_columns', [])
        
        quoted_table = self._quote_identifier(table_name)
        quoted_order = self._quote_identifier(order_column)
        
        partition_clause = ""
        if partition_columns:
            quoted_partitions = [self._quote_identifier(col) for col in partition_columns]
            partition_clause = f"PARTITION BY {', '.join(quoted_partitions)} "
        
        return f"ROW_NUMBER() OVER ({partition_clause}ORDER BY {quoted_table}.{quoted_order})"
    
    def _build_rank(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build RANK window function"""
        order_column = kbi_def.get('order_column', column_name)
        partition_columns = kbi_def.get('partition_columns', [])
        
        quoted_table = self._quote_identifier(table_name)
        quoted_order = self._quote_identifier(order_column)
        
        partition_clause = ""
        if partition_columns:
            quoted_partitions = [self._quote_identifier(col) for col in partition_columns]
            partition_clause = f"PARTITION BY {', '.join(quoted_partitions)} "
        
        return f"RANK() OVER ({partition_clause}ORDER BY {quoted_table}.{quoted_order})"
    
    def _build_dense_rank(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """Build DENSE_RANK window function"""
        order_column = kbi_def.get('order_column', column_name)
        partition_columns = kbi_def.get('partition_columns', [])
        
        quoted_table = self._quote_identifier(table_name)
        quoted_order = self._quote_identifier(order_column)
        
        partition_clause = ""
        if partition_columns:
            quoted_partitions = [self._quote_identifier(col) for col in partition_columns]
            partition_clause = f"PARTITION BY {', '.join(quoted_partitions)} "
        
        return f"DENSE_RANK() OVER ({partition_clause}ORDER BY {quoted_table}.{quoted_order})"

    def _build_exception_aggregation(self, column_name: str, table_name: str, kbi_def: Dict) -> str:
        """
        Build exception aggregation with proper 3-step pattern

        Mirrors reference KbiProvider._calculate_exceptional_aggregation_kbi:

        Step 1: Calculate at target + exception fields level (inner subquery)
        Step 2: Apply formula on calculated values (middle calculation)
        Step 3: Aggregate back to target level (outer query)

        Args:
            column_name: Column or formula to aggregate
            table_name: Source table
            kbi_def: Full KBI definition with exception aggregation settings

        Returns:
            Complete SQL subquery string for exception aggregation
        """
        exception_agg_type = kbi_def.get('exception_aggregation', 'sum').upper()
        exception_fields = kbi_def.get('fields_for_exception_aggregation', [])
        target_columns = kbi_def.get('target_columns', [])  # Columns we want final result at
        formula = kbi_def.get('formula', column_name)

        quoted_table = self._quote_identifier(table_name)

        if not exception_fields:
            # Fallback to regular aggregation if no exception fields specified
            return f"SUM({self._quote_identifier(column_name)})"

        # Quote all fields
        quoted_exception_fields = [self._quote_identifier(field) for field in exception_fields]
        quoted_target_fields = [self._quote_identifier(field) for field in target_columns] if target_columns else []

        # STEP 1: Inner subquery - Calculate base value at exception granularity
        # This is equivalent to: df.select(*target_columns, *exception_fields, calc_value)
        inner_select_fields = []

        if quoted_target_fields:
            inner_select_fields.extend(quoted_target_fields)

        inner_select_fields.extend(quoted_exception_fields)

        # Handle complex formulas vs simple column references
        if self._is_complex_formula(formula):
            # Complex formula - use as-is
            calc_expression = f"({formula}) AS calc_value"
        else:
            # Simple column reference
            quoted_column = self._quote_identifier(formula)
            calc_expression = f"{quoted_table}.{quoted_column} AS calc_value"

        inner_query_select = ", ".join(inner_select_fields + [calc_expression])

        # STEP 2: Middle aggregation - Aggregate at exception level
        # This is equivalent to: df.groupBy(*target_columns, *exception_fields).agg(...)
        middle_group_by = inner_select_fields  # Group by target + exception fields
        middle_agg_func = self._map_exception_aggregation_to_sql(exception_agg_type)

        middle_select_fields = []
        if quoted_target_fields:
            middle_select_fields.extend(quoted_target_fields)
        middle_select_fields.extend(quoted_exception_fields)
        middle_select = ", ".join(middle_select_fields)

        # STEP 3: Outer query - Aggregate back to target level only
        # This is equivalent to: df.groupBy(*target_columns).agg(exception_agg_expression)
        if quoted_target_fields:
            outer_group_by = quoted_target_fields
            outer_select = ", ".join(quoted_target_fields)
            outer_agg = f"{middle_agg_func}(agg_value) AS {self._quote_identifier('result')}"

            # Build complete 3-level query
            return f"""
SELECT {outer_select}, {outer_agg}
FROM (
    SELECT {middle_select}, {middle_agg_func}(calc_value) AS agg_value
    FROM (
        SELECT {inner_query_select}
        FROM {quoted_table}
    ) AS base_calc
    GROUP BY {", ".join(middle_group_by)}
) AS exception_agg
GROUP BY {", ".join(outer_group_by)}"""
        else:
            # No target columns - just aggregate at exception level
            return f"""
SELECT {middle_select}, {middle_agg_func}(calc_value) AS {self._quote_identifier('result')}
FROM (
    SELECT {inner_query_select}
    FROM {quoted_table}
) AS base_calc
GROUP BY {", ".join(middle_group_by)}"""

    def _is_complex_formula(self, formula: str) -> bool:
        """Check if formula is complex (contains operators, functions) vs simple column reference"""
        if not formula:
            return False

        # Simple column pattern: alphanumeric and underscores only
        simple_pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'

        return not bool(re.match(simple_pattern, formula.strip()))

    def _map_exception_aggregation_to_sql(self, exception_agg_type: str) -> str:
        """Map exception aggregation type to SQL function"""
        mapping = {
            'SUM': 'SUM',
            'AVG': 'AVG',
            'COUNT': 'COUNT',
            'MIN': 'MIN',
            'MAX': 'MAX'
        }
        return mapping.get(exception_agg_type, 'SUM')

    def _supports_window_functions(self) -> bool:
        """Check if the dialect supports window functions"""
        # DATABRICKS and STANDARD both support window functions
        return True
    
    def build_conditional_aggregation(self,
                                    base_aggregation: str,
                                    conditions: List[str],
                                    table_name: str) -> str:
        """Build conditional aggregation with CASE WHEN logic"""
        if not conditions:
            return base_aggregation
        
        # Combine conditions with AND
        combined_condition = " AND ".join(conditions)
        
        # Extract the aggregation function and column from base aggregation
        # This is a simplified approach - real implementation would be more robust
        if self.dialect == SQLDialect.DATABRICKS:
            # Use FILTER clause where supported
            return f"{base_aggregation} FILTER (WHERE {combined_condition})"
        else:
            # Use CASE WHEN for other dialects
            # Extract column reference from base aggregation
            column_pattern = r'\(([^)]+)\)'
            match = re.search(column_pattern, base_aggregation)
            
            if match:
                column_ref = match.group(1)
                agg_function = base_aggregation[:base_aggregation.find('(')]
                case_expr = f"CASE WHEN {combined_condition} THEN {column_ref} ELSE NULL END"
                return f"{agg_function}({case_expr})"
            else:
                return base_aggregation
    
    def build_exception_handling(self,
                               base_expression: str,
                               exceptions: List[Dict[str, Any]]) -> str:
        """Build SQL with exception handling"""
        result = base_expression
        
        for exception in exceptions:
            exception_type = exception.get('type')
            
            if exception_type == 'null_to_zero':
                result = f"COALESCE({result}, 0)"
            
            elif exception_type == 'division_by_zero':
                result = f"CASE WHEN {result} IS NULL OR {result} = 0 THEN 0 ELSE {result} END"
            
            elif exception_type == 'negative_to_zero':
                result = f"GREATEST(0, {result})"
            
            elif exception_type == 'threshold':
                threshold_value = exception.get('value', 0)
                comparison = exception.get('comparison', 'min')
                if comparison == 'min':
                    result = f"GREATEST({threshold_value}, {result})"
                elif comparison == 'max':
                    result = f"LEAST({threshold_value}, {result})"
            
            elif exception_type == 'custom_condition':
                condition = exception.get('condition', '')
                true_value = exception.get('true_value', result)
                false_value = exception.get('false_value', '0')
                result = f"CASE WHEN {condition} THEN {true_value} ELSE {false_value} END"
        
        return result


class SQLFilterProcessor:
    """Processes filters for SQL WHERE clauses"""
    
    def __init__(self, dialect: SQLDialect = SQLDialect.STANDARD):
        self.dialect = dialect
    
    def process_filters(self,
                       filters: List[str],
                       variables: Dict[str, Any] = None,
                       definition_filters: Dict[str, Any] = None) -> List[str]:
        """Process a list of filters for SQL"""
        if variables is None:
            variables = {}
        if definition_filters is None:
            definition_filters = {}

        processed_filters = []

        for filter_condition in filters:
            try:
                # Handle special $query_filter expansion
                if filter_condition.strip() == "$query_filter":
                    with open('/tmp/sql_debug.log', 'a') as f:
                        f.write(f"SQLFilterProcessor: Found $query_filter, definition_filters = {definition_filters}\n")

                    # Expand $query_filter into individual filter conditions
                    if 'query_filter' in definition_filters:
                        query_filters = definition_filters['query_filter']
                        with open('/tmp/sql_debug.log', 'a') as f:
                            f.write(f"SQLFilterProcessor: Expanding query_filters = {query_filters}\n")

                        if isinstance(query_filters, dict):
                            for filter_name, filter_value in query_filters.items():
                                processed = self._process_single_filter(filter_value, variables)
                                with open('/tmp/sql_debug.log', 'a') as f:
                                    f.write(f"SQLFilterProcessor: Processed {filter_name}: {filter_value} -> {processed}\n")
                                if processed:
                                    processed_filters.append(processed)
                        else:
                            processed = self._process_single_filter(str(query_filters), variables)
                            if processed:
                                processed_filters.append(processed)
                    else:
                        with open('/tmp/sql_debug.log', 'a') as f:
                            f.write(f"SQLFilterProcessor: No 'query_filter' found in definition_filters\n")
                    continue

                processed = self._process_single_filter(filter_condition, variables)
                if processed:
                    processed_filters.append(processed)
            except Exception as e:
                # Log error but continue processing other filters
                continue

        return processed_filters
    
    def _process_single_filter(self,
                             filter_condition: str,
                             variables: Dict[str, Any]) -> str:
        """Process a single filter condition"""
        condition = filter_condition.strip()
        
        # Substitute variables
        condition = self._substitute_variables(condition, variables)
        
        # Convert DAX/SAP BW syntax to SQL
        condition = self._convert_to_sql_syntax(condition)
        
        # Handle dialect-specific syntax
        condition = self._apply_dialect_specific_syntax(condition)
        
        return condition
    
    def _substitute_variables(self, condition: str, variables: Dict[str, Any]) -> str:
        """Substitute variables in filter conditions"""
        result = condition
        
        for var_name, var_value in variables.items():
            # Handle both $var_name and $name formats
            patterns = [f"\\$var_{var_name}", f"\\${var_name}"]
            
            for pattern in patterns:
                if isinstance(var_value, list):
                    # Handle list variables for IN clauses
                    if isinstance(var_value[0], str):
                        quoted_values = [f"'{v}'" for v in var_value]
                    else:
                        quoted_values = [str(v) for v in var_value]
                    replacement = f"({', '.join(quoted_values)})"
                else:
                    # Handle single variables
                    if isinstance(var_value, str):
                        replacement = f"'{var_value}'"
                    else:
                        replacement = str(var_value)
                
                result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
        return result
    
    def _convert_to_sql_syntax(self, condition: str) -> str:
        """Convert DAX/SAP BW syntax to SQL"""
        # Handle NOT IN
        condition = re.sub(r'\bNOT\s+IN\s*\(',
                          'NOT IN (',
                          condition,
                          flags=re.IGNORECASE)
        
        # Handle BETWEEN
        condition = re.sub(r'\bBETWEEN\s+([\'"][^\'"]*[\'"])\s+AND\s+([\'"][^\'"]*[\'"])',
                          r'BETWEEN \1 AND \2',
                          condition,
                          flags=re.IGNORECASE)
        
        # Convert AND/OR (if they need conversion for specific dialects)
        condition = condition.replace(' AND ', ' AND ')
        condition = condition.replace(' OR ', ' OR ')
        
        return condition
    
    def _apply_dialect_specific_syntax(self, condition: str) -> str:
        """Apply dialect-specific syntax modifications"""
        # DATABRICKS and STANDARD use compatible syntax - no modifications needed
        return condition


def detect_and_build_sql_aggregation(kbi_definition: Dict[str, Any],
                                    dialect: SQLDialect = SQLDialect.STANDARD) -> str:
    """
    Main function to detect aggregation type and build SQL expression

    Args:
        kbi_definition: Full KPI definition dictionary
        dialect: Target SQL dialect

    Returns:
        Complete SQL aggregation expression
    """
    formula = kbi_definition.get('formula', '')
    source_table = kbi_definition.get('source_table', 'fact_table')
    aggregation_hint = kbi_definition.get('aggregation_type')

    # Detect SQL aggregation type
    sql_agg_type = _detect_sql_aggregation_type(formula, aggregation_hint)

    # Build base aggregation
    builder = SQLAggregationBuilder(dialect)
    base_sql = builder.build_aggregation(sql_agg_type, formula, source_table, kbi_definition)

    # Exception aggregation returns complete SELECT statement, so handle differently
    if sql_agg_type == SQLAggregationType.EXCEPTION_AGGREGATION:
        # Apply display sign before returning
        display_sign = kbi_definition.get('display_sign', 1)
        if display_sign == -1:
            # Wrap the entire subquery aggregation in a negative sign
            base_sql = f"(-1) * ({base_sql})"
        elif display_sign != 1:
            base_sql = f"{display_sign} * ({base_sql})"
        return base_sql

    # Handle exceptions for regular aggregations
    exceptions = kbi_definition.get('exceptions', [])
    if exceptions:
        base_sql = builder.build_exception_handling(base_sql, exceptions)

    # Apply display sign for regular aggregations
    display_sign = kbi_definition.get('display_sign', 1)
    if display_sign == -1:
        base_sql = f"(-1) * ({base_sql})"
    elif display_sign != 1:
        base_sql = f"{display_sign} * ({base_sql})"

    return base_sql


def _detect_sql_aggregation_type(formula: str, aggregation_hint: str = None) -> SQLAggregationType:
    """Detect SQL aggregation type from formula or hint"""
    if aggregation_hint:
        # Map DAX aggregation types to SQL
        dax_to_sql_mapping = {
            'SUM': SQLAggregationType.SUM,
            'COUNT': SQLAggregationType.COUNT,
            'COUNTROWS': SQLAggregationType.COUNT,
            'AVERAGE': SQLAggregationType.AVG,
            'MIN': SQLAggregationType.MIN,
            'MAX': SQLAggregationType.MAX,
            'DISTINCTCOUNT': SQLAggregationType.COUNT_DISTINCT,
            # Enhanced aggregations
            'DIVIDE': SQLAggregationType.RATIO,
            'WEIGHTED_AVERAGE': SQLAggregationType.WEIGHTED_AVG,
            'VARIANCE': SQLAggregationType.VARIANCE,
            'PERCENTILE': SQLAggregationType.PERCENTILE,
            'SUMX': SQLAggregationType.SUM,  # SUMX maps to SUM for SQL
            'EXCEPTION_AGGREGATION': SQLAggregationType.EXCEPTION_AGGREGATION,
        }
        
        return dax_to_sql_mapping.get(aggregation_hint.upper(), SQLAggregationType.SUM)
    
    # Detect from formula
    if not formula:
        return SQLAggregationType.SUM
    
    formula_upper = formula.upper()
    
    if 'COUNT' in formula_upper:
        if 'DISTINCT' in formula_upper:
            return SQLAggregationType.COUNT_DISTINCT
        else:
            return SQLAggregationType.COUNT
    elif 'AVG' in formula_upper or 'AVERAGE' in formula_upper:
        return SQLAggregationType.AVG
    elif 'MIN' in formula_upper:
        return SQLAggregationType.MIN
    elif 'MAX' in formula_upper:
        return SQLAggregationType.MAX
    else:
        return SQLAggregationType.SUM