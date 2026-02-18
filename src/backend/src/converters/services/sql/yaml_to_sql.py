"""
SQL Generator for YAML2DAX SQL Translation
Converts KPI definitions to SQL queries for various SQL dialects
"""

from typing import List, Dict, Any, Optional, Tuple
import re
import logging
from ...base.models import KPI, KPIDefinition
from .models import (
    SQLDialect, SQLAggregationType, SQLQuery, SQLMeasure, SQLDefinition,
    SQLTranslationOptions, SQLTranslationResult, SQLStructure
)
from .helpers.sql_structures import SQLStructureExpander


class SQLGenerator:
    """Base SQL generator for converting KPI definitions to SQL queries"""

    def __init__(self, dialect: SQLDialect = SQLDialect.DATABRICKS):
        self.dialect = dialect
        self.logger = logging.getLogger(__name__)

        # Dialect-specific configurations
        self.dialect_config = self._get_dialect_config()

        # Initialize SQL structure processor for improved SQL generation
        self.structure_processor = SQLStructureExpander(dialect)
    
    def _get_dialect_config(self) -> Dict[str, Any]:
        """
        Get dialect-specific configuration.

        Supports:
        - DATABRICKS (primary): Databricks SQL / Spark SQL with Unity Catalog
        - STANDARD (fallback): ANSI SQL standard for compatibility
        """
        configs = {
            SQLDialect.DATABRICKS: {
                "quote_char": "`",
                "limit_syntax": "LIMIT",
                "supports_cte": True,
                "supports_window_functions": True,
                "date_format": "yyyy-MM-dd",
                "string_concat": "||",
                "case_sensitive": False,
                "unity_catalog": True,
            },
            SQLDialect.STANDARD: {
                "quote_char": '"',
                "limit_syntax": "LIMIT",
                "supports_cte": True,
                "supports_window_functions": True,
                "date_format": "YYYY-MM-DD",
                "string_concat": "||",
                "case_sensitive": True,
            },
        }

        return configs.get(self.dialect, configs[SQLDialect.DATABRICKS])
    
    def quote_identifier(self, identifier: str) -> str:
        """Quote an identifier according to dialect"""
        quote_start = self.dialect_config["quote_char"]
        quote_end = self.dialect_config.get("quote_char_end", quote_start)
        return f"{quote_start}{identifier}{quote_end}"
    
    def generate_sql_from_kbi_definition(self, 
                                       definition: KPIDefinition, 
                                       options: SQLTranslationOptions = None) -> SQLTranslationResult:
        """
        Generate SQL translation from KPI definition using improved structure processor
        
        Args:
            definition: KPI definition to translate
            options: Translation options
            
        Returns:
            SQLTranslationResult with translated SQL queries and measures
        """
        if options is None:
            options = SQLTranslationOptions(target_dialect=self.dialect)
        
        try:
            # Use the improved structure processor for comprehensive SQL generation
            sql_definition = self.structure_processor.process_definition(definition, options)
            
            # Generate SQL queries using the structure processor
            sql_queries = self.structure_processor.generate_sql_queries_from_definition(sql_definition, options)
            
            # Create result with comprehensive data
            result = SQLTranslationResult(
                sql_queries=sql_queries,
                sql_measures=sql_definition.sql_measures,
                sql_definition=sql_definition,
                translation_options=options,
                measures_count=len(sql_definition.sql_measures),
                queries_count=len(sql_queries),
                syntax_valid=True,
                estimated_complexity=self._estimate_complexity(sql_definition)
            )
            
            # Add validation and optimization suggestions
            result = self._enhance_result_with_analysis(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error generating SQL from KPI definition: {str(e)}")
            # Return minimal error result
            return SQLTranslationResult(
                sql_queries=[],
                sql_measures=[],
                sql_definition=SQLDefinition(
                    description=definition.description,
                    technical_name=definition.technical_name,
                    dialect=self.dialect
                ),
                translation_options=options,
                measures_count=0,
                queries_count=0,
                syntax_valid=False,
                validation_messages=[f"Generation failed: {str(e)}"]
            )
        
    def _estimate_complexity(self, sql_definition: SQLDefinition) -> str:
        """Estimate the complexity of the SQL definition"""
        measure_count = len(sql_definition.sql_measures)
        has_structures = bool(sql_definition.sql_structures)
        has_filters = any(measure.filters for measure in sql_definition.sql_measures)
        
        if measure_count > 10 or has_structures:
            return "HIGH"
        elif measure_count > 5 or has_filters:
            return "MEDIUM"
        else:
            return "LOW"
    
    def _enhance_result_with_analysis(self, result: SQLTranslationResult) -> SQLTranslationResult:
        """Add validation and optimization suggestions to the result"""
        validation_messages = []
        optimization_suggestions = []
        
        # Validate SQL queries
        for query in result.sql_queries:
            sql_text = query.to_sql()
            
            # Basic validation
            if not sql_text or not sql_text.strip():
                validation_messages.append("Empty SQL query generated")
                result.syntax_valid = False
            elif "SELECT" not in sql_text.upper():
                validation_messages.append("SQL query missing SELECT clause")
                result.syntax_valid = False
            elif "FROM" not in sql_text.upper():
                validation_messages.append("SQL query missing FROM clause")
                result.syntax_valid = False
            
            # Check for unresolved variables
            if "$" in sql_text:
                validation_messages.append("SQL contains unresolved variables")
                optimization_suggestions.append("Ensure all variables are properly defined in default_variables")
        
        # Performance optimization suggestions
        if len(result.sql_measures) > 5:
            optimization_suggestions.append("Consider using CTEs for better readability with many measures")
        
        if any(len(measure.filters) > 3 for measure in result.sql_measures):
            optimization_suggestions.append("Consider creating filtered views for complex filter conditions")
        
        # Update result with findings
        result.validation_messages.extend(validation_messages)
        result.optimization_suggestions.extend(optimization_suggestions)
        
        return result
    
    def _create_sql_definition(self, definition: KPIDefinition) -> SQLDefinition:
        """Create SQL definition from KPI definition"""
        return SQLDefinition(
            description=definition.description,
            technical_name=definition.technical_name,
            dialect=self.dialect,
            default_variables=definition.default_variables,
            original_kbis=definition.kpis,
        )
    
    def _translate_kbi_to_sql_measure(self,
                                    kpi: KPI,
                                    definition: KPIDefinition,
                                    options: SQLTranslationOptions) -> SQLMeasure:
        """
        Translate a single KPI to SQL measure

        Args:
            kpi: KPI to translate
            definition: Full KPI definition for context
            options: Translation options

        Returns:
            SQLMeasure object
        """
        # Determine SQL aggregation type
        sql_agg_type = self._map_aggregation_type(kpi.aggregation_type, kpi.formula)

        # Generate SQL expression
        sql_expression = self._generate_sql_expression(kpi, sql_agg_type, definition)

        # Process filters
        sql_filters = self._process_filters(kpi.filters, definition, options)

        # Create SQL measure
        sql_measure = SQLMeasure(
            name=kpi.description or kpi.technical_name or "Unnamed Measure",
            description=kpi.description or "",
            sql_expression=sql_expression,
            aggregation_type=sql_agg_type,
            source_table=kpi.source_table or "fact_table",
            source_column=kpi.formula if self._is_simple_column_reference(kpi.formula) else None,
            filters=sql_filters,
            display_sign=kpi.display_sign,
            technical_name=kpi.technical_name or "",
            original_kbi=kpi,
            dialect=self.dialect
        )
        
        return sql_measure
    
    def _map_aggregation_type(self, dax_agg_type: str, formula: str) -> SQLAggregationType:
        """Map DAX aggregation type to SQL aggregation type"""
        if not dax_agg_type:
            # Infer from formula
            formula_upper = formula.upper() if formula else ""
            if "COUNT" in formula_upper:
                return SQLAggregationType.COUNT
            elif "AVG" in formula_upper or "AVERAGE" in formula_upper:
                return SQLAggregationType.AVG
            elif "MIN" in formula_upper:
                return SQLAggregationType.MIN
            elif "MAX" in formula_upper:
                return SQLAggregationType.MAX
            else:
                return SQLAggregationType.SUM
        
        # Direct mapping
        mapping = {
            "SUM": SQLAggregationType.SUM,
            "COUNT": SQLAggregationType.COUNT,
            "AVERAGE": SQLAggregationType.AVG,
            "MIN": SQLAggregationType.MIN,
            "MAX": SQLAggregationType.MAX,
            "DISTINCTCOUNT": SQLAggregationType.COUNT_DISTINCT,
            "COUNTROWS": SQLAggregationType.COUNT,
            "CALCULATED": SQLAggregationType.SUM,  # Default for calculated measures
        }
        
        return mapping.get(dax_agg_type.upper(), SQLAggregationType.SUM)
    
    def _generate_sql_expression(self,
                                kpi: KPI,
                                sql_agg_type: SQLAggregationType,
                                definition: KPIDefinition) -> str:
        """Generate SQL expression for the measure"""
        formula = kpi.formula or ""
        source_table = kpi.source_table or "fact_table"
        
        # Handle different aggregation types
        if sql_agg_type == SQLAggregationType.SUM:
            if self._is_simple_column_reference(formula):
                return f"SUM({self.quote_identifier(source_table)}.{self.quote_identifier(formula)})"
            else:
                return f"SUM({self._convert_formula_to_sql(formula, source_table, definition)})"
        
        elif sql_agg_type == SQLAggregationType.COUNT:
            if formula and formula.upper() != "*":
                return f"COUNT({self.quote_identifier(source_table)}.{self.quote_identifier(formula)})"
            else:
                return f"COUNT(*)"
        
        elif sql_agg_type == SQLAggregationType.COUNT_DISTINCT:
            column = formula if self._is_simple_column_reference(formula) else "*"
            if column != "*":
                return f"COUNT(DISTINCT {self.quote_identifier(source_table)}.{self.quote_identifier(column)})"
            else:
                return f"COUNT(DISTINCT {self.quote_identifier(source_table)}.id)"  # Fallback
        
        elif sql_agg_type == SQLAggregationType.AVG:
            if self._is_simple_column_reference(formula):
                return f"AVG({self.quote_identifier(source_table)}.{self.quote_identifier(formula)})"
            else:
                return f"AVG({self._convert_formula_to_sql(formula, source_table, definition)})"
        
        elif sql_agg_type == SQLAggregationType.MIN:
            return f"MIN({self.quote_identifier(source_table)}.{self.quote_identifier(formula)})"
        
        elif sql_agg_type == SQLAggregationType.MAX:
            return f"MAX({self.quote_identifier(source_table)}.{self.quote_identifier(formula)})"
        
        else:
            # Default to SUM
            return f"SUM({self.quote_identifier(source_table)}.{self.quote_identifier(formula)})"
    
    def _is_simple_column_reference(self, formula: str) -> bool:
        """Check if formula is a simple column reference"""
        if not formula:
            return False
        
        # Simple column names or bic_ prefixed columns
        pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
        return bool(re.match(pattern, formula.strip()))
    
    def _convert_formula_to_sql(self, 
                               formula: str, 
                               source_table: str,
                               definition: KPIDefinition) -> str:
        """Convert DAX-style formula to SQL expression"""
        if not formula:
            return "1"
        
        # Handle CASE WHEN expressions
        if "CASE WHEN" in formula.upper():
            return self._convert_case_when_to_sql(formula, source_table)
        
        # Handle IF expressions (DAX style)
        if formula.upper().startswith("IF("):
            return self._convert_if_to_case_when(formula, source_table)
        
        # Handle arithmetic expressions with column references
        sql_formula = formula
        
        # Replace column references with table.column format
        column_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
        def replace_column(match):
            column_name = match.group(1)
            # Skip SQL keywords and functions
            sql_keywords = {'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN'}
            if column_name.upper() not in sql_keywords and not column_name.isdigit():
                return f"{self.quote_identifier(source_table)}.{self.quote_identifier(column_name)}"
            return column_name
        
        sql_formula = re.sub(column_pattern, replace_column, sql_formula)
        
        return sql_formula
    
    def _convert_case_when_to_sql(self, formula: str, source_table: str) -> str:
        """Convert CASE WHEN expressions to SQL"""
        # CASE WHEN is already SQL, just need to update column references
        return self._convert_formula_to_sql(formula, source_table, None)
    
    def _convert_if_to_case_when(self, formula: str, source_table: str) -> str:
        """Convert DAX IF() to SQL CASE WHEN"""
        # Simple IF(condition, true_value, false_value) to CASE WHEN conversion
        # This is a basic implementation - real conversion would be more complex
        if_pattern = r'IF\s*\(\s*([^,]+),\s*([^,]+),\s*([^)]+)\)'
        
        def convert_if(match):
            condition = match.group(1).strip()
            true_value = match.group(2).strip()
            false_value = match.group(3).strip()
            
            # Convert condition to SQL
            sql_condition = self._convert_formula_to_sql(condition, source_table, None)
            sql_true = self._convert_formula_to_sql(true_value, source_table, None)
            sql_false = self._convert_formula_to_sql(false_value, source_table, None)
            
            return f"CASE WHEN {sql_condition} THEN {sql_true} ELSE {sql_false} END"
        
        return re.sub(if_pattern, convert_if, formula, flags=re.IGNORECASE)
    
    def _process_filters(self, 
                        filters: List[str], 
                        definition: KPIDefinition,
                        options: SQLTranslationOptions) -> List[str]:
        """Process and convert filters to SQL WHERE conditions"""
        sql_filters = []
        
        for filter_condition in filters:
            try:
                sql_condition = self._convert_filter_to_sql(filter_condition, definition)
                if sql_condition:
                    sql_filters.append(sql_condition)
            except Exception as e:
                self.logger.warning(f"Could not convert filter '{filter_condition}': {str(e)}")
        
        return sql_filters
    
    def _convert_filter_to_sql(self, filter_condition: str, definition: KPIDefinition) -> str:
        """Convert a single filter condition to SQL"""
        if not filter_condition:
            return ""
        
        condition = filter_condition.strip()
        
        # Handle variable substitution
        condition = self._substitute_variables(condition, definition.default_variables)
        
        # Convert DAX/SAP BW operators to SQL
        # NOT IN
        condition = re.sub(r'NOT\s+IN\s*\(([^)]+)\)', r'NOT IN (\1)', condition, flags=re.IGNORECASE)
        
        # BETWEEN
        condition = re.sub(r'BETWEEN\s+\'([^\']+)\'\s+AND\s+\'([^\']+)\'', 
                          r"BETWEEN '\1' AND '\2'", condition, flags=re.IGNORECASE)
        
        # Convert AND/OR operators
        condition = condition.replace(' AND ', ' AND ').replace(' OR ', ' OR ')
        
        # Ensure proper quoting of string literals
        condition = self._ensure_proper_quoting(condition)
        
        return condition
    
    def _substitute_variables(self, condition: str, variables: Dict[str, Any]) -> str:
        """Substitute variables in filter conditions"""
        result = condition
        
        for var_name, var_value in variables.items():
            var_pattern = f"\\$var_{var_name}|\\${var_name}"
            
            if isinstance(var_value, list):
                # Handle list variables
                quoted_values = [f"'{str(v)}'" for v in var_value]
                replacement = f"({', '.join(quoted_values)})"
            else:
                # Handle single variables
                replacement = f"'{str(var_value)}'"
            
            result = re.sub(var_pattern, replacement, result, flags=re.IGNORECASE)
        
        return result
    
    def _ensure_proper_quoting(self, condition: str) -> str:
        """Ensure proper quoting of string literals in conditions"""
        # This is a simplified implementation
        # In practice, you'd want more sophisticated parsing
        return condition
    
    def _generate_query_for_measure(self, 
                                   measure: SQLMeasure, 
                                   sql_definition: SQLDefinition,
                                   options: SQLTranslationOptions) -> SQLQuery:
        """Generate a complete SQL query for a single measure"""
        
        # Build SELECT clause
        select_expressions = []

        # Add constant selection (grouping) columns FIRST for SAP BW constant selection behavior
        if measure.group_by_columns:
            select_expressions.extend([self.quote_identifier(col) for col in measure.group_by_columns])

        # Add the measure expression
        select_expressions.append(f"{measure.to_sql_expression()} AS {self.quote_identifier(measure.technical_name or 'measure_value')}")
        
        # Build FROM clause
        from_clause = self.quote_identifier(measure.source_table)
        if sql_definition.database_schema:
            schema_part = self.quote_identifier(sql_definition.database_schema)
            from_clause = f"{schema_part}.{from_clause}"
        
        # Create query
        query = SQLQuery(
            dialect=self.dialect,
            select_clause=select_expressions,
            from_clause=from_clause,
            where_clause=measure.filters,
            group_by_clause=measure.group_by_columns,
            description=f"SQL query for measure: {measure.name}",
            original_kbi=measure.original_kbi
        )
        
        return query
    
    def _generate_combined_query(self, 
                               measures: List[SQLMeasure],
                               sql_definition: SQLDefinition,
                               options: SQLTranslationOptions) -> SQLQuery:
        """Generate a combined SQL query for multiple measures"""
        
        # Build SELECT clause with all measures
        select_expressions = []
        common_table = None
        
        for measure in measures:
            # Use alias for each measure
            alias = measure.technical_name or f"measure_{len(select_expressions) + 1}"
            select_expressions.append(f"{measure.to_sql_expression()} AS {self.quote_identifier(alias)}")
            
            # Use first table as base (could be improved with proper join logic)
            if common_table is None:
                common_table = measure.source_table
        
        # Build FROM clause
        from_clause = self.quote_identifier(common_table or "fact_table")
        if sql_definition.database_schema:
            schema_part = self.quote_identifier(sql_definition.database_schema)
            from_clause = f"{schema_part}.{from_clause}"
        
        # Combine all filters (this is simplified - real implementation would handle conflicts)
        all_filters = []
        for measure in measures:
            all_filters.extend(measure.filters)
        
        # Remove duplicates while preserving order
        unique_filters = list(dict.fromkeys(all_filters))
        
        # Create combined query
        query = SQLQuery(
            dialect=self.dialect,
            select_clause=select_expressions,
            from_clause=from_clause,
            where_clause=unique_filters,
            description=f"Combined SQL query for {len(measures)} measures",
        )
        
        return query
    
    def _validate_and_optimize_result(self, result: SQLTranslationResult) -> SQLTranslationResult:
        """Validate SQL syntax and add optimization suggestions"""
        
        # Basic validation
        result.syntax_valid = True
        result.validation_messages = []
        
        for query in result.sql_queries:
            if not query.from_clause:
                result.syntax_valid = False
                result.validation_messages.append("Missing FROM clause")
        
        # Estimate complexity
        total_measures = len(result.sql_measures)
        total_filters = sum(len(m.filters) for m in result.sql_measures)
        
        if total_measures <= 3 and total_filters <= 5:
            result.estimated_complexity = "LOW"
        elif total_measures <= 10 and total_filters <= 15:
            result.estimated_complexity = "MEDIUM"
        else:
            result.estimated_complexity = "HIGH"
        
        # Add optimization suggestions
        result.optimization_suggestions = []
        
        if total_filters > 10:
            result.optimization_suggestions.append("Consider using indexes on filtered columns")
        
        if len(result.sql_queries) > 5:
            result.optimization_suggestions.append("Consider combining queries to reduce database round trips")
        
        if any("DISTINCT" in q.to_sql().upper() for q in result.sql_queries):
            result.optimization_suggestions.append("DISTINCT operations can be expensive - ensure they're necessary")
        
        return result