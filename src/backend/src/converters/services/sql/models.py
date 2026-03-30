"""
SQL-specific models for YAML2DAX SQL translation
Extends the base KBI models with SQL-specific functionality
"""

import re
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Union
from enum import Enum
from ...base.models import KPI, KPIDefinition


class SQLDialect(Enum):
    """
    Supported SQL dialects.

    Primary focus: Databricks SQL (Spark SQL dialect)
    STANDARD is kept as a fallback for ANSI SQL compatibility.
    """
    DATABRICKS = "DATABRICKS"  # Primary: Databricks/Spark SQL
    STANDARD = "ANSI_SQL"       # Fallback: ANSI SQL standard


class SQLAggregationType(Enum):
    """SQL aggregation functions"""
    SUM = "SUM"
    COUNT = "COUNT"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    STDDEV = "STDDEV"
    VARIANCE = "VARIANCE"
    MEDIAN = "MEDIAN"
    PERCENTILE = "PERCENTILE"
    # Window functions
    ROW_NUMBER = "ROW_NUMBER"
    RANK = "RANK"
    DENSE_RANK = "DENSE_RANK"
    # Custom aggregations
    WEIGHTED_AVG = "WEIGHTED_AVG"
    RATIO = "RATIO"
    RUNNING_SUM = "RUNNING_SUM"
    COALESCE = "COALESCE"
    EXCEPTION_AGGREGATION = "EXCEPTION_AGGREGATION"


class SQLJoinType(Enum):
    """SQL join types"""
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"
    CROSS = "CROSS JOIN"


class SQLQuery(BaseModel):
    """Represents a complete SQL query"""
    
    dialect: SQLDialect = SQLDialect.STANDARD
    select_clause: List[str] = Field(default=[])
    from_clause: str = ""
    join_clauses: List[str] = Field(default=[])
    where_clause: List[str] = Field(default=[])
    group_by_clause: List[str] = Field(default=[])
    having_clause: List[str] = Field(default=[])
    order_by_clause: List[str] = Field(default=[])
    limit_clause: Optional[int] = None
    
    # Metadata
    description: str = ""
    original_kbi: Optional[KPI] = None
    
    def to_sql(self, formatted: bool = True) -> str:
        """Generate the complete SQL query string with proper formatting"""
        # Check if we have custom SQL (for complex multi-table queries)
        if hasattr(self, '_custom_sql') and self._custom_sql:
            return self._format_sql(self._custom_sql) if formatted else self._custom_sql
        
        if formatted:
            return self._generate_formatted_sql()
        else:
            return self._generate_compact_sql()
    
    def _generate_formatted_sql(self) -> str:
        """Generate beautifully formatted SQL for copy-pasting"""
        lines = []
        indent = "    "  # 4 spaces for indentation
        
        # SELECT clause with proper formatting
        if self.select_clause:
            if len(self.select_clause) == 1:
                lines.append(f"SELECT {self.select_clause[0]}")
            else:
                lines.append("SELECT")
                for i, col in enumerate(self.select_clause):
                    comma = "," if i < len(self.select_clause) - 1 else ""
                    lines.append(f"{indent}{col}{comma}")
        else:
            lines.append("SELECT *")
        
        # FROM clause
        if self.from_clause:
            lines.append(f"FROM {self.from_clause}")
        
        # JOIN clauses
        for join in self.join_clauses:
            lines.append(join)
        
        # WHERE clause with proper formatting
        if self.where_clause:
            lines.append("WHERE")
            for i, condition in enumerate(self.where_clause):
                if i == 0:
                    lines.append(f"{indent}{condition}")
                else:
                    lines.append(f"{indent}AND {condition}")
        
        # GROUP BY clause
        if self.group_by_clause:
            if len(self.group_by_clause) <= 2:
                lines.append(f"GROUP BY {', '.join(self.group_by_clause)}")
            else:
                lines.append("GROUP BY")
                for i, col in enumerate(self.group_by_clause):
                    comma = "," if i < len(self.group_by_clause) - 1 else ""
                    lines.append(f"{indent}{col}{comma}")
        
        # HAVING clause
        if self.having_clause:
            lines.append("HAVING")
            for i, condition in enumerate(self.having_clause):
                if i == 0:
                    lines.append(f"{indent}{condition}")
                else:
                    lines.append(f"{indent}AND {condition}")
        
        # ORDER BY clause
        if self.order_by_clause:
            if len(self.order_by_clause) <= 2:
                lines.append(f"ORDER BY {', '.join(self.order_by_clause)}")
            else:
                lines.append("ORDER BY")
                for i, col in enumerate(self.order_by_clause):
                    comma = "," if i < len(self.order_by_clause) - 1 else ""
                    lines.append(f"{indent}{col}{comma}")
        
        # LIMIT clause
        if self.limit_clause:
            lines.append(f"LIMIT {self.limit_clause}")
        
        return "\n".join(lines) + ";"
    
    def _generate_compact_sql(self) -> str:
        """Generate compact SQL (original implementation)"""
        sql_parts = []
        
        # SELECT
        if self.select_clause:
            sql_parts.append(f"SELECT {', '.join(self.select_clause)}")
        else:
            sql_parts.append("SELECT *")
        
        # FROM
        if self.from_clause:
            sql_parts.append(f"FROM {self.from_clause}")
        
        # JOINs
        for join in self.join_clauses:
            sql_parts.append(join)
        
        # WHERE
        if self.where_clause:
            where_conditions = " AND ".join(self.where_clause)
            sql_parts.append(f"WHERE {where_conditions}")
        
        # GROUP BY
        if self.group_by_clause:
            sql_parts.append(f"GROUP BY {', '.join(self.group_by_clause)}")
        
        # HAVING
        if self.having_clause:
            having_conditions = " AND ".join(self.having_clause)
            sql_parts.append(f"HAVING {having_conditions}")
        
        # ORDER BY
        if self.order_by_clause:
            sql_parts.append(f"ORDER BY {', '.join(self.order_by_clause)}")
        
        # LIMIT
        if self.limit_clause:
            sql_parts.append(f"LIMIT {self.limit_clause}")
        
        return "\n".join(sql_parts)
    
    def _format_sql(self, sql: str) -> str:
        """Format a custom SQL string for better readability"""
        if not sql:
            return sql
        
        # Handle UNION ALL formatting specially
        if 'UNION ALL' in sql.upper():
            return self._format_union_sql(sql)
        
        # Basic SQL formatting
        lines = []
        current_line = ""
        
        # Split by SQL keywords for basic formatting
        keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'OUTER JOIN']
        
        words = sql.split()
        indent = "    "
        in_select = False
        
        for word in words:
            word_upper = word.upper().rstrip(',();')
            
            if word_upper in keywords:
                if current_line.strip():
                    lines.append(current_line.strip())
                    current_line = ""
                
                if word_upper == 'SELECT':
                    in_select = True
                    current_line = word + " "
                elif word_upper in ['FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT']:
                    in_select = False
                    lines.append(word)
                    current_line = indent
                else:
                    lines.append(word)
                    current_line = indent
            else:
                current_line += word + " "
        
        if current_line.strip():
            lines.append(current_line.strip())
        
        # Add semicolon if not present
        formatted_sql = "\n".join(lines)
        if not formatted_sql.strip().endswith(';'):
            formatted_sql += ";"
            
        return formatted_sql
    
    def _format_union_sql(self, sql: str) -> str:
        """Format SQL with UNION ALL statements for better readability"""
        # Split by UNION ALL
        parts = re.split(r'\s+UNION\s+ALL\s+', sql, flags=re.IGNORECASE)
        
        formatted_parts = []
        for i, part in enumerate(parts):
            # Format each SELECT statement
            formatted_part = self._format_single_select(part.strip())
            formatted_parts.append(formatted_part)
        
        # Join with nicely formatted UNION ALL
        result = '\n\nUNION ALL\n\n'.join(formatted_parts)
        
        # Add semicolon if not present
        if not result.strip().endswith(';'):
            result += ";"
        
        return result
    
    def _format_single_select(self, sql: str) -> str:
        """Format a single SELECT statement"""
        lines = []
        indent = "    "
        
        # Split into tokens and rebuild with formatting
        tokens = sql.split()
        current_line = ""
        in_select = False
        in_from = False
        
        i = 0
        while i < len(tokens):
            token = tokens[i]
            token_upper = token.upper().rstrip(',();')
            
            if token_upper == 'SELECT':
                in_select = True
                current_line = token + " "
            elif token_upper == 'FROM':
                if current_line.strip():
                    lines.append(current_line.strip())
                lines.append("FROM")
                current_line = indent
                in_select = False
                in_from = True
            elif token_upper == 'WHERE':
                if current_line.strip():
                    lines.append(current_line.strip())
                lines.append("WHERE")
                current_line = indent
                in_from = False
            elif token_upper in ['GROUP', 'ORDER', 'HAVING', 'LIMIT']:
                if current_line.strip():
                    lines.append(current_line.strip())
                if i + 1 < len(tokens) and tokens[i + 1].upper() == 'BY':
                    lines.append(f"{token} {tokens[i + 1]}")
                    i += 1  # skip the 'BY'
                else:
                    lines.append(token)
                current_line = indent
            elif token_upper == 'AND' and not in_select and not in_from:
                if current_line.strip():
                    lines.append(current_line.strip())
                current_line = indent + "AND "
            else:
                current_line += token + " "
            
            i += 1
        
        if current_line.strip():
            lines.append(current_line.strip())
        
        return "\n".join(lines)


class SQLMeasure(BaseModel):
    """Represents a SQL measure/metric"""
    
    name: str
    description: str = ""
    sql_expression: str
    aggregation_type: SQLAggregationType
    source_table: str
    source_column: Optional[str] = None
    
    # Filters and conditions
    filters: List[str] = Field(default=[])
    group_by_columns: List[str] = Field(default=[])
    
    # Formatting and display
    display_format: Optional[str] = None
    display_sign: int = 1
    
    # Metadata
    technical_name: str = ""
    original_kbi: Optional[KPI] = None
    dialect: SQLDialect = SQLDialect.STANDARD
    
    def to_sql_expression(self) -> str:
        """Generate SQL expression for this measure"""
        base_expression = self.sql_expression
        
        # Apply display sign
        if self.display_sign == -1:
            base_expression = f"(-1) * ({base_expression})"
        elif self.display_sign != 1:
            base_expression = f"{self.display_sign} * ({base_expression})"
        
        return base_expression
    
    def to_case_statement(self) -> str:
        """Generate CASE statement for conditional logic"""
        if not self.filters:
            return self.to_sql_expression()
        
        # Build CASE WHEN statement with filters
        conditions = " AND ".join(self.filters)
        return f"CASE WHEN {conditions} THEN {self.to_sql_expression()} ELSE NULL END"


class SQLStructure(BaseModel):
    """SQL equivalent of SAP BW structures"""
    
    description: str
    sql_template: Optional[str] = None  # SQL template with placeholders
    joins: List[str] = Field(default=[])
    filters: List[str] = Field(default=[])
    group_by: List[str] = Field(default=[])
    having_conditions: List[str] = Field(default=[])
    
    # Time intelligence specific
    date_column: Optional[str] = None
    date_filters: List[str] = Field(default=[])
    
    # For structure formulas that reference other structures
    formula: Optional[str] = None
    referenced_structures: List[str] = Field(default=[])
    
    display_sign: int = 1


class SQLDefinition(BaseModel):
    """SQL equivalent of KPIDefinition"""
    
    description: str
    technical_name: str
    dialect: SQLDialect = SQLDialect.STANDARD

    # Connection information
    database: Optional[str] = None
    database_schema: Optional[str] = None  # Renamed from 'schema' to avoid Pydantic conflict
    
    # Variables for SQL parameterization
    default_variables: Dict[str, Any] = Field(default={})
    
    # Filters section from YAML (like query_filter with nested filters)
    filters: Optional[Dict[str, Dict[str, str]]] = None
    
    # SQL structures (equivalent to SAP BW structures)
    sql_structures: Optional[Dict[str, SQLStructure]] = None
    
    # Common table expressions
    ctes: List[str] = Field(default=[])
    
    # SQL measures
    sql_measures: List[SQLMeasure] = Field(default=[])
    
    # Original KBI data for reference
    original_kbis: List[KPI] = Field(default=[])
    
    def get_full_table_name(self, table_name: str) -> str:
        """Get fully qualified table name"""
        parts = []
        if self.database:
            parts.append(self.database)
        if self.database_schema:
            parts.append(self.database_schema)
        parts.append(table_name)
        
        # DATABRICKS and STANDARD both use dot notation
        return ".".join(parts) if len(parts) > 1 else table_name


class SQLTranslationOptions(BaseModel):
    """Options for SQL translation"""
    
    target_dialect: SQLDialect = SQLDialect.STANDARD
    include_comments: bool = True
    format_output: bool = True
    use_ctes: bool = False
    generate_select_statement: bool = True
    include_metadata: bool = True
    
    # Aggregation options
    use_window_functions: bool = False
    include_null_handling: bool = True
    optimize_for_performance: bool = True
    
    # Structure processing
    expand_structures: bool = True
    inline_structure_logic: bool = False
    
    # Output options
    separate_measures: bool = False  # Generate separate queries for each measure
    create_view_statements: bool = False
    include_data_types: bool = False


class SQLTranslationResult(BaseModel):
    """Result of SQL translation"""
    
    sql_queries: List[SQLQuery] = Field(default=[])
    sql_measures: List[SQLMeasure] = Field(default=[])
    sql_definition: SQLDefinition
    
    # Metadata
    translation_options: SQLTranslationOptions
    measures_count: int = 0
    queries_count: int = 0
    
    # Validation
    syntax_valid: bool = True
    validation_messages: List[str] = Field(default=[])
    
    # Performance info
    estimated_complexity: str = "LOW"  # LOW, MEDIUM, HIGH
    optimization_suggestions: List[str] = Field(default=[])
    
    def get_primary_query(self, formatted: bool = True) -> Optional[str]:
        """Get the main SQL query as a string"""
        if self.sql_queries:
            return self.sql_queries[0].to_sql(formatted=formatted)
        return None
    
    def get_all_sql_statements(self, formatted: bool = True) -> List[str]:
        """Get all SQL statements as strings"""
        statements = []
        
        # Add any CREATE VIEW statements if requested
        if self.translation_options.create_view_statements:
            for i, query in enumerate(self.sql_queries):
                view_name = f"vw_{query.original_kbi.technical_name if query.original_kbi else f'measure_{i+1}'}"
                formatted_query = query.to_sql(formatted=formatted)
                statements.append(f"CREATE OR REPLACE VIEW {view_name} AS\n{formatted_query}")
        
        # Add the main queries
        for query in self.sql_queries:
            statements.append(query.to_sql(formatted=formatted))
        
        return statements
    
    def get_formatted_sql_output(self) -> str:
        """Get beautifully formatted SQL output ready for copy-pasting"""
        if not self.sql_queries:
            return "-- No SQL queries generated"
        
        output_lines = []
        
        # Add header comment
        output_lines.append(f"-- Generated SQL for: {self.sql_definition.description}")
        output_lines.append(f"-- Target Dialect: {self.translation_options.target_dialect.value}")
        output_lines.append(f"-- Generated {len(self.sql_queries)} quer{'y' if len(self.sql_queries) == 1 else 'ies'} for {self.measures_count} measure{'s' if self.measures_count != 1 else ''}")
        
        if self.optimization_suggestions:
            output_lines.append("--")
            output_lines.append("-- Optimization Suggestions:")
            for suggestion in self.optimization_suggestions:
                output_lines.append(f"-- • {suggestion}")
        
        output_lines.append("")
        
        # Add each query with proper separation
        for i, query in enumerate(self.sql_queries):
            if i > 0:
                output_lines.append("")
                output_lines.append("-- " + "="*50)
                output_lines.append("")
            
            if query.description:
                output_lines.append(f"-- {query.description}")
                output_lines.append("")
            
            output_lines.append(query.to_sql(formatted=True))
        
        return "\n".join(output_lines)
    
    def get_measures_summary(self) -> Dict[str, Any]:
        """Get summary of translated measures"""
        return {
            "total_measures": len(self.sql_measures),
            "aggregation_types": list(set(measure.aggregation_type.value for measure in self.sql_measures)),
            "dialects": list(set(measure.dialect.value for measure in self.sql_measures)),
            "has_filters": sum(1 for measure in self.sql_measures if measure.filters),
            "has_grouping": sum(1 for measure in self.sql_measures if measure.group_by_columns),
        }