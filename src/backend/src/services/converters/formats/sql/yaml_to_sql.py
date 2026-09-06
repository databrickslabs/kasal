"""
SQL Generator for YAML2DAX SQL Translation
Converts KPI definitions to SQL queries for various SQL dialects
"""

import logging
from typing import Any, Dict

from ...base.models import KPIDefinition
from .helpers.sql_structures import SQLStructureExpander
from .models import (
    SQLDefinition,
    SQLDialect,
    SQLTranslationOptions,
    SQLTranslationResult,
)


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

    def generate_sql_from_kbi_definition(
        self, definition: KPIDefinition, options: SQLTranslationOptions = None
    ) -> SQLTranslationResult:
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
            sql_definition = self.structure_processor.process_definition(
                definition, options
            )

            # Generate SQL queries using the structure processor
            sql_queries = self.structure_processor.generate_sql_queries_from_definition(
                sql_definition, options
            )

            # Create result with comprehensive data
            result = SQLTranslationResult(
                sql_queries=sql_queries,
                sql_measures=sql_definition.sql_measures,
                sql_definition=sql_definition,
                translation_options=options,
                measures_count=len(sql_definition.sql_measures),
                queries_count=len(sql_queries),
                syntax_valid=True,
                estimated_complexity=self._estimate_complexity(sql_definition),
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
                    dialect=self.dialect,
                ),
                translation_options=options,
                measures_count=0,
                queries_count=0,
                syntax_valid=False,
                validation_messages=[f"Generation failed: {str(e)}"],
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

    def _enhance_result_with_analysis(
        self, result: SQLTranslationResult
    ) -> SQLTranslationResult:
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
                optimization_suggestions.append(
                    "Ensure all variables are properly defined in default_variables"
                )

        # Performance optimization suggestions
        if len(result.sql_measures) > 5:
            optimization_suggestions.append(
                "Consider using CTEs for better readability with many measures"
            )

        if any(len(measure.filters) > 3 for measure in result.sql_measures):
            optimization_suggestions.append(
                "Consider creating filtered views for complex filter conditions"
            )

        # Update result with findings
        result.validation_messages.extend(validation_messages)
        result.optimization_suggestions.extend(optimization_suggestions)

        return result
