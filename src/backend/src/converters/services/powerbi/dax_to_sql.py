"""
DAX to SQL Transpilation
Converts DAX expressions to SQL-compatible syntax using pattern matching

This module provides the transpilation engine for converting existing DAX
expressions (extracted from PowerBI) into SQL equivalents for analysis,
querying, and cross-platform compatibility.
"""

import logging
from typing import List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    # Only import for type checking to avoid circular import
    from .dax_parser import DaxToken

logger = logging.getLogger(__name__)


class DaxToSqlTranspiler:
    """
    DAX to SQL transpilation engine with comprehensive pattern mappings.

    Handles conversion of DAX expressions to SQL-compatible syntax
    using pattern matching and signature-based transpilation.

    Features:
    - 70+ DAX-to-SQL pattern mappings
    - Signature-based pattern matching
    - Transpilability validation
    - Support for aggregations, date functions, mathematical operations

    Usage:
        transpiler = DaxToSqlTranspiler()

        # Check if transpilable
        can_transpile, reason = transpiler.can_transpile(tokens)

        # Transpile to SQL
        if can_transpile:
            sql = transpiler.transpile(signature, tokens)
    """

    # Operators that cannot be transpiled to SQL
    NON_TRANSPILABLE_OPERATORS = [
        'KEEPFILTERS', 'ALLSELECTED', 'ISINSCOPE', 'PATHCONTAINS',
        'CALCULATETABLE', 'GENERATE', 'REMOVEFILTERS'
    ]

    def __init__(self):
        """Initialize transpilation mappings."""
        self.dax_to_sql_mappings = {
            # Basic aggregations
            'sum ( <<table:1>> <<column:1>> )': 'SUM(`<<column:1>>`)',
            'average ( <<table:1>> <<column:1>> )': 'AVG(`<<column:1>>`)',
            'count ( <<table:1>> <<column:1>> )': 'COUNT(`<<column:1>>`)',
            'counta ( <<table:1>> <<column:1>> )': 'COUNT(`<<column:1>>`)',
            'distinctcount ( <<table:1>> <<column:1>> )': 'COUNT(DISTINCT `<<column:1>>`)',
            'min ( <<table:1>> <<column:1>> )': 'MIN(`<<column:1>>`)',
            'max ( <<table:1>> <<column:1>> )': 'MAX(`<<column:1>>`)',

            # Date functions
            'lastdate ( <<table:1>> <<column:1>> )': 'MAX(`<<column:1>>`)',
            'firstdate ( <<table:1>> <<column:1>> )': 'MIN(`<<column:1>>`)',
            'year ( max ( <<table:1>> <<column:1>> ) )': 'YEAR(MAX(`<<column:1>>`))',
            'eomonth ( max ( <<table:1>> <<column:1>> ) , <<number:1>> )': 'last_day(add_months(MAX(`<<column:1>>`), <<number:1>>))',

            # Mathematical operations
            'divide ( <<measure:1>> , <<measure:2>> )': 'TRY_DIVIDE(`<<measure:1>>`, `<<measure:2>>`)',
            'divide ( <<measure:1>> , <<number:1>> )': 'TRY_DIVIDE(`<<measure:1>>`, <<number:1>>)',
            'divide ( sum ( <<table:1>> <<column:1>> ) , <<number:1>> )': 'TRY_DIVIDE(SUM(`<<column:1>>`), <<number:1>>)',
            'abs ( divide ( <<measure:1>> , <<number:1>> ) )': 'ABS(TRY_DIVIDE(`<<measure:1>>`, <<number:1>>))',

            # Measure references
            '<<measure:1>>': '`<<measure:1>>`',
            '<<measure:1>> - <<measure:2>>': '`<<measure:1>>` - `<<measure:2>>`',
            '<<measure:1>> / <<number:1>>': '`<<measure:1>>` / <<number:1>>',
            '<<measure:1>> * <<number:1>>': '`<<measure:1>>` * <<number:1>>',

            # Numbers and strings
            '<<number:1>>': '<<number:1>>',
            '<<string:1>>': '<<string:1>>',

            # CALCULATE patterns
            'calculate ( max ( <<table:1>> <<column:1>> ) )': 'MAX(`<<column:1>>`)',
            'calculate ( min ( <<table:1>> <<column:1>> ) )': 'MIN(`<<column:1>>`)',
            'calculate ( sum ( <<table:1>> <<column:1>> ) )': 'SUM(`<<column:1>>`)',
            'calculate ( count ( <<table:1>> <<column:1>> ) )': 'COUNT(`<<column:1>>`)',
            'calculate ( distinctcountnoblank ( <<table:1>> <<column:1>> ) )': 'COUNT(DISTINCT `<<column:1>>`)',

            # CALCULATE with filters
            'calculate ( sum ( <<table:1>> <<column:1>> ) , <<table:1>> <<column:2>> == <<string:1>> )': 'SUM(CASE WHEN `<<column:2>>` = <<string:1>> THEN `<<column:1>>` END)',
            'calculate ( sum ( <<table:1>> <<column:1>> ) , <<table:1>> <<column:2>> == <<number:1>> )': 'SUM(CASE WHEN `<<column:2>>` = <<number:1>> THEN `<<column:1>>` END)',
            'calculate ( sum ( <<table:1>> <<column:1>> ) , <<table:1>> <<column:2>> == true ( ) )': 'SUM(CASE WHEN `<<column:2>>` = TRUE THEN `<<column:1>>` END)',
            'calculate ( count ( <<table:1>> <<column:1>> ) , <<table:1>> <<column:2>> == <<string:1>> )': 'COUNT(CASE WHEN `<<column:2>>` = <<string:1>> THEN `<<column:1>>` END)',
            'calculate ( max ( <<table:1>> <<column:1>> ) , <<table:1>> <<column:2>> == <<string:1>> )': 'MAX(CASE WHEN `<<column:2>>` = <<string:1>> THEN `<<column:1>>` END)',

            # CALCULATE with DIVIDE
            'calculate ( divide ( sum ( <<table:1>> <<column:1>> ) , sum ( <<table:1>> <<column:2>> ) ) * <<number:1>> )': 'TRY_DIVIDE(SUM(`<<column:1>>`), SUM(`<<column:2>>`)) * <<number:1>>',
            'calculate ( divide ( sum ( <<table:1>> <<column:1>> ) , <<number:1>> ) )': 'TRY_DIVIDE(SUM(`<<column:1>>`), <<number:1>>)',

            # DIVIDE patterns
            'divide ( calculate ( sum ( <<table:1>> <<column:1>> ) ) , calculate ( sum ( <<table:1>> <<column:2>> ) ) )': 'TRY_DIVIDE(SUM(`<<column:1>>`), SUM(`<<column:2>>`))',
            'divide ( calculate ( sum ( <<table:1>> <<column:1>> ) , <<table:1>> <<column:2>> == true ( ) ) , calculate ( sum ( <<table:1>> <<column:1>> ) , all ( <<table:1>> <<column:2>> ) ) )': 'TRY_DIVIDE(SUM(CASE WHEN `<<column:2>>` = TRUE THEN `<<column:1>>` END), SUM(`<<column:1>>`))',

            # CALCULATE with measures
            'calculate ( <<measure:1>> )': '`<<measure:1>>`',
            'calculate ( <<measure:1>> + <<measure:2>> )': '`<<measure:1>>` + `<<measure:2>>`',

            # CALCULATE with complex logic
            'calculate ( <<table:1>> <<column:1>> )': '`<<column:1>>`',
            'calculate ( <<table:1>> <<column:1>> + <<table:1>> <<column:2>> )': '`<<column:1>>` + `<<column:2>>`',

            # CALCULATE with AVERAGE and FILTER
            'calculate ( average ( <<table:1>> <<column:1>> ) , filter ( <<table:1>> , and ( <<table:1>> <<column:2>> == true ( ) , <<table:1>> <<column:3>> == false ( ) ) ) )': 'AVG(CASE WHEN `<<column:2>>` = TRUE AND `<<column:3>>` = FALSE THEN `<<column:1>>` END)',
            'calculate ( average ( <<table:1>> <<column:1>> ) , filter ( <<table:1>> , and ( <<table:1>> <<column:2>> == true ( ) , <<table:1>> <<column:3>> == true ( ) ) ) )': 'AVG(CASE WHEN `<<column:2>>` = TRUE AND `<<column:3>>` = TRUE THEN `<<column:1>>` END)',

            # IF statements
            'if ( <<table:1>> <<column:1>> > <<table:1>> <<column:2>> , <<string:1>> , <<string:2>> )': 'CASE WHEN `<<column:1>>` > `<<column:2>>` THEN <<string:1>> ELSE <<string:2>> END',
            'if ( <<table:1>> <<column:1>> == <<string:1>> , <<string:2>> , <<string:3>> )': 'CASE WHEN `<<column:1>>` == <<string:1>> THEN <<string:2>> ELSE <<string:3>> END',
            'if ( <<table:1>> <<column:1>> < <<number:1>> , <<number:2>> , <<number:3>> )': 'CASE WHEN `<<column:1>>` < <<number:1>> THEN <<number:2>> ELSE <<number:3>> END',
            'if ( <<table:1>> <<column:1>> > <<number:1>> , <<number:2>> , <<number:3>> )': 'CASE WHEN `<<column:1>>` > <<number:1>> THEN <<number:2>> ELSE <<number:3>> END',

            # DATEDIFF
            'datediff ( min ( <<table:1>> <<column:1>> ) , max ( <<table:1>> <<column:1>> ) , <<interval:1>> )': 'DATEDIFF(<<interval:1>>, MIN(`<<column:1>>`), MAX(`<<column:1>>`))',

            # CONCATENATE
            'concatenate ( <<string:1>> , max ( <<table:1>> <<column:1>> ) )': 'CONCAT(<<string:1>>, MAX(`<<column:1>>`))',
            'concatenate ( <<string:1>> , min ( <<table:1>> <<column:1>> ) )': 'CONCAT(<<string:1>>, MIN(`<<column:1>>`))',

            # PERCENTILE and STDEV
            'percentile.inc ( <<table:1>> <<column:1>> , <<number:1>> )': 'PERCENTILE(`<<column:1>>`, <<number:1>>)',
            'stdev.p ( <<table:1>> <<column:1>> )': 'STDDEV(`<<column:1>>`)',

            # Arithmetic with columns
            '<<table:1>> <<column:1>> + <<measure:1>>': '`<<column:1>>` + <<measure:1>>',
            'sum ( <<table:1>> <<column:1>> ) / count ( <<table:1>> <<column:1>> )': 'SUM(`<<column:1>>`) / COUNT(`<<column:1>>`)',
            'sum ( <<table:1>> <<column:1>> ) / <<number:1>>': 'SUM(`<<column:1>>`) / <<number:1>>',
            'sum ( <<table:1>> <<column:1>> ) - sum ( <<table:1>> <<column:2>> )': 'SUM(`<<column:1>>`) - SUM(`<<column:2>>`)',
            'count ( <<table:1>> <<column:1>> ) * <<number:1>>': 'COUNT(`<<column:1>>`) * <<number:1>>',
        }

    def can_transpile(self, tokens: List['DaxToken']) -> Tuple[bool, Optional[str]]:
        """
        Check if expression can be transpiled to SQL.

        Args:
            tokens: List of DAX tokens

        Returns:
            (is_transpilable, reason) - reason is None if transpilable
        """
        for token in tokens:
            if token.type in ['function', 'operator']:
                if token.value.upper() in self.NON_TRANSPILABLE_OPERATORS:
                    return False, f"contains unsupported {token.type} {token.value}"
        return True, None

    def transpile(self, signature: str, tokens: List['DaxToken']) -> Optional[str]:
        """
        Transpile DAX expression to SQL using signature matching.

        Args:
            signature: Generic signature of the expression
            tokens: List of DAX tokens

        Returns:
            Transpiled SQL string or None if no mapping found
        """
        # Check if signature matches any known pattern
        if signature.lower() not in self.dax_to_sql_mappings:
            logger.warning(f"No transpilation mapping found for signature: {signature}")
            return None

        sql_template = self.dax_to_sql_mappings[signature.lower()]

        # Build replacement map from tokens
        type_counts = {}
        replacements = {}

        for token in tokens:
            if token.type in ['column', 'measure', 'table', 'string', 'number', 'interval']:
                if token.type not in type_counts:
                    type_counts[token.type] = {}

                # Track unique values
                if token.value not in type_counts[token.type]:
                    count = len(type_counts[token.type]) + 1
                    type_counts[token.type][token.value] = count
                    placeholder = f'<<{token.type}:{count}>>'

                    # Clean up value
                    value = token.value
                    if token.type in ['column', 'measure']:
                        value = value.replace('[', '').replace(']', '')

                    replacements[placeholder] = value

        # Apply replacements
        result = sql_template
        for placeholder, value in replacements.items():
            result = result.replace(placeholder, value)

        return result
