"""
DAX Parser - Token-based DAX Expression Parsing

Provides comprehensive DAX expression parsing with tokenization,
signature generation, and expression analysis.

This module handles:
- Tokenization of DAX expressions into structured tokens
- Signature generation for pattern matching
- Basic extraction (source tables, aggregations, filters)
- Advanced parsing for transpilation support
"""

import re
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class DaxToken:
    """
    Represents a single token in a DAX expression.

    Attributes:
        type: Token type (operator, column, function, etc.)
        value: Token value
        group: Group number for nested expressions
        parent_group: Parent group number
        sequence: Position in token sequence
        group_type: Type of group (comparison, function, etc.)
    """
    type: str
    value: str
    group: int = 0
    parent_group: int = 0
    sequence: int = 0
    group_type: str = ''

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DaxToken':
        """Create token from dictionary."""
        return cls(**data)


class DAXExpressionParser:
    """
    Comprehensive DAX Expression Parser.

    Provides both simple regex-based extraction and advanced token-based
    parsing with SQL transpilation capabilities.

    Features:
    - Simple parse() method for basic extraction (backward compatible)
    - Advanced parse_advanced() with full tokenization
    - DAX to SQL transpilation
    - Transpilability validation
    - Signature generation for pattern matching
    """

    # Common aggregation functions
    AGG_FUNCTIONS = [
        'SUM', 'SUMX', 'AVERAGE', 'AVERAGEX', 'COUNT', 'COUNTX',
        'COUNTA', 'COUNTAX', 'DISTINCTCOUNT', 'MIN', 'MAX', 'MINX', 'MAXX'
    ]

    def __init__(self):
        """Initialize parser with transpilation engine."""
        self.logger = logging.getLogger(__name__)
        # Lazy import to avoid circular dependency
        from .dax_to_sql import DaxToSqlTranspiler
        self.transpilation_engine = DaxToSqlTranspiler()

    def parse(self, expression: str) -> Dict[str, Any]:
        """
        Simple parse for basic extraction (backward compatible).

        Args:
            expression: DAX expression string

        Returns:
            {
                'base_formula': str,
                'source_table': str,
                'aggregation_type': str,
                'filters': List[str],
                'is_complex': bool,
            }
        """
        if not expression:
            return {
                'base_formula': '',
                'source_table': None,
                'aggregation_type': 'SUM',
                'filters': [],
                'is_complex': False,
            }

        expression = expression.strip()

        # Detect complexity
        is_complex = 'CALCULATE' in expression.upper()

        # Extract components
        aggregation_type = self._extract_aggregation(expression)
        base_formula = self._extract_base_formula(expression)
        source_table = self._extract_source_table(expression)
        filters = self._extract_filters(expression)

        return {
            'base_formula': base_formula,
            'source_table': source_table,
            'aggregation_type': aggregation_type,
            'filters': filters,
            'is_complex': is_complex,
        }

    def parse_advanced(
        self,
        expression: str,
        measures_list: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Advanced parse with full tokenization and transpilation.

        Args:
            expression: DAX expression string
            measures_list: List of known measure names

        Returns:
            {
                'tokens': List[DaxToken],
                'signature': str,
                'generic_signature': str,
                'is_transpilable': bool,
                'transpilability_reason': Optional[str],
                'transpiled_sql': Optional[str],
                'functions': List[DaxToken],
                'columns': List[DaxToken],
                'operators': List[DaxToken],
                'base_formula': str,
                'source_table': str,
                'aggregation_type': str,
            }
        """
        if not expression or not expression.strip():
            return self._empty_advanced_result()

        measures_list = measures_list or []

        # Step 1: Tokenize
        tokens = self._tokenize(expression, measures_list)

        # Step 2: Generate signatures
        signature, generic_signature = self._generate_signature(tokens)

        # Step 3: Check transpilability
        is_transpilable, transpilability_reason = self.transpilation_engine.can_transpile(tokens)

        # Step 4: Transpile if possible
        transpiled_sql = None
        if is_transpilable:
            transpiled_sql = self.transpilation_engine.transpile(generic_signature, tokens)

        # Step 5: Extract components (for backward compatibility)
        simple_parse = self.parse(expression)

        # Step 6: Categorize tokens
        functions = [t for t in tokens if t.type == 'function']
        columns = [t for t in tokens if t.type == 'column']
        operators = [t for t in tokens if t.type == 'operator']

        return {
            'tokens': tokens,
            'signature': signature,
            'generic_signature': generic_signature,
            'is_transpilable': is_transpilable,
            'transpilability_reason': transpilability_reason,
            'transpiled_sql': transpiled_sql,
            'functions': functions,
            'columns': columns,
            'operators': operators,
            'base_formula': simple_parse['base_formula'],
            'source_table': simple_parse['source_table'],
            'aggregation_type': simple_parse['aggregation_type'],
        }

    def check_transpilability(
        self,
        expression: str,
        measures_list: Optional[List[str]] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Quick check if expression can be transpiled to SQL.

        Args:
            expression: DAX expression
            measures_list: List of known measure names

        Returns:
            (is_transpilable, reason_if_not)
        """
        tokens = self._tokenize(expression, measures_list or [])
        return self.transpilation_engine.can_transpile(tokens)

    def _empty_advanced_result(self) -> Dict[str, Any]:
        """Return empty result structure for advanced parsing."""
        return {
            'tokens': [],
            'signature': '',
            'generic_signature': '',
            'is_transpilable': False,
            'transpilability_reason': 'Empty expression',
            'transpiled_sql': None,
            'functions': [],
            'columns': [],
            'operators': [],
            'base_formula': '',
            'source_table': None,
            'aggregation_type': 'SUM',
        }

    def _tokenize(self, expression: str, measures_list: List[str]) -> List[DaxToken]:
        """
        Tokenize DAX expression into structured tokens.

        Args:
            expression: DAX expression
            measures_list: List of known measure names for disambiguation

        Returns:
            List of DaxToken objects
        """
        # Clean expression
        expr = self._clean_whitespace(expression)

        # Reserved words and functions
        FUNCTIONS = {'SUM', 'AVERAGE', 'COUNT', 'COUNTA', 'DISTINCTCOUNT', 'DISTINCTCOUNTNOBLANK',
                     'MIN', 'MAX', 'CALCULATE', 'FILTER', 'ALL', 'ALLEXCEPT', 'DIVIDE',
                     'IF', 'SWITCH', 'AND', 'OR', 'NOT', 'TRUE', 'FALSE', 'BLANK',
                     'FIRSTDATE', 'LASTDATE', 'YEAR', 'MONTH', 'DAY', 'EOMONTH',
                     'DATEDIFF', 'DATEADD', 'CONCATENATE', 'FORMAT',
                     'PERCENTILE.INC', 'PERCENTILE.EXC', 'STDEV.P', 'STDEV.S',
                     'KEEPFILTERS', 'ALLSELECTED', 'ISINSCOPE', 'PATHCONTAINS',
                     'CALCULATETABLE', 'GENERATE', 'REMOVEFILTERS', 'SUMX', 'AVERAGEX',
                     'COUNTX', 'COUNTAX', 'MINX', 'MAXX'}

        OPERATORS = {'==', '!=', '<=', '>=', '<>', '<', '>', '=', '+', '-', '*', '/', '&&', '||', '&'}

        tokens = []
        i = 0
        current_group = 0
        group_stack = [0]  # Track nested groups
        sequence = 0

        while i < len(expr):
            char = expr[i]

            # Skip whitespace
            if char.isspace():
                i += 1
                continue

            # Handle opening parenthesis
            if char == '(':
                current_group += 1
                group_stack.append(current_group)
                tokens.append(DaxToken(
                    type='open_paren',
                    value='(',
                    group=current_group,
                    parent_group=group_stack[-2] if len(group_stack) > 1 else 0,
                    sequence=sequence
                ))
                sequence += 1
                i += 1
                continue

            # Handle closing parenthesis
            if char == ')':
                tokens.append(DaxToken(
                    type='close_paren',
                    value=')',
                    group=current_group,
                    parent_group=group_stack[-2] if len(group_stack) > 1 else 0,
                    sequence=sequence
                ))
                sequence += 1
                if len(group_stack) > 1:
                    group_stack.pop()
                    current_group = group_stack[-1]
                i += 1
                continue

            # Handle comma
            if char == ',':
                tokens.append(DaxToken(
                    type='comma',
                    value=',',
                    group=current_group,
                    parent_group=group_stack[-2] if len(group_stack) > 1 else 0,
                    sequence=sequence
                ))
                sequence += 1
                i += 1
                continue

            # Handle string literals
            if char in ['"', "'"]:
                string_value, new_i = self._extract_string(expr, i, char)
                tokens.append(DaxToken(
                    type='string',
                    value=string_value,
                    group=current_group,
                    parent_group=group_stack[-2] if len(group_stack) > 1 else 0,
                    sequence=sequence
                ))
                sequence += 1
                i = new_i
                continue

            # Handle brackets (table[column] or [measure])
            if char == '[':
                bracketed, new_i = self._extract_bracketed(expr, i)

                # Determine if it's a measure reference or column reference
                # Check if there's a table prefix before the bracket
                is_measure = True
                if tokens:
                    # Look back for table name pattern (word followed by bracket)
                    prev_idx = len(tokens) - 1
                    while prev_idx >= 0 and tokens[prev_idx].type in ['whitespace']:
                        prev_idx -= 1
                    if prev_idx >= 0:
                        prev_token = tokens[prev_idx]
                        # If previous token is a word/table, this is a column, not a measure
                        if prev_token.type in ['table', 'word']:
                            is_measure = False
                        # If the bracketed value is in measures_list, it's definitely a measure
                        elif bracketed.strip('[]') in measures_list:
                            is_measure = True

                # Also check if bracketed name is in measures list
                clean_name = bracketed.strip('[]')
                if clean_name in measures_list:
                    is_measure = True
                # If it looks like a column reference (has table prefix in tokens)
                elif tokens and tokens[-1].type == 'table':
                    is_measure = False

                token_type = 'measure' if is_measure else 'column'
                tokens.append(DaxToken(
                    type=token_type,
                    value=bracketed,
                    group=current_group,
                    parent_group=group_stack[-2] if len(group_stack) > 1 else 0,
                    sequence=sequence
                ))
                sequence += 1
                i = new_i
                continue

            # Handle numbers
            if char.isdigit() or (char == '.' and i + 1 < len(expr) and expr[i+1].isdigit()):
                number, new_i = self._extract_number(expr, i)
                tokens.append(DaxToken(
                    type='number',
                    value=number,
                    group=current_group,
                    parent_group=group_stack[-2] if len(group_stack) > 1 else 0,
                    sequence=sequence
                ))
                sequence += 1
                i = new_i
                continue

            # Handle operators (check two-char operators first)
            if i + 1 < len(expr):
                two_char = expr[i:i+2]
                if two_char in OPERATORS:
                    tokens.append(DaxToken(
                        type='operator',
                        value=two_char,
                        group=current_group,
                        parent_group=group_stack[-2] if len(group_stack) > 1 else 0,
                        sequence=sequence
                    ))
                    sequence += 1
                    i += 2
                    continue

            # Handle single-char operators
            if char in OPERATORS:
                operator, new_i = self._extract_operator(expr, i)
                tokens.append(DaxToken(
                    type='operator',
                    value=operator,
                    group=current_group,
                    parent_group=group_stack[-2] if len(group_stack) > 1 else 0,
                    sequence=sequence
                ))
                sequence += 1
                i = new_i
                continue

            # Handle words (functions, table names, etc.)
            if char.isalpha() or char == '_':
                word, new_i = self._extract_word(expr, i)
                word_upper = word.upper()

                # Check if it's a function
                if word_upper in FUNCTIONS:
                    token_type = 'function'
                # Check if it's followed by '[' (table name)
                elif new_i < len(expr) and expr[new_i:new_i+1].strip() == '[':
                    token_type = 'table'
                # Check if it's a known measure name
                elif word in measures_list:
                    token_type = 'measure'
                # Check if it looks like an interval keyword
                elif word_upper in ['DAY', 'MONTH', 'YEAR', 'QUARTER', 'WEEK', 'HOUR', 'MINUTE', 'SECOND']:
                    # Check context - if in DATEDIFF, it's an interval
                    if self._is_interval_context(tokens, current_group):
                        token_type = 'interval'
                    else:
                        token_type = 'function'
                else:
                    token_type = 'word'

                tokens.append(DaxToken(
                    type=token_type,
                    value=word,
                    group=current_group,
                    parent_group=group_stack[-2] if len(group_stack) > 1 else 0,
                    sequence=sequence
                ))
                sequence += 1
                i = new_i
                continue

            # Unknown character - skip
            i += 1

        # Post-processing: identify comparison groups
        self._identify_comparison_groups(tokens)

        return tokens

    def _generate_signature(self, tokens: List[DaxToken]) -> Tuple[str, str]:
        """
        Generate both specific and generic signatures from tokens.

        Args:
            tokens: List of tokens

        Returns:
            (specific_signature, generic_signature)
            - specific_signature: Preserves actual values
            - generic_signature: Uses placeholders (<<type:n>>)
        """
        if not tokens:
            return ('', '')

        # Build specific signature (lowercase with actual values)
        specific_parts = []
        for token in tokens:
            if token.type in ['function', 'operator', 'comma']:
                specific_parts.append(token.value.lower())
            elif token.type in ['open_paren', 'close_paren']:
                specific_parts.append(token.value)
            elif token.type in ['table', 'column', 'measure', 'string', 'number', 'interval', 'word']:
                specific_parts.append(token.value)
            # Skip whitespace

        specific_signature = ' '.join(specific_parts)

        # Build generic signature with placeholders
        generic_parts = []
        type_counters = {}  # Track count of each type for numbering

        for token in tokens:
            if token.type in ['function', 'operator', 'comma', 'open_paren', 'close_paren']:
                generic_parts.append(token.value.lower())
            elif token.type in ['table', 'column', 'measure', 'string', 'number', 'interval', 'word']:
                # Generate placeholder
                if token.type not in type_counters:
                    type_counters[token.type] = {}

                # Track unique values for each type
                if token.value not in type_counters[token.type]:
                    count = len(type_counters[token.type]) + 1
                    type_counters[token.type][token.value] = count

                count = type_counters[token.type][token.value]
                placeholder = f'<<{token.type}:{count}>>'
                generic_parts.append(placeholder)

        generic_signature = ' '.join(generic_parts)

        return (specific_signature, generic_signature)

    def _clean_whitespace(self, expression: str) -> str:
        """
        Normalize whitespace in expression while preserving strings.

        Args:
            expression: DAX expression

        Returns:
            Expression with normalized whitespace
        """
        # Replace multiple spaces with single space (except in strings)
        # This is a simple version - more sophisticated handling could be added
        result = []
        in_string = False
        quote_char = None

        for char in expression:
            if char in ['"', "'"]:
                if not in_string:
                    in_string = True
                    quote_char = char
                elif char == quote_char:
                    in_string = False
                    quote_char = None

            result.append(char)

        return ''.join(result)

    def _extract_word(self, expression: str, i: int) -> Tuple[str, int]:
        """Extract a word (function name, table name, etc.)."""
        start = i
        while i < len(expression) and (expression[i].isalnum() or expression[i] in ['_', '.']):
            i += 1
        return (expression[start:i], i)

    def _extract_bracketed(self, expression: str, i: int) -> Tuple[str, int]:
        """Extract content within brackets [...]."""
        start = i
        i += 1  # Skip opening bracket
        while i < len(expression) and expression[i] != ']':
            i += 1
        i += 1  # Include closing bracket
        return (expression[start:i], i)

    def _extract_string(self, expression: str, i: int, quote: str) -> Tuple[str, int]:
        """Extract quoted string."""
        start = i
        i += 1  # Skip opening quote
        while i < len(expression) and expression[i] != quote:
            i += 1
        i += 1  # Include closing quote
        return (expression[start:i], i)

    def _extract_number(self, expression: str, i: int) -> Tuple[str, int]:
        """Extract number (integer or decimal)."""
        start = i
        has_decimal = False
        while i < len(expression):
            if expression[i].isdigit():
                i += 1
            elif expression[i] == '.' and not has_decimal:
                has_decimal = True
                i += 1
            else:
                break
        return (expression[start:i], i)

    def _extract_operator(self, expression: str, i: int) -> Tuple[str, int]:
        """Extract operator."""
        # Already handled multi-char operators before calling this
        return (expression[i], i + 1)

    def _is_interval_context(self, tokens: List[DaxToken], current_group: int) -> bool:
        """Check if current position is in an interval context (e.g., DATEDIFF third argument)."""
        # Look for DATEDIFF function in parent groups
        for token in reversed(tokens):
            if token.group == current_group and token.type == 'function' and token.value.upper() == 'DATEDIFF':
                # Count commas to determine position - if we're after 2nd comma, it's interval position
                comma_count = sum(1 for t in tokens if t.group == current_group and t.type == 'comma')
                return comma_count >= 2
        return False

    def _identify_comparison_groups(self, tokens: List[DaxToken]) -> None:
        """Identify groups that represent comparisons and mark them."""
        # Find groups with comparison operators (==, !=, <, >, etc.)
        comparison_groups = set()

        for token in tokens:
            if token.type == 'operator' and token.value in ['==', '!=', '<', '>', '<=', '>=', '<>', '=']:
                comparison_groups.add(token.group)

        # Mark all tokens in comparison groups
        for token in tokens:
            if token.group in comparison_groups:
                token.group_type = 'comparison'

    def _extract_aggregation(self, expression: str) -> str:
        """Extract aggregation type from expression."""
        expr_upper = expression.upper()

        for agg in self.AGG_FUNCTIONS:
            if agg in expr_upper:
                return agg

        return 'SUM'  # Default

    def _extract_base_formula(self, expression: str) -> str:
        """
        Extract the base formula (innermost aggregation operand).

        Example:
            SUM(Table[Column]) -> Table[Column]
            CALCULATE(SUM(Sales[Amount])) -> Sales[Amount]
        """
        # Simple extraction for common patterns
        # Look for pattern: FUNCTION(Table[Column])
        pattern = r'\w+\[([^\]]+)\]'
        match = re.search(pattern, expression)

        if match:
            return match.group(1)

        # If no match, return the expression itself (simplified)
        # Remove outer function calls
        expr = expression.strip()
        for agg in self.AGG_FUNCTIONS:
            pattern = rf'{agg}\s*\('
            expr = re.sub(pattern, '', expr, flags=re.IGNORECASE)

        # Remove trailing parentheses
        while expr.endswith(')'):
            expr = expr[:-1]

        return expr.strip()

    def _extract_source_table(self, expression: str) -> Optional[str]:
        """
        Extract source table name from expression.

        Example:
            SUM(FactSales[Amount]) -> FactSales
            CALCULATE(COUNT(DimCustomer[ID])) -> DimCustomer
        """
        # Look for pattern: Table[Column]
        pattern = r'(\w+)\['
        match = re.search(pattern, expression)

        if match:
            return match.group(1)

        return None

    def _extract_filters(self, expression: str) -> List[str]:
        """
        Extract filter conditions from CALCULATE expressions.

        Example:
            CALCULATE(SUM(Sales[Amount]), Region[Name] = "EMEA") -> ["Region[Name] = \"EMEA\""]
        """
        filters = []

        # Check if this is a CALCULATE expression
        if 'CALCULATE' not in expression.upper():
            return filters

        # Find the arguments after the first argument in CALCULATE
        # Pattern: CALCULATE(agg_expr, filter1, filter2, ...)
        try:
            # Find the opening paren of CALCULATE
            calc_match = re.search(r'CALCULATE\s*\(', expression, re.IGNORECASE)
            if not calc_match:
                return filters

            start_idx = calc_match.end()
            # Find matching closing paren
            paren_count = 1
            i = start_idx
            while i < len(expression) and paren_count > 0:
                if expression[i] == '(':
                    paren_count += 1
                elif expression[i] == ')':
                    paren_count -= 1
                i += 1

            # Extract content between parens
            content = expression[start_idx:i-1]

            # Split by commas (accounting for nested parens)
            parts = self._smart_split(content, ',')

            # Skip first part (aggregation expression), rest are filters
            if len(parts) > 1:
                filters = [self._format_filter(f.strip()) for f in parts[1:]]

        except Exception as e:
            self.logger.debug(f"Error extracting filters: {e}")

        return filters

    def _smart_split(self, text: str, delimiter: str = ',') -> List[str]:
        """
        Split text by delimiter, respecting nested parentheses and quotes.

        Args:
            text: Text to split
            delimiter: Delimiter character

        Returns:
            List of split parts
        """
        parts = []
        current = []
        paren_depth = 0
        in_string = False
        quote_char = None

        for char in text:
            if char in ['"', "'"]:
                if not in_string:
                    in_string = True
                    quote_char = char
                elif char == quote_char:
                    in_string = False
                    quote_char = None
                current.append(char)
            elif in_string:
                current.append(char)
            elif char == '(':
                paren_depth += 1
                current.append(char)
            elif char == ')':
                paren_depth -= 1
                current.append(char)
            elif char == delimiter and paren_depth == 0:
                parts.append(''.join(current))
                current = []
            else:
                current.append(char)

        if current:
            parts.append(''.join(current))

        return parts

    def _format_filter(self, filter_condition: str) -> str:
        """Format a filter condition for consistency."""
        # Remove extra whitespace
        filter_condition = ' '.join(filter_condition.split())
        return filter_condition
