"""
KBI Formula Parser for Dependency Extraction

Extracts KBI references and variable references from formulas to build dependency tree.
Used by all converters (SQL, UC Metrics, DAX) for semantic formula parsing.
Mirrors the token extraction pattern from reference KbiComponent.
"""

import re
from typing import List, Set, Dict, Optional, Tuple
from enum import Enum
import logging
from ...base.models import KPI


class TokenType(Enum):
    """Types of tokens found in formulas"""
    KBI_REFERENCE = "kbi_reference"      # Reference to another KBI
    VARIABLE = "variable"                 # Variable reference ($var_name)
    COLUMN = "column"                     # Database column reference
    FUNCTION = "function"                 # SQL function call
    OPERATOR = "operator"                 # Mathematical/logical operator
    LITERAL = "literal"                   # Numeric or string literal


class FormulaToken:
    """Represents a token extracted from a formula"""

    def __init__(self, value: str, token_type: TokenType, position: int = 0):
        self.value = value
        self.token_type = token_type
        self.position = position

    def __repr__(self):
        return f"Token({self.token_type.value}={self.value})"

    def __eq__(self, other):
        if isinstance(other, FormulaToken):
            return self.value == other.value and self.token_type == other.token_type
        return False

    def __hash__(self):
        return hash((self.value, self.token_type))


class KbiFormulaParser:
    """
    Parses SQL formulas to extract KBI dependencies and variables

    Supported patterns:
    - KBI references: [KBI_NAME] or {KBI_NAME}
    - Variables: $var_name or $var_VARIABLE_NAME
    - Column references: simple identifiers
    - Functions: FUNC_NAME(...)
    - Operators: +, -, *, /, etc.

    Mirrors reference KbiComponent.extract_tokens() pattern.
    """

    # Regex patterns for token extraction
    KBI_REFERENCE_PATTERN = r'\[([a-zA-Z_][a-zA-Z0-9_]*)\]|\{([a-zA-Z_][a-zA-Z0-9_]*)\}'
    VARIABLE_PATTERN = r'\$(?:var_)?([a-zA-Z_][a-zA-Z0-9_]*)'
    FUNCTION_PATTERN = r'([A-Z_]+)\s*\('
    IDENTIFIER_PATTERN = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def parse_formula(self, formula: str) -> List[FormulaToken]:
        """
        Parse formula into tokens

        Args:
            formula: Formula string to parse

        Returns:
            List of FormulaToken objects
        """
        if not formula:
            return []

        tokens = []

        # Extract KBI references first (highest priority)
        kbi_tokens = self._extract_kbi_references(formula)
        tokens.extend(kbi_tokens)

        # Extract variable references
        var_tokens = self._extract_variables(formula)
        tokens.extend(var_tokens)

        # Extract function calls
        func_tokens = self._extract_functions(formula)
        tokens.extend(func_tokens)

        # Extract identifiers (column names, etc.)
        id_tokens = self._extract_identifiers(formula, exclude=kbi_tokens + var_tokens + func_tokens)
        tokens.extend(id_tokens)

        return tokens

    def extract_kbi_references(self, formula: str) -> List[str]:
        """
        Extract KBI reference names from formula

        Supports patterns:
        - [KBI_NAME] - Square bracket notation (common in DAX/Excel)
        - {KBI_NAME} - Curly brace notation (alternative)

        Args:
            formula: Formula string

        Returns:
            List of KBI names referenced in formula
        """
        kbi_names = []

        # Find all KBI references
        matches = re.finditer(self.KBI_REFERENCE_PATTERN, formula)

        for match in matches:
            # Pattern has two capture groups (square and curly brackets)
            kbi_name = match.group(1) or match.group(2)
            if kbi_name and kbi_name not in kbi_names:  # Deduplicate
                kbi_names.append(kbi_name)

        return kbi_names

    def extract_variables(self, formula: str) -> List[str]:
        """
        Extract variable references from formula

        Supports patterns:
        - $variable_name
        - $var_VARIABLE_NAME

        Args:
            formula: Formula string

        Returns:
            List of variable names
        """
        var_names = []

        matches = re.finditer(self.VARIABLE_PATTERN, formula)

        for match in matches:
            var_name = match.group(1)
            if var_name:
                var_names.append(var_name)

        return var_names

    def extract_dependencies(self, formula: str) -> Dict[str, List[str]]:
        """
        Extract all dependencies from formula

        Returns:
            Dictionary with keys: 'kbis', 'variables', 'columns'
        """
        return {
            'kbis': self.extract_kbi_references(formula),
            'variables': self.extract_variables(formula),
            'columns': self._extract_column_references(formula)
        }

    def _extract_kbi_references(self, formula: str) -> List[FormulaToken]:
        """Extract KBI reference tokens"""
        tokens = []

        matches = re.finditer(self.KBI_REFERENCE_PATTERN, formula)

        for match in matches:
            kbi_name = match.group(1) or match.group(2)
            if kbi_name:
                token = FormulaToken(
                    value=kbi_name,
                    token_type=TokenType.KBI_REFERENCE,
                    position=match.start()
                )
                tokens.append(token)

        return tokens

    def _extract_variables(self, formula: str) -> List[FormulaToken]:
        """Extract variable tokens"""
        tokens = []

        matches = re.finditer(self.VARIABLE_PATTERN, formula)

        for match in matches:
            var_name = match.group(1)
            if var_name:
                token = FormulaToken(
                    value=var_name,
                    token_type=TokenType.VARIABLE,
                    position=match.start()
                )
                tokens.append(token)

        return tokens

    def _extract_functions(self, formula: str) -> List[FormulaToken]:
        """Extract SQL function tokens"""
        tokens = []

        matches = re.finditer(self.FUNCTION_PATTERN, formula)

        for match in matches:
            func_name = match.group(1)
            if func_name and func_name.upper() in self._get_sql_functions():
                token = FormulaToken(
                    value=func_name,
                    token_type=TokenType.FUNCTION,
                    position=match.start()
                )
                tokens.append(token)

        return tokens

    def _extract_identifiers(self, formula: str, exclude: List[FormulaToken] = None) -> List[FormulaToken]:
        """Extract identifier tokens (column names, etc.) excluding already found tokens"""
        tokens = []
        exclude_values = {t.value for t in (exclude or [])}

        matches = re.finditer(self.IDENTIFIER_PATTERN, formula)

        for match in matches:
            identifier = match.group(1)
            if identifier and identifier not in exclude_values and not self._is_sql_keyword(identifier):
                token = FormulaToken(
                    value=identifier,
                    token_type=TokenType.COLUMN,
                    position=match.start()
                )
                tokens.append(token)

        return tokens

    def _extract_column_references(self, formula: str) -> List[str]:
        """Extract column references from formula"""
        # Get all identifiers
        matches = re.finditer(self.IDENTIFIER_PATTERN, formula)

        columns = []
        kbi_refs = self.extract_kbi_references(formula)
        var_refs = self.extract_variables(formula)
        exclude = set(kbi_refs + var_refs)

        for match in matches:
            identifier = match.group(1)
            if (identifier and
                identifier not in exclude and
                not self._is_sql_keyword(identifier) and
                not self._is_sql_function(identifier)):
                columns.append(identifier)

        return list(set(columns))  # Deduplicate

    def _is_sql_keyword(self, word: str) -> bool:
        """Check if word is a SQL keyword"""
        sql_keywords = {
            'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN',
            'LIKE', 'IS', 'NULL', 'TRUE', 'FALSE', 'CASE', 'WHEN', 'THEN',
            'ELSE', 'END', 'AS', 'ON', 'JOIN', 'LEFT', 'RIGHT', 'INNER',
            'OUTER', 'GROUP', 'BY', 'HAVING', 'ORDER', 'ASC', 'DESC',
            'LIMIT', 'OFFSET', 'UNION', 'DISTINCT', 'ALL'
        }
        return word.upper() in sql_keywords

    def _is_sql_function(self, word: str) -> bool:
        """Check if word is a SQL function"""
        return word.upper() in self._get_sql_functions()

    def _get_sql_functions(self) -> Set[str]:
        """Get set of common SQL functions"""
        return {
            'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE',
            'COALESCE', 'NULLIF', 'CAST', 'CONVERT', 'CASE',
            'SUBSTR', 'SUBSTRING', 'CONCAT', 'UPPER', 'LOWER', 'TRIM',
            'DATE', 'YEAR', 'MONTH', 'DAY', 'NOW', 'CURRENT_DATE',
            'ABS', 'ROUND', 'CEIL', 'FLOOR', 'MOD', 'POWER', 'SQRT',
            'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD',
            'FIRST_VALUE', 'LAST_VALUE', 'PERCENTILE_CONT'
        }


class KBIDependencyResolver:
    """
    Resolves KBI dependencies from formulas and builds dependency graph

    Mirrors reference KbiComponent.load_tokens() pattern.
    """

    def __init__(self, parser: KbiFormulaParser = None):
        self.parser = parser or KbiFormulaParser()
        self.logger = logging.getLogger(__name__)
        self._kbi_lookup: Dict[str, KPI] = {}

    def build_kbi_lookup(self, kpis: List[KPI]) -> None:
        """
        Build lookup dictionary for KBIs by technical_name

        Args:
            kbis: List of all KBIs in definition
        """
        self._kbi_lookup = {kpi.technical_name: kpi for kpi in kpis}

        # Also index by description for fallback
        for kpi in kpis:
            if kpi.description and kpi.description not in self._kbi_lookup:
                self._kbi_lookup[kpi.description] = kpi

    def resolve_formula_kbis(self, kbi: KPI) -> List[KPI]:
        """
        Resolve KBI dependencies from a KBI's formula

        Args:
            kbi: KBI to extract dependencies from

        Returns:
            List of KBIs referenced in the formula
        """
        if not kbi.formula:
            return []

        # Extract KBI references from formula
        kbi_names = self.parser.extract_kbi_references(kbi.formula)

        # Resolve to actual KBI objects
        resolved_kbis = []

        for kbi_name in kbi_names:
            if kbi_name in self._kbi_lookup:
                referenced_kbi = self._kbi_lookup[kbi_name]
                resolved_kbis.append(referenced_kbi)
                self.logger.debug(f"Resolved KBI reference '{kbi_name}' in formula for '{kbi.technical_name}'")
            else:
                self.logger.warning(
                    f"KBI reference '{kbi_name}' in formula for '{kbi.technical_name}' could not be resolved"
                )

        return resolved_kbis

    def get_dependency_tree(self, kbi: KPI, visited: Set[str] = None) -> Dict[str, any]:
        """
        Build complete dependency tree for a KBI

        Returns tree structure:
        {
            'kbi': KBI object,
            'dependencies': [
                {'kbi': child_kbi, 'dependencies': [...]},
                ...
            ]
        }
        """
        if visited is None:
            visited = set()

        # Prevent circular dependencies
        if kbi.technical_name in visited:
            return {'kbi': kbi, 'dependencies': [], 'circular': True}

        visited.add(kbi.technical_name)

        # Get direct dependencies
        formula_kbis = self.resolve_formula_kbis(kbi)

        # Recursively build tree
        dependencies = []
        for child_kbi in formula_kbis:
            child_tree = self.get_dependency_tree(child_kbi, visited.copy())
            dependencies.append(child_tree)

        return {
            'kbi': kbi,
            'dependencies': dependencies,
            'is_base': len(dependencies) == 0
        }
