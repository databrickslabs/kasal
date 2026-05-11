"""Parser for Unity Catalog Metrics View YAML files."""
import logging
import re
import yaml
from typing import Any, Dict, List, Optional, Set

from .constants import SQL_IDENTIFIER_EXCLUSIONS, AGGREGATION_FUNCTIONS

logger = logging.getLogger(__name__)

# Pre-compiled regex patterns for performance
# Build the aggregation-function alternation dynamically from AGGREGATION_FUNCTIONS
# so that adding a new entry to the constant is automatically picked up here.
# Sort longest-first to avoid partial matches (e.g. 'SUMX' before 'SUM').
_agg_alts = '|'.join(sorted(AGGREGATION_FUNCTIONS, key=len, reverse=True))
_AGG_PATTERN = re.compile(rf'({_agg_alts})\s*\(', re.IGNORECASE)
_FILTER_PATTERN = re.compile(r'FILTER\s*\(\s*WHERE\s+([^)]+)\)', re.IGNORECASE)
_TABLE_COLUMN_PATTERN = re.compile(r'(\w+)\.(\w+)')
_STRING_LITERAL_PATTERN = re.compile(r"['\"].*?['\"]")
_IDENTIFIER_PATTERN = re.compile(r'\b(\w+)\b')
_IN_CLAUSE_PATTERN = re.compile(r'(\w+\.\w+)\s+IN\s*\(([^)]+)', re.IGNORECASE)
_EQUALS_PATTERN = re.compile(r'(\w+\.\w+)\s*=\s*(.+)')


class UCMetricsViewParser:
    """Parser for Unity Catalog Metrics View YAML files."""
    
    def __init__(self, yaml_path: str):
        """
        Initialize the parser.
        
        Args:
            yaml_path: Path to the YAML file to parse
            
        Raises:
            ValueError: If yaml_path is None or empty
        """
        if not yaml_path:
            raise ValueError("yaml_path cannot be None or empty")
        self.yaml_path = yaml_path
        self.data = None
        self.measures = []
        self._measures_index: Dict[str, Dict] = {}  # name → measure dict for O(1) lookup
        
    @classmethod
    def create_headless(cls) -> "UCMetricsViewParser":
        """Create a parser instance that can parse expressions without a backing YAML file.

        Use this when you need access to the pure expression-parsing methods
        (``_parse_measure``, ``_extract_*``, etc.) but have no YAML file to
        load.  Avoids the fragile ``__new__`` bypass pattern.
        """
        instance = object.__new__(cls)
        instance.yaml_path = None
        instance.data = None
        instance.measures = []
        instance._measures_index = {}
        return instance

    def load(self) -> Dict:
        """Load and parse the YAML file."""
        with open(self.yaml_path, 'r', encoding='utf-8') as f:
            self.data = yaml.safe_load(f)
        return self.data
    
    def extract_measures(self) -> List[Dict]:
        """Extract all measures from the YAML and build a name index."""
        if not self.data:
            self.load()

        self.measures = []
        self._measures_index = {}

        if self.data is None:
            logger.warning(
                "YAML file '%s' is empty or could not be parsed; no measures extracted.",
                self.yaml_path,
            )
            return self.measures

        for measure in self.data.get('measures', []):
            entry = {
                'name': measure.get('name'),
                'expr': measure.get('expr'),
                'parsed_expr': self._parse_measure(measure.get('expr')),
                'comment': measure.get('comment', ''),
                'display_name': measure.get('display_name'),
                'synonyms': measure.get('synonyms', []),
            }
            self.measures.append(entry)
            if entry['name']:
                self._measures_index[entry['name']] = entry

        return self.measures

    def get_measure_by_name(self, name: str) -> Optional[Dict]:
        """Get a specific measure by name (O(1) via index)."""
        if not self.measures:
            self.extract_measures()
        return self._measures_index.get(name)
    
    def _parse_measure(self, expr: str) -> Dict[str, Any]:
        """Parse a Databricks expression into structured components."""
        if not expr:
            logger.warning("Empty or None expression provided to _parse_measure")
            return {
                "raw": "",
                "aggregations": [],
                "filters": [],
                "references": set(),
                "operations": [],
                "structure": {
                    "is_division": False,
                    "has_filter": False,
                    "has_coalesce": False,
                    "has_nullif": False,
                    "complexity": "simple"
                },
            }
        
        expr = expr.strip()
        
        result = {
            "raw": expr,
            "aggregations": self._extract_aggregations(expr),
            "filters": self._extract_filters(expr),
            "references": self._extract_references(expr),
            "operations": self._extract_operations(expr),
            "structure": self._analyze_structure(expr),
        }
        
        return result
    
    def _extract_aggregations(self, expr: str) -> List[Dict[str, Any]]:
        """Extract aggregation functions (SUM, COUNT, AVG, etc.)."""
        aggregations = []
        
        for match in _AGG_PATTERN.finditer(expr):
            agg_type = match.group(1).upper()
            start_pos = match.end() - 1
            
            # Find matching closing parenthesis
            content = self._extract_balanced_parens(expr, start_pos)
            
            aggregations.append({
                "type": agg_type,
                "content": content,
                "position": match.start()
            })
        
        return aggregations
    
    def _extract_filters(self, expr: str) -> List[Dict[str, Any]]:
        """Extract FILTER clauses and WHERE conditions."""
        filters = []
        
        for match in _FILTER_PATTERN.finditer(expr):
            condition = match.group(1).strip()
            filters.append({
                "type": "FILTER_WHERE",
                "condition": condition,
                "parsed_condition": self._parse_condition(condition)
            })
        
        return filters
    
    def _parse_condition(self, condition: str) -> Dict[str, Any]:
        """Parse a filter condition into components."""
        # Handle IN clauses
        in_match = _IN_CLAUSE_PATTERN.search(condition)
        if in_match:
            column = in_match.group(1)
            values = [v.strip().strip("'\"") for v in in_match.group(2).split(',')]
            return {
                "type": "IN",
                "column": column,
                "values": sorted(values)
            }
        
        # Handle equality
        eq_match = _EQUALS_PATTERN.search(condition)
        if eq_match:
            return {
                "type": "EQUALS",
                "column": eq_match.group(1),
                "value": eq_match.group(2).strip().strip("'\"")
            }
        
        return {"type": "UNKNOWN", "raw": condition}
    
    def _extract_references(self, expr: str) -> Set[str]:
        """Extract references (e.g., source.column, dim_wkctr.column, and standalone columns)."""
        references = set()
        
        # Remove string literals (both single and double quoted) to avoid extracting from them
        expr_without_strings = _STRING_LITERAL_PATTERN.sub('', expr)
        
        # Pattern 1: table.column (qualified references)
        for match in _TABLE_COLUMN_PATTERN.finditer(expr_without_strings):
            table_name = match.group(1)
            column_name = match.group(2)
            qualified_ref = f"{table_name}.{column_name}"
            references.add(qualified_ref)
        
        # Pattern 2: standalone identifiers (unqualified columns)
        for match in _IDENTIFIER_PATTERN.finditer(expr_without_strings):
            identifier = match.group(1)
            
            # Skip if it's a SQL keyword or well-known function name
            if identifier.upper() in SQL_IDENTIFIER_EXCLUSIONS:
                continue
            
            # Skip if it's a pure numeric value (integer or float)
            if identifier.isdigit():
                continue
            
            # Skip if it's part of a qualified reference (e.g., skip 'source' in 'source.column')
            # Check if this identifier is followed by a dot (making it a table name)
            if match.end() < len(expr_without_strings) and expr_without_strings[match.end()] == '.':
                continue
            
            # Skip if this identifier is preceded by a dot (making it part of qualified ref)
            if match.start() > 0 and expr_without_strings[match.start() - 1] == '.':
                continue
            
            # Add the standalone identifier
            references.add(identifier)
        
        return references
    
    def _extract_operations(self, expr: str) -> List[str]:
        """Extract mathematical operations (division, multiplication, etc.)."""
        operations = []
        
        if '/' in expr or 'DIVIDE' in expr.upper():
            operations.append('DIVISION')
        if '*' in expr:
            operations.append('MULTIPLICATION')
        if '+' in expr:
            operations.append('ADDITION')
        if '-' in expr:
            operations.append('SUBTRACTION')
        if 'NULLIF' in expr.upper():
            operations.append('NULLIF')
        if 'COALESCE' in expr.upper():
            operations.append('COALESCE')
        
        return operations
    
    def _analyze_structure(self, expr: str) -> Dict[str, Any]:
        """Analyze the overall structure of the expression."""
        structure = {
            "is_division": False,
            "has_filter": False,
            "has_coalesce": False,
            "has_nullif": False,
            "complexity": "simple"
        }
        
        # Check for actual division operators or function.
        if '/' in expr or 'DIVIDE' in expr.upper():
            structure["is_division"] = True
        
        # Check for filters
        if 'FILTER' in expr.upper() or 'WHERE' in expr.upper():
            structure["has_filter"] = True
        
        # Check for null handling
        if 'COALESCE' in expr.upper():
            structure["has_coalesce"] = True
        if 'NULLIF' in expr.upper():
            structure["has_nullif"] = True
        
        # Determine complexity
        if structure["is_division"] and structure["has_filter"]:
            structure["complexity"] = "complex"
        elif structure["has_filter"]:
            structure["complexity"] = "medium"
        
        return structure
    
    def _extract_balanced_parens(self, text: str, start_pos: int) -> str:
        """Extract content within balanced parentheses starting at start_pos."""
        if start_pos >= len(text) or text[start_pos] != '(':
            return ""
        
        depth = 0
        for i in range(start_pos, len(text)):
            if text[i] == '(':
                depth += 1
            elif text[i] == ')':
                depth -= 1
                if depth == 0:
                    return text[start_pos + 1:i]
        
        return text[start_pos + 1:]
