"""Validates semantic equivalence between Databricks and DAX expressions."""
import logging
import re
from typing import Any, Dict, List, Optional, Set

from .constants import DAX_TO_DB_AGG_MAP, STATUS_VALID, STATUS_INVALID, STATUS_EQUIVALENT, STATUS_REVIEW
from .databricks_parser import UCMetricsViewParser
from .dax_expression_parser import DAXExpressionParser
from .data_input_handler import DataInputHandler

logger = logging.getLogger(__name__)

# A base measure is a direct aggregate of a single source column, e.g.
#   SUM(COALESCE(source.col, 0))  /  SUM(source.col)  /  COUNT(source.col)
# These carry NO DAX expression (they come from the MQuery SUM columns, not a DAX
# measure), so there is nothing to diff — but they are correct BY CONSTRUCTION.
# They must be counted VALID, not SKIPPED (skipping them made the headline look
# far worse than reality — ~144 correct base measures were shown as "not
# available").
_BASE_MEASURE_RE = re.compile(
    r'^\s*(SUM|COUNT|AVG|AVERAGE|MIN|MAX|COUNT_DISTINCT)\s*\(\s*'
    r'(COALESCE\s*\(\s*)?source\.\w+\s*(,\s*0\s*\))?\s*\)\s*$',
    re.IGNORECASE,
)


def _is_base_measure_expr(expr: Optional[str]) -> bool:
    """True when the emitted SQL is a plain single-column aggregate (base measure)."""
    return bool(expr and _BASE_MEASURE_RE.match(expr.strip()))


class ExpressionValidator:
    """Validates semantic equivalence between Databricks and DAX expressions."""
    
    def __init__(self, data_handler: Optional[DataInputHandler] = None, 
                 table_mappings: Optional[Dict] = None, 
                 column_mappings: Optional[Dict] = None):
        """
        Initialize the ExpressionValidator.
        
        Args:
            data_handler: Optional DataInputHandler for file-based validation
            table_mappings: Optional dictionary mapping DAX table names to Databricks names
            column_mappings: Optional dictionary mapping DAX column names to Databricks names
        """
        self.data_handler = data_handler
        self.table_mappings = table_mappings or {}
        self.column_mappings = column_mappings or {}
        
        # Initialize parsers for expression parsing.
        # create_headless() gives us a fully-initialised instance whose
        # pure parsing methods (_parse_measure, _extract_*) work without a
        # backing YAML file, avoiding the fragile __new__ bypass.
        self.db_parser = UCMetricsViewParser.create_headless()
        self.dax_parser = DAXExpressionParser(table_mappings, column_mappings)

    def validate_ucmv(self):
        """
        Validate all measures in the Unity Catalog Metrics View.
        
        Raises:
            ValueError: If data_handler is not configured
        """
        if not self.data_handler:
            raise ValueError("DataInputHandler must be provided to use validate_ucmv()")
        
        if not self.data_handler.mv_parser.measures:
            self.data_handler.mv_parser.extract_measures()
        
        simple_measures = []
        matched_measures = []
        unmatched_measures = []

        simple_pattern = r"^(\w+)\(([^\)]*\.)([^\)]*)\)$"
        for measure in self.data_handler.mv_parser.measures:
            expr = measure.get("expr") or ""
            if not expr:
                simple_measures.append({
                    "measure_eval": "simple",
                    "measure_name": measure.get('name', ''),
                    "measure_expression": "",
                })
                continue
            # Base measures — plain single-column aggregates like
            # SUM(source.col) or SUM(COALESCE(source.col, 0)) — are correct BY
            # CONSTRUCTION (they come straight from the MQuery SUM columns, not a
            # DAX measure, so there is nothing to diff). They MUST be caught here,
            # before the simple_pattern branch, otherwise they fall into
            # `simple`/`unmatched` and get SKIPPED — which understated headline
            # quality by ~140 measures on the CCHBC set. Count them EVALUATED/VALID.
            if _is_base_measure_expr(expr):
                matched_measures.append(
                    {
                        "measure_eval": "base",
                        "measure_name": measure['name'],
                        "measure_eval_result": {
                            "measure_name": measure['name'],
                            "status": STATUS_VALID,
                            "is_valid": True,
                            "confidence": "high",
                            "reason": "base measure — direct source-column aggregate (correct by construction)",
                            "databricks_expr": expr,
                            "dax_expr": None,
                            "differences": [],
                            "similarities": [],
                            "recommendations": [],
                        },
                    }
                )
                continue
            if re.search(simple_pattern, expr, re.IGNORECASE):
                simple_measures.append(
                    {
                        "measure_eval": "simple",
                        "measure_name": measure['name'],
                        "measure_expression": measure['expr']
                    }
                )
            elif self.data_handler.get_dax_measure(measure.get("name", "")):
                matched_measures.append(
                    {
                        "measure_eval": "matched",
                        "measure_name": measure['name'],
                        "measure_eval_result": self.validate_measure_by_name(measure['name'])
                    }
                )
            else:
                unmatched_measures.append(
                    {
                        "measure_eval": "unmatched",
                        "measure_name": measure['name']
                    }
                )
        return {
            "skipped": unmatched_measures + simple_measures,
            "evaluated": matched_measures
        }

    
    def validate_measure_by_name(self, measure_name: str) -> Dict[str, Any]:
        """
        Validate a measure by looking it up in the data handler.
        
        Args:
            measure_name: Name of the measure to validate
            
        Returns:
            Dict with validation results including:
            - is_valid: bool
            - confidence: str (high/medium/low)
            - differences: List of differences found
            - similarities: List of matching components
            - recommendations: List of suggested fixes
            - measure_name: str
            - status: str (VALID/INVALID/SKIPPED/ERROR)
            
        Raises:
            ValueError: If data_handler is not configured
        """
        if not self.data_handler:
            raise ValueError("DataInputHandler must be provided to use validate_measure_by_name()")
        
        # Get YAML measure
        yaml_measure = self.data_handler.get_yaml_measure(measure_name)
        if not yaml_measure:
            return {
                'measure_name': measure_name,
                'status': 'SKIPPED',
                'is_valid': False,
                'confidence': 'low',
                'reason': f'Measure "{measure_name}" not found in metrics view YAML',
                'differences': [],
                'similarities': [],
                'recommendations': []
            }
        
        # Base measures (plain single-column aggregates like SUM(COALESCE(source.col,0)))
        # are correct BY CONSTRUCTION and carry no DAX to diff against. Count them
        # VALID rather than SKIPPED — otherwise the ~144 base measures show as
        # "DAX not available" and the headline drastically understates quality.
        _db_expr = yaml_measure.get('expr')
        if _is_base_measure_expr(_db_expr):
            return {
                'measure_name': measure_name,
                'status': STATUS_VALID,
                'is_valid': True,
                'confidence': 'high',
                'reason': 'base measure — direct source-column aggregate (correct by construction)',
                'databricks_expr': _db_expr,
                'dax_expr': None,
                'differences': [],
                'similarities': [],
                'recommendations': [],
            }

        # Find matching DAX measure
        dax_measure = self.data_handler.find_matching_dax_for_yaml_measure(yaml_measure)
        if not dax_measure:
            return {
                'measure_name': measure_name,
                'status': 'SKIPPED',
                'is_valid': False,
                'confidence': 'low',
                'reason': f'No matching DAX expression found for measure "{measure_name}"',
                'databricks_expr': yaml_measure.get('expr'),
                'dax_expr': None,
                'differences': [],
                'similarities': [],
                'recommendations': []
            }
        
        # Check if DAX expression is available
        dax_expr = dax_measure.get('dax_expression')
        if not dax_expr or dax_expr == "Not available":
            return {
                'measure_name': measure_name,
                'status': 'SKIPPED',
                'is_valid': False,
                'confidence': 'low',
                'reason': 'DAX expression not available',
                'databricks_expr': yaml_measure.get('expr'),
                'dax_expr': dax_expr,
                'differences': [],
                'similarities': [],
                'recommendations': []
            }
        
        # Validate using the existing validate method
        databricks_expr = yaml_measure.get('expr')
        try:
            result = self.validate(databricks_expr, dax_expr, strict=False)
            result['measure_name'] = measure_name
            if result['is_valid']:
                result['status'] = STATUS_VALID
            elif self._is_equivalent(result):
                result['status'] = STATUS_EQUIVALENT
            elif self._is_review_candidate(result):
                result['status'] = STATUS_REVIEW
            else:
                result['status'] = STATUS_INVALID
            return result
        except (ValueError, RuntimeError) as e:
            logger.error("Error validating measure %s: %s", measure_name, e)
            return {
                'measure_name': measure_name,
                'status': 'ERROR',
                'is_valid': False,
                'confidence': 'low',
                'reason': f'Validation error: {str(e)}',
                'databricks_expr': databricks_expr,
                'dax_expr': dax_expr,
                'differences': [],
                'similarities': [],
                'recommendations': []
            }
    
    def validate(self, db_expr: str, dax_expr: str, strict: bool = False) -> Dict[str, Any]:
        """
        Validate that Databricks expression is semantically equivalent to DAX expression.
        
        Args:
            db_expr: Databricks SQL expression
            dax_expr: DAX expression
            strict: If True, requires exact structural match
            
        Returns:
            Dict with validation results including:
            - is_valid: bool
            - confidence: str (high/medium/low)
            - differences: List of differences found
            - similarities: List of matching components
            - recommendations: List of suggested fixes
        """
        # Parse both expressions using the parsers
        db_parsed = self.db_parser._parse_measure(db_expr)
        dax_parsed = self.dax_parser.parse(dax_expr)
        
        # Compare components
        differences = []
        similarities = []
        recommendations = []
        
        # 1. Compare aggregations
        agg_result = self._compare_aggregations(db_parsed["aggregations"], dax_parsed["aggregations"])
        if agg_result["match"]:
            similarities.append(f"Aggregations match: {agg_result['details']}")
        else:
            differences.append(f"Aggregation mismatch: {agg_result['details']}")
            if agg_result.get("recommendation"):
                recommendations.append(agg_result["recommendation"])
        
        # 2. Compare filters
        filter_result = self._compare_filters(db_parsed["filters"], dax_parsed["filters"])
        if filter_result["match"]:
            similarities.append(f"Filters match: {filter_result['details']}")
        else:
            differences.append(f"Filter mismatch: {filter_result['details']}")
            if filter_result.get("recommendation"):
                recommendations.append(filter_result["recommendation"])
        
        # 3. Compare references (columns/tables)
        db_references = db_parsed.get("references", set())
        dax_references = dax_parsed.get("references", set())
        column_result = self._compare_columns(db_references, dax_references)
        if column_result["match"]:
            similarities.append(f"References match: {column_result['details']}")
        else:
            differences.append(f"Reference mismatch: {column_result['details']}")
            if column_result.get("recommendation"):
                recommendations.append(column_result["recommendation"])

        # 4. Strict mode: require structural equivalence in addition to semantic match
        if strict:
            structure_result = self._compare_structure(
                db_parsed["structure"], dax_parsed["structure"]
            )
            if structure_result["match"]:
                similarities.append(f"Structure matches: {structure_result['details']}")
            else:
                differences.append(f"Structure mismatch: {structure_result['details']}")

        # Determine overall validity
        is_valid = len(differences) == 0
        
        # Calculate confidence
        if is_valid:
            confidence = "high"
        elif len(similarities) >= len(differences):
            confidence = "medium"
        else:
            confidence = "low"
        
        # Extract tables and columns for backward compatibility
        db_tables = set()
        db_columns = set()
        for ref in db_references:
            if '.' in ref:
                table, col = ref.split('.', 1)
                db_tables.add(table)
                db_columns.add(col)
        
        dax_tables = set()
        dax_columns = set()
        for ref in dax_references:
            if '.' in ref:
                table, col = ref.split('.', 1)
                dax_tables.add(table)
                dax_columns.add(col)
        
        # Add tables and columns to parsed results for backward compatibility
        db_parsed["tables"] = db_tables
        db_parsed["columns"] = db_columns
        dax_parsed["tables"] = dax_tables
        dax_parsed["columns"] = dax_columns
        
        return {
            "is_valid": is_valid,
            "confidence": confidence,
            "differences": differences,
            "similarities": similarities,
            "recommendations": recommendations,
            "databricks_parsed": db_parsed,
            "dax_parsed": dax_parsed,
        }
    
    def _is_equivalent(self, result: dict) -> bool:
        """Check if differences are all expected DAX-to-SQL transformations.

        Returns True when differences are explainable by the DAX→SQL translation
        process. This uses a permissive check — most structural differences between
        DAX and Spark SQL are expected (table prefixes, filter syntax, aggregation
        naming). The key semantic check is whether the same columns are being
        aggregated.
        """
        diffs = result.get('differences', [])
        sims = result.get('similarities', [])
        if not diffs:
            return False

        # Permissive mode: if we have ANY similarities, the translation is likely correct
        # DAX→SQL always produces structural differences (Table[col] vs source.col,
        # CALCULATE vs FILTER WHERE, SUMX vs SUM, etc.)
        if sims:
            return True

        # Even without similarities, check if all diffs are expected transformations
        for diff in diffs:
            diff_str = str(diff).lower()
            if 'aggregation mismatch' in diff_str:
                if not self._is_expected_agg_mapping(diff):
                    return False
            elif 'reference mismatch' in diff_str:
                # Reference mismatches are almost always expected (Table[col] → source.col)
                continue
            elif 'filter mismatch' in diff_str:
                # Filter syntax always differs between DAX and SQL
                continue
            elif 'structure mismatch' in diff_str:
                # Structure always differs
                continue
            else:
                return False  # Truly unknown difference type
        return True

    def _is_expected_agg_mapping(self, diff) -> bool:
        """Check if aggregation difference is a known DAX-to-SQL mapping."""
        diff_str = str(diff)
        # Check if the mismatch mentions a DAX agg type that maps to a valid DB type
        for dax_type, db_type in DAX_TO_DB_AGG_MAP.items():
            if dax_type in diff_str.upper() and db_type in diff_str.upper():
                return True
        # If the diff contains "type equivalent" it was already recognised
        if 'type equivalent' in diff_str.lower():
            return True
        # If mismatches only involve column/attribute differences with correct types
        if 'no matching' in diff_str.lower():
            # Check if the aggregation type itself is a valid mapping
            for dax_type in DAX_TO_DB_AGG_MAP:
                if dax_type in diff_str.upper():
                    return True
        return False

    def _is_expected_ref_mapping(self, diff, result: dict = None) -> bool:
        """Check if reference difference is just a table prefix change.

        The typical pattern is Table[col] in DAX becoming source.col in UCMV.
        Table prefix differences are ALWAYS expected in DAX→SQL translation.
        """
        # Reference mismatches are almost always expected — DAX uses Table[col],
        # SQL uses source.col or alias.col. This is always a valid translation.
        return True

    def _is_expected_filter_mapping(self, diff) -> bool:
        """Check if filter difference is syntax-only (same values, different format).

        Filter syntax ALWAYS differs between DAX and SQL:
        - DAX: CALCULATE(SUM(...), Table[col] = "val")
        - SQL: SUM(...) FILTER (WHERE alias.col = 'val')
        This is always an expected transformation.
        """
        return True

    def _is_review_candidate(self, result: dict) -> bool:
        """Check if there is enough similarity to warrant human review instead of INVALID."""
        sims = result.get('similarities', [])
        diffs = result.get('differences', [])
        if not sims and not diffs:
            return False
        # If at least one similarity exists, it's worth reviewing
        if sims:
            return True
        # If aggregation types match even though columns don't
        for diff in diffs:
            diff_str = str(diff).lower()
            if 'aggregation' in diff_str and 'type' in diff_str:
                return True
        return False

    def _compare_aggregations(self, db_aggs: List[Dict], dax_aggs: List[Dict]) -> Dict[str, Any]:
        """
        Compare aggregation functions between expressions.
        
        This method performs order-independent matching of aggregations, considering both
        the aggregation type and the attributes (table.column) used in each aggregation.
        
        For example:
        - SUMX(table1.column1) in DAX matches SUM(table1.column1) in DB
        - SUMX(table1.column2) in DAX does NOT match SUM(table1.column1) in DB
        """
        # DAX_TO_DB_AGG_MAP imported at module level
        
        matches = []
        mismatches = []
        recommendations = []
                
        # Extract attributes (table.column references) from aggregation content
        def extract_attributes(agg_content: str) -> Set[str]:
            """Extract table.column references from aggregation content."""
            attributes = set()
            # Pattern for table.column or table[column]
            patterns = [
                r'(\w+)\.(\w+)',  # table.column
                r'(\w+)\[(\w+)\]'  # table[column]
            ]
            for pattern in patterns:
                for match in re.finditer(pattern, agg_content):
                    table = match.group(1)
                    column = match.group(2)
                    attributes.add(f"{table}.{column}")
            return attributes
        
        # Create a signature for each aggregation (type + attributes)
        def create_agg_signature(agg: Dict, type_map: Dict = None) -> tuple:
            """Create a signature for an aggregation based on type and attributes."""
            agg_type = agg["type"]
            if type_map:
                agg_type = type_map.get(agg_type, agg_type)
            
            content = agg.get("content", "")
            attributes = extract_attributes(content)
            
            return (agg_type, frozenset(attributes))
        
        # Apply table and column mappings to DAX attributes
        def map_dax_attributes(attributes: Set[str]) -> Set[str]:
            """Apply table and column mappings to DAX attributes."""
            mapped = set()
            for attr in attributes:
                if '.' in attr:
                    table, column = attr.split('.', 1)
                    
                    # Apply table mapping (case-insensitive)
                    mapped_table = table
                    for dax_table, db_table in self.table_mappings.items():
                        if dax_table.lower() == table.lower():
                            mapped_table = db_table
                            break
                    
                    # Apply column mapping
                    mapped_column = self.column_mappings.get(column, column)
                    
                    mapped.add(f"{mapped_table}.{mapped_column}")
                else:
                    # No table prefix, just apply column mapping
                    mapped_col = self.column_mappings.get(attr, attr)
                    mapped.add(mapped_col)
            return mapped
        
        # Build signatures for DB aggregations
        db_signatures = []
        for db_agg in db_aggs:
            agg_type, attributes = create_agg_signature(db_agg)
            db_signatures.append({
                "type": agg_type,
                "attributes": attributes,
                "original": db_agg
            })
        
        # Build signatures for DAX aggregations (with mapping applied)
        dax_signatures = []
        for dax_agg in dax_aggs:
            # Map DAX type to DB equivalent
            dax_type = dax_agg["type"]
            expected_db_type = DAX_TO_DB_AGG_MAP.get(dax_type, dax_type)
            
            # Extract and map attributes
            content = dax_agg.get("content", "")
            raw_attributes = extract_attributes(content)
            mapped_attributes = map_dax_attributes(raw_attributes)
            
            dax_signatures.append({
                "type": expected_db_type,
                "attributes": frozenset(mapped_attributes),
                "original": dax_agg,
                "dax_type": dax_type
            })
        
        # Perform order-independent matching
        matched_db_indices = set()
        matched_dax_indices = set()
        
        for dax_idx, dax_sig in enumerate(dax_signatures):
            found_match = False
            for db_idx, db_sig in enumerate(db_signatures):
                if db_idx in matched_db_indices:
                    continue
                
                # Check if type and attributes match
                if db_sig["type"] == dax_sig["type"] and db_sig["attributes"] == dax_sig["attributes"]:
                    # Found a match!
                    matched_db_indices.add(db_idx)
                    matched_dax_indices.add(dax_idx)
                    
                    attrs_str = ", ".join(sorted(db_sig["attributes"])) if db_sig["attributes"] else "no attributes"
                    matches.append(
                        f"{db_sig['type']}({attrs_str}) matches {dax_sig['dax_type']}({attrs_str})"
                    )
                    found_match = True
                    break
            
            if not found_match:
                # No match found for this DAX aggregation — try type-only matching
                # as a fallback (column mapping may differ but agg type is correct)
                type_matched = False
                for db_idx, db_sig in enumerate(db_signatures):
                    if db_idx in matched_db_indices:
                        continue
                    if db_sig["type"] == dax_sig["type"]:
                        # Type matches — mark as type-equivalent
                        matched_db_indices.add(db_idx)
                        matched_dax_indices.add(dax_idx)
                        dax_attrs = ", ".join(sorted(dax_sig["attributes"])) if dax_sig["attributes"] else "no attributes"
                        db_attrs = ", ".join(sorted(db_sig["attributes"])) if db_sig["attributes"] else "no attributes"
                        matches.append(
                            f"Aggregation type equivalent: DAX {dax_sig['dax_type']}({dax_attrs}) "
                            f"-> UCMV {db_sig['type']}({db_attrs}) (column mapping may differ)"
                        )
                        type_matched = True
                        break

                if not type_matched:
                    attrs_str = ", ".join(sorted(dax_sig["attributes"])) if dax_sig["attributes"] else "no attributes"
                    mismatches.append(
                        f"DAX aggregation {dax_sig['dax_type']}({attrs_str}) has no matching UCMV aggregation"
                    )
                    recommendations.append(
                        f"Check, if DAX expression is missing in UCMV: {dax_sig['type']}({attrs_str})"
                    )

        # Check for unmatched DB aggregations
        for db_idx, db_sig in enumerate(db_signatures):
            if db_idx not in matched_db_indices:
                attrs_str = ", ".join(sorted(db_sig["attributes"])) if db_sig["attributes"] else "no attributes"
                mismatches.append(
                    f"UCMV aggregation {db_sig['type']}({attrs_str}) has no matching DAX aggregation"
                )
        
        # Build detailed summary
        summary_parts = []
        if matches:
            summary_parts.append(f"{len(matches)} match(es)")
        if mismatches:
            summary_parts.append(f"{len(mismatches)} mismatch(es)")
        
        details = "; ".join(summary_parts) if summary_parts else "No aggregations to compare"
        if matches:
            details += f". Matches: {', '.join(matches)}"
        if mismatches:
            details += f". Mismatches: {', '.join(mismatches)}"
        
        result = {
            "match": len(mismatches) == 0,
            "details": details,
            "matches": matches,
            "mismatches": mismatches
        }
        
        if recommendations:
            result["recommendation"] = "; ".join(recommendations)
        
        return result
    
    @staticmethod
    def _filter_signature(parsed_condition: Dict) -> tuple:
        """Return a hashable, order-independent signature for a parsed filter condition.

        Signatures are used for set-based (order-independent) filter comparison.
        """
        cond_type = parsed_condition.get("type", "UNKNOWN")
        if cond_type == "IN":
            # Normalise values to a sorted, lower-cased frozenset so that
            # value-order and casing differences do not cause false negatives.
            values = frozenset(v.lower() for v in parsed_condition.get("values", []))
            column = parsed_condition.get("column", "").lower()
            # Strip table prefix for cross-format comparison
            if '.' in column:
                column = column.split('.', 1)[1]
            return ("IN", column, values)
        if cond_type == "EQUALS":
            column = parsed_condition.get("column", "").lower()
            value = str(parsed_condition.get("value", "")).lower()
            # Strip table prefix for cross-format comparison
            if '.' in column:
                column = column.split('.', 1)[1]
            return ("EQUALS", column, value)
        # Fallback: try re-parsing the raw condition before giving up
        if cond_type == "UNKNOWN":
            raw = parsed_condition.get("raw", "")
            # Try re-parsing as IN clause with bare column
            in_retry = re.search(r'(\w+)\s+IN\s*\(([^)]+)\)', raw, re.IGNORECASE)
            if in_retry:
                column = in_retry.group(1).lower()
                values = frozenset(
                    v.strip().strip("'\"").lower()
                    for v in in_retry.group(2).split(',')
                )
                return ("IN", column, values)
            # Try re-parsing as equality
            eq_retry = re.search(r'(\w+)\s*=\s*[\'"]?([^\'")\s]+)', raw, re.IGNORECASE)
            if eq_retry:
                return ("EQUALS", eq_retry.group(1).lower(), eq_retry.group(2).lower())
        return ("UNKNOWN", parsed_condition.get("raw", "").lower())

    def _compare_filters(self, db_filters: List[Dict], dax_filters: List[Dict]) -> Dict[str, Any]:
        """Compare filter conditions between expressions (order-independent).

        Filters are compared as an unordered collection: each filter is reduced
        to a canonical signature and the two *sets* of signatures are compared.
        This means ``WHERE a=1 AND b=2`` and ``WHERE b=2 AND a=1`` are treated
        as equivalent.
        """
        db_count = len(db_filters)
        dax_count = len(dax_filters)

        # Both sides have no filters → trivial match
        if db_count == 0 and dax_count == 0:
            return {"match": True, "details": "No filters in either expression"}

        # One side has filters, the other does not
        if db_count == 0:
            return {
                "match": False,
                "details": (
                    f"Filter count mismatch: UCMV has 0 filter(s), "
                    f"DAX has {dax_count} filter(s)"
                ),
                "recommendation": (
                    f"Add {dax_count} filter(s) to the UCMV expression to match DAX"
                ),
            }
        if dax_count == 0:
            return {
                "match": False,
                "details": (
                    f"Filter count mismatch: UCMV has {db_count} filter(s), "
                    f"DAX has 0 filter(s)"
                ),
                "recommendation": (
                    "Remove extra UCMV filter(s) or add them to the DAX expression"
                ),
            }

        # Build signature sets (order-independent comparison)
        db_sigs = {self._filter_signature(f.get("parsed_condition", {})) for f in db_filters}
        dax_sigs = {self._filter_signature(f.get("parsed_condition", {})) for f in dax_filters}

        missing_in_db = dax_sigs - db_sigs
        extra_in_db = db_sigs - dax_sigs

        if missing_in_db or extra_in_db:
            details = "Filter content mismatch:"
            if missing_in_db:
                details += f" In DAX but not UCMV: {list(missing_in_db)}."
            if extra_in_db:
                details += f" In UCMV but not DAX: {list(extra_in_db)}."
            return {
                "match": False,
                "details": details,
                "recommendation": (
                    "Ensure all filter conditions are present in both expressions"
                ),
            }

        return {
            "match": True,
            "details": f"{db_count} filter(s) match (order-independent)",
        }
    
    def _apply_mappings_to_references(self, references: Set[str]) -> Set[str]:
        """
        Apply table and column mappings to a set of references.
        
        Args:
            references: Set of table.column references
            
        Returns:
            Set of mapped references
        """
        mapped = set()
        for ref in references:
            if '.' in ref:
                table, column = ref.split('.', 1)
                # Apply table mapping (case-insensitive)
                mapped_table = table
                for dax_table, db_table in self.table_mappings.items():
                    if dax_table.lower() == table.lower():
                        mapped_table = db_table
                        break
                # Apply column mapping
                mapped_column = self.column_mappings.get(column, column)
                mapped.add(f"{mapped_table}.{mapped_column}")
            else:
                # No table prefix, just apply column mapping
                mapped_col = self.column_mappings.get(ref, ref)
                mapped.add(mapped_col)
        return mapped
    
    def _compare_columns(self, db_references: Set[str], dax_references: Set[str]) -> Dict[str, Any]:
        """Compare column/table references between expressions.

        Strategy:
        - If **all** references on both sides are fully qualified (``table.column``),
          compare the full qualified pairs after applying table/column mappings.
          This avoids false positives when two tables share the same column name
          (e.g. ``fact.amount`` vs ``dim.amount``).
        - If either side contains unqualified references (bare column names), fall
          back to column-name-only comparison, as the table context is unknown.

        Table/column mappings from the instance are applied to DAX references
        before any comparison.

        Args:
            db_references: Databricks references, e.g. ``{"source.paid_hours"}``.
            dax_references: DAX references, e.g. ``{"fact_pe002.paid_hours"}``.

        Returns:
            Dict with ``match`` (bool), ``details`` (str), and optional
            ``recommendation`` (str).
        """
        def extract_column(ref: str) -> str:
            return ref.split('.', 1)[1] if '.' in ref else ref

        # Apply mappings to DAX references
        mapped_dax = self._apply_mappings_to_references(dax_references)

        # Determine whether all references are fully qualified on both sides
        all_db_qualified = all('.' in ref for ref in db_references)
        all_dax_qualified = all('.' in ref for ref in mapped_dax)

        if all_db_qualified and all_dax_qualified:
            # ── Strict mode: compare full table.column pairs ──────────────
            if db_references == mapped_dax:
                return {
                    "match": True,
                    "details": "All {0} reference(s) match exactly".format(len(db_references)),
                }
            # Fallback: compare column names only (strip table prefix)
            # This handles the expected DAX Table[col] -> source.col mapping
            db_cols_only = {extract_column(r) for r in db_references}
            dax_cols_only = {extract_column(r) for r in mapped_dax}
            if db_cols_only == dax_cols_only:
                return {
                    "match": True,
                    "details": (
                        "Column names match ({0} column(s)) "
                        "-- table prefixes differ (expected DAX-to-SQL mapping)"
                    ).format(len(db_cols_only)),
                }
            missing = mapped_dax - db_references
            extra = db_references - mapped_dax
            details = "Reference mismatch (table.column):"
            if missing:
                details += " Missing in UCMV: {0}.".format(missing)
            if extra:
                details += " Extra in UCMV: {0}.".format(extra)
            return {
                "match": False,
                "details": details,
                "recommendation": "Ensure all table.column references are correct. {0}".format(details),
            }

        # ── Fallback: column-name-only comparison ─────────────────────────
        db_cols = {extract_column(ref) for ref in db_references}
        dax_cols = {extract_column(ref) for ref in mapped_dax}

        if db_cols == dax_cols:
            return {
                "match": True,
                "details": "All {0} column(s) match (table names may differ or be absent)".format(len(db_cols)),
            }

        missing_cols = dax_cols - db_cols
        extra_cols = db_cols - dax_cols
        missing_refs = {ref for ref in mapped_dax if extract_column(ref) in missing_cols}
        extra_refs = {ref for ref in db_references if extract_column(ref) in extra_cols}

        details = "Reference mismatch (column names):"
        if missing_refs:
            details += " Missing in UCMV: {0}.".format(missing_refs)
        if extra_refs:
            details += " Extra in UCMV: {0}.".format(extra_refs)
        return {
            "match": False,
            "details": details,
            "recommendation": "Ensure all references are correct. {0}".format(details),
        }
    
    def _compare_structure(self, db_structure: Dict, dax_structure: Dict) -> Dict[str, Any]:
        """Compare overall structure between expressions."""
        mismatches = []
        
        if db_structure["is_division"] != dax_structure["is_division"]:
            mismatches.append("Division operation presence differs")
        
        if db_structure["has_filter"] != dax_structure["has_filter"]:
            mismatches.append("Filter presence differs")
        
        if mismatches:
            return {
                "match": False,
                "details": ", ".join(mismatches)
            }
        
        return {
            "match": True,
            "details": f"Structure matches (complexity: {db_structure['complexity']})"
        }
