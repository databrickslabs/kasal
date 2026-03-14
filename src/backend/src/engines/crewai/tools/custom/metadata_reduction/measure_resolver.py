"""
Lightweight Measure Resolver for Power BI Metadata Reduction.

After fuzzy+LLM selection, resolves selected measures into concrete types:
- MODEL_MEASURE: Direct match to a model measure
- FILTERED_MEASURE: Base measure filtered by a column value (e.g., "Completeness Score")
- COMPOSITE_MEASURE: Aggregation of multiple filtered siblings (e.g., "Total Score")

Also performs expression analysis:
- safe_for_decompose: True if expression uses CALCULATE without dangerous context modifiers
- handles_date_internally: True if expression contains date intelligence functions
- has_removefilters: True if expression contains REMOVEFILTERS

All logic is deterministic (no LLM).

Author: Kasal Team
Date: 2026
"""

import re
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class MeasureType(str, Enum):
    MODEL_MEASURE = "model_measure"
    FILTERED_MEASURE = "filtered_measure"
    COMPOSITE_MEASURE = "composite_measure"
    UNRESOLVED = "unresolved"


@dataclass
class ExpressionFlags:
    """Analysis flags for a DAX measure expression."""
    safe_for_decompose: bool = True
    handles_date_internally: bool = False
    has_removefilters: bool = False
    has_allselected: bool = False
    has_allexcept: bool = False
    uses_calculate: bool = False


@dataclass
class ResolvedMeasure:
    """A measure with resolution type and expression analysis."""
    name: str
    table: str = ""
    expression: str = ""
    resolution_type: MeasureType = MeasureType.UNRESOLVED
    base_measure: Optional[str] = None  # For FILTERED: the base measure name
    filter_column: Optional[str] = None  # For FILTERED: "Table[Column]"
    filter_value: Optional[str] = None   # For FILTERED: the filter value
    sibling_measures: List[str] = field(default_factory=list)  # For COMPOSITE
    expression_flags: ExpressionFlags = field(default_factory=ExpressionFlags)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for output."""
        result: Dict[str, Any] = {
            "name": self.name,
            "table": self.table,
            "resolution_type": self.resolution_type.value,
            "expression_flags": {
                "safe_for_decompose": self.expression_flags.safe_for_decompose,
                "handles_date_internally": self.expression_flags.handles_date_internally,
                "has_removefilters": self.expression_flags.has_removefilters,
                "has_allselected": self.expression_flags.has_allselected,
                "has_allexcept": self.expression_flags.has_allexcept,
                "uses_calculate": self.expression_flags.uses_calculate,
            },
        }
        if self.base_measure:
            result["base_measure"] = self.base_measure
        if self.filter_column:
            result["filter_column"] = self.filter_column
        if self.filter_value:
            result["filter_value"] = self.filter_value
        if self.sibling_measures:
            result["sibling_measures"] = self.sibling_measures
        return result


# ─── DAX Pattern Constants ───────────────────────────────────────────────────

# Date intelligence functions
_DATE_FUNCTIONS = re.compile(
    r"\b(DATESYTD|DATESMTD|DATESQTD|DATESBETWEEN|DATESINPERIOD|"
    r"TOTALYTD|TOTALMTD|TOTALQTD|SAMEPERIODLASTYEAR|"
    r"PREVIOUSMONTH|PREVIOUSQUARTER|PREVIOUSYEAR|PREVIOUSDAY|"
    r"NEXTMONTH|NEXTQUARTER|NEXTYEAR|NEXTDAY|"
    r"PARALLELPERIOD|DATEADD|LASTDATE|FIRSTDATE|"
    r"STARTOFMONTH|STARTOFQUARTER|STARTOFYEAR|"
    r"ENDOFMONTH|ENDOFQUARTER|ENDOFYEAR|"
    r"CLOSINGBALANCEMONTH|CLOSINGBALANCEQUARTER|CLOSINGBALANCEYEAR|"
    r"OPENINGBALANCEMONTH|OPENINGBALANCEQUARTER|OPENINGBALANCEYEAR)\b",
    re.IGNORECASE,
)

# Context modifier patterns (make decompose unsafe)
_DANGEROUS_CONTEXT = re.compile(
    r"\b(USERELATIONSHIP|CROSSFILTER|TREATAS)\b",
    re.IGNORECASE,
)

# REMOVEFILTERS pattern
_REMOVEFILTERS = re.compile(r"\bREMOVEFILTERS\b", re.IGNORECASE)

# ALL / ALLSELECTED / ALLEXCEPT patterns
_ALLSELECTED = re.compile(r"\bALLSELECTED\b", re.IGNORECASE)
_ALLEXCEPT = re.compile(r"\bALLEXCEPT\b", re.IGNORECASE)
_ALL_FUNC = re.compile(r"\bALL\s*\(", re.IGNORECASE)

# CALCULATE pattern
_CALCULATE = re.compile(r"\bCALCULATE\s*\(", re.IGNORECASE)

# Qualified column reference: 'Table'[Column] or Table[Column]
_QUALIFIED_COL_REF = re.compile(r"'?([^'[\]]+)'?\[([^\]]+)\]")

# Measure reference: [MeasureName]
_MEASURE_REF = re.compile(r"(?<!')\[([^\]]+)\]")


class MeasureResolver:
    """Deterministic measure resolution: classify measures by type and analyze expressions."""

    def __init__(
        self,
        all_measures: List[Dict],
        all_tables: List[Dict],
        sample_data: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            all_measures: Flat list of measure dicts with 'name', 'expression', 'table'.
            all_tables: List of table dicts with 'name', 'columns', 'measures'.
            sample_data: Optional sample data dict (key="Table[Column]" → {"sample_values": [...]}).
        """
        self._measure_map: Dict[str, Dict] = {m["name"]: m for m in all_measures}
        self._measure_to_table: Dict[str, str] = {}
        self._sample_data = sample_data or {}

        # Index measures
        for m in all_measures:
            if "table" in m:
                self._measure_to_table[m["name"]] = m["table"]

        # Also index from table-embedded measures
        for table in all_tables:
            for m in table.get("measures", []):
                if isinstance(m, dict):
                    if m.get("name") not in self._measure_map:
                        self._measure_map[m["name"]] = m
                    if m.get("name") not in self._measure_to_table:
                        self._measure_to_table[m["name"]] = table["name"]

        # Build column value index for filtered measure detection
        # Maps normalized_value → [(table, column, original_value)]
        self._value_index: Dict[str, List[Tuple[str, str, str]]] = {}
        for key, entry in self._sample_data.items():
            if "[" not in key:
                continue
            table_name = key.split("[")[0]
            col_name = key.split("[")[1].rstrip("]")
            values = entry.get("sample_values", []) if isinstance(entry, dict) else []
            for val in values:
                if isinstance(val, str) and val:
                    norm_val = val.lower().strip()
                    self._value_index.setdefault(norm_val, []).append(
                        (table_name, col_name, val)
                    )

        # Also build from column metadata
        self._column_values: Dict[str, List[Tuple[str, str]]] = {}
        for table in all_tables:
            tname = table.get("name", "")
            for col in table.get("columns", []):
                if isinstance(col, dict):
                    cname = col.get("name", "")
                    if cname:
                        self._column_values.setdefault(cname, []).append((tname, cname))

    def resolve(self, selected_measures: List[Dict]) -> List[Dict]:
        """Resolve selected measures and return enriched measure dicts.

        For each measure:
        1. Try exact match → MODEL_MEASURE
        2. Try decomposition (name = base + filter_value) → FILTERED_MEASURE
        3. Try sibling detection → COMPOSITE_MEASURE
        4. Else → UNRESOLVED

        Then analyze expression for flags.

        Returns: Original measure dicts with added '_resolution' key.
        """
        results = []
        resolved_cache: Dict[str, ResolvedMeasure] = {}

        for measure_dict in selected_measures:
            name = measure_dict.get("name", "")
            expression = measure_dict.get("expression", "") or ""
            table = measure_dict.get("table", "")

            resolved = self._resolve_single(name, expression, table)
            resolved_cache[name] = resolved

            # Enrich the original measure dict
            enriched = dict(measure_dict)
            enriched["_resolution"] = resolved.to_dict()
            results.append(enriched)

        # Second pass: detect composites (measures whose components are all filtered)
        self._detect_composites(results, resolved_cache)

        resolved_types = {}
        for r in results:
            rt = r.get("_resolution", {}).get("resolution_type", "unresolved")
            resolved_types[rt] = resolved_types.get(rt, 0) + 1

        logger.info(
            f"[MeasureResolver] Resolved {len(results)} measures: {resolved_types}"
        )

        return results

    def _resolve_single(
        self, name: str, expression: str, table: str
    ) -> ResolvedMeasure:
        """Resolve a single measure."""
        # Step 1: Check if it's a known model measure
        if name in self._measure_map:
            flags = self._analyze_expression(expression)
            return ResolvedMeasure(
                name=name,
                table=table or self._measure_to_table.get(name, ""),
                expression=expression,
                resolution_type=MeasureType.MODEL_MEASURE,
                expression_flags=flags,
            )

        # Step 2: Try filtered measure decomposition
        filtered = self._try_filtered_decomposition(name)
        if filtered:
            return filtered

        # Step 3: Unresolved
        return ResolvedMeasure(
            name=name,
            table=table,
            expression=expression,
            resolution_type=MeasureType.UNRESOLVED,
            expression_flags=self._analyze_expression(expression),
        )

    def _try_filtered_decomposition(self, name: str) -> Optional[ResolvedMeasure]:
        """Try to decompose a measure name into base_measure + filter_value.

        Example: "Completeness Score" → base="Score", filter_value="Completeness"
        if "Completeness" exists as a value in some column.
        """
        name_lower = name.lower()
        words = name.split()

        if len(words) < 2:
            return None

        # Try splitting at each word boundary
        for i in range(1, len(words)):
            prefix = " ".join(words[:i]).lower()
            suffix = " ".join(words[i:]).lower()

            # Check if prefix is a filter value and suffix is a known measure
            base_from_suffix = self._find_measure_match(suffix)
            if base_from_suffix and prefix in self._value_index:
                matches = self._value_index[prefix]
                if matches:
                    table_name, col_name, original_value = matches[0]
                    base_expr = self._measure_map.get(base_from_suffix, {}).get("expression", "")
                    return ResolvedMeasure(
                        name=name,
                        table=self._measure_to_table.get(base_from_suffix, ""),
                        expression=base_expr,
                        resolution_type=MeasureType.FILTERED_MEASURE,
                        base_measure=base_from_suffix,
                        filter_column=f"{table_name}[{col_name}]",
                        filter_value=original_value,
                        expression_flags=self._analyze_expression(base_expr),
                    )

            # Check reverse: suffix is filter value, prefix is base measure
            base_from_prefix = self._find_measure_match(prefix)
            if base_from_prefix and suffix in self._value_index:
                matches = self._value_index[suffix]
                if matches:
                    table_name, col_name, original_value = matches[0]
                    base_expr = self._measure_map.get(base_from_prefix, {}).get("expression", "")
                    return ResolvedMeasure(
                        name=name,
                        table=self._measure_to_table.get(base_from_prefix, ""),
                        expression=base_expr,
                        resolution_type=MeasureType.FILTERED_MEASURE,
                        base_measure=base_from_prefix,
                        filter_column=f"{table_name}[{col_name}]",
                        filter_value=original_value,
                        expression_flags=self._analyze_expression(base_expr),
                    )

        return None

    def _find_measure_match(self, name_fragment: str) -> Optional[str]:
        """Find a known measure matching the name fragment (case-insensitive)."""
        for measure_name in self._measure_map:
            if measure_name.lower() == name_fragment:
                return measure_name
        return None

    def _detect_composites(
        self,
        results: List[Dict],
        resolved_cache: Dict[str, ResolvedMeasure],
    ) -> None:
        """Second pass: detect composite measures.

        A measure is composite if:
        - It's currently MODEL_MEASURE or UNRESOLVED
        - Its expression references multiple measures that are FILTERED siblings
          (same base measure, different filter values)
        """
        # Find all filtered measures by base
        filtered_by_base: Dict[str, List[str]] = {}
        for name, resolved in resolved_cache.items():
            if resolved.resolution_type == MeasureType.FILTERED_MEASURE and resolved.base_measure:
                filtered_by_base.setdefault(resolved.base_measure, []).append(name)

        if not filtered_by_base:
            return

        # Check each result for composite pattern
        for result in results:
            name = result.get("name", "")
            expression = result.get("expression", "") or ""
            resolution = result.get("_resolution", {})
            current_type = resolution.get("resolution_type", "")

            if current_type not in ("model_measure", "unresolved"):
                continue

            # Extract measure references from expression
            refs = set(_MEASURE_REF.findall(expression))
            refs.discard(name)  # Exclude self-reference

            # Check if refs are filtered siblings of the same base
            for base_name, siblings in filtered_by_base.items():
                overlap = refs & set(siblings)
                if len(overlap) >= 2:
                    # This measure references multiple filtered siblings → composite
                    resolved = resolved_cache[name]
                    resolved.resolution_type = MeasureType.COMPOSITE_MEASURE
                    resolved.base_measure = base_name
                    resolved.sibling_measures = list(overlap)
                    result["_resolution"] = resolved.to_dict()
                    logger.info(
                        f"[MeasureResolver] Composite detected: '{name}' "
                        f"aggregates {len(overlap)} filtered siblings of '{base_name}'"
                    )
                    break

    @staticmethod
    def _analyze_expression(expression: str) -> ExpressionFlags:
        """Analyze a DAX expression for semantic flags."""
        if not expression:
            return ExpressionFlags()

        flags = ExpressionFlags()

        flags.uses_calculate = bool(_CALCULATE.search(expression))
        flags.has_removefilters = bool(_REMOVEFILTERS.search(expression))
        flags.has_allselected = bool(_ALLSELECTED.search(expression))
        flags.has_allexcept = bool(_ALLEXCEPT.search(expression))
        flags.handles_date_internally = bool(_DATE_FUNCTIONS.search(expression))

        # safe_for_decompose: True if no dangerous context modifiers
        if _DANGEROUS_CONTEXT.search(expression):
            flags.safe_for_decompose = False
        if flags.has_removefilters:
            flags.safe_for_decompose = False
        if flags.has_allexcept:
            flags.safe_for_decompose = False

        return flags
