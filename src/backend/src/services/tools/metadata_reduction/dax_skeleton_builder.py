"""
DAX Skeleton Builder for Power BI Metadata Reduction.

Builds partial DAX skeletons based on measure resolver output:
- MODEL_MEASURE (no REMOVEFILTERS) → SUMMARIZECOLUMNS
- FILTERED_MEASURE → CALCULATE with filter
- COMPOSITE_MEASURE → VAR approach with multiple CALCULATE lines

Output: skeleton string + can_skip_llm flag + open_placeholders list.
The DAX tool receives the skeleton as additional context in the LLM prompt.

Author: Kasal Team
Date: 2026
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Date table detection: canonical names (case-insensitive)
_DATE_TABLE_NAMES = frozenset({
    "date", "calendar", "dates", "dim_date", "dimdate", "date table", "calendar table",
    "dim date", "dim calendar", "datetable",
})

# Date column detection: canonical column names in a date table (case-insensitive)
_DATE_COLUMN_HINTS = frozenset({"date", "year", "month", "day", "quarter", "week"})


@dataclass
class DaxSkeleton:
    """Result of DAX skeleton building."""
    skeleton: str = ""
    can_skip_llm: bool = False
    open_placeholders: List[str] = field(default_factory=list)
    strategy_notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "skeleton": self.skeleton,
            "can_skip_llm": self.can_skip_llm,
            "open_placeholders": self.open_placeholders,
            "strategy_notes": self.strategy_notes,
        }


class DaxSkeletonBuilder:
    """Build partial DAX skeletons based on measure resolution results."""

    def build(
        self,
        resolved_measures: List[Dict],
        relationships: Optional[List[Dict]] = None,
        active_filters: Optional[Dict[str, Any]] = None,
        question_intent: Optional[Dict[str, Any]] = None,
        tables: Optional[List[Dict]] = None,
        dimension_bindings: Optional[List[Dict]] = None,
    ) -> DaxSkeleton:
        """Build a DAX skeleton from resolved measures.

        Args:
            resolved_measures: List of measure dicts with '_resolution' metadata.
            relationships: Model relationships for join hints.
            active_filters: Active filters to apply.
            question_intent: Preprocessor output for shape/grouping hints.
            tables: Full list of model tables (for date table detection).
            dimension_bindings: Resolved dimension bindings with table-qualified columns.

        Returns:
            DaxSkeleton with partial DAX and metadata.
        """
        if not resolved_measures:
            return DaxSkeleton(strategy_notes=["No measures to build skeleton for"])

        # Separate by resolution type
        model_measures = []
        filtered_measures = []
        composite_measures = []
        other_measures = []

        for m in resolved_measures:
            resolution = m.get("_resolution", {})
            rtype = resolution.get("resolution_type", "unresolved")

            if rtype == "model_measure":
                model_measures.append(m)
            elif rtype == "filtered_measure":
                filtered_measures.append(m)
            elif rtype == "composite_measure":
                composite_measures.append(m)
            else:
                other_measures.append(m)

        # Determine grouping columns from intent, preferring dimension_bindings
        group_cols = self._extract_group_columns(question_intent, dimension_bindings)

        # Detect date table for time intelligence skeleton generation
        date_table, date_col = self._detect_date_table(tables or [])

        # Build skeleton based on what we have
        if len(resolved_measures) == 1:
            result = self._build_single_measure(
                resolved_measures[0], group_cols, active_filters
            )
        elif composite_measures:
            result = self._build_composite(
                composite_measures[0], filtered_measures, group_cols, active_filters
            )
        elif filtered_measures and not model_measures and not other_measures:
            result = self._build_filtered_only(
                filtered_measures, group_cols, active_filters
            )
        else:
            # Multiple model measures or mixed types
            result = self._build_multi_measure(
                resolved_measures, group_cols, active_filters
            )

        # Inject time intelligence date filter VAR block if applicable
        if date_table and date_col and question_intent:
            time_intelligence = question_intent.get("time_intelligence", {})
            if time_intelligence:
                date_filter_block = self._build_date_filter(
                    time_intelligence, date_table, date_col
                )
                if date_filter_block:
                    result.skeleton = date_filter_block + "\n" + result.skeleton
                    result.strategy_notes.append(
                        f"Time intelligence applied using '{date_table}'[{date_col}]"
                    )
                    result.can_skip_llm = False

        return result

    def _build_single_measure(
        self,
        measure: Dict,
        group_cols: List[str],
        active_filters: Optional[Dict[str, Any]] = None,
    ) -> DaxSkeleton:
        """Build skeleton for a single measure query."""
        name = measure.get("name", "")
        resolution = measure.get("_resolution", {})
        rtype = resolution.get("resolution_type", "unresolved")
        flags = resolution.get("expression_flags", {})

        placeholders = []
        notes = []

        if rtype == "filtered_measure":
            return self._build_filtered_skeleton(
                resolution, group_cols, active_filters
            )

        if rtype == "model_measure" and flags.get("has_removefilters"):
            notes.append(
                f"WARNING: [{name}] uses REMOVEFILTERS — do NOT call directly in SUMMARIZECOLUMNS. "
                f"Decompose the measure or use CALCULATE with explicit filter context."
            )
            placeholders.append("DECOMPOSE_MEASURE")
            skeleton = (
                f"// [{name}] uses REMOVEFILTERS — needs decomposition\n"
                f"// LLM: analyze the measure expression and decompose appropriately\n"
                f"EVALUATE\n"
                f"SUMMARIZECOLUMNS(\n"
                f"    {self._format_group_cols(group_cols, placeholders)}\n"
                f'    "Result", CALCULATE([{name}], /* add explicit filter context */)\n'
                f")"
            )
            return DaxSkeleton(
                skeleton=skeleton,
                can_skip_llm=False,
                open_placeholders=placeholders,
                strategy_notes=notes,
            )

        # Context-routing measure: ISFILTERED/SELECTEDVALUE/HASONEVALUE/ISINSCOPE
        # These measures branch on filter context — must wrap in explicit CALCULATE
        if rtype == "model_measure" and flags.get("has_context_routing"):
            notes.append(
                f"CONTEXT-ROUTING: [{name}] uses ISFILTERED/SELECTEDVALUE/HASONEVALUE — "
                f"wrap in CALCULATE with explicit filter to activate the correct branch."
            )
            placeholders.append("EXPLICIT_FILTER_CONTEXT")
            filter_lines = self._format_filter_lines(active_filters)
            skeleton = (
                f"// [{name}] uses context-routing — LLM must supply explicit filter in CALCULATE\n"
                f"EVALUATE\n"
                f"SUMMARIZECOLUMNS(\n"
                f"    {self._format_group_cols(group_cols, placeholders)}"
                f"{filter_lines}"
                f'\n    "Result", CALCULATE([{name}], /* LLM: add explicit filter context to activate routing branch */)\n'
                f")"
            )
            return DaxSkeleton(
                skeleton=skeleton,
                can_skip_llm=False,
                open_placeholders=placeholders,
                strategy_notes=notes,
            )

        # Simple model measure — can be a complete skeleton
        if group_cols:
            filter_lines = self._format_filter_lines(active_filters)
            skeleton = (
                f"EVALUATE\n"
                f"SUMMARIZECOLUMNS(\n"
                f"    {self._format_group_cols(group_cols, placeholders)}"
                f"{filter_lines}"
                f'\n    "Result", [{name}]\n'
                f")"
            )
            can_skip = not placeholders and not flags.get("has_removefilters")
        else:
            placeholders.append("GROUPING_COLUMNS")
            skeleton = (
                f"EVALUATE\n"
                f"SUMMARIZECOLUMNS(\n"
                f"    /* LLM: add grouping columns here */\n"
                f'    "Result", [{name}]\n'
                f")"
            )
            can_skip = False

        return DaxSkeleton(
            skeleton=skeleton,
            can_skip_llm=can_skip,
            open_placeholders=placeholders,
            strategy_notes=notes,
        )

    def _build_filtered_skeleton(
        self,
        resolution: Dict,
        group_cols: List[str],
        active_filters: Optional[Dict[str, Any]] = None,
    ) -> DaxSkeleton:
        """Build skeleton for a filtered measure."""
        base = resolution.get("base_measure", "")
        filter_col = resolution.get("filter_column", "")
        filter_val = resolution.get("filter_value", "")

        placeholders = []
        filter_lines = self._format_filter_lines(active_filters)

        skeleton = (
            f"EVALUATE\n"
            f"SUMMARIZECOLUMNS(\n"
            f"    {self._format_group_cols(group_cols, placeholders)}"
            f"{filter_lines}"
            f'\n    "Result", CALCULATE([{base}], {filter_col} = "{filter_val}")\n'
            f")"
        )

        can_skip = bool(base and filter_col and filter_val and not placeholders)

        return DaxSkeleton(
            skeleton=skeleton,
            can_skip_llm=can_skip,
            open_placeholders=placeholders,
            strategy_notes=[f"Filtered measure: [{base}] WHERE {filter_col} = \"{filter_val}\""],
        )

    def _build_composite(
        self,
        composite: Dict,
        siblings: List[Dict],
        group_cols: List[str],
        active_filters: Optional[Dict[str, Any]] = None,
    ) -> DaxSkeleton:
        """Build skeleton for a composite measure using VAR approach."""
        resolution = composite.get("_resolution", {})
        base = resolution.get("base_measure", "")
        sibling_names = resolution.get("sibling_measures", [])

        placeholders = []
        notes = [f"Composite measure aggregating {len(sibling_names)} filtered siblings"]

        # Build VAR lines for each sibling
        var_lines = []
        result_parts = []
        for i, sib_name in enumerate(sibling_names):
            # Find the sibling's resolution
            sib_resolution = None
            for s in siblings:
                if s.get("name") == sib_name:
                    sib_resolution = s.get("_resolution", {})
                    break

            var_name = f"_v{i+1}"
            if sib_resolution and sib_resolution.get("filter_column"):
                fc = sib_resolution["filter_column"]
                fv = sib_resolution.get("filter_value", "")
                var_lines.append(
                    f'VAR {var_name} = CALCULATE([{base}], {fc} = "{fv}")'
                )
            else:
                var_lines.append(f"VAR {var_name} = [{sib_name}]")
            result_parts.append(var_name)

        filter_lines = self._format_filter_lines(active_filters)

        vars_block = "\n".join(f"    {vl}" for vl in var_lines)
        result_expr = " + ".join(result_parts)

        skeleton = (
            f"EVALUATE\n"
            f"ADDCOLUMNS(\n"
            f"    SUMMARIZECOLUMNS(\n"
            f"        {self._format_group_cols(group_cols, placeholders)}"
            f"{filter_lines}\n"
            f"    ),\n"
            f'{vars_block}\n'
            f'    "Result", {result_expr}\n'
            f")"
        )

        return DaxSkeleton(
            skeleton=skeleton,
            can_skip_llm=False,
            open_placeholders=placeholders,
            strategy_notes=notes,
        )

    def _build_filtered_only(
        self,
        filtered_measures: List[Dict],
        group_cols: List[str],
        active_filters: Optional[Dict[str, Any]] = None,
    ) -> DaxSkeleton:
        """Build skeleton for multiple filtered measures."""
        placeholders = []
        measure_lines = []
        notes = []

        for m in filtered_measures:
            res = m.get("_resolution", {})
            base = res.get("base_measure", "")
            fc = res.get("filter_column", "")
            fv = res.get("filter_value", "")
            label = m.get("name", base)

            if base and fc and fv:
                measure_lines.append(
                    f'    "{label}", CALCULATE([{base}], {fc} = "{fv}")'
                )
                notes.append(f"[{label}] = [{base}] WHERE {fc}=\"{fv}\"")
            else:
                measure_lines.append(f'    "{label}", [{m.get("name", "")}]')

        filter_lines = self._format_filter_lines(active_filters)
        measures_block = ",\n".join(measure_lines)

        skeleton = (
            f"EVALUATE\n"
            f"SUMMARIZECOLUMNS(\n"
            f"    {self._format_group_cols(group_cols, placeholders)}"
            f"{filter_lines},\n"
            f"{measures_block}\n"
            f")"
        )

        return DaxSkeleton(
            skeleton=skeleton,
            can_skip_llm=not placeholders,
            open_placeholders=placeholders,
            strategy_notes=notes,
        )

    def _build_multi_measure(
        self,
        measures: List[Dict],
        group_cols: List[str],
        active_filters: Optional[Dict[str, Any]] = None,
    ) -> DaxSkeleton:
        """Build skeleton for multiple model measures."""
        placeholders = []
        measure_lines = []

        for m in measures:
            name = m.get("name", "")
            flags = m.get("_resolution", {}).get("expression_flags", {})

            if flags.get("has_removefilters"):
                measure_lines.append(
                    f'    "{name}", /* REMOVEFILTERS — LLM: decompose */ [{name}]'
                )
                placeholders.append(f"DECOMPOSE_{name}")
            else:
                measure_lines.append(f'    "{name}", [{name}]')

        filter_lines = self._format_filter_lines(active_filters)
        measures_block = ",\n".join(measure_lines)

        skeleton = (
            f"EVALUATE\n"
            f"SUMMARIZECOLUMNS(\n"
            f"    {self._format_group_cols(group_cols, placeholders)}"
            f"{filter_lines},\n"
            f"{measures_block}\n"
            f")"
        )

        return DaxSkeleton(
            skeleton=skeleton,
            can_skip_llm=not placeholders,
            open_placeholders=placeholders,
            strategy_notes=[f"Multi-measure query: {len(measures)} measures"],
        )

    @staticmethod
    def _extract_group_columns(
        question_intent: Optional[Dict[str, Any]],
        dimension_bindings: Optional[List[Dict]] = None,
    ) -> List[str]:
        """Extract grouping column references from question intent.

        Prefers table-qualified references from dimension_bindings over raw dimension strings.
        """
        if not question_intent:
            return []
        dims = question_intent.get("dimensions", [])
        if not dims:
            return []

        if dimension_bindings:
            # Build lookup: user_term (lower) → 'Table'[Column]
            binding_map: Dict[str, str] = {}
            for b in dimension_bindings:
                user_term = b.get("user_term", "").lower()
                tbl = b.get("resolved_table", "")
                col = b.get("resolved_column", "")
                if user_term and tbl and col:
                    binding_map[user_term] = f"'{tbl}'[{col}]"

            qualified = []
            for dim in dims:
                key = dim.lower()
                if key in binding_map:
                    qualified.append(binding_map[key])
                else:
                    qualified.append(dim)
            return qualified

        return dims

    @staticmethod
    def _detect_date_table(tables: List[Dict]) -> Tuple[Optional[str], Optional[str]]:
        """Detect the date/calendar table and its primary date column.

        Priority 1: table name in _DATE_TABLE_NAMES (case-insensitive).
        Priority 2: table with ≥2 columns matching _DATE_COLUMN_HINTS.

        Returns (table_name, date_column) or (None, None).
        """
        # Priority 1: canonical name match
        for table in tables:
            name = table.get("name", "")
            if name.lower() in _DATE_TABLE_NAMES:
                cols = table.get("columns", [])
                # Find the best date column: prefer exact "Date" or "date", then first matching
                date_col = None
                for col in cols:
                    col_name = col if isinstance(col, str) else col.get("name", "")
                    if col_name.lower() == "date":
                        date_col = col_name
                        break
                if date_col is None and cols:
                    # Fall back to first column that hints at a date
                    for col in cols:
                        col_name = col if isinstance(col, str) else col.get("name", "")
                        if col_name.lower() in _DATE_COLUMN_HINTS:
                            date_col = col_name
                            break
                if date_col is None and cols:
                    date_col = cols[0] if isinstance(cols[0], str) else cols[0].get("name", "")
                if date_col:
                    return name, date_col

        # Priority 2: table with ≥2 date-hint columns
        best_table = None
        best_col = None
        best_hits = 0
        for table in tables:
            cols = table.get("columns", [])
            col_names = [
                (col if isinstance(col, str) else col.get("name", "")).lower()
                for col in cols
            ]
            hits = sum(1 for c in col_names if c in _DATE_COLUMN_HINTS)
            if hits >= 2 and hits > best_hits:
                best_hits = hits
                best_table = table.get("name", "")
                # Find best date column
                for col in cols:
                    col_name = col if isinstance(col, str) else col.get("name", "")
                    if col_name.lower() == "date":
                        best_col = col_name
                        break
                if best_col is None:
                    for col in cols:
                        col_name = col if isinstance(col, str) else col.get("name", "")
                        if col_name.lower() in _DATE_COLUMN_HINTS:
                            best_col = col_name
                            break

        return best_table, best_col

    @staticmethod
    def _build_date_filter(
        time_intelligence: Dict[str, Any],
        date_table: str,
        date_col: str,
    ) -> str:
        """Build a DAX VAR block comment for time intelligence date filtering.

        Returns a comment block that the LLM uses as structural guidance.
        The actual filter must be integrated into CALCULATE by the LLM.
        """
        if not time_intelligence or not date_table or not date_col:
            return ""

        tbl = f"'{date_table}'" if " " in date_table else date_table
        col_ref = f"{tbl}[{date_col}]"
        lines = [f"// TIME INTELLIGENCE — date table: {tbl}[{date_col}]"]

        has_ytd = time_intelligence.get("has_ytd", False)
        has_mtd = time_intelligence.get("has_mtd", False)
        has_qtd = time_intelligence.get("has_qtd", False)
        delta_periods = time_intelligence.get("delta_periods", [])
        grain = time_intelligence.get("grain", "")

        if has_ytd:
            lines.append(f"// YTD: use DATESYTD({col_ref})")
        elif has_mtd:
            lines.append(f"// MTD: use DATESMTD({col_ref})")
        elif has_qtd:
            lines.append(f"// QTD: use DATESQTD({col_ref})")
        elif "yoy" in (delta_periods or []):
            lines.append(f"// YoY: use SAMEPERIODLASTYEAR({col_ref})")
        elif "mom" in (delta_periods or []):
            lines.append(f"// MoM: use DATEADD({col_ref}, -1, MONTH)")
        elif grain:
            lines.append(
                f"// Latest {grain}: VAR _LatestDate = MAX({col_ref})"
                f" — FILTER(ALL({col_ref}), {col_ref} <= _LatestDate)"
            )
        else:
            return ""

        return "\n".join(lines)

    @staticmethod
    def _format_group_cols(
        group_cols: List[str], placeholders: List[str]
    ) -> str:
        """Format grouping columns for SUMMARIZECOLUMNS."""
        if not group_cols:
            placeholders.append("GROUPING_COLUMNS")
            return "/* LLM: add grouping columns here */"

        # Try to format as 'Table'[Column] if possible
        formatted = []
        for col in group_cols:
            if "[" in col:
                formatted.append(col)
            else:
                # Plain column name — needs table qualification
                formatted.append(f"/* '{col}' — LLM: qualify with table */")
                placeholders.append(f"QUALIFY_{col}")
        return ",\n    ".join(formatted)

    @staticmethod
    def _format_filter_lines(
        active_filters: Optional[Dict[str, Any]],
    ) -> str:
        """Format active filters as TREATAS lines."""
        if not active_filters:
            return ""

        lines = []
        for filter_key, filter_value in active_filters.items():
            if "[" not in filter_key:
                continue
            table = filter_key.split("[")[0]
            col = filter_key.split("[")[1].rstrip("]")

            if isinstance(filter_value, list):
                values = ", ".join(f'"{v}"' for v in filter_value)
                lines.append(
                    f'    TREATAS({{{values}}}, \'{table}\'[{col}])'
                )
            elif isinstance(filter_value, str):
                val = filter_value.strip()
                if val.upper() == "NOT NULL":
                    lines.append(
                        f'    FILTER(ALL(\'{table}\'[{col}]), NOT ISBLANK(\'{table}\'[{col}]))'
                    )
                else:
                    lines.append(
                        f'    TREATAS({{"{val}"}}, \'{table}\'[{col}])'
                    )

        if lines:
            return ",\n" + ",\n".join(lines)
        return ""
