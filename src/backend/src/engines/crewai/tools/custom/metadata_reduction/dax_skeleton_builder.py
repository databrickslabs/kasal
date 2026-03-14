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
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


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
    ) -> DaxSkeleton:
        """Build a DAX skeleton from resolved measures.

        Args:
            resolved_measures: List of measure dicts with '_resolution' metadata.
            relationships: Model relationships for join hints.
            active_filters: Active filters to apply.
            question_intent: Preprocessor output for shape/grouping hints.

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
            flags = resolution.get("expression_flags", {})

            if rtype == "model_measure":
                model_measures.append(m)
            elif rtype == "filtered_measure":
                filtered_measures.append(m)
            elif rtype == "composite_measure":
                composite_measures.append(m)
            else:
                other_measures.append(m)

        # Determine grouping columns from intent
        group_cols = self._extract_group_columns(question_intent)

        # Build skeleton based on what we have
        if len(resolved_measures) == 1:
            return self._build_single_measure(
                resolved_measures[0], group_cols, active_filters
            )

        if composite_measures:
            return self._build_composite(
                composite_measures[0], filtered_measures, group_cols, active_filters
            )

        if filtered_measures and not model_measures and not other_measures:
            return self._build_filtered_only(
                filtered_measures, group_cols, active_filters
            )

        # Multiple model measures or mixed types
        return self._build_multi_measure(
            resolved_measures, group_cols, active_filters
        )

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
    ) -> List[str]:
        """Extract grouping column references from question intent."""
        if not question_intent:
            return []
        dims = question_intent.get("dimensions", [])
        return dims

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
