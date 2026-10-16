"""Applying the report's default filters to generated DAX, so a measure is
evaluated in the same context the report shows it in.

Mixed into ``PowerBIAnalysisTool`` rather than composed, so this is pure
movement: every method still reads ``self`` exactly as it did in the single
3,506-line file, and every ``tool._method(...)`` call site is unchanged.
"""

import logging
from typing import Any, Dict

from src.services.tools.metadata_reduction.report_filters import filter_to_dax

logger = logging.getLogger(__name__)


class PowerBIDaxFilterMixin:
    def _auto_wrap_with_report_filters(
        self, dax_query: str, config: Dict[str, Any]
    ) -> str:
        """
        Automatically wrap the LLM-generated DAX with report-level filters.

        This ensures ALL report-level filters are applied, even if the LLM forgot to include them.
        This is more reliable than relying on the LLM to remember all filters.
        """
        active_filters = config.get("active_filters", {})

        if not active_filters:
            # No filters to apply, return original DAX
            return dax_query

        logger.info("[DAX Auto-Wrap] Wrapping DAX with report-level filters...")

        # Generate DAX filter conditions
        filter_conditions = []
        for filter_name, filter_description in active_filters.items():
            # Check if this filter is already in the DAX (avoid duplicates)
            if filter_name in dax_query:
                logger.info(
                    f"[DAX Auto-Wrap]   ⊘ Skipping {filter_name} (already in query)"
                )
                continue

            dax_condition = self._generate_dax_filter_condition(
                filter_name, filter_description
            )
            if dax_condition and not dax_condition.startswith("//"):
                filter_conditions.append(dax_condition)
                logger.info(f"[DAX Auto-Wrap]   + {filter_name}: {dax_condition}")

        if not filter_conditions:
            logger.info("[DAX Auto-Wrap] No additional filters to apply")
            return dax_query

        # Extract the inner part of the DAX (remove EVALUATE if present)
        inner_dax = dax_query.strip()
        if inner_dax.upper().startswith("EVALUATE"):
            inner_dax = inner_dax[8:].strip()  # Remove "EVALUATE"

        # Check if already wrapped in CALCULATETABLE
        if inner_dax.upper().startswith("CALCULATETABLE"):
            # Already has CALCULATETABLE - extract its contents and merge filters
            logger.info(
                "[DAX Auto-Wrap] DAX already uses CALCULATETABLE - merging filters"
            )

            # Find the opening and closing parentheses
            paren_count = 0
            start_idx = inner_dax.find("(")
            end_idx = -1

            for i, char in enumerate(inner_dax[start_idx:], start=start_idx):
                if char == "(":
                    paren_count += 1
                elif char == ")":
                    paren_count -= 1
                    if paren_count == 0:
                        end_idx = i
                        break

            if end_idx > start_idx:
                # Extract content inside CALCULATETABLE
                inner_content = inner_dax[start_idx + 1 : end_idx]

                # Add our filters to the existing CALCULATETABLE
                wrapped_dax = f"EVALUATE\nCALCULATETABLE(\n    {inner_content},\n"
                for condition in filter_conditions:
                    wrapped_dax += f"    {condition},\n"

                wrapped_dax = wrapped_dax.rstrip(",\n") + "\n)"
            else:
                # Couldn't parse - just wrap it
                wrapped_dax = f"EVALUATE\nCALCULATETABLE(\n    {inner_dax},\n"
                for condition in filter_conditions:
                    wrapped_dax += f"    {condition},\n"
                wrapped_dax = wrapped_dax.rstrip(",\n") + "\n)"

        else:
            # Not using CALCULATETABLE - wrap it
            wrapped_dax = f"EVALUATE\nCALCULATETABLE(\n    {inner_dax},\n"
            for condition in filter_conditions:
                wrapped_dax += f"    {condition},\n"
            wrapped_dax = wrapped_dax.rstrip(",\n") + "\n)"

        logger.info(
            "[DAX Auto-Wrap] ✅ Successfully wrapped DAX with report-level filters"
        )
        logger.info(f"[DAX Auto-Wrap] Wrapped DAX:\n{wrapped_dax}")

        return wrapped_dax

    def _generate_dax_filter_condition(
        self, filter_name: str, filter_value: object
    ) -> str:
        """The CALCULATETABLE filter for one stored filter value: a structured
        spec, a UI value or accepted legacy text (``report_filters``, shared
        with Tool 80). A ``//`` comment, which the caller skips, when the
        filter has no exact value: it is never applied as its description."""
        dax_filter = filter_to_dax(filter_name, filter_value)
        if dax_filter:
            return dax_filter
        logger.warning("[DAX Filter] Not applied (no exact value): %s", filter_name)
        return f"// No value to apply for filter {filter_name}"
