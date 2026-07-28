"""Applying the report's default filters to generated DAX, so a measure is
evaluated in the same context the report shows it in.

Mixed into ``PowerBIAnalysisTool`` rather than composed, so this is pure
movement: every method still reads ``self`` exactly as it did in the single
3,506-line file, and every ``tool._method(...)`` call site is unchanged.
"""

import asyncio
import base64
import contextvars
import logging
import json
import re
from typing import Any, Optional, Type, Dict, List
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from src.services.tools.base import BaseTool
from pydantic import BaseModel, Field, PrivateAttr
import httpx

from src.services.tools.tool_session_provider import ToolSessionProvider


logger = logging.getLogger(__name__)


class PowerBIDaxFilterMixin:
    def _auto_wrap_with_report_filters(self, dax_query: str, config: Dict[str, Any]) -> str:
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
                logger.info(f"[DAX Auto-Wrap]   ⊘ Skipping {filter_name} (already in query)")
                continue

            dax_condition = self._generate_dax_filter_condition(filter_name, filter_description)
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
            logger.info("[DAX Auto-Wrap] DAX already uses CALCULATETABLE - merging filters")

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
                inner_content = inner_dax[start_idx + 1:end_idx]

                # Add our filters to the existing CALCULATETABLE
                wrapped_dax = f"EVALUATE\nCALCULATETABLE(\n    {inner_content},\n"
                for condition in filter_conditions:
                    wrapped_dax += f"    {condition},\n"

                wrapped_dax = wrapped_dax.rstrip(',\n') + "\n)"
            else:
                # Couldn't parse - just wrap it
                wrapped_dax = f"EVALUATE\nCALCULATETABLE(\n    {inner_dax},\n"
                for condition in filter_conditions:
                    wrapped_dax += f"    {condition},\n"
                wrapped_dax = wrapped_dax.rstrip(',\n') + "\n)"

        else:
            # Not using CALCULATETABLE - wrap it
            wrapped_dax = f"EVALUATE\nCALCULATETABLE(\n    {inner_dax},\n"
            for condition in filter_conditions:
                wrapped_dax += f"    {condition},\n"
            wrapped_dax = wrapped_dax.rstrip(',\n') + "\n)"

        logger.info("[DAX Auto-Wrap] ✅ Successfully wrapped DAX with report-level filters")
        logger.info(f"[DAX Auto-Wrap] Wrapped DAX:\n{wrapped_dax}")

        return wrapped_dax

    def _generate_dax_filter_condition(self, filter_name: str, filter_description: str) -> str:
        """
        Generate a DAX filter condition from a filter name and description.

        Handles:
        - NOT NULL → ISBLANK(column) = FALSE
        - NOT STARTS WITH 'X' → FILTER(VALUES(column), NOT(LEFT(column, LEN("X")) = "X"))
        - = 'Value' → column = "Value"
        - IN (val1, val2) → column IN {"val1", "val2"}
        """
        try:
            # Parse the filter description
            filter_desc = str(filter_description).strip()

            # Handle NOT NULL
            if filter_desc == "NOT NULL":
                return f"ISBLANK({filter_name}) = FALSE"

            # Handle NOT STARTS WITH
            if filter_desc.startswith("NOT STARTS WITH"):
                # Extract the value
                value = filter_desc.replace("NOT STARTS WITH", "").strip().strip("'\"")
                # Use LEFT() instead of STARTSWITH() - STARTSWITH not supported by Power BI API
                prefix_length = len(value)
                return f'FILTER(VALUES({filter_name}), NOT(LEFT({filter_name}, {prefix_length}) = "{value}"))'

            # Handle equals
            if filter_desc.startswith("= "):
                value = filter_desc[2:].strip().strip("'\"")
                return f'{filter_name} = "{value}"'

            # Handle IN (multiple values)
            if filter_desc.startswith("IN ("):
                # Extract values from "IN (val1, val2, val3)"
                values_str = filter_desc[4:-1]  # Remove "IN (" and ")"
                values = [v.strip().strip("'\"") for v in values_str.split(",")]
                values_list = ', '.join([f'"{v}"' for v in values])
                return f'{filter_name} IN {{{values_list}}}'

            # Handle plain value (treat as equals)
            if not filter_desc.startswith("NOT") and not filter_desc.startswith("IN"):
                # Might be just a value
                value = filter_desc.strip("'\"")
                return f'{filter_name} = "{value}"'

            # Fallback - return as comment
            logger.warning(f"[DAX Filter] Could not generate condition for: {filter_name} {filter_desc}")
            return f'// TODO: Apply filter {filter_name} {filter_desc}'

        except Exception as e:
            logger.warning(f"[DAX Filter] Error generating condition for {filter_name}: {e}")
            return f'// Error: Could not apply filter {filter_name}'
