"""Building the enriched semantic context handed to the DAX-generating LLM.

Mixed into ``PowerBIAnalysisTool`` rather than composed, so this is pure
movement: every method still reads ``self`` exactly as it did in the single
3,506-line file, and every ``tool._method(...)`` call site is unchanged.
"""

import asyncio
import base64
import contextvars
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Any, Dict, List, Optional, Type

import httpx
from pydantic import BaseModel, Field, PrivateAttr

from src.services.tools.base import BaseTool
from src.services.tools.tool_session_provider import ToolSessionProvider

logger = logging.getLogger(__name__)


class PowerBISemanticContextMixin:
    def _build_enriched_semantic_context(
        self, model_context: Dict[str, Any], config: Dict[str, Any]
    ) -> str:
        """
        Build enriched semantic context for LLM prompt (Microsoft Copilot-style).

        This combines:
        1. Model schema (tables, columns, measures, relationships)
        2. Business mappings (natural language to DAX expressions)
        3. Field synonyms (alternative names for fields)
        4. Active filters (current view state)
        5. Conversation history (previous Q&A)
        6. Sample values (data patterns)
        """
        sections = []

        # ===== SEMANTIC MODEL SCHEMA =====
        sections.append("## 📊 SEMANTIC MODEL SCHEMA\n")

        # Tables with enhanced metadata
        tables = model_context.get("tables", [])
        measures = model_context.get("measures", [])
        visible_tables = config.get("visible_tables", [])

        # Build list of tables that have measures (these are critical to include)
        tables_with_measures = set()
        for measure in measures[:20]:  # Check measures we're showing
            table_name = measure.get("table")
            if table_name:
                tables_with_measures.add(table_name)

        # Smart table selection strategy:
        # 1. Include all tables that have measures (critical)
        # 2. Include visible tables if specified
        # 3. Fill remaining slots with other tables (up to 15 total)

        tables_to_show = []
        tables_seen = set()

        # Priority 1: Tables with measures (MUST include)
        for table in tables:
            if table["name"] in tables_with_measures:
                tables_to_show.append(table)
                tables_seen.add(table["name"])

        # Priority 2: Visible tables (if specified)
        if visible_tables:
            for table in tables:
                if table["name"] in visible_tables and table["name"] not in tables_seen:
                    tables_to_show.append(table)
                    tables_seen.add(table["name"])

        # Priority 3: Other tables (fill up to 15 total)
        remaining_slots = 15 - len(tables_to_show)
        for table in tables:
            if table["name"] not in tables_seen and remaining_slots > 0:
                tables_to_show.append(table)
                tables_seen.add(table["name"])
                remaining_slots -= 1

        logger.info(
            f"[Context Enrichment] Including {len(tables_to_show)} tables in context "
            f"({len(tables_with_measures)} with measures, {len(tables)} total available)"
        )

        for table in tables_to_show:
            table_name = table["name"]
            sections.append(f"### Table: **{table_name}**")

            # Columns with types
            columns = table.get("columns", [])
            if columns:
                column_types = table.get("column_types", {})
                column_list = []
                for col in columns[:15]:  # Limit columns shown
                    col_type = column_types.get(col, "")
                    if col_type:
                        column_list.append(f"{col} ({col_type})")
                    else:
                        column_list.append(col)
                sections.append(f"**Columns**: {', '.join(column_list)}")

            # Column descriptions if available
            column_descriptions = table.get("column_descriptions", {})
            if column_descriptions:
                sections.append("**Column Descriptions**:")
                for col, desc in list(column_descriptions.items())[:5]:
                    sections.append(f"  - {col}: {desc}")

            sections.append("")  # Blank line

        # Measures
        measures = model_context.get("measures", [])
        if measures:
            sections.append("### Available Measures")
            for measure in measures[:20]:  # Limit measures shown
                measure_name = measure["name"]
                measure_table = measure.get("table", "")
                measure_expr = measure.get("expression", "")[:100]
                sections.append(f"- **{measure_name}** (Table: {measure_table})")
                sections.append(f"  Expression: `{measure_expr}...`")
            sections.append("")

        # Relationships (filter to only show relationships for tables we're including)
        relationships = model_context.get("relationships", [])
        if relationships:
            # Filter relationships to only those involving tables in our context
            relevant_relationships = [
                rel
                for rel in relationships
                if rel["from_table"] in tables_seen or rel["to_table"] in tables_seen
            ]

            if relevant_relationships:
                sections.append("### Table Relationships")
                for rel in relevant_relationships:
                    sections.append(
                        f"- {rel['from_table']}[{rel['from_column']}] → {rel['to_table']}[{rel['to_column']}]"
                    )
                sections.append(
                    f"\n**Note**: Showing {len(relevant_relationships)} relationships for included tables"
                )
                sections.append("")

        # ===== BUSINESS TERMINOLOGY & SYNONYMS =====
        business_mappings = config.get("business_mappings", {})
        field_synonyms = config.get("field_synonyms", {})

        if business_mappings or field_synonyms:
            sections.append("## 🗣️ BUSINESS TERMINOLOGY & NATURAL LANGUAGE MAPPINGS\n")

            if business_mappings:
                sections.append("### Business Term Mappings")
                sections.append(
                    "Use these to translate natural language into DAX filter expressions:\n"
                )
                for term, expression in business_mappings.items():
                    sections.append(f'- **"{term}"** → `{expression}`')
                sections.append("")

            if field_synonyms:
                sections.append("### Field Synonyms")
                sections.append("These alternative names refer to the same fields:\n")
                for field, synonyms in field_synonyms.items():
                    sections.append(f"- **{field}**: {', '.join(synonyms)}")
                sections.append("")

        # ===== SAMPLE DATA VALUES =====
        sample_values = model_context.get("sample_values", {})
        if sample_values:
            sections.append("## 📝 SAMPLE DATA VALUES\n")
            sections.append("Example values to help understand the data:\n")
            for column, value_info in list(sample_values.items())[:10]:
                if value_info.get("type") == "categorical":
                    values = value_info.get("sample_values", [])
                    sections.append(
                        f"- **{column}**: {', '.join([str(v) for v in values[:5]])}"
                    )
            sections.append("")

        # ===== CURRENT VIEW STATE (Active Filters) =====
        active_filters = config.get("active_filters", {})
        if active_filters:
            sections.append("## 🎯 CURRENT VIEW STATE (AUTO-APPLY FILTERS)\n")
            sections.append(
                "**IMPORTANT**: The following filters are CURRENTLY ACTIVE and should be automatically applied to the query:\n"
            )
            for filter_name, filter_value in active_filters.items():
                if isinstance(filter_value, list):
                    quoted_values = ", ".join([f"'{v}'" for v in filter_value])
                    sections.append(f"- **{filter_name}** IN ({quoted_values})")
                else:
                    sections.append(f"- **{filter_name}** = {filter_value}")
            sections.append(
                "\n**Note**: User questions may not explicitly mention these filters, but they should still be applied!\n"
            )

        # ===== CONVERSATION HISTORY =====
        conversation_history = config.get("conversation_history", [])
        if conversation_history:
            sections.append("## 💬 RECENT CONVERSATION HISTORY\n")
            sections.append("Previous questions in this session (for context):\n")
            for i, turn in enumerate(conversation_history[-3:], 1):  # Last 3 turns
                sections.append(f"**Q{i}**: {turn.get('question', '')}")
                if turn.get("filters_used"):
                    sections.append(f"  Filters used: {turn['filters_used']}")
                if turn.get("answer"):
                    sections.append(f"  Answer: {turn['answer']}")
            sections.append("")

        return "\n".join(sections)
