"""Rendering the analysis result for the agent.

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

from kasal_engine.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr
import httpx

from src.services.tools.tool_session_provider import ToolSessionProvider



class PowerBIOutputMixin:
    def _format_output(self, results: Dict[str, Any], output_format: str) -> str:
        """Format the results for output."""
        if output_format == "json":
            return json.dumps(results, indent=2, default=str)

        # Markdown format
        output = []

        output.append("# Power BI Analysis Results\n")
        output.append(f"**Question**: {results['user_question']}")
        output.append(f"**Workspace**: `{results['workspace_id']}`")
        output.append(f"**Dataset**: `{results['dataset_id']}`\n")

        # Errors
        if results.get("errors"):
            output.append("## ⚠️ Errors\n")
            for error in results["errors"]:
                output.append(f"- {error}")
            output.append("")

        # Model Context Summary
        ctx = results.get("model_context", {})
        output.append("## Model Context\n")
        output.append(f"- **Measures**: {len(ctx.get('measures', []))}")
        output.append(f"- **Tables**: {len(ctx.get('tables', []))}")
        output.append(f"- **Relationships**: {len(ctx.get('relationships', []))}\n")

        # Generated DAX
        if results.get("generated_dax"):
            output.append("## Generated DAX Query\n")

            # Show retry attempts if there were multiple
            dax_attempts = results.get("dax_attempts", [])
            if len(dax_attempts) > 1:
                output.append(f"**Attempts**: {len(dax_attempts)} (successful on attempt {len(dax_attempts)})\n")
                output.append("\n### Retry History\n")
                for att in dax_attempts[:-1]:  # Show all failed attempts
                    output.append(f"**Attempt {att['attempt']}**: ❌ Failed")
                    if att.get('error'):
                        output.append(f"  - Error: {att['error'][:100]}...")
                output.append(f"**Attempt {dax_attempts[-1]['attempt']}**: ✅ Success\n")

            output.append("```dax")
            output.append(results["generated_dax"])
            output.append("```\n")

        # Execution Results
        exec_result = results.get("dax_execution", {})
        output.append("## Execution Results\n")

        if exec_result.get("success"):
            output.append(f"✅ **Success** - {exec_result.get('row_count', 0)} rows returned\n")

            # Show data as table
            data = exec_result.get("data", [])
            if data:
                columns = exec_result.get("columns", list(data[0].keys()) if data else [])

                # Table header
                output.append("| " + " | ".join(str(c).replace("[", "").replace("]", "") for c in columns) + " |")
                output.append("| " + " | ".join(["---"] * len(columns)) + " |")

                # Table rows (limit to 20)
                for row in data[:20]:
                    values = [str(row.get(c, ""))[:50] for c in columns]
                    output.append("| " + " | ".join(values) + " |")

                if len(data) > 20:
                    output.append(f"\n*... and {len(data) - 20} more rows*")
        else:
            output.append(f"❌ **Failed**: {exec_result.get('error', 'Unknown error')}")
        output.append("")

        # Visual References
        if results.get("visual_references"):
            output.append("## Visual References\n")
            output.append("Reports and pages using the queried measures:\n")

            # Group by report, then by page
            report_refs = {}
            for ref in results["visual_references"]:
                report_name = ref.get("report_name", "Unknown")
                if report_name not in report_refs:
                    report_refs[report_name] = {
                        "report_url": ref.get("report_url", ""),
                        "pages": {}
                    }

                page_name = ref.get("page_name")
                page_url = ref.get("page_url")
                measure = ref.get("measure", "Unknown")
                visual_type = ref.get("visual_type")

                if page_name:
                    if page_name not in report_refs[report_name]["pages"]:
                        report_refs[report_name]["pages"][page_name] = {
                            "page_url": page_url,
                            "measures": [],
                            "visual_types": set()
                        }
                    report_refs[report_name]["pages"][page_name]["measures"].append(measure)
                    if visual_type:
                        report_refs[report_name]["pages"][page_name]["visual_types"].add(visual_type)
                else:
                    # No page info - store at report level
                    if "_no_page_" not in report_refs[report_name]["pages"]:
                        report_refs[report_name]["pages"]["_no_page_"] = {
                            "page_url": None,
                            "measures": [],
                            "visual_types": set()
                        }
                    report_refs[report_name]["pages"]["_no_page_"]["measures"].append(measure)

            # Format output
            for report_name, report_data in report_refs.items():
                report_url = report_data["report_url"]
                output.append(f"\n### 📊 {report_name}")
                output.append(f"[Open Report]({report_url})\n")

                for page_name, page_data in report_data["pages"].items():
                    if page_name == "_no_page_":
                        # Measures without page-level detail
                        unique_measures = list(set(page_data["measures"]))
                        output.append(f"- Measures in report: {', '.join(unique_measures)}")
                    else:
                        page_url = page_data["page_url"]
                        unique_measures = list(set(page_data["measures"]))
                        visual_types = list(page_data["visual_types"])

                        output.append(f"- **📄 {page_name}**: [Open Page]({page_url})")
                        output.append(f"  - Measures: {', '.join(unique_measures)}")
                        if visual_types:
                            output.append(f"  - Visual types: {', '.join(visual_types)}")

        return "\n".join(output)
