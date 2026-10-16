"""TMDL parsing: measures, tables and the report's default filters.

Pure text/JSON parsing — no network, no LLM.

Mixed into ``PowerBIAnalysisTool`` rather than composed, so this is pure
movement: every method still reads ``self`` exactly as it did in the single
3,506-line file, and every ``tool._method(...)`` call site is unchanged.
"""

import asyncio
import base64
import json
import logging
import re
from typing import Any, Dict, List

import httpx

from src.services.tools.metadata_reduction.report_filters import (
    describe_filter,
    filter_from_definition,
    spec_from_pbi_condition,
)

logger = logging.getLogger(__name__)


class PowerBITmdlParsingMixin:
    def _parse_tmdl_for_measures_and_tables(
        self, tmdl_parts: List[Dict[str, Any]], config: Dict[str, Any]
    ) -> tuple:
        """Parse TMDL parts to extract measures and tables."""
        measures = []
        tables = []

        for part in tmdl_parts:
            path = part.get("path", "")
            payload = part.get("payload", "")

            if not path.startswith("definition/tables/") or not path.endswith(".tmdl"):
                continue

            try:
                tmdl_content = base64.b64decode(payload).decode("utf-8")

                # Extract table name
                table_match = re.match(
                    r"table\s+(?:'([^']+)'|(\w+))", tmdl_content.strip()
                )
                if not table_match:
                    continue
                table_name = table_match.group(1) or table_match.group(2)

                # Skip system tables
                if config.get("skip_system_tables", True):
                    if (
                        "LocalDateTable" in table_name
                        or "DateTableTemplate" in table_name
                    ):
                        continue

                # Add table
                tables.append({"name": table_name})

                # Extract columns
                column_pattern = re.compile(
                    r"column\s+(?:'([^']+)'|(\w+))", re.MULTILINE
                )
                columns = []
                for col_match in column_pattern.finditer(tmdl_content):
                    col_name = col_match.group(1) or col_match.group(2)
                    columns.append(col_name)

                if columns:
                    tables[-1]["columns"] = columns

                # Extract measures
                measure_pattern = re.compile(
                    r"measure\s+(?:'([^']+)'|(\w+))\s*=\s*([\s\S]*?)(?=\n\s*measure|\n\s*column|\n\t[^\t]|\Z)",
                    re.MULTILINE,
                )

                for match in measure_pattern.finditer(tmdl_content):
                    measure_name = match.group(1) or match.group(2)
                    expression = match.group(3).strip()

                    # Clean expression (remove metadata lines)
                    clean_lines = []
                    for line in expression.split("\n"):
                        stripped = line.strip()
                        if stripped.startswith(
                            ("lineageTag:", "formatString:", "annotation", "isHidden")
                        ):
                            break
                        clean_lines.append(line)

                    measures.append(
                        {
                            "name": measure_name,
                            "table": table_name,
                            "expression": "\n".join(clean_lines).strip(),
                        }
                    )

            except Exception as e:
                logger.warning(f"Error parsing TMDL from {path}: {e}")

        logger.info(f"Parsed {len(measures)} measure(s), {len(tables)} table(s)")
        return measures, tables

    async def _extract_default_filters(
        self, workspace_id: str, report_id: str, access_token: str
    ) -> Dict[str, Any]:
        """
        Extract default filters from Power BI report definition.

        Uses Fabric API to get report definition in PBIR format, which includes:
        - Report-level filters (filters on all pages)
        - Page-level filters
        - Visual-level filters

        Returns:
            Dict mapping filter names to their values/expressions
        """
        logger.info(
            f"[Filter Extraction] Extracting default filters from report {report_id} via Fabric API"
        )

        filters = {}

        try:
            # Get report definition via Fabric API (PBIR format for reports, not TMDL)
            # TMDL is for semantic models, reports use PBIR (Power BI Report) format
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/reports/{report_id}/getDefinition"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }

            async with httpx.AsyncClient() as client:
                logger.info("[Filter Extraction] Fetching report TMDL definition...")
                logger.info(f"[Filter Extraction] 🔍 DEBUG: API URL: {url}")
                response = await client.post(
                    url, headers=headers, json={}, timeout=60.0
                )
                logger.info(
                    f"[Filter Extraction] 🔍 DEBUG: Got response status: {response.status_code}"
                )

                if response.status_code == 202:
                    logger.info(
                        "[Filter Extraction] 🔍 DEBUG: Using async polling flow (202)"
                    )
                    # Long-running operation - poll for completion
                    location = response.headers.get("Location")
                    logger.info(
                        f"[Filter Extraction] 🔍 DEBUG: Location header: {location}"
                    )
                    if location:
                        for poll_attempt in range(10):  # Poll up to 10 times
                            await asyncio.sleep(2)
                            logger.info(
                                f"[Filter Extraction] 🔍 DEBUG: Polling attempt {poll_attempt + 1}/10"
                            )
                            poll_response = await client.get(location, headers=headers)
                            poll_data = poll_response.json()
                            logger.info(
                                f"[Filter Extraction] 🔍 DEBUG: Poll status: {poll_data.get('status')}"
                            )

                            if poll_data.get("status") == "Succeeded":
                                logger.info(
                                    "[Filter Extraction] 🔍 DEBUG: Operation succeeded, fetching result"
                                )
                                result_url = location + "/result"
                                result_response = await client.get(
                                    result_url, headers=headers
                                )
                                result_response.raise_for_status()
                                result_json = result_response.json()
                                logger.info(
                                    f"[Filter Extraction] 🔍 DEBUG: Result JSON keys: {list(result_json.keys())}"
                                )
                                report_tmdl_parts = result_json.get(
                                    "definition", {}
                                ).get("parts", [])
                                logger.info(
                                    f"[Filter Extraction] 🔍 DEBUG: Got {len(report_tmdl_parts)} TMDL parts"
                                )
                                filters = self._parse_tmdl_for_filters(
                                    report_tmdl_parts
                                )
                                break
                            elif poll_data.get("status") == "Failed":
                                logger.error(
                                    f"[Filter Extraction] Report TMDL fetch failed: {poll_data}"
                                )
                                break
                    else:
                        logger.warning(
                            "[Filter Extraction] ⚠️ No Location header in 202 response"
                        )

                elif response.status_code == 200:
                    logger.info(
                        "[Filter Extraction] 🔍 DEBUG: Using direct response flow (200)"
                    )
                    # Direct response
                    response_json = response.json()
                    logger.info(
                        f"[Filter Extraction] 🔍 DEBUG: Response JSON keys: {list(response_json.keys())}"
                    )
                    report_tmdl_parts = response_json.get("definition", {}).get(
                        "parts", []
                    )
                    logger.info(
                        f"[Filter Extraction] 🔍 DEBUG: Got {len(report_tmdl_parts)} TMDL parts"
                    )
                    filters = self._parse_tmdl_for_filters(report_tmdl_parts)
                else:
                    logger.warning(
                        f"[Filter Extraction] ⚠️ Unexpected status code: {response.status_code}"
                    )
                    logger.warning(
                        f"[Filter Extraction] 🔍 DEBUG: Response body: {response.text[:500]}"
                    )

                if filters:
                    logger.info(
                        f"[Filter Extraction] ✅ Successfully extracted {len(filters)} report-level filters"
                    )
                    for filter_name, filter_desc in filters.items():
                        logger.info(
                            f"[Filter Extraction]   • {filter_name}: {filter_desc}"
                        )
                else:
                    logger.info("[Filter Extraction] No report-level filters found")

        except httpx.HTTPStatusError as e:
            logger.warning(
                f"[Filter Extraction] HTTP error getting report TMDL: {e.response.status_code}"
            )
        except Exception as e:
            logger.warning(
                f"[Filter Extraction] Error extracting filters from TMDL: {e}"
            )

        return filters

    def _parse_tmdl_for_filters(
        self, tmdl_parts: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Parse report definition (PBIR format) to extract filters.

        PBIR format stores filters in report.json as a JSON string.
        Structure example:
            {
              "filters": "[{\"$schema\":\"...\", \"target\":{...}, \"filter\":{...}}]",
              "config": "...",
              "layoutOptimization": 0
            }
        """
        filters = {}

        try:
            logger.info(
                f"[Filter Extraction] 🔍 DEBUG: Received {len(tmdl_parts)} definition parts to parse"
            )

            for idx, part in enumerate(tmdl_parts):
                payload = part.get("payload", "")
                path = part.get("path", "")

                logger.info(
                    f"[Filter Extraction] 🔍 DEBUG: Part {idx+1}/{len(tmdl_parts)}"
                )
                logger.info(f"[Filter Extraction] 🔍 DEBUG:   - Path: {path}")
                logger.info(
                    f"[Filter Extraction] 🔍 DEBUG:   - Payload size: {len(payload)} chars (base64)"
                )

                # Look for report.json file (contains filter definitions)
                if "report.json" in path.lower():
                    logger.info(
                        f"[Filter Extraction] ✅ Found report JSON file: {path}"
                    )

                    try:
                        # Decode base64 payload
                        import base64

                        content = base64.b64decode(payload).decode("utf-8")
                        logger.info(
                            f"[Filter Extraction] 🔍 DEBUG: Decoded content size: {len(content)} chars"
                        )
                        logger.info(
                            f"[Filter Extraction] 🔍 DEBUG: Content preview: {content[:300]}..."
                        )

                        # Parse JSON content
                        report_json = json.loads(content)
                        logger.info(
                            f"[Filter Extraction] Report JSON keys: {list(report_json.keys())}"
                        )

                        # Look for filters field
                        if "filters" in report_json:
                            filters_str = report_json["filters"]
                            logger.info(
                                "[Filter Extraction] Found 'filters' field, parsing..."
                            )

                            # Parse filters JSON string
                            if isinstance(filters_str, str):
                                filter_definitions = json.loads(filters_str)
                            else:
                                filter_definitions = filters_str

                            logger.info(
                                f"[Filter Extraction] Parsing {len(filter_definitions)} filter definitions..."
                            )

                            # Extract filter values
                            for filter_def in filter_definitions:
                                filter_name, filter_description = (
                                    self._extract_filter_from_definition(filter_def)
                                )
                                if filter_name and filter_description:
                                    filters[filter_name] = filter_description

                        else:
                            logger.info(
                                "[Filter Extraction] ⚠️ No 'filters' field found in report JSON"
                            )

                    except json.JSONDecodeError as e:
                        logger.warning(f"[Filter Extraction] Failed to parse JSON: {e}")
                        # Log the content for debugging
                        logger.info(
                            f"[Filter Extraction] 🔍 DEBUG: Full content:\n{content[:2000]}"
                        )

        except Exception as e:
            logger.warning(f"[Filter Extraction] Error parsing report for filters: {e}")
            import traceback

            logger.warning(f"[Filter Extraction] Traceback: {traceback.format_exc()}")

        return filters

    def _extract_filter_from_definition(self, filter_def: Dict[str, Any]) -> tuple:
        """``(Table[Column], spec)`` from a Power BI filter definition.

        The spec is structured (operator + typed values, shared with Tool 79
        through ``report_filters``). A filter that cannot be applied exactly (a
        card with nothing selected, a relative date, an advanced filter) is
        ``{"unapplied": reason}``: never applied as its description, and
        listed in the answer as a report filter that was not applied.
        """
        try:
            name, spec = filter_from_definition(filter_def)
        except (AttributeError, TypeError, ValueError):
            return (None, None)
        if spec and "unapplied" in spec:
            reason = spec["unapplied"]
            logger.warning("[Filter Extraction] Not applied: %s (%s)", name, reason)
        return (name, spec)

    def _parse_filter_condition(self, condition: Dict[str, Any]) -> str:
        """Human-readable text of a Power BI filter condition (for logs)."""
        spec = spec_from_pbi_condition(condition)
        return describe_filter(spec) if spec else "has complex filter"
