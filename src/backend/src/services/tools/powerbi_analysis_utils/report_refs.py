"""Resolving which report pages and visuals reference a measure, so an answer
can link back to where the number is shown.

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


logger = logging.getLogger(__name__)


class PowerBIReportReferenceMixin:
    async def _find_visual_references(
        self,
        workspace_id: str,
        dataset_id: str,
        access_token: str,
        measures: List[str]
    ) -> List[Dict[str, Any]]:
        """Find reports, pages, and visuals that use the specified measures."""
        visual_refs = []

        # Get reports using this dataset
        reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                reports_response = await client.get(reports_url, headers=headers)
                reports_response.raise_for_status()
                reports_data = reports_response.json()

                # Filter reports using our dataset
                reports = [
                    r for r in reports_data.get("value", [])
                    if r.get("datasetId") == dataset_id
                ]

                for report in reports[:5]:  # Limit to 5 reports
                    report_id = report.get("id")
                    report_name = report.get("name")
                    report_url = report.get("webUrl", "")

                    # Try to fetch detailed page/visual info from report definition
                    try:
                        page_refs = await self._get_measure_page_references(
                            workspace_id, report_id, report_name, report_url,
                            measures, access_token, client
                        )
                        if page_refs:
                            visual_refs.extend(page_refs)
                        else:
                            # Fallback to report-level reference if page parsing fails
                            for measure in measures:
                                visual_refs.append({
                                    "report_name": report_name,
                                    "report_url": report_url,
                                    "page_name": None,
                                    "page_url": None,
                                    "measure": measure,
                                    "visual_type": None,
                                    "note": "Report uses the same dataset - measure likely present"
                                })
                    except Exception as e:
                        logger.warning(f"Could not get page details for report {report_name}: {e}")
                        # Fallback to report-level reference
                        for measure in measures:
                            visual_refs.append({
                                "report_name": report_name,
                                "report_url": report_url,
                                "page_name": None,
                                "page_url": None,
                                "measure": measure,
                                "visual_type": None,
                                "note": "Report uses the same dataset - measure likely present"
                            })

            except Exception as e:
                logger.error(f"Visual reference search error: {e}")

        return visual_refs

    async def _get_measure_page_references(
        self,
        workspace_id: str,
        report_id: str,
        report_name: str,
        report_url: str,
        measures: List[str],
        access_token: str,
        client: httpx.AsyncClient
    ) -> List[Dict[str, Any]]:
        """
        Get page-level references for measures by parsing the report definition.
        Returns list of references with page names and URLs where measures are used.
        """
        refs = []

        # Fetch report definition from Fabric API
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/reports/{report_id}/getDefinition"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        try:
            response = await client.post(url, headers=headers, timeout=60.0)

            if response.status_code == 202:
                # Long-running operation - poll for completion
                location = response.headers.get("Location")
                if not location:
                    return []

                for _ in range(30):  # Max 60 seconds
                    await asyncio.sleep(2)
                    poll_response = await client.get(location, headers=headers)
                    poll_data = poll_response.json()

                    if poll_data.get("status") == "Succeeded":
                        result_url = location + "/result"
                        result_response = await client.get(result_url, headers=headers)
                        result_response.raise_for_status()
                        report_parts = result_response.json().get("definition", {}).get("parts", [])
                        break
                    elif poll_data.get("status") == "Failed":
                        return []
                else:
                    return []  # Timeout

            elif response.status_code == 200:
                report_parts = response.json().get("definition", {}).get("parts", [])
            else:
                return []

            if not report_parts:
                return []

            # Parse pages and visuals from report definition
            pages = self._parse_report_pages(report_parts)
            visuals = self._parse_report_visuals(report_parts)

            # Build page lookup
            page_lookup = {p.get("id"): p for p in pages}

            # Find which measures are used in which visuals/pages
            measure_locations = {}  # measure_name -> list of (page_id, visual_type)

            for visual in visuals:
                visual_measures = self._extract_measures_from_visual(visual)
                page_id = visual.get("page_id")
                visual_type = visual.get("type", "unknown")

                for measure in measures:
                    if measure in visual_measures:
                        if measure not in measure_locations:
                            measure_locations[measure] = []
                        measure_locations[measure].append({
                            "page_id": page_id,
                            "visual_type": visual_type
                        })

            # Build references with page info
            for measure in measures:
                if measure in measure_locations:
                    # Measure found in specific pages/visuals
                    seen_pages = set()
                    for loc in measure_locations[measure]:
                        page_id = loc["page_id"]
                        if page_id in seen_pages:
                            continue
                        seen_pages.add(page_id)

                        page_info = page_lookup.get(page_id, {})
                        page_name = page_info.get("displayName") or page_info.get("name") or page_id
                        page_url = self._build_page_url(workspace_id, report_id, page_id)

                        refs.append({
                            "report_name": report_name,
                            "report_url": report_url,
                            "page_name": page_name,
                            "page_url": page_url,
                            "measure": measure,
                            "visual_type": loc["visual_type"],
                            "note": f"Measure found in visual on page '{page_name}'"
                        })
                else:
                    # Measure not found in visuals - add report-level reference
                    refs.append({
                        "report_name": report_name,
                        "report_url": report_url,
                        "page_name": None,
                        "page_url": None,
                        "measure": measure,
                        "visual_type": None,
                        "note": "Measure in dataset but not detected in report visuals"
                    })

            return refs

        except Exception as e:
            logger.warning(f"Error fetching report definition: {e}")
            return []

    def _build_page_url(self, workspace_id: str, report_id: str, page_id: str) -> str:
        """Build the Power BI report page URL."""
        if page_id:
            return f"https://app.powerbi.com/groups/{workspace_id}/reports/{report_id}/ReportSection{page_id}"
        return f"https://app.powerbi.com/groups/{workspace_id}/reports/{report_id}"

    def _parse_report_pages(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse page definitions from PBIR report structure."""
        pages = []

        for part in report_parts:
            path = part.get("path", "")
            path_lower = path.lower()

            # Look for page.json files
            is_page_file = (
                ("/pages/" in path_lower and path_lower.endswith("/page.json")) or
                (path_lower.endswith("/page.json"))
            )

            if is_page_file:
                try:
                    payload = part.get("payload", "")
                    content = base64.b64decode(payload).decode("utf-8")
                    page_data = json.loads(content)

                    # Extract page ID from path
                    path_parts = path.split("/")
                    page_id = None

                    if "pages" in path_parts:
                        idx = path_parts.index("pages") + 1
                        page_id = path_parts[idx] if idx < len(path_parts) else None
                    elif "Pages" in path_parts:
                        idx = path_parts.index("Pages") + 1
                        page_id = path_parts[idx] if idx < len(path_parts) else None
                    else:
                        page_id = path_parts[-2] if len(path_parts) >= 2 else "unknown"

                    pages.append({
                        "id": page_id,
                        "name": page_data.get("name", page_id),
                        "displayName": page_data.get("displayName", page_data.get("name", page_id)),
                        "ordinal": page_data.get("ordinal", 0),
                    })
                except Exception as e:
                    logger.warning(f"Error parsing page from {path}: {e}")

        # Try embedded format if no pages found
        if not pages:
            pages = self._parse_pages_from_report_json(report_parts)

        pages.sort(key=lambda p: p.get("ordinal", 0))
        return pages

    def _parse_pages_from_report_json(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse pages from report.json (embedded format)."""
        pages = []

        for part in report_parts:
            path = part.get("path", "")
            if path.lower() == "report.json" or path.lower().endswith("/report.json"):
                try:
                    payload = part.get("payload", "")
                    content = base64.b64decode(payload).decode("utf-8")
                    report_data = json.loads(content)

                    pages_data = (
                        report_data.get("pages") or
                        report_data.get("sections") or
                        report_data.get("reportPages")
                    )

                    if pages_data and isinstance(pages_data, list):
                        for idx, page_data in enumerate(pages_data):
                            if isinstance(page_data, dict):
                                page_id = page_data.get("name") or page_data.get("id") or f"page_{idx}"
                                pages.append({
                                    "id": page_id,
                                    "name": page_data.get("name", page_id),
                                    "displayName": page_data.get("displayName", page_data.get("name", page_id)),
                                    "ordinal": page_data.get("ordinal", idx),
                                })
                except Exception as e:
                    logger.warning(f"Error parsing report.json for pages: {e}")

        return pages

    def _parse_report_visuals(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse visual definitions from PBIR report structure."""
        visuals = []

        for part in report_parts:
            path = part.get("path", "")
            path_lower = path.lower()

            # Look for visual.json files
            is_visual_file = (
                ("/visuals/" in path_lower and path_lower.endswith("/visual.json")) or
                ("/visuals/" in path_lower and path_lower.endswith(".json"))
            )

            if is_visual_file:
                try:
                    payload = part.get("payload", "")
                    content = base64.b64decode(payload).decode("utf-8")
                    visual_data = json.loads(content)

                    # Extract page ID and visual ID from path
                    path_parts = path.split("/")

                    page_id = None
                    if "pages" in path_parts:
                        idx = path_parts.index("pages") + 1
                        page_id = path_parts[idx] if idx < len(path_parts) else None
                    elif "Pages" in path_parts:
                        idx = path_parts.index("Pages") + 1
                        page_id = path_parts[idx] if idx < len(path_parts) else None

                    visual_id = None
                    if "visuals" in path_parts:
                        idx = path_parts.index("visuals") + 1
                        visual_id = path_parts[idx] if idx < len(path_parts) else None
                    elif "Visuals" in path_parts:
                        idx = path_parts.index("Visuals") + 1
                        visual_id = path_parts[idx] if idx < len(path_parts) else None
                    else:
                        visual_id = path_parts[-2] if len(path_parts) >= 2 else "unknown"

                    visuals.append({
                        "id": visual_id,
                        "page_id": page_id,
                        "type": visual_data.get("visual", {}).get("visualType", "unknown"),
                        "config": visual_data
                    })
                except Exception as e:
                    logger.warning(f"Error parsing visual from {path}: {e}")

        # Try embedded format if no visuals found
        if not visuals:
            visuals = self._parse_visuals_from_report_json(report_parts)

        return visuals

    def _parse_visuals_from_report_json(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse visuals from report.json (embedded format)."""
        visuals = []

        for part in report_parts:
            path = part.get("path", "")
            if path.lower() == "report.json" or path.lower().endswith("/report.json"):
                try:
                    payload = part.get("payload", "")
                    content = base64.b64decode(payload).decode("utf-8")
                    report_data = json.loads(content)

                    pages_data = (
                        report_data.get("pages") or
                        report_data.get("sections") or
                        report_data.get("reportPages")
                    )

                    if pages_data and isinstance(pages_data, list):
                        for page_data in pages_data:
                            if not isinstance(page_data, dict):
                                continue

                            page_id = page_data.get("name") or page_data.get("id")
                            visuals_data = (
                                page_data.get("visualContainers") or
                                page_data.get("visuals")
                            )

                            if visuals_data and isinstance(visuals_data, list):
                                for vis_idx, vis_data in enumerate(visuals_data):
                                    if not isinstance(vis_data, dict):
                                        continue

                                    visual_id = vis_data.get("name") or vis_data.get("id") or f"visual_{vis_idx}"
                                    visual_type = "unknown"
                                    parsed_config = {}

                                    if "config" in vis_data:
                                        config_str = vis_data.get("config", "")
                                        if isinstance(config_str, str):
                                            try:
                                                parsed_config = json.loads(config_str)
                                                visual_type = parsed_config.get("singleVisual", {}).get("visualType", "unknown")
                                            except json.JSONDecodeError:
                                                pass
                                        elif isinstance(config_str, dict):
                                            parsed_config = config_str
                                            visual_type = parsed_config.get("singleVisual", {}).get("visualType", "unknown")
                                    elif "visualType" in vis_data:
                                        visual_type = vis_data.get("visualType", "unknown")
                                        parsed_config = vis_data

                                    visuals.append({
                                        "id": visual_id,
                                        "page_id": page_id,
                                        "type": visual_type,
                                        "config": parsed_config
                                    })
                except Exception as e:
                    logger.warning(f"Error parsing report.json for visuals: {e}")

        return visuals

    def _extract_measures_from_visual(self, visual: Dict[str, Any]) -> List[str]:
        """Extract measure names referenced in a visual configuration."""
        measures = set()
        config = visual.get("config", {})

        # Handle config as JSON string
        if isinstance(config, str):
            try:
                config = json.loads(config)
            except json.JSONDecodeError:
                return []

        # Deep search for measure references
        self._find_measures_in_dict(config, measures)

        return list(measures)

    def _find_measures_in_dict(self, obj: Any, measures: set) -> None:
        """Recursively search for measure references in a dictionary."""
        if isinstance(obj, dict):
            # Check for measure patterns
            if "measure" in obj:
                measure_ref = obj["measure"]
                if isinstance(measure_ref, dict):
                    name = measure_ref.get("property") or measure_ref.get("name")
                    if name:
                        measures.add(name)
                elif isinstance(measure_ref, str):
                    measures.add(measure_ref)

            # Check for Measure key (used in some formats)
            if "Measure" in obj:
                measure_ref = obj["Measure"]
                if isinstance(measure_ref, dict):
                    name = measure_ref.get("Property") or measure_ref.get("property") or measure_ref.get("Name") or measure_ref.get("name")
                    if name:
                        measures.add(name)

            # Check for property in aggregation context
            if obj.get("aggregation") and "property" in obj:
                prop = obj["property"]
                if prop:
                    measures.add(prop)

            # Recurse into nested dicts and lists
            for value in obj.values():
                self._find_measures_in_dict(value, measures)

        elif isinstance(obj, list):
            for item in obj:
                self._find_measures_in_dict(item, measures)
