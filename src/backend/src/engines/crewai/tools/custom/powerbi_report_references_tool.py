"""
Power BI Report References Extraction Tool for CrewAI

Extracts visual-to-measure/table references from Power BI/Microsoft Fabric reports
using the Fabric Report Definition API (PBIR format).

Generates:
1. Report structure (pages, visuals)
2. Visual-to-measure mappings (which measures are used in which visuals)
3. Visual-to-table mappings (which tables are referenced by visuals)
4. Cross-reference matrix (measure/table usage across pages)

Author: Kasal Team
Date: 2026
"""

import asyncio
import base64
import logging
import re
import json
from typing import Any, Optional, Type, Dict, List, Set

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr
import httpx

logger = logging.getLogger(__name__)


class PowerBIReportReferencesSchema(BaseModel):
    """Input schema for PowerBIReportReferencesTool."""

    # ===== POWER BI CONFIGURATION =====
    workspace_id: Optional[str] = Field(
        None,
        description="[Power BI] Workspace ID (GUID) containing the reports/dataset. Leave empty to use pre-configured value."
    )
    dataset_id: Optional[str] = Field(
        None,
        description="[Power BI] Dataset/Semantic Model ID (GUID). When provided, discovers ALL reports using this dataset and extracts references from each. Recommended approach."
    )
    report_id: Optional[str] = Field(
        None,
        description="[Power BI] Single Report ID (GUID) to extract references from. Use dataset_id instead for comprehensive analysis across all reports."
    )

    # ===== SERVICE PRINCIPAL AUTHENTICATION =====
    tenant_id: Optional[str] = Field(
        None,
        description="[Auth] Azure AD tenant ID for Service Principal authentication."
    )
    client_id: Optional[str] = Field(
        None,
        description="[Auth] Application/Client ID for Service Principal authentication."
    )
    client_secret: Optional[str] = Field(
        None,
        description="[Auth] Client secret for Service Principal authentication."
    )

    # User OAuth token (alternative to Service Principal)
    access_token: Optional[str] = Field(
        None,
        description="[Auth] Pre-obtained OAuth access token (alternative to Service Principal)."
    )

    # ===== OUTPUT OPTIONS =====
    output_format: str = Field(
        "markdown",
        description="[Output] Output format: 'markdown', 'json', or 'matrix' (default: 'markdown')."
    )
    include_visual_details: bool = Field(
        True,
        description="[Output] Include detailed visual configurations (default: True)."
    )
    group_by: str = Field(
        "page",
        description="[Output] Group results by: 'page', 'measure', or 'table' (default: 'page')."
    )


class PowerBIReportReferencesTool(BaseTool):
    """
    Power BI Report References Extraction Tool.

    Extracts visual-to-measure/table references from Fabric reports
    using the Fabric Report Definition API (PBIR format). Generates:

    1. **Report Structure**: Pages and visuals hierarchy
    2. **Measure References**: Which measures are used in each visual
    3. **Table References**: Which tables are accessed by each visual
    4. **Cross-Reference Matrix**: Usage patterns across pages

    **Use Cases**:
    - Identify which report pages use a specific measure
    - Find unused measures in a report
    - Understand report dependencies on semantic model
    - Impact analysis for measure/table changes

    **Requirements**:
    - Service Principal with Report.ReadWrite.All permissions
    - Workspace must be a Microsoft Fabric workspace
    - Report must be in PBIR format (Fabric reports)

    **API Reference**:
    - POST /v1/workspaces/{workspaceId}/reports/{reportId}/getDefinition
    """

    name: str = "Power BI Report References Tool"
    description: str = (
        "Extracts visual-to-measure/table references from Microsoft Fabric reports "
        "using the Fabric Report Definition API (PBIR format). "
        "Shows which measures, tables, and fields are used in each report page and visual. "
        "Useful for understanding report dependencies, impact analysis, and documentation. "
        "Requires Service Principal with Report.ReadWrite.All permissions."
    )
    args_schema: Type[BaseModel] = PowerBIReportReferencesSchema

    # Private attributes
    _instance_id: str = PrivateAttr()
    _default_config: Dict[str, Any] = PrivateAttr()

    # Allow extra attributes for config
    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the tool with configuration."""
        import uuid
        instance_id = str(uuid.uuid4())[:8]

        logger.info(f"[PowerBIReportReferencesTool.__init__] Instance ID: {instance_id}")
        logger.info(f"[PowerBIReportReferencesTool.__init__] kwargs keys: {list(kwargs.keys())}")

        # Helper to check if a value is a placeholder that should be treated as empty
        def is_placeholder_value(value: Any) -> bool:
            if not isinstance(value, str):
                return False
            # Check for {placeholder} patterns
            if re.search(r'^\{[a-z_]+\}$', value):
                return True
            return False

        # Helper to get value, treating placeholders as None
        def get_filtered_value(key: str, default: Any = None) -> Any:
            value = kwargs.get(key, default)
            if is_placeholder_value(value):
                logger.info(f"[PowerBIReportReferencesTool.__init__] Filtering placeholder for {key}: {value}")
                return default
            return value

        # Extract execution_inputs for dynamic parameter resolution
        execution_inputs = kwargs.get("execution_inputs", {})

        # Store configuration values - filter out placeholder values
        default_config = {
            "workspace_id": get_filtered_value("workspace_id"),
            "dataset_id": get_filtered_value("dataset_id"),
            "report_id": get_filtered_value("report_id"),
            "tenant_id": get_filtered_value("tenant_id"),
            "client_id": get_filtered_value("client_id"),
            "client_secret": get_filtered_value("client_secret"),
            "access_token": get_filtered_value("access_token"),
            "output_format": get_filtered_value("output_format", "markdown"),
            "include_visual_details": get_filtered_value("include_visual_details", True),
            "group_by": get_filtered_value("group_by", "page"),
            "mode": get_filtered_value("mode", "static"),
        }

        # Dynamic parameter resolution
        if execution_inputs:
            resolved_config = {}
            for key, value in default_config.items():
                resolved_config[key] = self._resolve_placeholder(value, execution_inputs)
            default_config = resolved_config

        # Log configuration (mask secrets)
        safe_config = {k: v if 'secret' not in k.lower() and 'token' not in k.lower() else '***'
                       for k, v in default_config.items() if v is not None}
        logger.info(f"[PowerBIReportReferencesTool] Config: {safe_config}")

        # Call parent init
        tool_kwargs = {k: v for k, v in kwargs.items() if k not in default_config}
        super().__init__(**tool_kwargs)

        # Set private attributes
        self._instance_id = instance_id
        self._default_config = default_config

    def _resolve_placeholder(self, value: Any, execution_inputs: Dict[str, Any]) -> Any:
        """Resolve {placeholder} parameters from execution_inputs."""
        if not isinstance(value, str):
            return value

        placeholders = re.findall(r'\{(\w+)\}', value)
        if not placeholders:
            return value

        resolved_value = value
        for placeholder in placeholders:
            if placeholder in execution_inputs:
                replacement = str(execution_inputs[placeholder])
                resolved_value = resolved_value.replace(f'{{{placeholder}}}', replacement)

        return resolved_value

    def _run(self, **kwargs: Any) -> str:
        """Execute report references extraction."""
        try:
            instance_id = getattr(self, '_instance_id', 'UNKNOWN')
            logger.info(f"[PowerBIReportReferencesTool] Instance {instance_id} - _run() called")

            # Extract execution_inputs
            execution_inputs = kwargs.pop('execution_inputs', {})

            # Filter placeholder values
            def is_placeholder(value: Any) -> bool:
                if not isinstance(value, str):
                    return False
                patterns = ["your_", "placeholder", "example_", "xxx", "insert_", "<"]
                if any(p in value.lower() for p in patterns):
                    return True
                if re.search(r'^\{[a-z_]+\}$', value):
                    return True
                return False

            filtered_kwargs = {
                k: v for k, v in kwargs.items()
                if v is not None and not is_placeholder(v)
            }

            # Log what was filtered
            filtered_out = {k: v for k, v in kwargs.items() if v is not None and is_placeholder(v)}
            if filtered_out:
                logger.info(f"[PowerBIReportReferencesTool] Filtered out placeholder kwargs: {list(filtered_out.keys())}")

            # Merge with defaults
            merged_kwargs = {**self._default_config, **filtered_kwargs}

            # Resolve dynamic parameters
            if execution_inputs:
                resolved_kwargs = {}
                for key, value in merged_kwargs.items():
                    resolved_kwargs[key] = self._resolve_placeholder(value, execution_inputs)
                merged_kwargs = resolved_kwargs

            # Extract and validate parameters
            workspace_id = merged_kwargs.get("workspace_id")
            dataset_id = merged_kwargs.get("dataset_id")
            report_id = merged_kwargs.get("report_id")
            tenant_id = merged_kwargs.get("tenant_id")
            client_id = merged_kwargs.get("client_id")
            client_secret = merged_kwargs.get("client_secret")
            access_token = merged_kwargs.get("access_token")

            # Helper to check for unresolved placeholders
            def has_unresolved_placeholder(value: Any) -> bool:
                if not isinstance(value, str):
                    return False
                return bool(re.search(r'\{[a-z_]+\}', value))

            # Check for unresolved placeholders
            unresolved = []
            for param_name, param_value in [
                ("workspace_id", workspace_id),
                ("dataset_id", dataset_id),
                ("report_id", report_id),
                ("tenant_id", tenant_id),
                ("client_id", client_id),
                ("client_secret", client_secret),
                ("access_token", access_token),
            ]:
                if has_unresolved_placeholder(param_value):
                    unresolved.append(param_name)

            if unresolved:
                mode = merged_kwargs.get('mode', 'unknown')
                logger.error(f"[PowerBIReportReferencesTool] Unresolved placeholders: {unresolved}")
                return (
                    f"Error: Unresolved placeholder(s) detected: {', '.join(unresolved)}\n\n"
                    f"**Debug Info**:\n"
                    f"- Mode in config: {mode}\n"
                    f"- workspace_id value: `{workspace_id}`\n"
                    f"- report_id value: `{report_id}`\n\n"
                    "For static configuration, enter real values (not {placeholder} values)."
                )

            # Validate required parameters
            if not workspace_id:
                return "Error: workspace_id is required. Provide via parameter or execution_inputs."
            if not dataset_id and not report_id:
                return (
                    "Error: Either dataset_id or report_id is required.\n\n"
                    "**Recommended**: Provide `dataset_id` to discover ALL reports using the dataset.\n"
                    "**Alternative**: Provide `report_id` to analyze a single specific report."
                )

            # Check authentication
            has_spn_auth = all([
                tenant_id and not has_unresolved_placeholder(tenant_id),
                client_id and not has_unresolved_placeholder(client_id),
                client_secret and not has_unresolved_placeholder(client_secret)
            ])
            has_token_auth = bool(access_token and not has_unresolved_placeholder(access_token))

            if not has_spn_auth and not has_token_auth:
                return (
                    "Error: Authentication required.\n\n"
                    "Provide either:\n"
                    "- **Service Principal**: tenant_id + client_id + client_secret\n"
                    "- **User OAuth**: access_token\n\n"
                    "Service Principal requires Report.ReadWrite.All permission."
                )

            # Run async extraction
            result = self._run_sync(self._extract_report_references(
                workspace_id=workspace_id,
                dataset_id=dataset_id,
                report_id=report_id,
                tenant_id=tenant_id if has_spn_auth else None,
                client_id=client_id if has_spn_auth else None,
                client_secret=client_secret if has_spn_auth else None,
                access_token=access_token if has_token_auth else None,
                output_format=merged_kwargs.get("output_format", "markdown"),
                include_visual_details=merged_kwargs.get("include_visual_details", True),
                group_by=merged_kwargs.get("group_by", "page"),
            ))

            return result

        except Exception as e:
            logger.error(f"[PowerBIReportReferencesTool] Error: {str(e)}", exc_info=True)
            return f"Error: {str(e)}"

    def _run_sync(self, coro):
        """Run async coroutine from sync context."""
        try:
            loop = asyncio.get_running_loop()
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(coro)
            finally:
                loop.close()

    async def _extract_report_references(
        self,
        workspace_id: str,
        dataset_id: Optional[str],
        report_id: Optional[str],
        tenant_id: Optional[str],
        client_id: Optional[str],
        client_secret: Optional[str],
        access_token: Optional[str],
        output_format: str,
        include_visual_details: bool,
        group_by: str,
    ) -> str:
        """Main extraction logic for report references."""

        # Step 1: Get access token
        if access_token:
            token = access_token
            logger.info("Using provided access token")
        else:
            logger.info("Obtaining access token via Service Principal")
            assert tenant_id is not None
            assert client_id is not None
            assert client_secret is not None
            token = await self._get_access_token(tenant_id, client_id, client_secret)

        # Step 2: Determine which reports to analyze
        reports_to_analyze: List[Dict[str, Any]] = []

        if dataset_id:
            # Discover all reports using this dataset
            logger.info(f"Discovering reports using dataset {dataset_id}")
            all_reports = await self._list_workspace_reports(workspace_id, token)

            for report in all_reports:
                if report.get("datasetId") == dataset_id:
                    reports_to_analyze.append({
                        "id": report.get("id"),
                        "name": report.get("name", "Unknown"),
                        "webUrl": report.get("webUrl", ""),
                    })

            if not reports_to_analyze:
                return (
                    "# Report References Extraction\n\n"
                    f"**Workspace**: `{workspace_id}`\n"
                    f"**Dataset**: `{dataset_id}`\n\n"
                    "No reports found using this dataset in the workspace.\n\n"
                    "**Possible Causes**:\n"
                    "- No reports have been created from this dataset yet\n"
                    "- Reports exist but use a different dataset ID\n"
                    "- Service Principal lacks access to view reports\n"
                )

            logger.info(f"Found {len(reports_to_analyze)} report(s) using dataset {dataset_id}")
        else:
            # Single report mode
            assert report_id is not None
            reports_to_analyze.append({
                "id": report_id,
                "name": "Single Report",
                "webUrl": self._build_report_url(workspace_id, report_id),
            })

        # Step 3: Process each report
        all_report_results: List[Dict[str, Any]] = []
        failed_reports: List[Dict[str, str]] = []

        for report_info in reports_to_analyze:
            rid = report_info["id"]
            rname = report_info["name"]

            logger.info(f"Processing report: {rname} ({rid})")

            try:
                report_parts = await self._fetch_report_definition(workspace_id, rid, token)

                if not report_parts:
                    failed_reports.append({
                        "id": rid,
                        "name": rname,
                        "error": "Could not fetch report definition (not PBIR format or access denied)"
                    })
                    continue

                # Log all paths for debugging
                all_paths = [p.get("path", "") for p in report_parts]
                logger.info(f"[Report {rname}] Definition has {len(report_parts)} parts: {all_paths}")

                # Parse report structure
                parsed_report_info = self._parse_report_info(report_parts)
                pages = self._parse_pages(report_parts)
                visuals = self._parse_visuals(report_parts)

                # If no pages found, store debug info
                debug_info = None
                if not pages:
                    debug_info = {
                        "parts_count": len(report_parts),
                        "paths": all_paths[:30],  # First 30 paths for debugging
                        "note": "Report definition found but no pages detected. May not be PBIR format."
                    }
                    logger.warning(f"[Report {rname}] No pages found. Paths: {all_paths}")

                # Add page URLs
                for page in pages:
                    page["url"] = self._build_page_url(workspace_id, rid, page.get("id", ""))

                # Extract visual references
                visual_references = self._extract_visual_references(visuals)

                # Build cross-reference data
                cross_ref = self._build_cross_reference(pages, visual_references)

                result_entry = {
                    "report_id": rid,
                    "report_name": rname,
                    "report_url": report_info.get("webUrl") or self._build_report_url(workspace_id, rid),
                    "report_info": parsed_report_info,
                    "pages": pages,
                    "visual_references": visual_references,
                    "cross_ref": cross_ref,
                }

                # Add debug info if no pages found
                if debug_info:
                    result_entry["debug_info"] = debug_info

                all_report_results.append(result_entry)

            except Exception as e:
                logger.warning(f"Error processing report {rname}: {e}", exc_info=True)
                failed_reports.append({
                    "id": rid,
                    "name": rname,
                    "error": str(e)
                })

        if not all_report_results:
            return (
                "# Report References Extraction\n\n"
                f"**Workspace**: `{workspace_id}`\n"
                f"**Dataset**: `{dataset_id or 'N/A'}`\n\n"
                "Error: Could not process any reports.\n\n"
                "**Failed Reports**:\n" +
                "\n".join([f"- {r['name']}: {r['error']}" for r in failed_reports])
            )

        # Step 4: Generate output
        if output_format == "json":
            return self._format_json_output_multi(
                workspace_id, dataset_id, all_report_results, failed_reports
            )
        elif output_format == "matrix":
            return self._format_matrix_output_multi(
                workspace_id, dataset_id, all_report_results
            )
        else:
            return self._format_markdown_output_multi(
                workspace_id, dataset_id, all_report_results, failed_reports,
                include_visual_details, group_by
            )

    def _build_report_url(self, workspace_id: str, report_id: str) -> str:
        """Build the Power BI report URL."""
        return f"https://app.powerbi.com/groups/{workspace_id}/reports/{report_id}"

    def _build_page_url(self, workspace_id: str, report_id: str, page_id: str) -> str:
        """Build the Power BI report page URL."""
        if page_id:
            return f"https://app.powerbi.com/groups/{workspace_id}/reports/{report_id}/ReportSection{page_id}"
        return f"https://app.powerbi.com/groups/{workspace_id}/reports/{report_id}"

    async def _list_workspace_reports(
        self,
        workspace_id: str,
        access_token: str,
    ) -> List[Dict[str, Any]]:
        """List all reports in a workspace."""
        # Use Power BI REST API to list reports
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
                return data.get("value", [])
            except Exception as e:
                logger.error(f"Error listing workspace reports: {e}")
                return []

    async def _get_access_token(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
    ) -> str:
        """Get OAuth access token using Service Principal."""
        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://api.fabric.microsoft.com/.default"
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, data=data)
            response.raise_for_status()
            return response.json()["access_token"]

    async def _fetch_report_definition(
        self,
        workspace_id: str,
        report_id: str,
        access_token: str,
    ) -> List[Dict[str, Any]]:
        """Fetch report definition from Fabric API."""
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/reports/{report_id}/getDefinition"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient(timeout=180.0) as client:
            try:
                # POST to initiate getDefinition
                response = await client.post(url, headers=headers)

                if response.status_code == 202:
                    # Long-running operation
                    location = response.headers.get("Location")
                    if not location:
                        logger.error("No Location header in 202 response")
                        return []

                    # Poll until complete
                    for attempt in range(60):
                        await asyncio.sleep(2)
                        poll_response = await client.get(location, headers=headers)
                        poll_data = poll_response.json()
                        status = poll_data.get("status", "")

                        if status == "Succeeded":
                            logger.info(f"Report definition fetch succeeded after {attempt + 1} poll(s)")
                            result_url = location + "/result"
                            result_response = await client.get(result_url, headers=headers)
                            result_response.raise_for_status()
                            definition = result_response.json()
                            parts = definition.get("definition", {}).get("parts", [])
                            # Log the paths found for debugging
                            if parts:
                                paths = [p.get("path", "") for p in parts[:20]]
                                logger.info(f"Report definition paths (first 20): {paths}")
                            else:
                                logger.warning(f"Report definition returned no parts for report {report_id}")
                            return parts
                        elif status == "Failed":
                            error = poll_data.get("error", {})
                            logger.error(f"Report definition fetch failed: {error}")
                            return []

                    logger.error("Report definition fetch timed out")
                    return []

                elif response.status_code == 200:
                    definition = response.json()
                    parts = definition.get("definition", {}).get("parts", [])
                    # Log the paths found for debugging
                    if parts:
                        paths = [p.get("path", "") for p in parts[:20]]
                        logger.info(f"Report definition paths (first 20): {paths}")
                    else:
                        logger.warning(f"Report definition returned no parts for report {report_id}")
                    return parts
                else:
                    logger.error(f"Unexpected status code: {response.status_code}")
                    logger.error(f"Response: {response.text}")
                    return []

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
                return []
            except Exception as e:
                logger.error(f"Error fetching report definition: {e}")
                return []

    def _parse_report_info(self, report_parts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Parse report.json to get report metadata."""
        for part in report_parts:
            path = part.get("path", "")
            if path == "definition/report.json" or path.endswith("/report.json"):
                try:
                    payload = part.get("payload", "")
                    content = base64.b64decode(payload).decode("utf-8")
                    return json.loads(content)
                except Exception as e:
                    logger.warning(f"Error parsing report.json: {e}")
        return {}

    def _parse_pages(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse page definitions from PBIR structure."""
        pages = []

        # Log all paths for debugging if no pages found
        all_paths = [part.get("path", "") for part in report_parts]
        logger.info(f"[_parse_pages] Total parts: {len(report_parts)}, paths: {all_paths[:10]}...")

        for part in report_parts:
            path = part.get("path", "")
            path_lower = path.lower()

            # Look for page.json files with flexible path matching
            # Patterns: definition/pages/{pageId}/page.json, pages/{pageId}/page.json,
            # report/pages/{pageId}.json, etc.
            is_page_file = (
                ("/pages/" in path_lower and path_lower.endswith("/page.json")) or
                ("/pages/" in path_lower and path_lower.endswith(".json") and "visual" not in path_lower) or
                (path_lower.endswith("/page.json"))
            )

            if is_page_file:
                try:
                    payload = part.get("payload", "")
                    content = base64.b64decode(payload).decode("utf-8")
                    page_data = json.loads(content)

                    # Extract page ID from path - try multiple patterns
                    path_parts = path.split("/")
                    page_id = None

                    # Try to find "pages" in path
                    if "pages" in path_parts:
                        page_id_idx = path_parts.index("pages") + 1
                        page_id = path_parts[page_id_idx] if page_id_idx < len(path_parts) else None
                    elif "Pages" in path_parts:
                        page_id_idx = path_parts.index("Pages") + 1
                        page_id = path_parts[page_id_idx] if page_id_idx < len(path_parts) else None
                    else:
                        # Fallback: use the folder name before page.json
                        page_id = path_parts[-2] if len(path_parts) >= 2 else "unknown"

                    pages.append({
                        "id": page_id,
                        "name": page_data.get("name", page_id),
                        "displayName": page_data.get("displayName", page_data.get("name", page_id)),
                        "ordinal": page_data.get("ordinal", 0),
                        "config": page_data
                    })

                    logger.info(f"Found page: {page_data.get('displayName', page_id)} from path: {path}")

                except Exception as e:
                    logger.warning(f"Error parsing page from {path}: {e}")

        if not pages:
            logger.warning(f"[_parse_pages] No pages found. All paths: {all_paths}")

        # Sort pages by ordinal
        pages.sort(key=lambda p: p.get("ordinal", 0))
        return pages

    def _parse_visuals(self, report_parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse visual definitions from PBIR structure."""
        visuals = []

        for part in report_parts:
            path = part.get("path", "")
            path_lower = path.lower()

            # Look for visual.json files with flexible path matching
            # Patterns: definition/pages/{pageId}/visuals/{visualId}/visual.json
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

                    # Find page ID
                    page_id = None
                    if "pages" in path_parts:
                        page_id_idx = path_parts.index("pages") + 1
                        page_id = path_parts[page_id_idx] if page_id_idx < len(path_parts) else None
                    elif "Pages" in path_parts:
                        page_id_idx = path_parts.index("Pages") + 1
                        page_id = path_parts[page_id_idx] if page_id_idx < len(path_parts) else None

                    # Find visual ID
                    visual_id = None
                    if "visuals" in path_parts:
                        visual_id_idx = path_parts.index("visuals") + 1
                        visual_id = path_parts[visual_id_idx] if visual_id_idx < len(path_parts) else None
                    elif "Visuals" in path_parts:
                        visual_id_idx = path_parts.index("Visuals") + 1
                        visual_id = path_parts[visual_id_idx] if visual_id_idx < len(path_parts) else None
                    else:
                        # Fallback: use folder name before visual.json
                        visual_id = path_parts[-2] if len(path_parts) >= 2 else "unknown"

                    visuals.append({
                        "id": visual_id,
                        "page_id": page_id,
                        "type": visual_data.get("visual", {}).get("visualType", "unknown"),
                        "name": visual_data.get("name", visual_id),
                        "config": visual_data
                    })

                    logger.debug(f"Found visual: {visual_id} (type: {visual_data.get('visual', {}).get('visualType', 'unknown')}) from path: {path}")

                except Exception as e:
                    logger.warning(f"Error parsing visual from {path}: {e}")

        logger.info(f"[_parse_visuals] Found {len(visuals)} visuals")
        return visuals

    def _extract_visual_references(self, visuals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract measure/table references from visual configurations."""
        references = []

        for visual in visuals:
            config = visual.get("config", {})
            visual_config = config.get("visual", {})

            # Extract data bindings
            measures: Set[str] = set()
            tables: Set[str] = set()
            columns: Set[str] = set()

            # Method 1: Parse queryDefinition (common in newer PBIR format)
            query_def = visual_config.get("queryDefinition", {})
            self._extract_from_query_definition(query_def, measures, tables, columns)

            # Method 2: Parse visualContainerObjects (for filters, slicers)
            container_objects = visual_config.get("visualContainerObjects", {})
            self._extract_from_container_objects(container_objects, measures, tables, columns)

            # Method 3: Parse prototypeQuery (legacy format)
            prototype_query = visual_config.get("prototypeQuery", {})
            self._extract_from_prototype_query(prototype_query, measures, tables, columns)

            # Method 4: Parse dataTransforms (data bindings)
            data_transforms = visual_config.get("dataTransforms", {})
            self._extract_from_data_transforms(data_transforms, measures, tables, columns)

            # Method 5: Deep search for any field references in config
            self._deep_search_references(visual_config, measures, tables, columns)

            references.append({
                "visual_id": visual.get("id"),
                "page_id": visual.get("page_id"),
                "visual_type": visual.get("type"),
                "visual_name": visual.get("name"),
                "measures": sorted(list(measures)),
                "tables": sorted(list(tables)),
                "columns": sorted(list(columns)),
            })

        return references

    def _extract_from_query_definition(
        self,
        query_def: Dict[str, Any],
        measures: Set[str],
        tables: Set[str],
        columns: Set[str]
    ) -> None:
        """Extract references from queryDefinition structure."""
        # Check From clause for tables
        from_clause = query_def.get("from", [])
        for item in from_clause:
            if isinstance(item, dict):
                entity = item.get("entity")
                if entity:
                    tables.add(entity)

        # Check Select clause for measures and columns
        select_clause = query_def.get("select", [])
        for item in select_clause:
            if isinstance(item, dict):
                # Check for measure reference
                measure = item.get("measure", {})
                if measure:
                    measure_name = measure.get("property") or measure.get("name")
                    if measure_name:
                        measures.add(measure_name)

                # Check for column reference
                column = item.get("column", {})
                if column:
                    col_name = column.get("property") or column.get("name")
                    if col_name:
                        columns.add(col_name)
                    entity = column.get("entity")
                    if entity:
                        tables.add(entity)

    def _extract_from_container_objects(
        self,
        container_objects: Dict[str, Any],
        measures: Set[str],
        tables: Set[str],
        columns: Set[str]
    ) -> None:
        """Extract references from visualContainerObjects (filters, slicers)."""
        for _key, value in container_objects.items():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        # Check properties
                        props = item.get("properties", {})
                        for _prop_name, prop_value in props.items():
                            if isinstance(prop_value, dict):
                                expr = prop_value.get("expr", {})
                                self._extract_from_expression(expr, measures, tables, columns)

    def _extract_from_prototype_query(
        self,
        prototype_query: Dict[str, Any],
        measures: Set[str],
        tables: Set[str],
        columns: Set[str]
    ) -> None:
        """Extract references from prototypeQuery (legacy format)."""
        select = prototype_query.get("Select", [])
        for item in select:
            if isinstance(item, dict):
                # Check Measure
                measure_ref = item.get("Measure", {})
                if measure_ref:
                    prop = measure_ref.get("Property")
                    if prop:
                        measures.add(prop)
                    expr = measure_ref.get("Expression", {})
                    source_ref = expr.get("SourceRef", {})
                    entity = source_ref.get("Entity")
                    if entity:
                        tables.add(entity)

                # Check Column
                col_ref = item.get("Column", {})
                if col_ref:
                    prop = col_ref.get("Property")
                    if prop:
                        columns.add(prop)
                    expr = col_ref.get("Expression", {})
                    source_ref = expr.get("SourceRef", {})
                    entity = source_ref.get("Entity")
                    if entity:
                        tables.add(entity)

    def _extract_from_data_transforms(
        self,
        data_transforms: Dict[str, Any],
        measures: Set[str],
        tables: Set[str],
        columns: Set[str]
    ) -> None:
        """Extract references from dataTransforms."""
        # Check selects
        selects = data_transforms.get("selects", [])
        for select in selects:
            if isinstance(select, dict):
                display_name = select.get("displayName")
                query_name = select.get("queryName")

                # Parse queryName format: Table.Measure or Table.Column
                for name in [display_name, query_name]:
                    if name and "." in name:
                        parts = name.split(".")
                        if len(parts) >= 2:
                            tables.add(parts[0])
                            # Determine if it's a measure (usually aggregated) or column
                            field_name = parts[-1]
                            columns.add(field_name)

        # Check queryMetadata
        query_metadata = data_transforms.get("queryMetadata", {})
        bindings = query_metadata.get("Binding", {})
        primary = bindings.get("Primary", {})
        groupings = primary.get("Groupings", [])

        for group in groupings:
            if isinstance(group, dict):
                projections = group.get("Projections", [])
                for proj in projections:
                    if isinstance(proj, int):
                        # Reference to selects index
                        if proj < len(selects):
                            select = selects[proj]
                            if isinstance(select, dict):
                                query_name = select.get("queryName", "")
                                if "." in query_name:
                                    parts = query_name.split(".")
                                    tables.add(parts[0])

    def _extract_from_expression(
        self,
        expr: Dict[str, Any],
        measures: Set[str],
        tables: Set[str],
        columns: Set[str]
    ) -> None:
        """Recursively extract references from expression trees."""
        if not isinstance(expr, dict):
            return

        # Check for measure reference
        if "Measure" in expr:
            measure_info = expr["Measure"]
            if isinstance(measure_info, dict):
                prop = measure_info.get("Property")
                if prop:
                    measures.add(prop)

        # Check for column reference
        if "Column" in expr:
            col_info = expr["Column"]
            if isinstance(col_info, dict):
                prop = col_info.get("Property")
                if prop:
                    columns.add(prop)
                # Get table from expression source
                expr_source = col_info.get("Expression", {})
                source_ref = expr_source.get("SourceRef", {})
                entity = source_ref.get("Entity")
                if entity:
                    tables.add(entity)

        # Recurse into nested structures
        for _key, value in expr.items():
            if isinstance(value, dict):
                self._extract_from_expression(value, measures, tables, columns)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self._extract_from_expression(item, measures, tables, columns)

    def _deep_search_references(
        self,
        obj: Any,
        measures: Set[str],
        tables: Set[str],
        columns: Set[str],
        depth: int = 0
    ) -> None:
        """Deep search for field references in any structure."""
        if depth > 10:  # Prevent infinite recursion
            return

        if isinstance(obj, dict):
            # Look for common reference patterns
            for key, value in obj.items():
                key_lower = key.lower()

                # Entity/Table references
                if key_lower in ("entity", "table", "source"):
                    if isinstance(value, str) and value and not value.startswith("_"):
                        tables.add(value)

                # Measure references
                elif key_lower in ("measure", "measurename"):
                    if isinstance(value, str) and value:
                        measures.add(value)

                # Column references
                elif key_lower in ("column", "property", "field"):
                    if isinstance(value, str) and value and not value.startswith("_"):
                        columns.add(value)

                # Recurse
                self._deep_search_references(value, measures, tables, columns, depth + 1)

        elif isinstance(obj, list):
            for item in obj:
                self._deep_search_references(item, measures, tables, columns, depth + 1)

    def _build_cross_reference(
        self,
        pages: List[Dict[str, Any]],
        visual_references: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Build cross-reference matrix showing usage across pages."""
        # Build page lookup
        page_lookup = {p["id"]: p for p in pages}

        # Build measure-to-pages mapping
        measure_pages: Dict[str, Set[str]] = {}
        table_pages: Dict[str, Set[str]] = {}
        column_pages: Dict[str, Set[str]] = {}

        # Build page-to-measures mapping
        page_measures: Dict[str, Set[str]] = {}
        page_tables: Dict[str, Set[str]] = {}

        for ref in visual_references:
            page_id = ref.get("page_id", "unknown")
            if not page_id:
                page_id = "unknown"
            page_info = page_lookup.get(page_id, {})
            page_name = page_info.get("displayName", page_id) if page_info else page_id

            if page_id not in page_measures:
                page_measures[page_id] = set()
                page_tables[page_id] = set()

            for measure in ref.get("measures", []):
                if measure not in measure_pages:
                    measure_pages[measure] = set()
                if page_name:
                    measure_pages[measure].add(page_name)
                page_measures[page_id].add(measure)

            for table in ref.get("tables", []):
                if table not in table_pages:
                    table_pages[table] = set()
                if page_name:
                    table_pages[table].add(page_name)
                page_tables[page_id].add(table)

            for column in ref.get("columns", []):
                if column not in column_pages:
                    column_pages[column] = set()
                if page_name:
                    column_pages[column].add(page_name)

        return {
            "measure_pages": {k: sorted(list(v)) for k, v in measure_pages.items()},
            "table_pages": {k: sorted(list(v)) for k, v in table_pages.items()},
            "column_pages": {k: sorted(list(v)) for k, v in column_pages.items()},
            "page_measures": {k: sorted(list(v)) for k, v in page_measures.items()},
            "page_tables": {k: sorted(list(v)) for k, v in page_tables.items()},
        }

    def _format_markdown_output(
        self,
        workspace_id: str,
        report_id: str,
        report_info: Dict[str, Any],
        pages: List[Dict[str, Any]],
        visual_references: List[Dict[str, Any]],
        cross_ref: Dict[str, Any],
        include_visual_details: bool,
        group_by: str,
    ) -> str:
        """Format output as markdown."""
        output = []

        # Header
        output.append("# Power BI Report References Extraction Results\n")
        output.append(f"**Workspace ID**: `{workspace_id}`")
        output.append(f"**Report ID**: `{report_id}`")
        if report_info.get("name"):
            output.append(f"**Report Name**: {report_info.get('name')}")
        output.append("")
        output.append(f"**Pages Found**: {len(pages)}")
        output.append(f"**Visuals Found**: {len(visual_references)}")

        # Count unique measures and tables
        all_measures = set()
        all_tables = set()
        for ref in visual_references:
            all_measures.update(ref.get("measures", []))
            all_tables.update(ref.get("tables", []))

        output.append(f"**Unique Measures Referenced**: {len(all_measures)}")
        output.append(f"**Unique Tables Referenced**: {len(all_tables)}\n")

        # Pages Overview
        output.append("---\n")
        output.append("## Report Pages\n")
        output.append("| # | Page Name | Visuals |")
        output.append("|---|-----------|---------|")

        for idx, page in enumerate(pages, 1):
            page_visuals = [v for v in visual_references if v["page_id"] == page["id"]]
            output.append(f"| {idx} | {page['displayName']} | {len(page_visuals)} |")
        output.append("")

        if group_by == "page":
            # Group by page
            output.append("---\n")
            output.append("## References by Page\n")

            for page in pages:
                output.append(f"### {page['displayName']}\n")

                page_visuals = [v for v in visual_references if v["page_id"] == page["id"]]

                if not page_visuals:
                    output.append("*No visuals with data bindings on this page*\n")
                    continue

                # Aggregate measures and tables for this page
                page_measures = set()
                page_tables = set()
                for vis in page_visuals:
                    page_measures.update(vis.get("measures", []))
                    page_tables.update(vis.get("tables", []))

                if page_measures:
                    output.append("**Measures Used**:")
                    for m in sorted(page_measures):
                        output.append(f"- `{m}`")
                    output.append("")

                if page_tables:
                    output.append("**Tables Referenced**:")
                    for t in sorted(page_tables):
                        output.append(f"- `{t}`")
                    output.append("")

                if include_visual_details:
                    output.append("**Visual Details**:\n")
                    output.append("| Visual | Type | Measures | Tables |")
                    output.append("|--------|------|----------|--------|")
                    for vis in page_visuals:
                        measures = ", ".join(vis.get("measures", [])[:3])
                        if len(vis.get("measures", [])) > 3:
                            measures += "..."
                        tables = ", ".join(vis.get("tables", [])[:3])
                        if len(vis.get("tables", [])) > 3:
                            tables += "..."
                        output.append(f"| {vis['visual_name'] or vis['visual_id'][:8]} | {vis['visual_type']} | {measures} | {tables} |")
                    output.append("")

        elif group_by == "measure":
            # Group by measure
            output.append("---\n")
            output.append("## References by Measure\n")

            measure_pages = cross_ref.get("measure_pages", {})
            for measure in sorted(measure_pages.keys()):
                pages_list = measure_pages[measure]
                output.append(f"### `{measure}`\n")
                output.append(f"**Used in {len(pages_list)} page(s)**:")
                for p in pages_list:
                    output.append(f"- {p}")
                output.append("")

        elif group_by == "table":
            # Group by table
            output.append("---\n")
            output.append("## References by Table\n")

            table_pages = cross_ref.get("table_pages", {})
            for table in sorted(table_pages.keys()):
                pages_list = table_pages[table]
                output.append(f"### `{table}`\n")
                output.append(f"**Referenced in {len(pages_list)} page(s)**:")
                for p in pages_list:
                    output.append(f"- {p}")
                output.append("")

        # Cross-Reference Matrix
        output.append("---\n")
        output.append("## Cross-Reference Summary\n")

        output.append("### Measures by Page Count\n")
        output.append("| Measure | # Pages | Pages |")
        output.append("|---------|---------|-------|")
        measure_pages = cross_ref.get("measure_pages", {})
        for measure in sorted(measure_pages.keys(), key=lambda m: -len(measure_pages[m])):
            pages_list = measure_pages[measure]
            pages_str = ", ".join(pages_list[:3])
            if len(pages_list) > 3:
                pages_str += f" (+{len(pages_list) - 3} more)"
            output.append(f"| `{measure}` | {len(pages_list)} | {pages_str} |")
        output.append("")

        output.append("### Tables by Page Count\n")
        output.append("| Table | # Pages | Pages |")
        output.append("|-------|---------|-------|")
        table_pages = cross_ref.get("table_pages", {})
        for table in sorted(table_pages.keys(), key=lambda t: -len(table_pages[t])):
            pages_list = table_pages[table]
            pages_str = ", ".join(pages_list[:3])
            if len(pages_list) > 3:
                pages_str += f" (+{len(pages_list) - 3} more)"
            output.append(f"| `{table}` | {len(pages_list)} | {pages_str} |")
        output.append("")

        # Summary
        output.append("---\n")
        output.append("## Summary\n")
        output.append(f"- **Total Pages**: {len(pages)}")
        output.append(f"- **Total Visuals**: {len(visual_references)}")
        output.append(f"- **Unique Measures**: {len(all_measures)}")
        output.append(f"- **Unique Tables**: {len(all_tables)}")

        return "\n".join(output)

    def _format_json_output(
        self,
        workspace_id: str,
        report_id: str,
        report_info: Dict[str, Any],
        pages: List[Dict[str, Any]],
        visual_references: List[Dict[str, Any]],
        cross_ref: Dict[str, Any],
    ) -> str:
        """Format output as JSON."""
        # Clean pages for output
        clean_pages = []
        for p in pages:
            clean_pages.append({
                "id": p["id"],
                "name": p["name"],
                "displayName": p["displayName"],
                "ordinal": p.get("ordinal", 0)
            })

        result = {
            "workspace_id": workspace_id,
            "report_id": report_id,
            "report_name": report_info.get("name"),
            "pages": clean_pages,
            "visual_references": visual_references,
            "cross_reference": cross_ref,
            "summary": {
                "page_count": len(pages),
                "visual_count": len(visual_references),
                "unique_measures": len(cross_ref.get("measure_pages", {})),
                "unique_tables": len(cross_ref.get("table_pages", {})),
            }
        }

        return json.dumps(result, indent=2)

    def _format_matrix_output(
        self,
        _workspace_id: str,  # kept for API consistency with other format methods
        report_id: str,
        report_info: Dict[str, Any],
        pages: List[Dict[str, Any]],
        _visual_references: List[Dict[str, Any]],  # kept for API consistency
        cross_ref: Dict[str, Any],
    ) -> str:
        """Format output as a usage matrix (measures vs pages)."""
        output = []

        output.append("# Power BI Report References Matrix\n")
        output.append(f"**Report**: {report_info.get('name', report_id)}\n")

        # Build matrix header
        page_names = [p["displayName"] for p in pages]

        # Measures matrix
        output.append("## Measure Usage Matrix\n")
        output.append("| Measure | " + " | ".join(page_names) + " |")
        output.append("|---------|" + "|".join(["---"] * len(page_names)) + "|")

        measure_pages = cross_ref.get("measure_pages", {})
        for measure in sorted(measure_pages.keys()):
            row = [measure]
            for page_name in page_names:
                if page_name in measure_pages[measure]:
                    row.append("✓")
                else:
                    row.append("")
            output.append("| " + " | ".join(row) + " |")

        output.append("")

        # Tables matrix
        output.append("## Table Usage Matrix\n")
        output.append("| Table | " + " | ".join(page_names) + " |")
        output.append("|-------|" + "|".join(["---"] * len(page_names)) + "|")

        table_pages = cross_ref.get("table_pages", {})
        for table in sorted(table_pages.keys()):
            row = [table]
            for page_name in page_names:
                if page_name in table_pages[table]:
                    row.append("✓")
                else:
                    row.append("")
            output.append("| " + " | ".join(row) + " |")

        return "\n".join(output)

    # ========================================
    # MULTI-REPORT OUTPUT METHODS
    # ========================================

    def _format_markdown_output_multi(
        self,
        workspace_id: str,
        dataset_id: Optional[str],
        all_report_results: List[Dict[str, Any]],
        failed_reports: List[Dict[str, str]],
        include_visual_details: bool,
        group_by: str,
    ) -> str:
        """Format multi-report output as markdown with page URLs."""
        output = []

        # Header
        output.append("# Power BI Report References Extraction Results\n")
        output.append(f"**Workspace ID**: `{workspace_id}`")
        if dataset_id:
            output.append(f"**Dataset ID**: `{dataset_id}`")
        output.append(f"**Reports Analyzed**: {len(all_report_results)}")
        if failed_reports:
            output.append(f"**Failed Reports**: {len(failed_reports)}")
        output.append("")

        # Global statistics
        total_pages = sum(len(r["pages"]) for r in all_report_results)
        total_visuals = sum(len(r["visual_references"]) for r in all_report_results)
        all_measures: Set[str] = set()
        all_tables: Set[str] = set()
        for r in all_report_results:
            for ref in r["visual_references"]:
                all_measures.update(ref.get("measures", []))
                all_tables.update(ref.get("tables", []))

        output.append(f"**Total Pages**: {total_pages}")
        output.append(f"**Total Visuals**: {total_visuals}")
        output.append(f"**Unique Measures**: {len(all_measures)}")
        output.append(f"**Unique Tables**: {len(all_tables)}\n")

        # Reports Overview Table
        output.append("---\n")
        output.append("## Reports Overview\n")
        output.append("| Report Name | Pages | Visuals | Measures | Tables | Link |")
        output.append("|-------------|-------|---------|----------|--------|------|")

        for r in all_report_results:
            rname = r["report_name"]
            rurl = r["report_url"]
            npages = len(r["pages"])
            nvisuals = len(r["visual_references"])
            nmeasures = len(r["cross_ref"].get("measure_pages", {}))
            ntables = len(r["cross_ref"].get("table_pages", {}))
            output.append(f"| {rname} | {npages} | {nvisuals} | {nmeasures} | {ntables} | [Open]({rurl}) |")
        output.append("")

        # Failed reports
        if failed_reports:
            output.append("### Failed Reports\n")
            for fr in failed_reports:
                output.append(f"- **{fr['name']}** (`{fr['id']}`): {fr['error']}")
            output.append("")

        # Per-report details
        for r in all_report_results:
            rname = r["report_name"]
            rurl = r["report_url"]
            pages = r["pages"]
            visual_references = r["visual_references"]
            cross_ref = r["cross_ref"]

            output.append("---\n")
            output.append(f"## Report: {rname}\n")
            output.append(f"**Report URL**: [{rname}]({rurl})\n")

            # Pages with URLs
            output.append("### Pages\n")
            output.append("| # | Page Name | Visuals | Link |")
            output.append("|---|-----------|---------|------|")

            for idx, page in enumerate(pages, 1):
                page_visuals = [v for v in visual_references if v["page_id"] == page["id"]]
                page_url = page.get("url", rurl)
                output.append(f"| {idx} | {page['displayName']} | {len(page_visuals)} | [Open]({page_url}) |")
            output.append("")

            if group_by == "page":
                output.append("### References by Page\n")
                for page in pages:
                    page_url = page.get("url", rurl)
                    output.append(f"#### [{page['displayName']}]({page_url})\n")

                    page_visuals = [v for v in visual_references if v["page_id"] == page["id"]]

                    if not page_visuals:
                        output.append("*No visuals with data bindings on this page*\n")
                        continue

                    # Aggregate measures and tables for this page
                    page_measures: Set[str] = set()
                    page_tables: Set[str] = set()
                    for vis in page_visuals:
                        page_measures.update(vis.get("measures", []))
                        page_tables.update(vis.get("tables", []))

                    if page_measures:
                        output.append("**Measures**:")
                        for m in sorted(page_measures):
                            output.append(f"- `{m}`")
                        output.append("")

                    if page_tables:
                        output.append("**Tables**:")
                        for t in sorted(page_tables):
                            output.append(f"- `{t}`")
                        output.append("")

                    if include_visual_details:
                        output.append("**Visuals**:\n")
                        output.append("| Visual | Type | Measures | Tables |")
                        output.append("|--------|------|----------|--------|")
                        for vis in page_visuals:
                            measures_str = ", ".join(vis.get("measures", [])[:3])
                            if len(vis.get("measures", [])) > 3:
                                measures_str += "..."
                            tables_str = ", ".join(vis.get("tables", [])[:3])
                            if len(vis.get("tables", [])) > 3:
                                tables_str += "..."
                            output.append(f"| {vis.get('visual_name') or vis.get('visual_id', '')[:8]} | {vis.get('visual_type', 'unknown')} | {measures_str} | {tables_str} |")
                        output.append("")

            elif group_by == "measure":
                output.append("### References by Measure\n")
                measure_pages = cross_ref.get("measure_pages", {})
                for measure in sorted(measure_pages.keys()):
                    pages_list = measure_pages[measure]
                    output.append(f"#### `{measure}`\n")
                    output.append(f"**Used in {len(pages_list)} page(s)**:")
                    for pname in pages_list:
                        # Find page URL
                        page_obj = next((p for p in pages if p["displayName"] == pname), None)
                        if page_obj:
                            output.append(f"- [{pname}]({page_obj.get('url', rurl)})")
                        else:
                            output.append(f"- {pname}")
                    output.append("")

            elif group_by == "table":
                output.append("### References by Table\n")
                table_pages = cross_ref.get("table_pages", {})
                for table in sorted(table_pages.keys()):
                    pages_list = table_pages[table]
                    output.append(f"#### `{table}`\n")
                    output.append(f"**Referenced in {len(pages_list)} page(s)**:")
                    for pname in pages_list:
                        # Find page URL
                        page_obj = next((p for p in pages if p["displayName"] == pname), None)
                        if page_obj:
                            output.append(f"- [{pname}]({page_obj.get('url', rurl)})")
                        else:
                            output.append(f"- {pname}")
                    output.append("")

        # Global Cross-Reference Summary
        output.append("---\n")
        output.append("## Global Cross-Reference Summary\n")
        output.append("*Aggregated across all reports*\n")

        # Build global measure -> reports mapping
        measure_reports: Dict[str, List[str]] = {}
        table_reports: Dict[str, List[str]] = {}

        for r in all_report_results:
            rname = r["report_name"]
            for measure in r["cross_ref"].get("measure_pages", {}).keys():
                if measure not in measure_reports:
                    measure_reports[measure] = []
                if rname not in measure_reports[measure]:
                    measure_reports[measure].append(rname)

            for table in r["cross_ref"].get("table_pages", {}).keys():
                if table not in table_reports:
                    table_reports[table] = []
                if rname not in table_reports[table]:
                    table_reports[table].append(rname)

        output.append("### Measures by Report Usage\n")
        output.append("| Measure | # Reports | Reports |")
        output.append("|---------|-----------|---------|")
        for measure in sorted(measure_reports.keys(), key=lambda m: -len(measure_reports[m])):
            reports = measure_reports[measure]
            reports_str = ", ".join(reports[:3])
            if len(reports) > 3:
                reports_str += f" (+{len(reports) - 3} more)"
            output.append(f"| `{measure}` | {len(reports)} | {reports_str} |")
        output.append("")

        output.append("### Tables by Report Usage\n")
        output.append("| Table | # Reports | Reports |")
        output.append("|-------|-----------|---------|")
        for table in sorted(table_reports.keys(), key=lambda t: -len(table_reports[t])):
            reports = table_reports[table]
            reports_str = ", ".join(reports[:3])
            if len(reports) > 3:
                reports_str += f" (+{len(reports) - 3} more)"
            output.append(f"| `{table}` | {len(reports)} | {reports_str} |")
        output.append("")

        return "\n".join(output)

    def _format_json_output_multi(
        self,
        workspace_id: str,
        dataset_id: Optional[str],
        all_report_results: List[Dict[str, Any]],
        failed_reports: List[Dict[str, str]],
    ) -> str:
        """Format multi-report output as JSON with page URLs."""
        # Build reports array
        reports = []
        for r in all_report_results:
            # Clean pages for output (include URLs)
            clean_pages = []
            for p in r["pages"]:
                clean_pages.append({
                    "id": p["id"],
                    "name": p["name"],
                    "displayName": p["displayName"],
                    "ordinal": p.get("ordinal", 0),
                    "url": p.get("url", ""),
                })

            report_entry = {
                "report_id": r["report_id"],
                "report_name": r["report_name"],
                "report_url": r["report_url"],
                "pages": clean_pages,
                "visual_references": r["visual_references"],
                "cross_reference": r["cross_ref"],
            }

            # Include debug info if present (helps diagnose empty results)
            if r.get("debug_info"):
                report_entry["debug_info"] = r["debug_info"]

            reports.append(report_entry)

        # Build global cross-reference
        global_measures: Dict[str, List[str]] = {}
        global_tables: Dict[str, List[str]] = {}

        for r in all_report_results:
            rname = r["report_name"]
            # rurl used for potential future enhancement (report-level URLs in cross-ref)
            for measure in r["cross_ref"].get("measure_pages", {}).keys():
                if measure not in global_measures:
                    global_measures[measure] = []
                global_measures[measure].append(rname)

            for table in r["cross_ref"].get("table_pages", {}).keys():
                if table not in global_tables:
                    global_tables[table] = []
                global_tables[table].append(rname)

        result = {
            "workspace_id": workspace_id,
            "dataset_id": dataset_id,
            "reports": reports,
            "failed_reports": failed_reports,
            "global_cross_reference": {
                "measure_reports": global_measures,
                "table_reports": global_tables,
            },
            "summary": {
                "reports_analyzed": len(all_report_results),
                "reports_failed": len(failed_reports),
                "total_pages": sum(len(r["pages"]) for r in all_report_results),
                "total_visuals": sum(len(r["visual_references"]) for r in all_report_results),
                "unique_measures": len(global_measures),
                "unique_tables": len(global_tables),
            }
        }

        return json.dumps(result, indent=2)

    def _format_matrix_output_multi(
        self,
        workspace_id: str,
        dataset_id: Optional[str],
        all_report_results: List[Dict[str, Any]],
    ) -> str:
        """Format multi-report output as a usage matrix."""
        output = []

        output.append("# Power BI Report References Matrix\n")
        output.append(f"**Workspace**: `{workspace_id}`")
        if dataset_id:
            output.append(f"**Dataset**: `{dataset_id}`")
        output.append(f"**Reports**: {len(all_report_results)}\n")

        # Build global measure -> reports mapping
        measure_reports: Dict[str, Set[str]] = {}
        table_reports: Dict[str, Set[str]] = {}
        report_names = []

        for r in all_report_results:
            rname = r["report_name"]
            report_names.append(rname)

            for measure in r["cross_ref"].get("measure_pages", {}).keys():
                if measure not in measure_reports:
                    measure_reports[measure] = set()
                measure_reports[measure].add(rname)

            for table in r["cross_ref"].get("table_pages", {}).keys():
                if table not in table_reports:
                    table_reports[table] = set()
                table_reports[table].add(rname)

        # Measures x Reports matrix
        output.append("## Measure Usage Matrix (Measures × Reports)\n")
        output.append("| Measure | " + " | ".join(report_names) + " |")
        output.append("|---------|" + "|".join(["---"] * len(report_names)) + "|")

        for measure in sorted(measure_reports.keys()):
            row = [f"`{measure}`"]
            for rname in report_names:
                if rname in measure_reports[measure]:
                    row.append("✓")
                else:
                    row.append("")
            output.append("| " + " | ".join(row) + " |")
        output.append("")

        # Tables x Reports matrix
        output.append("## Table Usage Matrix (Tables × Reports)\n")
        output.append("| Table | " + " | ".join(report_names) + " |")
        output.append("|-------|" + "|".join(["---"] * len(report_names)) + "|")

        for table in sorted(table_reports.keys()):
            row = [f"`{table}`"]
            for rname in report_names:
                if rname in table_reports[table]:
                    row.append("✓")
                else:
                    row.append("")
            output.append("| " + " | ".join(row) + " |")
        output.append("")

        # Per-report page matrices
        for r in all_report_results:
            rname = r["report_name"]
            rurl = r["report_url"]
            pages = r["pages"]
            cross_ref = r["cross_ref"]

            output.append(f"---\n")
            output.append(f"## Report: [{rname}]({rurl})\n")

            page_names = [p["displayName"] for p in pages]

            output.append("### Measure × Page Matrix\n")
            output.append("| Measure | " + " | ".join(page_names) + " |")
            output.append("|---------|" + "|".join(["---"] * len(page_names)) + "|")

            measure_pages = cross_ref.get("measure_pages", {})
            for measure in sorted(measure_pages.keys()):
                row = [f"`{measure}`"]
                for pname in page_names:
                    if pname in measure_pages[measure]:
                        row.append("✓")
                    else:
                        row.append("")
                output.append("| " + " | ".join(row) + " |")
            output.append("")

        return "\n".join(output)
