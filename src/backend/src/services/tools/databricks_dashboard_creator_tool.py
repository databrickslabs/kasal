"""Databricks Dashboard Creator Tool for CrewAI.

Takes visual-to-UCMV mappings from the PBI Visual-UCMV Mapper (tool 94) and creates
a Databricks AI/BI (Lakeview) dashboard via the REST API.

Datasets are the UC metric views themselves (``asset_name``) and widgets use
``MEASURE()``, so measures are evaluated at the widget's grain and under every filter
(``dataset_mode: sql`` keeps one grouped SQL dataset per visual instead). Power BI visual
types map to bar / line / area / pie / scatter / counter / table widgets (see
``dashboard_creator_utils/visual_types.py``), slicers become filter widgets, and the
output carries a per-visual coverage report (mapped / approximated / table fallback /
filter / decorative / unsupported / unmapped, with a note for every loss).
"""

import json
import logging
import urllib.parse
from typing import TYPE_CHECKING, Any, Optional, Type

from pydantic import BaseModel, Field, PrivateAttr

from src.services.tools.base import BaseTool
from src.services.tools.dashboard_creator_utils.build import (
    build_dashboard,
    coverage,
    dashboard_counts,
    safe_name,
)
from src.services.tools.dashboard_creator_utils.visual_types import rule_for

if TYPE_CHECKING:
    from src.utils.databricks_auth import AuthContext

logger = logging.getLogger(__name__)


class DatabricksDashboardCreatorSchema(BaseModel):
    """Input schema for DatabricksDashboardCreatorTool."""

    visual_mappings_json: Optional[str] = Field(
        None,
        description="JSON from tool 94 containing visual_mappings array (auto-injected from flow)",
    )
    dashboard_title: Optional[str] = Field(None, description="Dashboard display name")
    catalog: Optional[str] = Field(None, description="UC catalog")
    schema_name: Optional[str] = Field(None, description="UC schema")
    warehouse_id: Optional[str] = Field(
        None, description="SQL warehouse for dashboard queries"
    )
    databricks_host: Optional[str] = Field(
        None, description="Override workspace URL (optional)"
    )
    parent_path: Optional[str] = Field(
        None, description="Workspace folder path (default: /Workspace/Shared)"
    )
    publish_dashboard: Optional[bool] = Field(
        None, description="Whether to publish after creation (default: True)"
    )
    dataset_mode: Optional[str] = Field(
        None,
        description="metric_view (default: datasets are the metric views, filters work) "
        "| sql (one grouped SQL dataset per visual, no filters)",
    )


class DatabricksDashboardCreatorTool(BaseTool):
    """Create Databricks AI/BI (Lakeview) dashboards from PBI visual-to-UCMV mappings."""

    name: str = "Databricks Dashboard Creator"
    description: str = (
        "Creates Databricks AI/BI (Lakeview) dashboards from visual-to-UCMV mappings. "
        "Takes the structured visual mappings from the PBI Visual-UCMV Mapper (tool 94), "
        "generates a Lakeview dashboard JSON on the metric views (bar, line, area, pie, scatter, "
        "counter, table widgets, slicers as filter widgets) with a per-visual coverage report. "
        "Calls the Databricks Lakeview REST API to create or update the dashboard and optionally "
        "publishes it. Returns the dashboard URL. "
        "Final step in the CI/CD dashboard pipeline."
    )
    args_schema: Type[BaseModel] = DatabricksDashboardCreatorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    _ALLOWED_HOST_SUFFIXES = (
        ".cloud.databricks.com",
        ".azuredatabricks.net",
        ".gcp.databricks.com",
        ".databricks.azure.cn",
        ".databricksapps.com",
    )

    def __init__(self, **kwargs: Any) -> None:
        config_keys = (
            "visual_mappings_json",
            "dashboard_title",
            "catalog",
            "schema_name",
            "warehouse_id",
            "databricks_host",
            "parent_path",
            "publish_dashboard",
            "dataset_mode",
        )
        # Seed visual_mappings_json so flow injection check fires
        default_config: dict = {"visual_mappings_json": None}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _authenticate(
        self, host_override: Optional[str] = None
    ) -> Optional["AuthContext"]:
        """Obtain an AuthContext synchronously (OBO → PAT → SPN).

        ``host_override`` (the ``databricks_host`` input) may only re-spell the
        credential's own workspace; any other host is refused (audit N1/F3a).
        Kept as a method so tests can patch it.
        """
        from src.services.tools.databricks_tool_utils import resolve_tool_auth

        return resolve_tool_auth(host_override)

    # ──────────────────────────────────────────────────────────────────────────
    # Visual mappings parsing
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _fetch_latest_mapper_output_from_db() -> str:
        """Fetch the latest PBI Visual-UCMV Mapper output from execution traces.

        Same DB-fallback pattern as the UCMV validator / Genie config generator:
        covers resumed flows and runs where flow-state injection did not deliver
        the mapper output. Returns the raw JSON string ('' if none found).
        Scoped to the run's workspace; it used to read any tenant's (audit V3-1).
        """
        from src.services.tools.tool_session_provider import ToolSessionProvider

        group_ids = ToolSessionProvider.run_group_ids()

        async def _query() -> str:
            async with ToolSessionProvider.session() as session:
                from src.services.trace.service import ExecutionTraceService

                raw = await ExecutionTraceService(
                    session
                ).latest_output_for_span_prefix(
                    "PBI Visual-UCMV Mapper", group_ids=group_ids
                )
                if not raw:
                    return ""
                data = json.loads(raw)
                content = data.get("content", "")
                if isinstance(content, str) and "visual_mappings" in content:
                    return content
                return ""

        from src.services.tools.async_bridge import run_sync_with_context
        from src.utils.asyncio_utils import create_and_run_loop

        found: str = run_sync_with_context(
            lambda: create_and_run_loop(_query()), timeout=15
        )
        return found

    def _parse_visual_mappings(self, raw: Any) -> tuple[list, str, str, str]:
        """Parse visual_mappings_json — may be a string with the full tool 94 output.

        Returns (visual_mappings, dashboard_title, catalog, schema_name).
        """
        if raw is None:
            return [], "", "", ""
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            return [], "", "", ""

        if isinstance(data, list):
            return data, "", "", ""
        if isinstance(data, dict):
            # Tool 94 output has key "visual_mappings"
            mappings = data.get("visual_mappings", [])
            if not isinstance(mappings, list):
                mappings = []
            title = data.get("dashboard_title", "")
            catalog = data.get("catalog", "")
            schema = data.get("schema_name", "")
            return mappings, title, catalog, schema
        return [], "", "", ""

    # ──────────────────────────────────────────────────────────────────────────
    # Lakeview dashboard JSON builder
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _safe_name(s: str) -> str:
        """Make a string safe for widget/dataset names (alphanumeric + underscores only)."""
        return safe_name(s)

    @staticmethod
    def _widget_type_for_visual(visual_type: str) -> tuple[str, int]:
        """Return (widgetType, version) for a given PBI visual type (filters and
        decorative visuals report the table they would fall back to)."""
        rule = rule_for(visual_type)
        return (rule.widget, rule.version) if rule.widget else ("table", 2)

    @staticmethod
    def _parse_slicers(raw: Any) -> list:
        """Slicers resolved by tool 94 (``slicers``), when present."""
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except (ValueError, TypeError):
            return []
        slicers = data.get("slicers") if isinstance(data, dict) else None
        return (
            [s for s in slicers if isinstance(s, dict)]
            if isinstance(slicers, list)
            else []
        )

    @staticmethod
    def _parse_skipped(raw: Any) -> list:
        """Visuals tool 94 did not map (decorative, no fields), for the coverage report."""
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except (ValueError, TypeError):
            return []
        skipped = data.get("skipped_visuals") if isinstance(data, dict) else None
        return (
            [s for s in skipped if isinstance(s, dict)]
            if isinstance(skipped, list)
            else []
        )

    def _build_lakeview_json(
        self,
        visual_mappings: list,
        warehouse_id: str,
        slicers: Optional[list] = None,
        dataset_mode: str = "metric_view",
    ) -> dict:
        """Build the full Lakeview dashboard serialized_dashboard dict."""
        dashboard, _ = build_dashboard(
            visual_mappings, slicers, dataset_mode=dataset_mode
        )
        return dashboard

    # ──────────────────────────────────────────────────────────────────────────
    # API calls
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _api_request(
        method: str,
        url: str,
        headers: dict,
        payload: Optional[dict] = None,
        timeout: int = 60,
    ) -> tuple[int, dict]:
        """Make an HTTP request and return (status_code, json_body)."""
        import requests as _requests

        resp = _requests.request(  # host allow-listed in _run before any call — nosemgrep: tmp.databricks.ssrf.requests-user-url
            method, url, json=payload, headers=headers, timeout=timeout
        )
        try:
            body = resp.json()
        except Exception:
            body = {"raw": resp.text[:500]}
        return resp.status_code, body

    def _create_or_update_dashboard(
        self,
        workspace_url: str,
        headers: dict,
        display_name: str,
        serialized_dashboard_str: str,
        warehouse_id: str,
        parent_path: str,
    ) -> tuple[str, str, str]:
        """Create or update the dashboard.

        Returns (dashboard_id, url, status).
        """
        # List existing dashboards to check for same name
        list_url = f"{workspace_url}/api/2.0/lakeview/dashboards"
        list_status, list_body = self._api_request("GET", list_url, headers)
        existing_id: Optional[str] = None
        if list_status == 200:
            items = list_body.get("dashboards", list_body.get("results", []))
            for item in items:
                if item.get("display_name") == display_name:
                    existing_id = item.get("dashboard_id") or item.get("id")
                    break

        payload = {
            "display_name": display_name,
            "serialized_dashboard": serialized_dashboard_str,
            "warehouse_id": warehouse_id,
            "parent_path": parent_path,
        }

        if existing_id:
            # Lakeview update is PATCH (PUT returns 404 ENDPOINT_NOT_FOUND);
            # parent_path is create-only — sending it on update is rejected.
            update_url = f"{workspace_url}/api/2.0/lakeview/dashboards/{existing_id}"
            update_payload = {k: v for k, v in payload.items() if k != "parent_path"}
            status_code, body = self._api_request(
                "PATCH", update_url, headers, update_payload
            )
            op = "updated"
        else:
            status_code, body = self._api_request("POST", list_url, headers, payload)
            op = "created"

        if status_code not in (200, 201):
            raise RuntimeError(
                f"Dashboard API returned HTTP {status_code}: {json.dumps(body)[:300]}"
            )

        dashboard_id = (
            body.get("dashboard_id") or body.get("id") or existing_id or "unknown"
        )
        dashboard_url = f"{workspace_url}/sql/dashboardsv3/{dashboard_id}"
        return dashboard_id, dashboard_url, op

    def _publish_dashboard(
        self,
        workspace_url: str,
        headers: dict,
        dashboard_id: str,
        warehouse_id: str,
    ) -> bool:
        """Publish the dashboard. Returns True on success."""
        pub_url = (
            f"{workspace_url}/api/2.0/lakeview/dashboards/{dashboard_id}/published"
        )
        payload = {
            "warehouse_id": warehouse_id,
            "embed_credentials": True,
        }
        # Lakeview publish is POST (PUT returns 404 ENDPOINT_NOT_FOUND)
        status_code, _ = self._api_request("POST", pub_url, headers, payload)
        return status_code in (200, 201)

    # ──────────────────────────────────────────────────────────────────────────
    # Main _run
    # ──────────────────────────────────────────────────────────────────────────

    def _run(self, **kwargs: Any) -> str:
        def _get(key: str) -> Any:
            val = kwargs.get(key)
            if val is not None:
                return val
            return self._default_config.get(key)

        host_override = _get("databricks_host") or None
        parent_path = _get("parent_path") or "/Workspace/Shared"
        warehouse_id = _get("warehouse_id") or ""
        publish = _get("publish_dashboard")
        if publish is None:
            publish = True

        # ── 1. Parse visual_mappings_json ────────────────────────────────────
        raw = _get("visual_mappings_json")
        if not raw:
            # DB fallback (same pattern as mapper/Genie/validator): pull the
            # latest PBI Visual-UCMV Mapper output from the execution traces —
            # covers resumed flows and runs where flow-state injection did not
            # deliver the mapper output to this tool.
            try:
                latest = self._fetch_latest_mapper_output_from_db()
                if latest:
                    logger.info(
                        "[DashboardCreator] visual_mappings_json not injected — using latest mapper output from DB"
                    )
                    raw = latest
            except Exception as e:
                logger.warning(
                    f"[DashboardCreator] DB fallback for visual_mappings_json failed: {e}"
                )
        if not raw:
            return json.dumps(
                {
                    "error": "No visual_mappings_json available — run the PBI Visual-UCMV Mapper first (flow injection or a prior run in this workspace)"
                }
            )

        visual_mappings, injected_title, injected_catalog, injected_schema = (
            self._parse_visual_mappings(raw)
        )
        if not visual_mappings:
            return json.dumps(
                {"error": "No visual_mappings found in visual_mappings_json"}
            )

        dashboard_title = _get("dashboard_title") or injected_title or "PBI Dashboard"
        catalog = _get("catalog") or injected_catalog or "main"
        schema = _get("schema_name") or injected_schema or "default"

        logger.info(
            f"[DashboardCreator] Building dashboard '{dashboard_title}' with {len(visual_mappings)} visuals"
        )

        # Datasets are built from the metric views, so a mapping needs a view, not SQL
        if not any(isinstance(m, dict) and m.get("ucmv_view") for m in visual_mappings):
            return json.dumps(
                {
                    "error": "No visual is mapped to a metric view — cannot create dashboard"
                }
            )

        # ── 2. Authenticate ──────────────────────────────────────────────────
        try:
            auth = self._authenticate(host_override=host_override)
            if not auth:
                return json.dumps({"error": "Authentication failed"})
            workspace_url = (auth.workspace_url or "").rstrip("/")
            headers = auth.get_headers()
            from src.utils.telemetry import KasalProduct, get_user_agent_header

            headers.update(get_user_agent_header(KasalProduct.DASHBOARD))
        except Exception as e:
            return json.dumps({"error": f"Authentication error: {e}"})

        if not workspace_url:
            return json.dumps({"error": "No Databricks workspace URL configured"})

        # ── 3. SSRF check ────────────────────────────────────────────────────
        parsed_host = urllib.parse.urlparse(workspace_url)
        if not parsed_host.hostname or not any(
            parsed_host.hostname.endswith(s) for s in self._ALLOWED_HOST_SUFFIXES
        ):
            return json.dumps(
                {"error": f"Untrusted Databricks host: {parsed_host.hostname}"}
            )

        # ── 4. Build Lakeview JSON ────────────────────────────────────────────
        dataset_mode = str(_get("dataset_mode") or "metric_view")
        lakeview_dict, outcomes = build_dashboard(
            [m for m in visual_mappings if isinstance(m, dict)],
            self._parse_slicers(raw),
            dataset_mode=dataset_mode,
        )
        for skipped in self._parse_skipped(raw):
            outcomes.append(
                {
                    "visual_id": str(skipped.get("visual_id") or ""),
                    "page": str(skipped.get("page_name") or "Default"),
                    "visual_type": str(skipped.get("visual_type") or ""),
                    "status": str(skipped.get("status") or "unmapped"),
                    "widget": "",
                    "note": str(skipped.get("note") or ""),
                }
            )
        serialized_dashboard_str = json.dumps(lakeview_dict)

        page_count, widget_count, dataset_count = dashboard_counts(lakeview_dict)

        logger.info(
            f"[DashboardCreator] Lakeview JSON: {page_count} pages, "
            f"{widget_count} widgets, {dataset_count} datasets"
        )

        # ── 5. Create/update dashboard via API ────────────────────────────────
        try:
            dashboard_id, dashboard_url, op = self._create_or_update_dashboard(
                workspace_url=workspace_url,
                headers=headers,
                display_name=dashboard_title,
                serialized_dashboard_str=serialized_dashboard_str,
                warehouse_id=warehouse_id,
                parent_path=parent_path,
            )
            logger.info(f"[DashboardCreator] Dashboard {op}: {dashboard_id}")
        except Exception as e:
            return json.dumps({"error": f"Dashboard API call failed: {e}"})

        # ── 6. Optionally publish ─────────────────────────────────────────────
        published = False
        if publish and warehouse_id:
            try:
                published = self._publish_dashboard(
                    workspace_url, headers, dashboard_id, warehouse_id
                )
                logger.info(f"[DashboardCreator] Published: {published}")
            except Exception as e:
                logger.warning(f"[DashboardCreator] Publish failed (non-fatal): {e}")

        return json.dumps(
            {
                "dashboard_id": dashboard_id,
                "dashboard_url": dashboard_url,
                "display_name": dashboard_title,
                "status": op,
                "published": published,
                "widget_count": widget_count,
                "page_count": page_count,
                "dataset_count": dataset_count,
                "dataset_mode": dataset_mode,
                "visual_coverage": coverage(outcomes),
                "visual_outcomes": outcomes,
                "catalog": catalog,
                "schema_name": schema,
                # CI/CD export — download a ZIP of YAML configuration files
                "cicd_type": "lakeflow_dashboard",
                "cicd_name": dashboard_title,
                "cicd_download_url": f"/api/analytics-export/dashboards/{dashboard_id}/download",
            },
            indent=2,
        )
