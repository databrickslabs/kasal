"""Databricks Dashboard Creator Tool for CrewAI.

Takes visual-to-UCMV mappings from the PBI Visual-UCMV Mapper (tool 94) and creates
a Databricks AI/BI (Lakeview) dashboard via the REST API.

Visual types → widget types:
  barChart   → bar   (version 3)
  lineChart  → line  (version 3)
  tableEx    → table (version 2)
  matrix     → table (version 2)
  card       → counter (version 2)
  kpiVisual  → counter (version 2)
  others     → table (version 2)
"""
import json
import logging
import re
import urllib.parse
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)


class DatabricksDashboardCreatorSchema(BaseModel):
    """Input schema for DatabricksDashboardCreatorTool."""
    visual_mappings_json: Optional[str] = Field(
        None,
        description="JSON from tool 94 containing visual_mappings array (auto-injected from flow)"
    )
    dashboard_title: Optional[str] = Field(None, description="Dashboard display name")
    catalog: Optional[str] = Field(None, description="UC catalog")
    schema_name: Optional[str] = Field(None, description="UC schema")
    warehouse_id: Optional[str] = Field(None, description="SQL warehouse for dashboard queries")
    databricks_host: Optional[str] = Field(None, description="Override workspace URL (optional)")
    parent_path: Optional[str] = Field(
        None,
        description="Workspace folder path (default: /Workspace/Shared)"
    )
    publish_dashboard: Optional[bool] = Field(
        None,
        description="Whether to publish after creation (default: True)"
    )


class DatabricksDashboardCreatorTool(BaseTool):
    """Create Databricks AI/BI (Lakeview) dashboards from PBI visual-to-UCMV mappings."""

    name: str = "Databricks Dashboard Creator"
    description: str = (
        "Creates Databricks AI/BI (Lakeview) dashboards from visual-to-UCMV mappings. "
        "Takes the structured visual mappings from the PBI Visual-UCMV Mapper (tool 94), "
        "generates a Lakeview dashboard JSON with correct widget types (bar, line, table, counter), "
        "datasets with MEASURE() SQL queries, and page layouts. "
        "Calls the Databricks Lakeview REST API to create or update the dashboard and optionally "
        "publishes it. Returns the dashboard URL. "
        "Final step in the CI/CD dashboard pipeline."
    )
    args_schema: Type[BaseModel] = DatabricksDashboardCreatorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    _ALLOWED_HOST_SUFFIXES = (
        '.cloud.databricks.com', '.azuredatabricks.net', '.gcp.databricks.com',
        '.databricks.azure.cn', '.databricksapps.com',
    )

    def __init__(self, **kwargs: Any) -> None:
        config_keys = (
            'visual_mappings_json', 'dashboard_title', 'catalog', 'schema_name',
            'warehouse_id', 'databricks_host', 'parent_path', 'publish_dashboard',
        )
        # Seed visual_mappings_json so flow injection check fires
        default_config: dict = {'visual_mappings_json': None}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _authenticate(self, host_override: Optional[str] = None):
        """Obtain AuthContext synchronously."""
        import asyncio
        import concurrent.futures
        from src.utils.databricks_auth import get_auth_context

        def _run_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(get_auth_context())
            finally:
                loop.close()
                asyncio.set_event_loop(None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            auth = executor.submit(_run_in_thread).result(timeout=30)

        if auth is not None and host_override:
            url = host_override.strip().rstrip('/')
            if not url.startswith('https://'):
                url = f'https://{url}'
            auth.workspace_url = url
        return auth

    # ──────────────────────────────────────────────────────────────────────────
    # Visual mappings parsing
    # ──────────────────────────────────────────────────────────────────────────

    def _parse_visual_mappings(self, raw: Any) -> tuple[list, str, str, str]:
        """Parse visual_mappings_json — may be a string with the full tool 94 output.

        Returns (visual_mappings, dashboard_title, catalog, schema_name).
        """
        if raw is None:
            return [], '', '', ''
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            return [], '', '', ''

        if isinstance(data, list):
            return data, '', '', ''
        if isinstance(data, dict):
            # Tool 94 output has key "visual_mappings"
            mappings = data.get('visual_mappings', [])
            if not isinstance(mappings, list):
                mappings = []
            title = data.get('dashboard_title', '')
            catalog = data.get('catalog', '')
            schema = data.get('schema_name', '')
            return mappings, title, catalog, schema
        return [], '', '', ''

    # ──────────────────────────────────────────────────────────────────────────
    # Lakeview dashboard JSON builder
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _safe_name(s: str) -> str:
        """Make a string safe for widget/dataset names (alphanumeric + underscores only)."""
        return re.sub(r'[^a-zA-Z0-9_]', '_', s).strip('_')[:60] or 'unnamed'

    @staticmethod
    def _widget_type_for_visual(visual_type: str) -> tuple[str, int]:
        """Return (widgetType, version) for a given PBI visual type."""
        mapping = {
            'barChart': ('bar', 3),
            'lineChart': ('line', 3),
            'tableEx': ('table', 2),
            'matrix': ('table', 2),
            'card': ('counter', 2),
            'cardVisual': ('counter', 2),
            'kpiVisual': ('counter', 2),
        }
        return mapping.get(visual_type, ('table', 2))

    def _build_widget(
        self,
        mapping: dict,
        dataset_name: str,
        position: dict,
    ) -> dict:
        """Build a single Lakeview widget from a visual mapping."""
        visual_type = mapping.get('visual_type', 'tableEx')
        chart_title = mapping.get('chart_title') or f"{mapping.get('page_name', '')} - {visual_type}"
        widget_type, version = self._widget_type_for_visual(visual_type)

        visual_id = mapping.get('visual_id', '')
        page_name = mapping.get('page_name', '')
        widget_name = self._safe_name(f"w_{visual_id or page_name}_{widget_type}")

        dimensions = mapping.get('dimensions') or []
        measures = mapping.get('measures') or []

        # Build query fields — match your CCHBC script conventions:
        # dimensions: {"name": "col",        "expression": "`col`"}
        # measures:   {"name": "sum(col)",   "expression": "SUM(`col`)"}
        fields = []
        for dim in dimensions:
            fields.append({"name": dim, "expression": f"`{dim}`"})
        for m in measures:
            fields.append({"name": f"sum({m})", "expression": f"SUM(`{m}`)"})

        query = {
            "name": "main_query",
            "query": {
                "datasetName": dataset_name,   # camelCase — Lakeview API requirement
                "fields": fields,
                "disaggregated": False,        # widget handles aggregation
            }
        }

        # Build widget spec based on type
        if widget_type in ('bar', 'line') and version == 3:
            # Need at least one dim and one measure
            x_field = dimensions[0] if dimensions else (fields[0]['name'] if fields else 'dim')
            y_field = f"sum({measures[0]})" if measures else (fields[-1]['name'] if fields else 'value')
            spec = {
                "version": version,
                "widgetType": widget_type,
                "encodings": {
                    "x": {"fieldName": x_field, "scale": {"type": "categorical"}},
                    "y": {"fieldName": y_field, "scale": {"type": "quantitative"}},
                }
            }
            if len(dimensions) > 1 and version == 3:
                color_field = dimensions[1]
                spec["encodings"]["color"] = {"fieldName": color_field, "scale": {"type": "categorical"}}
        elif widget_type == 'table' and version == 2:
            columns = []
            for f in fields:
                columns.append({"fieldName": f["name"], "displayName": f["name"].replace('_', ' ').title()})
            spec = {
                "version": version,
                "widgetType": widget_type,
                "encodings": {
                    "columns": columns
                },
                "frame": {"showTitle": True, "title": chart_title},
            }
        elif widget_type == 'counter' and version == 2:
            value_field = f"sum({measures[0]})" if measures else (fields[0]['name'] if fields else 'value')
            spec = {
                "version": version,
                "widgetType": widget_type,
                "encodings": {
                    "value": {
                        "fieldName": value_field,
                        "displayName": (measures[0] if measures else 'Value').replace('_', ' ').title(),
                        "aggregation": "SUM",
                    }
                },
                "disaggregated": True,
            }
        else:
            # Generic table fallback
            columns = [{"fieldName": f["name"], "displayName": f["name"].replace('_', ' ').title()} for f in fields]
            spec = {
                "version": 2,
                "widgetType": "table",
                "encodings": {"columns": columns},
                "frame": {"showTitle": True, "title": chart_title},
            }

        # Add frame title to bar/line specs (table/counter already have it in spec)
        if widget_type in ('bar', 'line'):
            spec["frame"] = {"title": chart_title, "showTitle": True}

        widget_obj = {
            "name": widget_name,
            "queries": [query],
            "spec": spec,               # always required — fixes "missing spec" API error
            "frame": {"title": chart_title, "showTitle": True},
        }

        return {
            "widget": widget_obj,
            "position": position,
        }

    def _build_dataset(self, view_name: str, sql: Optional[str], dataset_name: str) -> dict:
        """Build a Lakeview dataset entry."""
        query_sql = sql or f"SELECT * FROM {view_name} LIMIT 1000"
        display = view_name.split('.')[-1].replace('_', ' ').title()
        return {
            "name": dataset_name,
            "displayName": display,        # required by Lakeview API
            "query": query_sql,
            "type": "DBSQL",
        }

    def _build_lakeview_json(
        self,
        visual_mappings: list,
        warehouse_id: str,
    ) -> dict:
        """Build the full Lakeview dashboard serialized_dashboard dict."""
        # Group visuals by page
        pages_map: dict[str, list] = {}
        for m in visual_mappings:
            page = m.get('page_name') or 'Default'
            pages_map.setdefault(page, []).append(m)

        # Build datasets: one per unique UCMV view
        datasets = []
        view_to_dataset: dict[str, str] = {}
        view_to_sql: dict[str, Optional[str]] = {}
        for m in visual_mappings:
            view = m.get('ucmv_view')
            sql = m.get('sql')
            if view and view not in view_to_dataset:
                ds_name = self._safe_name(f"ds_{view.split('.')[-1]}")
                # Ensure uniqueness
                base = ds_name
                suffix = 1
                existing = {d['name'] for d in datasets}
                while ds_name in existing:
                    ds_name = f"{base}_{suffix}"
                    suffix += 1
                view_to_dataset[view] = ds_name
                view_to_sql[view] = sql
                datasets.append(self._build_dataset(view, sql, ds_name))

        # Build pages
        pages = []
        for page_display_name, page_visuals in pages_map.items():
            page_id = self._safe_name(f"page_{page_display_name}")
            layout = []
            col = 0
            row = 0
            max_col = 6

            for mapping in page_visuals:
                view = mapping.get('ucmv_view')
                if not view:
                    continue
                dataset_name = view_to_dataset.get(view, 'ds_unknown')

                vtype = mapping.get('visual_type', 'tableEx')
                widget_type, _ = self._widget_type_for_visual(vtype)

                # Sizing by widget type
                if widget_type == 'counter':
                    width = 2
                    height = 3
                elif widget_type == 'table':
                    width = 6
                    height = 5
                    col = 0  # Tables always start at left edge
                else:
                    width = 3
                    height = 5

                if col + width > max_col:
                    col = 0
                    row += 5 if col == 0 else height

                position = {"x": col, "y": row, "width": width, "height": height}
                widget_layout = self._build_widget(mapping, dataset_name, position)
                layout.append(widget_layout)

                col += width
                if col >= max_col:
                    col = 0
                    row += height

            pages.append({
                "name": page_id,
                "displayName": page_display_name,   # camelCase required by Lakeview API
                "page_type": "PAGE_TYPE_CANVAS",
                "layout": layout,
            })

        return {
            "pages": pages,
            "datasets": datasets,
        }

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
        resp = _requests.request(method, url, json=payload, headers=headers, timeout=timeout)
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
        list_status, list_body = self._api_request('GET', list_url, headers)
        existing_id: Optional[str] = None
        if list_status == 200:
            items = list_body.get('dashboards', list_body.get('results', []))
            for item in items:
                if item.get('display_name') == display_name:
                    existing_id = item.get('dashboard_id') or item.get('id')
                    break

        payload = {
            "display_name": display_name,
            "serialized_dashboard": serialized_dashboard_str,
            "warehouse_id": warehouse_id,
            "parent_path": parent_path,
        }

        if existing_id:
            update_url = f"{workspace_url}/api/2.0/lakeview/dashboards/{existing_id}"
            status_code, body = self._api_request('PUT', update_url, headers, payload)
            op = 'updated'
        else:
            status_code, body = self._api_request('POST', list_url, headers, payload)
            op = 'created'

        if status_code not in (200, 201):
            raise RuntimeError(
                f"Dashboard API returned HTTP {status_code}: {json.dumps(body)[:300]}"
            )

        dashboard_id = (
            body.get('dashboard_id')
            or body.get('id')
            or existing_id
            or 'unknown'
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
        pub_url = f"{workspace_url}/api/2.0/lakeview/dashboards/{dashboard_id}/published"
        payload = {
            "warehouse_id": warehouse_id,
            "embed_credentials": True,
        }
        status_code, _ = self._api_request('PUT', pub_url, headers, payload)
        return status_code in (200, 201)

    # ──────────────────────────────────────────────────────────────────────────
    # Main _run
    # ──────────────────────────────────────────────────────────────────────────

    def _run(self, **kwargs: Any) -> str:  # noqa: C901
        def _get(key):
            val = kwargs.get(key)
            if val is not None:
                return val
            return self._default_config.get(key)

        host_override = _get('databricks_host') or None
        parent_path = _get('parent_path') or '/Workspace/Shared'
        warehouse_id = _get('warehouse_id') or ''
        publish = _get('publish_dashboard')
        if publish is None:
            publish = True

        # ── 1. Parse visual_mappings_json ────────────────────────────────────
        raw = _get('visual_mappings_json')
        if not raw:
            return json.dumps({"error": "No visual_mappings_json available — required to create dashboard"})

        visual_mappings, injected_title, injected_catalog, injected_schema = self._parse_visual_mappings(raw)
        if not visual_mappings:
            return json.dumps({"error": "No visual_mappings found in visual_mappings_json"})

        dashboard_title = _get('dashboard_title') or injected_title or 'PBI Dashboard'
        catalog = _get('catalog') or injected_catalog or 'main'
        schema = _get('schema_name') or injected_schema or 'default'

        logger.info(f"[DashboardCreator] Building dashboard '{dashboard_title}' with {len(visual_mappings)} visuals")

        # Filter out unmapped visuals (no ucmv_view)
        mapped = [m for m in visual_mappings if m.get('ucmv_view') and m.get('sql')]
        if not mapped:
            return json.dumps({"error": "No mapped visuals with SQL found — cannot create dashboard"})

        # ── 2. Authenticate ──────────────────────────────────────────────────
        try:
            auth = self._authenticate(host_override=host_override)
            if not auth:
                return json.dumps({"error": "Authentication failed"})
            workspace_url = (auth.workspace_url or '').rstrip('/')
            headers = auth.get_headers()
        except Exception as e:
            return json.dumps({"error": f"Authentication error: {e}"})

        if not workspace_url:
            return json.dumps({"error": "No Databricks workspace URL configured"})

        # ── 3. SSRF check ────────────────────────────────────────────────────
        parsed_host = urllib.parse.urlparse(workspace_url)
        if not parsed_host.hostname or not any(
            parsed_host.hostname.endswith(s) for s in self._ALLOWED_HOST_SUFFIXES
        ):
            return json.dumps({
                "error": f"Untrusted Databricks host: {parsed_host.hostname}"
            })

        # ── 4. Build Lakeview JSON ────────────────────────────────────────────
        lakeview_dict = self._build_lakeview_json(mapped, warehouse_id)
        serialized_dashboard_str = json.dumps(lakeview_dict)

        widget_count = sum(len(p.get('layout', [])) for p in lakeview_dict.get('pages', []))
        page_count = len(lakeview_dict.get('pages', []))
        dataset_count = len(lakeview_dict.get('datasets', []))

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
                published = self._publish_dashboard(workspace_url, headers, dashboard_id, warehouse_id)
                logger.info(f"[DashboardCreator] Published: {published}")
            except Exception as e:
                logger.warning(f"[DashboardCreator] Publish failed (non-fatal): {e}")

        return json.dumps({
            "dashboard_id": dashboard_id,
            "dashboard_url": dashboard_url,
            "display_name": dashboard_title,
            "status": op,
            "published": published,
            "widget_count": widget_count,
            "page_count": page_count,
            "dataset_count": dataset_count,
            "catalog": catalog,
            "schema_name": schema,
            # CI/CD export — download a ZIP of YAML configuration files
            "cicd_type": "lakeflow_dashboard",
            "cicd_name": dashboard_title,
            "cicd_download_url": f"/api/analytics-export/dashboards/{dashboard_id}/download",
        }, indent=2)
