"""
Analytics Export Service

Generates downloadable CI/CD YAML bundles for:
  - Databricks Genie Spaces  → downloads/spaces/<name>/
  - Lakeview Dashboards      → downloads/dashboards/<name>/

Each bundle is a ZIP archive with human-readable YAML files that can be
version-controlled and used as input to a customer's own deployment engine.

YAML bundle layouts
-------------------
Genie Space:
    config.yaml        – space metadata + deployment config
    tables.yaml        – metric views + plain tables registered in the space
    instructions.yaml  – text instructions, join specs, SQL snippets
    questions.yaml     – sample questions + example SQL queries

Lakeview Dashboard:
    config.yaml        – dashboard metadata + deployment config
    datasets.yaml      – dataset SQL queries powering the widgets
    pages.yaml         – page layout with widget specs (human-readable)
"""
import json
import logging
import re
from typing import Any, Dict, List, Optional

import yaml

from src.repositories.dashboard_repository import DashboardRepository
from src.repositories.genie_repository import GenieRepository
from src.schemas.analytics_export import ExportFile
from src.schemas.genie import GenieAuthConfig

logger = logging.getLogger(__name__)


def _safe_folder_name(s: str) -> str:
    """Convert a display name to a safe lowercase folder name."""
    safe = re.sub(r"[^a-zA-Z0-9_\-]", "_", s).strip("_").lower()
    return safe[:80] or "export"


def _yaml_dump(data: Any, header_comment: str = "") -> str:
    """Dump data to a nicely formatted YAML string with optional comment header."""

    class _LiteralStr(str):
        pass

    def _literal_representer(dumper: Any, data: Any) -> Any:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")

    class _Dumper(yaml.Dumper):
        pass

    _Dumper.add_representer(_LiteralStr, _literal_representer)

    def _str_representer(dumper: Any, s: str) -> Any:
        # Multi-line strings → literal block scalar (|)
        style = "|" if "\n" in s else None
        return dumper.represent_scalar("tag:yaml.org,2002:str", s, style=style)

    _Dumper.add_representer(str, _str_representer)

    body = yaml.dump(
        data,
        Dumper=_Dumper,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        indent=2,
        width=120,
    )
    return (header_comment + "\n\n" if header_comment else "") + body


# Text-list fields: the Genie API stores these as list[str] even for a single
# item. We join them into a scalar string for human editing — matching the
# reference implementation in downloads/genie/_yaml_io.py.
_TEXT_LIST_FIELDS = frozenset({
    "content", "sql", "question", "usage_guidance", "instruction", "description",
})


def _join_text_lists(data: Any) -> Any:
    """Recursively join list[str] values for text-list fields into scalar strings.

    Mirrors _yaml_io.join_text_lists from the reference implementation.
    Multi-line results will be rendered as | block scalars by _yaml_dump.
    """
    if isinstance(data, dict):
        result: Dict[str, Any] = {}
        for k, v in data.items():
            if (
                k in _TEXT_LIST_FIELDS
                and isinstance(v, list)
                and v
                and all(isinstance(s, str) for s in v)
            ):
                joined = "\n".join(
                    s.replace("\r\n", "\n").replace("\r", "\n").rstrip() for s in v
                ).rstrip()
                result[k] = joined
            else:
                result[k] = _join_text_lists(v)
        return result
    if isinstance(data, list):
        return [_join_text_lists(item) for item in data]
    return data


# ──────────────────────────────────────────────────────────────────────────────
# Genie Space → YAML  (mirrors downloads/genie/extract_genie_space.py)
# ──────────────────────────────────────────────────────────────────────────────

def _genie_space_to_files(space_data: Dict[str, Any]) -> List[ExportFile]:
    """Convert the raw Genie space API response to a list of YAML ExportFiles.

    Output structure mirrors the reference implementation:
      header.yaml        — space_id, title, description, warehouse_id
      config.yaml        — sample questions
      data_sources.yaml  — tables / metric-view references
      instructions.yaml  — text instructions, join specs, SQL snippets, example SQLs
      benchmarks.yaml    — benchmark Q&A pairs (only if present)
    """
    # Parse serialized_space (JSON string or embedded dict)
    serialized_raw = space_data.get("serialized_space") or "{}"
    if isinstance(serialized_raw, str):
        try:
            spec: Dict[str, Any] = json.loads(serialized_raw)
        except (json.JSONDecodeError, ValueError):
            spec = {}
    else:
        spec = serialized_raw or {}

    space_id = space_data.get("space_id") or space_data.get("id") or ""
    title = space_data.get("title") or space_data.get("display_name") or "Genie Space"
    description = space_data.get("description") or ""
    warehouse_id = space_data.get("warehouse_id") or ""

    files: List[ExportFile] = []

    # ── header.yaml ──────────────────────────────────────────────────────────
    header_data: Dict[str, Any] = {
        "space_id": space_id,
        "title": title,
        "description": description,
        "warehouse_id": warehouse_id,
    }
    files.append(ExportFile(
        path="header.yaml",
        content=_yaml_dump(
            header_data,
            "# Genie Space header — generated by Kasal\n"
            "# Redeploy:\n"
            "#   POST  /api/2.0/genie/spaces            (create)\n"
            "#   PATCH /api/2.0/genie/spaces/{space_id} (update)"
        ),
    ))

    # ── config.yaml — sample questions ───────────────────────────────────────
    config_section = spec.get("config")
    if config_section is not None:
        files.append(ExportFile(
            path="config.yaml",
            content=_yaml_dump(
                _join_text_lists(config_section),
                "# Sample questions shown in the Genie UI\n"
                "# Re-deploy by including under the 'config' key in serialized_space."
            ),
        ))

    # ── data_sources.yaml — tables & metric views ─────────────────────────────
    data_sources_section = spec.get("data_sources")
    if data_sources_section is not None:
        files.append(ExportFile(
            path="data_sources.yaml",
            content=_yaml_dump(
                _join_text_lists(data_sources_section),
                "# Tables and Metric Views registered in this Genie Space\n"
                "# Add or remove identifiers, then redeploy."
            ),
        ))

    # ── instructions.yaml — text instructions, join specs, SQL snippets ───────
    instructions_section = spec.get("instructions")
    if instructions_section is not None:
        files.append(ExportFile(
            path="instructions.yaml",
            content=_yaml_dump(
                _join_text_lists(instructions_section),
                "# Genie Space instructions — text instructions, join specs,\n"
                "# SQL snippets (expressions/measures/filters), example Q&A SQLs."
            ),
        ))

    # ── benchmarks.yaml — benchmark Q&A pairs (optional) ─────────────────────
    benchmarks_section = spec.get("benchmarks")
    if benchmarks_section is not None:
        files.append(ExportFile(
            path="benchmarks.yaml",
            content=_yaml_dump(
                _join_text_lists(benchmarks_section),
                "# Benchmark questions for evaluating Genie answer quality."
            ),
        ))

    # If serialized_space was absent (no EDIT permission), at least return header
    if len(files) == 1:
        files.append(ExportFile(
            path="config.yaml",
            content=_yaml_dump(
                {"sample_questions": []},
                "# No serialized_space returned — ensure the token has EDIT permission.\n"
                "# Re-run after granting EDIT access to populate all YAML files."
            ),
        ))
        files.append(ExportFile(
            path="data_sources.yaml",
            content=_yaml_dump({"tables": [], "metric_views": []}, ""),
        ))
        files.append(ExportFile(
            path="instructions.yaml",
            content=_yaml_dump(
                {"text_instructions": [], "join_specs": [], "sql_snippets": {}},
                ""
            ),
        ))

    return files


# ──────────────────────────────────────────────────────────────────────────────
# Lakeview Dashboard → YAML
# ──────────────────────────────────────────────────────────────────────────────

def _widget_layout_to_yaml_dict(layout_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a single Lakeview widget layout entry to a clean, human-readable dict.

    Handles: text, counter, bar, line, pie, table, filter-multi-select,
             filter-single-select, filter-date-range-picker.
    """
    widget: Dict[str, Any] = layout_item.get("widget") or {}
    position: Dict[str, Any] = layout_item.get("position") or {}

    widget_name: str = widget.get("name") or "unnamed"
    pos_dict = {
        "x": position.get("x", 0),
        "y": position.get("y", 0),
        "width": position.get("width", 3),
        "height": position.get("height", 4),
    }

    spec: Dict[str, Any] = widget.get("spec") or {}
    widget_type: str = spec.get("widgetType") or ""
    queries: List[Dict[str, Any]] = widget.get("queries") or []

    # ── Text widget ──────────────────────────────────────────────────────────
    if not widget_type and widget.get("multilineTextboxSpec"):
        lines: List[str] = widget["multilineTextboxSpec"].get("lines") or []
        return {
            "type": "text",
            "name": widget_name,
            "position": pos_dict,
            "content": "".join(lines),
        }

    # ── Filter widgets ────────────────────────────────────────────────────────
    if widget_type.startswith("filter-"):
        frame = spec.get("frame") or widget.get("frame") or {}
        title = frame.get("title") or widget_name

        encodings: Dict[str, Any] = spec.get("encodings") or {}
        fields_enc: List[Dict[str, Any]] = encodings.get("fields") or []

        dataset_name = ""
        field_name = ""
        display_name = ""
        if queries:
            q = (queries[0].get("query") or {})
            dataset_name = q.get("datasetName") or ""
        if fields_enc:
            field_name = fields_enc[0].get("fieldName") or ""
            display_name = fields_enc[0].get("displayName") or field_name

        return {
            "type": widget_type,
            "name": widget_name,
            "position": pos_dict,
            "title": title,
            "dataset": dataset_name,
            "field": field_name,
            "displayName": display_name,
        }

    # ── Common helpers for data widgets ──────────────────────────────────────
    frame: Dict[str, Any] = spec.get("frame") or widget.get("frame") or {}
    title: str = frame.get("title") or widget_name
    encodings: Dict[str, Any] = spec.get("encodings") or {}

    dataset_name = ""
    raw_fields: List[Dict[str, Any]] = []
    if queries:
        q = queries[0].get("query") or {}
        dataset_name = q.get("datasetName") or ""
        raw_fields = q.get("fields") or []

    fields_out: List[Dict[str, Any]] = [
        {"name": f.get("name", ""), "expression": f.get("expression", "")}
        for f in raw_fields
    ]

    # ── Counter ───────────────────────────────────────────────────────────────
    if widget_type == "counter":
        value_enc: Dict[str, Any] = encodings.get("value") or {}
        return {
            "type": "counter",
            "name": widget_name,
            "position": pos_dict,
            "title": title,
            "dataset": dataset_name,
            "value_field": value_enc.get("fieldName") or "",
            "fields": fields_out,
        }

    # ── Table ─────────────────────────────────────────────────────────────────
    if widget_type == "table":
        columns_enc: List[Dict[str, Any]] = encodings.get("columns") or []
        columns_out = [
            {"field": c.get("fieldName", ""), "displayName": c.get("displayName", "")}
            for c in columns_enc
        ]
        return {
            "type": "table",
            "name": widget_name,
            "position": pos_dict,
            "title": title,
            "dataset": dataset_name,
            "columns": columns_out,
            "fields": fields_out,
        }

    # ── Bar / Line / Pie charts ───────────────────────────────────────────────
    if widget_type in ("bar", "line", "pie"):
        x_enc: Dict[str, Any] = encodings.get("x") or {}
        y_enc: Dict[str, Any] = encodings.get("y") or {}
        color_enc: Optional[Dict[str, Any]] = encodings.get("color")

        out: Dict[str, Any] = {
            "type": widget_type,
            "name": widget_name,
            "position": pos_dict,
            "title": title,
            "dataset": dataset_name,
            "x": {
                "field": x_enc.get("fieldName") or "",
                "type": (x_enc.get("scale") or {}).get("type") or "categorical",
            },
            "y": {
                "field": y_enc.get("fieldName") or "",
                "type": (y_enc.get("scale") or {}).get("type") or "quantitative",
            },
            "fields": fields_out,
        }
        if color_enc:
            out["color"] = {"field": color_enc.get("fieldName") or ""}
        return out

    # ── Fallback ───────────────────────────────────────────────────────────────
    return {
        "type": widget_type or "unknown",
        "name": widget_name,
        "position": pos_dict,
        "title": title,
        "dataset": dataset_name,
        "fields": fields_out,
        "raw_spec": spec,
    }


def _dashboard_to_files(dashboard_data: Dict[str, Any]) -> List[ExportFile]:
    """Convert the raw Lakeview dashboard API response to a list of YAML ExportFiles."""

    dashboard_id: str = dashboard_data.get("dashboard_id") or ""
    display_name: str = dashboard_data.get("display_name") or "Dashboard"
    warehouse_id: str = dashboard_data.get("warehouse_id") or ""
    parent_path: str = dashboard_data.get("parent_path") or "/Workspace/Shared"

    # Parse the serialized_dashboard JSON string
    serialized_raw = dashboard_data.get("serialized_dashboard") or "{}"
    if isinstance(serialized_raw, str):
        try:
            spec: Dict[str, Any] = json.loads(serialized_raw)
        except (json.JSONDecodeError, ValueError):
            spec = {}
    else:
        spec = serialized_raw or {}

    pages_raw: List[Dict[str, Any]] = spec.get("pages") or []
    datasets_raw: List[Dict[str, Any]] = spec.get("datasets") or []

    # ── config.yaml ──────────────────────────────────────────────────────────
    config_data: Dict[str, Any] = {
        "apiVersion": "lakeview/v1",
        "kind": "Dashboard",
        "metadata": {
            "name": _safe_folder_name(display_name),
            "dashboard_id": dashboard_id,
        },
        "spec": {
            "title": display_name,
            "warehouse_id": warehouse_id,
            "parent_path": parent_path,
            "publish": True,
        },
    }

    config_yaml = (
        "# Lakeview Dashboard CI/CD Configuration — generated by Kasal\n"
        "# Redeploy with the Databricks Lakeview REST API:\n"
        "#   POST /api/2.0/lakeview/dashboards   (create)\n"
        "#   PATCH /api/2.0/lakeview/dashboards/{id}  (update)\n"
        "#   POST  /api/2.0/lakeview/dashboards/{id}/published  (publish)\n\n"
    ) + _yaml_dump(config_data)

    # ── datasets.yaml ─────────────────────────────────────────────────────────
    datasets_out: List[Dict[str, Any]] = []
    for ds in datasets_raw:
        # Lakeview API stores SQL as either "query" (string) or "queryLines" (list[str])
        query = (
            ds.get("query")
            or "\n".join(ds.get("queryLines") or [])
            or ""
        )
        ds_entry: Dict[str, Any] = {
            "name": ds.get("name") or "",
            "displayName": ds.get("displayName") or ds.get("display_name") or "",
            "query": query,
        }
        datasets_out.append(ds_entry)

    datasets_yaml = (
        "# Dashboard Dataset Queries\n"
        "# Each dataset is a SQL query that powers one or more widgets.\n"
        "# For UCMV-backed dashboards use MEASURE() syntax:\n"
        "#   MEASURE(metric_name)  — aggregated by the widget encodings\n\n"
        "datasets:\n"
    )
    # Emit datasets with literal block scalars for multi-line SQL
    for ds in datasets_out:
        datasets_yaml += f"  - name: {ds['name']}\n"
        if ds["displayName"]:
            datasets_yaml += f"    displayName: {json.dumps(ds['displayName'])}\n"
        sql = ds["query"].strip()
        if "\n" in sql:
            # Literal block scalar keeps SQL indentation
            indented = "\n".join("      " + line for line in sql.splitlines())
            datasets_yaml += f"    query: |\n{indented}\n"
        else:
            datasets_yaml += f"    query: {json.dumps(sql)}\n"
        datasets_yaml += "\n"

    # ── pages.yaml ────────────────────────────────────────────────────────────
    pages_out: List[Dict[str, Any]] = []
    for page in pages_raw:
        page_name: str = page.get("name") or ""
        page_display: str = page.get("displayName") or page.get("display_name") or page_name
        layout: List[Dict[str, Any]] = page.get("layout") or []

        widgets_out: List[Dict[str, Any]] = [
            _widget_layout_to_yaml_dict(item) for item in layout
        ]

        pages_out.append({
            "name": page_name,
            "displayName": page_display,
            "widgets": widgets_out,
        })

    pages_yaml = (
        "# Dashboard Pages and Widget Layout\n"
        "# Grid: 6 columns wide. Each row must sum to width=6 (no gaps).\n"
        "#\n"
        "# Widget types: text | counter | bar | line | pie | table\n"
        "#               filter-multi-select | filter-single-select | filter-date-range-picker\n"
        "#\n"
        "# Widget field naming rules:\n"
        "#   dimension: {name: 'col',        expression: '`col`'}\n"
        "#   measure:   {name: 'sum(col)',    expression: 'SUM(`col`)'}\n"
        "#   x/y/value encodings must use the field 'name' exactly\n\n"
    ) + _yaml_dump({"pages": pages_out})

    return [
        ExportFile(path="config.yaml", content=config_yaml),
        ExportFile(path="datasets.yaml", content=datasets_yaml),
        ExportFile(path="pages.yaml", content=pages_yaml),
    ]


# ──────────────────────────────────────────────────────────────────────────────
# Public service
# ──────────────────────────────────────────────────────────────────────────────

class AnalyticsExportService:
    """
    Service that orchestrates fetching resources from Databricks
    and converting them to CI/CD-ready YAML bundles.
    """

    def __init__(self, user_token: Optional[str] = None):
        """
        Args:
            user_token: Optional OBO user token forwarded from the HTTP request.
        """
        self._user_token = user_token

    async def export_genie_space(
        self,
        space_id: str,
        serialized_space: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Convert a Genie space to a YAML bundle.

        The Databricks GET /api/2.0/genie/spaces/{id} endpoint does NOT return
        serialized_space (instructions, join specs, SQL snippets, questions).
        Pass serialized_space explicitly (from the tool output stored in the
        execution result) to get fully-populated YAML files.

        Returns:
            {space_id, space_name, folder_name, files: List[ExportFile]}
        """
        # Build space_dict from what we know
        if serialized_space:
            # Best path: full config provided by the tool at creation time.
            # No Databricks API call needed for the content — only try to enrich
            # with basic metadata (title, warehouse_id) with a short timeout.
            space_dict: Dict[str, Any] = {
                "space_id": space_id,
                "serialized_space": serialized_space,
            }
            try:
                import asyncio as _asyncio
                raw = await _asyncio.wait_for(
                    self._fetch_raw_genie_space(space_id), timeout=8.0
                )
                space_dict.update({k: v for k, v in raw.items() if k != "serialized_space"})
            except Exception:
                # Metadata enrichment is optional — proceed with what we have.
                pass
        else:
            # Fallback: fetch from API with ?include_serialized_space=true.
            # Requires EDIT permission. If auth hangs, re-run the crew to get
            # cicd_serialized_space in the tool output (POST endpoint is faster).
            space_dict = await self._fetch_raw_genie_space(space_id)

        title = space_dict.get("title") or space_dict.get("name") or space_id
        folder_name = _safe_folder_name(title)
        files = _genie_space_to_files(space_dict)

        return {
            "space_id": space_id,
            "space_name": title,
            "folder_name": folder_name,
            "files": files,
        }

    async def _fetch_raw_genie_space(self, space_id: str) -> Dict[str, Any]:
        """Direct HTTP fetch of a Genie space including its serialized_space config.

        Uses ?include_serialized_space=true — requires EDIT permission on the space.
        Avoids get_auth_context() to prevent the slow SPN token-refresh path;
        instead uses the same safe auth strategy as DashboardRepository.
        """
        import httpx
        import os as _os
        from src.utils.databricks_auth import _databricks_auth

        # ── workspace URL (already loaded by _load_config on app startup) ─────
        workspace_url: Optional[str] = None
        try:
            if _databricks_auth._workspace_host:
                workspace_url = _databricks_auth._workspace_host.rstrip("/")
        except Exception:
            pass
        if not workspace_url:
            raise RuntimeError("No Databricks workspace URL configured")

        # ── PAT: OBO → env var → DB lookup ────────────────────────────────────
        # Check env var before DB to avoid slow connection if DB is unreachable.
        token: Optional[str] = self._user_token
        if not token:
            for env_key in ("DATABRICKS_TOKEN", "DATABRICKS_API_KEY"):
                token = _os.environ.get(env_key)
                if token:
                    break
        if not token:
            try:
                from src.services.api_keys_service import ApiKeysService
                from src.db.session import async_session_factory
                from src.utils.user_context import UserContext
                group_id: Optional[str] = None
                try:
                    ctx = UserContext.get_group_context()
                    if ctx and hasattr(ctx, "primary_group_id"):
                        group_id = ctx.primary_group_id
                except Exception:
                    pass
                if group_id:
                    async with async_session_factory() as session:
                        svc = ApiKeysService(session, group_id=group_id)
                        for key_name in ("DATABRICKS_TOKEN", "DATABRICKS_API_KEY"):
                            api_key = await svc.find_by_name(key_name)
                            if api_key and api_key.encrypted_value:
                                from src.utils.encryption_utils import EncryptionUtils
                                pat = EncryptionUtils.decrypt_value(api_key.encrypted_value)
                                if pat:
                                    token = pat
                                    break
            except Exception:
                pass

        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        url = f"{workspace_url}/api/2.0/genie/spaces/{space_id}"

        async with httpx.AsyncClient(timeout=30.0) as client:
            # include_serialized_space=true returns the full space config
            # (text instructions, join specs, SQL snippets, questions).
            # Requires at least EDIT permission on the space.
            resp = await client.get(
                url,
                headers=headers,
                params={"include_serialized_space": "true"},
            )
            resp.raise_for_status()
            return resp.json()

    async def export_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """
        Fetch a Lakeview dashboard from Databricks and convert it to a YAML bundle.

        Returns:
            {
              "dashboard_id": str,
              "dashboard_name": str,
              "folder_name": str,
              "files": List[ExportFile],
            }
        Raises ValueError when not found, httpx.HTTPStatusError on API errors.
        """
        repo = DashboardRepository(user_token=self._user_token)
        dashboard_data = await repo.get_dashboard(dashboard_id)
        await repo.close()

        if not dashboard_data:
            raise ValueError(f"Dashboard {dashboard_id!r} not found")

        display_name = dashboard_data.get("display_name") or dashboard_id
        folder_name = _safe_folder_name(display_name)
        files = _dashboard_to_files(dashboard_data)

        return {
            "dashboard_id": dashboard_id,
            "dashboard_name": display_name,
            "folder_name": folder_name,
            "files": files,
        }

    async def list_dashboards(self, page_size: int = 50) -> List[Dict[str, Any]]:
        """Return a list of all accessible Lakeview dashboards (summary only)."""
        repo = DashboardRepository(user_token=self._user_token)
        result = await repo.list_dashboards(page_size=page_size)
        await repo.close()
        return result
