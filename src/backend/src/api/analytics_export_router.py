"""
Analytics Export Router

Provides CI/CD download endpoints for:
  - Genie Spaces   → GET /api/analytics-export/genie-spaces/{space_id}/download
  - AI/BI Dashboards → GET /api/analytics-export/dashboards/{dashboard_id}/download
  - Dashboard listing → GET /api/analytics-export/dashboards

Each download returns a ZIP archive containing human-readable YAML files:

  Genie Space ZIP:
    <space_name>/config.yaml
    <space_name>/tables.yaml
    <space_name>/instructions.yaml
    <space_name>/questions.yaml

  Dashboard ZIP:
    <dashboard_name>/config.yaml
    <dashboard_name>/datasets.yaml
    <dashboard_name>/pages.yaml

The YAML files are designed so that customers can build their own deployment
engine on top of them without needing to understand the raw Databricks API JSON.
"""
import io
import logging
import zipfile
from typing import List

from fastapi import APIRouter, Request, Query
from fastapi.responses import StreamingResponse, JSONResponse

from src.core.exceptions import NotFoundError
from src.core.dependencies import GroupContextDep
from src.schemas.analytics_export import DashboardSummary
from src.services.analytics_export_service import AnalyticsExportService
from src.utils.databricks_auth import extract_user_token_from_request
from src.utils.user_context import UserContext

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/analytics-export",
    tags=["analytics-export"],
)


def _make_zip(folder_name: str, files: list) -> bytes:
    """Pack a list of ExportFile objects into an in-memory ZIP archive."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            archive_path = f"{folder_name}/{f.path}"
            zf.writestr(archive_path, f.content.encode("utf-8"))
    buf.seek(0)
    return buf.read()


# ─────────────────────────────────────────────────────────────────────────────
# Genie Space export
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/genie-spaces/{space_id}/download",
    summary="Download Genie Space CI/CD YAML bundle",
    response_description="ZIP archive with YAML configuration files",
)
async def download_genie_space_export(
    space_id: str,
    request: Request,
    group_context: GroupContextDep = None,
) -> StreamingResponse:
    """
    Download a ZIP archive containing CI/CD YAML files for a Genie Space.

    The archive structure is:
    ```
    <space_name>/
      config.yaml        – space metadata and deployment config
      tables.yaml        – metric views and tables registered in the space
      instructions.yaml  – text instructions, join specs, SQL snippets
      questions.yaml     – sample questions and example SQL queries
    ```

    Customers can version-control these files and deploy them with any
    engine that calls the Databricks Genie REST API.
    """
    if group_context:
        UserContext.set_group_context(group_context)

    user_token = extract_user_token_from_request(request)
    service = AnalyticsExportService(user_token=user_token)

    try:
        result = await service.export_genie_space(space_id)
    except ValueError as exc:
        raise NotFoundError(str(exc))

    folder_name: str = result["folder_name"]
    files: list = result["files"]
    zip_bytes = _make_zip(folder_name, files)

    zip_name = f"{folder_name}_genie_space.zip"
    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_name}"'},
    )


@router.get(
    "/genie-spaces/{space_id}/preview",
    summary="Preview Genie Space CI/CD YAML files (JSON response)",
    response_description="JSON listing of YAML file paths and contents",
)
async def preview_genie_space_export(
    space_id: str,
    request: Request,
    group_context: GroupContextDep = None,
) -> JSONResponse:
    """
    Return the YAML file contents as JSON without downloading a ZIP.
    Useful for inspecting what will be exported before downloading.
    """
    if group_context:
        UserContext.set_group_context(group_context)

    user_token = extract_user_token_from_request(request)
    service = AnalyticsExportService(user_token=user_token)

    try:
        result = await service.export_genie_space(space_id)
    except ValueError as exc:
        raise NotFoundError(str(exc))

    return JSONResponse({
        "space_id": result["space_id"],
        "space_name": result["space_name"],
        "folder_name": result["folder_name"],
        "file_count": len(result["files"]),
        "files": [
            {"path": f"{result['folder_name']}/{f.path}", "content": f.content}
            for f in result["files"]
        ],
    })


# ─────────────────────────────────────────────────────────────────────────────
# Lakeview Dashboard export
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/dashboards",
    summary="List accessible Lakeview dashboards",
    response_model=List[DashboardSummary],
)
async def list_dashboards(
    request: Request,
    page_size: int = Query(50, ge=1, le=200, description="Max dashboards to return"),
    group_context: GroupContextDep = None,
) -> List[DashboardSummary]:
    """List all Lakeview (AI/BI) dashboards accessible with current credentials."""
    if group_context:
        UserContext.set_group_context(group_context)

    user_token = extract_user_token_from_request(request)
    service = AnalyticsExportService(user_token=user_token)
    raw_list = await service.list_dashboards(page_size=page_size)

    return [
        DashboardSummary(
            dashboard_id=d.get("dashboard_id") or d.get("id") or "",
            display_name=d.get("display_name") or "",
            warehouse_id=d.get("warehouse_id"),
            parent_path=d.get("parent_path"),
            lifecycle_state=d.get("lifecycle_state"),
        )
        for d in raw_list
    ]


@router.get(
    "/dashboards/{dashboard_id}/download",
    summary="Download Lakeview Dashboard CI/CD YAML bundle",
    response_description="ZIP archive with YAML configuration files",
)
async def download_dashboard_export(
    dashboard_id: str,
    request: Request,
    group_context: GroupContextDep = None,
) -> StreamingResponse:
    """
    Download a ZIP archive containing CI/CD YAML files for a Lakeview Dashboard.

    The archive structure is:
    ```
    <dashboard_name>/
      config.yaml    – dashboard metadata and deployment config
      datasets.yaml  – SQL queries for each dataset
      pages.yaml     – page layout with human-readable widget specs
    ```

    Together these three files contain everything needed to recreate the dashboard
    programmatically using the Databricks Lakeview REST API.

    **Deploying from YAML:**
    1. Rebuild the `serialized_dashboard` JSON from `datasets.yaml` + `pages.yaml`
    2. Call `POST /api/2.0/lakeview/dashboards` with `config.yaml` parameters
    3. Call `POST /api/2.0/lakeview/dashboards/{id}/published` to publish
    """
    if group_context:
        UserContext.set_group_context(group_context)

    user_token = extract_user_token_from_request(request)
    service = AnalyticsExportService(user_token=user_token)

    try:
        result = await service.export_dashboard(dashboard_id)
    except ValueError as exc:
        raise NotFoundError(str(exc))

    folder_name: str = result["folder_name"]
    files: list = result["files"]
    zip_bytes = _make_zip(folder_name, files)

    zip_name = f"{folder_name}_dashboard.zip"
    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_name}"'},
    )


@router.get(
    "/dashboards/{dashboard_id}/preview",
    summary="Preview Lakeview Dashboard CI/CD YAML files (JSON response)",
)
async def preview_dashboard_export(
    dashboard_id: str,
    request: Request,
    group_context: GroupContextDep = None,
) -> JSONResponse:
    """
    Return dashboard YAML file contents as JSON without downloading a ZIP.
    Useful for UI previews or programmatic consumption.
    """
    if group_context:
        UserContext.set_group_context(group_context)

    user_token = extract_user_token_from_request(request)
    service = AnalyticsExportService(user_token=user_token)

    try:
        result = await service.export_dashboard(dashboard_id)
    except ValueError as exc:
        raise NotFoundError(str(exc))

    return JSONResponse({
        "dashboard_id": result["dashboard_id"],
        "dashboard_name": result["dashboard_name"],
        "folder_name": result["folder_name"],
        "file_count": len(result["files"]),
        "files": [
            {"path": f"{result['folder_name']}/{f.path}", "content": f.content}
            for f in result["files"]
        ],
    })
