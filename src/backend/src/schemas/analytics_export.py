"""
Pydantic schemas for Genie Space and Lakeview Dashboard CI/CD export.
"""
from typing import List, Optional
from pydantic import BaseModel, Field


class ExportFile(BaseModel):
    """A single file in an export bundle."""
    path: str = Field(..., description="Relative file path within the ZIP archive")
    content: str = Field(..., description="UTF-8 file content")


class GenieSpaceExportResponse(BaseModel):
    """Result of a Genie Space export."""
    space_id: str
    space_name: str
    file_count: int
    files: List[ExportFile]
    download_url: str


class DashboardExportResponse(BaseModel):
    """Result of a Lakeview Dashboard export."""
    dashboard_id: str
    dashboard_name: str
    file_count: int
    files: List[ExportFile]
    download_url: str


class DashboardSummary(BaseModel):
    """Summary of a Lakeview dashboard for listing."""
    dashboard_id: str
    display_name: str
    warehouse_id: Optional[str] = None
    parent_path: Optional[str] = None
    lifecycle_state: Optional[str] = None
