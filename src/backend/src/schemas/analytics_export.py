"""
Pydantic schemas for Genie Space and Lakeview Dashboard CI/CD export.
"""

from typing import Optional

from pydantic import BaseModel, Field


class ExportFile(BaseModel):
    """A single file in an export bundle."""

    path: str = Field(..., description="Relative file path within the ZIP archive")
    content: str = Field(..., description="UTF-8 file content")


class DashboardSummary(BaseModel):
    """Summary of a Lakeview dashboard for listing."""

    dashboard_id: str
    display_name: str
    warehouse_id: Optional[str] = None
    parent_path: Optional[str] = None
    lifecycle_state: Optional[str] = None


class GenieSpaceExportBody(BaseModel):
    """Request body for POST download — carries the serialized_space from the tool output."""

    serialized_space: Optional[str] = None
