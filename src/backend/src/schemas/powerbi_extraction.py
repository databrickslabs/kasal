"""
Pydantic schemas for PowerBI extraction records.

Validates the raw extraction artifacts persisted per Pipeline Config Generator
run (see :class:`src.models.powerbi_extraction.PowerBIExtraction`).
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class PowerBIExtractionBase(BaseModel):
    """Shared fields for a PowerBI extraction record."""

    execution_id: Optional[str] = Field(None, description="Crew/flow execution id")
    workspace_id: Optional[str] = Field(None, description="Power BI workspace id")
    dataset_id: Optional[str] = Field(None, description="Power BI dataset id")
    report_id: Optional[str] = Field(None, description="Power BI report id, if used")
    summary: Optional[str] = Field(None, description="Human-readable one-line summary")
    group_id: Optional[str] = Field(None, description="Tenant/group id")
    created_by_email: Optional[str] = Field(None, description="Creator email")


class PowerBIExtractionCreate(PowerBIExtractionBase):
    """Payload for creating a PowerBI extraction record."""

    relationships: Optional[List[Dict[str, Any]]] = Field(
        None, description="Raw relationship rows (from/to table+column, cardinality)")
    measures: Optional[List[Dict[str, Any]]] = Field(
        None, description="Raw measures with DAX expressions")
    admin_tables: Optional[Dict[str, Any]] = Field(
        None, description="Admin/TMDL table metadata {table: {columns, mquery, measures}}")
    report_definition: Optional[Dict[str, Any]] = Field(
        None, description="Report visual bindings / definition")
    proposed_config: Optional[Dict[str, Any]] = Field(
        None, description="Derived pipeline_config")
    warnings: Optional[List[str]] = Field(None, description="Extraction warnings")

    relationships_count: Optional[int] = Field(None)
    measures_count: Optional[int] = Field(None)
    measures_with_dax_count: Optional[int] = Field(None)
    admin_tables_count: Optional[int] = Field(None)


class PowerBIExtractionResponse(PowerBIExtractionBase):
    """A PowerBI extraction record returned to a caller."""

    id: int
    relationships: Optional[List[Dict[str, Any]]] = None
    measures: Optional[List[Dict[str, Any]]] = None
    admin_tables: Optional[Dict[str, Any]] = None
    report_definition: Optional[Dict[str, Any]] = None
    proposed_config: Optional[Dict[str, Any]] = None
    warnings: Optional[List[str]] = None
    relationships_count: Optional[int] = None
    measures_count: Optional[int] = None
    measures_with_dax_count: Optional[int] = None
    admin_tables_count: Optional[int] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
