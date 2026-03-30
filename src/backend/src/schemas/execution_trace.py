"""
Schemas for execution trace operations.

This module provides Pydantic models for validating and structuring
data related to execution traces.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict

class ExecutionTraceItem(BaseModel):
    """Schema for an execution trace entry."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    run_id: Optional[int] = None
    job_id: Optional[str] = None
    created_at: Optional[datetime] = None
    event_source: Optional[str] = None
    event_context: Optional[str] = None
    event_type: Optional[str] = None
    output: Optional[Any] = None  # Accept any type of output (dict or string)
    trace_metadata: Optional[Dict[str, Any]] = None  # Metadata including task_id
    # OTel span hierarchy fields
    span_id: Optional[str] = None
    trace_id: Optional[str] = None
    parent_span_id: Optional[str] = None
    # OTel-native fields
    span_name: Optional[str] = None
    status_code: Optional[str] = None
    duration_ms: Optional[int] = None
    
class ExecutionTraceList(BaseModel):
    """Schema for a paginated list of execution traces."""
    
    traces: List[ExecutionTraceItem]
    total: int = Field(description="Total number of traces")
    limit: int = Field(description="Maximum number of items per page")
    offset: int = Field(description="Offset for pagination")
    
class ExecutionTraceResponseByRunId(BaseModel):
    """Schema for a list of traces for a specific run."""
    
    run_id: int = Field(description="Database ID of the execution")
    traces: List[ExecutionTraceItem]
    
class ExecutionTraceResponseByJobId(BaseModel):
    """Schema for a list of traces for a specific job."""
    
    job_id: str = Field(description="String ID of the execution (job_id)")
    traces: List[ExecutionTraceItem]
    
class DeleteTraceResponse(BaseModel):
    """Schema for a response to a delete trace operation."""
    
    message: str = Field(description="Success message")
    deleted_trace_id: Optional[int] = Field(None, description="ID of the deleted trace (if deleting by ID)")
    deleted_traces: Optional[int] = Field(None, description="Number of deleted traces") 