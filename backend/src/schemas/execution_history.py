"""
Schemas for execution history operations.

This module provides Pydantic models for validating and structuring
data related to execution history records and related data.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict

class ExecutionHistoryItem(BaseModel):
    """Schema for an execution history item."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    job_id: str = Field(description="Unique string identifier for the execution")
    name: Optional[str] = None
    agents_yaml: Optional[str] = None
    tasks_yaml: Optional[str] = None
    model: Optional[str] = None
    status: Optional[str] = None
    error: Optional[str] = None
    created_at: datetime
    input: Optional[Dict[str, Any]] = None
    execution_type: Optional[str] = Field(default=None, description="Type of execution (crew or flow)")
    result: Optional[Dict[str, Any]] = None
    
class ExecutionHistoryList(BaseModel):
    """Schema for a paginated list of execution history items."""
    
    executions: List[ExecutionHistoryItem]
    total: int = Field(description="Total number of executions")
    limit: int = Field(description="Maximum number of items per page")
    offset: int = Field(description="Offset for pagination")
    
class ExecutionOutput(BaseModel):
    """Schema for an execution output entry."""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    job_id: str = Field(description="ID of the execution this output belongs to")
    task_name: Optional[str] = None
    agent_name: Optional[str] = None
    output: str = Field(description="The output content")
    timestamp: datetime = Field(description="When this output was generated")
    
class ExecutionOutputList(BaseModel):
    """Schema for a paginated list of execution outputs."""
    
    execution_id: str = Field(description="ID of the execution these outputs belong to")
    outputs: List[ExecutionOutput]
    total: int = Field(description="Total number of outputs for this execution")
    limit: int = Field(description="Maximum number of items per page")
    offset: int = Field(description="Offset for pagination")
    
class ExecutionOutputDebug(BaseModel):
    """Schema for debugging information about an execution output."""
    
    id: int
    timestamp: datetime
    task_name: Optional[str] = None
    agent_name: Optional[str] = None
    output_preview: Optional[str] = None
    
class ExecutionOutputDebugList(BaseModel):
    """Schema for a list of execution output debug information."""
    
    run_id: int = Field(description="Database ID of the execution")
    execution_id: str = Field(description="String ID of the execution")
    total_outputs: int = Field(description="Total number of outputs for this execution")
    outputs: List[ExecutionOutputDebug]
    
class DeleteResponse(BaseModel):
    """Schema for a response to a delete operation."""
    
    message: str = Field(description="Success message")
    deleted_run_id: Optional[int] = Field(None, description="ID of the deleted execution (if deleting by ID)")
    deleted_job_id: Optional[str] = Field(None, description="Job ID of the deleted execution (if deleting by job_id)")
    deleted_runs: Optional[int] = Field(None, description="Number of deleted executions (if deleting all)")
    deleted_outputs: Optional[int] = Field(None, description="Number of deleted outputs") 