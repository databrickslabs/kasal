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
    name: Optional[str] = Field(None, alias="run_name")
    agents_yaml: Optional[str] = None
    tasks_yaml: Optional[str] = None
    model: Optional[str] = None
    status: Optional[str] = None
    error: Optional[str] = None
    created_at: datetime
    input: Optional[Dict[str, Any]] = None
    execution_type: Optional[str] = Field(default=None, description="Type of execution (crew or flow)")
    result: Optional[Dict[str, Any]] = None
    group_email: Optional[str] = Field(None, description="Email of the user who submitted the execution")

    # MLflow integration fields
    mlflow_trace_id: Optional[str] = Field(None, description="MLflow trace ID for evaluation linking")
    mlflow_experiment_name: Optional[str] = Field(None, description="MLflow experiment name for reference")
    mlflow_evaluation_run_id: Optional[str] = Field(None, description="MLflow evaluation run ID")

    # Checkpoint/Persistence fields
    flow_uuid: Optional[str] = Field(None, description="CrewAI state.id for checkpoint persistence")
    checkpoint_status: Optional[str] = Field(None, description="Checkpoint status: active, resumed, expired")
    checkpoint_method: Optional[str] = Field(None, description="Last checkpointed method name")
    
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

    success: bool = Field(default=True, description="Whether the delete operation was successful")
    message: str = Field(description="Success message")
    deleted_run_id: Optional[int] = Field(None, description="ID of the deleted execution (if deleting by ID)")
    deleted_job_id: Optional[str] = Field(None, description="Job ID of the deleted execution (if deleting by job_id)")
    deleted_runs: Optional[int] = Field(None, description="Number of deleted executions (if deleting all)")
    deleted_outputs: Optional[int] = Field(None, description="Number of deleted outputs")


class CrewCheckpointInfo(BaseModel):
    """Schema for crew-level checkpoint information within a flow execution."""

    crew_name: str = Field(description="Name of the crew that completed")
    sequence: int = Field(description="Order in which the crew executed (1, 2, 3...)")
    status: str = Field(description="Status: completed or failed")
    output_preview: Optional[str] = Field(None, description="First 200 chars of crew output")
    completed_at: datetime = Field(description="When the crew completed")


class CheckpointInfo(BaseModel):
    """Schema for checkpoint information."""

    model_config = ConfigDict(from_attributes=True)

    execution_id: int = Field(description="ID of the execution with the checkpoint")
    job_id: str = Field(description="Job ID of the execution")
    flow_uuid: str = Field(description="CrewAI state.id for resuming")
    checkpoint_method: Optional[str] = Field(None, description="Last checkpointed method name")
    checkpoint_status: str = Field(description="Status: active, resumed, expired")
    created_at: datetime = Field(description="When the checkpoint was created")
    run_name: Optional[str] = Field(None, description="Name of the execution run")
    # Crew-level checkpoints for granular resume
    crew_checkpoints: List[CrewCheckpointInfo] = Field(default_factory=list, description="List of completed crews")


class CheckpointListResponse(BaseModel):
    """Schema for a list of available checkpoints."""

    flow_id: Optional[str] = Field(None, description="Flow ID the checkpoints belong to")
    checkpoints: List[CheckpointInfo] = Field(description="List of available checkpoints")
    total: int = Field(description="Total number of checkpoints")


class ResumeFromCheckpointRequest(BaseModel):
    """Schema for requesting execution resume from checkpoint."""

    flow_uuid: Optional[str] = Field(None, description="CrewAI state.id to resume from")
    execution_id: Optional[int] = Field(None, description="Execution ID to resume from (alternative to flow_uuid)") 