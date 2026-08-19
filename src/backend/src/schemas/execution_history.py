"""
Schemas for execution history operations.

This module provides Pydantic models for validating and structuring
data related to execution history records and related data.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


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
    execution_type: Optional[str] = Field(
        default=None, description="Type of execution (crew or flow)"
    )
    harness: Optional[str] = Field(
        default=None,
        description=(
            "Which agent runtime ran this execution ('kasal' or 'crewai'). "
            "`from_attributes` reads it straight off the row it was stamped on."
        ),
    )
    result: Optional[Dict[str, Any]] = None
    group_email: Optional[str] = Field(
        None, description="Email of the user who submitted the execution"
    )

    # MLflow integration fields
    mlflow_trace_id: Optional[str] = Field(
        None, description="MLflow trace ID for evaluation linking"
    )
    mlflow_experiment_name: Optional[str] = Field(
        None, description="MLflow experiment name for reference"
    )
    mlflow_evaluation_run_id: Optional[str] = Field(
        None, description="MLflow evaluation run ID"
    )

    # Checkpoint/Persistence fields
    flow_uuid: Optional[str] = Field(
        None, description="CrewAI state.id for checkpoint persistence"
    )
    checkpoint_status: Optional[str] = Field(
        None, description="Checkpoint status: active, resumed, expired"
    )
    checkpoint_method: Optional[str] = Field(
        None, description="Last checkpointed method name"
    )


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

    success: bool = Field(
        default=True, description="Whether the delete operation was successful"
    )
    message: str = Field(description="Success message")
    deleted_run_id: Optional[int] = Field(
        None, description="ID of the deleted execution (if deleting by ID)"
    )
    deleted_job_id: Optional[str] = Field(
        None, description="Job ID of the deleted execution (if deleting by job_id)"
    )
    deleted_runs: Optional[int] = Field(
        None, description="Number of deleted executions (if deleting all)"
    )
    deleted_outputs: Optional[int] = Field(
        None, description="Number of deleted outputs"
    )


class CrewCheckpointInfo(BaseModel):
    """Schema for crew-level checkpoint information within a flow execution."""

    crew_name: str = Field(description="Name of the crew that completed")
    sequence: int = Field(description="Order in which the crew executed (1, 2, 3...)")
    status: str = Field(description="Status: completed or failed")
    output_preview: Optional[str] = Field(
        None, description="First 200 chars of crew output"
    )
    completed_at: datetime = Field(description="When the crew completed")


class CheckpointInfo(BaseModel):
    """Schema for checkpoint information."""

    model_config = ConfigDict(from_attributes=True)

    execution_id: int = Field(description="ID of the execution with the checkpoint")
    job_id: str = Field(description="Job ID of the execution")
    flow_uuid: str = Field(description="CrewAI state.id for resuming")
    checkpoint_method: Optional[str] = Field(
        None, description="Last checkpointed method name"
    )
    checkpoint_status: str = Field(description="Status: active, resumed, expired")
    created_at: datetime = Field(description="When the checkpoint was created")
    run_name: Optional[str] = Field(None, description="Name of the execution run")
    # Crew-level checkpoints for granular resume
    crew_checkpoints: List[CrewCheckpointInfo] = Field(
        default_factory=list, description="List of completed crews"
    )


class CheckpointListResponse(BaseModel):
    """Schema for a list of available checkpoints."""

    flow_id: Optional[str] = Field(
        None, description="Flow ID the checkpoints belong to"
    )
    checkpoints: List[CheckpointInfo] = Field(
        description="List of available checkpoints"
    )
    total: int = Field(description="Total number of checkpoints")


class CheckpointUnitInfo(BaseModel):
    """One completed unit of work inside a checkpoint.

    A unit is a TASK for a crew and a CREW for a flow — the same shape either
    way, which is what lets one UI serve both.
    """

    key: str = Field(description="Unit identity: task index, or crew sequence")
    name: Optional[str] = Field(None, description="Task or crew name")
    agent: Optional[str] = Field(None, description="Agent that produced the output")
    output_preview: Optional[str] = Field(
        None, description="First 200 chars of the output"
    )
    truncated: bool = Field(
        False,
        description=(
            "The stored output was capped. Present so a resume with reduced "
            "fidelity is visible rather than silent."
        ),
    )
    completed_at: Optional[str] = Field(None, description="ISO completion timestamp")
    will_restore: Optional[bool] = Field(
        None,
        description=(
            "Whether a resume would replay this unit rather than re-run it. "
            "THREE-valued: null means 'cannot tell' — a checkpoint written "
            "before content keys existed, or a run with no saved definition to "
            "compare against. Null is not 'yes'. Even a true here is a floor "
            "rather than a promise: it compares task TEXT only, and run time "
            "additionally re-runs a unit whose model or tools changed."
        ),
    )


class CheckpointUnitDetail(CheckpointUnitInfo):
    """One unit WITH its full output."""

    output_raw: str = Field("", description="The unit's full stored output")
    output_json: Optional[Dict[str, Any]] = Field(
        None, description="Structured output, when the unit produced one"
    )


class ExecutionCheckpointResponse(BaseModel):
    """An execution's checkpoint — crew or flow, same shape."""

    job_id: str = Field(description="Job ID of the execution")
    execution_id: int = Field(description="Database ID of the execution")
    kind: Optional[str] = Field(None, description="'crew' or 'flow'")
    version: Optional[int] = Field(None, description="Checkpoint schema version")
    status: Optional[str] = Field(
        None, description="Lifecycle status: active, resumed, expired"
    )
    execution_status: Optional[str] = Field(
        None, description="Status of the execution itself"
    )
    run_name: Optional[str] = Field(None, description="Name of the run")
    created_at: Optional[datetime] = Field(None, description="When the run started")
    unit_count: Optional[int] = Field(
        None, description="Total units the execution has, when known"
    )
    completed_count: int = Field(description="Units recorded as complete")
    truncated: bool = Field(False, description="Any unit's output was capped")
    derived: bool = Field(
        False,
        description=(
            "Reconstructed from a pre-unification payload rather than written "
            "by the recorder — fidelity is not guaranteed."
        ),
    )
    changed_from_index: Optional[int] = Field(
        None,
        description=(
            "Index of the earliest unit that changed since this run, or null "
            "when nothing detectable changed. A resume re-runs from here — "
            "including units that did not themselves change, because their "
            "input did."
        ),
    )
    restorable_count: int = Field(
        0,
        description=(
            "Units expected to be replayed rather than re-run. A FLOOR: it "
            "compares task text only, so the run may restore fewer."
        ),
    )
    resumable: bool = Field(description="Whether this checkpoint can be resumed now")
    blocked_reason: Optional[str] = Field(
        None, description="Why it cannot be resumed, when it cannot"
    )
    units: List[CheckpointUnitInfo] = Field(
        default_factory=list, description="Completed units, in execution order"
    )
    # Flow-only, absent for crews.
    flow_uuid: Optional[str] = Field(None, description="Flow state id, for flows")
    checkpoint_method: Optional[str] = Field(
        None, description="Last checkpointed flow method"
    )


class ExecutionCheckpointListResponse(BaseModel):
    """A list of checkpoints."""

    checkpoints: List[ExecutionCheckpointResponse] = Field(default_factory=list)
    total: int = Field(description="Number of checkpoints returned")


class ResumeExecutionRequest(BaseModel):
    """Optional body for POST /executions/{id}/resume."""

    from_unit: Optional[str] = Field(
        None,
        description=(
            "Unit key to resume AT — everything before it is restored, it and "
            "everything after re-runs. Omit to continue from the first "
            "incomplete unit."
        ),
    )


class ResumeFromCheckpointRequest(BaseModel):
    """Schema for requesting execution resume from checkpoint."""

    flow_uuid: Optional[str] = Field(None, description="CrewAI state.id to resume from")
    execution_id: Optional[int] = Field(
        None, description="Execution ID to resume from (alternative to flow_uuid)"
    )


class UpdateExecutionResultRequest(BaseModel):
    """Schema for updating an execution's result data."""

    result: Dict[str, Any] = Field(description="The updated result data to store")


class UpdateExecutionResultResponse(BaseModel):
    """Schema for the response after updating an execution result."""

    success: bool = Field(description="Whether the update was successful")
    job_id: str = Field(description="Job ID of the updated execution")
    updated_at: str = Field(description="ISO timestamp of the update")
