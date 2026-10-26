"""
Schemas for Flow execution models and responses.
"""

from enum import Enum
from typing import Any, Dict, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field


class FlowExecutionStatus(str, Enum):
    """Flow execution status values"""

    PENDING = "pending"
    PREPARING = "preparing"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    WAITING_FOR_APPROVAL = "waiting_for_approval"  # HITL gate pause


class FlowExecutionBase(BaseModel):
    """Base model for flow execution data"""

    flow_id: Optional[Union[UUID, str]] = None  # Optional for ad-hoc executions
    job_id: str
    status: FlowExecutionStatus = FlowExecutionStatus.PENDING
    config: Optional[Dict[str, Any]] = Field(default_factory=dict)
    run_name: Optional[str] = None  # Descriptive name for the execution
    group_id: Optional[str] = None  # Multi-tenant isolation


class FlowNodeExecutionBase(BaseModel):
    """Base model for flow node execution data"""

    flow_execution_id: int
    node_id: str
    status: FlowExecutionStatus = FlowExecutionStatus.PENDING
    agent_id: Optional[int] = None
    task_id: Optional[int] = None
    group_id: Optional[str] = None  # Multi-tenant isolation


class FlowExecutionRequest(BaseModel):
    """Request model for flow execution"""

    flow_id: Union[str, int, UUID]
    job_id: str
    run_name: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    # Checkpoint resume fields
    resume_from_flow_uuid: Optional[str] = None  # CrewAI state.id to resume from
    resume_from_execution_id: Optional[int] = (
        None  # Execution ID of checkpoint to resume
    )
