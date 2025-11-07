"""
Schemas for Flow execution models and responses.
"""
from enum import Enum
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field, ConfigDict


class FlowExecutionStatus(str, Enum):
    """Flow execution status values"""
    PENDING = "pending"
    PREPARING = "preparing"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class FlowExecutionBase(BaseModel):
    """Base model for flow execution data"""
    flow_id: Union[UUID, str]
    job_id: str
    status: FlowExecutionStatus = FlowExecutionStatus.PENDING
    config: Optional[Dict[str, Any]] = Field(default_factory=dict)


class FlowExecutionCreate(FlowExecutionBase):
    """Model for creating a new flow execution"""
    pass


class FlowExecutionUpdate(BaseModel):
    """Model for updating an existing flow execution"""
    status: Optional[FlowExecutionStatus] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class FlowExecutionResponse(FlowExecutionBase):
    """Response model for flow execution data"""
    id: int
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class FlowNodeExecutionBase(BaseModel):
    """Base model for flow node execution data"""
    flow_execution_id: int
    node_id: str
    status: FlowExecutionStatus = FlowExecutionStatus.PENDING
    agent_id: Optional[int] = None
    task_id: Optional[int] = None


class FlowNodeExecutionCreate(FlowNodeExecutionBase):
    """Model for creating a new flow node execution"""
    pass


class FlowNodeExecutionUpdate(BaseModel):
    """Model for updating an existing flow node execution"""
    status: Optional[FlowExecutionStatus] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class FlowNodeExecutionResponse(FlowNodeExecutionBase):
    """Response model for flow node execution data"""
    id: int
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class FlowExecutionDetailResponse(FlowExecutionResponse):
    """Detailed response model for flow execution including node executions"""
    nodes: List[FlowNodeExecutionResponse] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True) 