"""
Schemas for task tracking and status management.
"""
from enum import Enum
from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class TaskStatusEnum(str, Enum):
    """Task status constants"""
    RUNNING = "running"
    COMPLETED = "completed" 
    FAILED = "failed"

class TaskStatusBase(BaseModel):
    """Base model for task status"""
    job_id: str
    task_id: str
    status: TaskStatusEnum
    agent_name: Optional[str] = None

class TaskStatusCreate(TaskStatusBase):
    """Model for creating a task status"""
    pass

class TaskStatusUpdate(BaseModel):
    """Model for updating task status"""
    status: TaskStatusEnum

class TaskStatusResponse(TaskStatusBase):
    """Response model for task status"""
    id: int
    started_at: datetime
    completed_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class TaskCallbackMetadata(BaseModel):
    """Metadata for task callbacks"""
    callback_name: Optional[str] = None
    retry_count: Optional[int] = 0
    error: Optional[str] = None
    
class TaskErrorTrace(BaseModel):
    """Model for task error traces"""
    run_id: int
    task_key: str
    error_type: str
    error_message: str
    timestamp: datetime
    error_metadata: Optional[Dict[str, Any]] = None

class TaskStatusSchema(BaseModel):
    """Schema for task status information within a job execution"""
    id: int = Field(..., description="Unique identifier for the task status record")
    task_id: str = Field(..., description="Task identifier")
    status: str = Field(..., description="Current status of the task")
    agent_name: Optional[str] = Field(None, description="Name of the agent handling the task")
    started_at: datetime = Field(..., description="Timestamp when the task started")
    completed_at: Optional[datetime] = Field(None, description="Timestamp when the task completed, if applicable")

    class Config:
        from_attributes = True


class JobExecutionStatusResponse(BaseModel):
    """Schema for job execution status response including task statuses"""
    job_id: str = Field(..., description="Job identifier")
    status: str = Field(..., description="Current status of the job execution")
    tasks: List[TaskStatusSchema] = Field(..., description="List of tasks and their statuses") 