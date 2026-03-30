from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict

class SchedulerJobBase(BaseModel):
    """Base schema for scheduler job"""
    name: str = Field(..., description="Name of the scheduler job")
    description: Optional[str] = Field(None, description="Description of the job")
    schedule: str = Field(..., description="Cron expression for job scheduling")
    enabled: bool = Field(True, description="Whether the job is enabled")
    job_data: Dict[str, Any] = Field(default_factory=dict, description="Job specific configuration data")


class SchedulerJobSchema(SchedulerJobBase):
    """Schema for scheduler job"""
    id: int = Field(..., description="Unique identifier for the job")
    created_at: datetime = Field(..., description="When the job was created")
    updated_at: datetime = Field(..., description="When the job was last updated")
    last_run_at: Optional[datetime] = Field(None, description="When the job was last run")
    next_run_at: Optional[datetime] = Field(None, description="When the job will run next")
    
    model_config = ConfigDict(from_attributes=True)


class SchedulerJobCreate(SchedulerJobBase):
    """Schema for creating a new scheduler job"""
    pass


class SchedulerJobUpdate(BaseModel):
    """Schema for updating an existing scheduler job"""
    name: Optional[str] = Field(None, description="Name of the scheduler job")
    description: Optional[str] = Field(None, description="Description of the job")
    schedule: Optional[str] = Field(None, description="Cron expression for job scheduling")
    enabled: Optional[bool] = Field(None, description="Whether the job is enabled")
    job_data: Optional[Dict[str, Any]] = Field(None, description="Job specific configuration data")


class SchedulerJobResponse(SchedulerJobSchema):
    """Schema for scheduler job response"""
    pass 