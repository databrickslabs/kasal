from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict

class ScheduleBase(BaseModel):
    """Base schema for schedule configuration"""
    name: str = Field(..., description="Name of the scheduled job")
    cron_expression: str = Field(..., description="Cron expression for schedule timing")
    agents_yaml: Dict[str, Any] = Field(..., description="Agent configuration in YAML format")
    tasks_yaml: Dict[str, Any] = Field(..., description="Tasks configuration in YAML format")
    inputs: Dict[str, Any] = Field(default_factory=dict, description="Input values for the job")
    is_active: bool = Field(default=True, description="Whether the schedule is active")
    planning: bool = Field(default=False, description="Whether to use planning mode")
    model: str = Field(default="gpt-4o-mini", description="Model to use for the job")


class ScheduleCreate(ScheduleBase):
    """Schema for creating a new schedule"""
    pass


class ScheduleUpdate(ScheduleBase):
    """Schema for updating an existing schedule"""
    pass


class ScheduleResponse(ScheduleBase):
    """Schema for schedule responses"""
    id: int = Field(..., description="Unique identifier for the schedule")
    last_run_at: Optional[datetime] = Field(None, description="Timestamp of the last execution")
    next_run_at: Optional[datetime] = Field(None, description="Timestamp of the next scheduled execution")
    created_at: datetime = Field(..., description="Timestamp when the schedule was created")
    updated_at: datetime = Field(..., description="Timestamp when the schedule was last updated")

    model_config = ConfigDict(from_attributes=True)


class ScheduleListResponse(BaseModel):
    """Schema for list of schedules response"""
    schedules: List[ScheduleResponse] = Field(..., description="List of schedules")
    count: int = Field(..., description="Total number of schedules")


class ToggleResponse(ScheduleResponse):
    """Schema for toggle schedule response"""
    pass


class CrewConfig(BaseModel):
    """Configuration for a crew job"""
    agents_yaml: Dict[str, Any] = Field(..., description="Agent configuration in YAML format")
    tasks_yaml: Dict[str, Any] = Field(..., description="Tasks configuration in YAML format")
    inputs: Dict[str, Any] = Field(default_factory=dict, description="Input values for the job")
    planning: bool = Field(default=False, description="Whether to use planning mode")
    model: str = Field(default="gpt-4o-mini", description="Model to use for the job") 