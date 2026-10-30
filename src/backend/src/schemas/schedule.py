from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.utils.cron_utils import load_timezone
from src.utils.model_config import DEFAULT_ENGINE_MODEL

TIMEZONE_DESCRIPTION = (
    "IANA timezone the cron line is read in, e.g. 'Europe/Berlin' (the "
    "browser's). Omitted or null: the server's clock (UTC on Databricks Apps)."
)


def _known_timezone(value: Optional[str]) -> Optional[str]:
    """``value`` if it names an IANA zone; blank is no zone."""
    if value is None or not value.strip():
        return None
    load_timezone(value.strip())  # ValueError -> 422 naming the bad zone
    return value.strip()


class ScheduleBase(BaseModel):
    """Base schema for schedule configuration supporting both crew and flow executions"""

    name: str = Field(..., description="Name of the scheduled job")
    cron_expression: str = Field(..., description="Cron expression for schedule timing")

    # Execution type (crew or flow)
    execution_type: str = Field(
        default="crew", description="Type of execution: 'crew' or 'flow'"
    )

    # Crew execution fields (required for crew, optional for flow)
    agents_yaml: Optional[Dict[str, Any]] = Field(
        None, description="Agent configuration in YAML format (for crew executions)"
    )
    tasks_yaml: Optional[Dict[str, Any]] = Field(
        None, description="Tasks configuration in YAML format (for crew executions)"
    )

    # Flow execution fields (required for flow, optional for crew)
    flow_id: Optional[UUID] = Field(
        None, description="ID of the saved flow (for flow executions)"
    )
    nodes: Optional[List[Dict[str, Any]]] = Field(
        None, description="Flow nodes configuration (for ad-hoc flow executions)"
    )
    edges: Optional[List[Dict[str, Any]]] = Field(
        None, description="Flow edges configuration (for ad-hoc flow executions)"
    )
    flow_config: Optional[Dict[str, Any]] = Field(
        None, description="Flow-specific configuration"
    )

    # Common fields
    inputs: Dict[str, Any] = Field(
        default_factory=dict, description="Input values for the job"
    )
    is_active: bool = Field(default=True, description="Whether the schedule is active")
    model: str = Field(
        default_factory=lambda: DEFAULT_ENGINE_MODEL,
        description=(
            'Model to use for the job. "auto" is accepted on purpose: it is '
            "resolved to one of the workspace's enabled models at each run, "
            "not when the schedule is saved."
        ),
    )
    timezone: Optional[str] = Field(None, description=TIMEZONE_DESCRIPTION)

    _check_timezone = field_validator("timezone")(_known_timezone)

    @model_validator(mode="after")
    def validate_execution_type_requirements(self) -> "ScheduleBase":
        """Validate that required fields are present based on execution type"""
        if self.execution_type == "crew":
            if not self.agents_yaml or not self.tasks_yaml:
                raise ValueError(
                    "agents_yaml and tasks_yaml are required for crew executions"
                )
        elif self.execution_type == "flow":
            # Flow execution requires either flow_id or nodes/edges
            if not self.flow_id and not (self.nodes and self.edges):
                raise ValueError(
                    "flow_id or nodes/edges are required for flow executions"
                )
        return self


class ScheduleCreate(ScheduleBase):
    """Schema for creating a new schedule"""

    pass


class ScheduleCreateFromExecution(BaseModel):
    """Schema for creating a schedule from an existing execution"""

    name: str = Field(..., description="Name of the scheduled job")
    cron_expression: str = Field(..., description="Cron expression for schedule timing")
    execution_id: Union[int, str] = Field(
        ...,
        description=(
            "The execution to use as template: its integer id, or its job id "
            "(UUID) — chat anchors messages by job id, the runs table by row id."
        ),
    )
    is_active: bool = Field(default=True, description="Whether the schedule is active")
    timezone: Optional[str] = Field(None, description=TIMEZONE_DESCRIPTION)

    _check_timezone = field_validator("timezone")(_known_timezone)


class ScheduleUpdate(ScheduleBase):
    """Schema for updating an existing schedule"""

    pass


class ScheduleResponse(BaseModel):
    """Schema for schedule responses - does not validate execution type requirements since data comes from DB"""

    id: int = Field(..., description="Unique identifier for the schedule")
    name: str = Field(..., description="Name of the scheduled job")
    cron_expression: str = Field(..., description="Cron expression for schedule timing")

    # Execution type
    execution_type: str = Field(
        default="crew", description="Type of execution: 'crew' or 'flow'"
    )

    # Crew execution fields
    agents_yaml: Optional[Dict[str, Any]] = Field(
        None, description="Agent configuration (for crew executions)"
    )
    tasks_yaml: Optional[Dict[str, Any]] = Field(
        None, description="Tasks configuration (for crew executions)"
    )

    # Flow execution fields
    flow_id: Optional[UUID] = Field(
        None, description="ID of the saved flow (for flow executions)"
    )
    nodes: Optional[List[Dict[str, Any]]] = Field(
        None, description="Flow nodes configuration"
    )
    edges: Optional[List[Dict[str, Any]]] = Field(
        None, description="Flow edges configuration"
    )
    flow_config: Optional[Dict[str, Any]] = Field(
        None, description="Flow-specific configuration"
    )
    crew_id: Optional[UUID] = Field(
        None,
        description=(
            "The saved crew each run is rebuilt from; agents_yaml/tasks_yaml "
            "are then a masked copy for display."
        ),
    )

    # Common fields
    inputs: Dict[str, Any] = Field(
        default_factory=dict, description="Input values for the job"
    )
    is_active: bool = Field(default=True, description="Whether the schedule is active")
    model: str = Field(
        default_factory=lambda: DEFAULT_ENGINE_MODEL,
        description="Model to use for the job",
    )
    timezone: Optional[str] = Field(None, description=TIMEZONE_DESCRIPTION)

    # Timestamps
    last_run_at: Optional[datetime] = Field(
        None, description="Timestamp of the last execution"
    )
    next_run_at: Optional[datetime] = Field(
        None, description="Timestamp of the next scheduled execution"
    )
    created_at: datetime = Field(
        ..., description="Timestamp when the schedule was created"
    )
    updated_at: datetime = Field(
        ..., description="Timestamp when the schedule was last updated"
    )

    model_config = ConfigDict(from_attributes=True)


class ScheduleListResponse(BaseModel):
    """Schema for list of schedules response"""

    schedules: List[ScheduleResponse] = Field(..., description="List of schedules")
    count: int = Field(..., description="Total number of schedules")


class ToggleResponse(ScheduleResponse):
    """Schema for toggle schedule response"""

    pass


class CrewConfig(BaseModel):
    """Configuration for a scheduled job (supports both crew and flow executions)"""

    # Execution type
    execution_type: str = Field(
        default="crew", description="Type of execution: 'crew' or 'flow'"
    )

    # Crew execution fields (optional for flows)
    agents_yaml: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Agent configuration in YAML format"
    )
    tasks_yaml: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Tasks configuration in YAML format"
    )

    # Flow execution fields (optional for crews)
    flow_id: Optional[UUID] = Field(
        None, description="ID of the saved flow (for flow executions)"
    )
    nodes: Optional[List[Dict[str, Any]]] = Field(
        None, description="Flow nodes configuration"
    )
    edges: Optional[List[Dict[str, Any]]] = Field(
        None, description="Flow edges configuration"
    )
    flow_config: Optional[Dict[str, Any]] = Field(
        None, description="Flow-specific configuration"
    )

    # Common fields
    inputs: Dict[str, Any] = Field(
        default_factory=dict, description="Input values for the job"
    )
    model: str = Field(
        default_factory=lambda: DEFAULT_ENGINE_MODEL,
        description="Model to use for the job",
    )
