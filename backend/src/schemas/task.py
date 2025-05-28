from datetime import datetime
from typing import List, Optional, Dict, Any, Union

from pydantic import BaseModel, Field, ConfigDict


class ConditionConfig(BaseModel):
    """Schema for task condition configuration."""
    type: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    dependent_task: Optional[str] = None


class TaskConfig(BaseModel):
    """Schema for task configuration settings."""
    cache_response: Optional[bool] = None
    cache_ttl: Optional[int] = None
    retry_on_fail: Optional[bool] = None
    max_retries: Optional[int] = None
    timeout: Optional[int] = None
    priority: Optional[int] = None
    error_handling: Optional[str] = None
    output_file: Optional[str] = None
    output_json: Optional[str] = None
    output_pydantic: Optional[str] = None
    callback: Optional[str] = None
    human_input: Optional[bool] = None
    condition: Optional[ConditionConfig] = None
    guardrail: Optional[str] = None
    markdown: Optional[bool] = None


# Shared properties
class TaskBase(BaseModel):
    """Base Pydantic model for Tasks with shared attributes."""
    name: str
    description: str
    agent_id: Optional[str] = None
    expected_output: str
    tools: List[str] = Field(default_factory=list)
    async_execution: bool = False
    context: List[Union[str, str]] = Field(default_factory=list)
    config: TaskConfig = Field(default_factory=TaskConfig)
    output_json: Optional[str] = None
    output_pydantic: Optional[str] = None
    output_file: Optional[str] = None
    output: Optional[Dict[str, Any]] = None
    markdown: bool = False
    callback: Optional[str] = None
    human_input: bool = False
    converter_cls: Optional[str] = None
    guardrail: Optional[str] = None


# Properties to receive on task creation
class TaskCreate(TaskBase):
    """Pydantic model for creating a task."""
    pass


# Properties to receive on task update
class TaskUpdate(BaseModel):
    """Pydantic model for updating a task, all fields optional."""
    name: Optional[str] = None
    description: Optional[str] = None
    agent_id: Optional[str] = None
    expected_output: Optional[str] = None
    tools: Optional[List[str]] = None
    async_execution: Optional[bool] = None
    context: Optional[List[Union[str, str]]] = None
    config: Optional[TaskConfig] = None
    output_json: Optional[str] = None
    output_pydantic: Optional[str] = None
    output_file: Optional[str] = None
    output: Optional[Dict[str, Any]] = None
    markdown: Optional[bool] = None
    callback: Optional[str] = None
    human_input: Optional[bool] = None
    converter_cls: Optional[str] = None
    guardrail: Optional[str] = None


# Properties shared by models stored in DB
class TaskInDBBase(TaskBase):
    """Base Pydantic model for tasks in the database, including id and timestamps."""
    id: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# Properties to return to client
class Task(TaskInDBBase):
    """Pydantic model for returning tasks to clients."""
    pass 