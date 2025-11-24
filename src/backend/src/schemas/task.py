import json
from datetime import datetime
from typing import List, Optional, Dict, Any, Union

from pydantic import BaseModel, Field, ConfigDict, field_validator


class ConditionConfig(BaseModel):
    """Schema for task condition configuration."""
    type: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    dependent_task: Optional[str] = None


class LLMGuardrailConfig(BaseModel):
    """Schema for LLM guardrail configuration.

    LLM guardrails use an AI model to validate task outputs against custom criteria.
    This is the OSS-compatible guardrail using crewai.tasks.llm_guardrail.LLMGuardrail.
    """
    description: str = Field(..., description="Validation criteria description")
    llm_model: Optional[str] = Field(
        default="databricks-claude-sonnet-4-5",
        description="LLM model to use for validation"
    )


class TaskConfig(BaseModel):
    """Schema for task configuration settings."""
    cache_response: Optional[bool] = None
    cache_ttl: Optional[int] = None
    retry_on_fail: Optional[bool] = None
    guardrail_max_retries: Optional[int] = None
    timeout: Optional[int] = None
    priority: Optional[int] = None
    error_handling: Optional[str] = None
    output_file: Optional[str] = None
    output_json: Optional[str] = None
    output_pydantic: Optional[str] = None
    callback: Optional[str] = None
    callback_config: Optional[Dict[str, Any]] = None
    human_input: Optional[bool] = None
    condition: Optional[ConditionConfig] = None
    guardrail: Optional[str] = None  # Code-based guardrail (function name)
    llm_guardrail: Optional[LLMGuardrailConfig] = None  # LLM-based guardrail configuration
    markdown: Optional[bool] = None


# Shared properties
class TaskBase(BaseModel):
    """Base Pydantic model for Tasks with shared attributes."""
    name: str
    description: str
    agent_id: Optional[str] = None
    expected_output: str
    tools: List[str] = Field(default_factory=list)
    tool_configs: Optional[Dict[str, Dict[str, Any]]] = Field(default_factory=dict)  # Tool-specific config overrides
    async_execution: bool = False
    context: List[Union[str, str]] = Field(default_factory=list)
    config: TaskConfig = Field(default_factory=TaskConfig)
    output_json: Optional[str] = None
    output_pydantic: Optional[str] = None
    output_file: Optional[str] = None
    output: Optional[Dict[str, Any]] = None
    markdown: bool = False
    callback: Optional[str] = None
    callback_config: Optional[Dict[str, Any]] = None
    human_input: bool = False
    converter_cls: Optional[str] = None
    guardrail: Optional[str] = None  # Code-based guardrail (function name)
    llm_guardrail: Optional[LLMGuardrailConfig] = None  # LLM-based guardrail configuration


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
    tool_configs: Optional[Dict[str, Dict[str, Any]]] = None  # Tool-specific config overrides
    async_execution: Optional[bool] = None
    context: Optional[List[Union[str, str]]] = None
    config: Optional[TaskConfig] = None
    output_json: Optional[str] = None
    output_pydantic: Optional[str] = None
    output_file: Optional[str] = None
    output: Optional[Dict[str, Any]] = None
    markdown: Optional[bool] = None
    callback: Optional[str] = None
    callback_config: Optional[Dict[str, Any]] = None
    human_input: Optional[bool] = None
    converter_cls: Optional[str] = None
    guardrail: Optional[str] = None  # Code-based guardrail (function name)
    llm_guardrail: Optional[LLMGuardrailConfig] = None  # LLM-based guardrail configuration


# Properties shared by models stored in DB
class TaskInDBBase(TaskBase):
    """Base Pydantic model for tasks in the database, including id and timestamps."""
    id: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

    @field_validator('llm_guardrail', mode='before')
    @classmethod
    def parse_llm_guardrail(cls, v):
        """Parse llm_guardrail if it's a JSON string from database."""
        if isinstance(v, str):
            try:
                return json.loads(v)
            except (json.JSONDecodeError, TypeError):
                return None
        return v


# Properties to return to client
class Task(TaskInDBBase):
    """Pydantic model for returning tasks to clients."""
    pass


# Backward compatibility alias
TaskResponse = Task