"""
Pydantic schemas for task generation operations.

This module defines schemas used for validating and structuring data
in task generation API requests and responses.
"""

from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field


class Agent(BaseModel):
    """Schema for agent information."""
    name: str = Field(..., description="Name of the agent")
    role: str = Field(..., description="Role of the agent")
    goal: str = Field(..., description="Goal of the agent")
    backstory: str = Field(..., description="Backstory of the agent")


class TaskGenerationRequest(BaseModel):
    """Request schema for generating a task."""
    text: str = Field(..., description="Text prompt for task generation")
    model: Optional[str] = Field(None, description="LLM model to use for task generation")
    agent: Optional[Agent] = Field(None, description="Agent context for task generation")
    markdown: Optional[bool] = Field(False, description="Whether the task should include markdown")


class AdvancedConfig(BaseModel):
    """Advanced configuration for tasks."""
    async_execution: bool = Field(False, description="Whether the task should be executed asynchronously")
    context: List[str] = Field(default_factory=list, description="List of context tasks")
    output_json: Optional[Dict] = Field(None, description="JSON schema for output validation")
    output_pydantic: Optional[str] = Field(None, description="Pydantic model for output validation")
    output_file: Optional[str] = Field(None, description="File path for output")
    human_input: bool = Field(False, description="Whether human input is required")
    retry_on_fail: bool = Field(True, description="Whether to retry on failure")
    max_retries: int = Field(3, description="Maximum number of retries")
    timeout: Optional[int] = Field(None, description="Timeout in seconds")
    priority: int = Field(1, description="Task priority")
    dependencies: List[str] = Field(default_factory=list, description="Task dependencies")
    callback: Optional[str] = Field(None, description="Callback function")
    error_handling: str = Field("default", description="Error handling mode")
    output_parser: Optional[str] = Field(None, description="Output parser function")
    cache_response: bool = Field(True, description="Whether to cache response")
    cache_ttl: int = Field(3600, description="Cache TTL in seconds")
    markdown: bool = Field(False, description="Whether the task should include markdown")


class TaskGenerationResponse(BaseModel):
    """Response schema for task generation."""
    name: str = Field(..., description="Name of the task")
    description: str = Field(..., description="Description of the task")
    expected_output: str = Field(..., description="Expected output of the task")
    tools: List[Dict[str, Any]] = Field(default_factory=list, description="Tools to use for the task")
    advanced_config: AdvancedConfig = Field(default_factory=AdvancedConfig, description="Advanced configuration") 