"""
Pydantic schemas for connection operations.

This module defines schemas used for validating and structuring data
in connection-related API requests and responses.
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class Agent(BaseModel):
    """Agent information for connection generation."""
    name: str = Field(..., description="Name of the agent")
    role: str = Field(..., description="Role of the agent")
    goal: str = Field(..., description="Goal of the agent")
    backstory: Optional[str] = Field(None, description="Backstory of the agent")
    tools: Optional[List[str]] = Field(None, description="Tools available to the agent")


class TaskContext(BaseModel):
    """Context information for a task."""
    type: Optional[str] = Field("general", description="Type of task")
    priority: Optional[str] = Field("medium", description="Priority of the task")
    complexity: Optional[str] = Field("medium", description="Complexity of the task")
    required_skills: Optional[List[str]] = Field(None, description="Skills required for the task")


class Task(BaseModel):
    """Task information for connection generation."""
    name: str = Field(..., description="Name of the task")
    description: str = Field(..., description="Description of the task")
    expected_output: Optional[str] = Field(None, description="Expected output from the task")
    tools: Optional[List[str]] = Field(None, description="Tools required for the task")
    markdown: Optional[bool] = Field(False, description="Whether to use markdown formatting")
    context: Optional[TaskContext] = Field(None, description="Additional context for the task")
    human_input: Optional[bool] = Field(False, description="Whether to request human input")


class ConnectionRequest(BaseModel):
    """Request schema for generating connections between agents and tasks."""
    agents: List[Agent] = Field(..., description="List of agents available for task assignment")
    tasks: List[Task] = Field(..., description="List of tasks to be assigned to agents")
    model: str = Field("gpt-4-turbo", description="LLM model to use for generating connections")


class TaskAssignment(BaseModel):
    """Assignment of a task to an agent with reasoning."""
    task_name: str = Field(..., description="Name of the assigned task")
    reasoning: str = Field(..., description="Reasoning for assigning this task to the agent")


class AgentAssignment(BaseModel):
    """Tasks assigned to a specific agent."""
    agent_name: str = Field(..., description="Name of the agent")
    tasks: List[TaskAssignment] = Field(..., description="Tasks assigned to this agent")


class Dependency(BaseModel):
    """Dependency between tasks."""
    task_name: str = Field(..., description="Name of the dependent task")
    depends_on: List[str] = Field(..., description="Names of tasks this task depends on")
    reasoning: str = Field(..., description="Reasoning for the dependency")


class ConnectionResponse(BaseModel):
    """Response schema for connections between agents and tasks."""
    assignments: List[AgentAssignment] = Field(..., description="Task assignments to agents")
    dependencies: List[Dependency] = Field(..., description="Dependencies between tasks")


class ApiKeyTestResult(BaseModel):
    """Result of testing an API key."""
    has_key: bool = Field(..., description="Whether the API key exists")
    valid: Optional[bool] = Field(None, description="Whether the API key is valid")
    message: Optional[str] = Field(None, description="Message about the API key")
    key_prefix: Optional[str] = Field(None, description="First few characters of the API key")


class PythonInfo(BaseModel):
    """Information about the Python environment."""
    version: str = Field(..., description="Python version")
    executable: str = Field(..., description="Python executable path")
    platform: str = Field(..., description="Python platform")


class ApiKeyTestResponse(BaseModel):
    """Response for API key testing."""
    openai: ApiKeyTestResult = Field(..., description="OpenAI API key test results")
    anthropic: ApiKeyTestResult = Field(..., description="Anthropic API key test results")
    deepseek: ApiKeyTestResult = Field(..., description="DeepSeek API key test results")
    python_info: PythonInfo = Field(..., description="Python environment information") 