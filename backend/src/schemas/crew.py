from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from uuid import UUID as PyUUID

from pydantic import BaseModel, Field, ConfigDict


# Node data models
class Position(BaseModel):
    """Position of a node in the flow diagram."""
    x: float
    y: float


class Style(BaseModel):
    """Visual styling for a node."""
    background: Optional[str] = None
    border: Optional[str] = None
    borderRadius: Optional[str] = None
    padding: Optional[str] = None
    boxShadow: Optional[str] = None


class TaskConfig(BaseModel):
    """Configuration specific to tasks."""
    cache_response: Optional[bool] = False
    cache_ttl: Optional[int] = 3600
    retry_on_fail: Optional[bool] = False
    max_retries: Optional[int] = 3
    timeout: Optional[Any] = None
    priority: Optional[int] = 1
    error_handling: Optional[str] = "default"
    output_file: Optional[str] = None
    output_json: Optional[str] = None
    output_pydantic: Optional[str] = None
    validation_function: Optional[str] = None
    callback_function: Optional[str] = None
    human_input: Optional[bool] = False
    markdown: Optional[bool] = False


class NodeData(BaseModel):
    """Data associated with a node in the flow diagram."""
    label: str
    role: Optional[str] = None
    goal: Optional[str] = None
    backstory: Optional[str] = None
    tools: List[Any] = Field(default_factory=list)
    agentId: Optional[str] = None
    taskId: Optional[str] = None
    llm: Optional[str] = None
    function_calling_llm: Optional[str] = None
    max_iter: Optional[int] = None
    max_rpm: Optional[int] = None
    max_execution_time: Optional[int] = None
    verbose: Optional[bool] = None
    allow_delegation: Optional[bool] = None
    cache: Optional[bool] = None
    # Memory settings
    memory: Optional[bool] = True
    embedder_config: Optional[Dict[str, Any]] = None
    system_template: Optional[str] = None
    prompt_template: Optional[str] = None
    response_template: Optional[str] = None
    allow_code_execution: Optional[bool] = None
    code_execution_mode: Optional[str] = None
    max_retry_limit: Optional[int] = None
    use_system_prompt: Optional[bool] = None
    respect_context_window: Optional[bool] = None
    type: Optional[str] = None
    description: Optional[str] = None
    expected_output: Optional[str] = None
    icon: Optional[str] = None
    advanced_config: Optional[Dict[str, Any]] = None
    config: Optional[TaskConfig] = None
    context: List[str] = Field(default_factory=list)
    async_execution: Optional[bool] = False
    knowledge_sources: Optional[List[Dict[str, Any]]] = None
    markdown: Optional[bool] = Field(False, description="Whether to use markdown formatting")


class Node(BaseModel):
    """A node in the flow diagram representing an agent or task."""
    id: str
    type: str
    position: Position
    data: NodeData
    width: Optional[float] = None
    height: Optional[float] = None
    selected: Optional[bool] = None
    positionAbsolute: Optional[Position] = None
    dragging: Optional[bool] = None
    style: Optional[Style] = None


class Edge(BaseModel):
    """An edge in the flow diagram representing a connection between nodes."""
    source: str
    target: str
    id: str
    sourceHandle: Optional[str] = None
    targetHandle: Optional[str] = None


# Shared properties
class CrewBase(BaseModel):
    """Base Pydantic model for Crews with shared attributes."""
    name: str
    agent_ids: List[str] = Field(default_factory=list)
    task_ids: List[str] = Field(default_factory=list)
    nodes: List[Node] = Field(default_factory=list)
    edges: List[Edge] = Field(default_factory=list)


# Properties to receive on crew creation
class CrewCreate(CrewBase):
    """Pydantic model for creating a crew."""
    pass


# Properties to receive on crew update
class CrewUpdate(BaseModel):
    """Pydantic model for updating a crew, all fields optional."""
    name: Optional[str] = None
    agent_ids: Optional[List[str]] = None
    task_ids: Optional[List[str]] = None
    nodes: Optional[List[Node]] = None
    edges: Optional[List[Edge]] = None


# Properties shared by models stored in DB
class CrewInDBBase(CrewBase):
    """Base Pydantic model for crews in the database, including id and timestamps."""
    id: PyUUID
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# Properties to return to client
class Crew(CrewInDBBase):
    """Pydantic model for returning crews to clients."""
    pass


# Custom response model with string timestamps
class CrewResponse(BaseModel):
    """Pydantic model for Crew response with string timestamps."""
    id: PyUUID
    name: str
    agent_ids: List[str]
    task_ids: List[str]
    nodes: List[Node]
    edges: List[Edge]
    created_at: str
    updated_at: str

    model_config = ConfigDict(from_attributes=True)


class CrewGenerationRequest(BaseModel):
    """Request schema for generating crew setup with agents and tasks."""
    prompt: str = Field(..., description="Natural language description of the crew setup")
    model: Optional[str] = Field(None, description="LLM model to use for generation")
    tools: Optional[List[str]] = Field([], description="List of available tools for the crew")
    api_key: Optional[str] = Field(None, description="API key for the LLM provider")


class AgentConfig(BaseModel):
    """Configuration for an agent in the crew."""
    llm: Optional[str] = Field("databricks-llama-4-maverick", description="LLM model for the agent")
    function_calling_llm: Optional[str] = Field(None, description="LLM model for function calling")
    max_iter: Optional[int] = Field(25, description="Maximum iterations for the agent")
    max_rpm: Optional[int] = Field(None, description="Maximum requests per minute")
    max_execution_time: Optional[int] = Field(None, description="Maximum execution time in seconds")
    verbose: Optional[bool] = Field(False, description="Whether to use verbose output")
    allow_delegation: Optional[bool] = Field(False, description="Whether to allow delegation")
    cache: Optional[bool] = Field(True, description="Whether to use cache")
    system_template: Optional[str] = Field(None, description="System template")
    prompt_template: Optional[str] = Field(None, description="Prompt template")
    response_template: Optional[str] = Field(None, description="Response template")
    allow_code_execution: Optional[bool] = Field(False, description="Whether to allow code execution")
    code_execution_mode: Optional[str] = Field("safe", description="Code execution mode")
    max_retry_limit: Optional[int] = Field(2, description="Maximum retry limit")
    use_system_prompt: Optional[bool] = Field(True, description="Whether to use system prompt")
    respect_context_window: Optional[bool] = Field(True, description="Whether to respect context window")


class Agent(BaseModel):
    """Agent in the crew."""
    id: Optional[str] = Field(None, description="Unique identifier for the agent")
    name: str = Field(..., description="Name of the agent")
    role: str = Field(..., description="Role of the agent")
    goal: str = Field(..., description="Goal of the agent")
    backstory: str = Field(..., description="Backstory of the agent")
    tools: Optional[List[str]] = Field([], description="Tools available to the agent")
    llm: Optional[str] = Field("databricks-llama-4-maverick", description="LLM model for the agent")
    function_calling_llm: Optional[str] = Field(None, description="LLM model for function calling")
    max_iter: Optional[int] = Field(25, description="Maximum iterations")
    max_rpm: Optional[int] = Field(None, description="Maximum requests per minute")
    max_execution_time: Optional[int] = Field(None, description="Maximum execution time in seconds")
    verbose: Optional[bool] = Field(False, description="Whether to use verbose output")
    allow_delegation: Optional[bool] = Field(False, description="Whether to allow delegation")
    cache: Optional[bool] = Field(True, description="Whether to use cache")
    system_template: Optional[str] = Field(None, description="System template")
    prompt_template: Optional[str] = Field(None, description="Prompt template")
    response_template: Optional[str] = Field(None, description="Response template")
    allow_code_execution: Optional[bool] = Field(False, description="Whether to allow code execution")
    code_execution_mode: Optional[str] = Field("safe", description="Code execution mode")
    max_retry_limit: Optional[int] = Field(2, description="Maximum retry limit")
    use_system_prompt: Optional[bool] = Field(True, description="Whether to use system prompt")
    respect_context_window: Optional[bool] = Field(True, description="Whether to respect context window")


class Task(BaseModel):
    """Task in the crew."""
    id: Optional[str] = Field(None, description="Unique identifier for the task")
    name: str = Field(..., description="Name of the task")
    description: str = Field(..., description="Description of the task")
    expected_output: Optional[str] = Field(None, description="Expected output of the task")
    tools: Optional[List[str]] = Field([], description="Tools required for the task")
    assigned_agent: Optional[str] = Field(None, description="Agent assigned to the task")
    async_execution: Optional[bool] = Field(False, description="Whether to execute the task asynchronously")
    context: Optional[List[str]] = Field([], description="Task IDs that provide context")
    config: Optional[Dict[str, Any]] = Field({}, description="Additional configuration")
    output_json: Optional[bool] = Field(None, description="Whether to output as JSON")
    output_pydantic: Optional[str] = Field(None, description="Pydantic model for output")
    output_file: Optional[str] = Field(None, description="File to output to")
    output: Optional[str] = Field(None, description="Output variable name")
    callback: Optional[str] = Field(None, description="Callback function")
    human_input: Optional[bool] = Field(False, description="Whether to request human input")
    converter_cls: Optional[str] = Field(None, description="Converter class")
    markdown: Optional[bool] = Field(False, description="Whether to use markdown formatting")


class CrewGenerationResponse(BaseModel):
    """Response schema for crew generation."""
    agents: List[Agent] = Field(..., description="List of agents in the crew")
    tasks: List[Task] = Field(..., description="List of tasks for the crew")


class CrewCreationResponse(BaseModel):
    """Response schema for crew creation with database entities."""
    agents: List[Any] = Field(..., description="List of created agent entities with database IDs")
    tasks: List[Any] = Field(..., description="List of created task entities with database IDs")
    
    model_config = ConfigDict(from_attributes=True) 