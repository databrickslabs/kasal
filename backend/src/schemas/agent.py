from datetime import datetime
from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field


# Shared properties
class AgentBase(BaseModel):
    """Base Pydantic model for Agents with shared attributes."""
    name: str = Field(default="Unnamed Agent")
    role: str
    goal: str
    backstory: str
    
    # Core configuration
    llm: str = Field(default="databricks-llama-4-maverick")
    tools: List[Any] = Field(default_factory=list)
    function_calling_llm: Optional[str] = None
    
    # Execution settings
    max_iter: int = Field(default=25)
    max_rpm: Optional[int] = None
    max_execution_time: Optional[int] = None
    verbose: bool = Field(default=False)
    allow_delegation: bool = Field(default=False)
    cache: bool = Field(default=True)
    
    # Memory settings
    memory: bool = Field(default=True)
    embedder_config: Optional[Dict[str, Any]] = None
    
    # Templates
    system_template: Optional[str] = None
    prompt_template: Optional[str] = None
    response_template: Optional[str] = None
    
    # Code execution settings
    allow_code_execution: bool = Field(default=False)
    code_execution_mode: str = Field(default="safe")
    
    # Additional settings
    max_retry_limit: int = Field(default=2)
    use_system_prompt: bool = Field(default=True)
    respect_context_window: bool = Field(default=True)
    
    # Knowledge sources
    knowledge_sources: List[Any] = Field(default_factory=list)


# Properties to receive on agent creation
class AgentCreate(AgentBase):
    """Pydantic model for creating an agent."""
    pass


# Properties to receive on agent update
class AgentUpdate(BaseModel):
    """Pydantic model for updating an agent, all fields optional."""
    name: Optional[str] = None
    role: Optional[str] = None
    goal: Optional[str] = None
    backstory: Optional[str] = None
    
    # Core configuration
    llm: Optional[str] = None
    tools: Optional[List[Any]] = None
    function_calling_llm: Optional[str] = None
    
    # Execution settings
    max_iter: Optional[int] = None
    max_rpm: Optional[int] = None
    max_execution_time: Optional[int] = None
    verbose: Optional[bool] = None
    allow_delegation: Optional[bool] = None
    cache: Optional[bool] = None
    
    # Memory settings
    memory: Optional[bool] = None
    embedder_config: Optional[Dict[str, Any]] = None
    
    # Templates
    system_template: Optional[str] = None
    prompt_template: Optional[str] = None
    response_template: Optional[str] = None
    
    # Code execution settings
    allow_code_execution: Optional[bool] = None
    code_execution_mode: Optional[str] = None
    
    # Additional settings
    max_retry_limit: Optional[int] = None
    use_system_prompt: Optional[bool] = None
    respect_context_window: Optional[bool] = None
    
    # Knowledge sources
    knowledge_sources: Optional[List[Any]] = None


# Properties to receive on agent limited update
class AgentLimitedUpdate(BaseModel):
    """Pydantic model for limited agent updates."""
    name: Optional[str] = None
    role: Optional[str] = None
    goal: Optional[str] = None
    backstory: Optional[str] = None


# Properties shared by models stored in DB
class AgentInDBBase(AgentBase):
    """Base Pydantic model for agents in the database, including id and timestamps."""
    id: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# Properties to return to client
class Agent(AgentInDBBase):
    """Pydantic model for returning agents to clients."""
    pass 