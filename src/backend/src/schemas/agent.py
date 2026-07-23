from datetime import datetime
from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field, ConfigDict, field_validator


# Shared properties
class AgentBase(BaseModel):
    """Base Pydantic model for Agents with shared attributes."""
    name: str = Field(default="Unnamed Agent")
    role: str
    goal: str
    backstory: str
    
    # Core configuration
    llm: str = Field(default="databricks-llama-4-maverick")
    temperature: Optional[int] = Field(default=None, ge=0, le=100, description="Temperature override (0-100)")
    tools: List[Any] = Field(default_factory=list)
    tool_configs: Optional[Dict[str, Dict[str, Any]]] = Field(default_factory=dict)  # Tool-specific config overrides
    function_calling_llm: Optional[str] = None
    
    # Execution settings
    max_iter: int = Field(default=25)
    max_rpm: Optional[int] = Field(default=10, description="Maximum requests per minute to avoid rate limits")
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
    # SECURITY: Always force to False for safety
    allow_code_execution: bool = Field(default=False)
    code_execution_mode: str = Field(default="safe")
    
    # Additional settings
    max_retry_limit: int = Field(default=2)
    use_system_prompt: bool = Field(default=True)
    respect_context_window: bool = Field(default=True)
    
    # Knowledge sources
    knowledge_sources: List[Any] = Field(default_factory=list)

    # Date awareness settings (CrewAI 1.9+)
    inject_date: bool = Field(default=True, description="Injects current date into agent's context for time-sensitive tasks. Enabled by default.")
    date_format: Optional[str] = Field(default=None, description="Custom date format string (e.g., '%B %d, %Y' for 'February 05, 2026'). Defaults to ISO format if not specified.")

    @field_validator('max_rpm', mode='before')
    @classmethod
    def coerce_max_rpm_none_to_default(cls, v):
        """Convert None to default value for max_rpm."""
        if v is None:
            return 10
        return v

    @field_validator('llm', mode='before')
    @classmethod
    def coerce_llm_none_to_default(cls, v):
        return v if v is not None else "databricks-llama-4-maverick"

    @field_validator('max_iter', mode='before')
    @classmethod
    def coerce_max_iter_none_to_default(cls, v):
        return v if v is not None else 25

    @field_validator('verbose', mode='before')
    @classmethod
    def coerce_verbose_none_to_default(cls, v):
        return v if v is not None else False

    @field_validator('allow_delegation', mode='before')
    @classmethod
    def coerce_allow_delegation_none_to_default(cls, v):
        return v if v is not None else False

    @field_validator('cache', mode='before')
    @classmethod
    def coerce_cache_none_to_default(cls, v):
        return v if v is not None else True

    @field_validator('memory', mode='before')
    @classmethod
    def coerce_memory_none_to_default(cls, v):
        return v if v is not None else True

    @field_validator('code_execution_mode', mode='before')
    @classmethod
    def coerce_code_execution_mode_none_to_default(cls, v):
        return v if v is not None else "safe"

    @field_validator('max_retry_limit', mode='before')
    @classmethod
    def coerce_max_retry_limit_none_to_default(cls, v):
        return v if v is not None else 2

    @field_validator('use_system_prompt', mode='before')
    @classmethod
    def coerce_use_system_prompt_none_to_default(cls, v):
        return v if v is not None else True

    @field_validator('respect_context_window', mode='before')
    @classmethod
    def coerce_respect_context_window_none_to_default(cls, v):
        return v if v is not None else True

    @field_validator('knowledge_sources', mode='before')
    @classmethod
    def coerce_knowledge_sources_none_to_default(cls, v):
        return v if v is not None else []

    @field_validator('inject_date', mode='before')
    @classmethod
    def coerce_inject_date_none_to_default(cls, v):
        return v if v is not None else True

    @field_validator('allow_code_execution', mode='before')
    @classmethod
    def force_code_execution_false(cls, v):
        """SECURITY: Always force allow_code_execution to False for safety."""
        if v is True:
            print(f"WARNING: Attempted to set allow_code_execution=True, forcing to False for security")
        return False


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
    temperature: Optional[int] = Field(default=None, ge=0, le=100, description="Temperature override (0-100)")
    tools: Optional[List[Any]] = None
    tool_configs: Optional[Dict[str, Dict[str, Any]]] = None  # Tool-specific config overrides
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

    # Date awareness settings (CrewAI 1.9+)
    inject_date: Optional[bool] = None
    date_format: Optional[str] = None

    @field_validator('allow_code_execution', mode='before')
    @classmethod
    def force_code_execution_false(cls, v):
        """SECURITY: Always force allow_code_execution to False for safety."""
        if v is not None and v is True:
            print(f"WARNING: Attempted to set allow_code_execution=True in update, forcing to False for security")
            return False
        return v  # Return None if None, False if False


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

    model_config = ConfigDict(from_attributes=True)
# Properties to return to client
class Agent(AgentInDBBase):
    """Pydantic model for returning agents to clients."""
    pass 