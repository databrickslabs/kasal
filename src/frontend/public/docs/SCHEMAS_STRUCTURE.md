# Kasal Schemas Structure

This document provides a detailed guide to the organization and structure of the schemas folder in Kasal's FastAPI backend.

## Overview

The `backend/src/schemas/` directory contains all Pydantic models used for AI agent workflow validation, serialization, and API documentation. These schemas are organized by Kasal domain concepts and follow consistent patterns for AI orchestration.

## Directory Structure

```
backend/src/schemas/
├── __init__.py                 # Re-exports important schemas for easier imports
├── agent.py                    # AI agent configuration and response schemas
├── api_key.py                  # API key management schemas
├── connection.py               # Database and external connection schemas
├── crew.py                     # AI crew configuration and workflow schemas
├── databricks_config.py        # Databricks integration schemas
├── databricks_secret.py        # Databricks secrets management
├── dispatcher.py               # Task dispatching and routing schemas
├── documentation_embedding.py  # Documentation vectorization schemas
├── engine_config.py            # AI engine configuration schemas
├── execution.py                # Agent execution and monitoring schemas
├── execution_history.py        # Execution tracking and history
├── execution_logs.py           # Execution logging and trace schemas
├── execution_trace.py          # Detailed execution trace schemas
├── flow.py                     # CrewAI flow configuration schemas
├── flow_execution.py           # Flow execution management
├── group.py                    # User group and tenant schemas
├── job_execution.py            # Job execution wrapper schemas
├── log.py                      # System logging schemas
├── mcp.py                      # Model Context Protocol schemas
├── memory.py                   # Agent memory and context schemas
├── model_config.py             # LLM model configuration schemas
├── model_provider.py           # LLM provider integration schemas
├── schedule.py                 # Workflow scheduling schemas
├── scheduler.py                # Scheduler management schemas
├── schema.py                   # JSON schema validation schemas
├── task.py                     # AI task definition schemas
├── task_generation.py          # AI-powered task generation
├── task_tracking.py            # Task execution monitoring
├── template.py                 # Prompt template schemas
├── template_generation.py      # AI template generation schemas
├── tool.py                     # AI tool configuration schemas
├── uc_function.py              # Unity Catalog function schemas
├── uc_tool.py                  # Unity Catalog tool integration
├── upload.py                   # File upload and management schemas
└── user.py                     # User management and authentication schemas
```

## Organization Principles

### AI Domain-Based Organization

Kasal schemas are organized by AI workflow domains to maintain clear separation of concerns:

- **Core AI entities**: `agent.py`, `crew.py`, `task.py`, `tool.py`
- **Execution management**: `execution.py`, `execution_history.py`, `execution_logs.py`, `execution_trace.py`
- **Configuration**: `engine_config.py`, `model_config.py`, `databricks_config.py`
- **Generation and AI assistance**: `task_generation.py`, `template_generation.py`
- **Integration**: `mcp.py`, `uc_function.py`, `uc_tool.py`
- **Infrastructure**: `user.py`, `group.py`, `api_key.py`, `schedule.py`

### Re-export Pattern

Kasal uses the re-export pattern for cleaner imports throughout the application:

```python
# backend/src/schemas/__init__.py
from src.schemas.agent import Agent, AgentCreate, AgentUpdate, AgentResponse
from src.schemas.crew import Crew, CrewCreate, CrewConfig
from src.schemas.task import Task, TaskCreate, TaskUpdate
from src.schemas.execution import ExecutionCreate, ExecutionResponse
from src.schemas.user import User, UserCreate, UserUpdate

# This allows imports like:
# from src.schemas import Agent, CrewConfig, ExecutionResponse
# Instead of:
# from src.schemas.agent import Agent
# from src.schemas.crew import CrewConfig
# from src.schemas.execution import ExecutionResponse
```

### File Naming Conventions

- Use singular nouns for schema files (`agent.py`, not `agents.py`)
- Use snake_case for filenames
- Name files after the primary AI entity they represent
- Use descriptive suffixes for related functionality:
  - `task_generation.py` for AI-powered task creation
  - `execution_history.py` for execution tracking
  - `databricks_config.py` for platform-specific configuration

## Schema Organization Within Files

Each Kasal schema file follows a consistent organization pattern:

```python
# Imports
from datetime import datetime
from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field

# Base schemas
class BaseAgent(BaseModel):
    """Base schema with common agent attributes."""
    name: str = Field(..., description="Agent name")
    role: str = Field(..., description="Agent role description")
    goal: str = Field(..., description="Agent goal")
    backstory: str = Field(..., description="Agent backstory")
    
# Input schemas
class AgentCreate(BaseAgent):
    """Schema for creating a new AI agent."""
    tools: List[str] = Field(default_factory=list, description="List of tool names")
    model_config: Optional[Dict[str, Any]] = Field(None, description="LLM configuration")
    max_rpm: Optional[int] = Field(10, description="Maximum requests per minute")

class AgentUpdate(BaseModel):
    """Schema for updating an existing agent."""
    name: Optional[str] = None
    role: Optional[str] = None
    goal: Optional[str] = None
    backstory: Optional[str] = None
    tools: Optional[List[str]] = None
    model_config: Optional[Dict[str, Any]] = None
    
# Response schemas
class AgentResponse(BaseAgent):
    """Schema for agent API responses."""
    id: str = Field(..., description="Agent UUID")
    tools: List[str]
    model_config: Optional[Dict[str, Any]]
    is_active: bool
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True
```

## Common Schema Types in Kasal Domains

Each Kasal domain typically includes these schema types:

1. **Base Schemas**: Common AI entity attributes (e.g., `BaseAgent`, `BaseTask`)
2. **Create Schemas**: Validation for AI resource creation (e.g., `AgentCreate`, `CrewCreate`)
3. **Update Schemas**: Validation for AI resource updates (e.g., `AgentUpdate`, `TaskUpdate`)
4. **Response Schemas**: API response shapes (e.g., `AgentResponse`, `ExecutionResponse`)
5. **Config Schemas**: Configuration for AI operations (e.g., `CrewConfig`, `ModelConfig`)
6. **Generation Schemas**: AI-powered generation requests (e.g., `TaskGenerationRequest`)

## AI-Specific Schema Patterns

Kasal implements specialized schema patterns for AI operations:

### Configuration Schemas
Complex nested configurations for AI engines and models:

```python
# backend/src/schemas/model_config.py
class ModelConfig(BaseModel):
    """LLM model configuration schema."""
    provider: str = Field(..., description="LLM provider (openai, anthropic, etc.)")
    model: str = Field(..., description="Model name")
    temperature: float = Field(0.7, ge=0.0, le=2.0)
    max_tokens: Optional[int] = Field(None, gt=0)
    api_key: Optional[str] = Field(None, description="Provider API key")
```

### Execution Schemas
Schemas for tracking AI agent execution state:

```python
# backend/src/schemas/execution.py
class ExecutionCreate(BaseModel):
    """Schema for creating agent executions."""
    crew_config: CrewConfig
    inputs: Optional[Dict[str, Any]] = None
    
class ExecutionResponse(BaseModel):
    """Schema for execution status responses."""
    id: str
    status: ExecutionStatus
    result: Optional[str] = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
```

## Best Practices for Kasal Schema Organization

1. **Group AI-related schemas**: Keep agent, task, and execution schemas logically grouped
2. **Use flat structure**: Avoid deep nesting since AI domains are well-defined
3. **Be consistent with AI naming**: Follow AI/ML terminology conventions
4. **Provide comprehensive re-exports**: Make AI schema imports clean and intuitive
5. **Document AI relationships**: Comment on agent-task-crew relationships
6. **Avoid circular imports**: Especially important with complex AI workflow dependencies
7. **Validate AI configurations**: Use Pydantic validators for tool configs and model parameters
8. **Support extensibility**: Design schemas to accommodate new AI engines and tools

## Integration with Kasal API Routes

The schema structure aligns with Kasal's AI-focused API routes:

- For `/api/v1/agents` endpoints, use schemas from `schemas/agent.py`
- For `/api/v1/crews` endpoints, use schemas from `schemas/crew.py`
- For `/api/v1/executions` endpoints, use schemas from `schemas/execution.py`
- For `/api/v1/tasks` endpoints, use schemas from `schemas/task.py`
- For AI generation endpoints, use schemas from `schemas/task_generation.py`, `schemas/template_generation.py`

This alignment makes it intuitive to locate AI-specific schemas for each endpoint.

## AI Schema Evolution and Versioning

As Kasal's AI capabilities evolve, schemas require careful versioning:

- **Backwards compatibility**: Keep existing AI workflows functional when adding new capabilities
- **Engine updates**: When CrewAI or other engines update, create versioned schemas if needed
- **Model evolution**: Support new LLM providers and models without breaking existing configurations
- **Tool expansion**: Design tool schemas to accommodate new AI tools and capabilities
- **Deprecation warnings**: Document deprecated AI features with migration paths

## Conclusion

Kasal's schema organization is specifically designed for AI agent workflow orchestration. The flat, domain-driven structure ensures that AI-related schemas are easily discoverable and maintainable. The consistent naming and organization patterns make it straightforward to add new AI capabilities while maintaining backwards compatibility for existing workflows. 