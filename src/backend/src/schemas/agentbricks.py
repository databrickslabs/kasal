"""
Pydantic schemas for AgentBricks API operations.

AgentBricks (Mosaic AI Agent Bricks) is Databricks' no-code AI agent builder platform.
This module defines schemas for interacting with AgentBricks serving endpoints.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class AgentBricksEndpointState(str, Enum):
    """State of an AgentBricks endpoint."""
    NOT_UPDATING = "NOT_UPDATING"
    UPDATING = "UPDATING"
    UPDATE_FAILED = "UPDATE_FAILED"
    READY = "READY"


class AgentBricksQueryStatus(str, Enum):
    """Status of an AgentBricks query."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"


class AgentBricksEndpoint(BaseModel):
    """Schema for an AgentBricks serving endpoint."""
    id: str = Field(..., description="Unique identifier for the endpoint")
    name: str = Field(..., description="Name of the endpoint")
    creator: Optional[str] = Field(None, description="Creator of the endpoint")
    creation_timestamp: Optional[int] = Field(None, description="Creation timestamp (epoch milliseconds)")
    last_updated_timestamp: Optional[int] = Field(None, description="Last update timestamp (epoch milliseconds)")
    state: Optional[AgentBricksEndpointState] = Field(None, description="Current state of the endpoint")
    config: Optional[Dict[str, Any]] = Field(None, description="Endpoint configuration")
    tags: Optional[List[Dict[str, str]]] = Field(default_factory=list, description="Endpoint tags")
    # Additional metadata
    task: Optional[str] = Field(None, description="Task type (e.g., llm/v1/chat, llm/v1/completions)")
    endpoint_type: Optional[str] = Field(None, description="Type of endpoint")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }
        use_enum_values = True


class AgentBricksEndpointsRequest(BaseModel):
    """Request for fetching AgentBricks endpoints with optional filtering."""
    search_query: Optional[str] = Field(None, description="Search query to filter endpoints by name or creator")
    endpoint_ids: Optional[List[str]] = Field(None, description="List of specific endpoint IDs to fetch")
    ready_only: bool = Field(True, description="Only return ready endpoints")
    creator_filter: Optional[str] = Field(None, description="Filter by creator")


class AgentBricksEndpointsResponse(BaseModel):
    """Response containing list of AgentBricks endpoints."""
    endpoints: List[AgentBricksEndpoint] = Field(default_factory=list, description="List of available endpoints")
    total_count: int = Field(0, description="Total number of endpoints found")
    filtered: bool = Field(False, description="Whether results were filtered")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class AgentBricksMessage(BaseModel):
    """Schema for an AgentBricks message (input/output)."""
    role: str = Field(..., description="Message role (user/assistant)")
    content: str = Field(..., description="Message content")


class AgentBricksQueryRequest(BaseModel):
    """Request to query an AgentBricks endpoint."""
    endpoint_name: str = Field(..., description="Name of the AgentBricks endpoint")
    messages: List[AgentBricksMessage] = Field(..., description="Conversation messages")
    custom_inputs: Optional[Dict[str, Any]] = Field(None, description="Custom inputs for the agent")
    return_trace: bool = Field(False, description="Whether to return execution trace")
    stream: bool = Field(False, description="Whether to stream the response")


class AgentBricksQueryResponse(BaseModel):
    """Response from querying an AgentBricks endpoint."""
    response: str = Field(..., description="Agent response content")
    status: AgentBricksQueryStatus = Field(..., description="Query status")
    error: Optional[str] = Field(None, description="Error message if query failed")
    trace: Optional[Dict[str, Any]] = Field(None, description="Execution trace if requested")
    usage: Optional[Dict[str, Any]] = Field(None, description="Token usage information")

    class Config:
        use_enum_values = True


class AgentBricksAuthConfig(BaseModel):
    """Configuration for AgentBricks authentication."""
    use_obo: bool = Field(True, description="Use On-Behalf-Of authentication")
    user_token: Optional[str] = Field(None, description="User token for OBO", exclude=True)
    pat_token: Optional[str] = Field(None, description="Personal Access Token", exclude=True)
    host: Optional[str] = Field(None, description="Databricks host")

    model_config = {"json_schema_extra": {"example": {"use_obo": True, "host": "https://example.databricks.com"}}}


class AgentBricksExecutionRequest(BaseModel):
    """Request to execute an AgentBricks query."""
    endpoint_name: str = Field(..., description="AgentBricks endpoint name")
    question: str = Field(..., description="Question to ask the agent")
    custom_inputs: Optional[Dict[str, Any]] = Field(None, description="Custom inputs")
    return_trace: bool = Field(False, description="Include execution trace")
    timeout: Optional[int] = Field(120, description="Timeout in seconds")


class AgentBricksExecutionResponse(BaseModel):
    """Response from executing an AgentBricks query."""
    endpoint_name: str = Field(..., description="Endpoint name")
    status: AgentBricksQueryStatus = Field(..., description="Query status")
    result: Optional[str] = Field(None, description="Query result")
    error: Optional[str] = Field(None, description="Error message if failed")
    trace: Optional[Dict[str, Any]] = Field(None, description="Execution trace")

    class Config:
        use_enum_values = True
