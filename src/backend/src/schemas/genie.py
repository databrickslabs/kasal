"""
Pydantic schemas for Genie API operations.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
from enum import Enum


class GenieMessageStatus(str, Enum):
    """Status of a Genie message."""
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PENDING = "PENDING"


class GenieQueryStatus(str, Enum):
    """Status of a Genie query."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"


class GenieSpace(BaseModel):
    """Schema for a Genie space."""
    id: str = Field(..., description="Unique identifier for the space")
    name: str = Field(..., description="Name of the space")
    description: Optional[str] = Field(None, description="Description of the space")
    type: Optional[str] = Field(None, description="Type of the space")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    # Additional fields from API
    enabled: Optional[bool] = Field(True, description="Whether the space is enabled")
    owner: Optional[str] = Field(None, description="Owner of the space")
    workspace_id: Optional[str] = Field(None, description="Workspace ID")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class GenieSpacesRequest(BaseModel):
    """Request for fetching Genie spaces with optional filtering and pagination."""
    search_query: Optional[str] = Field(None, description="Search query to filter spaces by name or description")
    space_ids: Optional[List[str]] = Field(None, description="List of specific space IDs to fetch")
    enabled_only: bool = Field(True, description="Only return enabled spaces")
    page_token: Optional[str] = Field(None, description="Token for fetching next page of results")
    page_size: int = Field(100, ge=1, le=200, description="Number of items per page")
    

class GenieSpacesResponse(BaseModel):
    """Response containing paginated list of Genie spaces."""
    spaces: List[GenieSpace] = Field(default_factory=list, description="List of available spaces")
    next_page_token: Optional[str] = Field(None, description="Token for fetching next page")
    page_size: int = Field(50, description="Number of items requested per page")
    has_more: bool = Field(False, description="Whether there are more results")
    filtered: bool = Field(False, description="Whether results were filtered locally")
    total_fetched: Optional[int] = Field(None, description="Total number of spaces fetched so far")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class GenieConversation(BaseModel):
    """Schema for a Genie conversation."""
    conversation_id: str = Field(..., description="Unique conversation identifier")
    space_id: str = Field(..., description="Space ID for the conversation")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    title: Optional[str] = Field(None, description="Conversation title")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class GenieMessage(BaseModel):
    """Schema for a Genie message."""
    message_id: str = Field(..., description="Unique message identifier")
    conversation_id: str = Field(..., description="Conversation ID")
    content: str = Field(..., description="Message content")
    role: Optional[str] = Field("user", description="Message role (user/assistant)")
    status: Optional[GenieMessageStatus] = Field(None, description="Message status")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    attachments: Optional[List[Dict[str, Any]]] = Field(default_factory=list, description="Message attachments")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }
        use_enum_values = True


class GenieQueryResult(BaseModel):
    """Schema for a Genie query result."""
    query_id: Optional[str] = Field(None, description="Query identifier")
    status: GenieQueryStatus = Field(..., description="Query status")
    result: Optional[Union[str, Dict[str, Any]]] = Field(None, description="Query result")
    error: Optional[str] = Field(None, description="Error message if query failed")
    sql: Optional[str] = Field(None, description="Generated SQL query")
    data: Optional[List[Dict[str, Any]]] = Field(None, description="Query result data")
    columns: Optional[List[str]] = Field(None, description="Column names")
    row_count: Optional[int] = Field(None, description="Number of rows returned")
    execution_time: Optional[float] = Field(None, description="Query execution time in seconds")
    
    class Config:
        use_enum_values = True


class GenieStartConversationRequest(BaseModel):
    """Request to start a new Genie conversation."""
    space_id: str = Field(..., description="Space ID to start conversation in")
    initial_message: Optional[str] = Field(None, description="Optional initial message")
    title: Optional[str] = Field(None, description="Optional conversation title")


class GenieStartConversationResponse(BaseModel):
    """Response from starting a new conversation."""
    conversation_id: str = Field(..., description="New conversation ID")
    message_id: Optional[str] = Field(None, description="Initial message ID if provided")
    space_id: str = Field(..., description="Space ID")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class GenieSendMessageRequest(BaseModel):
    """Request to send a message to Genie."""
    space_id: str = Field(..., description="Space ID")
    conversation_id: Optional[str] = Field(None, description="Conversation ID (creates new if not provided)")
    message: str = Field(..., description="Message content")
    attachments: Optional[List[Dict[str, Any]]] = Field(default_factory=list, description="Optional attachments")


class GenieSendMessageResponse(BaseModel):
    """Response from sending a message."""
    conversation_id: str = Field(..., description="Conversation ID")
    message_id: str = Field(..., description="Message ID")
    status: GenieMessageStatus = Field(..., description="Message status")
    response: Optional[str] = Field(None, description="Initial response if available")
    
    class Config:
        use_enum_values = True


class GenieGetMessageStatusRequest(BaseModel):
    """Request to get message status."""
    space_id: str = Field(..., description="Space ID")
    conversation_id: str = Field(..., description="Conversation ID")
    message_id: str = Field(..., description="Message ID")


class GenieGetQueryResultRequest(BaseModel):
    """Request to get query result."""
    space_id: str = Field(..., description="Space ID")
    conversation_id: str = Field(..., description="Conversation ID")
    message_id: str = Field(..., description="Message ID")


class GenieAuthConfig(BaseModel):
    """Configuration for Genie authentication."""
    use_obo: bool = Field(True, description="Use On-Behalf-Of authentication")
    user_token: Optional[str] = Field(None, description="User token for OBO")
    pat_token: Optional[str] = Field(None, description="Personal Access Token")
    host: Optional[str] = Field(None, description="Databricks host")
    
    class Config:
        # Don't include tokens in logs
        fields = {
            'user_token': {'exclude': True},
            'pat_token': {'exclude': True}
        }


class GenieExecutionRequest(BaseModel):
    """Request to execute a Genie query."""
    space_id: str = Field(..., description="Space ID")
    question: str = Field(..., description="Question to ask Genie")
    conversation_id: Optional[str] = Field(None, description="Existing conversation ID")
    timeout: Optional[int] = Field(120, description="Timeout in seconds")
    max_retries: Optional[int] = Field(3, description="Maximum number of retries")


class GenieExecutionResponse(BaseModel):
    """Response from executing a Genie query."""
    conversation_id: str = Field(..., description="Conversation ID")
    message_id: str = Field(..., description="Message ID")
    status: GenieQueryStatus = Field(..., description="Query status")
    result: Optional[str] = Field(None, description="Query result")
    query_result: Optional[GenieQueryResult] = Field(None, description="Detailed query result")
    error: Optional[str] = Field(None, description="Error message if failed")
    
    class Config:
        use_enum_values = True