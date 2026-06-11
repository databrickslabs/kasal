from datetime import datetime
from typing import Optional, Dict, Any, List

from pydantic import BaseModel, Field, ConfigDict


# Shared properties
class ChatHistoryBase(BaseModel):
    """Base Pydantic model for ChatHistory with shared attributes."""
    session_id: str = Field(..., description="Chat session identifier")
    user_id: str = Field(..., description="User identifier")
    message_type: str = Field(..., pattern="^(user|assistant|system|execution|trace|result)$", description="Message type: user, assistant, system, execution, trace, or result")
    content: str = Field(..., min_length=1, description="Message content")
    intent: Optional[str] = Field(None, description="Detected intent (generate_agent, generate_task, etc.)")
    confidence: Optional[str] = Field(None, description="Confidence score as string")
    generation_result: Optional[Dict[str, Any]] = Field(None, description="Generated agent/task/crew data")


# Properties to receive on chat message creation
class ChatHistoryCreate(ChatHistoryBase):
    """Schema for creating a new chat message."""
    # Timestamp will be auto-generated in the service
    pass


# Properties to receive on chat message update
class ChatHistoryUpdate(BaseModel):
    """Schema for updating a chat message."""
    content: Optional[str] = Field(None, min_length=1, description="Updated message content")
    intent: Optional[str] = Field(None, description="Updated intent")
    confidence: Optional[str] = Field(None, description="Updated confidence score")
    generation_result: Optional[Dict[str, Any]] = Field(None, description="Updated generation result")


# Properties shared by models stored in DB
class ChatHistoryInDBBase(ChatHistoryBase):
    """Base schema for ChatHistory models in database."""
    id: str = Field(..., description="Unique message identifier")
    timestamp: datetime = Field(..., description="Message timestamp")
    
    # Multi-group fields
    group_id: Optional[str] = Field(None, description="Group identifier for isolation")
    group_email: Optional[str] = Field(None, description="Group email for audit")

    model_config = ConfigDict(from_attributes=True)
# Properties to return to client
class ChatHistoryResponse(ChatHistoryInDBBase):
    """Schema for ChatHistory API responses."""
    pass


# Properties stored in DB
class ChatHistoryInDB(ChatHistoryInDBBase):
    """Schema for ChatHistory stored in database."""
    pass


# Session-related schemas
class ChatSessionInfo(BaseModel):
    """Schema for chat session information."""
    session_id: str = Field(..., description="Chat session identifier")
    user_id: str = Field(..., description="User identifier")
    latest_timestamp: datetime = Field(..., description="Latest message timestamp")
    message_count: Optional[int] = Field(None, description="Total messages in session")

    model_config = ConfigDict(from_attributes=True)
class ChatSessionListResponse(BaseModel):
    """Schema for listing chat sessions."""
    sessions: List[ChatSessionInfo] = Field(..., description="List of chat sessions")
    total_sessions: int = Field(..., description="Total number of sessions")
    page: int = Field(..., description="Current page number")
    per_page: int = Field(..., description="Messages per page")


class ChatHistoryListResponse(BaseModel):
    """Schema for listing chat messages."""
    messages: List[ChatHistoryResponse] = Field(..., description="List of chat messages")
    total_messages: int = Field(..., description="Total number of messages")
    page: int = Field(..., description="Current page number")
    per_page: int = Field(..., description="Messages per page")
    session_id: str = Field(..., description="Chat session identifier")


# Request schemas
class SaveMessageRequest(BaseModel):
    """Schema for saving a chat message via API."""
    session_id: str = Field(..., description="Chat session identifier")
    id: Optional[str] = Field(None, description="Client-generated message id (optional; server generates when absent)")
    message_type: str = Field(..., pattern="^(user|assistant|system|execution|trace|result)$", description="Message type")
    content: str = Field(..., min_length=1, description="Message content")
    intent: Optional[str] = Field(None, description="Detected intent")
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Confidence score")
    generation_result: Optional[Dict[str, Any]] = Field(None, description="Generation result")


class GetSessionRequest(BaseModel):
    """Schema for getting chat session messages."""
    page: int = Field(0, ge=0, description="Page number (0-based)")
    per_page: int = Field(50, ge=1, le=100, description="Messages per page")


class GetUserSessionsRequest(BaseModel):
    """Schema for getting user chat sessions."""
    page: int = Field(0, ge=0, description="Page number (0-based)")
    per_page: int = Field(20, ge=1, le=50, description="Sessions per page")

class UpdateMessageRequest(BaseModel):
    """Schema for updating a chat message (streaming append / result attach)."""
    content: Optional[str] = Field(None, min_length=1, description="Updated message content")
    intent: Optional[str] = Field(None, description="Updated intent")
    generation_result: Optional[Dict[str, Any]] = Field(None, description="Updated generation result")


# Named chat sessions (chat-mode workspace) — server-side replacement for
# the browser IndexedDB session store.
class ChatSessionCreateRequest(BaseModel):
    """Schema for creating a named chat session."""
    id: Optional[str] = Field(None, description="Client-generated session id (optional)")
    title: str = Field("New Chat", max_length=255, description="Session title")


class ChatSessionRenameRequest(BaseModel):
    """Schema for renaming a named chat session."""
    title: str = Field(..., min_length=1, max_length=255, description="New session title")


class NamedChatSessionResponse(BaseModel):
    """Schema for a named chat session."""
    id: str = Field(..., description="Session identifier")
    title: str = Field(..., description="Session title")
    user_id: str = Field(..., description="Owner user id")
    group_id: Optional[str] = Field(None, description="Workspace (group) id")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last activity timestamp")

    model_config = ConfigDict(from_attributes=True)

