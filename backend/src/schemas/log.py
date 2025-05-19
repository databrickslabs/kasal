from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class LLMLogBase(BaseModel):
    """Base schema with common LLM log attributes."""
    endpoint: str
    prompt: str
    response: str
    model: str
    status: str
    tokens_used: Optional[int] = None
    duration_ms: Optional[int] = None
    error_message: Optional[str] = None
    extra_data: Optional[Dict[str, Any]] = None


class LLMLogCreate(LLMLogBase):
    """Schema for creating a new LLM log."""
    pass


class LLMLogResponse(LLMLogBase):
    """Schema for LLM log responses."""
    id: int
    created_at: datetime
    
    class Config:
        from_attributes = True


class LLMLogsQueryParams(BaseModel):
    """Schema for LLM logs query parameters."""
    page: int = Field(0, ge=0, description="Page number, starting from 0")
    per_page: int = Field(10, ge=1, le=100, description="Items per page, between 1 and 100")
    endpoint: Optional[str] = Field(None, description="Filter by endpoint, 'all' or None for all endpoints") 