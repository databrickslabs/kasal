from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict


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


class LLMLogResponse(LLMLogBase):
    """Schema for LLM log responses."""

    id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)
