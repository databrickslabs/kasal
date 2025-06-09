from typing import Optional
from pydantic import BaseModel, Field


class ApiKeyBase(BaseModel):
    """Base schema with common API key attributes."""
    name: str = Field(..., min_length=1)
    description: Optional[str] = Field(None)


class ApiKeyCreate(ApiKeyBase):
    """Schema for creating a new API key."""
    value: str = Field(..., min_length=1)


class ApiKeyUpdate(BaseModel):
    """Schema for updating an existing API key."""
    value: str = Field(..., min_length=1)
    description: Optional[str] = Field(None)


class ApiKeyResponse(ApiKeyBase):
    """Schema for API key responses."""
    id: int
    value: str  # This will be decrypted when sent to the client
    
    class Config:
        from_attributes = True 