"""
Pydantic schemas for Databricks Vector Search Endpoints.

This module defines the request/response schemas for endpoint operations.
"""
from typing import Optional, Dict, Any, List
from enum import Enum
from pydantic import BaseModel, Field
from datetime import datetime


class EndpointType(str, Enum):
    """Endpoint type enumeration."""
    STANDARD = "STANDARD"
    SERVERLESS = "SERVERLESS"


class EndpointState(str, Enum):
    """Endpoint state enumeration."""
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"
    PROVISIONING = "PROVISIONING"
    FAILED = "FAILED"
    NOT_FOUND = "NOT_FOUND"
    UNKNOWN = "UNKNOWN"


class EndpointCreate(BaseModel):
    """Schema for creating a vector search endpoint."""
    name: str = Field(..., description="Endpoint name")
    endpoint_type: EndpointType = Field(EndpointType.STANDARD, description="Type of endpoint")
    
    model_config = {
        "use_enum_values": True
    }


class EndpointInfo(BaseModel):
    """Schema for endpoint information."""
    name: str = Field(..., description="Endpoint name")
    endpoint_type: Optional[EndpointType] = Field(None, description="Type of endpoint")
    state: EndpointState = Field(..., description="Current state of the endpoint")
    ready: bool = Field(False, description="Whether the endpoint is ready for use")
    creation_timestamp: Optional[datetime] = Field(None, description="When the endpoint was created")
    last_updated_timestamp: Optional[datetime] = Field(None, description="Last update time")
    
    model_config = {
        "use_enum_values": True
    }


class EndpointResponse(BaseModel):
    """Response schema for endpoint operations."""
    success: bool = Field(..., description="Whether the operation was successful")
    endpoint: Optional[EndpointInfo] = Field(None, description="Endpoint information")
    message: Optional[str] = Field(None, description="Success or error message")
    error: Optional[str] = Field(None, description="Error details if failed")


class EndpointListResponse(BaseModel):
    """Response schema for listing endpoints."""
    success: bool = Field(..., description="Whether the operation was successful")
    endpoints: List[EndpointInfo] = Field(default_factory=list, description="List of endpoints")
    message: Optional[str] = Field(None, description="Success or error message")