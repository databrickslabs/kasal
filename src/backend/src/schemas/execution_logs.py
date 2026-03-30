"""
Pydantic schemas for execution logs.

This module defines schemas used for structuring and validating
API messages related to execution logs.
"""

from typing import List, Optional, Literal
from datetime import datetime
from pydantic import BaseModel, Field


class LogMessage(BaseModel):
    """Base model for execution log messages."""
    execution_id: str = Field(..., description="ID of the execution this log relates to")
    content: str = Field(..., description="Content of the log message")
    timestamp: str = Field(..., description="ISO-formatted timestamp of the message")
    type: Literal["live", "historical"] = Field(..., description="Type of message (live or historical)")


class ExecutionLogResponse(BaseModel):
    """Schema for execution log responses."""
    content: str = Field(..., description="Content of the log message")
    timestamp: str = Field(..., description="ISO-formatted timestamp when the log was created")


class ExecutionLogsResponse(BaseModel):
    """Schema for a collection of execution logs."""
    logs: List[ExecutionLogResponse] = Field(..., description="List of execution logs") 