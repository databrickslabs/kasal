"""
Schemas for CrewAI tools.

This module contains schema definitions used by various CrewAI tools.
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union



class GoogleSlidesToolOutput(BaseModel):
    """Output schema for GoogleSlidesTool."""
    success: bool = Field(description="Whether the operation was successful")
    message: str = Field(description="Status message")
    presentation_id: Optional[str] = Field(None, description="ID of the created or updated presentation")
    presentation_url: Optional[str] = Field(None, description="URL of the created or updated presentation") 