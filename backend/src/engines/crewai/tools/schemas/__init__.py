"""
Schemas for CrewAI tools.

This module contains schema definitions used by various CrewAI tools.
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union


class SendPulseEmailOutput(BaseModel):
    """Output schema for SendPulseEmailTool."""
    success: bool = Field(description="Whether the email was sent successfully")
    message: str = Field(description="Status message")


class EmailContent(BaseModel):
    """Schema for email content."""
    subject: str = Field(description="Email subject")
    html: str = Field(description="HTML content of the email")
    text: Optional[str] = Field(None, description="Plain text content of the email")


class EmailSender(BaseModel):
    """Schema for email sender."""
    name: str = Field(description="Sender name")
    email: str = Field(description="Sender email")


class EmailRecipient(BaseModel):
    """Schema for email recipient."""
    name: Optional[str] = Field(None, description="Recipient name")
    email: str = Field(description="Recipient email")


class GoogleSlidesToolOutput(BaseModel):
    """Output schema for GoogleSlidesTool."""
    success: bool = Field(description="Whether the operation was successful")
    message: str = Field(description="Status message")
    presentation_id: Optional[str] = Field(None, description="ID of the created or updated presentation")
    presentation_url: Optional[str] = Field(None, description="URL of the created or updated presentation")


class PythonPPTXToolOutput(BaseModel):
    """Output schema for PythonPPTXTool."""
    success: bool = Field(description="Whether the operation was successful")
    message: str = Field(description="Status message")
    file_path: str = Field(description="Absolute path to the created presentation file")
    relative_path: str = Field(description="Relative path to the created presentation file")
    content: str = Field(description="Content used to create the presentation")
    title: str = Field(description="Title of the presentation") 