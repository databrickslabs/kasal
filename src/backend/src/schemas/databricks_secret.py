"""
Databricks secret schemas.

This module defines the schemas for Databricks secrets.
"""

from pydantic import BaseModel
from typing import Optional


class SecretBase(BaseModel):
    """Base schema for Databricks secrets."""
    name: str
    description: Optional[str] = ""


class SecretCreate(SecretBase):
    """Schema for creating a new Databricks secret."""
    value: str


class SecretUpdate(BaseModel):
    """Schema for updating an existing Databricks secret."""
    value: str
    description: Optional[str] = ""


class SecretResponse(BaseModel):
    """Schema for Databricks secret response."""
    id: int
    name: str
    value: str
    description: str
    scope: str
    source: str = "databricks"  # 'databricks' or 'sqlite'


class DatabricksTokenRequest(BaseModel):
    """Schema for Databricks token request."""
    workspace_url: str
    token: str 