from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class UCToolParameterSchema(BaseModel):
    """Schema for Unity Catalog tool parameter"""
    name: str = Field(..., description="Parameter name")
    type: str = Field(..., description="Parameter data type")
    required: bool = Field(..., description="Whether the parameter is required")


class UCToolSchema(BaseModel):
    """Schema for Unity Catalog tool"""
    name: str = Field(..., description="Name of the tool")
    full_name: str = Field(..., description="Fully qualified name (catalog.schema.function)")
    catalog: str = Field(..., description="Catalog name")
    db_schema: str = Field(..., description="Schema name")
    comment: Optional[str] = Field(None, description="Tool description or comment")
    return_type: Optional[str] = Field(None, description="Return data type")
    input_params: List[UCToolParameterSchema] = Field(default_factory=list, description="Input parameters for the tool")


class UCToolListResponse(BaseModel):
    """Schema for list of Unity Catalog tools response"""
    tools: List[UCToolSchema] = Field(..., description="List of UC tools")
    count: int = Field(..., description="Total number of UC tools") 