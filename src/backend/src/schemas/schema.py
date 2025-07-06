from typing import Dict, List, Optional, Any, ClassVar, Union
from datetime import datetime
from pydantic import BaseModel, Field, field_validator, model_validator


class SchemaBase(BaseModel):
    """Base class for Schema model schemas"""
    name: str = Field(..., description="Unique name identifier for the schema")
    description: str = Field(..., description="Description of what the schema represents")
    schema_type: str = Field(..., description="Type of schema (e.g., 'data_model', 'tool_config')")
    schema_definition: Dict[str, Any] = Field(..., description="JSON schema definition")
    field_descriptions: Optional[Dict[str, str]] = Field(default_factory=dict, description="Descriptions for each field in the schema")
    keywords: Optional[List[str]] = Field(default_factory=list, description="List of related keywords for search")
    tools: Optional[List[str]] = Field(default_factory=list, description="Tools that can use this schema")
    example_data: Optional[Dict[str, Any]] = Field(default=None, description="Example data conforming to this schema")


class SchemaCreate(SchemaBase):
    """Schema for creating a new Schema"""
    # For backward compatibility
    legacy_schema_json: Optional[Dict[str, Any]] = Field(default=None, description="Legacy field for schema definition")

    @model_validator(mode='after')
    def validate_schema_json(self) -> 'SchemaCreate':
        """Handle legacy schema_json field"""
        if self.legacy_schema_json and not self.schema_definition:
            self.schema_definition = self.legacy_schema_json
        return self


class SchemaUpdate(BaseModel):
    """Schema for updating an existing Schema"""
    name: Optional[str] = Field(default=None, description="Unique name identifier for the schema")
    description: Optional[str] = Field(default=None, description="Description of what the schema represents")
    schema_type: Optional[str] = Field(default=None, description="Type of schema (e.g., 'data_model', 'tool_config')")
    schema_definition: Optional[Dict[str, Any]] = Field(default=None, description="JSON schema definition")
    field_descriptions: Optional[Dict[str, str]] = Field(default=None, description="Descriptions for each field in the schema")
    keywords: Optional[List[str]] = Field(default=None, description="List of related keywords for search")
    tools: Optional[List[str]] = Field(default=None, description="Tools that can use this schema")
    example_data: Optional[Dict[str, Any]] = Field(default=None, description="Example data conforming to this schema")
    # For backward compatibility
    legacy_schema_json: Optional[Dict[str, Any]] = Field(default=None, description="Legacy field for schema definition")


class SchemaResponse(SchemaBase):
    """Schema for returning a Schema"""
    id: int = Field(..., description="Unique identifier for the schema")
    created_at: datetime = Field(..., description="Timestamp when the schema was created")
    updated_at: datetime = Field(..., description="Timestamp when the schema was last updated")

    model_config: ClassVar[Dict[str, Any]] = {
        "from_attributes": True
    }


class SchemaListResponse(BaseModel):
    """Schema for returning a list of Schemas"""
    schemas: List[SchemaResponse] = Field(..., description="List of schemas")
    count: int = Field(..., description="Total number of schemas") 