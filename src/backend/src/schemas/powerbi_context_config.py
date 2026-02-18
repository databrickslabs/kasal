from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


# ===== Business Mappings Schemas =====

class PowerBIBusinessMappingBase(BaseModel):
    """Base schema for business terminology mappings."""
    natural_term: str = Field(..., description="Natural language business term")
    dax_expression: str = Field(..., description="Corresponding DAX expression")
    description: Optional[str] = Field(None, description="Optional description of the mapping")


class PowerBIBusinessMappingCreate(PowerBIBusinessMappingBase):
    """Schema for creating a business mapping."""
    semantic_model_id: str = Field(..., description="Power BI semantic model ID")


class PowerBIBusinessMappingUpdate(BaseModel):
    """Schema for updating a business mapping."""
    natural_term: Optional[str] = Field(None, description="Natural language business term")
    dax_expression: Optional[str] = Field(None, description="Corresponding DAX expression")
    description: Optional[str] = Field(None, description="Optional description of the mapping")


class PowerBIBusinessMappingInDB(PowerBIBusinessMappingBase):
    """Schema for business mapping from database."""
    id: int
    group_id: str
    semantic_model_id: str
    created_at: datetime
    updated_at: datetime

    model_config = {
        "from_attributes": True,
        "populate_by_name": True
    }


class PowerBIBusinessMappingResponse(PowerBIBusinessMappingInDB):
    """Schema for business mapping API response."""
    pass


# ===== Field Synonyms Schemas =====

class PowerBIFieldSynonymBase(BaseModel):
    """Base schema for field synonyms."""
    field_name: str = Field(..., description="Canonical field name in the semantic model")
    synonyms: List[str] = Field(..., description="List of alternative names for the field")


class PowerBIFieldSynonymCreate(PowerBIFieldSynonymBase):
    """Schema for creating a field synonym."""
    semantic_model_id: str = Field(..., description="Power BI semantic model ID")


class PowerBIFieldSynonymUpdate(BaseModel):
    """Schema for updating a field synonym."""
    field_name: Optional[str] = Field(None, description="Canonical field name")
    synonyms: Optional[List[str]] = Field(None, description="List of alternative names")


class PowerBIFieldSynonymInDB(PowerBIFieldSynonymBase):
    """Schema for field synonym from database."""
    id: int
    group_id: str
    semantic_model_id: str
    created_at: datetime
    updated_at: datetime

    model_config = {
        "from_attributes": True,
        "populate_by_name": True
    }


class PowerBIFieldSynonymResponse(PowerBIFieldSynonymInDB):
    """Schema for field synonym API response."""
    pass


# ===== Bulk Operations Schemas =====

class PowerBIContextConfigBulkResponse(BaseModel):
    """Schema for bulk context configuration response."""
    business_mappings: List[PowerBIBusinessMappingResponse]
    field_synonyms: List[PowerBIFieldSynonymResponse]


# ===== Dictionary Format Schemas (for Tool Compatibility) =====

class PowerBIContextConfigDict(BaseModel):
    """
    Schema for context configuration in dictionary format (used by tools).
    Matches the format expected by powerbi_analysis_tool.
    """
    business_mappings: Optional[dict] = Field(
        None,
        description="Dictionary mapping natural terms to DAX expressions"
    )
    field_synonyms: Optional[dict] = Field(
        None,
        description="Dictionary mapping field names to lists of synonyms"
    )
