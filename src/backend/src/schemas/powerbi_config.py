from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, model_validator


class PowerBIConfigBase(BaseModel):
    """Base schema for Power BI configuration."""
    tenant_id: str = ""
    client_id: str = ""
    workspace_id: Optional[str] = None
    semantic_model_id: Optional[str] = None
    enabled: bool = True


class PowerBIConfigCreate(PowerBIConfigBase):
    """Schema for creating Power BI configuration."""

    @property
    def required_fields(self) -> List[str]:
        """Get list of required fields based on configuration"""
        if self.enabled:
            return ["tenant_id", "client_id"]
        return []

    @model_validator(mode='after')
    def validate_required_fields(self):
        """Validate required fields based on configuration."""
        # Only validate if Power BI is enabled
        if not self.enabled:
            return self

        # Check required fields
        required_fields = ["tenant_id", "client_id"]
        empty_fields = []

        for field in required_fields:
            value = getattr(self, field, "")
            if not value:
                empty_fields.append(field)

        if empty_fields:
            raise ValueError(f"Invalid configuration: {', '.join(empty_fields)} must be non-empty when Power BI is enabled")

        return self


class PowerBIConfigUpdate(PowerBIConfigBase):
    """Schema for updating Power BI configuration."""
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    workspace_id: Optional[str] = None
    semantic_model_id: Optional[str] = None
    enabled: Optional[bool] = None


class PowerBIConfigInDB(PowerBIConfigBase):
    """Base schema for Power BI configuration in the database."""
    id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = {
        "from_attributes": True,
        "populate_by_name": True
    }


class PowerBIConfigResponse(PowerBIConfigBase):
    """Schema for Power BI configuration response."""
    pass


class DAXQueryRequest(BaseModel):
    """Schema for DAX query execution request."""
    dax_query: str = Field(..., description="DAX query to execute against the semantic model")
    semantic_model_id: Optional[str] = Field(None, description="Semantic model ID (uses default if not provided)")
    workspace_id: Optional[str] = Field(None, description="Workspace ID (uses default if not provided)")


class DAXQueryResponse(BaseModel):
    """Schema for DAX query execution response."""
    status: str = Field(..., description="Execution status: 'success' or 'error'")
    data: Optional[List[dict]] = Field(None, description="Query results as list of dictionaries")
    row_count: int = Field(0, description="Number of rows returned")
    columns: Optional[List[str]] = Field(None, description="Column names in the result set")
    error: Optional[str] = Field(None, description="Error message if execution failed")
    execution_time_ms: Optional[int] = Field(None, description="Query execution time in milliseconds")


class DAXAnalysisRequest(BaseModel):
    """Schema for DAX analysis request with questions."""
    dashboard_id: str = Field(..., description="Power BI dashboard/semantic model ID")
    questions: List[str] = Field(..., description="Business questions to analyze")
    workspace_id: Optional[str] = Field(None, description="Workspace ID (uses default if not provided)")


class DAXAnalysisResponse(BaseModel):
    """Schema for DAX analysis response."""
    status: str = Field(..., description="Analysis status: 'success' or 'error'")
    dashboard_id: str = Field(..., description="Dashboard/semantic model analyzed")
    questions: List[str] = Field(..., description="Questions that were analyzed")
    dax_statement: Optional[str] = Field(None, description="Generated DAX statement")
    results: Optional[dict] = Field(None, description="Analysis results and insights")
    error: Optional[str] = Field(None, description="Error message if analysis failed")
