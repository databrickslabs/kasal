from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator


class DatabricksConfigBase(BaseModel):
    """Base schema for Databricks configuration."""
    workspace_url: str = ""
    warehouse_id: str = ""
    catalog: str = ""
    db_schema: str = Field("", alias="schema")
    secret_scope: str = ""
    enabled: bool = True
    apps_enabled: bool = False


class DatabricksConfigCreate(DatabricksConfigBase):
    """Schema for creating Databricks configuration."""
    
    @property
    def required_fields(self) -> List[str]:
        """Get list of required fields based on configuration"""
        if self.enabled and not self.apps_enabled:
            return ["warehouse_id", "catalog", "db_schema", "secret_scope"]
        return []
    
    @model_validator(mode='after')
    def validate_required_fields(self):
        """Validate required fields based on configuration."""
        # Only validate if Databricks is enabled
        if not self.enabled:
            return self

        # If apps are enabled, skip validation
        if self.apps_enabled:
            return self

        # Check required fields
        required_fields = ["warehouse_id", "catalog", "db_schema", "secret_scope"]
        empty_fields = []
        
        for field in required_fields:
            # Handle the schema field
            if field == "db_schema":
                value = self.db_schema
            else:
                value = getattr(self, field, "")
                
            if not value:
                empty_fields.append(field)
        
        if empty_fields:
            raise ValueError(f"Invalid configuration: {', '.join(empty_fields)} must be non-empty when Databricks is enabled and apps are disabled")
            
        return self


class DatabricksConfigUpdate(DatabricksConfigBase):
    """Schema for updating Databricks configuration."""
    workspace_url: Optional[str] = None
    warehouse_id: Optional[str] = None
    catalog: Optional[str] = None
    db_schema: Optional[str] = Field(None, alias="schema")
    secret_scope: Optional[str] = None
    enabled: Optional[bool] = None
    apps_enabled: Optional[bool] = None


class DatabricksConfigInDB(DatabricksConfigBase):
    """Base schema for Databricks configuration in the database."""
    id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = {
        "from_attributes": True,
        "populate_by_name": True
    }


class DatabricksConfigResponse(DatabricksConfigBase):
    """Schema for Databricks configuration response."""
    pass


class DatabricksTokenStatus(BaseModel):
    """Schema for Databricks token status response."""
    personal_token_required: bool
    message: str 