from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, validator


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
    
    @validator('*', pre=True)
    def validate_required_fields(cls, v, values, **kwargs):
        """Validate required fields based on configuration."""
        field_name = kwargs.get('field_name')
        
        # Skip validation for non-field properties
        if field_name is None:
            return v
            
        # Only validate if we've processed all fields
        if field_name != 'apps_enabled':
            return v
            
        # Only validate if Databricks is enabled
        if not values.get('enabled', True):
            return v

        # If apps are enabled, skip validation
        if values.get('apps_enabled', False):
            return v

        # Check required fields
        required_fields = ["warehouse_id", "catalog", "db_schema", "secret_scope"]
        empty_fields = []
        
        for field in required_fields:
            # Handle the schema field
            if field == "db_schema":
                value = values.get("db_schema", "")
            else:
                value = values.get(field, "")
                
            if not value:
                empty_fields.append(field)
        
        if empty_fields:
            raise ValueError(f"Invalid configuration: {', '.join(empty_fields)} must be non-empty when Databricks is enabled and apps are disabled")
            
        return v


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