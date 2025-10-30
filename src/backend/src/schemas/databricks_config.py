from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator


class DatabricksConfigBase(BaseModel):
    """Base schema for Databricks configuration."""
    workspace_url: str = ""
    warehouse_id: str = ""
    catalog: str = ""
    db_schema: str = Field("", alias="schema")
    enabled: bool = True

    # MLflow configuration
    mlflow_enabled: bool = False
    mlflow_experiment_name: Optional[str] = "kasal-crew-execution-traces"
    # MLflow Evaluation configuration
    evaluation_enabled: bool = False
    evaluation_judge_model: Optional[str] = None  # Databricks judge endpoint route, e.g., "databricks:/<endpoint>"

    # Volume configuration fields
    volume_enabled: bool = False
    volume_path: Optional[str] = None
    volume_file_format: str = "json"
    volume_create_date_dirs: bool = True

    # Knowledge source volume configuration
    knowledge_volume_enabled: bool = False
    knowledge_volume_path: Optional[str] = None
    knowledge_chunk_size: int = 1000
    knowledge_chunk_overlap: int = 200


class DatabricksConfigCreate(DatabricksConfigBase):
    """Schema for creating Databricks configuration."""

    @property
    def required_fields(self) -> List[str]:
        """Get list of required fields based on configuration"""
        if self.enabled:
            return ["warehouse_id", "catalog", "db_schema"]
        return []

    @model_validator(mode='after')
    def validate_required_fields(self):
        """Validate required fields based on configuration."""
        # Only validate if Databricks is enabled
        if not self.enabled:
            return self

        # Check required fields
        required_fields = ["warehouse_id", "catalog", "db_schema"]
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
            raise ValueError(f"Invalid configuration: {', '.join(empty_fields)} must be non-empty when Databricks is enabled")

        return self


class DatabricksConfigUpdate(DatabricksConfigBase):
    """Schema for updating Databricks configuration."""
    workspace_url: Optional[str] = None
    warehouse_id: Optional[str] = None
    catalog: Optional[str] = None
    db_schema: Optional[str] = Field(None, alias="schema")
    enabled: Optional[bool] = None

    # MLflow configuration
    mlflow_enabled: Optional[bool] = None
    mlflow_experiment_name: Optional[str] = None
    # MLflow Evaluation configuration
    evaluation_enabled: Optional[bool] = None
    evaluation_judge_model: Optional[str] = None

    # Volume configuration fields
    volume_enabled: Optional[bool] = None
    volume_path: Optional[str] = None
    volume_file_format: Optional[str] = None
    volume_create_date_dirs: Optional[bool] = None

    # Knowledge source volume configuration
    knowledge_volume_enabled: Optional[bool] = None
    knowledge_volume_path: Optional[str] = None
    knowledge_chunk_size: Optional[int] = None
    knowledge_chunk_overlap: Optional[int] = None


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