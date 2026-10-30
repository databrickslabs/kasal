from typing import List, Optional

from pydantic import BaseModel, Field, model_validator

from src.core.databricks_app import is_databricks_app


class DatabricksConfigBase(BaseModel):
    """Base schema for Databricks configuration."""

    workspace_url: str = ""
    warehouse_id: str = ""
    catalog: str = ""
    db_schema: str = Field("", alias="schema")
    enabled: bool = True

    # AI Gateway: route LLM/embedding traffic through /ai-gateway/mlflow/v1
    ai_gateway_enabled: bool = False

    # MLflow configuration
    mlflow_enabled: bool = False
    mlflow_experiment_name: Optional[str] = "kasal-crew-execution-traces"
    # MLflow Evaluation configuration
    evaluation_enabled: bool = False
    evaluation_judge_model: Optional[str] = (
        None  # Databricks judge endpoint route, e.g., "databricks:/<endpoint>"
    )

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
        if self.enabled and not is_databricks_app():
            return ["warehouse_id", "catalog", "db_schema"]
        return []

    @model_validator(mode="after")
    def validate_required_fields(self) -> "DatabricksConfigCreate":
        """Validate required fields based on configuration."""
        # Only validate if Databricks is enabled
        if not self.enabled or is_databricks_app():
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
            raise ValueError(
                f"Invalid configuration: {', '.join(empty_fields)} must be non-empty when Databricks is enabled"
            )

        return self


class DatabricksConfigResponse(DatabricksConfigBase):
    """Schema for Databricks configuration response."""

    installation_managed: bool = False
    warehouse_from_resource: bool = False
    resource_error: Optional[str] = None
    lakebase_managed: bool = False
    default_model: Optional[str] = None


class DatabricksTokenStatus(BaseModel):
    """Schema for Databricks token status response."""

    personal_token_required: bool
    message: str


class AIGatewayStatusUpdate(BaseModel):
    """Lightweight payload for the AI Gateway toggle — persists just the flag
    on the active Databricks config without a full config payload."""

    enabled: bool
