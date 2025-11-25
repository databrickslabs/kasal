"""
Schemas for crew export and deployment operations.
"""

from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from enum import Enum
from uuid import UUID


class ExportFormat(str, Enum):
    """Available export formats"""
    PYTHON_PROJECT = "python_project"
    DATABRICKS_NOTEBOOK = "databricks_notebook"


class DeploymentTarget(str, Enum):
    """Available deployment targets"""
    DATABRICKS_MODEL_SERVING = "databricks_model_serving"


class ExportOptions(BaseModel):
    """Options for crew export"""
    include_custom_tools: bool = Field(True, description="Include custom tool implementations")
    include_comments: bool = Field(True, description="Add explanatory comments")
    include_tests: bool = Field(True, description="Include test files (python_project only)")
    model_override: Optional[str] = Field(None, description="Override LLM model for all agents")
    include_memory_config: bool = Field(True, description="Include memory backend configuration")

    # Databricks notebook options
    include_tracing: bool = Field(True, description="Include MLflow tracing/autolog (databricks_notebook only)")
    include_evaluation: bool = Field(True, description="Include MLflow evaluation cell (databricks_notebook only)")
    include_deployment: bool = Field(True, description="Include model deployment cell (databricks_notebook only)")


class CrewExportRequest(BaseModel):
    """Request to export a crew"""
    export_format: ExportFormat = Field(..., description="Target export format")
    options: ExportOptions = Field(default_factory=ExportOptions)


class ExportFile(BaseModel):
    """Individual file in export"""
    path: str = Field(..., description="Relative path in project")
    content: str = Field(..., description="File content")
    type: str = Field(..., description="File type (python, yaml, markdown, text)")


class CrewExportResponse(BaseModel):
    """Response from crew export"""
    crew_id: str
    crew_name: str
    export_format: ExportFormat

    # For python_project format
    files: Optional[List[ExportFile]] = None

    # For databricks_notebook format
    notebook: Optional[Dict[str, Any]] = None
    notebook_content: Optional[str] = None  # JSON string for download

    # Common metadata
    metadata: Dict[str, Any] = Field(default_factory=dict)
    generated_at: str
    download_url: Optional[str] = None
    size_bytes: Optional[int] = None


class ModelServingConfig(BaseModel):
    """Configuration for Databricks Model Serving deployment"""
    model_name: str = Field(..., description="Name for the registered model")
    endpoint_name: Optional[str] = Field(None, description="Model serving endpoint name (defaults to model_name)")

    # Compute configuration
    workload_size: str = Field("Small", description="Workload size: Small, Medium, Large")
    scale_to_zero_enabled: bool = Field(True, description="Enable scale to zero")
    min_instances: int = Field(0, description="Minimum number of instances")
    max_instances: int = Field(1, description="Maximum number of instances")

    # Model configuration
    unity_catalog_model: bool = Field(True, description="Register in Unity Catalog")
    catalog_name: Optional[str] = Field(None, description="Unity Catalog name")
    schema_name: Optional[str] = Field(None, description="Unity Catalog schema name")

    # Environment
    environment_vars: Optional[Dict[str, str]] = Field(default_factory=dict, description="Environment variables")

    # Tags
    tags: Optional[Dict[str, str]] = Field(default_factory=dict, description="Model tags")


class DeploymentRequest(BaseModel):
    """Request to deploy a crew"""
    deployment_target: DeploymentTarget = Field(..., description="Deployment target")
    config: ModelServingConfig = Field(..., description="Deployment configuration")


class DeploymentStatus(str, Enum):
    """Deployment status"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    READY = "ready"
    FAILED = "failed"
    UPDATING = "updating"


class DeploymentResponse(BaseModel):
    """Response from crew deployment"""
    crew_id: str
    crew_name: str
    deployment_target: DeploymentTarget

    # Model information
    model_name: str
    model_version: Optional[str] = None
    model_uri: Optional[str] = None

    # Endpoint information
    endpoint_name: str
    endpoint_url: Optional[str] = None
    endpoint_status: DeploymentStatus

    # Deployment details
    deployment_id: Optional[str] = None
    deployed_at: Optional[str] = None

    # Metadata
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Usage instructions
    usage_example: Optional[str] = None


class DeploymentStatusResponse(BaseModel):
    """Response for deployment status check"""
    deployment_id: str
    endpoint_name: str
    endpoint_url: Optional[str] = None
    status: DeploymentStatus

    # Status details
    state_message: Optional[str] = None
    ready_replicas: Optional[int] = None
    target_replicas: Optional[int] = None

    # Timestamps
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    # Configuration
    config: Optional[Dict[str, Any]] = None


class EndpointInvokeRequest(BaseModel):
    """Request to invoke a deployed endpoint"""
    inputs: Dict[str, Any] = Field(..., description="Input parameters for the crew")

    # Optional overrides
    stream: bool = Field(False, description="Enable streaming response")
    timeout: Optional[int] = Field(None, description="Request timeout in seconds")


class EndpointInvokeResponse(BaseModel):
    """Response from endpoint invocation"""
    result: Any = Field(..., description="Crew execution result")

    # Execution metadata
    execution_time_seconds: Optional[float] = None
    tokens_used: Optional[int] = None

    # Task outputs
    task_outputs: Optional[List[Dict[str, Any]]] = None

    # Metadata
    metadata: Optional[Dict[str, Any]] = None
