"""
Pydantic schemas for DSPy optimization data validation and serialization.

These schemas are used for API requests/responses and internal data validation.
"""

from datetime import datetime
from enum import Enum
from typing import Dict, Any, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, ConfigDict


class OptimizationType(str, Enum):
    """Types of optimization supported by DSPy."""
    INTENT_DETECTION = "intent_detection"
    AGENT_GENERATION = "agent_generation"
    TASK_GENERATION = "task_generation"
    CREW_GENERATION = "crew_generation"


class OptimizerType(str, Enum):
    """Types of DSPy optimizers available."""
    BOOTSTRAP_FEWSHOT = "bootstrap_fewshot"
    BOOTSTRAP_RANDOM_SEARCH = "bootstrap_random_search"
    MIPRO = "mipro"
    MIPRO_V2 = "mipro_v2"


class DeploymentStage(str, Enum):
    """Deployment stages for optimized models."""
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"


class OptimizationStatus(str, Enum):
    """Status of optimization runs."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ExampleSourceType(str, Enum):
    """Source types for training examples."""
    TRACE = "trace"
    MANUAL = "manual"
    SYNTHETIC = "synthetic"


# Base Schemas
class DSPyConfigBase(BaseModel):
    """Base schema for DSPy configuration."""
    optimization_type: OptimizationType
    deployment_stage: Optional[DeploymentStage] = Field(default=DeploymentStage.STAGING)
    is_active: Optional[bool] = Field(default=True)


class DSPyConfigCreate(DSPyConfigBase):
    """Schema for creating a new DSPy configuration."""
    prompts_json: Optional[Dict[str, Any]] = None
    module_config: Optional[Dict[str, Any]] = None
    optimizer_config: Optional[Dict[str, Any]] = None
    mlflow_run_id: Optional[str] = None
    mlflow_model_uri: Optional[str] = None


class DSPyConfigInternalUpdate(BaseModel):
    """Schema for updating internal DSPy configuration (models, deployment, etc)."""
    deployment_stage: Optional[DeploymentStage] = None
    is_active: Optional[bool] = None
    performance_metrics: Optional[Dict[str, Any]] = None


class DSPyConfigInternalResponse(DSPyConfigBase):
    """Schema for internal DSPy configuration response (models, deployment, etc)."""
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    version: int
    mlflow_run_id: Optional[str] = None
    mlflow_model_uri: Optional[str] = None
    mlflow_experiment_id: Optional[str] = None
    prompts_json: Optional[Dict[str, Any]] = None
    module_config: Optional[Dict[str, Any]] = None
    optimizer_config: Optional[Dict[str, Any]] = None
    performance_metrics: Optional[Dict[str, Any]] = None
    test_score: Optional[float] = None
    num_training_examples: Optional[int] = None
    created_at: datetime
    updated_at: datetime
    deployed_at: Optional[datetime] = None
    group_id: Optional[UUID] = None


# Training Example Schemas
class DSPyExampleBase(BaseModel):
    """Base schema for DSPy training examples."""
    optimization_type: OptimizationType
    input_data: Dict[str, Any]
    output_data: Dict[str, Any]
    quality_score: Optional[float] = Field(default=0.0, ge=0.0, le=1.0)


class DSPyExampleCreate(DSPyExampleBase):
    """Schema for creating a new training example."""
    metadata: Optional[Dict[str, Any]] = None
    trace_id: Optional[str] = None
    execution_id: Optional[UUID] = None
    source_type: Optional[ExampleSourceType] = Field(default=ExampleSourceType.TRACE)


class DSPyExampleResponse(DSPyExampleBase):
    """Schema for training example response."""
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    metadata: Optional[Dict[str, Any]] = None
    trace_id: Optional[str] = None
    execution_id: Optional[UUID] = None
    source_type: ExampleSourceType
    created_at: datetime
    collected_at: Optional[datetime] = None
    used_in_optimization: bool
    optimization_run_ids: Optional[List[UUID]] = None
    group_id: Optional[UUID] = None
    config_id: Optional[UUID] = None


class DSPyExampleBatch(BaseModel):
    """Schema for batch example operations."""
    examples: List[DSPyExampleCreate]
    optimization_type: OptimizationType


# Optimization Run Schemas
class OptimizationRunCreate(BaseModel):
    """Schema for creating an optimization run."""
    optimization_type: OptimizationType
    optimizer_type: OptimizerType
    optimizer_params: Optional[Dict[str, Any]] = None
    min_quality_threshold: Optional[float] = Field(default=0.7, ge=0.0, le=1.0)
    triggered_by: Optional[str] = Field(default="api")
    triggered_by_user: Optional[str] = None


class OptimizationRunResponse(BaseModel):
    """Schema for optimization run response."""
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    optimization_type: OptimizationType
    optimizer_type: Optional[OptimizerType] = None
    optimizer_params: Optional[Dict[str, Any]] = None
    num_training_examples: Optional[int] = None
    num_validation_examples: Optional[int] = None
    min_quality_threshold: Optional[float] = None
    status: OptimizationStatus
    metrics: Optional[Dict[str, Any]] = None
    best_score: Optional[float] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    error_message: Optional[str] = None
    mlflow_run_id: Optional[str] = None
    triggered_by: Optional[str] = None
    triggered_by_user: Optional[str] = None
    group_id: Optional[UUID] = None
    config_id: Optional[UUID] = None


# Workspace DSPy Configuration Schemas
class DSPyConfigUpdate(BaseModel):
    """Schema for updating workspace DSPy configuration."""
    dspy_enabled: Optional[bool] = None
    dspy_intent_detection: Optional[bool] = None
    dspy_agent_generation: Optional[bool] = None
    dspy_task_generation: Optional[bool] = None
    dspy_crew_generation: Optional[bool] = None
    dspy_optimization_interval: Optional[int] = None
    dspy_min_examples: Optional[int] = None
    dspy_confidence_threshold: Optional[float] = None


class DSPyConfigResponse(BaseModel):
    """Schema for workspace DSPy configuration response."""
    dspy_enabled: bool
    dspy_intent_detection: bool
    dspy_agent_generation: bool
    dspy_task_generation: bool
    dspy_crew_generation: bool
    dspy_optimization_interval: int
    dspy_min_examples: int
    dspy_confidence_threshold: float


class DSPyTrainingExampleResponse(BaseModel):
    """Schema for training example response in API."""
    id: UUID
    optimization_type: str
    input_data: Dict[str, Any]
    output_data: Dict[str, Any]
    quality_score: float
    trace_id: Optional[str]
    created_at: datetime


class DSPyOptimizationRunResponse(BaseModel):
    """Schema for optimization run response in API."""
    id: UUID
    optimization_type: str
    status: OptimizationStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    best_score: Optional[float] = None
    num_training_examples: Optional[int] = None
    num_validation_examples: Optional[int] = None
    error_message: Optional[str] = None
    message: Optional[str] = None


class DSPyOptimizationRequest(BaseModel):
    """Request schema for triggering optimization."""
    optimization_type: OptimizationType
    optimizer_type: Optional[OptimizerType] = Field(default=OptimizerType.BOOTSTRAP_FEWSHOT)
    optimizer_params: Optional[Dict[str, Any]] = None
    force: Optional[bool] = Field(default=False, description="Force optimization even if recently done")


class DSPyOptimizationStatus(str, Enum):
    """Status of optimization runs for API."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


# API Request/Response Schemas
class OptimizeRequest(BaseModel):
    """Request schema for triggering optimization."""
    optimization_types: List[OptimizationType]
    optimizer_type: Optional[OptimizerType] = Field(default=OptimizerType.BOOTSTRAP_FEWSHOT)
    force: Optional[bool] = Field(default=False, description="Force optimization even if recently done")
    hours_back: Optional[int] = Field(default=24, description="Hours of trace data to collect")
    min_examples: Optional[int] = Field(default=10, description="Minimum examples required")


class OptimizeResponse(BaseModel):
    """Response schema for optimization request."""
    runs: List[OptimizationRunResponse]
    message: str


class PredictIntentRequest(BaseModel):
    """Request schema for intent prediction using DSPy."""
    message: str
    use_dspy: Optional[bool] = Field(default=True, description="Use DSPy optimization if available")


class PredictIntentResponse(BaseModel):
    """Response schema for intent prediction."""
    intent: str
    confidence: float
    extracted_info: Dict[str, Any]
    reasoning: Optional[str] = None
    method: str = Field(description="Method used: dspy_optimized, template, or fallback")


class GenerateCrewRequest(BaseModel):
    """Request schema for crew generation using DSPy."""
    prompt: str
    tools_available: Optional[List[str]] = Field(default_factory=list)
    use_dspy: Optional[bool] = Field(default=True)


class GenerateCrewResponse(BaseModel):
    """Response schema for crew generation."""
    crew_name: str
    agents: List[Dict[str, Any]]
    tasks: List[Dict[str, Any]]
    workflow: Dict[str, Any]
    reasoning: Optional[str] = None
    method: str


class DSPyStatusResponse(BaseModel):
    """Response schema for DSPy optimization status."""
    enabled: bool
    optimization_types: Dict[OptimizationType, Dict[str, Any]]
    last_optimization: Optional[datetime] = None
    next_scheduled: Optional[datetime] = None
    total_examples: int
    active_configs: int


class ModuleCacheInfo(BaseModel):
    """Schema for module cache information."""
    optimization_type: OptimizationType
    config_version: int
    cache_key: str
    loaded_at: datetime
    last_accessed: datetime
    access_count: int
    ttl_hours: int
    expires_at: datetime


class CollectExamplesRequest(BaseModel):
    """Request schema for collecting training examples."""
    optimization_type: OptimizationType
    hours_back: Optional[int] = Field(default=24)
    min_quality_score: Optional[float] = Field(default=0.0)
    limit: Optional[int] = Field(default=1000)


class CollectExamplesResponse(BaseModel):
    """Response schema for example collection."""
    examples_collected: int
    examples_stored: int
    sources: Dict[str, int]
    average_quality_score: float