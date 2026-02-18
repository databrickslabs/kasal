"""
Conversion Schemas
Pydantic schemas for converter models and API validation
"""

from typing import Dict, Any, Optional, List, ClassVar
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum


# ===== ENUMS =====

class ConversionStatus(str, Enum):
    """Conversion status enumeration"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class JobStatus(str, Enum):
    """Job status enumeration"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ConversionFormat(str, Enum):
    """Supported conversion formats"""
    POWERBI = "powerbi"
    YAML = "yaml"
    DAX = "dax"
    SQL = "sql"
    UC_METRICS = "uc_metrics"


# ===== CONVERSION HISTORY SCHEMAS =====

class ConversionHistoryBase(BaseModel):
    """Base ConversionHistory schema with common attributes"""
    execution_id: Optional[str] = Field(None, description="Execution ID if part of crew execution")
    source_format: str = Field(..., description="Source format (powerbi, yaml, etc.)")
    target_format: str = Field(..., description="Target format (dax, sql, uc_metrics, yaml)")
    input_summary: Optional[str] = Field(None, description="Human-readable input summary")
    output_summary: Optional[str] = Field(None, description="Human-readable output summary")
    configuration: Optional[Dict[str, Any]] = Field(None, description="Converter configuration used")
    status: str = Field(default="pending", description="Conversion status")
    measure_count: Optional[int] = Field(None, description="Number of measures converted")


class ConversionHistoryCreate(ConversionHistoryBase):
    """Schema for creating a new conversion history entry"""
    input_data: Optional[Dict[str, Any]] = Field(None, description="Source input data")
    output_data: Optional[Dict[str, Any]] = Field(None, description="Generated output data")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    warnings: Optional[List[str]] = Field(None, description="Warning messages")
    execution_time_ms: Optional[int] = Field(None, description="Execution time in milliseconds")
    converter_version: Optional[str] = Field(None, description="Version of converter used")
    extra_metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class ConversionHistoryUpdate(BaseModel):
    """Schema for updating conversion history"""
    status: Optional[str] = Field(None, description="Conversion status")
    output_data: Optional[Dict[str, Any]] = Field(None, description="Generated output data")
    output_summary: Optional[str] = Field(None, description="Output summary")
    error_message: Optional[str] = Field(None, description="Error message")
    warnings: Optional[List[str]] = Field(None, description="Warning messages")
    measure_count: Optional[int] = Field(None, description="Number of measures")
    execution_time_ms: Optional[int] = Field(None, description="Execution time in ms")


class ConversionHistoryResponse(ConversionHistoryBase):
    """Schema for conversion history responses"""
    id: int = Field(..., description="Unique identifier")
    job_id: Optional[str] = Field(None, description="Associated job ID if async")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    warnings: Optional[List[str]] = Field(None, description="Warning messages")
    execution_time_ms: Optional[int] = Field(None, description="Execution time in milliseconds")
    converter_version: Optional[str] = Field(None, description="Converter version")
    group_id: Optional[str] = Field(None, description="Group ID for multi-tenant isolation")
    created_by_email: Optional[str] = Field(None, description="Creator email")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    # Optional: Include full data (can be large)
    input_data: Optional[Dict[str, Any]] = Field(None, description="Input data")
    output_data: Optional[Dict[str, Any]] = Field(None, description="Output data")

    model_config: ClassVar[Dict[str, Any]] = {
        "from_attributes": True
    }


class ConversionHistoryListResponse(BaseModel):
    """Schema for list of conversion history entries"""
    history: List[ConversionHistoryResponse] = Field(..., description="List of conversion history entries")
    count: int = Field(..., description="Total count")
    limit: int = Field(..., description="Limit used")
    offset: int = Field(..., description="Offset used")


class ConversionStatistics(BaseModel):
    """Schema for conversion statistics"""
    total_conversions: int = Field(..., description="Total number of conversions")
    successful: int = Field(..., description="Number of successful conversions")
    failed: int = Field(..., description="Number of failed conversions")
    success_rate: float = Field(..., description="Success rate percentage")
    average_execution_time_ms: float = Field(..., description="Average execution time in ms")
    popular_conversions: List[Dict[str, Any]] = Field(..., description="Most popular conversion paths")
    period_days: int = Field(..., description="Period in days for statistics")


# ===== CONVERSION JOB SCHEMAS =====

class ConversionJobBase(BaseModel):
    """Base ConversionJob schema"""
    name: Optional[str] = Field(None, description="Job name")
    description: Optional[str] = Field(None, description="Job description")
    source_format: str = Field(..., description="Source format")
    target_format: str = Field(..., description="Target format")
    configuration: Dict[str, Any] = Field(..., description="Converter configuration")


class ConversionJobCreate(ConversionJobBase):
    """Schema for creating a new conversion job"""
    tool_id: Optional[int] = Field(None, description="Associated tool ID")
    execution_id: Optional[str] = Field(None, description="Execution ID if part of crew")
    extra_metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class ConversionJobUpdate(BaseModel):
    """Schema for updating a conversion job"""
    name: Optional[str] = Field(None, description="Job name")
    description: Optional[str] = Field(None, description="Job description")
    status: Optional[str] = Field(None, description="Job status")
    progress: Optional[float] = Field(None, ge=0.0, le=1.0, description="Progress (0.0 to 1.0)")
    result: Optional[Dict[str, Any]] = Field(None, description="Conversion result")
    error_message: Optional[str] = Field(None, description="Error message if failed")


class ConversionJobResponse(ConversionJobBase):
    """Schema for conversion job responses"""
    id: str = Field(..., description="Job UUID")
    tool_id: Optional[int] = Field(None, description="Associated tool ID")
    status: str = Field(..., description="Job status")
    progress: Optional[float] = Field(None, description="Progress (0.0 to 1.0)")
    result: Optional[Dict[str, Any]] = Field(None, description="Conversion result")
    error_message: Optional[str] = Field(None, description="Error message")
    execution_id: Optional[str] = Field(None, description="Execution ID")
    history_id: Optional[int] = Field(None, description="Associated history ID")
    group_id: Optional[str] = Field(None, description="Group ID")
    created_by_email: Optional[str] = Field(None, description="Creator email")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    started_at: Optional[datetime] = Field(None, description="Start timestamp")
    completed_at: Optional[datetime] = Field(None, description="Completion timestamp")

    model_config: ClassVar[Dict[str, Any]] = {
        "from_attributes": True
    }


class ConversionJobListResponse(BaseModel):
    """Schema for list of conversion jobs"""
    jobs: List[ConversionJobResponse] = Field(..., description="List of conversion jobs")
    count: int = Field(..., description="Total count")


class ConversionJobStatusUpdate(BaseModel):
    """Schema for updating job status"""
    status: str = Field(..., description="New status (pending, running, completed, failed, cancelled)")
    progress: Optional[float] = Field(None, ge=0.0, le=1.0, description="Progress (0.0 to 1.0)")
    error_message: Optional[str] = Field(None, description="Error message if failed")


# ===== SAVED CONFIGURATION SCHEMAS =====

class SavedConfigurationBase(BaseModel):
    """Base SavedConverterConfiguration schema"""
    name: str = Field(..., description="Configuration name", max_length=255)
    description: Optional[str] = Field(None, description="Configuration description")
    source_format: str = Field(..., description="Source format")
    target_format: str = Field(..., description="Target format")
    configuration: Dict[str, Any] = Field(..., description="Converter configuration")
    is_public: bool = Field(default=False, description="Whether shared with group")
    is_template: bool = Field(default=False, description="Whether it's a system template")
    tags: Optional[List[str]] = Field(None, description="Tags for categorization")


class SavedConfigurationCreate(SavedConfigurationBase):
    """Schema for creating a saved configuration"""
    extra_metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class SavedConfigurationUpdate(BaseModel):
    """Schema for updating a saved configuration"""
    name: Optional[str] = Field(None, description="Configuration name", max_length=255)
    description: Optional[str] = Field(None, description="Configuration description")
    configuration: Optional[Dict[str, Any]] = Field(None, description="Converter configuration")
    is_public: Optional[bool] = Field(None, description="Whether shared with group")
    tags: Optional[List[str]] = Field(None, description="Tags for categorization")
    extra_metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class SavedConfigurationResponse(SavedConfigurationBase):
    """Schema for saved configuration responses"""
    id: int = Field(..., description="Unique identifier")
    use_count: int = Field(..., description="Number of times used")
    last_used_at: Optional[datetime] = Field(None, description="Last usage timestamp")
    group_id: Optional[str] = Field(None, description="Group ID")
    created_by_email: str = Field(..., description="Creator email")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    extra_metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")

    model_config: ClassVar[Dict[str, Any]] = {
        "from_attributes": True
    }


class SavedConfigurationListResponse(BaseModel):
    """Schema for list of saved configurations"""
    configurations: List[SavedConfigurationResponse] = Field(..., description="List of configurations")
    count: int = Field(..., description="Total count")


# ===== QUERY/FILTER SCHEMAS =====

class ConversionHistoryFilter(BaseModel):
    """Schema for filtering conversion history"""
    source_format: Optional[str] = Field(None, description="Filter by source format")
    target_format: Optional[str] = Field(None, description="Filter by target format")
    status: Optional[str] = Field(None, description="Filter by status")
    execution_id: Optional[str] = Field(None, description="Filter by execution ID")
    limit: int = Field(default=100, ge=1, le=1000, description="Number of results")
    offset: int = Field(default=0, ge=0, description="Offset for pagination")


class ConversionJobFilter(BaseModel):
    """Schema for filtering conversion jobs"""
    status: Optional[str] = Field(None, description="Filter by status")
    limit: int = Field(default=50, ge=1, le=500, description="Number of results")


class SavedConfigurationFilter(BaseModel):
    """Schema for filtering saved configurations"""
    source_format: Optional[str] = Field(None, description="Filter by source format")
    target_format: Optional[str] = Field(None, description="Filter by target format")
    is_public: Optional[bool] = Field(None, description="Filter by public status")
    is_template: Optional[bool] = Field(None, description="Filter by template status")
    search: Optional[str] = Field(None, description="Search in name")
    limit: int = Field(default=50, ge=1, le=200, description="Number of results")
