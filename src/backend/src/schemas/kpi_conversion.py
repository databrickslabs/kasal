"""Pydantic schemas for KPI conversion API"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from enum import Enum


class ConversionFormat(str, Enum):
    """Supported conversion formats"""
    YAML = "yaml"
    DAX = "dax"
    SQL = "sql"
    UC_METRICS = "uc_metrics"
    POWERBI = "powerbi"


class ConversionRequest(BaseModel):
    """Request model for KPI conversion"""
    source_format: ConversionFormat = Field(..., description="Source format of the input data")
    target_format: ConversionFormat = Field(..., description="Target format for conversion")
    input_data: Any = Field(..., description="Input data to convert")
    config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional configuration for conversion behavior"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "source_format": "yaml",
                "target_format": "dax",
                "input_data": {
                    "description": "Sales Metrics",
                    "technical_name": "SALES_METRICS",
                    "kpis": [
                        {
                            "description": "Total Revenue",
                            "formula": "SUM(Sales[Amount])"
                        }
                    ]
                },
                "config": {
                    "optimize": True,
                    "validate": True
                }
            }
        }


class ConversionResponse(BaseModel):
    """Response model for KPI conversion"""
    success: bool = Field(..., description="Whether conversion succeeded")
    source_format: ConversionFormat = Field(..., description="Original source format")
    target_format: ConversionFormat = Field(..., description="Target format")
    output_data: Any = Field(..., description="Converted data in target format")
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Additional metadata about the conversion"
    )
    warnings: Optional[List[str]] = Field(
        default=None,
        description="Any warnings generated during conversion"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "source_format": "yaml",
                "target_format": "dax",
                "output_data": {
                    "measures": [
                        {
                            "name": "Total Revenue",
                            "dax_formula": "Total Revenue = SUM(Sales[Amount])"
                        }
                    ]
                },
                "metadata": {
                    "measures_count": 1,
                    "conversion_time_ms": 125
                },
                "warnings": []
            }
        }


class ConversionPath(BaseModel):
    """Represents a supported conversion path"""
    source: ConversionFormat
    target: ConversionFormat
    description: Optional[str] = None


class ConversionFormatsResponse(BaseModel):
    """Response model for available conversion formats"""
    formats: List[ConversionFormat] = Field(..., description="All available formats")
    conversion_paths: List[ConversionPath] = Field(
        ...,
        description="Supported conversion paths"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "formats": ["yaml", "dax", "sql", "uc_metrics", "powerbi"],
                "conversion_paths": [
                    {"source": "yaml", "target": "dax"},
                    {"source": "yaml", "target": "sql"},
                    {"source": "yaml", "target": "uc_metrics"},
                    {"source": "powerbi", "target": "yaml"}
                ]
            }
        }


class ValidateRequest(BaseModel):
    """Request model for validation"""
    format: ConversionFormat = Field(..., description="Format of the data to validate")
    input_data: Any = Field(..., description="Data to validate")

    class Config:
        json_schema_extra = {
            "example": {
                "format": "yaml",
                "input_data": {
                    "description": "Sales Metrics",
                    "technical_name": "SALES_METRICS",
                    "kbis": []
                }
            }
        }


class ValidationError(BaseModel):
    """Validation error detail"""
    field: Optional[str] = Field(None, description="Field that caused the error")
    message: str = Field(..., description="Error message")
    severity: str = Field(..., description="Error severity: error, warning, info")


class ValidationResponse(BaseModel):
    """Response model for validation"""
    valid: bool = Field(..., description="Whether the data is valid")
    errors: List[ValidationError] = Field(
        default_factory=list,
        description="List of validation errors"
    )
    warnings: List[ValidationError] = Field(
        default_factory=list,
        description="List of validation warnings"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "valid": False,
                "errors": [
                    {
                        "field": "kbis",
                        "message": "At least one KBI is required",
                        "severity": "error"
                    }
                ],
                "warnings": []
            }
        }
