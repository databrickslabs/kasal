"""
Power BI and DAX Generation Schemas

Pydantic schemas for Power BI integration and DAX query generation.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class PowerBIConnectionConfig(BaseModel):
    """Configuration for Power BI connection."""

    xmla_endpoint: str = Field(
        ...,
        description="Power BI XMLA endpoint (e.g., powerbi://api.powerbi.com/v1.0/myorg/workspace)",
        examples=["powerbi://api.powerbi.com/v1.0/myorg/test_workspace"]
    )
    dataset_name: str = Field(
        ...,
        description="Name of the Power BI dataset/semantic model",
        examples=["SalesDataset"]
    )
    tenant_id: Optional[str] = Field(
        None,
        description="Azure AD tenant ID for service principal authentication"
    )
    client_id: Optional[str] = Field(
        None,
        description="Service principal client ID"
    )
    client_secret: Optional[str] = Field(
        None,
        description="Service principal client secret"
    )

    @field_validator('xmla_endpoint')
    @classmethod
    def validate_xmla_endpoint(cls, v: str) -> str:
        """Validate XMLA endpoint format."""
        if not v.startswith('powerbi://'):
            raise ValueError('XMLA endpoint must start with powerbi://')
        return v


class TableMetadata(BaseModel):
    """Metadata for a Power BI table."""

    name: str = Field(..., description="Table name")
    description: Optional[str] = Field(None, description="Table description")
    columns: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of columns with their metadata"
    )
    relationships: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Relationships to other tables"
    )


class DatasetMetadata(BaseModel):
    """Complete metadata for a Power BI dataset."""

    tables: List[TableMetadata] = Field(
        default_factory=list,
        description="List of tables in the dataset"
    )
    dataset_name: str = Field(..., description="Name of the dataset")


class DAXGenerationRequest(BaseModel):
    """Request to generate a DAX query from natural language."""

    question: str = Field(
        ...,
        description="Natural language question about the data",
        examples=["What is the total NSR per product?"]
    )
    metadata: Dict[str, Any] = Field(
        ...,
        description="Power BI dataset metadata (tables, columns, relationships)"
    )
    sample_data: Optional[Dict[str, List[Dict[str, Any]]]] = Field(
        None,
        description="Optional sample data from tables for better context"
    )
    model_name: str = Field(
        default="databricks-meta-llama-3-1-405b-instruct",
        description="LLM model to use for DAX generation"
    )
    temperature: float = Field(
        default=0.1,
        ge=0.0,
        le=2.0,
        description="Temperature for LLM generation (0.0-2.0)"
    )

    @field_validator('question')
    @classmethod
    def validate_question(cls, v: str) -> str:
        """Validate question is not empty."""
        if not v or not v.strip():
            raise ValueError('Question cannot be empty')
        return v.strip()


class DAXGenerationResponse(BaseModel):
    """Response containing generated DAX query."""

    dax_query: str = Field(..., description="Generated DAX query")
    explanation: str = Field(..., description="Explanation of what the query does")
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence score for the generated query (0-1)"
    )
    raw_response: Optional[str] = Field(
        None,
        description="Raw LLM response before parsing"
    )


class QuestionSuggestionRequest(BaseModel):
    """Request to suggest questions based on dataset metadata."""

    metadata: Dict[str, Any] = Field(
        ...,
        description="Power BI dataset metadata"
    )
    model_name: str = Field(
        default="databricks-meta-llama-3-1-405b-instruct",
        description="LLM model to use"
    )
    num_suggestions: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Number of questions to suggest (1-20)"
    )


class QuestionSuggestionResponse(BaseModel):
    """Response containing suggested questions."""

    questions: List[str] = Field(
        ...,
        description="List of suggested questions"
    )


class PowerBIToolInput(BaseModel):
    """Input schema for PowerBI CrewAI tool."""

    question: str = Field(
        ...,
        description="Natural language question to convert to DAX"
    )
    xmla_endpoint: Optional[str] = Field(
        None,
        description="Power BI XMLA endpoint (uses configured default if not provided)"
    )
    dataset_name: Optional[str] = Field(
        None,
        description="Dataset name (uses configured default if not provided)"
    )
    include_execution_instructions: bool = Field(
        default=True,
        description="Whether to include instructions for executing the DAX in Databricks"
    )
