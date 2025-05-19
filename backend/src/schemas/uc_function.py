"""
Pydantic schemas for Unity Catalog functions.

These schemas define the data structures for Unity Catalog function-related
API operations, such as listing functions and retrieving function details.
"""

from typing import List, Optional
from pydantic import BaseModel, Field


class FunctionParameter(BaseModel):
    """Schema for a function parameter."""
    
    name: str = Field(
        description="Name of the parameter"
    )
    param_type: str = Field(
        description="Data type of the parameter"
    )
    description: Optional[str] = Field(
        None, 
        description="Description of the parameter"
    )


class UCFunction(BaseModel):
    """Schema for a Unity Catalog function."""
    
    name: str = Field(
        description="Name of the function"
    )
    comment: Optional[str] = Field(
        None,
        description="Comment or description of the function"
    )
    return_type: str = Field(
        description="Return type of the function"
    )
    input_params: List[FunctionParameter] = Field(
        default_factory=list,
        description="List of input parameters for the function"
    )
    catalog_name: Optional[str] = Field(
        None,
        description="Catalog containing the function"
    )
    schema_name: Optional[str] = Field(
        None, 
        description="Schema containing the function"
    )


class UCFunctionListResponse(BaseModel):
    """Response model for function listing."""
    
    functions: List[UCFunction] = Field(
        description="List of functions"
    )
    count: int = Field(
        description="Total count of functions"
    )
    catalog_name: str = Field(
        description="Catalog name used for the query"
    )
    schema_name: str = Field(
        description="Schema name used for the query"
    )


class UCFunctionResponse(BaseModel):
    """Response model for a single function."""
    
    function: UCFunction = Field(
        description="Function details"
    )
    catalog_name: str = Field(
        description="Catalog name used for the query"
    )
    schema_name: str = Field(
        description="Schema name used for the query"
    )


class CatalogSchemaRequest(BaseModel):
    """Request model for catalog and schema specification."""
    
    catalog_name: str = Field(
        description="Name of the catalog"
    )
    schema_name: str = Field(
        description="Name of the schema"
    ) 