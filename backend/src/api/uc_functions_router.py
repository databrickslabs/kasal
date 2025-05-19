"""
API router for Unity Catalog function operations.

This module defines the FastAPI router for interacting with Databricks
Unity Catalog functions, including listing and retrieving function details.
"""

import logging
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_db
from src.services.uc_function_service import UCFunctionService
from src.schemas.uc_function import (
    UCFunctionListResponse,
    UCFunctionResponse,
    CatalogSchemaRequest
)

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/uc-functions",
    tags=["Unity Catalog Functions"],
    responses={404: {"description": "Not found"}},
)

# Create service dependency
async def get_uc_function_service(db: Annotated[AsyncSession, Depends(get_db)]) -> UCFunctionService:
    return UCFunctionService(db)


@router.get("/list/{catalog_name}/{schema_name}", response_model=UCFunctionListResponse)
async def list_functions(
    service: Annotated[UCFunctionService, Depends(get_uc_function_service)],
    catalog_name: str = Path(..., description="Catalog name to query"),
    schema_name: str = Path(..., description="Schema name to query"),
) -> UCFunctionListResponse:
    """
    List functions in a specified catalog and schema.
    
    This endpoint fetches all functions from the given catalog and schema in Databricks Unity Catalog.
    
    Args:
        service: Injected service dependency
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        
    Returns:
        UCFunctionListResponse: List of functions with count and query details
    """
    logger.info(f"Listing functions in {catalog_name}.{schema_name}")
    try:
        return await service.list_functions(catalog_name, schema_name)
    except Exception as e:
        logger.error(f"Error listing functions: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error listing functions: {str(e)}"
        )


@router.get("/details/{catalog_name}/{schema_name}/{function_name}", response_model=UCFunctionResponse)
async def get_function_details(
    service: Annotated[UCFunctionService, Depends(get_uc_function_service)],
    catalog_name: str = Path(..., description="Catalog name to query"),
    schema_name: str = Path(..., description="Schema name to query"),
    function_name: str = Path(..., description="Function name to query"),
) -> UCFunctionResponse:
    """
    Get details of a specific function.
    
    This endpoint fetches details of a specific function from the given catalog and schema 
    in Databricks Unity Catalog.
    
    Args:
        service: Injected service dependency
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        function_name: Name of the function
        
    Returns:
        UCFunctionResponse: Function details with query information
    """
    logger.info(f"Getting function details for {catalog_name}.{schema_name}.{function_name}")
    try:
        return await service.get_function(catalog_name, schema_name, function_name)
    except ValueError as e:
        logger.warning(f"Function not found: {str(e)}")
        raise HTTPException(
            status_code=404,
            detail=f"Function not found: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error getting function details: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error getting function details: {str(e)}"
        )


@router.post("/list", response_model=UCFunctionListResponse)
async def list_functions_post(
    service: Annotated[UCFunctionService, Depends(get_uc_function_service)],
    request: CatalogSchemaRequest,
) -> UCFunctionListResponse:
    """
    List functions in a specified catalog and schema (POST method).
    
    This endpoint provides the same functionality as the GET list functions endpoint,
    but accepts catalog and schema through a request body instead of URL parameters.
    
    Args:
        service: Injected service dependency
        request: Request with catalog and schema names
        
    Returns:
        UCFunctionListResponse: List of functions with count and query details
    """
    logger.info(f"POST: Listing functions in {request.catalog_name}.{request.schema_name}")
    try:
        return await service.list_functions(request.catalog_name, request.schema_name)
    except Exception as e:
        logger.error(f"Error listing functions: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error listing functions: {str(e)}"
        )