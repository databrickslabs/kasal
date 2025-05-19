from typing import List
import logging

from fastapi import APIRouter, HTTPException, status

from src.services.schema_service import SchemaService
from src.schemas.schema import SchemaCreate, SchemaUpdate, SchemaResponse, SchemaListResponse

# Create router instance
router = APIRouter(
    prefix="/schemas",
    tags=["schemas"],
    responses={404: {"description": "Not found"}},
)

# Set up logger
logger = logging.getLogger(__name__)

@router.get("", response_model=SchemaListResponse)
async def get_all_schemas() -> SchemaListResponse:
    """
    Get all schemas.
    """
    logger.info("Getting all schemas")
    schemas_response = await SchemaService.get_all_schemas()
    logger.info(f"Found {schemas_response.count} schemas")
    return schemas_response


@router.get("/by-type/{schema_type}", response_model=SchemaListResponse)
async def get_schemas_by_type(
    schema_type: str,
) -> SchemaListResponse:
    """
    Get schemas by type.
    """
    logger.info(f"Getting schemas with type '{schema_type}'")
    schemas_response = await SchemaService.get_schemas_by_type(schema_type)
    logger.info(f"Found {schemas_response.count} schemas with type '{schema_type}'")
    return schemas_response


@router.get("/{schema_name}", response_model=SchemaResponse)
async def get_schema_by_name(
    schema_name: str,
) -> SchemaResponse:
    """
    Get a schema by name.
    """
    logger.info(f"Getting schema with name '{schema_name}'")
    try:
        schema = await SchemaService.get_schema_by_name(schema_name)
        logger.info(f"Found schema with name '{schema_name}'")
        return schema
    except HTTPException as e:
        logger.warning(f"Schema retrieval failed: {str(e)}")
        raise


@router.post("", response_model=SchemaResponse, status_code=status.HTTP_201_CREATED)
async def create_schema(
    schema_data: SchemaCreate,
) -> SchemaResponse:
    """
    Create a new schema.
    """
    logger.info(f"Creating schema with name '{schema_data.name}'")
    try:
        schema = await SchemaService.create_schema(schema_data)
        logger.info(f"Created schema with name '{schema.name}'")
        return schema
    except HTTPException as e:
        logger.warning(f"Schema creation failed: {str(e)}")
        raise


@router.put("/{schema_name}", response_model=SchemaResponse)
async def update_schema(
    schema_name: str,
    schema_data: SchemaUpdate,
) -> SchemaResponse:
    """
    Update an existing schema.
    """
    logger.info(f"Updating schema with name '{schema_name}'")
    try:
        schema = await SchemaService.update_schema(schema_name, schema_data)
        logger.info(f"Updated schema with name '{schema_name}'")
        return schema
    except HTTPException as e:
        logger.warning(f"Schema update failed: {str(e)}")
        raise


@router.delete("/{schema_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schema(
    schema_name: str,
) -> None:
    """
    Delete a schema.
    """
    logger.info(f"Deleting schema with name '{schema_name}'")
    try:
        await SchemaService.delete_schema(schema_name)
        logger.info(f"Deleted schema with name '{schema_name}'")
    except HTTPException as e:
        logger.warning(f"Schema deletion failed: {str(e)}")
        raise 