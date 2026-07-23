import logging
from typing import Annotated, List

from fastapi import APIRouter, Depends, status

from src.core.dependencies import GroupContextDep, SessionDep
from src.schemas.schema import (
    SchemaCreate,
    SchemaListResponse,
    SchemaResponse,
    SchemaUpdate,
)
from src.services.schema_service import SchemaService

# Create router instance
router = APIRouter(
    prefix="/schemas",
    tags=["schemas"],
    responses={404: {"description": "Not found"}},
)

# Set up logger
logger = logging.getLogger(__name__)


async def get_schema_service(session: SessionDep) -> SchemaService:
    """
    Dependency provider for SchemaService.

    Creates service with properly injected session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI

    Returns:
        SchemaService instance with injected session
    """
    return SchemaService(session)


# Type alias for cleaner function signatures
SchemaServiceDep = Annotated[SchemaService, Depends(get_schema_service)]


@router.get("", response_model=SchemaListResponse)
async def get_all_schemas(
    service: SchemaServiceDep, group_context: GroupContextDep = None
) -> SchemaListResponse:
    """
    Get all schemas.

    Uses dependency injection to get SchemaService with repository.
    """
    logger.info("Getting all schemas")
    schemas_response = await service.get_all_schemas()
    logger.info(f"Found {schemas_response.count} schemas")
    return schemas_response


@router.get("/by-type/{schema_type}", response_model=SchemaListResponse)
async def get_schemas_by_type(
    schema_type: str, service: SchemaServiceDep, group_context: GroupContextDep = None
) -> SchemaListResponse:
    """
    Get schemas by type.

    Uses dependency injection to get SchemaService with repository.
    """
    logger.info(f"Getting schemas with type '{schema_type}'")
    schemas_response = await service.get_schemas_by_type(schema_type)
    logger.info(f"Found {schemas_response.count} schemas with type '{schema_type}'")
    return schemas_response


@router.get("/{schema_name}", response_model=SchemaResponse)
async def get_schema_by_name(
    schema_name: str, service: SchemaServiceDep, group_context: GroupContextDep = None
) -> SchemaResponse:
    """
    Get a schema by name.

    Uses dependency injection to get SchemaService with repository.
    """
    logger.info(f"Getting schema with name '{schema_name}'")
    schema = await service.get_schema_by_name(schema_name)
    logger.info(f"Found schema with name '{schema_name}'")
    return schema


@router.post("", response_model=SchemaResponse, status_code=status.HTTP_201_CREATED)
async def create_schema(
    schema_data: SchemaCreate,
    service: SchemaServiceDep,
    group_context: GroupContextDep = None,
) -> SchemaResponse:
    """
    Create a new schema.

    Uses dependency injection to get SchemaService with repository.
    """
    logger.info(f"Creating schema with name '{schema_data.name}'")
    schema = await service.create_schema(schema_data)
    logger.info(f"Created schema with name '{schema.name}'")
    return schema


@router.put("/{schema_name}", response_model=SchemaResponse)
async def update_schema(
    schema_name: str,
    schema_data: SchemaUpdate,
    service: SchemaServiceDep,
    group_context: GroupContextDep = None,
) -> SchemaResponse:
    """
    Update an existing schema.

    Uses dependency injection to get SchemaService with repository.
    """
    logger.info(f"Updating schema with name '{schema_name}'")
    schema = await service.update_schema(schema_name, schema_data)
    logger.info(f"Updated schema with name '{schema_name}'")
    return schema


@router.delete("/{schema_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schema(
    schema_name: str, service: SchemaServiceDep, group_context: GroupContextDep = None
) -> None:
    """
    Delete a schema.

    Uses dependency injection to get SchemaService with repository.
    """
    logger.info(f"Deleting schema with name '{schema_name}'")
    await service.delete_schema(schema_name)
    logger.info(f"Deleted schema with name '{schema_name}'")
