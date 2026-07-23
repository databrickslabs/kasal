"""
Memory backend configuration API endpoints.

This module provides API endpoints for managing memory backend configurations,
including validation, testing connections, and retrieving available indexes.
"""

import os
from datetime import datetime
from typing import Annotated, Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.dependencies import GroupContextDep, SessionDep
from src.core.exceptions import (
    BadRequestError,
    ForbiddenError,
    KasalError,
    NotFoundError,
)
from src.core.logger import LoggerManager
from src.core.permissions import check_role_in_context, is_workspace_admin
from src.models.memory_backend import MemoryBackend
from src.schemas.memory_backend import (
    DatabricksMemoryConfig,
    MemoryBackendConfig,
    MemoryBackendCreate,
    MemoryBackendResponse,
    MemoryBackendType,
    MemoryBackendUpdate,
)
from src.services.memory_backend_service import MemoryBackendService
from src.utils.databricks_auth import extract_user_token_from_request
from src.utils.memory_paths import local_memory_store_dir

logger = LoggerManager.get_instance().api

router = APIRouter(prefix="/memory-backend", tags=["memory-backend"])

# The local (LanceDB) storage layer limits the table SCAN before sorting by
# created_at, so a small limit yields an arbitrary slice rather than the newest
# records. When browsing we scan the whole store (up to this cap) and sort +
# paginate ourselves so the returned page is truly the newest. Matches the
# storage layer's own scan cap.
_BROWSE_FULL_SCAN_LIMIT = 50_000


# Dependency to get MemoryBackendService with injected session
def get_memory_backend_service(session: SessionDep) -> MemoryBackendService:
    """Get MemoryBackendService instance with injected session."""
    return MemoryBackendService(session)


@router.get("/databricks/workspace-url")
async def get_workspace_url(
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
    group_context: GroupContextDep,
) -> Dict[str, Any]:
    """
    Get the Databricks workspace URL from environment or configuration.

    Returns:
        Dict with workspace URL if available, or None
    """
    result = await service.get_workspace_url()
    return result


@router.post("/lakebase/test-connection")
async def test_lakebase_connection(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
    request: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Test connection to Lakebase and verify pgvector extension availability.

    Args:
        request: Optional dict with instance_name

    Returns:
        Connection test result
    """
    try:
        instance_name = request.get("instance_name") if request else None
        result = await service.test_lakebase_connection(instance_name=instance_name)
        return result
    except Exception as e:
        logger.error(f"Error testing Lakebase connection: {e}")
        return {
            "success": False,
            "message": f"Connection test failed: {str(e)}",
            "details": {"error": str(e)},
        }


@router.post("/lakebase/initialize-tables")
async def initialize_lakebase_tables(
    request: Dict[str, Any],
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Create pgvector extension and memory tables on Lakebase.
    Only workspace admins can initialize tables.

    Args:
        request: Table configuration overrides including optional instance_name
        group_context: Current group context
        service: Memory backend service

    Returns:
        Table initialization result
    """
    if not is_workspace_admin(group_context):
        raise ForbiddenError(
            "Only workspace admins can initialize Lakebase memory tables"
        )

    instance_name = request.get("instance_name")
    embedding_dimension = request.get("embedding_dimension", 1024)
    memory_table = request.get("memory_table", "crew_memory")

    result = await service.initialize_lakebase_tables(
        embedding_dimension=embedding_dimension,
        memory_table=memory_table,
        instance_name=instance_name,
    )
    return result


@router.get("/lakebase/table-stats")
async def get_lakebase_table_stats(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
    instance_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get row counts per memory table on Lakebase.

    Args:
        instance_name: Optional Lakebase instance name

    Returns:
        Table statistics
    """
    result = await service.get_lakebase_table_stats(
        instance_name=instance_name,
        group_id=group_context.primary_group_id,
    )
    return result


@router.get("/lakebase/table-data")
async def get_lakebase_table_data(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
    table_name: str = Query(..., description="Memory table name to query"),
    limit: int = Query(50, description="Maximum rows to return"),
    instance_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch rows from a Lakebase memory table.

    Args:
        table_name: Name of the memory table (crew_short_term_memory, etc.)
        limit: Maximum number of rows to return (default: 50)
        instance_name: Optional Lakebase instance name

    Returns:
        Dict with success, documents list, and total count
    """
    result = await service.get_lakebase_table_data(
        table_name=table_name,
        limit=limit,
        instance_name=instance_name,
        group_id=group_context.primary_group_id,
    )
    return result


@router.get("/lakebase/entity-data")
async def get_lakebase_entity_data(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
    memory_table: str = Query(
        "crew_memory",
        description="Unified memory table to read entities from",
    ),
    limit: int = Query(200, description="Maximum entities to return"),
    instance_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch entity-like memory records from the unified Lakebase memory table.

    Returns entities and relationships in the same format as the
    Databricks entity-data endpoint, suitable for the EntityGraphVisualization
    component.

    Args:
        memory_table: Name of the unified memory table (default "crew_memory").
        limit: Maximum number of entities to return.
        instance_name: Optional Lakebase instance name.

    Returns:
        Dict with entities and relationships lists.
    """
    result = await service.get_lakebase_entity_data(
        memory_table=memory_table,
        limit=limit,
        instance_name=instance_name,
        group_id=group_context.primary_group_id,
    )
    return result


@router.post("/lakebase/save-config")
async def save_lakebase_config(
    request: Dict[str, Any],
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Save Lakebase memory backend configuration.

    Deletes existing configs and creates a new Lakebase config,
    similar to the Databricks one-click-setup pattern.
    """
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can configure memory backends")

    group_id = group_context.primary_group_id
    lakebase_config = request.get("lakebase_config", {})
    # Cognitive tuning knobs (recall speed, exploration budget, memory LLM) are
    # optional; persist them on the same config so crew execution picks them up
    # via ``active_config.cognitive_config``.
    cognitive_config = request.get("cognitive_config")

    # Create the new Lakebase config FIRST. Deleting the existing configs before
    # this hits the "Cannot delete the only memory backend configuration" guard
    # whenever exactly one config exists, leaving the setup half-done (warning +
    # a stale leftover config). Creating first keeps the count > 0 so the
    # subsequent cleanup of the OLD configs never trips that guard.
    from src.schemas.memory_backend import CognitiveMemoryConfig, LakebaseMemoryConfig

    config = MemoryBackendCreate(
        name=f"Lakebase Setup {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        backend_type=MemoryBackendType.LAKEBASE,
        lakebase_config=LakebaseMemoryConfig(**lakebase_config),
        cognitive_config=(
            CognitiveMemoryConfig(**cognitive_config) if cognitive_config else None
        ),
    )
    backend = await service.create_memory_backend(group_id, config)

    # Now remove the OLD configs (everything except the one we just created).
    try:
        existing = await service.get_memory_backends(group_id)
        for old in existing:
            if str(old.id) != str(backend.id):
                await service.delete_memory_backend(group_id, str(old.id))
    except Exception as e:
        logger.warning(f"Error cleaning up existing configs: {e}")

    await service.set_default_backend(group_id, str(backend.id))

    return {
        "success": True,
        "backend_id": str(backend.id),
        "message": "Lakebase memory backend configured successfully",
    }


@router.post("/default/save-config")
async def save_default_config(
    request: Dict[str, Any],
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """Save the local (DEFAULT / LanceDB) memory backend configuration.

    Local memory has no connection settings, but it DOES carry cognitive tuning
    (recall weights, query-analysis threshold, exploration budget, memory LLM).
    Those only take effect when persisted on an ACTIVE config that crew
    execution loads via ``get_active_config`` — saving them to the browser's
    localStorage never reaches the runtime, so the tuning was silently ignored
    for local memory. Mirror the Lakebase save flow: create an active DEFAULT
    config carrying ``cognitive_config``, then remove the old configs.
    """
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can configure memory backends")

    group_id = group_context.primary_group_id
    cognitive_config = request.get("cognitive_config")

    from src.schemas.memory_backend import CognitiveMemoryConfig

    # Create the new config FIRST (count stays > 0 so the "cannot delete the only
    # config" guard never trips), then clean up the OLD ones — same ordering as
    # the Lakebase setup above.
    config = MemoryBackendCreate(
        name=f"Local (LanceDB) {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        backend_type=MemoryBackendType.DEFAULT,
        cognitive_config=(
            CognitiveMemoryConfig(**cognitive_config) if cognitive_config else None
        ),
    )
    backend = await service.create_memory_backend(group_id, config)

    try:
        existing = await service.get_memory_backends(group_id)
        for old in existing:
            if str(old.id) != str(backend.id):
                await service.delete_memory_backend(group_id, str(old.id))
    except Exception as e:
        logger.warning(f"Error cleaning up existing configs: {e}")

    await service.set_default_backend(group_id, str(backend.id))

    return {
        "success": True,
        "backend_id": str(backend.id),
        "message": "Local memory backend configured successfully",
    }


@router.post("/validate")
async def validate_memory_config(
    config: MemoryBackendConfig,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
    group_context: GroupContextDep,
) -> Dict[str, Any]:
    """
    Validate memory backend configuration.

    Args:
        config: Memory backend configuration to validate
        group_context: Current group context

    Returns:
        Validation result with any errors
    """
    errors = []

    # Basic validation
    if config.backend_type == "databricks":
        if not config.databricks_config:
            errors.append("Databricks configuration is required for Databricks backend")
        else:
            if not config.databricks_config.endpoint_name:
                errors.append("Endpoint name is required")
            if not config.databricks_config.memory_index:
                errors.append("Unified memory index is required")
            if config.databricks_config.embedding_dimension < 1:
                errors.append("Embedding dimension must be positive")

    if config.backend_type == "lakebase":
        if not config.lakebase_config:
            errors.append("Lakebase configuration is required for Lakebase backend")
        elif not config.lakebase_config.memory_table:
            errors.append("Unified memory table is required")

    return {"valid": len(errors) == 0, "errors": errors}


@router.post("/databricks/test-connection")
async def test_databricks_connection(
    config: DatabricksMemoryConfig,
    request: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Test connection to Databricks Vector Search.

    Args:
        config: Databricks configuration
        request: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Connection test result
    """
    try:
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(request)

        # Service is injected via dependency
        result = await service.test_databricks_connection(config, user_token)
        return result

    except Exception as e:
        logger.error(f"Error testing Databricks connection: {e}")
        return {
            "success": False,
            "message": f"Connection test failed: {str(e)}",
            "details": {"error": str(e)},
        }


@router.post("/databricks/indexes")
async def get_databricks_indexes(
    config: DatabricksMemoryConfig,
    request: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Get available indexes for a Databricks endpoint.

    Args:
        config: Databricks configuration
        request: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        List of available indexes
    """
    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(request)

    # Get indexes from the service
    result = await service.get_databricks_indexes(config, user_token)
    return result


@router.post("/databricks/create-index")
async def create_databricks_index(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Create a new Databricks Vector Search index.
    Only workspace admins can create Databricks indexes.

    Args:
        request: Request containing index creation parameters
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Index creation result
    """
    # Check permissions - only workspace admins can create indexes
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can create Databricks indexes")

    # Extract parameters
    try:
        config = DatabricksMemoryConfig(**request.get("config", {}))
    except Exception as e:
        raise BadRequestError(f"Invalid Databricks configuration: {str(e)}")

    index_type = request.get("index_type")
    catalog = request.get("catalog")
    schema = request.get("schema")
    table_name = request.get("table_name")
    primary_key = request.get("primary_key", "id")

    # Validate required parameters
    if not all([index_type, catalog, schema, table_name]):
        raise BadRequestError(
            "index_type, catalog, schema, and table_name are required"
        )

    if index_type not in ["short_term", "long_term", "entity", "document"]:
        raise BadRequestError(
            "index_type must be one of: short_term, long_term, entity, document"
        )

    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Create the index
    result = await service.create_databricks_index(
        config=config,
        index_type=index_type,
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        primary_key=primary_key,
        user_token=user_token,
    )

    return result


@router.post("/configs", response_model=MemoryBackendResponse)
async def create_memory_config(
    config: MemoryBackendCreate,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> MemoryBackendResponse:
    """
    Create a new memory backend configuration.
    Only workspace admins can create memory configurations for their workspace.

    Args:
        config: Memory backend configuration
        group_context: Current group context
        service: Memory backend service

    Returns:
        Created memory backend configuration
    """
    # Check permissions - only workspace admins can create memory configs
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can create memory configurations")

    # Service is injected via dependency
    backend = await service.create_memory_backend(
        group_context.primary_group_id, config
    )
    return MemoryBackendResponse.model_validate(backend)


@router.get("/configs", response_model=List[MemoryBackendResponse])
async def get_memory_configs(
    request: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> List[MemoryBackendResponse]:
    """
    Get all memory backend configurations for the current user.

    Args:
        group_context: Current group context
        service: Memory backend service

    Returns:
        List of memory backend configurations
    """
    # Only log group context at debug level for frequently called endpoint
    logger.debug(f"Getting memory backends for group: {group_context.primary_group_id}")
    backends = await service.get_memory_backends(group_context.primary_group_id)
    logger.debug(f"Found {len(backends)} backends for group")

    return [MemoryBackendResponse.model_validate(backend) for backend in backends]


@router.get("/configs/default", response_model=Optional[MemoryBackendResponse])
async def get_default_memory_config(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Optional[MemoryBackendResponse]:
    """
    Get the default memory backend configuration for the current user.

    Args:
        group_context: Current group context
        service: Memory backend service

    Returns:
        Default memory backend configuration or None
    """
    # Service is injected via dependency
    logger.debug(
        f"Getting default memory backend for group: {group_context.primary_group_id}"
    )
    backend = await service.get_default_memory_backend(group_context.primary_group_id)

    if backend:
        logger.debug(f"Found default backend: {backend.name}")
        return MemoryBackendResponse.model_validate(backend)
    else:
        logger.debug(
            f"No default backend found for group: {group_context.primary_group_id}"
        )
        return None


@router.get("/configs/{backend_id}", response_model=MemoryBackendResponse)
async def get_memory_config_by_id(
    backend_id: str,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> MemoryBackendResponse:
    """
    Get a specific memory backend configuration.

    Args:
        backend_id: Backend ID
        group_context: Current group context
        service: Memory backend service

    Returns:
        Memory backend configuration
    """
    # Service is injected via dependency
    backend = await service.get_memory_backend(
        group_context.primary_group_id, backend_id
    )

    if not backend:
        raise NotFoundError("Memory backend configuration not found")

    return MemoryBackendResponse.model_validate(backend)


@router.put("/configs/{backend_id}", response_model=MemoryBackendResponse)
async def update_memory_config(
    backend_id: str,
    update_data: MemoryBackendUpdate,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> MemoryBackendResponse:
    """
    Update a memory backend configuration.
    Only workspace admins can update memory configurations for their workspace.

    Args:
        backend_id: Backend ID
        update_data: Update data
        group_context: Current group context
        service: Memory backend service

    Returns:
        Updated memory backend configuration
    """
    # Check permissions - only workspace admins can update memory configs
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can update memory configurations")

    # Service is injected via dependency
    backend = await service.update_memory_backend(
        group_context.primary_group_id, backend_id, update_data
    )

    if not backend:
        raise NotFoundError("Memory backend configuration not found")

    return MemoryBackendResponse.model_validate(backend)


@router.delete("/configs/{backend_id}")
async def delete_memory_config(
    backend_id: str,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Delete a memory backend configuration.
    Only workspace admins can delete memory configurations for their workspace.

    Args:
        backend_id: Backend ID
        group_context: Current group context
        service: Memory backend service

    Returns:
        Success status
    """
    # Check permissions - only workspace admins can delete memory configs
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can delete memory configurations")

    # Service is injected via dependency
    success = await service.delete_memory_backend(
        group_context.primary_group_id, backend_id
    )

    if not success:
        raise NotFoundError("Memory backend configuration not found")

    return {"success": True, "message": "Memory backend configuration deleted"}


@router.post("/configs/{backend_id}/set-default")
async def set_default_memory_config(
    backend_id: str,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Set a memory backend configuration as default.

    Args:
        backend_id: Backend ID
        group_context: Current group context
        service: Memory backend service

    Returns:
        Success status
    """
    # Service is injected via dependency
    success = await service.set_default_backend(
        group_context.primary_group_id, backend_id
    )

    if not success:
        raise NotFoundError("Memory backend configuration not found")

    return {"success": True, "message": "Default memory backend configuration set"}


@router.get("/stats/{crew_id}")
async def get_memory_stats(
    crew_id: str,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Get memory usage statistics for a crew.

    Args:
        crew_id: Crew identifier
        group_context: Current group context
        service: Memory backend service

    Returns:
        Memory usage statistics
    """
    # Service is injected via dependency
    stats = await service.get_memory_stats(group_context.primary_group_id, crew_id)
    return stats


@router.post("/databricks/one-click-setup")
async def one_click_databricks_setup(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    One-click setup for Databricks Vector Search.
    Creates all endpoints and indexes automatically.
    Only workspace admins can set up memory backend for their workspace.

    Args:
        request: Request containing workspace_url, catalog, and schema
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Setup result with created resources
    """
    # Check permissions - only workspace admins can set up memory backend
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can set up memory backend")

    # CRITICAL: Set UserContext for authentication system to access group_id
    # The authentication system needs group_id to look up PAT tokens from database
    from src.utils.user_context import UserContext

    UserContext.set_group_context(group_context)
    logger.info(
        f"[ONE-CLICK-SETUP] Set UserContext with group_id: {group_context.primary_group_id}"
    )

    # Get workspace URL from unified auth or user request
    workspace_url = request.get("workspace_url")

    # Try to get from unified auth first
    if not workspace_url:
        try:
            from src.utils.databricks_auth import get_auth_context

            auth = await get_auth_context()
            if auth and auth.workspace_url:
                workspace_url = auth.workspace_url
                logger.info(
                    f"Using workspace URL from unified {auth.auth_method} auth: {workspace_url}"
                )
        except Exception as e:
            logger.warning(f"Failed to get unified auth: {e}")

    if not workspace_url:
        raise BadRequestError(
            "workspace_url is required and not available from unified auth"
        )

    catalog = request.get("catalog", "ml")
    schema = request.get("schema", "agents")
    embedding_dimension = request.get(
        "embedding_dimension", 1024
    )  # Default to 1024 for databricks-gte-large-en

    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Run one-click setup with user_id from group context
    logger.info(f"Starting one-click setup for group: {group_context.primary_group_id}")
    logger.info(
        f"Workspace URL: {workspace_url}, Catalog: {catalog}, Schema: {schema}, Embedding dimension: {embedding_dimension}"
    )

    result = await service.one_click_databricks_setup(
        workspace_url=workspace_url,
        catalog=catalog,
        schema=schema,
        embedding_dimension=embedding_dimension,
        user_token=user_token,
        group_id=group_context.primary_group_id,  # Pass group_id from group context
    )

    logger.info(f"One-click setup result: {result}")

    return result


@router.post("/clear/{crew_id}")
async def clear_crew_memory(
    crew_id: str,
    request: Dict[str, List[str]],
    group_context: GroupContextDep,
) -> Dict[str, Any]:
    """
    Clear memory for a specific crew.

    Args:
        crew_id: Crew identifier
        request: Memory types to clear
        group_context: Current group context

    Returns:
        Success status
    """
    memory_types = request.get("memory_types", [])
    if not memory_types:
        raise BadRequestError("No memory types specified")

    # In a real implementation, clear the actual memory
    logger.info(f"Clearing {memory_types} memory for crew {crew_id}")

    return {
        "success": True,
        "message": f"Cleared {', '.join(memory_types)} memory for crew {crew_id}",
    }


@router.get("/databricks/verify-resources")
async def verify_databricks_resources(
    workspace_url: str,
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
    backend_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Verify which Databricks resources actually exist.

    Args:
        workspace_url: Databricks workspace URL
        group_context: Current group context
        service: Memory backend service
        backend_id: Optional backend ID to verify specific configuration

    Returns:
        Dict with existing resources information
    """
    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)
    logger.info(f"Extracted user token from request: {bool(user_token)}")

    # Get the configuration to check
    config = None
    if backend_id:
        # Get specific backend configuration
        config = await service.get_memory_backend(
            group_context.primary_group_id, backend_id
        )
        logger.info(f"Using specific backend config: {backend_id}")
    else:
        # Get default configuration
        config = await service.get_default_memory_backend(
            group_context.primary_group_id
        )
        logger.info(f"Using default backend config")

    # Use the service to verify resources
    result = await service.verify_databricks_resources(
        workspace_url, user_token, config
    )

    return result


@router.get("/databricks/endpoint-status")
async def get_databricks_endpoint_status(
    workspace_url: str,
    endpoint_name: str,
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Get the status of a Databricks Vector Search endpoint.

    Args:
        workspace_url: Databricks workspace URL
        endpoint_name: Name of the endpoint to check
        group_context: Current group context
        service: Memory backend service

    Returns:
        Dict with endpoint status information
    """
    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Use the service to get endpoint status
    result = await service.get_databricks_endpoint_status(
        workspace_url=workspace_url, endpoint_name=endpoint_name, user_token=user_token
    )

    return result


@router.delete("/databricks/index")
async def delete_databricks_index(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Delete a Databricks Vector Search index.
    Only workspace admins can delete Databricks indexes.

    Args:
        request: Request containing deletion parameters
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Deletion result
    """
    # Check permissions - only workspace admins can delete indexes
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can delete Databricks indexes")

    # Extract parameters
    workspace_url = request.get("workspace_url")
    index_name = request.get("index_name")
    endpoint_name = request.get("endpoint_name")

    # Validate required parameters
    if not all([workspace_url, index_name, endpoint_name]):
        raise BadRequestError(
            "workspace_url, index_name, and endpoint_name are required"
        )

    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Delete the index
    result = await service.delete_databricks_index(
        workspace_url=workspace_url,
        index_name=index_name,
        endpoint_name=endpoint_name,
        user_token=user_token,
    )

    return result


@router.delete("/databricks/endpoint")
async def delete_databricks_endpoint(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Delete a Databricks Vector Search endpoint.
    Only workspace admins can delete Databricks endpoints.

    Args:
        request: Request containing deletion parameters
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Deletion result
    """
    # Check permissions - only workspace admins can delete endpoints
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can delete Databricks endpoints")

    # Extract parameters
    workspace_url = request.get("workspace_url")
    endpoint_name = request.get("endpoint_name")

    # Validate required parameters
    if not all([workspace_url, endpoint_name]):
        raise BadRequestError("workspace_url and endpoint_name are required")

    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Delete the endpoint
    result = await service.delete_databricks_endpoint(
        workspace_url=workspace_url, endpoint_name=endpoint_name, user_token=user_token
    )

    return result


@router.delete("/configs/databricks/all")
async def delete_all_databricks_configs(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Delete all Databricks memory backend configurations for the current group.
    This is used when switching to disabled mode to ensure clean state.

    Args:
        group_context: Current group context
        service: Memory backend service

    Returns:
        Success status with count of deleted configurations
    """
    # Get all memory backends for the group
    backends = await service.get_memory_backends(group_context.primary_group_id)

    deleted_count = 0
    for backend in backends:
        # Only delete Databricks backends
        if backend.backend_type == MemoryBackendType.DATABRICKS:
            success = await service.delete_memory_backend(
                group_context.primary_group_id, backend.id
            )
            if success:
                deleted_count += 1
                logger.info(f"Deleted Databricks backend: {backend.id}")

    return {
        "success": True,
        "message": f"Deleted {deleted_count} Databricks configurations",
        "deleted_count": deleted_count,
    }


@router.post("/configs/switch-to-disabled")
async def switch_to_disabled_mode(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Switch to disabled mode by deleting all memory backend configurations
    and creating a new disabled configuration.
    Only workspace admins can switch to disabled mode.

    Args:
        group_context: Current group context
        service: Memory backend service

    Returns:
        Success status with deleted count and new disabled configuration
    """
    # Check permissions - only workspace admins can switch to disabled mode
    if not is_workspace_admin(group_context):
        raise ForbiddenError(
            "Only workspace admins can switch memory backend to disabled mode"
        )

    # Delete all configurations and create disabled one
    result = await service.delete_all_and_create_disabled(
        group_context.primary_group_id
    )

    if not result["success"]:
        raise KasalError(result["message"])

    logger.info(
        f"Switched to disabled mode for group {group_context.primary_group_id}: {result['message']}"
    )

    return result


@router.delete("/configs/disabled/cleanup")
async def cleanup_disabled_configs(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Delete all disabled (DEFAULT type) memory backend configurations.
    This is used when switching from disabled to enabled mode.

    Args:
        group_context: Current group context
        service: Memory backend service

    Returns:
        Success status with count of deleted configurations
    """
    # Delete all disabled configurations
    deleted_count = await service.delete_disabled_configurations(
        group_context.primary_group_id
    )

    logger.info(
        f"Cleaned up {deleted_count} disabled configurations for group {group_context.primary_group_id}"
    )

    return {
        "success": True,
        "deleted_count": deleted_count,
        "message": f"Deleted {deleted_count} disabled configurations",
    }


@router.get("/databricks/index-info")
async def get_index_info(
    workspace_url: str,
    index_name: str,
    endpoint_name: str,
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Get information about a Databricks Vector Search index including document count.

    Args:
        workspace_url: Databricks workspace URL
        index_name: Full index name (catalog.schema.table)
        endpoint_name: Endpoint name that hosts the index
        group_context: Current group context
        service: Memory backend service

    Returns:
        Index information including document count
    """
    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Get index info
    result = await service.get_index_info(
        workspace_url=workspace_url,
        index_name=index_name,
        endpoint_name=endpoint_name,
        user_token=user_token,
    )

    return result


@router.post("/databricks/empty-index")
async def empty_index(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
) -> Dict[str, Any]:
    """
    Empty a Databricks Vector Search index by deleting and recreating it.
    Only workspace admins can empty Databricks indexes.

    Args:
        request: Request containing index parameters
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Operation result
    """
    # Check permissions - only workspace admins can empty indexes
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can empty Databricks indexes")

    # Extract parameters
    workspace_url = request.get("workspace_url")
    index_name = request.get("index_name")
    endpoint_name = request.get("endpoint_name")
    index_type = request.get("index_type")
    embedding_dimension = request.get("embedding_dimension", 1024)

    # Validate required parameters
    if not all([workspace_url, index_name, endpoint_name, index_type]):
        raise BadRequestError(
            "workspace_url, index_name, endpoint_name, and index_type are required"
        )

    if index_type not in ["short_term", "long_term", "entity", "document"]:
        raise BadRequestError(
            "index_type must be one of: short_term, long_term, entity, document"
        )

    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Empty the index
    result = await service.empty_index(
        workspace_url=workspace_url,
        index_name=index_name,
        endpoint_name=endpoint_name,
        index_type=index_type,
        embedding_dimension=embedding_dimension,
        user_token=user_token,
    )

    return result


@router.get("/databricks/index-documents")
async def get_index_documents(
    index_name: str = Query(
        ..., description="Full name of the index (catalog.schema.index)"
    ),
    workspace_url: str = Query(..., description="Databricks workspace URL"),
    endpoint_name: str = Query(..., description="Vector Search endpoint name"),
    index_type: Optional[str] = Query(
        None, description="Type of index (short_term, long_term, entity, document)"
    ),
    backend_id: Optional[str] = Query(None, description="Backend configuration ID"),
    limit: int = Query(30, description="Maximum number of documents to return"),
    request: Request = None,
    group_context: GroupContextDep = None,
    service: Annotated[
        MemoryBackendService, Depends(get_memory_backend_service)
    ] = None,
) -> Dict[str, Any]:
    """
    Retrieve documents from any Databricks Vector Search index.

    This endpoint fetches the most recent documents from a specified index
    for viewing and inspection purposes.

    Args:
        index_name: Full name of the index (catalog.schema.index)
        workspace_url: Databricks workspace URL
        endpoint_name: Name of the Vector Search endpoint
        index_type: Type of index (short_term, long_term, entity, document)
        backend_id: Backend configuration ID to retrieve embedding dimension
        limit: Maximum number of documents to return (default: 30)
        request: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Dictionary containing documents and metadata
    """
    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(request) if request else None

    # Get embedding dimension from backend config if backend_id is provided
    embedding_dimension = 1024  # Default
    if backend_id and group_context:
        try:
            backend = await service.get_memory_backend(
                group_context.primary_group_id, backend_id
            )
            if backend and backend.databricks_config:
                db_config = backend.databricks_config
                if hasattr(db_config, "embedding_dimension"):
                    embedding_dimension = db_config.embedding_dimension or 1024
                elif isinstance(db_config, dict):
                    embedding_dimension = db_config.get("embedding_dimension", 1024)
                else:
                    embedding_dimension = 1024
                logger.info(
                    f"Retrieved embedding dimension {embedding_dimension} from backend config {backend_id}"
                )
        except Exception as e:
            logger.warning(
                f"Could not retrieve embedding dimension from backend config: {e}"
            )

    # Get documents from the service
    result = await service.get_index_documents(
        workspace_url=workspace_url,
        endpoint_name=endpoint_name,
        index_name=index_name,
        index_type=index_type,
        embedding_dimension=embedding_dimension,
        limit=limit,
        user_token=user_token,
    )

    return result


@router.get("/databricks/entity-data")
async def get_entity_data(
    index_name: str = Query(..., description="Name of the entity memory index"),
    workspace_url: str = Query(..., description="Databricks workspace URL"),
    endpoint_name: str = Query(..., description="Vector Search endpoint name"),
    embedding_dimension: int = Query(
        1024, description="Dimension of embedding vectors"
    ),
    limit: int = Query(100, description="Maximum number of entities to return"),
    request: Request = None,
    group_context: GroupContextDep = None,
    service: Annotated[
        MemoryBackendService, Depends(get_memory_backend_service)
    ] = None,
) -> Dict[str, Any]:
    """
    Retrieve entity data from the entity memory index for visualization.

    This endpoint fetches entities and their relationships from the Databricks
    Vector Search entity memory index and formats them for graph visualization.

    Args:
        index_name: Full name of the entity memory index (catalog.schema.index)
        workspace_url: Databricks workspace URL
        endpoint_name: Name of the Vector Search endpoint
        embedding_dimension: Dimension of embedding vectors (default: 1024)
        limit: Maximum number of entities to return (default: 100)
        request: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Dictionary containing entities and relationships for visualization
    """
    # Import the databricks logger
    from src.core.logger import LoggerManager

    databricks_logger = LoggerManager.get_instance().databricks_vector_search

    databricks_logger.info(f"[ENTITY] API endpoint called: /databricks/entity-data")
    databricks_logger.info(
        f"[ENTITY] Parameters: index_name={index_name}, workspace_url={workspace_url}, endpoint_name={endpoint_name}, limit={limit}"
    )

    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(request) if request else None
    databricks_logger.info(
        f"[ENTITY] User token extracted: {'Yes' if user_token else 'No'}"
    )

    # Get the index service
    from src.services.databricks_index_service import DatabricksIndexService

    index_service = DatabricksIndexService()

    # Query the actual entity data from Databricks Vector Search
    result = await index_service.query_entity_data(
        workspace_url=workspace_url,
        endpoint_name=endpoint_name,
        index_name=index_name,
        embedding_dimension=embedding_dimension,
        limit=limit,
        user_token=user_token,
    )

    databricks_logger.info(
        f"[ENTITY] Query result: success={result.get('success')}, entities={len(result.get('entities', []))}, relationships={len(result.get('relationships', []))}"
    )

    # Return the actual data from the index
    return result


# ---------------------------------------------------------------------------
# Unified cognitive memory browser (CrewAI 1.10+)
# ---------------------------------------------------------------------------


@router.get("/records")
async def list_memory_records(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
    request: Request,
    scope: Optional[str] = Query(
        None,
        description="Optional hierarchical scope prefix to filter records.",
    ),
    limit: int = Query(
        50, ge=1, le=5000,
        description=(
            "Maximum number of records to return. The browser pages with a "
            "small limit for the card list, but the concept/graph views fetch "
            "the whole store in one request, so the ceiling is high."
        ),
    ),
    offset: int = Query(
        0, ge=0,
        description="Number of records to skip for pagination.",
    ),
) -> Dict[str, Any]:
    """Browse records stored in the active unified cognitive memory backend.

    Backend-agnostic: routes to LanceDB (default), Databricks Vector Search,
    or Lakebase pgvector based on the user's active ``MemoryBackend``
    configuration. Records are filtered by the caller's group (tenant).
    """
    group_id = group_context.primary_group_id
    user_token = extract_user_token_from_request(request) if request else None

    active = await service.get_active_config(group_id)
    backend_type = (
        getattr(active, "backend_type", None).value
        if active and getattr(active, "backend_type", None)
        else "default"
    )

    logger.info(
        "[memory/records] group=%s backend=%s scope=%s limit=%s offset=%s",
        group_id,
        backend_type,
        scope,
        limit,
        offset,
    )

    total = 0
    if backend_type == "databricks":
        databricks_cfg = active.databricks_config if active else None
        if not databricks_cfg or not databricks_cfg.memory_index:
            return {"backend": backend_type, "records": [], "count": 0, "total": 0,
                    "offset": offset, "limit": limit}
        records, total = await _browse_databricks_records(
            databricks_cfg,
            group_id=group_id,
            scope=scope,
            limit=limit,
            offset=offset,
            user_token=user_token,
        )
    elif backend_type == "lakebase":
        lakebase_cfg = active.lakebase_config if active else None
        if not lakebase_cfg or not lakebase_cfg.memory_table:
            return {"backend": backend_type, "records": [], "count": 0, "total": 0,
                    "offset": offset, "limit": limit}
        records, total = await _browse_lakebase_records(
            lakebase_cfg,
            group_id=group_id,
            scope=scope,
            limit=limit,
            offset=offset,
        )
    else:
        records, total = _browse_default_records(
            group_id=group_id,
            scope=scope,
            limit=limit,
            offset=offset,
        )

    return {
        "backend": backend_type,
        "records": records,
        "count": len(records),
        # Total records available in the store for this scope, so the client
        # can paginate (fetch with a larger offset) until count == total.
        "total": total,
        "offset": offset,
        "limit": limit,
    }


@router.delete("/records")
async def delete_memory_records(
    group_context: GroupContextDep,
    service: Annotated[MemoryBackendService, Depends(get_memory_backend_service)],
    request: Request,
    scope: Optional[str] = Query(
        None,
        description=(
            "Optional scope prefix. When omitted, deletes every record "
            "owned by the caller's group."
        ),
    ),
) -> Dict[str, Any]:
    """Delete cognitive-memory records from the active backend.

    The caller can only delete records for their own group — both the
    Databricks / Lakebase paths enforce ``group_id`` in the filter, and the
    local (LanceDB) path only touches the group's store directory
    ``kasal_default_<group_id>``.
    """
    group_id = group_context.primary_group_id
    user_token = extract_user_token_from_request(request) if request else None

    active = await service.get_active_config(group_id)
    backend_type = (
        getattr(active, "backend_type", None).value
        if active and getattr(active, "backend_type", None)
        else "default"
    )

    logger.info(
        "[memory/records][DELETE] group=%s backend=%s scope=%s",
        group_id,
        backend_type,
        scope,
    )

    deleted = 0
    if backend_type == "databricks":
        databricks_cfg = active.databricks_config if active else None
        if databricks_cfg and databricks_cfg.memory_index:
            deleted = await _delete_databricks_records(
                databricks_cfg,
                group_id=group_id,
                scope=scope,
                user_token=user_token,
            )
    elif backend_type == "lakebase":
        lakebase_cfg = active.lakebase_config if active else None
        if lakebase_cfg and lakebase_cfg.memory_table:
            deleted = await _delete_lakebase_records(
                lakebase_cfg,
                group_id=group_id,
                scope=scope,
            )
    else:
        deleted = _delete_default_records(group_id=group_id, scope=scope)

    return {"backend": backend_type, "deleted": deleted}


async def _browse_databricks_records(
    databricks_cfg: DatabricksMemoryConfig,
    *,
    group_id: str,
    scope: Optional[str],
    limit: int,
    offset: int,
    user_token: Optional[str],
) -> Tuple[List[Dict[str, Any]], int]:
    """Read records from a Databricks Vector Search unified-memory index.

    Returns ``(records, total)`` where ``total`` is the number of records the
    group has in the index (used by the client for pagination). The total is a
    group-level count; when a ``scope`` filter is applied it is an upper bound.
    """
    from src.repositories.databricks_vector_index_repository import (
        DatabricksVectorIndexRepository,
    )
    from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

    repo = DatabricksVectorIndexRepository(
        databricks_cfg.workspace_url or "",
        group_id=group_id,
    )
    columns = DatabricksIndexSchemas.get_search_columns("unified")
    positions = DatabricksIndexSchemas.get_column_positions("unified")
    zero_vector = [0.0] * (databricks_cfg.embedding_dimension or 1024)

    filters: Dict[str, Any] = {"group_id": group_id}
    response = await repo.similarity_search(
        index_name=databricks_cfg.memory_index,
        endpoint_name=databricks_cfg.endpoint_name,
        query_vector=zero_vector,
        columns=columns,
        num_results=limit + offset,
        filters=filters,
        user_token=user_token,
    )

    data_array = (response or {}).get("result", {}).get("data_array") or []
    records: List[Dict[str, Any]] = []
    for row in data_array[offset:]:
        record = _row_to_record_dict(row, columns, positions)
        if scope and not (record.get("scope") or "").startswith(scope):
            continue
        records.append(record)
        if len(records) >= limit:
            break

    total = offset + len(records)
    try:
        total = await repo.count_documents(
            index_name=databricks_cfg.memory_index,
            endpoint_name=databricks_cfg.endpoint_name,
            filters=filters,
            user_token=user_token,
        )
    except Exception as exc:  # pragma: no cover - best-effort count
        logger.warning(
            "Databricks memory count failed, using page-based total: %s", exc
        )
    return records, total


async def _browse_lakebase_records(
    lakebase_cfg: Any,
    *,
    group_id: str,
    scope: Optional[str],
    limit: int,
    offset: int,
) -> Tuple[List[Dict[str, Any]], int]:
    """Read records from the unified Lakebase memory table.

    Returns ``(records, total)`` where ``total`` is the count of records
    matching the group (and optional scope) filter, for client pagination.
    """
    from sqlalchemy import text

    from src.db.lakebase_session import get_lakebase_session

    instance_name = getattr(lakebase_cfg, "instance_name", None)
    table_name = lakebase_cfg.memory_table
    where = ["group_id = :group_id"]
    params: Dict[str, Any] = {
        "group_id": group_id,
        "limit": limit,
        "offset": offset,
    }
    if scope:
        where.append("metadata->>'scope' LIKE :scope_prefix")
        params["scope_prefix"] = f"{scope}%"

    where_clause = " AND ".join(where)
    async with get_lakebase_session(
        instance_name=instance_name, group_id=group_id
    ) as session:
        count_sql = text(f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}")
        total = int(
            (await session.execute(count_sql, params)).scalar() or 0
        )
        sql = text(
            f"SELECT id, content, metadata, created_at, updated_at, agent, score "
            f"FROM {table_name} "
            f"WHERE {where_clause} "
            f"ORDER BY created_at DESC "
            f"LIMIT :limit OFFSET :offset"
        )
        result = await session.execute(sql, params)
        records: List[Dict[str, Any]] = []
        for row in result.fetchall():
            metadata_val = row[2]
            metadata = metadata_val if isinstance(metadata_val, dict) else _safe_json(metadata_val)
            records.append(
                {
                    "id": row[0],
                    "content": row[1],
                    "scope": metadata.get("scope", "/"),
                    "categories": metadata.get("categories") or [],
                    "importance": float(metadata.get("importance") or row[6] or 0.5),
                    "source": metadata.get("source") or row[5] or None,
                    "private": bool(metadata.get("private") or False),
                    "metadata": {
                        k: v for k, v in metadata.items()
                        if k not in ("scope", "categories", "importance", "source", "private")
                    },
                    "created_at": str(row[3]) if row[3] else None,
                    "last_accessed": str(row[4]) if row[4] else None,
                }
            )
        return records, total


def _browse_default_records(
    *,
    group_id: str,
    scope: Optional[str],
    limit: int,
    offset: int,
) -> Tuple[List[Dict[str, Any]], int]:
    """Read records from the LOCAL LanceDB memory store for the group.

    Kasal's DEFAULT backend sets ``CREWAI_STORAGE_DIR`` to
    ``kasal_default_{group_id}`` — one workspace-scoped store — which CrewAI
    resolves as a relative name (under the backend CWD or the platform data
    dir). This helper finds that store and reads its LanceDB records.

    Returns ``(records, total)`` where ``total`` is the full record count in
    the store for this scope, so the client can paginate beyond one page.
    """
    import os
    from pathlib import Path

    try:
        from crewai.memory import Memory  # type: ignore
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Default memory browse failed (crewai.memory missing): %s", exc)
        return [], 0

    # ONE deterministic store per group at the known memory root
    # (KASAL_MEMORY_DIR) — the exact path the runtime writes to, so the browser
    # never reads a different location than the writer used. (Legacy per-crew
    # stores are intentionally NOT read; the unified backend keeps one per group.)
    store_dir = local_memory_store_dir(group_id)
    if not store_dir.is_dir():
        logger.info(
            "[memory/records] No local memory store for group %s at %s",
            group_id,
            store_dir,
        )
        return [], 0
    storage_dirs: List[Path] = [store_dir]

    aggregated: List[Dict[str, Any]] = []
    total = 0
    original_storage = os.environ.get("CREWAI_STORAGE_DIR")
    try:
        for storage_dir in storage_dirs:
            try:
                # Point CrewAI's unified Memory at this crew's store. Using
                # env var propagation keeps us compatible with whatever path
                # layout the installed crewai version expects under the
                # storage dir (memory/memories.lance on 1.14.x).
                os.environ["CREWAI_STORAGE_DIR"] = str(storage_dir)
                memory = Memory()
                storage = (
                    getattr(memory, "_storage", None)
                    or getattr(memory, "storage", None)
                )
                if storage is None or not hasattr(storage, "list_records"):
                    continue
                # True store count for this scope, so the client knows whether
                # more pages exist beyond the one being returned.
                if hasattr(storage, "count"):
                    try:
                        total += int(storage.count(scope_prefix=scope))
                    except Exception:  # pragma: no cover - best-effort
                        logger.debug("Local memory count failed", exc_info=True)
                # IMPORTANT: the storage layer's list_records limits the SCAN
                # before sorting by created_at, so a small limit returns an
                # arbitrary slice (storage order), NOT the newest records. To
                # return the true newest page we scan the whole store here and
                # sort + paginate after merging below. (Capped to avoid
                # unbounded reads; matches the storage scan cap.)
                fetched = storage.list_records(
                    scope_prefix=scope,
                    limit=_BROWSE_FULL_SCAN_LIMIT,
                    offset=0,
                )
                crew_id = storage_dir.name.removeprefix("kasal_default_")
                for r in fetched:
                    record = _memory_record_to_dict(r)
                    md = record.setdefault("metadata", {})
                    md.setdefault("_crew_id", crew_id)
                    md.setdefault("_storage_path", str(storage_dir))
                    aggregated.append(record)
            except Exception as exc:
                logger.warning(
                    "Failed to read local memory store %s: %s", storage_dir, exc
                )
    finally:
        if original_storage is None:
            os.environ.pop("CREWAI_STORAGE_DIR", None)
        else:
            os.environ["CREWAI_STORAGE_DIR"] = original_storage

    aggregated.sort(key=lambda r: r.get("created_at") or "", reverse=True)
    return aggregated[offset : offset + limit], total


def _row_to_record_dict(
    row: List[Any],
    columns: List[str],
    positions: Dict[str, int],
) -> Dict[str, Any]:
    """Map a Databricks similarity-search row to a UI-friendly record dict."""
    def at(col: str) -> Any:
        idx = positions.get(col)
        if idx is None or idx >= len(row):
            return None
        return row[idx]

    metadata = _safe_json(at("metadata")) or {}
    categories = _safe_json_list(at("categories"))
    # Promote provenance fields into metadata so the UI can render them.
    for key in ("crew_id", "agent_id", "session_id", "llm_model"):
        metadata.setdefault(key, at(key))

    return {
        "id": at("id"),
        "content": at("content"),
        "scope": at("scope") or "/",
        "categories": categories,
        "importance": float(at("importance") or 0.5),
        "source": at("source") or None,
        "private": bool(at("private") or False),
        "metadata": metadata,
        "created_at": str(at("created_at")) if at("created_at") else None,
        "last_accessed": str(at("last_accessed")) if at("last_accessed") else None,
    }


def _memory_record_to_dict(record: Any) -> Dict[str, Any]:
    """Map a ``crewai.memory.types.MemoryRecord`` into the UI payload."""
    if hasattr(record, "model_dump"):
        data = record.model_dump()
    else:
        data = dict(record.__dict__)
    created_at = data.get("created_at")
    last_accessed = data.get("last_accessed")
    return {
        "id": data.get("id"),
        "content": data.get("content") or "",
        "scope": data.get("scope") or "/",
        "categories": data.get("categories") or [],
        "importance": float(data.get("importance") or 0.5),
        "source": data.get("source"),
        "private": bool(data.get("private") or False),
        "metadata": data.get("metadata") or {},
        "created_at": str(created_at) if created_at else None,
        "last_accessed": str(last_accessed) if last_accessed else None,
    }


def _safe_json(value: Any) -> Dict[str, Any]:
    import json as _json
    if not value:
        return {}
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = _json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def _safe_json_list(value: Any) -> List[Any]:
    import json as _json
    if not value:
        return []
    if isinstance(value, list):
        return list(value)
    try:
        parsed = _json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except (TypeError, ValueError):
        return []


# ---------------------------------------------------------------------------
# Delete helpers for the unified cognitive memory browser
# ---------------------------------------------------------------------------


async def _delete_databricks_records(
    databricks_cfg: DatabricksMemoryConfig,
    *,
    group_id: str,
    scope: Optional[str],
    user_token: Optional[str],
) -> int:
    """Delete every Databricks unified-memory record that belongs to the group."""
    from src.repositories.databricks_vector_index_repository import (
        DatabricksVectorIndexRepository,
    )
    from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

    repo = DatabricksVectorIndexRepository(
        databricks_cfg.workspace_url or "",
        group_id=group_id,
    )
    columns = DatabricksIndexSchemas.get_search_columns("unified")
    positions = DatabricksIndexSchemas.get_column_positions("unified")
    zero_vector = [0.0] * (databricks_cfg.embedding_dimension or 1024)

    filters: Dict[str, Any] = {"group_id": group_id}
    # Databricks similarity search has no hard upper bound we can push
    # server-side, so we pull records in pages and gather ids client-side.
    response = await repo.similarity_search(
        index_name=databricks_cfg.memory_index,
        endpoint_name=databricks_cfg.endpoint_name,
        query_vector=zero_vector,
        columns=columns,
        num_results=10_000,  # effective cap enforced by Vector Search
        filters=filters,
        user_token=user_token,
    )
    data_array = (response or {}).get("result", {}).get("data_array") or []
    id_idx = positions.get("id")
    scope_idx = positions.get("scope")
    ids: List[str] = []
    for row in data_array:
        if id_idx is None or id_idx >= len(row):
            continue
        if scope and scope_idx is not None and scope_idx < len(row):
            row_scope = row[scope_idx] or ""
            if not str(row_scope).startswith(scope):
                continue
        row_id = row[id_idx]
        if row_id is not None:
            ids.append(str(row_id))

    if not ids:
        return 0

    await repo.delete_records(
        index_name=databricks_cfg.memory_index,
        endpoint_name=databricks_cfg.endpoint_name,
        primary_keys=ids,
        user_token=user_token,
    )
    return len(ids)


async def _delete_lakebase_records(
    lakebase_cfg: Any,
    *,
    group_id: str,
    scope: Optional[str],
) -> int:
    """Delete Lakebase unified-memory rows scoped to this group."""
    from sqlalchemy import text

    from src.db.lakebase_session import get_lakebase_session

    instance_name = getattr(lakebase_cfg, "instance_name", None)
    table_name = lakebase_cfg.memory_table
    where = ["group_id = :group_id"]
    params: Dict[str, Any] = {"group_id": group_id}
    if scope:
        where.append("metadata->>'scope' LIKE :scope_prefix")
        params["scope_prefix"] = f"{scope}%"

    async with get_lakebase_session(
        instance_name=instance_name, group_id=group_id
    ) as session:
        sql = text(f"DELETE FROM {table_name} WHERE {' AND '.join(where)}")
        result = await session.execute(sql, params)
        return int(getattr(result, "rowcount", 0) or 0)


def _delete_default_records(
    *,
    group_id: str,
    scope: Optional[str],
) -> int:
    """Wipe the local LanceDB store for the group on the backend host.

    Looks in the same candidate roots as ``_browse_default_records``. When
    ``scope`` is provided, deletes only records matching that prefix via
    ``crewai.memory.Memory`` (the LanceDB storage exposes scope-aware
    ``delete``). When ``scope`` is unset, the entire group store directory is
    removed — the cleanest way to guarantee no orphan LanceDB manifest files
    remain.
    """
    import os
    import shutil
    from pathlib import Path

    # ONE deterministic store per group at the known memory root — the same path
    # the runtime writes to and the browser reads (legacy per-crew stores untouched).
    store_dir = local_memory_store_dir(group_id)
    if not store_dir.is_dir():
        return 0
    storage_dirs: List[Path] = [store_dir]

    deleted = 0
    # Scope-filtered delete → use the Memory API so we leave other records
    # intact in each store.
    if scope:
        try:
            from crewai.memory import Memory  # type: ignore
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Default memory delete failed (crewai.memory missing): %s", exc)
            return 0
        original_storage = os.environ.get("CREWAI_STORAGE_DIR")
        try:
            for storage_dir in storage_dirs:
                try:
                    os.environ["CREWAI_STORAGE_DIR"] = str(storage_dir)
                    memory = Memory()
                    storage = (
                        getattr(memory, "_storage", None)
                        or getattr(memory, "storage", None)
                    )
                    if storage is None or not hasattr(storage, "delete"):
                        continue
                    result = storage.delete(scope_prefix=scope)
                    if isinstance(result, int):
                        deleted += result
                    else:
                        deleted += 1
                except Exception as exc:
                    logger.warning(
                        "Failed to delete from local memory store %s: %s",
                        storage_dir,
                        exc,
                    )
        finally:
            if original_storage is None:
                os.environ.pop("CREWAI_STORAGE_DIR", None)
            else:
                os.environ["CREWAI_STORAGE_DIR"] = original_storage
        return deleted

    # Wholesale wipe → remove each per-crew directory.
    for storage_dir in storage_dirs:
        try:
            shutil.rmtree(storage_dir)
            deleted += 1
            logger.info("Removed local memory store %s", storage_dir)
        except Exception as exc:
            logger.warning("Failed to remove local memory store %s: %s", storage_dir, exc)
    return deleted
