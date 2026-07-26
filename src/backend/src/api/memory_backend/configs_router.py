"""Memory backend configuration CRUD.

Backend-agnostic management of ``MemoryBackend`` records: create/read/update/
delete, choosing the default, validation, and the bulk transitions between
enabled and disabled modes.

Route order matters here: ``GET /configs/default`` is declared before
``GET /configs/{backend_id}`` so "default" is not captured as a path parameter.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request

from src.core.dependencies import GroupContextDep
from src.core.exceptions import ForbiddenError, KasalError, NotFoundError
from src.core.permissions import is_workspace_admin
from src.schemas.memory_backend import (
    MemoryBackendConfig,
    MemoryBackendCreate,
    MemoryBackendResponse,
    MemoryBackendType,
    MemoryBackendUpdate,
)

from .dependencies import MemoryBackendServiceDep, logger

router = APIRouter()


@router.post("/default/save-config")
async def save_default_config(
    request: Dict[str, Any],
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
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
    # the Lakebase setup.
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
    service: MemoryBackendServiceDep,
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


@router.post("/configs", response_model=MemoryBackendResponse)
async def create_memory_config(
    config: MemoryBackendCreate,
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
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
    service: MemoryBackendServiceDep,
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


# NOTE: must stay ahead of GET /configs/{backend_id} — otherwise "default"
# is matched as a backend_id. Covered by test_memory_backend_router_route_ordering.
@router.get("/configs/default", response_model=Optional[MemoryBackendResponse])
async def get_default_memory_config(
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
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
    service: MemoryBackendServiceDep,
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
    service: MemoryBackendServiceDep,
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
    service: MemoryBackendServiceDep,
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
    service: MemoryBackendServiceDep,
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


@router.delete("/configs/databricks/all")
async def delete_all_databricks_configs(
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
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
    service: MemoryBackendServiceDep,
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
    service: MemoryBackendServiceDep,
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
