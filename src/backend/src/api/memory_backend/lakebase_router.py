"""Lakebase (PostgreSQL + pgvector) memory backend endpoints.

Connection testing, table provisioning, and read access to the unified Lakebase
memory table, plus the Lakebase setup flow.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query

from src.core.dependencies import GroupContextDep
from src.core.exceptions import ForbiddenError
from src.core.permissions import is_workspace_admin
from src.schemas.memory_backend import (
    MemoryBackendCreate,
    MemoryBackendType,
)

from .dependencies import MemoryBackendServiceDep, logger

router = APIRouter()


@router.post("/lakebase/test-connection")
async def test_lakebase_connection(
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
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
    service: MemoryBackendServiceDep,
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
    service: MemoryBackendServiceDep,
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
    service: MemoryBackendServiceDep,
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
    service: MemoryBackendServiceDep,
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
    service: MemoryBackendServiceDep,
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
