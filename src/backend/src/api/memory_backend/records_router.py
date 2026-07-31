"""Memory *contents* endpoints — stats, clearing, and the unified record browser.

The ``/records`` pair is backend-agnostic: it reads the group's active
``MemoryBackend`` config and dispatches to the matching helper in
``record_browsers``.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

from src.core.dependencies import GroupContextDep
from src.core.exceptions import BadRequestError
from src.utils.databricks_auth import extract_user_token_from_request

from .dependencies import MemoryBackendServiceDep, logger
from .record_browsers import (
    _browse_default_records,
    _browse_lakebase_records,
    _delete_default_records,
    _delete_lakebase_records,
)

router = APIRouter()


@router.get("/stats/{crew_id}")
async def get_memory_stats(
    crew_id: str,
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
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


# ---------------------------------------------------------------------------
# Unified memory browser (CrewAI 1.10+)
# ---------------------------------------------------------------------------


@router.get("/records")
async def list_memory_records(
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
    request: Request,
    scope: Optional[str] = Query(
        None,
        description="Optional hierarchical scope prefix to filter records.",
    ),
    limit: int = Query(
        50,
        ge=1,
        le=5000,
        description=(
            "Maximum number of records to return. The browser pages with a "
            "small limit for the card list, but the concept/graph views fetch "
            "the whole store in one request, so the ceiling is high."
        ),
    ),
    offset: int = Query(
        0,
        ge=0,
        description="Number of records to skip for pagination.",
    ),
) -> Dict[str, Any]:
    """Browse records stored in the active memory backend.

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
    if backend_type == "lakebase":
        lakebase_cfg = active.lakebase_config if active else None
        if not lakebase_cfg or not lakebase_cfg.memory_table:
            return {
                "backend": backend_type,
                "records": [],
                "count": 0,
                "total": 0,
                "offset": offset,
                "limit": limit,
            }
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
    service: MemoryBackendServiceDep,
    request: Request,
    scope: Optional[str] = Query(
        None,
        description=(
            "Optional scope prefix. When omitted, deletes every record "
            "owned by the caller's group."
        ),
    ),
) -> Dict[str, Any]:
    """Delete memory records from the active backend.

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
    if backend_type == "lakebase":
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
