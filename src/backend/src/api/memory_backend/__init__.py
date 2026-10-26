"""Memory backend configuration API.

Split by resource family — the endpoints used to live in one
``memory_backend_router.py``:

- ``configs_router``  — backend-agnostic ``MemoryBackend`` CRUD + mode switches
- ``lakebase_router``   — Lakebase (pgvector) connection + tables
- ``records_router``    — memory contents: stats, clear, unified record browser

All three share one service provider (``dependencies.get_memory_backend_service``),
so a single ``app.dependency_overrides`` entry covers the whole surface.
"""

from fastapi import APIRouter

from . import (
    configs_router,
    lakebase_router,
    records_router,
)
from .dependencies import MemoryBackendServiceDep, get_memory_backend_service

router = APIRouter(prefix="/memory-backend", tags=["memory-backend"])

# Bind the sub-routers by module, not by aliasing their `router` attribute onto
# the module name — the alias would shadow the submodule itself, so
# `from src.api.memory_backend import configs_router` would hand back an
# APIRouter instead of the module.
router.include_router(lakebase_router.router)
router.include_router(configs_router.router)
router.include_router(records_router.router)

__all__ = [
    "router",
    "get_memory_backend_service",
    "MemoryBackendServiceDep",
]
