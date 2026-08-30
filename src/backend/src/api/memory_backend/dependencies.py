"""Shared wiring for the memory-backend routers.

Every sub-router in this package resolves ``MemoryBackendService`` through the
SAME provider function, so one ``app.dependency_overrides[...]`` entry in a test
covers the whole surface. Defining a per-module provider would silently leave
the other routers wired to the real service.
"""

from typing import Annotated

from fastapi import Depends

from src.core.dependencies import SessionDep
from src.core.logger import LoggerManager
from src.services.memory.config.backend_service import MemoryBackendService

logger = LoggerManager.get_instance().api


def get_memory_backend_service(session: SessionDep) -> MemoryBackendService:
    """Get MemoryBackendService instance with injected session."""
    return MemoryBackendService(session)


MemoryBackendServiceDep = Annotated[
    MemoryBackendService, Depends(get_memory_backend_service)
]
