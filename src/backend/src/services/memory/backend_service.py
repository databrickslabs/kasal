"""
Memory backend service - facade for all memory backend operations.

This module acts as a facade that delegates to specialized services for different operations.
"""

from typing import Any, Dict, List, Optional, Tuple

from src.core.logger import LoggerManager
from src.models.memory_backend import MemoryBackend
from src.schemas.memory_backend import (
    DatabricksMemoryConfig,
    MemoryBackendConfig,
    MemoryBackendCreate,
    MemoryBackendType,
    MemoryBackendUpdate,
)

# Import specialized services
from src.services.memory.backend_base_service import MemoryBackendBaseService
from src.services.memory.config_service import MemoryConfigService

logger = LoggerManager.get_instance().system


class MemoryBackendService:
    """
    Facade service for managing memory backend configurations.

    This service delegates to specialized services for different operations:
    - Base CRUD operations -> MemoryBackendBaseService
    - Configuration retrieval -> MemoryConfigService
    - Lakebase operations -> LakebaseMemoryService
    """

    def __init__(self, session: Any):
        """
        Initialize the service with all sub-services.

        Args:
            session: Database session from dependency injection
        """
        self.session = session

        # Initialize sub-services with injected session
        self._base_service = MemoryBackendBaseService(session)
        self._config_service = MemoryConfigService(session)
        self._lakebase_service = None  # Lazy-initialized

    # ===== Base CRUD Operations (delegated to MemoryBackendBaseService) =====

    async def create_memory_backend(
        self, group_id: str, config: MemoryBackendCreate
    ) -> MemoryBackend:
        """Create a new memory backend configuration."""
        return await self._base_service.create_memory_backend(group_id, config)

    async def get_memory_backends(self, group_id: str) -> List[MemoryBackend]:
        """Get all memory backend configurations for a group."""
        return await self._base_service.get_memory_backends(group_id)

    async def get_all(self) -> List[MemoryBackend]:
        """Get all memory backend configurations across all groups."""
        from src.repositories.memory_backend_repository import MemoryBackendRepository

        repository = MemoryBackendRepository(self.session)
        return await repository.get_all()

    async def get_memory_backend(
        self, group_id: str, backend_id: str
    ) -> Optional[MemoryBackend]:
        """Get a specific memory backend configuration."""
        return await self._base_service.get_memory_backend(group_id, backend_id)

    async def get_default_memory_backend(
        self, group_id: str
    ) -> Optional[MemoryBackend]:
        """Get the default memory backend for a group."""
        return await self._base_service.get_default_memory_backend(group_id)

    async def update_memory_backend(
        self, group_id: str, backend_id: str, update_data: MemoryBackendUpdate
    ) -> Optional[MemoryBackend]:
        """Update a memory backend configuration."""
        return await self._base_service.update_memory_backend(
            group_id, backend_id, update_data
        )

    async def delete_memory_backend(self, group_id: str, backend_id: str) -> bool:
        """Delete a memory backend configuration."""
        return await self._base_service.delete_memory_backend(group_id, backend_id)

    async def set_default_backend(self, group_id: str, backend_id: str) -> bool:
        """Set a memory backend as default."""
        return await self._base_service.set_default_backend(group_id, backend_id)

    async def get_memory_stats(self, group_id: str, crew_id: str) -> Dict[str, Any]:
        """Get memory usage statistics for a crew."""
        return await self._base_service.get_memory_stats(group_id, crew_id)

    async def delete_all_and_create_disabled(self, group_id: str) -> Dict[str, Any]:
        """Delete all memory backend configurations for a group and create a disabled one."""
        return await self._base_service.delete_all_and_create_disabled(group_id)

    async def delete_disabled_configurations(self, group_id: str) -> int:
        """Delete all disabled (DEFAULT type) configurations for a group."""
        return await self._base_service.delete_disabled_configurations(group_id)

    # ===== Configuration Management (delegated to MemoryConfigService) =====

    async def get_active_config(
        self, group_id: str = None
    ) -> Optional[MemoryBackendConfig]:
        """Get the active memory backend configuration."""
        return await self._config_service.get_active_config(group_id)

    def _get_lakebase_service(self, instance_name: Optional[str] = None):
        """Create a LakebaseMemoryService for the given instance."""
        from src.services.memory.lakebase_service import LakebaseMemoryService

        return LakebaseMemoryService(instance_name=instance_name)

    async def test_lakebase_connection(
        self, instance_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Test connection to Lakebase and verify pgvector availability."""
        return await self._get_lakebase_service(instance_name).test_connection()

    async def initialize_lakebase_tables(
        self,
        embedding_dimension: int = 1024,
        memory_table: str = "crew_memory",
        instance_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create pgvector extension and the unified memory table on Lakebase."""
        return await self._get_lakebase_service(instance_name).initialize_tables(
            embedding_dimension=embedding_dimension,
            memory_table=memory_table,
        )

    async def get_lakebase_table_stats(
        self, instance_name: Optional[str] = None, group_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get row counts per memory table on Lakebase (scoped to group_id)."""
        return await self._get_lakebase_service(instance_name).get_table_stats(
            group_id=group_id,
        )

    async def get_lakebase_table_data(
        self,
        table_name: str,
        limit: int = 50,
        instance_name: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch rows from a Lakebase memory table (scoped to group_id)."""
        return await self._get_lakebase_service(instance_name).get_table_data(
            table_name=table_name,
            limit=limit,
            group_id=group_id,
        )

    async def get_lakebase_entity_data(
        self,
        memory_table: str = "crew_memory",
        limit: int = 200,
        instance_name: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch entity-like records from the unified memory table (scoped to group_id)."""
        return await self._get_lakebase_service(instance_name).get_entity_data(
            memory_table=memory_table,
            limit=limit,
            group_id=group_id,
        )
