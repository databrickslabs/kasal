"""
Base memory backend service for core CRUD operations.

This module implements the base service layer for memory backend configurations.
"""
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.memory_backend import MemoryBackend
from src.schemas.memory_backend import (
    MemoryBackendConfig, 
    MemoryBackendCreate,
    MemoryBackendUpdate,
    DatabricksMemoryConfig,
    MemoryBackendType
)
from src.core.logger import LoggerManager
from src.core.unit_of_work import UnitOfWork

logger = LoggerManager.get_instance().system


class MemoryBackendBaseService:
    """Base service for managing memory backend configurations."""
    
    def __init__(self, uow: UnitOfWork):
        """
        Initialize the service.
        
        Args:
            uow: Unit of Work instance
        """
        self.uow = uow
    
    async def create_memory_backend(
        self, 
        group_id: str,
        config: MemoryBackendCreate
    ) -> MemoryBackend:
        """
        Create a new memory backend configuration.
        
        Args:
            group_id: Group ID
            config: Memory backend configuration
            
        Returns:
            Created memory backend
        """
        try:
            repo = self.uow.memory_backend_repository
            
            # Check if name already exists
            existing = await repo.get_by_name(group_id, config.name)
            if existing:
                raise ValueError(f"Memory backend with name '{config.name}' already exists")
            
            # Prepare data
            data = {
                "group_id": group_id,
                "name": config.name,
                "description": config.description,
                "backend_type": config.backend_type,
                "enable_short_term": config.enable_short_term,
                "enable_long_term": config.enable_long_term,
                "enable_entity": config.enable_entity,
                "is_active": True,
                "is_default": False
            }
            
            # Add backend-specific configuration
            if config.backend_type == MemoryBackendType.DATABRICKS:
                if not config.databricks_config:
                    raise ValueError("Databricks configuration is required for Databricks backend")
                data["databricks_config"] = config.databricks_config.model_dump()
            
            if config.custom_config:
                data["custom_config"] = config.custom_config
            
            # Create the backend
            backend = await repo.create(data)
            
            # If this is the first backend, make it default
            all_backends = await repo.get_by_group_id(group_id)
            if len(all_backends) == 1:
                await repo.set_default(group_id, backend.id)
            
            await self.uow.commit()
            return backend
            
        except Exception as e:
            # UnitOfWork handles rollback automatically in __aexit__
            logger.error(f"Error creating memory backend: {e}")
            raise
    
    async def get_memory_backends(self, group_id: str) -> List[MemoryBackend]:
        """
        Get all memory backend configurations for a group.
        
        Args:
            group_id: Group ID
            
        Returns:
            List of memory backends
        """
        repo = self.uow.memory_backend_repository
        return await repo.get_by_group_id(group_id)
    
    async def get_memory_backend(self, group_id: str, backend_id: str) -> Optional[MemoryBackend]:
        """
        Get a specific memory backend configuration.
        
        Args:
            group_id: Group ID
            backend_id: Backend ID
            
        Returns:
            Memory backend or None
        """
        repo = self.uow.memory_backend_repository
        backend = await repo.get(backend_id)
        
        # Check ownership
        if backend and backend.group_id != group_id:
            return None
            
        return backend
    
    async def get_default_memory_backend(self, group_id: str) -> Optional[MemoryBackend]:
        """
        Get the default memory backend for a group.
        
        Args:
            group_id: Group ID
            
        Returns:
            Default memory backend or None
        """
        repo = self.uow.memory_backend_repository
        return await repo.get_default_by_group_id(group_id)
    
    async def update_memory_backend(
        self,
        group_id: str,
        backend_id: str,
        update_data: MemoryBackendUpdate
    ) -> Optional[MemoryBackend]:
        """
        Update a memory backend configuration.
        
        Args:
            group_id: Group ID
            backend_id: Backend ID
            update_data: Update data
            
        Returns:
            Updated memory backend or None
        """
        try:
            repo = self.uow.memory_backend_repository
            backend = await repo.get(backend_id)
            
            # Check ownership
            if not backend or backend.group_id != group_id:
                return None
            
            # Update fields
            update_dict = update_data.model_dump(exclude_unset=True)
            
            # Handle backend type change
            if "backend_type" in update_dict:
                backend.backend_type = update_dict["backend_type"]
                # Clear databricks config if switching away from databricks
                if update_dict["backend_type"] != MemoryBackendType.DATABRICKS:
                    backend.databricks_config = None
            
            # Handle backend-specific config
            if "databricks_config" in update_dict:
                if backend.backend_type == MemoryBackendType.DATABRICKS:
                    backend.databricks_config = update_dict["databricks_config"]
                del update_dict["databricks_config"]
            
            # Update other fields
            for key, value in update_dict.items():
                if hasattr(backend, key) and key != "backend_type":  # backend_type already handled
                    setattr(backend, key, value)
            
            await self.uow.commit()
            return backend
            
        except Exception as e:
            # UnitOfWork handles rollback automatically in __aexit__
            logger.error(f"Error updating memory backend: {e}")
            raise
    
    async def delete_memory_backend(self, group_id: str, backend_id: str) -> bool:
        """
        Delete a memory backend configuration.
        
        Args:
            group_id: Group ID
            backend_id: Backend ID
            
        Returns:
            True if deleted
        """
        try:
            repo = self.uow.memory_backend_repository
            backend = await repo.get(backend_id)
            
            # Check ownership
            if not backend or backend.group_id != group_id:
                return False
            
            # Don't delete if it's the only backend
            all_backends = await repo.get_by_group_id(group_id)
            if len(all_backends) <= 1:
                raise ValueError("Cannot delete the only memory backend configuration")
            
            # If deleting default, set another as default
            if backend.is_default:
                for other in all_backends:
                    if other.id != backend_id:
                        await repo.set_default(group_id, other.id)
                        break
            
            await repo.delete(backend_id)
            await self.uow.commit()
            return True
            
        except Exception as e:
            # UnitOfWork handles rollback automatically in __aexit__
            logger.error(f"Error deleting memory backend: {e}")
            raise
    
    async def set_default_backend(self, group_id: str, backend_id: str) -> bool:
        """
        Set a memory backend as default.
        
        Args:
            group_id: Group ID
            backend_id: Backend ID
            
        Returns:
            True if successful
        """
        try:
            repo = self.uow.memory_backend_repository
            success = await repo.set_default(group_id, backend_id)
            if success:
                await self.uow.commit()
            return success
        except Exception as e:
            # UnitOfWork handles rollback automatically in __aexit__
            logger.error(f"Error setting default backend: {e}")
            raise
    
    async def get_memory_stats(self, group_id: str, crew_id: str) -> Dict[str, Any]:
        """
        Get memory usage statistics for a crew.
        
        Args:
            group_id: Group ID
            crew_id: Crew ID
            
        Returns:
            Memory statistics
        """
        # Get the group's default backend
        backend = await self.get_default_memory_backend(group_id)
        if not backend:
            return {
                "short_term_count": 0,
                "long_term_count": 0,
                "entity_count": 0,
                "total_size_mb": 0.0
            }
        
        # In a real implementation, query the actual memory backend
        # For now, return placeholder data
        return {
            "backend_type": backend.backend_type.value,
            "backend_name": backend.name,
            "short_term_count": 0,
            "long_term_count": 0,
            "entity_count": 0,
            "total_size_mb": 0.0
        }
    
    async def delete_all_and_create_disabled(self, group_id: str) -> Dict[str, Any]:
        """
        Delete all memory backend configurations for a group and create a disabled one.
        
        Args:
            group_id: Group ID
            
        Returns:
            Result with deleted count and new disabled config
        """
        try:
            repo = self.uow.memory_backend_repository
            
            # Delete all existing configurations
            deleted_count = await repo.delete_all_by_group_id(group_id)
            
            # Create a new disabled configuration
            disabled_config = {
                "group_id": group_id,
                "name": "Disabled Configuration",
                "description": "Memory storage disabled",
                "backend_type": MemoryBackendType.DEFAULT,
                "enable_short_term": False,
                "enable_long_term": False,
                "enable_entity": False,
                "is_active": True,
                "is_default": True
            }
            
            backend = await repo.create(disabled_config)
            await self.uow.commit()
            
            return {
                "success": True,
                "deleted_count": deleted_count,
                "disabled_config": backend.to_dict() if backend else None,
                "message": f"Deleted {deleted_count} configurations and created disabled configuration"
            }
            
        except Exception as e:
            logger.error(f"Error deleting all configs and creating disabled: {e}")
            return {
                "success": False,
                "deleted_count": 0,
                "message": f"Failed to delete configurations: {str(e)}"
            }
    
    async def delete_disabled_configurations(self, group_id: str) -> int:
        """
        Delete all disabled (DEFAULT type) configurations for a group.
        This is used when switching from disabled to enabled mode.
        
        Args:
            group_id: Group ID
            
        Returns:
            Number of deleted configurations
        """
        try:
            repo = self.uow.memory_backend_repository
            
            # Get all backends for the group
            backends = await repo.get_by_group_id(group_id)
            
            deleted_count = 0
            for backend in backends:
                # Delete only DEFAULT (disabled) backends
                if backend.backend_type == MemoryBackendType.DEFAULT:
                    await repo.delete(backend.id)
                    deleted_count += 1
                    logger.info(f"Deleted disabled configuration: {backend.id}")
            
            await self.uow.commit()
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error deleting disabled configurations: {e}")
            raise