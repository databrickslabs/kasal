"""
Memory backend repository for database operations.

This module implements the repository pattern for memory backend configurations.
"""
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload

from src.models.memory_backend import MemoryBackend, MemoryBackendTypeEnum
from src.core.base_repository import BaseRepository
from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().system


class MemoryBackendRepository(BaseRepository[MemoryBackend]):
    """Repository for memory backend database operations."""
    
    def __init__(self, db: AsyncSession):
        super().__init__(MemoryBackend, db)
    
    async def get_by_group_id(self, group_id: str) -> List[MemoryBackend]:
        """
        Get all memory backend configurations for a group.
        
        Args:
            group_id: Group ID
            
        Returns:
            List of memory backend configurations
        """
        try:
            query = select(self.model).where(
                self.model.group_id == group_id
            ).order_by(self.model.created_at.desc())
            
            result = await self.session.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting memory backends for group {group_id}: {e}")
            return []
    
    async def get_default_by_group_id(self, group_id: str) -> Optional[MemoryBackend]:
        """
        Get the default memory backend configuration for a group.
        
        Args:
            group_id: Group ID
            
        Returns:
            Default memory backend configuration or None
        """
        try:
            query = select(self.model).where(
                and_(
                    self.model.group_id == group_id,
                    self.model.is_default == True,
                    self.model.is_active == True
                )
            )
            
            result = await self.session.execute(query)
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting default memory backend for group {group_id}: {e}")
            return None
    
    async def get_by_name(self, group_id: str, name: str) -> Optional[MemoryBackend]:
        """
        Get a memory backend configuration by name.
        
        Args:
            group_id: Group ID
            name: Configuration name
            
        Returns:
            Memory backend configuration or None
        """
        try:
            query = select(self.model).where(
                and_(
                    self.model.group_id == group_id,
                    self.model.name == name
                )
            )
            
            result = await self.session.execute(query)
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error getting memory backend by name {name}: {e}")
            return None
    
    async def set_default(self, group_id: str, backend_id: str) -> bool:
        """
        Set a memory backend as the default for a group.
        
        Args:
            group_id: Group ID
            backend_id: Backend ID to set as default
            
        Returns:
            True if successful
        """
        try:
            # First, unset any existing defaults
            query = select(self.model).where(
                and_(
                    self.model.group_id == group_id,
                    self.model.is_default == True
                )
            )
            result = await self.session.execute(query)
            existing_defaults = result.scalars().all()
            
            for backend in existing_defaults:
                backend.is_default = False
            
            # Set the new default
            backend = await self.get(backend_id)
            if backend and backend.group_id == group_id:
                backend.is_default = True
                await self.session.commit()
                return True
            
            return False
        except Exception as e:
            logger.error(f"Error setting default memory backend: {e}")
            await self.session.rollback()
            return False
    
    async def get_by_type(self, group_id: str, backend_type: MemoryBackendTypeEnum) -> List[MemoryBackend]:
        """
        Get all memory backend configurations of a specific type.
        
        Args:
            group_id: Group ID
            backend_type: Backend type to filter by
            
        Returns:
            List of memory backend configurations
        """
        try:
            query = select(self.model).where(
                and_(
                    self.model.group_id == group_id,
                    self.model.backend_type == backend_type,
                    self.model.is_active == True
                )
            ).order_by(self.model.created_at.desc())
            
            result = await self.session.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting memory backends by type {backend_type}: {e}")
            return []
    
    async def get_all(self) -> List[MemoryBackend]:
        """
        Get all memory backend configurations.
        
        Returns:
            List of all memory backend configurations
        """
        try:
            query = select(self.model).order_by(self.model.created_at.desc())
            result = await self.session.execute(query)
            return result.scalars().all()
        except Exception as e:
            logger.error(f"Error getting all memory backends: {e}")
            return []
    
    async def delete_all_by_group_id(self, group_id: str) -> int:
        """
        Delete all memory backend configurations for a group.
        
        Args:
            group_id: Group ID
            
        Returns:
            Number of deleted configurations
        """
        try:
            # Get all backends for the group
            backends = await self.get_by_group_id(group_id)
            count = len(backends)
            
            # Delete each backend
            for backend in backends:
                await self.session.delete(backend)
            
            await self.session.commit()
            logger.info(f"Deleted {count} memory backend configurations for group {group_id}")
            return count
        except Exception as e:
            logger.error(f"Error deleting all memory backends for group {group_id}: {e}")
            await self.session.rollback()
            return 0