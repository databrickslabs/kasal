"""
Repository for database configuration operations.
"""
from typing import Optional, Dict, Any
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.database_config import LakebaseConfig


class DatabaseConfigRepository(BaseRepository):
    """Repository for database configuration operations."""

    def __init__(self, model_class, session: AsyncSession):
        """Initialize repository with model class and session."""
        super().__init__(model_class, session)

    async def get_by_key(self, key: str) -> Optional[LakebaseConfig]:
        """
        Get configuration by key.

        Args:
            key: Configuration key

        Returns:
            Configuration object or None
        """
        query = select(self.model).where(self.model.key == key)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def upsert(self, key: str, value: Dict[str, Any]) -> LakebaseConfig:
        """
        Insert or update configuration.

        Args:
            key: Configuration key
            value: Configuration value (JSON)

        Returns:
            Configuration object
        """
        existing = await self.get_by_key(key)

        if existing:
            # Update existing
            existing.value = value
            await self.session.flush()
            return existing
        else:
            # Create new
            config = self.model(key=key, value=value)
            self.session.add(config)
            await self.session.flush()
            return config

    async def delete_by_key(self, key: str) -> bool:
        """
        Delete configuration by key.

        Args:
            key: Configuration key

        Returns:
            True if deleted, False if not found
        """
        config = await self.get_by_key(key)
        if config:
            await self.session.delete(config)
            await self.session.flush()
            return True
        return False