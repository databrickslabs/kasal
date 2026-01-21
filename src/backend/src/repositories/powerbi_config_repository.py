from typing import Optional
import logging
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.powerbi_config import PowerBIConfig

# Set up logger
logger = logging.getLogger(__name__)


class PowerBIConfigRepository(BaseRepository[PowerBIConfig]):
    """
    Repository for PowerBIConfig model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.

        Args:
            session: SQLAlchemy async session
        """
        super().__init__(PowerBIConfig, session)

    async def get_active_config(self, group_id: Optional[str] = None) -> Optional[PowerBIConfig]:
        """
        Get the currently active Power BI configuration for the specified group.
        If multiple active configurations exist, returns the most recently updated one.

        Args:
            group_id: Optional group ID to filter by

        Returns:
            Active configuration if found, else None
        """
        query = select(self.model).where(self.model.is_active == True)
        if group_id is not None:
            query = query.where(self.model.group_id == group_id)

        # Order by updated_at descending to get the most recent one
        query = query.order_by(self.model.updated_at.desc())

        result = await self.session.execute(query)
        return result.scalars().first()

    async def deactivate_all(self, group_id: Optional[str] = None) -> None:
        """
        Deactivate all existing Power BI configurations for the specified group.

        Args:
            group_id: Optional group ID to filter by

        Returns:
            None
        """
        query = (
            update(self.model)
            .where(self.model.is_active == True)
            .values(is_active=False, updated_at=datetime.now(timezone.utc))
        )
        if group_id is not None:
            query = query.where(self.model.group_id == group_id)
        await self.session.execute(query)
        await self.session.commit()  # Make sure the changes are committed

    async def create_config(self, config_data: dict) -> PowerBIConfig:
        """
        Create a new Power BI configuration.

        Args:
            config_data: Configuration data dictionary

        Returns:
            The created configuration
        """
        if config_data is None:
            raise TypeError("config_data cannot be None")

        # First deactivate any existing active configurations for this group
        group_id = config_data.get('group_id')
        await self.deactivate_all(group_id=group_id)

        # Map 'enabled' from API schema to 'is_enabled' in database model
        db_data = config_data.copy()
        if 'enabled' in db_data:
            db_data['is_enabled'] = db_data.pop('enabled')

        # Create the new configuration
        db_config = PowerBIConfig(**db_data)
        self.session.add(db_config)
        await self.session.flush()
        await self.session.commit()  # Make sure the changes are committed

        return db_config
