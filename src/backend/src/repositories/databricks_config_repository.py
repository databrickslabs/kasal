from typing import Optional
import logging
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.databricks_config import DatabricksConfig

# Set up logger
logger = logging.getLogger(__name__)

class DatabricksConfigRepository(BaseRepository[DatabricksConfig]):
    """
    Repository for DatabricksConfig model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.
        
        Args:
            session: SQLAlchemy async session
        """
        super().__init__(DatabricksConfig, session)
    
    async def get_active_config(self) -> Optional[DatabricksConfig]:
        """
        Get the currently active Databricks configuration.
        
        Returns:
            Active configuration if found, else None
        """
        query = select(self.model).where(self.model.is_active == True)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()
    
    async def deactivate_all(self) -> None:
        """
        Deactivate all existing Databricks configurations.
        
        Returns:
            None
        """
        query = (
            update(self.model)
            .where(self.model.is_active == True)
            .values(is_active=False, updated_at=datetime.now(timezone.utc))
        )
        await self.session.execute(query)
        await self.session.commit()  # Make sure the changes are committed
    
    async def create_config(self, config_data: dict) -> DatabricksConfig:
        """
        Create a new Databricks configuration.
        
        Args:
            config_data: Configuration data dictionary
            
        Returns:
            The created configuration
        """
        # First deactivate any existing active configurations
        await self.deactivate_all()
        
        # Create the new configuration
        db_config = DatabricksConfig(**config_data)
        self.session.add(db_config)
        await self.session.flush()
        await self.session.commit()  # Make sure the changes are committed
        
        return db_config 