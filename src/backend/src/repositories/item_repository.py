from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.item import Item


class ItemRepository(BaseRepository[Item]):
    """
    Repository for Item model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.
        
        Args:
            session: SQLAlchemy async session
        """
        super().__init__(Item, session)
    
    async def find_by_name(self, name: str) -> Optional[Item]:
        """
        Find an item by name.
        
        Args:
            name: Name to search for
            
        Returns:
            Item if found, else None
        """
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def find_active_items(self, skip: int = 0, limit: int = 100) -> List[Item]:
        """
        Find all active items.
        
        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            List of active items
        """
        query = (
            select(self.model)
            .where(self.model.is_active.is_(True))
            .offset(skip)
            .limit(limit)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all()) 