from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.crew import Crew


class CrewRepository(BaseRepository[Crew]):
    """
    Repository for Crew model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.
        
        Args:
            session: SQLAlchemy async session
        """
        super().__init__(Crew, session)
    
    async def find_by_name(self, name: str) -> Optional[Crew]:
        """
        Find a crew by name.
        
        Args:
            name: Name to search for
            
        Returns:
            Crew if found, else None
        """
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def find_all(self) -> List[Crew]:
        """
        Find all crews.
        
        Returns:
            List of all crews
        """
        query = select(self.model)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def delete_all(self) -> None:
        """
        Delete all crews.
        
        Returns:
            None
        """
        await self.session.execute(select(self.model).delete()) 