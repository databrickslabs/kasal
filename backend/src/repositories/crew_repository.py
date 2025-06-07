from typing import List, Optional
from uuid import UUID

from sqlalchemy import select, delete
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
        await self.session.execute(delete(self.model))
    
    # Tenant-aware methods
    async def find_by_tenant(self, tenant_ids: List[str]) -> List[Crew]:
        """
        Find all crews for the given tenant IDs.
        
        Args:
            tenant_ids: List of tenant IDs to filter by
            
        Returns:
            List of crews for the tenants
        """
        if not tenant_ids:
            return []
        
        query = select(self.model).where(self.model.tenant_id.in_(tenant_ids))
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_by_tenant(self, id: UUID, tenant_ids: List[str]) -> Optional[Crew]:
        """
        Get a crew by ID, ensuring it belongs to one of the given tenants.
        
        Args:
            id: ID of the crew to get
            tenant_ids: List of tenant IDs to filter by
            
        Returns:
            Crew if found and belongs to tenant, else None
        """
        if not tenant_ids:
            return None
        
        query = select(self.model).where(
            self.model.id == id,
            self.model.tenant_id.in_(tenant_ids)
        )
        result = await self.session.execute(query)
        return result.scalars().first()

    async def delete_by_tenant(self, id: UUID, tenant_ids: List[str]) -> bool:
        """
        Delete a crew by ID, ensuring it belongs to one of the given tenants.
        
        Args:
            id: ID of the crew to delete
            tenant_ids: List of tenant IDs to filter by
            
        Returns:
            True if crew was deleted, False if not found or doesn't belong to tenant
        """
        if not tenant_ids:
            return False
        
        # First check if the crew exists and belongs to the tenant
        crew = await self.get_by_tenant(id, tenant_ids)
        if not crew:
            return False
        
        # Delete the crew
        await self.session.delete(crew)
        return True

    async def delete_all_by_tenant(self, tenant_ids: List[str]) -> None:
        """
        Delete all crews for the given tenant IDs.
        
        Args:
            tenant_ids: List of tenant IDs to filter by
        """
        if not tenant_ids:
            return
        
        stmt = delete(self.model).where(self.model.tenant_id.in_(tenant_ids))
        await self.session.execute(stmt) 