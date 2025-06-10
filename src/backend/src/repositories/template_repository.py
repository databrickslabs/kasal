from typing import List, Optional, Dict, Any
from datetime import datetime

from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.template import PromptTemplate


class TemplateRepository:
    """
    Repository for PromptTemplate model with custom query methods.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.
        
        Args:
            session: SQLAlchemy async session
        """
        self.model = PromptTemplate
        self.session = session
    
    async def get(self, id: int) -> Optional[PromptTemplate]:
        """
        Get a prompt template by ID.
        
        Args:
            id: ID of the template to get
            
        Returns:
            PromptTemplate if found, else None
        """
        return await self.session.get(self.model, id)
    
    async def create(self, data: Dict[str, Any]) -> PromptTemplate:
        """
        Create a new prompt template.
        
        Args:
            data: Dictionary with fields to create template with
            
        Returns:
            Created PromptTemplate
        """
        template = self.model(**data)
        self.session.add(template)
        await self.session.flush()
        return template
    
    async def find_by_name(self, name: str) -> Optional[PromptTemplate]:
        """
        Find a prompt template by name.
        
        Args:
            name: Template name to search for
            
        Returns:
            PromptTemplate if found, else None
        """
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def find_active_templates(self) -> List[PromptTemplate]:
        """
        Find all active prompt templates.
        
        Returns:
            List of active prompt templates
        """
        query = select(self.model).where(self.model.is_active.is_(True))
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def update_template(self, id: int, update_data: Dict[str, Any]) -> Optional[PromptTemplate]:
        """
        Update a prompt template with partial data.
        
        Args:
            id: ID of the template to update
            update_data: Dictionary with fields to update
            
        Returns:
            Updated PromptTemplate if found, else None
        """
        # Add updated_at timestamp
        update_data["updated_at"] = datetime.utcnow()
        
        # Execute the update
        stmt = update(self.model).where(self.model.id == id).values(**update_data)
        await self.session.execute(stmt)
        await self.session.flush()
        
        # Get the updated template
        return await self.get(id)
    
    async def delete_all(self) -> int:
        """
        Delete all prompt templates.
        
        Returns:
            Number of deleted templates
        """
        # Get count first for the return value
        count_query = select(self.model)
        count_result = await self.session.execute(count_query)
        count = len(list(count_result.scalars().all()))
        
        # Execute the delete
        stmt = delete(self.model)
        await self.session.execute(stmt)
        await self.session.flush()
        
        return count
    
    async def delete(self, id: int) -> bool:
        """
        Delete a prompt template.
        
        Args:
            id: ID of the template to delete
            
        Returns:
            True if deleted, False if not found
        """
        template = await self.get(id)
        if not template:
            return False
        
        await self.session.delete(template)
        await self.session.flush()
        return True 