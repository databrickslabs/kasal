from typing import List, Optional, Dict, Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.core.base_repository import BaseRepository
from src.models.tool import Tool


class ToolRepository(BaseRepository[Tool]):
    """
    Repository for Tool model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.
        
        Args:
            session: SQLAlchemy async session
        """
        super().__init__(Tool, session)
    
    async def find_by_title(self, title: str) -> Optional[Tool]:
        """
        Find a tool by title.
        
        Args:
            title: Tool title to search for
            
        Returns:
            Tool if found, else None
        """
        query = select(self.model).where(self.model.title == title)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def find_enabled(self) -> List[Tool]:
        """
        Find all enabled tools.
        
        Returns:
            List of enabled tools
        """
        query = select(self.model).where(self.model.enabled == True)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def toggle_enabled(self, tool_id: int) -> Optional[Tool]:
        """
        Toggle the enabled status of a tool.
        
        Args:
            tool_id: ID of the tool to toggle
            
        Returns:
            Updated tool if found, else None
        """
        try:
            tool = await self.get(tool_id)
            if not tool:
                return None
            
            # Toggle the enabled status
            tool.enabled = not tool.enabled
            await self.session.commit()
            await self.session.refresh(tool)
            return tool
        except Exception as e:
            # Log the error and rollback
            import logging
            logging.error(f"Error in toggle_enabled for tool ID {tool_id}: {str(e)}")
            await self.session.rollback()
            raise

    async def update_configuration_by_title(self, title: str, config: Dict[str, Any]) -> Optional[Tool]:
        """
        Update configuration for a tool identified by its title.
        
        Args:
            title: Title of the tool to update
            config: New configuration dictionary
            
        Returns:
            Updated Tool object if found and updated, else None
        """
        try:
            tool = await self.find_by_title(title)
            if not tool:
                return None
                
            tool.config = config
            await self.session.commit()
            await self.session.refresh(tool)
            return tool
        except Exception as e:
            # Log the error and rollback
            import logging
            logging.error(f"Error in update_configuration_by_title for {title}: {str(e)}")
            await self.session.rollback()
            raise

    async def enable_all(self) -> List[Tool]:
        """
        Enable all tools in the database.
        
        Returns:
            List of all tools after enabling them.
        """
        try:
            # Update all tools where enabled is False to True
            stmt = update(self.model).where(self.model.enabled == False).values(enabled=True)
            await self.session.execute(stmt)
            await self.session.commit()
            
            # Return all tools (now enabled)
            return await self.list()
        except Exception as e:
            # Log the error and rollback
            import logging
            logging.error(f"Error in enable_all: {str(e)}")
            await self.session.rollback()
            raise

    async def disable_all(self) -> List[Tool]:
        """
        Disable all tools in the database.
        
        Returns:
            List of all tools after disabling them.
        """
        try:
            # Update all tools where enabled is True to False
            stmt = update(self.model).where(self.model.enabled == True).values(enabled=False)
            await self.session.execute(stmt)
            await self.session.commit()
            
            # Return all tools (now disabled)
            return await self.list()
        except Exception as e:
            # Log the error and rollback
            import logging
            logging.error(f"Error in disable_all: {str(e)}")
            await self.session.rollback()
            raise

class SyncToolRepository:
    """
    Synchronous repository for Tool model with custom query methods.
    Used by services that require synchronous DB operations.
    """
    
    def __init__(self, db: Session):
        """
        Initialize the repository with session.
        
        Args:
            db: SQLAlchemy synchronous session
        """
        self.db = db
    
    def find_by_id(self, tool_id: int) -> Optional[Tool]:
        """
        Find a tool by ID.
        
        Args:
            tool_id: ID of the tool to find
            
        Returns:
            Tool if found, else None
        """
        return self.db.query(Tool).filter(Tool.id == tool_id).first()
    
    def find_by_title(self, title: str) -> Optional[Tool]:
        """
        Find a tool by title.
        
        Args:
            title: Title to search for
            
        Returns:
            Tool if found, else None
        """
        return self.db.query(Tool).filter(Tool.title == title).first()
    
    def find_all(self) -> List[Tool]:
        """
        Find all tools.
        
        Returns:
            List of all tools
        """
        return self.db.query(Tool).all()
        
    def find_by_ids(self, tool_ids: List[int]) -> List[Tool]:
        """
        Find tools by their IDs.
        
        Args:
            tool_ids: List of tool IDs to find
            
        Returns:
            List of tools with matching IDs
        """
        return self.db.query(Tool).filter(Tool.id.in_(tool_ids)).all()