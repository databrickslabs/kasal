from typing import List, Optional, Dict, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.core.base_repository import BaseRepository
from src.models.mcp_server import MCPServer
from src.models.mcp_settings import MCPSettings


class MCPServerRepository(BaseRepository[MCPServer]):
    """
    Repository for MCPServer model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.
        
        Args:
            session: SQLAlchemy async session
        """
        super().__init__(MCPServer, session)
    
    async def find_by_name(self, name: str) -> Optional[MCPServer]:
        """
        Find a MCP server by name.
        
        Args:
            name: Server name to search for
            
        Returns:
            MCPServer if found, else None
        """
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def find_enabled(self) -> List[MCPServer]:
        """
        Find all enabled MCP servers.
        
        Returns:
            List of enabled MCP servers
        """
        query = select(self.model).where(self.model.enabled == True)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def toggle_enabled(self, server_id: int) -> Optional[MCPServer]:
        """
        Toggle the enabled status of a MCP server.
        
        Args:
            server_id: ID of the server to toggle
            
        Returns:
            Updated MCP server if found, else None
        """
        try:
            server = await self.get(server_id)
            if not server:
                return None
            
            # Toggle the enabled status
            server.enabled = not server.enabled
            await self.session.commit()
            await self.session.refresh(server)
            return server
        except Exception as e:
            # Log the error and rollback
            import logging
            logging.error(f"Error in toggle_enabled for MCP server ID {server_id}: {str(e)}")
            await self.session.rollback()
            raise


class MCPSettingsRepository(BaseRepository[MCPSettings]):
    """
    Repository for MCPSettings model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.
        
        Args:
            session: SQLAlchemy async session
        """
        super().__init__(MCPSettings, session)
    
    async def get_settings(self) -> MCPSettings:
        """
        Get global MCP settings, creating default settings if none exist.
        
        Returns:
            MCPSettings object
        """
        query = select(self.model)
        result = await self.session.execute(query)
        settings = result.scalars().first()
        
        if not settings:
            # Create default settings
            settings = MCPSettings(global_enabled=False)
            self.session.add(settings)
            await self.session.commit()
            await self.session.refresh(settings)
        
        return settings
    
    async def update_global_enabled(self, enabled: bool) -> MCPSettings:
        """
        Update the global enabled status.
        
        Args:
            enabled: New enabled status
            
        Returns:
            Updated MCPSettings object
        """
        settings = await self.get_settings()
        settings.global_enabled = enabled
        await self.session.commit()
        await self.session.refresh(settings)
        return settings


class SyncMCPServerRepository:
    """
    Synchronous repository for MCPServer model.
    Used by services that require synchronous DB operations.
    """
    
    def __init__(self, db: Session):
        """
        Initialize the repository with session.
        
        Args:
            db: SQLAlchemy synchronous session
        """
        self.db = db
    
    def find_by_id(self, server_id: int) -> Optional[MCPServer]:
        """
        Find a MCP server by ID.
        
        Args:
            server_id: ID of the server to find
            
        Returns:
            MCPServer if found, else None
        """
        return self.db.query(MCPServer).filter(MCPServer.id == server_id).first()
    
    def find_by_name(self, name: str) -> Optional[MCPServer]:
        """
        Find a MCP server by name.
        
        Args:
            name: Name to search for
            
        Returns:
            MCPServer if found, else None
        """
        return self.db.query(MCPServer).filter(MCPServer.name == name).first()
    
    def find_all(self) -> List[MCPServer]:
        """
        Find all MCP servers.
        
        Returns:
            List of all MCP servers
        """
        return self.db.query(MCPServer).all()
        
    def find_enabled(self) -> List[MCPServer]:
        """
        Find all enabled MCP servers.
        
        Returns:
            List of enabled MCP servers
        """
        return self.db.query(MCPServer).filter(MCPServer.enabled == True).all() 