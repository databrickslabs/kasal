from typing import List, Optional, Type

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.core.base_repository import BaseRepository
from src.models.agent import Agent
from src.db.session import SessionLocal


class AgentRepository(BaseRepository[Agent]):
    """
    Repository for Agent model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """
    
    def __init__(self, model: Type[Agent], session: AsyncSession):
        """
        Initialize the repository with model and session.
        
        Args:
            model: SQLAlchemy model class
            session: SQLAlchemy async session
        """
        super().__init__(model, session)
        
    async def get(self, id: str) -> Optional[Agent]:
        """
        Get a single agent by ID.
        
        Args:
            id: ID of the agent to get
            
        Returns:
            The agent if found, else None
        """
        try:
            query = select(self.model).where(self.model.id == id)
            result = await self.session.execute(query)
            return result.scalars().first()
        except Exception as e:
            await self.session.rollback()
            raise
            
    async def update(self, id: str, obj_in: dict) -> Optional[Agent]:
        """
        Update an existing agent.
        
        Args:
            id: ID of the agent to update
            obj_in: Dictionary of values to update model with
            
        Returns:
            The updated agent if found, else None
        """
        try:
            db_obj = await self.get(id)
            if db_obj:
                for key, value in obj_in.items():
                    setattr(db_obj, key, value)
                await self.session.flush()
            return db_obj
        except Exception as e:
            await self.session.rollback()
            raise
            
    async def delete(self, id: str) -> bool:
        """
        Delete an agent by ID.
        
        Args:
            id: ID of the agent to delete
            
        Returns:
            True if agent was deleted, False if not found
        """
        try:
            db_obj = await self.get(id)
            if db_obj:
                await self.session.delete(db_obj)
                await self.session.flush()
                return True
            return False
        except Exception as e:
            await self.session.rollback()
            raise
    
    async def find_by_name(self, name: str) -> Optional[Agent]:
        """
        Find an agent by name.
        
        Args:
            name: Name to search for
            
        Returns:
            Agent if found, else None
        """
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    async def find_all(self) -> List[Agent]:
        """
        Find all agents.
        
        Returns:
            List of all agents
        """
        query = select(self.model)
        result = await self.session.execute(query)
        return list(result.scalars().all())
    
    async def delete_all(self) -> None:
        """
        Delete all agents.
        
        Returns:
            None
        """
        stmt = delete(self.model)
        await self.session.execute(stmt)
        await self.session.flush()


class SyncAgentRepository:
    """
    Synchronous repository for Agent model with custom query methods.
    Used by services that require synchronous DB operations.
    """
    
    def __init__(self, db: Session):
        """
        Initialize the repository with session.
        
        Args:
            db: SQLAlchemy synchronous session
        """
        self.db = db
    
    def find_by_id(self, agent_id: int) -> Optional[Agent]:
        """
        Find an agent by ID.
        
        Args:
            agent_id: ID of the agent to find
            
        Returns:
            Agent if found, else None
        """
        return self.db.query(Agent).filter(Agent.id == agent_id).first()
    
    def find_by_name(self, name: str) -> Optional[Agent]:
        """
        Find an agent by name.
        
        Args:
            name: Name to search for
            
        Returns:
            Agent if found, else None
        """
        return self.db.query(Agent).filter(Agent.name == name).first()
    
    def find_all(self) -> List[Agent]:
        """
        Find all agents.
        
        Returns:
            List of all agents
        """
        return self.db.query(Agent).all()

# Factory function to get a repository instance without managing the session in the service
def get_sync_agent_repository() -> SyncAgentRepository:
    """
    Factory function to create and return a SyncAgentRepository instance.
    This handles session creation internally.
    
    Returns:
        A SyncAgentRepository instance with an active session
    """
    db = SessionLocal()
    return SyncAgentRepository(db) 