from typing import List, Optional, Type
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_service import BaseService
from src.models.agent import Agent
from src.repositories.agent_repository import AgentRepository
from src.schemas.agent import AgentCreate, AgentUpdate, AgentLimitedUpdate


class AgentService(BaseService[Agent, AgentCreate]):
    """
    Service for Agent model with business logic.
    """
    
    def __init__(
        self,
        session: AsyncSession,
        repository_class: Type[AgentRepository] = AgentRepository,
        model_class: Type[Agent] = Agent
    ):
        """
        Initialize the service with session and optional repository and model classes.
        
        Args:
            session: Database session for operations
            repository_class: Repository class to use for data access (optional)
            model_class: Model class associated with this service (optional)
        """
        super().__init__(session)
        self.repository_class = repository_class
        self.model_class = model_class
        self.repository = repository_class(model_class, session)
    
    @classmethod
    def create(cls, session: AsyncSession) -> 'AgentService':
        """
        Factory method to create a properly configured AgentService instance.
        
        Args:
            session: Database session for operations
            
        Returns:
            An instance of AgentService
        """
        return cls(session=session)
    
    async def get(self, id: str) -> Optional[Agent]:
        """
        Get an agent by ID.
        
        Args:
            id: ID of the agent to get
            
        Returns:
            Agent if found, else None
        """
        return await self.repository.get(id)
        
    async def create(self, obj_in: AgentCreate) -> Agent:
        """
        Create a new agent.
        
        Args:
            obj_in: Agent data for creation
            
        Returns:
            Created agent
        """
        return await self.repository.create(obj_in.model_dump())
    
    async def find_by_name(self, name: str) -> Optional[Agent]:
        """
        Find an agent by name.
        
        Args:
            name: Name to search for
            
        Returns:
            Agent if found, else None
        """
        return await self.repository.find_by_name(name)
    
    async def find_all(self) -> List[Agent]:
        """
        Find all agents.
        
        Returns:
            List of all agents
        """
        return await self.repository.find_all()
    
    async def update_with_partial_data(self, id: str, obj_in: AgentUpdate) -> Optional[Agent]:
        """
        Update an agent with partial data, only updating fields that are set.
        
        Args:
            id: ID of the agent to update
            obj_in: Schema with fields to update
            
        Returns:
            Updated agent if found, else None
        """
        # Exclude unset fields (None) from update
        update_data = obj_in.model_dump(exclude_none=True)
        if not update_data:
            # No fields to update
            return await self.get(id)
        
        return await self.repository.update(id, update_data)
    
    async def update_limited_fields(self, id: str, obj_in: AgentLimitedUpdate) -> Optional[Agent]:
        """
        Update only limited fields of an agent.
        
        Args:
            id: ID of the agent to update
            obj_in: Schema with limited fields to update
            
        Returns:
            Updated agent if found, else None
        """
        # Exclude unset fields (None) from update
        update_data = obj_in.model_dump(exclude_none=True)
        if not update_data:
            # No fields to update
            return await self.get(id)
        
        return await self.repository.update(id, update_data)
    
    async def delete(self, id: str) -> bool:
        """
        Delete an agent by ID.
        
        Args:
            id: ID of the agent to delete
            
        Returns:
            True if agent was deleted, False if not found
        """
        return await self.repository.delete(id)
    
    async def delete_all(self) -> None:
        """
        Delete all agents.
        
        Returns:
            None
        """
        await self.repository.delete_all() 