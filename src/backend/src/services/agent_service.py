from typing import List, Optional, Type
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.core.base_service import BaseService
from src.models.agent import Agent
from src.repositories.agent_repository import AgentRepository
from src.schemas.agent import AgentCreate, AgentUpdate, AgentLimitedUpdate
from src.utils.user_context import GroupContext


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
        self.repository = repository_class(session)
    
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
    
    async def get_with_group_check(self, id: str, group_context: GroupContext) -> Optional[Agent]:
        """
        Get an agent by ID with group verification.
        
        Args:
            id: ID of the agent to get
            group_context: Group context for verification
            
        Returns:
            Agent if found and belongs to user's group, else None
        """
        agent = await self.repository.get(id)
        if agent and group_context and group_context.group_ids:
            # Check if agent belongs to one of the user's groups
            if agent.group_id not in group_context.group_ids:
                return None  # Return None to trigger 404, not 403 (avoid information leakage)
        return agent
        
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
    
    async def update_with_group_check(self, id: str, obj_in: AgentUpdate, group_context: GroupContext) -> Optional[Agent]:
        """
        Update an agent with group verification.
        
        Args:
            id: ID of the agent to update
            obj_in: Schema with fields to update
            group_context: Group context for verification
            
        Returns:
            Updated agent if found and belongs to user's group, else None
        """
        # First verify the agent belongs to the user's group
        agent = await self.get_with_group_check(id, group_context)
        if not agent:
            return None
        
        # Exclude unset fields (None) from update
        update_data = obj_in.model_dump(exclude_none=True)
        if not update_data:
            # No fields to update
            return agent
        
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
    
    async def update_limited_with_group_check(self, id: str, obj_in: AgentLimitedUpdate, group_context: GroupContext) -> Optional[Agent]:
        """
        Update limited fields of an agent with group verification.
        
        Args:
            id: ID of the agent to update
            obj_in: Schema with limited fields to update
            group_context: Group context for verification
            
        Returns:
            Updated agent if found and belongs to user's group, else None
        """
        # First verify the agent belongs to the user's group
        agent = await self.get_with_group_check(id, group_context)
        if not agent:
            return None
        
        # Exclude unset fields (None) from update
        update_data = obj_in.model_dump(exclude_none=True)
        if not update_data:
            # No fields to update
            return agent
        
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
    
    async def delete_with_group_check(self, id: str, group_context: GroupContext) -> bool:
        """
        Delete an agent by ID with group verification.
        
        Args:
            id: ID of the agent to delete
            group_context: Group context for verification
            
        Returns:
            True if agent was deleted, False if not found or not authorized
        """
        # First verify the agent belongs to the user's group
        agent = await self.get_with_group_check(id, group_context)
        if not agent:
            return False
        
        return await self.repository.delete(id)
    
    async def delete_all(self) -> None:
        """
        Delete all agents.
        
        Returns:
            None
        """
        await self.repository.delete_all()
    
    async def delete_all_for_group(self, group_context: GroupContext) -> None:
        """
        Delete all agents for a specific group.
        
        Args:
            group_context: Group context for filtering
            
        Returns:
            None
        """
        if not group_context or not group_context.group_ids:
            return
        
        # Get all agents for the group
        agents = await self.find_by_group(group_context)
        
        # Delete each agent
        for agent in agents:
            await self.repository.delete(agent.id)
    

    async def create_with_group(self, obj_in: AgentCreate, group_context: GroupContext) -> Agent:
        """
        Create a new agent with group isolation.
        
        Args:
            obj_in: Agent data for creation
            group_context: Group context from headers
            
        Returns:
            Created agent with group information
        """
        # Convert schema to dict and add group fields
        agent_data = obj_in.model_dump()
        agent_data['group_id'] = group_context.primary_group_id
        agent_data['created_by_email'] = group_context.group_email
        
        # Create agent using repository (pass dict, not object)
        return await self.repository.create(agent_data)
    
    async def find_by_group(self, group_context: GroupContext) -> List[Agent]:
        """
        Find all agents for a specific group.
        
        Args:
            group_context: Group context from headers
            
        Returns:
            List of agents for the specified group
        """
        if not group_context.group_ids:
            # If no group context, return empty list for security
            return []
        
        # Filter by group IDs and order by created_at descending (newest first)
        stmt = select(Agent).where(Agent.group_id.in_(group_context.group_ids)).order_by(Agent.created_at.desc())
        result = await self.session.execute(stmt)
        return result.scalars().all() 