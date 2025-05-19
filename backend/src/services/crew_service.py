from typing import List, Optional, Dict, Any
import json
import logging
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.crew import Crew
from src.repositories.crew_repository import CrewRepository
from src.schemas.crew import CrewCreate, CrewUpdate

logger = logging.getLogger(__name__)


class CrewService:
    """
    Service for Crew model with business logic.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the service with database session.
        
        Args:
            session: Database session for operations
        """
        self.session = session
        self.repository = CrewRepository(session)
    
    async def get(self, id: UUID) -> Optional[Crew]:
        """
        Get a crew by ID.
        
        Args:
            id: ID of the crew to get
            
        Returns:
            Crew if found, else None
        """
        return await self.repository.get(id)
        
    async def create(self, obj_in: CrewCreate) -> Crew:
        """
        Create a new crew.
        
        Args:
            obj_in: Crew data for creation
            
        Returns:
            Created crew
        """
        return await self.repository.create(obj_in.model_dump())
    
    async def find_by_name(self, name: str) -> Optional[Crew]:
        """
        Find a crew by name.
        
        Args:
            name: Name to search for
            
        Returns:
            Crew if found, else None
        """
        return await self.repository.find_by_name(name)
    
    async def find_all(self) -> List[Crew]:
        """
        Find all crews.
        
        Returns:
            List of all crews
        """
        return await self.repository.find_all()
    
    async def update_with_partial_data(self, id: UUID, obj_in: CrewUpdate) -> Optional[Crew]:
        """
        Update a crew with partial data, only updating fields that are set.
        
        Args:
            id: ID of the crew to update
            obj_in: Schema with fields to update
            
        Returns:
            Updated crew if found, else None
        """
        # Exclude unset fields (None) from update
        update_data = obj_in.model_dump(exclude_none=True)
        if not update_data:
            # No fields to update
            return await self.get(id)
        
        return await self.repository.update(id, update_data)
    
    async def create_crew(self, obj_in: CrewCreate) -> Optional[Crew]:
        """
        Create a new crew with properly serialized data.
        
        Args:
            obj_in: Crew data for creation
            
        Returns:
            Created crew
        """
        try:
            # Log details for debugging
            logger.info(f"Creating crew with name: {obj_in.name}")
            logger.info(f"Agent IDs: {obj_in.agent_ids}")
            logger.info(f"Task IDs: {obj_in.task_ids}")
            logger.info(f"Number of nodes: {len(obj_in.nodes)}")
            logger.info(f"Number of edges: {len(obj_in.edges)}")
            
            # Properly serialize the complex JSON data
            crew_dict = obj_in.model_dump()
            
            # Ensure all lists are properly initialized
            if crew_dict.get('agent_ids') is None:
                crew_dict['agent_ids'] = []
            if crew_dict.get('task_ids') is None:
                crew_dict['task_ids'] = []
            if crew_dict.get('nodes') is None:
                crew_dict['nodes'] = []
            if crew_dict.get('edges') is None:
                crew_dict['edges'] = []
                
            # Ensure agent_ids and task_ids are strings
            crew_dict['agent_ids'] = [str(agent_id) for agent_id in crew_dict['agent_ids']]
            crew_dict['task_ids'] = [str(task_id) for task_id in crew_dict['task_ids']]
                
            # Create the model using the serialized data
            return await self.repository.create(crew_dict)
        except Exception as e:
            logger.error(f"Error creating crew: {str(e)}")
            raise
    
    async def delete(self, id: UUID) -> bool:
        """
        Delete a crew by ID.
        
        Args:
            id: ID of the crew to delete
            
        Returns:
            True if crew was deleted, False if not found
        """
        return await self.repository.delete(id)
    
    async def delete_all(self) -> None:
        """
        Delete all crews.
        
        Returns:
            None
        """
        await self.repository.delete_all() 