from typing import List, Optional, Dict, Any
import json
import logging
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.crew import Crew
from src.repositories.crew_repository import CrewRepository
from src.schemas.crew import CrewCreate, CrewUpdate
from src.utils.user_context import TenantContext

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
    
    # Tenant-aware methods
    async def create_with_tenant(self, obj_in: CrewCreate, tenant_context: TenantContext) -> Crew:
        """
        Create a new crew with tenant context.
        
        Args:
            obj_in: Crew data for creation
            tenant_context: Tenant context from headers
            
        Returns:
            Created crew
        """
        try:
            # Log details for debugging
            logger.info(f"Creating crew with name: {obj_in.name} for tenant: {tenant_context.primary_tenant_id}")
            logger.info(f"Agent IDs: {obj_in.agent_ids}")
            logger.info(f"Task IDs: {obj_in.task_ids}")
            logger.info(f"Number of nodes: {len(obj_in.nodes)}")
            logger.info(f"Number of edges: {len(obj_in.edges)}")
            
            # Convert schema to dict and add tenant fields
            crew_data = obj_in.model_dump()
            crew_data['tenant_id'] = tenant_context.primary_tenant_id
            crew_data['created_by_email'] = tenant_context.tenant_email
            
            # Ensure all lists are properly initialized
            if crew_data.get('agent_ids') is None:
                crew_data['agent_ids'] = []
            if crew_data.get('task_ids') is None:
                crew_data['task_ids'] = []
            if crew_data.get('nodes') is None:
                crew_data['nodes'] = []
            if crew_data.get('edges') is None:
                crew_data['edges'] = []
                
            # Ensure agent_ids and task_ids are strings
            crew_data['agent_ids'] = [str(agent_id) for agent_id in crew_data['agent_ids']]
            crew_data['task_ids'] = [str(task_id) for task_id in crew_data['task_ids']]
                
            # Create the model using the serialized data
            return await self.repository.create(crew_data)
        except Exception as e:
            logger.error(f"Error creating crew with tenant: {str(e)}")
            raise

    async def find_by_tenant(self, tenant_context: TenantContext) -> List[Crew]:
        """
        Find all crews for the given tenant.
        
        Args:
            tenant_context: Tenant context from headers
            
        Returns:
            List of crews for the tenant
        """
        if not tenant_context.tenant_ids:
            # If no tenant context, return empty list for security
            return []
        
        return await self.repository.find_by_tenant(tenant_context.tenant_ids)

    async def get_by_tenant(self, id: UUID, tenant_context: TenantContext) -> Optional[Crew]:
        """
        Get a crew by ID, ensuring it belongs to the tenant.
        
        Args:
            id: ID of the crew to get
            tenant_context: Tenant context from headers
            
        Returns:
            Crew if found and belongs to tenant, else None
        """
        if not tenant_context.tenant_ids:
            return None
        
        return await self.repository.get_by_tenant(id, tenant_context.tenant_ids)

    async def update_with_partial_data_by_tenant(self, id: UUID, obj_in: CrewUpdate, tenant_context: TenantContext) -> Optional[Crew]:
        """
        Update a crew with partial data, ensuring it belongs to the tenant.
        
        Args:
            id: ID of the crew to update
            obj_in: Schema with fields to update
            tenant_context: Tenant context from headers
            
        Returns:
            Updated crew if found and belongs to tenant, else None
        """
        if not tenant_context.tenant_ids:
            return None
        
        # First verify the crew exists and belongs to the tenant
        existing_crew = await self.repository.get_by_tenant(id, tenant_context.tenant_ids)
        if not existing_crew:
            return None
        
        # Exclude unset fields (None) from update
        update_data = obj_in.model_dump(exclude_none=True)
        if not update_data:
            # No fields to update
            return existing_crew
        
        return await self.repository.update(id, update_data)

    async def delete_by_tenant(self, id: UUID, tenant_context: TenantContext) -> bool:
        """
        Delete a crew by ID, ensuring it belongs to the tenant.
        
        Args:
            id: ID of the crew to delete
            tenant_context: Tenant context from headers
            
        Returns:
            True if crew was deleted, False if not found or doesn't belong to tenant
        """
        if not tenant_context.tenant_ids:
            return False
        
        return await self.repository.delete_by_tenant(id, tenant_context.tenant_ids)

    async def delete_all_by_tenant(self, tenant_context: TenantContext) -> None:
        """
        Delete all crews for the given tenant.
        
        Args:
            tenant_context: Tenant context from headers
        """
        if not tenant_context.tenant_ids:
            return
        
        await self.repository.delete_all_by_tenant(tenant_context.tenant_ids) 