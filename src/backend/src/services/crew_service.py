from typing import List, Optional, Dict, Any
import json
import logging
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import ConflictError
from src.models.crew import Crew
from src.repositories.crew_repository import CrewRepository
from src.schemas.crew import CrewCreate, CrewUpdate
from src.utils.user_context import GroupContext
from src.utils.sensitive_data_utils import (
    encrypt_sensitive_fields,
    decrypt_sensitive_fields,
    safe_log_tool_configs,
)

logger = logging.getLogger(__name__)


class CrewService:
    """
    Service for Crew model with business logic.

    Security: This service handles encryption/decryption of sensitive fields
    in tool_configs to protect credentials stored in the database.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the service with database session.

        Args:
            session: Database session for operations
        """
        self.session = session
        self.repository = CrewRepository(session)

    def _decrypt_crew_tool_configs(self, crew: Optional[Crew]) -> Optional[Crew]:
        """
        Decrypt sensitive fields in crew's tool_configs after retrieval.

        Args:
            crew: Crew with potentially encrypted tool_configs

        Returns:
            Crew with decrypted tool_configs (in-memory only)
        """
        if crew and crew.tool_configs:
            try:
                crew.tool_configs = decrypt_sensitive_fields(crew.tool_configs)
            except Exception as e:
                logger.error(f"Failed to decrypt tool_configs for crew {crew.id}: {e}")
        return crew

    def _encrypt_tool_configs_in_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encrypt sensitive fields in tool_configs before storage.

        Args:
            data: Dictionary containing crew data with tool_configs

        Returns:
            Dictionary with encrypted tool_configs
        """
        if 'tool_configs' in data and data['tool_configs']:
            try:
                data['tool_configs'] = encrypt_sensitive_fields(data['tool_configs'])
                logger.debug(safe_log_tool_configs(data['tool_configs'], "Encrypted "))
            except Exception as e:
                logger.error(f"Failed to encrypt tool_configs: {e}")
                raise
        return data
    
    async def get(self, id: UUID) -> Optional[Crew]:
        """
        Get a crew by ID with decrypted tool_configs.

        Args:
            id: ID of the crew to get

        Returns:
            Crew if found, else None (with decrypted tool_configs)
        """
        crew = await self.repository.get(id)
        return self._decrypt_crew_tool_configs(crew)

    async def create(self, obj_in: CrewCreate) -> Crew:
        """
        Create a new crew with encrypted tool_configs.

        Args:
            obj_in: Crew data for creation

        Returns:
            Created crew (with decrypted tool_configs for response)
        """
        data = obj_in.model_dump()
        data = self._encrypt_tool_configs_in_data(data)
        crew = await self.repository.create(data)
        self._decrypt_crew_tool_configs(crew)
        return crew

    async def find_by_name(self, name: str) -> Optional[Crew]:
        """
        Find a crew by name with decrypted tool_configs.

        Args:
            name: Name to search for

        Returns:
            Crew if found, else None (with decrypted tool_configs)
        """
        crew = await self.repository.find_by_name(name)
        return self._decrypt_crew_tool_configs(crew)

    async def find_all(self) -> List[Crew]:
        """
        Find all crews with decrypted tool_configs.

        Returns:
            List of all crews (with decrypted tool_configs)
        """
        crews = await self.repository.find_all()
        for crew in crews:
            self._decrypt_crew_tool_configs(crew)
        return crews

    async def update_with_partial_data(self, id: UUID, obj_in: CrewUpdate) -> Optional[Crew]:
        """
        Update a crew with partial data, only updating fields that are set.
        Encrypts sensitive fields in tool_configs before storage.

        Args:
            id: ID of the crew to update
            obj_in: Schema with fields to update

        Returns:
            Updated crew if found, else None (with decrypted tool_configs)
        """
        # Exclude unset fields (None) from update
        update_data = obj_in.model_dump(exclude_none=True)
        if not update_data:
            # No fields to update
            return await self.get(id)

        # Encrypt sensitive fields in tool_configs before storage
        if 'tool_configs' in update_data:
            logger.debug(f"CrewService: encrypting tool_configs for crew {id}")
            update_data = self._encrypt_tool_configs_in_data(update_data)

        crew = await self.repository.update(id, update_data)
        return self._decrypt_crew_tool_configs(crew)
    
    async def create_crew(self, obj_in: CrewCreate) -> Optional[Crew]:
        """
        Create a new crew with properly serialized data.
        Encrypts sensitive fields in tool_configs before storage.

        Args:
            obj_in: Crew data for creation

        Returns:
            Created crew (with decrypted tool_configs for response)
        """
        try:
            # Log details for debugging
            logger.info(f"Creating crew with name: {obj_in.name}")
            logger.info(f"Agent IDs: {obj_in.agent_ids}")
            logger.info(f"Task IDs: {obj_in.task_ids}")
            logger.info(f"Number of nodes: {len(obj_in.nodes) if obj_in.nodes else 0}")
            logger.info(f"Number of edges: {len(obj_in.edges) if obj_in.edges else 0}")

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
            crew_dict['agent_ids'] = [str(agent_id) for agent_id in crew_dict['agent_ids']] if crew_dict['agent_ids'] else []
            crew_dict['task_ids'] = [str(task_id) for task_id in crew_dict['task_ids']] if crew_dict['task_ids'] else []

            # Encrypt sensitive fields in tool_configs before storage
            crew_dict = self._encrypt_tool_configs_in_data(crew_dict)

            # Create the model using the serialized data
            crew = await self.repository.create(crew_dict)
            self._decrypt_crew_tool_configs(crew)
            return crew
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
    
    # Group-aware methods
    async def create_with_group(self, obj_in: CrewCreate, group_context: GroupContext) -> Crew:
        """
        Create a new crew with group context.
        Encrypts sensitive fields in tool_configs before storage.

        Args:
            obj_in: Crew data for creation
            group_context: Group context from headers

        Returns:
            Created crew (with decrypted tool_configs for response)

        Raises:
            ConflictError: If a crew with the same name already exists in the group
        """
        try:
            # Check for duplicate name within the group
            primary_group_id = group_context.primary_group_id
            if primary_group_id:
                existing = await self.repository.find_by_name_and_group(
                    obj_in.name, [primary_group_id]
                )
                if existing:
                    raise ConflictError(
                        detail=f"A crew with the name '{obj_in.name}' already exists. Please choose a different name."
                    )

            # Log details for debugging
            logger.info(f"Creating crew with name: {obj_in.name} for group: {group_context.primary_group_id}")
            logger.info(f"Agent IDs: {obj_in.agent_ids}")
            logger.info(f"Task IDs: {obj_in.task_ids}")
            logger.info(f"Number of nodes: {len(obj_in.nodes)}")
            logger.info(f"Number of edges: {len(obj_in.edges)}")

            # Convert schema to dict and add group fields
            crew_data = obj_in.model_dump()
            crew_data['group_id'] = group_context.primary_group_id
            crew_data['created_by_email'] = group_context.group_email

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

            # Encrypt sensitive fields in tool_configs before storage
            crew_data = self._encrypt_tool_configs_in_data(crew_data)

            # Create the model using the serialized data
            crew = await self.repository.create(crew_data)
            self._decrypt_crew_tool_configs(crew)
            return crew
        except Exception as e:
            logger.error(f"Error creating crew with group: {str(e)}")
            raise

    async def find_by_group(self, group_context: GroupContext) -> List[Crew]:
        """
        Find all crews for the CURRENT workspace (primary group only).
        Returns crews with decrypted tool_configs.

        Args:
            group_context: Group context from headers

        Returns:
            List of crews for the selected workspace (with decrypted tool_configs)
        """
        primary_group_id = getattr(group_context, "primary_group_id", None)
        if not primary_group_id:
            # If no current workspace, return empty list for security
            return []

        crews = await self.repository.find_by_group([primary_group_id])
        for crew in crews:
            self._decrypt_crew_tool_configs(crew)
        return crews

    async def get_by_group(self, id: UUID, group_context: GroupContext) -> Optional[Crew]:
        """
        Get a crew by ID, ensuring it belongs to the CURRENT workspace (primary group).
        Returns crew with decrypted tool_configs.

        Args:
            id: ID of the crew to get
            group_context: Group context from headers

        Returns:
            Crew if found and belongs to current workspace, else None (with decrypted tool_configs)
        """
        primary_group_id = getattr(group_context, "primary_group_id", None)
        if not primary_group_id:
            return None

        crew = await self.repository.get_by_group(id, [primary_group_id])
        return self._decrypt_crew_tool_configs(crew)

    async def update_with_partial_data_by_group(self, id: UUID, obj_in: CrewUpdate, group_context: GroupContext) -> Optional[Crew]:
        """
        Update a crew with partial data, ensuring it belongs to the CURRENT workspace (primary group).
        Encrypts sensitive fields in tool_configs before storage.

        Args:
            id: ID of the crew to update
            obj_in: Schema with fields to update
            group_context: Group context from headers

        Returns:
            Updated crew if found and belongs to current workspace, else None (with decrypted tool_configs)
        """
        primary_group_id = getattr(group_context, "primary_group_id", None)
        if not primary_group_id:
            return None

        # First verify the crew exists and belongs to the current workspace
        existing_crew = await self.repository.get_by_group(id, [primary_group_id])
        if not existing_crew:
            return None

        # Exclude unset fields (None) from update
        update_data = obj_in.model_dump(exclude_none=True)
        if not update_data:
            # No fields to update
            self._decrypt_crew_tool_configs(existing_crew)
            return existing_crew

        # Check for duplicate name within the group (if name is being changed)
        if 'name' in update_data and update_data['name'] != existing_crew.name:
            duplicate = await self.repository.find_by_name_and_group(
                update_data['name'], [primary_group_id], exclude_id=id
            )
            if duplicate:
                raise ConflictError(
                    detail=f"A crew with the name '{update_data['name']}' already exists. Please choose a different name."
                )

        # Encrypt sensitive fields in tool_configs before storage
        if 'tool_configs' in update_data:
            logger.debug(f"CrewService: encrypting tool_configs for crew {id}")
            update_data = self._encrypt_tool_configs_in_data(update_data)

        crew = await self.repository.update(id, update_data)
        return self._decrypt_crew_tool_configs(crew)

    async def delete_by_group(self, id: UUID, group_context: GroupContext) -> bool:
        """
        Delete a crew by ID, ensuring it belongs to the CURRENT workspace (primary group).

        Args:
            id: ID of the crew to delete
            group_context: Group context from headers

        Returns:
            True if crew was deleted, False if not found or doesn't belong to current workspace
        """
        primary_group_id = getattr(group_context, "primary_group_id", None)
        if not primary_group_id:
            return False

        return await self.repository.delete_by_group(id, [primary_group_id])

    async def delete_all_by_group(self, group_context: GroupContext) -> None:
        """
        Delete all crews for the CURRENT workspace (primary group only).
        
        Args:
            group_context: Group context from headers
        """
        primary_group_id = getattr(group_context, "primary_group_id", None)
        if not primary_group_id:
            return
        
        await self.repository.delete_all_by_group([primary_group_id])