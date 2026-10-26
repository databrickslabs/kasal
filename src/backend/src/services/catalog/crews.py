import logging
from typing import Any, List, Optional, Union
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import ConflictError
from src.models.crew import Crew
from src.repositories.crew_repository import CrewRepository
from src.schemas.crew import CrewCreate, CrewUpdate
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class CrewService:
    """
    Service for Crew model with business logic.

    Security: ``crews.tool_configs`` and ``crews.nodes`` are ``SensitiveJSON``
    columns, so the credentials in them are encrypted on every write and
    decrypted on load. This service works with plaintext and must never encrypt
    or decrypt them itself.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the service with database session.

        Args:
            session: Database session for operations
        """
        self.session = session
        self.repository = CrewRepository(session)

    async def get(self, id: Union[UUID, str]) -> Optional[Crew]:
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

    async def update_with_partial_data(
        self, id: UUID, obj_in: CrewUpdate
    ) -> Optional[Crew]:
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
            logger.info(f"Number of nodes: {len(obj_in.nodes) if obj_in.nodes else 0}")
            logger.info(f"Number of edges: {len(obj_in.edges) if obj_in.edges else 0}")

            # Properly serialize the complex JSON data
            crew_dict = obj_in.model_dump()

            # Ensure all lists are properly initialized
            if crew_dict.get("agent_ids") is None:
                crew_dict["agent_ids"] = []
            if crew_dict.get("task_ids") is None:
                crew_dict["task_ids"] = []
            if crew_dict.get("nodes") is None:
                crew_dict["nodes"] = []
            if crew_dict.get("edges") is None:
                crew_dict["edges"] = []

            # Ensure agent_ids and task_ids are strings
            crew_dict["agent_ids"] = (
                [str(agent_id) for agent_id in crew_dict["agent_ids"]]
                if crew_dict["agent_ids"]
                else []
            )
            crew_dict["task_ids"] = (
                [str(task_id) for task_id in crew_dict["task_ids"]]
                if crew_dict["task_ids"]
                else []
            )

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
        deleted = await self.repository.delete(id)
        if deleted:
            await self._withdraw_publications(entity_ids=[id])
        return deleted

    async def delete_all(self) -> None:
        """
        Delete all crews.

        Returns:
            None
        """
        await self.repository.delete_all()
        await self._withdraw_publications()

    async def _withdraw_publications(
        self,
        entity_ids: Optional[List[UUID]] = None,
        group_ids: Optional[List[str]] = None,
    ) -> None:
        """Take deleted crews off every external surface.

        A publication outlives the crew it names unless this removes it, and the
        registry is what the MCP tool list, the A2A card and the chat route
        catalogue all read — one workspace was advertising nine MCP tools for
        crews that no longer existed. A dangling row also holds its external
        name, so the deleted crew's name could never be reused.

        Best-effort: the crews ARE gone by the time this runs, and reporting a
        deletion that happened as a failure would be the worse trade. The
        catalogue also drops dangling rows on read, so a miss here costs a stale
        row in the publish dialog, not a phantom capability.
        """
        from src.services.publications import cleanup

        try:
            if entity_ids is None:
                await cleanup.withdraw_all(self.session, "crew", group_ids)
            else:
                await cleanup.withdraw_entities(
                    self.session, "crew", entity_ids, group_ids
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not unpublish deleted crew(s): %s", exc)

    # Group-aware methods
    async def create_with_group(
        self, obj_in: CrewCreate, group_context: GroupContext, overwrite: bool = False
    ) -> Crew:
        """
        Create a new crew with group context.

        Args:
            obj_in: Crew data for creation
            group_context: Group context from headers
            overwrite: When True and a crew with the same name already exists in the
                group, replace that crew's definition instead of raising ConflictError.

        Returns:
            Created (or overwritten) crew

        Raises:
            ConflictError: If a crew with the same name already exists and overwrite is False
        """
        try:
            # Check for duplicate name within the group
            primary_group_id = group_context.primary_group_id
            existing = None
            if primary_group_id:
                existing = await self.repository.find_by_name_and_group(
                    obj_in.name, [primary_group_id]
                )
                if existing and not overwrite:
                    raise ConflictError(
                        detail=f"A crew with the name '{obj_in.name}' already exists. Please choose a different name."
                    )

            # Log details for debugging
            action = "Overwriting" if existing else "Creating"
            logger.info(
                f"{action} crew with name: {obj_in.name} for group: {primary_group_id}"
            )
            logger.info(f"Agent IDs: {obj_in.agent_ids}")
            logger.info(f"Task IDs: {obj_in.task_ids}")
            logger.info(f"Number of nodes: {len(obj_in.nodes)}")
            logger.info(f"Number of edges: {len(obj_in.edges)}")

            # Convert schema to dict and add group fields
            crew_data = obj_in.model_dump()
            crew_data["group_id"] = primary_group_id

            # Ensure all lists are properly initialized
            if crew_data.get("agent_ids") is None:
                crew_data["agent_ids"] = []
            if crew_data.get("task_ids") is None:
                crew_data["task_ids"] = []
            if crew_data.get("nodes") is None:
                crew_data["nodes"] = []
            if crew_data.get("edges") is None:
                crew_data["edges"] = []

            # Ensure agent_ids and task_ids are strings
            crew_data["agent_ids"] = [
                str(agent_id) for agent_id in crew_data["agent_ids"]
            ]
            crew_data["task_ids"] = [str(task_id) for task_id in crew_data["task_ids"]]

            if existing:
                # Overwrite: replace the existing crew's definition, preserving its
                # id and original creator (don't overwrite created_by_email).
                logger.info(
                    f"Overwriting existing crew '{obj_in.name}' (id={existing.id})"
                )
                return await self.repository.update(existing.id, crew_data) or existing

            # Create the model using the serialized data
            crew_data["created_by_email"] = group_context.group_email
            return await self.repository.create(crew_data)
        except Exception as e:
            logger.error(f"Error creating crew with group: {str(e)}")
            raise

    async def find_by_group(self, group_context: Optional[GroupContext]) -> List[Crew]:
        """
        Find all crews for the CURRENT workspace (primary group only).

        Args:
            group_context: Group context from headers

        Returns:
            List of crews for the selected workspace
        """
        primary_group_id = getattr(group_context, "primary_group_id", None)
        if not primary_group_id:
            # If no current workspace, return empty list for security
            return []

        return await self.repository.find_by_group([primary_group_id])

    async def get_crews_by_ids(self, crew_ids: List[Any]) -> List[Crew]:
        """Crews for a set of ids.

        Crews are this service's domain; `publications` and the prompt optimiser
        used to build ``CrewRepository`` themselves to resolve references.
        """
        return await self.repository.find_by_ids(crew_ids)

    async def get_by_group(
        self, id: UUID, group_context: Optional[GroupContext]
    ) -> Optional[Crew]:
        """
        Get a crew by ID, ensuring it belongs to the CURRENT workspace (primary group).

        Args:
            id: ID of the crew to get
            group_context: Group context from headers

        Returns:
            Crew if found and belongs to current workspace, else None
        """
        primary_group_id = getattr(group_context, "primary_group_id", None)
        if not primary_group_id:
            return None

        return await self.repository.get_by_group(id, [primary_group_id])

    async def update_with_partial_data_by_group(
        self, id: UUID, obj_in: CrewUpdate, group_context: GroupContext
    ) -> Optional[Crew]:
        """
        Update a crew with partial data, ensuring it belongs to the CURRENT workspace (primary group).

        Args:
            id: ID of the crew to update
            obj_in: Schema with fields to update
            group_context: Group context from headers

        Returns:
            Updated crew if found and belongs to current workspace, else None
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
            return existing_crew

        # Check for duplicate name within the group (if name is being changed)
        if "name" in update_data and update_data["name"] != existing_crew.name:
            duplicate = await self.repository.find_by_name_and_group(
                update_data["name"], [primary_group_id], exclude_id=id
            )
            if duplicate:
                raise ConflictError(
                    detail=f"A crew with the name '{update_data['name']}' already exists. Please choose a different name."
                )

        return await self.repository.update(id, update_data)

    async def delete_by_group(
        self, id: UUID, group_context: Optional[GroupContext]
    ) -> bool:
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

        deleted = await self.repository.delete_by_group(id, [primary_group_id])
        if deleted:
            await self._withdraw_publications(
                entity_ids=[id], group_ids=[primary_group_id]
            )
        return deleted

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
        await self._withdraw_publications(group_ids=[primary_group_id])
