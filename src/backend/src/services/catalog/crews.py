import logging
from collections.abc import Sequence
from typing import Dict, List, Optional, Union
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

    async def _hydrate_task_nodes_from_tasks(
        self, crew: Optional[Crew]
    ) -> Optional[Crew]:
        """Overlay each task node's ``tool_configs`` from its LIVE task row (in memory).

        The Task editor writes tool_configs to the TASKS table, but the crew's
        embedded canvas snapshot (``crew.nodes``) is not resynced — so a reloaded
        canvas or a flow drill-through would render a stale ``warehouse_id`` /
        ``dataset_id``, even though the task row (what flow execution re-reads) is
        correct. The task row is the source of truth, so make the displayed node
        match it.

        Only task rows of the CREW'S OWN workspace are read: a crew imported from
        another workspace still names that workspace's task ids, and overlaying
        those rows would copy another tenant's tool_configs (credentials
        included) into this crew's canvas — and from there into a flow build.

        Reads the owning ``TaskService`` (never TaskRepository). In-memory only —
        a display convenience that is never persisted. Best-effort: a lookup
        failure must never fail a crew read.
        """
        if crew is None:
            return crew
        crew_group = getattr(crew, "__dict__", {}).get("group_id")
        if not crew_group:
            return crew  # no workspace to scope the task reads to
        try:
            from src.services.catalog.tasks import TaskService

            task_svc = TaskService(self.session)
            live: Dict[str, object] = {}
            for tid in getattr(crew, "task_ids", None) or []:
                task = await task_svc.get(str(tid))
                if task is None or getattr(task, "group_id", None) != crew_group:
                    continue
                tcfg = getattr(task, "tool_configs", None)
                if tcfg:
                    live[str(tid)] = tcfg
            self._overlay_task_nodes(crew, live)
        except Exception as e:  # noqa: BLE001 — display convenience, never fatal
            logger.warning("CrewService: task-node tool_configs hydrate skipped: %s", e)
        return crew

    @staticmethod
    def _overlay_task_nodes(crew: Crew, cfg_by_task_id: Dict[str, object]) -> None:
        """Overlay each task node's ``tool_configs`` from ``{task_id: tool_configs}``.

        Pure/in-memory. Shared by the single-crew and the catalog-list hydration
        so both resolve the node's task id the same way (``data.taskId`` else the
        ``task-<id>`` node id) and only overlay when a live config exists.
        """
        nodes = getattr(crew, "__dict__", {}).get("nodes")
        if not isinstance(nodes, list) or not nodes or not cfg_by_task_id:
            return
        for node in nodes:
            if not isinstance(node, dict) or node.get("type") != "taskNode":
                continue
            data = node.get("data")
            if not isinstance(data, dict):
                continue
            nid = node.get("id") or ""
            tid = str(
                data.get("taskId") or (nid.split("-", 1)[1] if "-" in nid else nid)
            )
            if tid in cfg_by_task_id:
                data["tool_configs"] = cfg_by_task_id[tid]

    async def _hydrate_crew_list_from_tasks(
        self, crews: List[Crew], group_context: GroupContext
    ) -> List[Crew]:
        """Hydrate a whole catalog list in ONE task query (not N per crew).

        The catalog list feeds "open crew on canvas" paths, so its nodes must carry
        the live tool_configs too — otherwise opening a crew straight from the
        catalog shows a stale ``dataset_id`` while the single-crew read is correct.
        Best-effort: never fail the list over it.
        """
        if not crews:
            return crews
        try:
            from src.services.catalog.tasks import TaskService

            tasks = await TaskService(self.session).find_by_group(group_context)
            cfg_by_id = {
                str(t.id): t.tool_configs
                for t in tasks
                if getattr(t, "tool_configs", None)
            }
            if cfg_by_id:
                for crew in crews:
                    self._overlay_task_nodes(crew, cfg_by_id)
        except Exception as e:  # noqa: BLE001 — display convenience, never fatal
            logger.warning("CrewService: catalog tool_configs hydrate skipped: %s", e)
        return crews

    async def _sync_task_rows_from_nodes(
        self, crew: Optional[Crew], group_context: GroupContext
    ) -> None:
        """Write each task node's ``tool_configs`` through to its TASK ROW on save.

        The task row is the source of truth every read hydrates from (and what flow
        execution re-reads). A crew save persists ``crew.nodes`` but not the task
        rows, so without this, editing a tool's config on the canvas and saving the
        CREW would leave the task row — and thus every drill-through / reload —
        stale. This makes it not matter where you save: the edit always reaches the
        source of truth. Routes through the owning ``TaskService`` (group-checked).
        Best-effort per task; a failure never fails the crew save.
        """
        if crew is None:
            return
        nodes = getattr(crew, "__dict__", {}).get("nodes")
        if not isinstance(nodes, list) or not nodes:
            return
        try:
            from src.schemas.task import TaskUpdate
            from src.services.catalog.tasks import TaskService

            task_svc = TaskService(self.session)
            valid_ids = {str(t) for t in (getattr(crew, "task_ids", None) or [])}
            for node in nodes:
                if not isinstance(node, dict) or node.get("type") != "taskNode":
                    continue
                data = node.get("data")
                if not isinstance(data, dict):
                    continue
                cfg = data.get("tool_configs")
                if not isinstance(cfg, dict) or not cfg:
                    continue
                nid = node.get("id") or ""
                tid = str(
                    data.get("taskId") or (nid.split("-", 1)[1] if "-" in nid else nid)
                )
                if valid_ids and tid not in valid_ids:
                    continue
                await task_svc.update_with_group_check(
                    tid, TaskUpdate(tool_configs=cfg), group_context
                )
        except Exception as e:  # noqa: BLE001 — never fail the crew save over it
            logger.warning("CrewService: task-row tool_configs sync skipped: %s", e)

    async def get(self, id: Union[UUID, str]) -> Optional[Crew]:
        """
        Get a crew by ID.

        Args:
            id: ID of the crew to get

        Returns:
            Crew if found, else None
        """
        crew = await self.repository.get(id)
        return await self._hydrate_task_nodes_from_tasks(crew)

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
            logger.info("Creating crew with name: %s", obj_in.name)
            logger.info("Agent IDs: %s", obj_in.agent_ids)
            logger.info("Task IDs: %s", obj_in.task_ids)
            logger.info("Number of nodes: %s", len(obj_in.nodes) if obj_in.nodes else 0)
            logger.info("Number of edges: %s", len(obj_in.edges) if obj_in.edges else 0)

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
            logger.error("Error creating crew: %s", e)
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
                "%s crew with name: %s for group: %s",
                action,
                obj_in.name,
                primary_group_id,
            )
            logger.info("Agent IDs: %s", obj_in.agent_ids)
            logger.info("Task IDs: %s", obj_in.task_ids)
            logger.info("Number of nodes: %s", len(obj_in.nodes))
            logger.info("Number of edges: %s", len(obj_in.edges))

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
                    "Overwriting existing crew '%s' (id=%s)", obj_in.name, existing.id
                )
                crew = await self.repository.update(existing.id, crew_data) or existing
                await self._sync_task_rows_from_nodes(crew, group_context)
                return crew

            # Create the model using the serialized data
            crew_data["created_by_email"] = group_context.group_email
            crew = await self.repository.create(crew_data)
            await self._sync_task_rows_from_nodes(crew, group_context)
            return crew
        except Exception as e:
            logger.error("Error creating crew with group: %s", e)
            raise

    async def clone_with_group(
        self, crew_id: UUID, new_name: Optional[str], group_context: GroupContext
    ) -> Optional[Crew]:
        """Clone a crew into a NEW, independent crew under ``new_name``.

        "Save as new crew" — the opposite of overwriting in place. The agents and
        tasks are DUPLICATED (new rows) and the canvas graph is rebuilt with the
        clone's IDs, so editing the copy never mutates the original. Returns the
        new crew, or ``None`` if the source crew is not in the current workspace.

        Raises ConflictError if ``new_name`` is already taken in the workspace.
        """
        from src.schemas.task import TaskUpdate
        from src.services.catalog.agents import AgentService
        from src.services.catalog.crew_clone import (
            agent_create_from_model,
            remap_context,
            remap_crew_graph,
            task_create_from_model,
        )
        from src.services.catalog.tasks import TaskService

        primary_group_id = getattr(group_context, "primary_group_id", None)
        if not primary_group_id:
            return None

        source = await self.repository.get_by_group(crew_id, [primary_group_id])
        if not source:
            return None
        # Hydrate the source's canvas nodes from the live task rows so the clone's
        # nodes carry current tool_configs, not a stale snapshot.
        await self._hydrate_task_nodes_from_tasks(source)

        new_name = (new_name or f"{source.name} (copy)").strip()
        if await self.repository.find_by_name_and_group(new_name, [primary_group_id]):
            raise ConflictError(
                detail=f"A crew with the name '{new_name}' already exists. Please choose a different name."
            )

        agent_svc = AgentService(self.session)
        task_svc = TaskService(self.session)

        # Duplicate agents first — tasks point at the cloned agent IDs.
        agent_id_map: Dict[str, str] = {}
        for aid in source.agent_ids or []:
            src_agent = await agent_svc.get_with_group_check(str(aid), group_context)
            if src_agent is None:
                continue
            new_agent = await agent_svc.create_with_group(
                agent_create_from_model(src_agent), group_context
            )
            agent_id_map[str(aid)] = str(new_agent.id)

        # Duplicate tasks (context deferred until every clone task has an ID).
        task_id_map: Dict[str, str] = {}
        cloned_tasks: List[tuple] = []
        for tid in source.task_ids or []:
            src_task = await task_svc.get_with_group_check(str(tid), group_context)
            if src_task is None:
                continue
            new_task = await task_svc.create_with_group(
                task_create_from_model(src_task, agent_id_map), group_context
            )
            task_id_map[str(tid)] = str(new_task.id)
            cloned_tasks.append((str(new_task.id), src_task))

        # Second pass: remap each clone task's context to the clone's task IDs.
        for new_tid, src_task in cloned_tasks:
            ctx = getattr(src_task, "context", None) or []
            if ctx:
                await task_svc.update_with_partial_data(
                    new_tid, TaskUpdate(context=remap_context(ctx, task_id_map))
                )

        new_nodes, new_edges = remap_crew_graph(
            source.nodes or [], source.edges or [], agent_id_map, task_id_map
        )

        crew_create = CrewCreate(
            name=new_name,
            agent_ids=list(agent_id_map.values()),
            task_ids=list(task_id_map.values()),
            nodes=new_nodes,
            edges=new_edges,
            process=source.process,
            reasoning=source.reasoning,
            reasoning_llm=source.reasoning_llm,
            reasoning_config=source.reasoning_config,
            manager_llm=source.manager_llm,
            tool_configs=source.tool_configs,
            memory=source.memory,
            verbose=source.verbose,
            max_rpm=source.max_rpm if isinstance(source.max_rpm, int) else None,
        )
        return await self.create_with_group(crew_create, group_context)

    async def find_by_group(self, group_context: Optional[GroupContext]) -> List[Crew]:
        """
        Find all crews for the CURRENT workspace (primary group only).

        Args:
            group_context: Group context from headers

        Returns:
            List of crews for the selected workspace
        """
        primary_group_id = getattr(group_context, "primary_group_id", None)
        if not primary_group_id or group_context is None:
            # If no current workspace, return empty list for security
            return []

        crews = await self.repository.find_by_group([primary_group_id])
        return await self._hydrate_crew_list_from_tasks(crews, group_context)

    async def get_crews_by_ids(
        self, crew_ids: Sequence[Union[UUID, str]]
    ) -> List[Crew]:
        """Crews for a set of ids.

        Crews are this service's domain; `publications` and the prompt optimiser
        used to build ``CrewRepository`` themselves to resolve references.
        """
        return await self.repository.find_by_ids(list(crew_ids))

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

        crew = await self.repository.get_by_group(id, [primary_group_id])
        return await self._hydrate_task_nodes_from_tasks(crew)

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

        crew = await self.repository.update(id, update_data)
        # Only sync task rows when this save actually carried nodes (a name-only
        # partial update has nothing to propagate).
        if "nodes" in update_data:
            await self._sync_task_rows_from_nodes(crew, group_context)
        return crew

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
