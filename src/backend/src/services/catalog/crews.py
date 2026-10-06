import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import ConflictError
from src.models.crew import Crew
from src.repositories.crew_repository import CrewRepository
from src.schemas.crew import CrewCreate, CrewUpdate
from src.utils.sensitive_data_utils import (
    decrypt_sensitive_fields,
    encrypt_sensitive_fields,
    safe_log_tool_configs,
)
from src.utils.user_context import GroupContext

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

        SYNC on purpose — it is called from 13 places, several of them right after a
        write. That makes plain attribute access unsafe: if the ORM considers the
        instance EXPIRED, ``crew.tool_configs`` is not a dict read but a lazy
        refresh, i.e. database IO from a sync frame. On the deployed app that
        raised, and every crew save returned 500:

            POST /api/v1/crews  500
            greenlet_spawn has not been called; can't call await_only() here
              crews.py:50  if crew and crew.tool_configs:
              sqlalchemy/orm/attributes.py  state._load_expired(...)

        ``expire_on_commit=False`` is set on every sessionmaker here, so expiry is
        not coming from a commit — which is exactly why this reads the attribute
        DEFENSIVELY rather than chasing whichever operation expired it. Asking the
        instance's own state first means the outcome no longer depends on knowing
        that.

        A loaded value is decrypted as before. An unloaded one is left alone: the
        caller gets the crew with its stored (encrypted) value rather than a 500,
        and decryption is a display convenience, not a correctness requirement.

        Args:
            crew: Crew with potentially encrypted tool_configs

        Returns:
            Crew with decrypted tool_configs (in-memory only)
        """
        if crew is None:
            return crew

        configs = self._loaded_tool_configs(crew)
        if configs:
            # Not crew.id — on an expired instance that read is itself a lazy load,
            # so logging the failure would raise from the except block.
            crew_id = getattr(crew, "__dict__", {}).get("id", "<unloaded>")
            try:
                crew.tool_configs = decrypt_sensitive_fields(configs)
            except Exception as e:
                logger.error(f"Failed to decrypt tool_configs for crew {crew_id}: {e}")
        return crew

    @staticmethod
    def _loaded_tool_configs(crew: Crew) -> Optional[Any]:
        """``crew.tool_configs`` only if it is already in memory, else None.

        ``inspect(crew).dict`` is the instance's loaded attribute dict: a key is
        present only when the value is in memory. Absent means touching the
        attribute would emit a SELECT — the lazy refresh that raised MissingGreenlet
        from this sync frame. Checking membership turns that into a cheap miss.

        ``state.expired`` is deliberately NOT the test: an expired instance can
        still hold some attributes, and only the ones actually missing from
        ``dict`` would do IO.
        """
        try:
            from sqlalchemy import inspect as sa_inspect

            state = sa_inspect(crew)
            if "tool_configs" not in state.dict:
                logger.debug(
                    "tool_configs not loaded for crew %s; skipping decrypt rather "
                    "than triggering a lazy refresh",
                    state.dict.get("id", "<unknown>"),
                )
                return None
        except Exception:  # noqa: BLE001 — a non-ORM object (or a test double)
            pass
        return crew.tool_configs

    def _encrypt_tool_configs_in_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encrypt sensitive fields in tool_configs before storage.

        Args:
            data: Dictionary containing crew data with tool_configs

        Returns:
            Dictionary with encrypted tool_configs
        """
        if "tool_configs" in data and data["tool_configs"]:
            try:
                data["tool_configs"] = encrypt_sensitive_fields(data["tool_configs"])
                logger.debug(safe_log_tool_configs(data["tool_configs"], "Encrypted "))
            except Exception as e:
                logger.error(f"Failed to encrypt tool_configs: {e}")
                raise
        return data

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

        Reads the owning ``TaskService`` (never TaskRepository). In-memory only —
        like decryption, this is a display convenience and is never persisted.
        Best-effort: a lookup failure must never fail a crew read.
        """
        if crew is None:
            return crew
        try:
            from src.services.catalog.tasks import TaskService

            task_svc = TaskService(self.session)
            live: Dict[str, Any] = {}
            for tid in getattr(crew, "task_ids", None) or []:
                task = await task_svc.get(str(tid))
                tcfg = getattr(task, "tool_configs", None) if task else None
                if tcfg:
                    live[str(tid)] = tcfg
            self._overlay_task_nodes(crew, live)
        except Exception as e:  # noqa: BLE001 — display convenience, never fatal
            logger.warning(f"CrewService: task-node tool_configs hydrate skipped: {e}")
        return crew

    @staticmethod
    def _overlay_task_nodes(crew: Crew, cfg_by_task_id: Dict[str, Any]) -> None:
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
            logger.warning(f"CrewService: catalog tool_configs hydrate skipped: {e}")
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
            logger.warning(f"CrewService: task-row tool_configs sync skipped: {e}")

    async def get(self, id: UUID) -> Optional[Crew]:
        """
        Get a crew by ID with decrypted tool_configs.

        Args:
            id: ID of the crew to get

        Returns:
            Crew if found, else None (with decrypted tool_configs)
        """
        crew = await self.repository.get(id)
        self._decrypt_crew_tool_configs(crew)
        return await self._hydrate_task_nodes_from_tasks(crew)

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

    async def update_with_partial_data(
        self, id: UUID, obj_in: CrewUpdate
    ) -> Optional[Crew]:
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
        if "tool_configs" in update_data:
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
        Encrypts sensitive fields in tool_configs before storage.

        Args:
            obj_in: Crew data for creation
            group_context: Group context from headers
            overwrite: When True and a crew with the same name already exists in the
                group, replace that crew's definition instead of raising ConflictError.

        Returns:
            Created (or overwritten) crew (with decrypted tool_configs for response)

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

            # Encrypt sensitive fields in tool_configs before storage
            crew_data = self._encrypt_tool_configs_in_data(crew_data)

            if existing:
                # Overwrite: replace the existing crew's definition, preserving its
                # id and original creator (don't overwrite created_by_email).
                logger.info(
                    f"Overwriting existing crew '{obj_in.name}' (id={existing.id})"
                )
                crew = await self.repository.update(existing.id, crew_data) or existing
                self._decrypt_crew_tool_configs(crew)
                await self._sync_task_rows_from_nodes(crew, group_context)
                return crew

            # Create the model using the serialized data
            crew_data["created_by_email"] = group_context.group_email
            crew = await self.repository.create(crew_data)
            self._decrypt_crew_tool_configs(crew)
            await self._sync_task_rows_from_nodes(crew, group_context)
            return crew
        except Exception as e:
            logger.error(f"Error creating crew with group: {str(e)}")
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
        self._decrypt_crew_tool_configs(source)
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
            src_agent = await agent_svc.get(str(aid))
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
            src_task = await task_svc.get(str(tid))
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
                await task_svc.update(
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
        return await self._hydrate_crew_list_from_tasks(crews, group_context)

    async def get_crews_by_ids(self, crew_ids: List[Any]) -> List[Crew]:
        """Crews for a set of ids.

        Crews are this service's domain; `publications` and the prompt optimiser
        used to build ``CrewRepository`` themselves to resolve references.
        """
        return await self.repository.find_by_ids(crew_ids)

    async def get_by_group(
        self, id: UUID, group_context: GroupContext
    ) -> Optional[Crew]:
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
        self._decrypt_crew_tool_configs(crew)
        return await self._hydrate_task_nodes_from_tasks(crew)

    async def update_with_partial_data_by_group(
        self, id: UUID, obj_in: CrewUpdate, group_context: GroupContext
    ) -> Optional[Crew]:
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
        if "name" in update_data and update_data["name"] != existing_crew.name:
            duplicate = await self.repository.find_by_name_and_group(
                update_data["name"], [primary_group_id], exclude_id=id
            )
            if duplicate:
                raise ConflictError(
                    detail=f"A crew with the name '{update_data['name']}' already exists. Please choose a different name."
                )

        # Encrypt sensitive fields in tool_configs before storage
        if "tool_configs" in update_data:
            logger.debug(f"CrewService: encrypting tool_configs for crew {id}")
            update_data = self._encrypt_tool_configs_in_data(update_data)

        crew = await self.repository.update(id, update_data)
        self._decrypt_crew_tool_configs(crew)
        # Only sync task rows when this save actually carried nodes (a name-only
        # partial update has nothing to propagate).
        if "nodes" in update_data:
            await self._sync_task_rows_from_nodes(crew, group_context)
        return crew

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
