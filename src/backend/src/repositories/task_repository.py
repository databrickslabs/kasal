from typing import List, Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.task import Task

# This repository RECEIVES its session and never opens one. Naming any session
# factory or engine here is a hard failure — NEVER_ACQUIRE_PREFIXES in
# tests/unit/architecture/test_sessions_go_through_the_router.py covers
# repositories/ with no allowlist. See src/repositories/CLAUDE.md.

#: Root columns that mirror a key of the same name in ``config``.
_CONFIG_MIRRORED_FIELDS = (
    "output_pydantic",
    "output_json",
    "output_file",
    "callback",
    "guardrail",
)


class TaskRepository(BaseRepository[Task, str]):
    """
    Repository for Task model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.

        Args:
            session: SQLAlchemy async session
        """
        super().__init__(Task, session)

    async def get(self, id: str) -> Optional[Task]:
        """
        Get a single task by ID.

        Args:
            id: ID of the task to get

        Returns:
            The task if found, else None
        """
        try:
            query = select(self.model).where(self.model.id == id)
            result = await self.session.execute(query)
            return result.scalars().first()
        except Exception:
            await self.session.rollback()
            raise

    async def create(self, obj_in: dict) -> Task:
        """
        Create a new task.

        Args:
            obj_in: Dictionary of values to create model with

        Returns:
            The created task
        """
        try:
            # Do not convert None to empty string for agent_id
            # as it would violate PostgreSQL foreign key constraints

            # Ensure synchronization between config and dedicated fields
            if "config" in obj_in and obj_in["config"] is not None:
                # If output_pydantic is in config, sync to root
                if (
                    "output_pydantic" in obj_in["config"]
                    and obj_in["config"]["output_pydantic"]
                ):
                    obj_in["output_pydantic"] = obj_in["config"]["output_pydantic"]

                # If output_json is in config, sync to root
                if (
                    "output_json" in obj_in["config"]
                    and obj_in["config"]["output_json"]
                ):
                    obj_in["output_json"] = obj_in["config"]["output_json"]

                # If output_file is in config, sync to root
                if (
                    "output_file" in obj_in["config"]
                    and obj_in["config"]["output_file"]
                ):
                    obj_in["output_file"] = obj_in["config"]["output_file"]

                # If callback is in config, sync to root
                if "callback" in obj_in["config"] and obj_in["config"]["callback"]:
                    obj_in["callback"] = obj_in["config"]["callback"]

                # If guardrail is in config, sync to root
                if "guardrail" in obj_in["config"] and obj_in["config"]["guardrail"]:
                    obj_in["guardrail"] = obj_in["config"]["guardrail"]

                # If llm_guardrail is in config, sync to root (including null to clear it)
                if "llm_guardrail" in obj_in["config"]:
                    obj_in["llm_guardrail"] = obj_in["config"]["llm_guardrail"]

            # Vice versa: if fields are at root level, ensure they're in config too
            if "config" not in obj_in:
                obj_in["config"] = {}

            if "output_pydantic" in obj_in and obj_in["output_pydantic"]:
                if "config" not in obj_in:
                    obj_in["config"] = {}
                obj_in["config"]["output_pydantic"] = obj_in["output_pydantic"]

            if "output_json" in obj_in and obj_in["output_json"]:
                if "config" not in obj_in:
                    obj_in["config"] = {}
                obj_in["config"]["output_json"] = obj_in["output_json"]

            if "output_file" in obj_in and obj_in["output_file"]:
                if "config" not in obj_in:
                    obj_in["config"] = {}
                obj_in["config"]["output_file"] = obj_in["output_file"]

            if "callback" in obj_in and obj_in["callback"]:
                if "config" not in obj_in:
                    obj_in["config"] = {}
                obj_in["config"]["callback"] = obj_in["callback"]

            if "guardrail" in obj_in and obj_in["guardrail"]:
                if "config" not in obj_in:
                    obj_in["config"] = {}
                obj_in["config"]["guardrail"] = obj_in["guardrail"]

            # Sync llm_guardrail from root to config (including null to clear it)
            if "llm_guardrail" in obj_in:
                if "config" not in obj_in:
                    obj_in["config"] = {}
                obj_in["config"]["llm_guardrail"] = obj_in["llm_guardrail"]

            if "markdown" in obj_in and obj_in["markdown"] is not None:
                if "config" not in obj_in:
                    obj_in["config"] = {}
                obj_in["config"]["markdown"] = obj_in["markdown"]

            # Also sync markdown from config to root if present
            if "config" in obj_in and obj_in["config"] is not None:
                if (
                    "markdown" in obj_in["config"]
                    and obj_in["config"]["markdown"] is not None
                ):
                    obj_in["markdown"] = obj_in["config"]["markdown"]

            db_obj = self.model(**obj_in)
            self.session.add(db_obj)
            await self.session.flush()
            return db_obj
        except Exception:
            await self.session.rollback()
            raise

    async def update(self, id: str, obj_in: dict) -> Optional[Task]:
        """
        Update an existing task, leaving what ``obj_in`` does not name alone.

        - No ``config`` in ``obj_in``: the row's config is kept, and root fields
          that are sent (output_pydantic, llm_guardrail, markdown, ...) are
          mirrored into a COPY of it.
        - A ``config`` in ``obj_in`` REPLACES the stored one. Deliberately not a
          key-by-key merge: the task form clears a field by leaving it out.
          The root columns that mirror it follow, so a cleared field is
          cleared there too.
        - ``agent_id: ""`` means "not provided" and is dropped, never written
          as NULL: an unassigned row runs on the crew's first agent wherever
          the crew is rebuilt from its rows.

        Args:
            id: ID of the task to update
            obj_in: Dictionary of values to update model with

        Returns:
            The updated task if found, else None
        """
        try:
            # Do not convert None to empty string for agent_id
            # as it would violate PostgreSQL foreign key constraints
            if obj_in.get("agent_id") == "":
                obj_in.pop("agent_id")

            config_sent = obj_in.get("config") is not None

            # Ensure synchronization between config and dedicated fields
            if config_sent:
                # A sent config replaces the stored one, so the root columns
                # that mirror it follow it — None included. Rebuilt crews
                # (MCP/A2A, triggers, resume) project these columns, and a
                # stale one would bring a cleared schema or guardrail back.
                for field in _CONFIG_MIRRORED_FIELDS:
                    obj_in[field] = obj_in["config"].get(field) or None

                # If llm_guardrail is in config, sync to root (including null to clear it)
                if "llm_guardrail" in obj_in["config"]:
                    obj_in["llm_guardrail"] = obj_in["config"]["llm_guardrail"]

            db_obj = await self.get(id)
            if db_obj is None:
                return None

            # Absent = untouched. A copy, not the stored dict: a JSON column
            # mutated in place is a change SQLAlchemy never writes.
            if not config_sent:
                obj_in["config"] = dict(db_obj.config or {})

            # Vice versa: if fields are at root level, ensure they're in config too
            if "output_pydantic" in obj_in and obj_in["output_pydantic"]:
                obj_in["config"]["output_pydantic"] = obj_in["output_pydantic"]

            if "output_json" in obj_in and obj_in["output_json"]:
                obj_in["config"]["output_json"] = obj_in["output_json"]

            if "output_file" in obj_in and obj_in["output_file"]:
                obj_in["config"]["output_file"] = obj_in["output_file"]

            if "callback" in obj_in and obj_in["callback"]:
                obj_in["config"]["callback"] = obj_in["callback"]

            if "guardrail" in obj_in and obj_in["guardrail"]:
                obj_in["config"]["guardrail"] = obj_in["guardrail"]

            # Sync llm_guardrail from root to config (including null to clear it)
            if "llm_guardrail" in obj_in:
                obj_in["config"]["llm_guardrail"] = obj_in["llm_guardrail"]

            if "markdown" in obj_in and obj_in["markdown"] is not None:
                obj_in["config"]["markdown"] = obj_in["markdown"]

            # Also sync markdown from a SENT config to root. A kept config is
            # not a request to rewrite the root column.
            if config_sent:
                if (
                    "markdown" in obj_in["config"]
                    and obj_in["config"]["markdown"] is not None
                ):
                    obj_in["markdown"] = obj_in["config"]["markdown"]

            if db_obj:
                # Debug logging for tool_configs
                if "tool_configs" in obj_in:
                    import logging

                    logger = logging.getLogger(__name__)
                    logger.info(
                        f"Updating task {id} with tool_configs: {sorted(obj_in.get('tool_configs') or {})}"
                    )

                for key, value in obj_in.items():
                    setattr(db_obj, key, value)
                await self.session.flush()
            return db_obj
        except Exception:
            await self.session.rollback()
            raise

    async def delete(self, id: str) -> bool:
        """
        Delete a task by ID.

        Args:
            id: ID of the task to delete

        Returns:
            True if task was deleted, False if not found
        """
        try:
            db_obj = await self.get(id)
            if db_obj:
                await self.session.delete(db_obj)
                await self.session.flush()
                return True
            return False
        except Exception:
            await self.session.rollback()
            raise

    async def find_by_name(self, name: str) -> Optional[Task]:
        """
        Find a task by name.

        Args:
            name: Name to search for

        Returns:
            Task if found, else None
        """
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def find_by_agent_id(self, agent_id: str) -> List[Task]:
        """
        Find all tasks for a specific agent.

        Args:
            agent_id: ID of the agent

        Returns:
            List of tasks assigned to the agent
        """
        query = select(self.model).where(self.model.agent_id == agent_id)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_all(self) -> List[Task]:
        """
        Find all tasks.

        Returns:
            List of all tasks
        """
        query = select(self.model)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def delete_all(self) -> None:
        """
        Delete all tasks.

        Returns:
            None
        """
        stmt = delete(self.model)
        await self.session.execute(stmt)
        await self.session.flush()

    async def find_by_group_ids(self, group_ids: List[str]) -> List[Task]:
        """Tasks visible to any of these groups.

        Group scoping is a repository concern: a caller that builds this query
        itself is one `.where()` away from leaking another tenant's rows.
        """
        if not group_ids:
            return []
        stmt = select(self.model).where(self.model.group_id.in_(group_ids))
        result = await self.session.execute(stmt)
        return list(result.scalars().all())


# SyncTaskRepository and get_sync_task_repository removed
# All database operations must be async - use TaskRepository instead
