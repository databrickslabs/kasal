import logging
import uuid
from typing import List, Optional, Union
from uuid import UUID

from sqlalchemy import bindparam, desc, func, select, text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.flow import Flow

logger = logging.getLogger(__name__)


class FlowRepository(BaseRepository[Flow]):
    """
    Repository for Flow model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.

        Args:
            session: SQLAlchemy async session
        """
        super().__init__(Flow, session)

    async def find_by_name(self, name: str) -> Optional[Flow]:
        """
        Find a flow by name.

        Args:
            name: Name to search for

        Returns:
            Flow if found, else None
        """
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def find_by_name_and_group(
        self, name: str, group_ids: List[str], exclude_id: Optional[UUID] = None
    ) -> Optional[Flow]:
        """
        Find a flow by name within the given groups.

        Args:
            name: Name to search for
            group_ids: List of group IDs to filter by
            exclude_id: Optional flow ID to exclude (for updates)

        Returns:
            Flow if found, else None
        """
        if not group_ids:
            return None

        conditions = [self.model.name == name, self.model.group_id.in_(group_ids)]
        if exclude_id is not None:
            conditions.append(self.model.id != exclude_id)

        query = select(self.model).where(*conditions)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def find_by_crew_id(self, crew_id: Union[uuid.UUID, str]) -> List[Flow]:
        """
        Find all flows for a specific crew.

        Args:
            crew_id: ID of the crew (UUID)

        Returns:
            List of flows associated with the crew
        """
        # Convert string to UUID if needed
        if isinstance(crew_id, str):
            try:
                crew_id = uuid.UUID(crew_id)
            except ValueError:
                return []

        query = select(self.model).where(self.model.crew_id == crew_id)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_by_ids(self, flow_ids: List[Union[uuid.UUID, str]]) -> List[Flow]:
        """Flows for a set of ids, in one query.

        Exists so a caller holding several ids does not issue one read per id.
        Ids that are not valid UUIDs are skipped rather than raising: the caller
        is typically holding ids that came from another table, and one bad value
        should narrow the result, not fail the request.
        """
        parsed: List[uuid.UUID] = []
        for flow_id in flow_ids:
            if isinstance(flow_id, uuid.UUID):
                parsed.append(flow_id)
                continue
            try:
                parsed.append(uuid.UUID(str(flow_id)))
            except (ValueError, TypeError):
                continue
        if not parsed:
            return []

        query = select(self.model).where(self.model.id.in_(parsed))
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def find_all(self) -> List[Flow]:
        """
        Find all flows.

        Returns:
            List of all flows
        """
        query = select(self.model)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_most_recent(self) -> Optional[Flow]:
        """The newest flow by created_at, or None.

        A fallback for starting a flow execution with no flow_id and no
        nodes/edges supplied — it picks whatever was authored last.
        """
        result = await self.session.execute(
            select(Flow).order_by(desc(Flow.created_at)).limit(1)
        )
        return result.scalars().first()

    async def list_for_groups(self, group_ids: List[str]) -> List[Flow]:
        """Flows visible to these groups, newest edit first."""
        if not group_ids:
            return []
        result = await self.session.execute(
            select(Flow)
            .where(Flow.group_id.in_(group_ids))
            .order_by(Flow.updated_at.desc())
        )
        return list(result.scalars().all())

    async def exists(self, flow_id: uuid.UUID) -> bool:
        """Whether a flow row is present, without loading it."""
        result = await self.session.execute(
            select(Flow.id).where(Flow.id == self._uuid_param(flow_id))
        )
        return result.first() is not None

    async def get_group_id(self, flow_id: uuid.UUID) -> tuple[bool, Optional[str]]:
        """``(exists, group_id)`` for one flow — the authorization pre-check."""
        result = await self.session.execute(
            select(Flow.id, Flow.group_id).where(Flow.id == self._uuid_param(flow_id))
        )
        row = result.first()
        return (row is not None, row[1] if row else None)

    async def count_executions(self, flow_id: uuid.UUID) -> int:
        """How many runs reference this flow (a non-force delete refuses if any)."""
        from src.models.execution_history import ExecutionHistory

        result = await self.session.execute(
            select(func.count())
            .select_from(ExecutionHistory)
            .where(ExecutionHistory.flow_id == flow_id)
        )
        return int(result.scalar_one())

    async def find_execution_keys(
        self, flow_id: uuid.UUID
    ) -> tuple[List[int], List[str]]:
        """``(execution_ids, job_ids)`` for this flow's runs.

        Both keys, because the FK children are split between them: some reference
        ``executionhistory.id`` and some ``executionhistory.job_id``.
        """
        result = await self.session.execute(
            text(
                "SELECT id, job_id FROM executionhistory "
                "WHERE flow_id = :flow_id AND execution_type = 'flow'"
            ).bindparams(self._flow_id_bindparam()),
            {"flow_id": self._uuid_param(flow_id)},
        )
        rows = result.fetchall()
        return [r[0] for r in rows], [r[1] for r in rows]

    async def delete_execution_children(
        self, execution_ids: List[int], job_ids: List[str]
    ) -> None:
        """Delete every row FK-referencing these runs.

        Must precede the ``executionhistory`` delete or SQLite raises "FOREIGN KEY
        constraint failed". Covers all enforced children:
          - execution_trace:   run_id (-> id) AND job_id (-> job_id)
          - errortrace:        run_id (-> id)
          - taskstatus:        job_id (-> job_id)
          - llm_usage_billing: execution_id (-> job_id)

        Table/column names are hardcoded constants — no injection surface.
        """
        if execution_ids:
            for table, col in (("execution_trace", "run_id"), ("errortrace", "run_id")):
                result = await self.session.execute(
                    text(f"DELETE FROM {table} WHERE {col} IN :ids").bindparams(
                        bindparam("ids", expanding=True)
                    ),
                    {"ids": execution_ids},
                )
                logger.info(f"Deleted {result.rowcount} {table} records (by {col})")

        if job_ids:
            for table, col in (
                ("execution_trace", "job_id"),
                ("taskstatus", "job_id"),
                ("llm_usage_billing", "execution_id"),
            ):
                result = await self.session.execute(
                    text(f"DELETE FROM {table} WHERE {col} IN :jids").bindparams(
                        bindparam("jids", expanding=True)
                    ),
                    {"jids": job_ids},
                )
                logger.info(f"Deleted {result.rowcount} {table} records (by {col})")

    async def delete_executions_of(self, flow_id: uuid.UUID) -> int:
        """Delete this flow's runs, returning how many."""
        result = await self.session.execute(
            text(
                "DELETE FROM executionhistory "
                "WHERE flow_id = :flow_id AND execution_type = 'flow'"
            ).bindparams(self._flow_id_bindparam()),
            {"flow_id": self._uuid_param(flow_id)},
        )
        return result.rowcount or 0

    async def delete_row(self, flow_id: uuid.UUID) -> int:
        """Delete the flow row itself, returning how many rows went."""
        result = await self.session.execute(
            text("DELETE FROM flows WHERE id = :flow_id").bindparams(
                self._flow_id_bindparam()
            ),
            {"flow_id": self._uuid_param(flow_id)},
        )
        return result.rowcount or 0

    @staticmethod
    def _uuid_param(flow_id) -> uuid.UUID:
        """Coerce to a real ``UUID``.

        Required, not cosmetic: a raw ``str``/``UUID`` passed into ``text()`` fails
        on SQLite (cannot bind UUID) and ``str()`` yields the dashed form, which
        does not match SQLite's stored dashless hex.
        """
        return flow_id if isinstance(flow_id, uuid.UUID) else uuid.UUID(str(flow_id))

    @staticmethod
    def _flow_id_bindparam():
        """Bind ``flow_id`` with the column's UUID type.

        Lets SQLAlchemy apply the per-dialect conversion — native UUID on
        Postgres, dashless hex on SQLite.
        """
        return bindparam("flow_id", type_=PGUUID(as_uuid=True))

    async def delete_with_executions(self, flow_id: uuid.UUID) -> bool:
        """
        Delete a flow and all its related execution records to handle foreign key constraints.

        Args:
            flow_id: UUID of the flow to delete

        Returns:
            True if flow was deleted, False if not found
        """
        import logging

        logger = logging.getLogger(__name__)

        # Check if the flow exists
        flow = await self.get(flow_id)
        if not flow:
            logger.warning(f"Flow with ID {flow_id} not found for deletion")
            return False

        try:
            # Delete all flow executions from executionhistory table
            exec_delete_query = text("""
            DELETE FROM executionhistory
            WHERE flow_id = :flow_id AND execution_type = 'flow'
            """)
            result = await self.session.execute(exec_delete_query, {"flow_id": flow_id})
            deleted_count = result.rowcount
            if deleted_count > 0:
                logger.info(
                    f"Deleted {deleted_count} flow executions for flow {flow_id}"
                )

            # Now delete the flow
            flow_delete_query = text("""
            DELETE FROM flows WHERE id = :flow_id
            """)
            result = await self.session.execute(flow_delete_query, {"flow_id": flow_id})

            # Flush all changes
            await self.session.flush()

            logger.info(f"Successfully deleted flow {flow_id} and all its executions")
            return True

        except Exception as e:
            # Roll back on error
            await self.session.rollback()
            logger.error(f"Error during cascading deletion of flow {flow_id}: {str(e)}")
            raise

    async def delete_all(self) -> None:
        """
        Delete all flows, handling foreign key constraints by deleting related records first.

        Returns:
            None
        """
        import logging

        logger = logging.getLogger(__name__)

        try:
            # Delete all flow executions from executionhistory table
            exec_delete_query = text("""
            DELETE FROM executionhistory WHERE execution_type = 'flow'
            """)
            await self.session.execute(exec_delete_query)
            logger.info("Deleted all flow executions from executionhistory")

            # Delete all flows
            flow_delete_query = text("""
            DELETE FROM flows
            """)
            await self.session.execute(flow_delete_query)
            logger.info("Deleted all flows")

            # Flush the changes
            await self.session.flush()

        except Exception as e:
            # Roll back on error
            await self.session.rollback()
            logger.error(f"Error during delete_all operation: {str(e)}")
            raise


# SyncFlowRepository removed - use async FlowRepository instead
# All database operations must be async
