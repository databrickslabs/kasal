"""
Repository for execution history data access.

This module provides database operations for execution history models.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from uuid import UUID

from sqlalchemy import delete, desc, distinct, func, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from src.models.billing import LLMUsageBilling
from src.models.execution_history import ErrorTrace, ExecutionHistory, TaskStatus
from src.models.execution_status import ExecutionStatus
from src.models.execution_trace import ExecutionTrace
from src.models.flow_state import FlowState

# Removed async_session_factory import - using injected session only

logger = logging.getLogger(__name__)

# Distinguishes "leave this column alone" from "set it to NULL" on partial
# updates, where None is itself a meaningful value rather than an absent
# argument.
_UNSET = object()


class ExecutionHistoryRepository:
    """Repository for execution history data access operations."""

    def __init__(self, session: AsyncSession):
        """Initialize with required session."""
        self.session = session

    async def reload(self, run: ExecutionHistory) -> ExecutionHistory:
        """Re-read a run after commit so server-side defaults are populated."""
        await self.session.refresh(run)
        return run

    async def save(self) -> None:
        """Flush pending attribute changes on already-tracked runs."""
        await self.session.flush()

    async def latest_result_with_keys(self, keys: List[str]) -> Optional[dict]:
        """The most recent run whose ``result`` dict holds ALL of ``keys``.

        Serves the UCMV validator's fallback lookup. Same dialect reasoning as
        :meth:`latest_checkpoint_containing`: the caller's raw ``result::text LIKE``
        chain was Postgres-only and matched substrings anywhere in the JSON — a
        value that merely CONTAINED the word counted as the key being present.
        Filtering the decoded dict checks real keys and works on every dialect.
        """
        result = await self.session.execute(
            select(ExecutionHistory.result)
            .where(ExecutionHistory.result.isnot(None))
            .order_by(ExecutionHistory.created_at.desc())
            .limit(50)
        )
        for (data,) in result.all():
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except ValueError:
                    continue
            if isinstance(data, dict) and all(k in data for k in keys):
                return data
        return None

    async def latest_checkpoint_containing(self, key: str) -> Optional[dict]:
        """The most recent run whose ``checkpoint_data`` holds ``key``.

        Serves the UCMV validator, which looks for edits a user saved in an
        earlier step of a multi-step flow.

        The caller's version cast ``checkpoint_data::text`` in raw SQL — a
        Postgres-only cast that fails on SQLite, so this silently found nothing in
        local dev. Filtering is done in Python over a bounded, ordered window
        instead, which behaves the same on every dialect.
        """
        result = await self.session.execute(
            select(ExecutionHistory.checkpoint_data)
            .where(ExecutionHistory.checkpoint_data.isnot(None))
            .order_by(ExecutionHistory.created_at.desc())
            .limit(50)
        )
        for (data,) in result.all():
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except ValueError:
                    continue
            if isinstance(data, dict) and key in data:
                return data
        return None

    async def insert(
        self, run: ExecutionHistory, commit: bool = False
    ) -> ExecutionHistory:
        """Persist a new run row.

        ``commit`` is opt-in because the two callers differ: the scheduler owns
        its session and must commit so the spawned run can see the row, while a
        request-scoped caller leaves the commit to the router. Refresh follows the
        commit so server-side defaults (id, created_at) are populated.
        """
        self.session.add(run)
        if commit:
            await self.session.commit()
            await self.session.refresh(run)
        else:
            await self.session.flush()
        return run

    async def remove(self, run: ExecutionHistory, commit: bool = True) -> None:
        """Delete a run row.

        Commits by default: the callers own their session and expect the row gone
        when this returns.
        """
        await self.session.delete(run)
        if commit:
            await self.session.commit()

    async def get_execution_history(
        self, limit: int = 50, offset: int = 0, group_ids: List[str] = None
    ) -> tuple[List[ExecutionHistory], int]:
        """
        Get paginated execution history with group filtering.

        Args:
            limit: Maximum number of items to return
            offset: Number of items to skip
            group_ids: List of group IDs for filtering

        Returns:
            Tuple of (list of Run objects, total count)
        """
        # Use the session from the repository
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        session = self.session

        # Build base query with group filtering
        if group_ids and len(group_ids) > 0:
            base_filter = ExecutionHistory.group_id.in_(group_ids)
        else:
            # No filtering (fallback for admin/system access)
            base_filter = True

        # Get total count
        count_stmt = (
            select(func.count()).select_from(ExecutionHistory).where(base_filter)
        )
        total_count_result = await session.execute(count_stmt)
        total_count = total_count_result.scalar() or 0

        # Get paginated runs
        stmt = (
            select(ExecutionHistory)
            .where(base_filter)
            .order_by(ExecutionHistory.created_at.desc())
            .offset(offset)
            .limit(limit)
        )
        result = await session.execute(stmt)
        runs = result.scalars().all()

        return runs, total_count

    async def get_job_ids_for_groups(self, group_ids: List[str]) -> List[str]:
        """Job ids belonging to any of these groups."""
        if not group_ids:
            return []
        result = await self.session.execute(
            select(ExecutionHistory.job_id).where(
                ExecutionHistory.group_id.in_(group_ids)
            )
        )
        return [row[0] for row in result.fetchall()]

    async def count_by_group(self) -> List[Tuple[Optional[str], int]]:
        """(group_id, run count) across every group. Admin reporting."""
        result = await self.session.execute(
            select(
                distinct(ExecutionHistory.group_id), func.count(ExecutionHistory.id)
            ).group_by(ExecutionHistory.group_id)
        )
        return [(row[0], row[1]) for row in result.fetchall()]

    async def get_recent(
        self, limit: int, status: Optional[str] = None
    ) -> List[ExecutionHistory]:
        """Most recent runs, newest first, optionally filtered to one status.

        Recipe mining and the effectiveness report both read runs in bulk; they
        used to build this query themselves against ExecutionHistory, which is
        this repository's table.
        """
        stmt = select(ExecutionHistory)
        if status is not None:
            stmt = stmt.where(ExecutionHistory.status == status)
        stmt = stmt.order_by(ExecutionHistory.created_at.desc()).limit(limit)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_execution_by_id(
        self, execution_id: int, group_ids: List[str] = None
    ) -> Optional[ExecutionHistory]:
        """
        Get a specific execution by ID with group filtering.

        Args:
            execution_id: ID of the execution
            group_ids: List of group IDs for filtering

        Returns:
            Run object if found, None otherwise
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        session = self.session

        filters = [ExecutionHistory.id == execution_id]

        # Add group filtering
        if group_ids and len(group_ids) > 0:
            filters.append(ExecutionHistory.group_id.in_(group_ids))

        stmt = select(ExecutionHistory).where(*filters)
        result = await session.execute(stmt)
        return result.scalars().first()

    async def get_execution_by_job_id(
        self, job_id: str, group_ids: List[str] = None
    ) -> Optional[ExecutionHistory]:
        """
        Get a specific execution by job_id with group filtering.

        Args:
            job_id: Job ID of the execution
            group_ids: List of group IDs for filtering

        Returns:
            Run object if found, None otherwise
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        session = self.session

        filters = [ExecutionHistory.job_id == job_id]

        # Add group filtering
        if group_ids and len(group_ids) > 0:
            filters.append(ExecutionHistory.group_id.in_(group_ids))

        stmt = select(ExecutionHistory).where(*filters)
        result = await session.execute(stmt)
        return result.scalars().first()

    async def get_execution_summary_by_job_id(
        self, job_id: str, group_ids: List[str] = None
    ):
        """Slim scalar-only lookup for hot polling paths.

        The full-row variant drags the result/inputs/partial_results/checkpoint
        JSON blobs through the driver on every poll just to authorize access or
        report a RUNNING status; this selects every scalar column the polling
        responses need and none of the blobs. Returns a Row (or None) with
        ``.id``, ``.job_id``, ``.group_id``, ``.status``, ``.run_name``,
        ``.error``, ``.created_at``, ``.completed_at`` and the mlflow fields.
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")

        filters = [ExecutionHistory.job_id == job_id]
        if group_ids and len(group_ids) > 0:
            filters.append(ExecutionHistory.group_id.in_(group_ids))

        stmt = select(
            ExecutionHistory.id,
            ExecutionHistory.job_id,
            ExecutionHistory.group_id,
            ExecutionHistory.status,
            ExecutionHistory.run_name,
            ExecutionHistory.error,
            ExecutionHistory.created_at,
            ExecutionHistory.completed_at,
            ExecutionHistory.execution_type,
            ExecutionHistory.mlflow_trace_id,
            ExecutionHistory.mlflow_experiment_name,
            ExecutionHistory.mlflow_evaluation_run_id,
        ).where(*filters)
        result = await self.session.execute(stmt)
        return result.first()

    async def find_by_id(self, execution_id: int) -> Optional[ExecutionHistory]:
        """
        Find execution by ID.

        Args:
            execution_id: ID of the execution

        Returns:
            ExecutionHistory object if found, None otherwise
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        # Use existing session
        return await self._get_execution_by_id_internal(self.session, execution_id)

    async def check_execution_exists(
        self, execution_id: int, group_ids: List[str] = None
    ) -> bool:
        """
        Check if an execution exists, scoped to the given groups.

        Args:
            execution_id: ID of the execution
            group_ids: List of group IDs for tenant filtering. When provided,
                the execution only "exists" if it belongs to one of these groups,
                preventing cross-tenant existence probing.

        Returns:
            True if exists (within the given groups), False otherwise
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        filters = [ExecutionHistory.id == execution_id]
        if group_ids and len(group_ids) > 0:
            filters.append(ExecutionHistory.group_id.in_(group_ids))
        stmt = select(func.count()).select_from(ExecutionHistory).where(*filters)
        result = await self.session.execute(stmt)
        count = result.scalar() or 0
        return count > 0

    @staticmethod
    async def _delete_orphaned_flow_states(
        session: AsyncSession, flow_uuids: Iterable[Optional[str]]
    ) -> int:
        """
        Delete flow_states rows nothing references any more.

        ``flow_states`` is append-only — one row per flow method completion —
        and holds the mid-graph state a flow resumes from. It has no foreign key
        to executionhistory, so deleting an execution used to leave its state
        behind forever; a workspace that ran flows and then cleared its history
        kept every state row it had ever written.

        MUST be called AFTER the executionhistory rows are deleted: a flow_uuid
        can be shared by several executions (a resume chain reuses the source's
        state id), so "orphaned" means "no SURVIVING execution references it".
        Deleting on the strength of the rows being removed alone would pull the
        state out from under a run that is still there.

        Args:
            session: Session to delete on
            flow_uuids: flow_uuid values from the executions just deleted

        Returns:
            Number of flow_states rows deleted
        """
        uuids = {u for u in flow_uuids if u}
        if not uuids:
            return 0

        still_referenced = select(ExecutionHistory.flow_uuid).where(
            ExecutionHistory.flow_uuid.in_(uuids)
        )
        result = await session.execute(
            delete(FlowState).where(
                FlowState.flow_uuid.in_(uuids),
                FlowState.flow_uuid.notin_(still_referenced),
            )
        )
        count = result.rowcount or 0
        if count:
            logger.info(f"Deleted {count} orphaned flow_states row(s)")
        return count

    async def delete_execution(self, execution_id: int) -> Optional[Dict[str, Any]]:
        """
        Delete a specific execution and its associated data.

        Args:
            execution_id: ID of the execution

        Returns:
            Dictionary with deletion counts or None if execution not found
        """
        # ALWAYS use the session passed to the repository
        # Never create our own session - this breaks transaction boundaries
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")

        # Use the provided session without committing
        # The database router or service layer will handle commits
        return await self._delete_execution_with_session(
            self.session, execution_id, commit=False
        )

    async def _delete_execution_with_session(
        self, session: AsyncSession, execution_id: int, commit: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Internal method to handle deletion with a given session."""
        import logging

        logger = logging.getLogger(__name__)

        try:
            logger.debug(
                f"[DELETE] Starting deletion of execution {execution_id}, commit={commit}, session={id(session)}"
            )

            # Get the run first to check existence and get job_id
            run = await self._get_execution_by_id_internal(session, execution_id)
            if not run:
                logger.warning(f"[DELETE] Execution {execution_id} not found")
                return None

            job_id = run.job_id
            flow_uuid = run.flow_uuid
            logger.debug(
                f"[DELETE] Found execution {execution_id} with job_id={job_id}"
            )
            result = {}

            # Delete associated task statuses
            task_status_stmt = delete(TaskStatus).where(TaskStatus.job_id == job_id)
            task_status_result = await session.execute(task_status_stmt)
            result["task_status_count"] = task_status_result.rowcount

            # Delete associated error traces
            error_trace_stmt = delete(ErrorTrace).where(
                ErrorTrace.run_id == execution_id
            )
            error_trace_result = await session.execute(error_trace_stmt)
            result["error_trace_count"] = error_trace_result.rowcount

            # Delete the run
            run_stmt = delete(ExecutionHistory).where(
                ExecutionHistory.id == execution_id
            )
            delete_result = await session.execute(run_stmt)
            logger.debug(
                f"[DELETE] Deleted execution record, affected rows: {delete_result.rowcount}"
            )

            # After the run is gone, so "still referenced" is accurate.
            result["flow_state_count"] = await self._delete_orphaned_flow_states(
                session, [flow_uuid]
            )

            # Flush to ensure operations are sent to database
            await session.flush()
            logger.debug("[DELETE] Flushed delete operations to database")

            # Commit only when the caller asked for it; this repository never owns a session
            if commit:
                logger.debug(
                    f"[DELETE] Committing transaction for session {id(session)}"
                )
                await session.commit()
                logger.debug("[DELETE] Transaction committed successfully")
            else:
                logger.debug("[DELETE] Not committing - external session management")

            logger.debug(f"[DELETE] Successfully deleted execution {execution_id}")
            return {
                "execution_id": execution_id,
                "job_id": job_id,
                "task_status_count": result["task_status_count"],
                "error_trace_count": result["error_trace_count"],
                "flow_state_count": result["flow_state_count"],
            }
        except Exception as e:
            if commit:
                await session.rollback()
            raise e

    async def _get_execution_by_id_internal(
        self, session: AsyncSession, execution_id: int
    ) -> Optional[ExecutionHistory]:
        """Internal method to get execution by ID using provided session."""
        stmt = select(ExecutionHistory).where(ExecutionHistory.id == execution_id)
        result = await session.execute(stmt)
        return result.scalars().first()

    async def delete_execution_by_job_id(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Delete a specific execution and its associated data by job_id.

        Args:
            job_id: Job ID of the execution

        Returns:
            Dictionary with deletion counts or None if execution not found
        """
        # ALWAYS use the session passed to the repository
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        return await self._delete_execution_by_job_id_with_session(
            self.session, job_id, commit=False
        )

    async def _delete_execution_by_job_id_with_session(
        self, session: AsyncSession, job_id: str, commit: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Internal method to handle deletion by job_id with a given session."""
        try:
            # Get the run first to check existence
            run = await self._get_execution_by_job_id_internal(session, job_id)
            if not run:
                return None

            execution_id = run.id
            flow_uuid = run.flow_uuid
            result = {}

            # Delete associated task statuses
            task_status_stmt = delete(TaskStatus).where(TaskStatus.job_id == job_id)
            task_status_result = await session.execute(task_status_stmt)
            result["task_status_count"] = task_status_result.rowcount

            # Delete associated error traces
            error_trace_stmt = delete(ErrorTrace).where(
                ErrorTrace.run_id == execution_id
            )
            error_trace_result = await session.execute(error_trace_stmt)
            result["error_trace_count"] = error_trace_result.rowcount

            # Delete the run
            run_stmt = delete(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
            await session.execute(run_stmt)

            # After the run is gone, so "still referenced" is accurate.
            result["flow_state_count"] = await self._delete_orphaned_flow_states(
                session, [flow_uuid]
            )

            # Flush to ensure operations are sent to database
            await session.flush()

            # Commit only when the caller asked for it; this repository never owns a session
            if commit:
                await session.commit()

            return {
                "execution_id": execution_id,
                "job_id": job_id,
                "task_status_count": result["task_status_count"],
                "error_trace_count": result["error_trace_count"],
                "flow_state_count": result["flow_state_count"],
            }
        except Exception as e:
            if commit:
                await session.rollback()
            raise e

    async def _get_execution_by_job_id_internal(
        self, session: AsyncSession, job_id: str
    ) -> Optional[ExecutionHistory]:
        """Internal method to get execution by job ID using provided session."""
        stmt = select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
        result = await session.execute(stmt)
        return result.scalars().first()

    async def update_mlflow_evaluation_run_id(
        self, job_id: str, evaluation_run_id: str
    ) -> bool:
        """
        Update the MLflow evaluation run ID for an execution.

        Args:
            job_id: Job ID of the execution
            evaluation_run_id: MLflow evaluation run ID to set

        Returns:
            True if successful, False otherwise
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")

        import logging

        logger = logging.getLogger(__name__)

        try:
            # Find the execution by job_id
            stmt = select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
            result = await self.session.execute(stmt)
            execution = result.scalar_one_or_none()

            if not execution:
                logger.warning(f"No execution found with job_id: {job_id}")
                return False

            # Update the MLflow evaluation run ID
            execution.mlflow_evaluation_run_id = evaluation_run_id

            # Flush changes to database
            await self.session.flush()
            logger.info(
                f"Successfully updated MLflow evaluation run ID for job_id: {job_id}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Error updating MLflow evaluation run ID for job_id {job_id}: {str(e)}",
                exc_info=True,
            )
            return False

    async def delete_all_executions(
        self, group_ids: List[str] = None
    ) -> Dict[str, int]:
        """
        Delete all executions and associated data for specified groups.

        Args:
            group_ids: List of group IDs to filter deletions. If provided, only
                      executions belonging to these groups will be deleted.
                      If None, deletes ALL executions (admin/system access only).

        Returns:
            Dictionary with deletion counts
        """
        # ALWAYS use the session passed to the repository
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")
        return await self._delete_all_executions_with_session(
            self.session, group_ids=group_ids, commit=False
        )

    async def _delete_all_executions_with_session(
        self, session: AsyncSession, group_ids: List[str] = None, commit: bool = False
    ) -> Dict[str, int]:
        """Internal method to handle deletion of all executions with a given session."""
        try:
            result = {}

            # If group_ids provided, only delete executions for those groups
            if group_ids and len(group_ids) > 0:
                # First get all job_ids and execution_ids for the group
                stmt = select(
                    ExecutionHistory.id,
                    ExecutionHistory.job_id,
                    ExecutionHistory.flow_uuid,
                ).where(ExecutionHistory.group_id.in_(group_ids))
                exec_result = await session.execute(stmt)
                executions = exec_result.fetchall()

                execution_ids = [row[0] for row in executions]
                job_ids = [row[1] for row in executions]
                flow_uuids = [row[2] for row in executions]

                if not execution_ids:
                    return {
                        "run_count": 0,
                        "task_status_count": 0,
                        "error_trace_count": 0,
                        "flow_state_count": 0,
                    }

                # Delete task statuses for these job_ids
                task_status_stmt = delete(TaskStatus).where(
                    TaskStatus.job_id.in_(job_ids)
                )
                task_status_result = await session.execute(task_status_stmt)
                result["task_status_count"] = task_status_result.rowcount

                # Delete error traces for these execution_ids
                error_trace_stmt = delete(ErrorTrace).where(
                    ErrorTrace.run_id.in_(execution_ids)
                )
                error_trace_result = await session.execute(error_trace_stmt)
                result["error_trace_count"] = error_trace_result.rowcount

                # Delete execution traces (FK to executionhistory.id and .job_id)
                await session.execute(
                    delete(ExecutionTrace).where(
                        ExecutionTrace.run_id.in_(execution_ids)
                    )
                )
                await session.execute(
                    delete(ExecutionTrace).where(ExecutionTrace.job_id.in_(job_ids))
                )

                # Delete billing records (FK to executionhistory.job_id)
                await session.execute(
                    delete(LLMUsageBilling).where(
                        LLMUsageBilling.execution_id.in_(job_ids)
                    )
                )

                # Delete executions for the group
                run_count = len(execution_ids)
                run_stmt = delete(ExecutionHistory).where(
                    ExecutionHistory.group_id.in_(group_ids)
                )
                await session.execute(run_stmt)

                # Flow method state, once nothing references it. Another group
                # can legitimately share a flow_uuid, so this is filtered on
                # surviving references rather than wiping the table.
                result["flow_state_count"] = await self._delete_orphaned_flow_states(
                    session, flow_uuids
                )
            else:
                # No group filtering - delete ALL (admin/system access)
                # Delete all task statuses
                task_status_stmt = delete(TaskStatus)
                task_status_result = await session.execute(task_status_stmt)
                result["task_status_count"] = task_status_result.rowcount

                # Delete all error traces
                error_trace_stmt = delete(ErrorTrace)
                error_trace_result = await session.execute(error_trace_stmt)
                result["error_trace_count"] = error_trace_result.rowcount

                # Delete all execution traces (FK to executionhistory.id and .job_id)
                await session.execute(delete(ExecutionTrace))

                # Delete all billing records (FK to executionhistory.job_id)
                await session.execute(delete(LLMUsageBilling))

                # Delete all runs and count them
                count_stmt = select(func.count()).select_from(ExecutionHistory)
                count_result = await session.execute(count_stmt)
                run_count = count_result.scalar() or 0

                run_stmt = delete(ExecutionHistory)
                await session.execute(run_stmt)

                # Every execution is gone, so every flow state is orphaned.
                flow_state_result = await session.execute(delete(FlowState))
                result["flow_state_count"] = flow_state_result.rowcount or 0

            # Flush to ensure operations are sent to database
            await session.flush()

            # Commit only when the caller asked for it; this repository never owns a session
            if commit:
                await session.commit()

            return {
                "run_count": run_count,
                "task_status_count": result["task_status_count"],
                "error_trace_count": result["error_trace_count"],
                "flow_state_count": result.get("flow_state_count", 0),
            }
        except Exception as e:
            if commit:
                await session.rollback()
            raise e

    async def get_checkpoints_for_flow(
        self,
        flow_id,
        group_id: Optional[str] = None,
        status_filter: Optional[str] = "active",
    ) -> List[ExecutionHistory]:
        """
        Get available checkpoints for a specific flow.

        Args:
            flow_id: UUID of the flow to get checkpoints for
            group_id: Group ID for filtering (multi-tenant isolation)
            status_filter: Filter by checkpoint status ('active', 'resumed', 'expired', or None for all)

        Returns:
            List of ExecutionHistory records with checkpoint information
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")

        import logging

        logger = logging.getLogger(__name__)

        try:
            # Build filters - must have flow_uuid (checkpoint enabled) and match flow_id
            filters = [
                ExecutionHistory.flow_id == flow_id,
                ExecutionHistory.flow_uuid.isnot(
                    None
                ),  # Only executions with checkpoints
            ]

            # Add group filtering for multi-tenant isolation
            if group_id:
                filters.append(ExecutionHistory.group_id == group_id)

            # Add status filter if provided
            if status_filter:
                filters.append(ExecutionHistory.checkpoint_status == status_filter)

            logger.info(
                f"🔍 Checkpoint query for flow_id={flow_id}, group_id={group_id}, status_filter={status_filter}"
            )

            # Query for checkpoints ordered by most recent
            stmt = (
                select(ExecutionHistory)
                .where(*filters)
                .order_by(ExecutionHistory.created_at.desc())
            )
            result = await self.session.execute(stmt)
            checkpoints = result.scalars().all()

            logger.info(f"📊 Found {len(checkpoints)} checkpoints for flow {flow_id}")
            if len(checkpoints) == 0:
                # Debug: Check if any executions exist for this flow
                debug_stmt = select(ExecutionHistory).where(
                    ExecutionHistory.flow_id == flow_id
                )
                debug_result = await self.session.execute(debug_stmt)
                all_executions = debug_result.scalars().all()
                logger.info(f"   Total executions for flow: {len(all_executions)}")
                for ex in all_executions[:5]:  # Log first 5
                    logger.info(
                        f"   - Execution {ex.id}: flow_uuid={ex.flow_uuid}, checkpoint_status={ex.checkpoint_status}, status={ex.status}"
                    )

            return list(checkpoints)

        except Exception as e:
            logger.error(
                f"Error getting checkpoints for flow {flow_id}: {str(e)}", exc_info=True
            )
            raise

    async def update_checkpoint_status(
        self, execution_id: int, status: str, group_id: Optional[str] = None
    ) -> bool:
        """
        Update the checkpoint status for an execution.

        Args:
            execution_id: ID of the execution to update
            status: New checkpoint status ('active', 'resumed', 'expired')
            group_id: Group ID for filtering (multi-tenant isolation)

        Returns:
            True if successful, False if execution not found
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")

        import logging

        logger = logging.getLogger(__name__)

        try:
            # Build filters
            filters = [ExecutionHistory.id == execution_id]
            if group_id:
                filters.append(ExecutionHistory.group_id == group_id)

            # Find the execution
            stmt = select(ExecutionHistory).where(*filters)
            result = await self.session.execute(stmt)
            execution = result.scalar_one_or_none()

            if not execution:
                logger.warning(f"No execution found with id: {execution_id}")
                return False

            # Update the checkpoint status
            execution.checkpoint_status = status

            # Flush changes to database
            await self.session.flush()
            logger.info(
                f"Updated checkpoint status to '{status}' for execution {execution_id}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Error updating checkpoint status for execution {execution_id}: {str(e)}",
                exc_info=True,
            )
            return False

    async def set_checkpoint_info(
        self,
        execution_id: int,
        flow_uuid: str,
        checkpoint_status: str = "active",
        checkpoint_method: Optional[str] = None,
    ) -> bool:
        """
        Set checkpoint information for an execution.

        Called when a flow execution with checkpoint enabled completes or checkpoints.

        Args:
            execution_id: ID of the execution
            flow_uuid: CrewAI state.id for resuming the flow
            checkpoint_status: Initial status (default: 'active')
            checkpoint_method: Name of the last checkpointed method

        Returns:
            True if successful, False if execution not found
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")

        import logging

        logger = logging.getLogger(__name__)

        try:
            logger.info(
                f"✏️ set_checkpoint_info called: execution_id={execution_id}, flow_uuid={flow_uuid}, status={checkpoint_status}"
            )

            # Find the execution
            stmt = select(ExecutionHistory).where(ExecutionHistory.id == execution_id)
            result = await self.session.execute(stmt)
            execution = result.scalar_one_or_none()

            if not execution:
                logger.warning(f"❌ No execution found with id: {execution_id}")
                return False

            logger.info(
                f"   Found execution {execution_id}: flow_id={execution.flow_id}, current flow_uuid={execution.flow_uuid}, current checkpoint_status={execution.checkpoint_status}"
            )

            # Set checkpoint information
            execution.flow_uuid = flow_uuid
            execution.checkpoint_status = checkpoint_status
            execution.checkpoint_method = checkpoint_method

            # Flush changes to database
            await self.session.flush()
            logger.info(
                f"✅ Set checkpoint info for execution {execution_id}: flow_uuid={flow_uuid}, status={checkpoint_status}, method={checkpoint_method}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Error setting checkpoint info for execution {execution_id}: {str(e)}",
                exc_info=True,
            )
            return False

    async def get_checkpoint_data(
        self, job_id: str, group_ids: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get the raw checkpoint_data column for an execution (group-scoped).

        Returns the column as stored. Interpreting it — which key holds the
        resume record, what version it is, how to migrate an old one — belongs
        to services/execution/checkpointing/schema.py; this layer must not
        import it, so it does not parse what it returns.

        Args:
            job_id: Job ID of the execution
            group_ids: Group IDs for tenant isolation

        Returns:
            The checkpoint_data dict, or None if absent
        """
        try:
            filters = [ExecutionHistory.job_id == job_id]
            if group_ids:
                filters.append(ExecutionHistory.group_id.in_(group_ids))
            result = await self.session.execute(
                select(ExecutionHistory).where(*filters)
            )
            execution = result.scalar_one_or_none()

            if not execution:
                return None
            return execution.checkpoint_data

        except Exception as e:
            logger.error(
                f"Error getting checkpoint data for job {job_id}: {str(e)}",
                exc_info=True,
            )
            return None

    async def set_checkpoint_status(
        self,
        job_id: str,
        status: Optional[str],
        group_ids: Optional[List[str]] = None,
    ) -> bool:
        """
        Move an execution's checkpoint_status, addressed by job_id.

        The flow path's update_checkpoint_status() takes the integer primary
        key; the unified checkpoint API addresses executions by job_id like
        every other /executions route, so both exist.

        Args:
            job_id: Job ID of the execution
            status: New lifecycle status, or None to clear it
            group_ids: Group IDs for tenant isolation

        Returns:
            True if updated, False if not found or on error
        """
        try:
            filters = [ExecutionHistory.job_id == job_id]
            if group_ids:
                filters.append(ExecutionHistory.group_id.in_(group_ids))

            result = await self.session.execute(
                select(ExecutionHistory).where(*filters)
            )
            execution = result.scalar_one_or_none()

            if not execution:
                logger.warning(f"Execution not found for job_id: {job_id}")
                return False

            execution.checkpoint_status = status
            await self.session.flush()
            return True

        except Exception as e:
            logger.error(
                f"Error setting checkpoint status for job {job_id}: {str(e)}",
                exc_info=True,
            )
            return False

    async def set_checkpoint_data(
        self,
        job_id: str,
        checkpoint_data: Optional[Dict[str, Any]],
        checkpoint_status: Any = _UNSET,
    ) -> bool:
        """
        Replace the checkpoint_data column, and optionally the status with it.

        The caller passes the FULL column value it wants stored (the
        checkpointing store merges the record into the existing keys before
        calling this), so sibling keys — HITL edited_config, ucmv_yaml_edits —
        survive only because the caller carried them over. That is deliberate:
        a repository that merged would need to know which key is the record.

        Called from the crew and flow SUBPROCESSES as units complete;
        fail-open, since a checkpoint write must never fail the run.

        Args:
            job_id: Job ID of the execution
            checkpoint_data: The complete column value (None clears it)
            checkpoint_status: New lifecycle status; omit to leave unchanged,
                pass None to clear it

        Returns:
            True if successful, False otherwise
        """
        try:
            result = await self.session.execute(
                select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
            )
            execution = result.scalar_one_or_none()

            if not execution:
                logger.warning(f"Execution not found for job_id: {job_id}")
                return False

            # A new dict, not the mutated original — reassigning the same object
            # would not register as a change on a JSON column.
            execution.checkpoint_data = (
                dict(checkpoint_data) if checkpoint_data else None
            )
            if checkpoint_status is not _UNSET:
                execution.checkpoint_status = checkpoint_status

            await self.session.flush()
            return True

        except Exception as e:
            logger.error(
                f"Error setting checkpoint data for job {job_id}: {str(e)}",
                exc_info=True,
            )
            return False

    async def update_execution_result(
        self, job_id: str, result_data: dict, group_ids: list[str] = None
    ) -> bool:
        """
        Update the result field for an execution identified by job_id.

        Args:
            job_id: Job ID of the execution
            result_data: New result data to store
            group_ids: Optional list of group IDs for tenant filtering

        Returns:
            True if successful, False if execution not found
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")

        try:
            filters = [ExecutionHistory.job_id == job_id]
            if group_ids and len(group_ids) > 0:
                filters.append(ExecutionHistory.group_id.in_(group_ids))

            stmt = select(ExecutionHistory).where(*filters)
            result = await self.session.execute(stmt)
            execution = result.scalar_one_or_none()

            if not execution:
                logger.warning(f"No execution found with job_id: {job_id}")
                return False

            import json as _json

            # Detect the type of result being saved so we use a dedicated
            # checkpoint_data key and never collide with other tools.
            is_ucmv = (
                isinstance(result_data, dict)
                and "yaml" in result_data
                and "sql" in result_data
            )
            is_config = isinstance(result_data, dict) and "join_key_map" in result_data

            checkpoint_data = execution.checkpoint_data or {}

            if is_ucmv:
                # UCMV Generator edit: use a dedicated key so Config Generator
                # saves (which also use edited_config) never overwrite it.
                checkpoint_data["ucmv_yaml_edits"] = result_data
                logger.info(
                    f"Saved UCMV yaml edits to checkpoint_data.ucmv_yaml_edits for job_id: {job_id}"
                )
                # Also write to /tmp so the Validator subprocess can read the
                # user's edits (subprocess can't connect to DB in deployed env).
                try:
                    import os as _os

                    _tmp_path = "/tmp/ucmv_user_edits.json"
                    with open(_tmp_path, "w") as _f:
                        _json.dump(result_data, _f)
                    logger.info(
                        f"[UCMV] Wrote user edits to {_tmp_path} for validator subprocess access"
                    )
                except Exception as _tmp_err:
                    logger.debug(
                        f"[UCMV] Could not write user edits to /tmp: {_tmp_err}"
                    )
            elif is_config:
                # Config Generator edit: keep using edited_config (existing behaviour)
                checkpoint_data["edited_config"] = result_data
                logger.info(
                    f"Saved config edits to checkpoint_data.edited_config for job_id: {job_id}"
                )
            else:
                # Generic: store under edited_config as before
                checkpoint_data["edited_config"] = result_data

            execution.checkpoint_data = checkpoint_data
            await self.session.flush()
            logger.info(f"Saved edited result for job_id: {job_id}")
            return True

        except Exception as e:
            logger.error(
                f"Error updating result for job_id {job_id}: {str(e)}",
                exc_info=True,
            )
            return False

    async def get_task_statuses_by_job_id(self, job_id: str) -> List[TaskStatus]:
        """All TaskStatus rows for a job.

        Used by the status-detail endpoint to compute task-progress counters
        while a run is RUNNING/STOPPING. ``taskstatus.job_id`` FKs to
        ``executionhistory.job_id``, so this table belongs to this repository
        even though the caller is interested in progress, not history rows.
        """
        result = await self.session.execute(
            select(TaskStatus).where(TaskStatus.job_id == job_id)
        )
        return list(result.scalars().all())

    async def get_job_ids_by_statuses(self, statuses: List[str]) -> List[str]:
        """Job ids currently in any of the given statuses, unfiltered by group.

        Shared by two system-level background sweeps rather than tenant
        requests: zombie-job recovery (looking for stuck RUNNING jobs) and the
        trace SSE poller (finding jobs to watch when only the global stream
        has listeners). Neither runs on behalf of one tenant, so there is no
        group scoping here — callers must not expose this beyond that context.
        """
        result = await self.session.execute(
            select(ExecutionHistory.job_id).where(ExecutionHistory.status.in_(statuses))
        )
        return [row[0] for row in result.fetchall() if row[0]]

    async def get_by_id_and_type(
        self, execution_id: int, execution_type: str
    ) -> Optional[ExecutionHistory]:
        """Single execution row scoped by both PK and execution_type.

        The three execution paths (agent/crew/flow) share this table; the
        flow-builder service filters on execution_type as well as id so it
        never picks up a row created under a different path.
        """
        stmt = select(ExecutionHistory).where(
            ExecutionHistory.id == execution_id,
            ExecutionHistory.execution_type == execution_type,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_job_id_and_type(
        self, job_id: str, execution_type: str
    ) -> Optional[ExecutionHistory]:
        """Same lookup as ``get_execution_by_job_id`` but also scoped to execution_type."""
        stmt = select(ExecutionHistory).where(
            ExecutionHistory.job_id == job_id,
            ExecutionHistory.execution_type == execution_type,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_flow_id_and_type(
        self, flow_id: Union[UUID, str], execution_type: str
    ) -> List[ExecutionHistory]:
        """All executions for one flow, newest first, scoped to execution_type."""
        stmt = (
            select(ExecutionHistory)
            .where(
                ExecutionHistory.flow_id == flow_id,
                ExecutionHistory.execution_type == execution_type,
            )
            .order_by(ExecutionHistory.created_at.desc())
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def mark_stopping(
        self, job_id: str, reason: Optional[str], requested_by: Optional[str]
    ) -> None:
        """Flip an execution to STOPPING and record who asked and why.

        A bulk UPDATE rather than select-then-mutate: ``stop_execution`` races
        the subprocess that owns this row while it attempts to terminate it,
        so the write stays as narrow as possible instead of loading the ORM
        object into this session too.
        """
        await self.session.execute(
            update(ExecutionHistory)
            .where(ExecutionHistory.job_id == job_id)
            .values(
                status=ExecutionStatus.STOPPING.value,
                is_stopping=True,
                stop_reason=reason,
                stop_requested_by=requested_by,
            )
        )

    async def mark_stopped(self, job_id: str, partial_results: Optional[Any]) -> None:
        """Final transition to STOPPED once termination has been attempted."""
        await self.session.execute(
            update(ExecutionHistory)
            .where(ExecutionHistory.job_id == job_id)
            .values(
                status=ExecutionStatus.STOPPED.value,
                is_stopping=False,
                stopped_at=datetime.utcnow(),
                partial_results=partial_results,
            )
        )

    async def mark_stop_failed(self, job_id: str, error_message: str) -> None:
        """Clear is_stopping and record the error when a stop attempt itself
        raises, so the row doesn't get stuck showing "stopping" forever."""
        await self.session.execute(
            update(ExecutionHistory)
            .where(ExecutionHistory.job_id == job_id)
            .values(is_stopping=False, error=error_message)
        )

    async def delete_older_than(self, cutoff: datetime) -> Dict[str, int]:
        """
        Delete execution history records older than a cutoff date, including
        associated taskstatus and errortrace rows.

        Args:
            cutoff: Delete records with created_at before this datetime

        Returns:
            Dictionary with deletion counts for each table
        """
        if not self.session:
            raise RuntimeError("ExecutionHistoryRepository requires a session")

        try:
            # Get execution ids and job_ids for records older than cutoff
            stmt = select(
                ExecutionHistory.id,
                ExecutionHistory.job_id,
                ExecutionHistory.flow_uuid,
            ).where(ExecutionHistory.created_at < cutoff)
            exec_result = await self.session.execute(stmt)
            executions = exec_result.fetchall()

            execution_ids = [row[0] for row in executions]
            job_ids = [row[1] for row in executions]
            flow_uuids = [row[2] for row in executions]

            if not execution_ids:
                return {
                    "executionhistory": 0,
                    "taskstatus": 0,
                    "errortrace": 0,
                    "flow_states": 0,
                }

            # Delete associated task statuses
            task_status_stmt = delete(TaskStatus).where(TaskStatus.job_id.in_(job_ids))
            task_status_result = await self.session.execute(task_status_stmt)
            task_status_count = task_status_result.rowcount

            # Delete associated error traces
            error_trace_stmt = delete(ErrorTrace).where(
                ErrorTrace.run_id.in_(execution_ids)
            )
            error_trace_result = await self.session.execute(error_trace_stmt)
            error_trace_count = error_trace_result.rowcount

            # Delete the execution history records
            run_stmt = delete(ExecutionHistory).where(
                ExecutionHistory.created_at < cutoff
            )
            run_result = await self.session.execute(run_stmt)
            run_count = run_result.rowcount

            # Flow method state for the purged runs. flow_states has no FK to
            # executionhistory, so housekeeping never reached it and the table
            # grew without bound on any workspace that ran flows.
            flow_state_count = await self._delete_orphaned_flow_states(
                self.session, flow_uuids
            )

            await self.session.flush()

            return {
                "executionhistory": run_count,
                "taskstatus": task_status_count,
                "errortrace": error_trace_count,
                "flow_states": flow_state_count,
            }
        except Exception as e:
            logger.error(
                f"Error deleting execution history older than {cutoff}: {str(e)}",
                exc_info=True,
            )
            raise


# Don't create a singleton instance - repositories should be created with sessions
# execution_history_repository = ExecutionHistoryRepository()  # Removed - causes session issues
