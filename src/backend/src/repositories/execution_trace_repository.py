"""
Repository for execution trace operations.

This module provides functions for CRUD operations on execution traces.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import Text, cast, delete, func, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from src.core.base_repository import BaseRepository
from src.core.logger import LoggerManager
from src.models.execution_history import ExecutionHistory
from src.models.execution_trace import ExecutionTrace

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system


class ExecutionTraceRepository(BaseRepository[ExecutionTrace]):
    """Repository class for handling ExecutionTrace database operations."""

    def __init__(self, session: AsyncSession):
        """
        Initialize repository with session.

        Args:
            session: Database session from FastAPI DI
        """
        super().__init__(ExecutionTrace, session)
        self.session = session

    # Methods that require an existing session (primarily for internal use)

    async def outputs_containing(self, job_id: str, needle: str) -> List[Any]:
        """This run's trace outputs whose serialised form contains ``needle``.

        Serves the flow runner's CI/CD artifact scrape. ``cast(..., Text)`` keeps
        the LIKE working whether the column is JSON (Postgres) or text (SQLite) —
        the caller's raw ``CAST(output AS TEXT)`` did the same thing but outside a
        repository, so nothing shared it.
        """
        result = await self.session.execute(
            select(ExecutionTrace.output)
            .where(
                ExecutionTrace.job_id == job_id,
                cast(ExecutionTrace.output, Text).like(f"%{needle}%"),
            )
            .order_by(ExecutionTrace.created_at.asc())
        )
        return [row[0] for row in result.all()]

    async def tool_recordings(
        self,
        group_ids: List[str],
        *,
        since: datetime,
        exclude_job_id: str,
        limit: int = 500,
    ) -> List[ExecutionTrace]:
        """Completed tool calls from EARLIER runs — the replay cassette.

        Only ``kasal.tool.complete`` rows: the start row repeats the arguments
        but has no result, and a recording without a result is not one. Scoped
        to the caller's groups because a recording is workspace data like any
        other, and excludes the run doing the asking so a run cannot replay
        itself.

        Newest first, bounded — the caller keeps the newest run's worth and
        drops the rest, so this is a page of candidates, not a full history.
        """
        result = await self.session.execute(
            select(ExecutionTrace)
            .where(
                ExecutionTrace.group_id.in_(group_ids),
                ExecutionTrace.event_type == "tool_usage",
                ExecutionTrace.span_name == "kasal.tool.complete",
                ExecutionTrace.created_at >= since,
                ExecutionTrace.job_id != exclude_job_id,
            )
            .order_by(ExecutionTrace.id.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def latest_output_for_span_prefix(self, prefix: str) -> Optional[str]:
        """The most recent trace ``output`` whose ``span_name`` starts with ``prefix``.

        Serves the PowerBI/UCMV tools, which hand later steps the output of an
        earlier one. Returns the raw JSON string, or None when nothing matched.

        Built with typed constructs rather than raw SQL on purpose: the callers'
        version used ``output::text``, a Postgres-only cast that fails on SQLite,
        so the tool silently found nothing in local dev. ``like(f"{prefix}%")``
        parameterises the prefix, and JSON serialisation is left to the caller.
        """
        result = await self.session.execute(
            select(ExecutionTrace.output)
            .where(ExecutionTrace.span_name.like(f"{prefix}%"))
            .order_by(ExecutionTrace.created_at.desc())
            .limit(1)
        )
        row = result.first()
        if not row or row[0] is None:
            return None
        value = row[0]
        return value if isinstance(value, str) else json.dumps(value)

    async def add_batch(self, traces: List[ExecutionTrace]) -> int:
        """Stage many trace rows in one transaction, returning how many.

        Separate from ``_create``: that path commits and refreshes per row, which
        for the OTel exporter's batches would be one round trip per span. Staging
        only — the caller commits once for the whole batch.
        """
        for trace in traces:
            self.session.add(trace)
        return len(traces)

    async def _create(self, trace_data: Dict[str, Any]) -> ExecutionTrace:
        """
        Create a new execution trace record.

        Args:
            trace_data: Dictionary with trace data

        Returns:
            Created ExecutionTrace record
        """
        try:
            trace = ExecutionTrace(**trace_data)
            self.session.add(trace)
            # Flush to assign primary key before commit (important for some backends)
            await self.session.flush()
            # Capture id early in case refresh fails
            _trace_id = getattr(trace, "id", None)
            # Best-effort refresh; not strictly needed with expire_on_commit=False
            try:
                if getattr(trace, "id", None) is None and _trace_id is not None:
                    # If PK wasn’t populated, set it from pre-commit value
                    trace.id = _trace_id
                else:
                    await self.session.refresh(trace)
            except Exception as refresh_err:
                logger.debug(
                    f"Refresh after trace insert failed (non-fatal): {refresh_err}"
                )
            return trace
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(f"Database error creating execution trace: {str(e)}")
            raise

    async def _get_by_id(self, trace_id: int) -> Optional[ExecutionTrace]:
        """
        Get an execution trace by ID.

        Args:
            trace_id: ID of the trace to retrieve

        Returns:
            ExecutionTrace if found, None otherwise
        """
        try:
            stmt = select(ExecutionTrace).where(ExecutionTrace.id == trace_id)
            result = await self.session.execute(stmt)
            return result.scalars().first()
        except SQLAlchemyError as e:
            logger.error(
                f"Database error retrieving execution trace {trace_id}: {str(e)}"
            )
            raise

    async def _get_by_run_id(
        self,
        run_id: int,
        limit: Optional[int] = None,
        offset: Optional[int] = 0,
        since_id: Optional[int] = None,
    ) -> List[ExecutionTrace]:
        """
        Get execution traces by run_id.

        Args:
            run_id: Run ID to filter by
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            since_id: Only return traces with id greater than this (incremental
                cursor for pollers — avoids re-reading the whole trace set)

        Returns:
            List of ExecutionTrace records
        """
        try:
            # Deterministic order is required: offset pagination without ORDER BY
            # lets Postgres return rows in any order, so pollers can skip or
            # duplicate traces between pages. PK order matches insertion order.
            stmt = (
                select(ExecutionTrace)
                .where(ExecutionTrace.run_id == run_id)
                .order_by(ExecutionTrace.id.asc())
            )

            if since_id:
                stmt = stmt.where(ExecutionTrace.id > since_id)
            if offset is not None:
                stmt = stmt.offset(offset)
            if limit is not None:
                stmt = stmt.limit(limit)

            result = await self.session.execute(stmt)
            return result.scalars().all()
        except SQLAlchemyError as e:
            logger.error(
                f"Database error retrieving traces for run_id {run_id}: {str(e)}"
            )
            raise

    async def get_span_ids_by_event_type(self, job_id: str) -> Dict[str, str]:
        """event_type -> span_id for a run's spans, earliest occurrence winning.

        Used to find a run's ROOT span so later rows can be parented to it; a
        span with no parent floats at the top of the trace as its own run.
        """
        result = await self.session.execute(
            select(ExecutionTrace.event_type, ExecutionTrace.span_id)
            .where(
                ExecutionTrace.job_id == job_id,
                ExecutionTrace.span_id.is_not(None),
            )
            .order_by(ExecutionTrace.id)
        )
        return {event_type: span_id for event_type, span_id in result.all()}

    async def get_attribution_candidates(
        self, job_id: str
    ) -> List[Tuple[Any, Any, Any]]:
        """(event_source, event_context, trace_metadata) newest first.

        The caller walks these to attribute an orphan row to the agent and task
        that were most recently active.
        """
        result = await self.session.execute(
            select(
                ExecutionTrace.event_source,
                ExecutionTrace.event_context,
                ExecutionTrace.trace_metadata,
            )
            .where(ExecutionTrace.job_id == job_id)
            .order_by(ExecutionTrace.id.desc())
        )
        return list(result.all())

    async def get_event_shape_by_job_id(
        self, job_id: str
    ) -> List[Tuple[Optional[str], Any, Any, Any, Optional[str]]]:
        """(event_type, output, created_at, trace_metadata, span_name) per span.

        A narrow projection on purpose: recipe mining reads what a crew ACTUALLY
        did, and pulling whole ExecutionTrace rows for that would drag every
        output blob into memory.

        ``trace_metadata`` and ``span_name`` are part of that projection because
        the things mining needs are only there: ``tool_name`` lives in
        ``trace_metadata`` (``output`` holds ``{duration_ms, extra_data}``), and
        a guardrail rejection is only distinguishable by ``span_name``
        (``kasal.guardrail.failed`` — its ``event_type`` is the same
        ``llm_guardrail`` a PASS carries). Selecting only event_type/output made
        every recipe record ``tool_names=[]`` and ``tool_call_count=0``.
        """
        result = await self.session.execute(
            select(
                ExecutionTrace.event_type,
                ExecutionTrace.output,
                ExecutionTrace.created_at,
                ExecutionTrace.trace_metadata,
                ExecutionTrace.span_name,
            ).where(ExecutionTrace.job_id == job_id)
        )
        return list(result.all())

    async def _get_by_job_id(
        self,
        job_id: str,
        limit: Optional[int] = None,
        offset: Optional[int] = 0,
        since_id: Optional[int] = None,
    ) -> List[ExecutionTrace]:
        """
        Get execution traces by job_id.

        Args:
            job_id: Job ID to filter by
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            since_id: Only return traces with id greater than this (incremental
                cursor for pollers — avoids re-reading the whole trace set)

        Returns:
            List of ExecutionTrace records
        """
        try:
            # Deterministic order is required: offset pagination without ORDER BY
            # lets Postgres return rows in any order, so pollers can skip or
            # duplicate traces between pages. PK order matches insertion order.
            stmt = (
                select(ExecutionTrace)
                .where(ExecutionTrace.job_id == job_id)
                .order_by(ExecutionTrace.id.asc())
            )

            if since_id:
                stmt = stmt.where(ExecutionTrace.id > since_id)
            if offset is not None:
                stmt = stmt.offset(offset)
            if limit is not None:
                stmt = stmt.limit(limit)

            result = await self.session.execute(stmt)
            return result.scalars().all()
        except SQLAlchemyError as e:
            logger.error(
                f"Database error retrieving traces for job_id {job_id}: {str(e)}"
            )
            raise

    async def _get_all_traces(
        self, limit: Optional[int] = None, offset: Optional[int] = 0
    ) -> Tuple[List[ExecutionTrace], int]:
        """
        Get all execution traces with pagination.

        Args:
            limit: Maximum number of traces to return
            offset: Number of traces to skip

        Returns:
            Tuple of (list of ExecutionTrace records, total count)
        """
        try:
            # Get all traces
            stmt = select(ExecutionTrace).order_by(ExecutionTrace.created_at.desc())

            if offset is not None:
                stmt = stmt.offset(offset)
            if limit is not None:
                stmt = stmt.limit(limit)

            result = await self.session.execute(stmt)
            traces = result.scalars().all()

            # Get total count
            count_stmt = select(func.count()).select_from(ExecutionTrace)
            total_count_result = await self.session.execute(count_stmt)
            total_count = total_count_result.scalar() or 0

            return traces, total_count
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving all traces: {str(e)}")
            raise

    async def _get_execution_job_id_by_run_id(self, run_id: int) -> Optional[str]:
        """
        Get job_id for an execution by run_id.

        Args:
            run_id: Run ID to look up

        Returns:
            job_id if found, None otherwise
        """
        try:
            stmt = select(ExecutionHistory.job_id).where(ExecutionHistory.id == run_id)
            result = await self.session.execute(stmt)
            return result.scalar()
        except SQLAlchemyError as e:
            logger.error(
                f"Database error retrieving job_id for run_id {run_id}: {str(e)}"
            )
            raise

    async def _get_execution_run_id_by_job_id(self, job_id: str) -> Optional[int]:
        """
        Get run_id for an execution by job_id.

        Args:
            job_id: Job ID to look up

        Returns:
            run_id if found, None otherwise
        """
        try:
            stmt = select(ExecutionHistory.id).where(ExecutionHistory.job_id == job_id)
            result = await self.session.execute(stmt)
            return result.scalar()
        except SQLAlchemyError as e:
            logger.error(
                f"Database error retrieving run_id for job_id {job_id}: {str(e)}"
            )
            raise

    async def _delete_by_id(self, trace_id: int) -> int:
        """
        Delete an execution trace by ID.

        Args:
            trace_id: ID of the trace to delete

        Returns:
            Number of deleted records (0 or 1)
        """
        try:
            stmt = delete(ExecutionTrace).where(ExecutionTrace.id == trace_id)
            result = await self.session.execute(stmt)
            await self.session.flush()
            return result.rowcount
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(f"Database error deleting trace {trace_id}: {str(e)}")
            raise

    async def _delete_by_run_id(self, run_id: int) -> int:
        """
        Delete all execution traces by run_id.

        Args:
            run_id: Run ID to filter by

        Returns:
            Number of deleted records
        """
        try:
            stmt = delete(ExecutionTrace).where(ExecutionTrace.run_id == run_id)
            result = await self.session.execute(stmt)
            await self.session.flush()
            return result.rowcount
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(
                f"Database error deleting traces for run_id {run_id}: {str(e)}"
            )
            raise

    async def _delete_by_job_id(self, job_id: str) -> int:
        """
        Delete all execution traces by job_id.

        Args:
            job_id: Job ID to filter by

        Returns:
            Number of deleted records
        """
        try:
            stmt = delete(ExecutionTrace).where(ExecutionTrace.job_id == job_id)
            result = await self.session.execute(stmt)
            await self.session.flush()
            return result.rowcount
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(
                f"Database error deleting traces for job_id {job_id}: {str(e)}"
            )
            raise

    async def _delete_all(self) -> int:
        """
        Delete all execution traces.

        Returns:
            Number of deleted records
        """
        try:
            stmt = delete(ExecutionTrace)
            result = await self.session.execute(stmt)
            await self.session.flush()
            return result.rowcount
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(f"Database error deleting all traces: {str(e)}")
            raise

    # Public methods. They use self.session — the one handed to the constructor —
    # and never acquire their own; the caller owns the transaction.

    async def create(
        self,
        trace_data: Dict[str, Any],
        verify_execution_exists: bool = True,
    ) -> ExecutionTrace:
        """
        Create a new execution trace record.

        Args:
            trace_data: Dictionary with trace data
            verify_execution_exists: Whether to run the parent ExecutionHistory
                existence SELECT before inserting. Per-run batch writers verify
                the first event and skip the check (one fewer roundtrip per
                event) for the rest of the run.

        Returns:
            Created ExecutionTrace record

        Raises:
            ValueError: If job_id is provided but doesn't exist in ExecutionHistory
                        (skipped in subprocess mode where the job record may live
                        in Lakebase and not be visible to the local DB connection)
        """
        import os

        job_id = trace_data.get("job_id")
        is_subprocess = os.environ.get("CREW_SUBPROCESS_MODE") == "true"

        if job_id and not is_subprocess and verify_execution_exists:
            # Check if job exists in executionhistory (main process only).
            # In subprocess mode we skip this check because:
            # 1. The subprocess already validated the job_id at launch
            # 2. When Lakebase is the active backend, execution_history lives
            #    in Lakebase but the subprocess OTel exporter uses a local
            #    NullPool engine that can't see Lakebase data
            stmt = select(ExecutionHistory).where(ExecutionHistory.job_id == job_id)
            result = await self.session.execute(stmt)
            job_exists = result.scalars().first()

            # If job doesn't exist, raise an error instead of creating orphan records
            if not job_exists:
                logger.warning(f"Attempt to create trace for non-existent job {job_id}")
                raise ValueError(
                    f"Job {job_id} does not exist in ExecutionHistory. Trace creation aborted."
                )
            else:
                # Job exists, ensure run_id is set in trace_data
                if "run_id" not in trace_data and job_exists:
                    trace_data["run_id"] = job_exists.id
                    logger.info(
                        f"Setting run_id={job_exists.id} for existing job {job_id}"
                    )
        elif job_id and is_subprocess:
            logger.debug(f"Subprocess mode: skipping job existence check for {job_id}")

        # Create the trace with the existing job
        return await self._create(trace_data)

    async def get_by_id(self, trace_id: int) -> Optional[ExecutionTrace]:
        """
        Get an execution trace by ID.

        Args:
            trace_id: ID of the trace to retrieve

        Returns:
            ExecutionTrace if found, None otherwise
        """
        return await self._get_by_id(trace_id)

    async def get_by_run_id(
        self,
        run_id: int,
        limit: Optional[int] = None,
        offset: Optional[int] = 0,
        since_id: Optional[int] = None,
    ) -> List[ExecutionTrace]:
        """
        Get execution traces by run_id.

        Args:
            run_id: Run ID to filter by
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            since_id: Incremental cursor — only traces with id greater than this

        Returns:
            List of ExecutionTrace records
        """
        return await self._get_by_run_id(run_id, limit, offset, since_id)

    async def get_by_job_id(
        self,
        job_id: str,
        limit: Optional[int] = None,
        offset: Optional[int] = 0,
        since_id: Optional[int] = None,
    ) -> List[ExecutionTrace]:
        """
        Get execution traces by job_id.

        Args:
            job_id: Job ID to filter by
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            since_id: Incremental cursor — only traces with id greater than this

        Returns:
            List of ExecutionTrace records
        """
        return await self._get_by_job_id(job_id, limit, offset, since_id)

    async def get_by_group_ids(
        self,
        group_ids: List[str],
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[ExecutionTrace], int]:
        """Group-scoped trace page + exact total, in two indexed queries.

        ``execution_trace.group_id`` is denormalized AND indexed precisely so
        this listing doesn't need the executions→per-job N+1 walk it replaced
        (hundreds of queries for groups with hundreds of runs, with a wrong
        ``total`` capped at 100 per job). Newest first.

        Returns:
            (traces page, exact total count)
        """
        try:
            group_filter = ExecutionTrace.group_id.in_(group_ids)
            stmt = (
                select(ExecutionTrace)
                .where(group_filter)
                .order_by(ExecutionTrace.id.desc())
                .offset(offset)
                .limit(limit)
            )
            result = await self.session.execute(stmt)
            traces = result.scalars().all()

            count_stmt = (
                select(func.count()).select_from(ExecutionTrace).where(group_filter)
            )
            total = (await self.session.execute(count_stmt)).scalar() or 0
            return traces, total
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving traces for groups: {str(e)}")
            raise

    async def get_state_events_by_job_id(
        self,
        job_id: str,
        event_types: List[str],
        limit: int = 15000,
    ) -> List[ExecutionTrace]:
        """Fetch ONLY the state-transition events (task/crew lifecycle) for a job.

        The crew-node-states / task-states endpoints derive a tiny state dict
        from these events; fetching the run's ENTIRE trace set (LLM/tool
        payload blobs included) per poll was the dominant cost. The filter is
        case-insensitive on event_type because writers differ in casing.

        Args:
            job_id: Job ID to filter by
            event_types: Event types to include (matched case-insensitively)
            limit: Safety cap on returned rows

        Returns:
            Matching ExecutionTrace rows in insertion (id) order
        """
        try:
            wanted = [e.lower() for e in event_types]
            stmt = (
                select(ExecutionTrace)
                .where(
                    ExecutionTrace.job_id == job_id,
                    func.lower(ExecutionTrace.event_type).in_(wanted),
                )
                .order_by(ExecutionTrace.id.asc())
                .limit(limit)
            )
            result = await self.session.execute(stmt)
            return result.scalars().all()
        except SQLAlchemyError as e:
            logger.error(
                f"Database error retrieving state events for job_id {job_id}: {str(e)}"
            )
            raise

    async def get_all_traces(
        self, limit: Optional[int] = None, offset: Optional[int] = 0
    ) -> Tuple[List[ExecutionTrace], int]:
        """
        Get all execution traces with pagination.

        Args:
            limit: Maximum number of traces to return
            offset: Number of traces to skip

        Returns:
            Tuple of (list of ExecutionTrace records, total count)
        """
        return await self._get_all_traces(limit, offset)

    async def get_execution_job_id_by_run_id(self, run_id: int) -> Optional[str]:
        """
        Get job_id for an execution by run_id.

        Args:
            run_id: Run ID to look up

        Returns:
            job_id if found, None otherwise
        """
        return await self._get_execution_job_id_by_run_id(run_id)

    async def get_execution_run_id_by_job_id(self, job_id: str) -> Optional[int]:
        """
        Get run_id for an execution by job_id.

        Args:
            job_id: Job ID to look up

        Returns:
            run_id if found, None otherwise
        """
        return await self._get_execution_run_id_by_job_id(job_id)

    async def delete_by_id(self, trace_id: int) -> int:
        """
        Delete an execution trace by ID.

        Args:
            trace_id: ID of the trace to delete

        Returns:
            Number of deleted records (0 or 1)
        """
        return await self._delete_by_id(trace_id)

    async def delete_by_run_id(self, run_id: int) -> int:
        """
        Delete all execution traces by run_id.

        Args:
            run_id: Run ID to filter by

        Returns:
            Number of deleted records
        """
        return await self._delete_by_run_id(run_id)

    async def delete_by_job_id(self, job_id: str) -> int:
        """
        Delete all execution traces by job_id.

        Args:
            job_id: Job ID to filter by

        Returns:
            Number of deleted records
        """
        return await self._delete_by_job_id(job_id)

    async def delete_all(self) -> int:
        """
        Delete all execution traces.

        Returns:
            Number of deleted records
        """
        return await self._delete_all()

    async def delete_older_than(self, cutoff: datetime) -> int:
        """
        Delete all execution traces older than a cutoff date.

        Args:
            cutoff: Delete records with created_at before this datetime

        Returns:
            Number of deleted records
        """
        try:
            stmt = delete(ExecutionTrace).where(ExecutionTrace.created_at < cutoff)
            result = await self.session.execute(stmt)
            await self.session.flush()
            return result.rowcount
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error(
                f"Database error deleting traces older than {cutoff}: {str(e)}"
            )
            raise

    async def get_max_id_for_job(self, job_id: str) -> int:
        """Highest trace id recorded for a job, or 0 if none exist.

        Used to seed the SSE trace poller's cursor when it starts tracking a
        job, so it doesn't re-broadcast traces the initial page load already
        fetched.
        """
        result = await self.session.execute(
            select(func.max(ExecutionTrace.id)).where(ExecutionTrace.job_id == job_id)
        )
        return result.scalar() or 0

    async def get_after_id(
        self, job_id: str, after_id: int, limit: int = 50
    ) -> List[ExecutionTrace]:
        """New traces for a job since a cursor, oldest first, capped.

        Deliberately its own method rather than reusing ``get_by_job_id``'s
        ``since_id`` parameter: that parameter is falsy-checked (``if
        since_id:``), so a cursor of exactly 0 would skip the filter and
        replay the job's entire trace history every poll interval. The SSE
        poller's cursor legitimately starts at 0, so the filter here is
        unconditional (``id > after_id``) instead.
        """
        stmt = (
            select(ExecutionTrace)
            .where(
                ExecutionTrace.job_id == job_id,
                ExecutionTrace.id > after_id,
            )
            .order_by(ExecutionTrace.id.asc())
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def has_completed_trace(
        self, job_id: str, event_type: str = "crew_completed"
    ) -> Tuple[bool, Optional[Any]]:
        """Whether a completion span exists for a job, and its output if so.

        Zombie-job recovery needs to distinguish "no crew_completed span yet"
        (still genuinely running — leave it alone) from "span exists but its
        output happens to be empty" (crew finished with nothing to report —
        still safe to mark COMPLETED). A plain ``Optional[output]`` return
        can't carry that distinction since both cases are falsy.

        Returns:
            (True, output) for the most recent event_type span, or
            (False, None) if the job has no such span at all.
        """
        result = await self.session.execute(
            select(ExecutionTrace.output)
            .where(
                ExecutionTrace.job_id == job_id,
                ExecutionTrace.event_type == event_type,
            )
            .order_by(ExecutionTrace.created_at.desc())
            .limit(1)
        )
        row = result.first()
        if row is None:
            return False, None
        return True, row[0]
