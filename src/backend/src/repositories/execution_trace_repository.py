"""
Repository for execution trace operations.

This module provides functions for CRUD operations on execution traces.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy import delete, update, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import SQLAlchemyError

from src.models.execution_trace import ExecutionTrace
from src.models.execution_history import ExecutionHistory
from src.core.logger import LoggerManager
from src.core.base_repository import BaseRepository

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
            _trace_id = getattr(trace, 'id', None)
            # Best-effort refresh; not strictly needed with expire_on_commit=False
            try:
                if getattr(trace, 'id', None) is None and _trace_id is not None:
                    # If PK wasn’t populated, set it from pre-commit value
                    trace.id = _trace_id
                else:
                    await self.session.refresh(trace)
            except Exception as refresh_err:
                logger.debug(f"Refresh after trace insert failed (non-fatal): {refresh_err}")
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
            logger.error(f"Database error retrieving execution trace {trace_id}: {str(e)}")
            raise
    
    async def _get_by_run_id(
        self,
        run_id: int,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
    ) -> List[ExecutionTrace]:
        """
        Get execution traces by run_id.

        Args:
            run_id: Run ID to filter by
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            List of ExecutionTrace records
        """
        try:
            stmt = select(ExecutionTrace).where(ExecutionTrace.run_id == run_id)
            
            if offset is not None:
                stmt = stmt.offset(offset)
            if limit is not None:
                stmt = stmt.limit(limit)
                
            result = await self.session.execute(stmt)
            return result.scalars().all()
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving traces for run_id {run_id}: {str(e)}")
            raise
    
    async def _get_by_job_id(
        self,
        job_id: str,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
    ) -> List[ExecutionTrace]:
        """
        Get execution traces by job_id.

        Args:
            job_id: Job ID to filter by
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            List of ExecutionTrace records
        """
        try:
            stmt = select(ExecutionTrace).where(ExecutionTrace.job_id == job_id)
            
            if offset is not None:
                stmt = stmt.offset(offset)
            if limit is not None:
                stmt = stmt.limit(limit)
                
            result = await self.session.execute(stmt)
            return result.scalars().all()
        except SQLAlchemyError as e:
            logger.error(f"Database error retrieving traces for job_id {job_id}: {str(e)}")
            raise
    
    async def _get_all_traces(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
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
    
    async def _get_execution_job_id_by_run_id(
        self,
        run_id: int
    ) -> Optional[str]:
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
            logger.error(f"Database error retrieving job_id for run_id {run_id}: {str(e)}")
            raise
    
    async def _get_execution_run_id_by_job_id(
        self,
        job_id: str
    ) -> Optional[int]:
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
            logger.error(f"Database error retrieving run_id for job_id {job_id}: {str(e)}")
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
            logger.error(f"Database error deleting traces for run_id {run_id}: {str(e)}")
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
            logger.error(f"Database error deleting traces for job_id {job_id}: {str(e)}")
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
    
    # Public methods that manage their own session lifecycle
    
    async def create(self, trace_data: Dict[str, Any]) -> ExecutionTrace:
        """
        Create a new execution trace record.

        Args:
            trace_data: Dictionary with trace data

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

        if job_id and not is_subprocess:
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
                raise ValueError(f"Job {job_id} does not exist in ExecutionHistory. Trace creation aborted.")
            else:
                # Job exists, ensure run_id is set in trace_data
                if "run_id" not in trace_data and job_exists:
                    trace_data["run_id"] = job_exists.id
                    logger.info(f"Setting run_id={job_exists.id} for existing job {job_id}")
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
        offset: Optional[int] = 0
    ) -> List[ExecutionTrace]:
        """
        Get execution traces by run_id.
        
        Args:
            run_id: Run ID to filter by
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            List of ExecutionTrace records
        """
        return await self._get_by_run_id(run_id, limit, offset)
    
    async def get_by_job_id(
        self, 
        job_id: str,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
    ) -> List[ExecutionTrace]:
        """
        Get execution traces by job_id.
        
        Args:
            job_id: Job ID to filter by
            limit: Maximum number of traces to return
            offset: Number of traces to skip
            
        Returns:
            List of ExecutionTrace records
        """
        return await self._get_by_job_id(job_id, limit, offset)
    
    async def get_all_traces(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = 0
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
            logger.error(f"Database error deleting traces older than {cutoff}: {str(e)}")
            raise

    async def get_crew_checkpoints_by_job_id(self, job_id: str) -> List[Dict[str, Any]]:
        """
        Get crew checkpoint information from traces for a specific job.

        This extracts crew completion events from traces to support
        granular checkpoint resume functionality.

        Args:
            job_id: Job ID to get crew checkpoints for

        Returns:
            List of crew checkpoint dicts with:
                - crew_name: Name of the crew
                - sequence: Order of execution (1-based)
                - status: 'completed' or 'failed'
                - output_preview: First 200 chars of output
                - completed_at: ISO timestamp
        """
        try:
            # Query for task_completed events (crews complete when their tasks complete)
            stmt = select(ExecutionTrace).where(
                ExecutionTrace.job_id == job_id,
                ExecutionTrace.event_type == "task_completed"
            ).order_by(ExecutionTrace.created_at)

            result = await self.session.execute(stmt)
            traces = result.scalars().all()

            # Extract unique crew completions from traces
            crew_checkpoints = []
            seen_crews = set()
            sequence = 0

            for trace in traces:
                # Get crew_name from trace_metadata (extra_data is saved directly as trace_metadata)
                # or from trace_metadata.extra_data (legacy format) or output.extra_data
                # Fallback to agent_role if crew_name not available (backward compatibility)
                crew_name = None

                if trace.trace_metadata and isinstance(trace.trace_metadata, dict):
                    # First check if crew_name is directly in trace_metadata (new format)
                    crew_name = trace.trace_metadata.get("crew_name")
                    # Fallback to extra_data.crew_name (legacy format)
                    if not crew_name:
                        extra_data = trace.trace_metadata.get("extra_data", {})
                        if isinstance(extra_data, dict):
                            crew_name = extra_data.get("crew_name")
                    # Fallback to agent_role (backward compatibility with traces before crew_name was added)
                    if not crew_name:
                        crew_name = trace.trace_metadata.get("agent_role")

                # Also check output if not found in trace_metadata
                if not crew_name and trace.output and isinstance(trace.output, dict):
                    crew_name = trace.output.get("crew_name")
                    if not crew_name:
                        extra_data = trace.output.get("extra_data", {})
                        if isinstance(extra_data, dict):
                            crew_name = extra_data.get("crew_name")

                if not crew_name:
                    continue

                # Only add each crew once (first completion = checkpoint)
                if crew_name in seen_crews:
                    continue

                seen_crews.add(crew_name)
                sequence += 1

                # Get output preview
                output_preview = ""
                if trace.output:
                    if isinstance(trace.output, dict):
                        output_preview = str(trace.output.get("output_content", ""))[:200]
                    else:
                        output_preview = str(trace.output)[:200]

                crew_checkpoints.append({
                    "crew_name": crew_name,
                    "sequence": sequence,
                    "status": "completed",
                    "output_preview": output_preview,
                    "completed_at": trace.created_at.isoformat() if trace.created_at else None
                })

            logger.info(f"Found {len(crew_checkpoints)} crew checkpoints for job {job_id}")
            if crew_checkpoints:
                for cp in crew_checkpoints:
                    logger.info(f"  Crew checkpoint: {cp['crew_name']} (sequence {cp['sequence']})")
            else:
                # Log trace info for debugging
                logger.info(f"No crew checkpoints found. Total task_completed traces: {len(traces)}")
                if traces:
                    sample_trace = traces[0]
                    logger.info(f"  Sample trace_metadata keys: {list(sample_trace.trace_metadata.keys()) if sample_trace.trace_metadata else 'None'}")
                    logger.info(f"  Sample trace_metadata: {sample_trace.trace_metadata}")
            return crew_checkpoints

        except SQLAlchemyError as e:
            logger.error(f"Database error getting crew checkpoints for job {job_id}: {str(e)}")
            return [] 

    async def get_crew_outputs_for_resume(self, job_id: str) -> Dict[str, Any]:
        """
        Get full crew outputs from traces for checkpoint resume.

        This returns a dictionary mapping crew names to their full outputs,
        which can be used to populate flow state when resuming from a checkpoint.

        Args:
            job_id: Job ID to get crew outputs for

        Returns:
            Dict mapping crew_name -> full output content
        """
        try:
            # Query for task_completed events (crews complete when their tasks complete)
            stmt = select(ExecutionTrace).where(
                ExecutionTrace.job_id == job_id,
                ExecutionTrace.event_type == "task_completed"
            ).order_by(ExecutionTrace.created_at)

            result = await self.session.execute(stmt)
            traces = result.scalars().all()

            # Extract crew outputs from traces
            crew_outputs = {}
            
            for trace in traces:
                # Get crew_name from trace_metadata
                crew_name = None

                if trace.trace_metadata and isinstance(trace.trace_metadata, dict):
                    crew_name = trace.trace_metadata.get("crew_name")
                    if not crew_name:
                        extra_data = trace.trace_metadata.get("extra_data", {})
                        if isinstance(extra_data, dict):
                            crew_name = extra_data.get("crew_name")
                    if not crew_name:
                        crew_name = trace.trace_metadata.get("agent_role")

                if not crew_name:
                    continue

                # Get the full output
                full_output = None
                if trace.output:
                    if isinstance(trace.output, dict):
                        # CRITICAL FIX: Try to get the actual output content
                        # First check for nested "content" key (from logging_callbacks format after trace_management processing)
                        # Then check for "output_content" or "raw" (legacy formats)
                        full_output = trace.output.get("content") or trace.output.get("output_content") or trace.output.get("raw") or trace.output
                    else:
                        # If output is a string (after trace_management processing), use it directly
                        full_output = trace.output

                # Store the output (last task's output for each crew)
                if crew_name and full_output:
                    crew_outputs[crew_name] = full_output
                    logger.debug(f"Stored output for crew '{crew_name}': {str(full_output)[:100]}...")

            logger.info(f"Retrieved outputs for {len(crew_outputs)} crews for resume: {list(crew_outputs.keys())}")
            return crew_outputs

        except SQLAlchemyError as e:
            logger.error(f"Database error getting crew outputs for job {job_id}: {str(e)}")
            return {}
