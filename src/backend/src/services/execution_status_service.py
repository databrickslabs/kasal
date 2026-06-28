"""
Execution Status Service.

This service manages execution status operations:
- Updating execution status in the database
- Retrieving execution status
"""

import logging
from typing import Dict, Any, Optional

from src.models.execution_status import ExecutionStatus
from src.repositories.execution_repository import ExecutionRepository
from src.utils.asyncio_utils import execute_db_operation_with_fresh_engine, execute_db_operation_smart
from src.core.sse_manager import sse_manager, SSEEvent

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

class ExecutionStatusService:
    """
    Service for managing execution status operations.
    """

    @staticmethod
    async def update_status(
        job_id: str,
        status: str,
        message: str,
        result: Any = None,
        session: AsyncSession | None = None,
        only_if_changed: bool = False
    ) -> bool:
        """
        Update the status of an execution in the database.

        Args:
            job_id: Execution ID (string UUID, maps to job_id field)
            status: New status string value
            message: Status message
            result: Optional result data
            only_if_changed: Skip the UPDATE/commit/SSE broadcast when the
                record already has this status (and no result payload was
                given). Used for idempotent transitions like the RUNNING
                write at execution start, where API-created records are
                already RUNNING but scheduler-created records start pending.

        Returns:
            True if successful, False otherwise
        """
        # Validate job_id
        if not job_id or not isinstance(job_id, str):
            logger.error(f"[ExecutionStatusService] Invalid job_id: {job_id}")
            return False

        try:
            # Define the database operation
            async def _update_operation(session):
                repo = ExecutionRepository(session)

                # Find the execution record by job_id (string UUID)
                logger.debug(f"[ExecutionStatusService] Finding execution by job_id: {job_id} to update status.")
                execution_record = await repo.get_execution_by_job_id(job_id=job_id)

                if not execution_record:
                    logger.error(f"[ExecutionStatusService] Execution record not found for job_id: {job_id}. Cannot update status.")
                    return False

                # Get the integer primary key (id) from the record
                record_id = execution_record.id
                logger.debug(f"[ExecutionStatusService] Found record_id: {record_id} for job_id: {job_id}. Preparing update data.")

                if (
                    only_if_changed
                    and result is None
                    and execution_record.status
                    and execution_record.status.upper() == status.upper()
                ):
                    logger.debug(
                        f"[ExecutionStatusService] Status for job_id {job_id} already {status}; skipping no-op update."
                    )
                    return True

                # Prepare complete update data with all fields
                update_data = {
                    "status": status,
                    "error": message  # Changed from "message" to "error" to match the database column
                }

                # Add result if provided - properly handle JSON serialization
                if result is not None:
                    logger.info(f"[ExecutionStatusService] Processing result of type {type(result)} for job_id: {job_id}")

                    # The result field is defined as JSON in the model
                    try:
                        # Check if we need to serialize to JSON
                        if isinstance(result, (dict, list)):
                            # For dict or list, store as is (SQLAlchemy handles JSON conversion)
                            stored_result = result
                        else:
                            # For other types, convert to string representation
                            stored_result = str(result)

                        # Predefined-UI runs end with an A2UI "UI document" as their
                        # final output, but weaker models wrap it in a prose preamble /
                        # a ```json fence / double-encoding, or emit mismatched or
                        # truncated brackets. Normalize it ONCE here — the single
                        # chokepoint every execution channel funnels result-writes
                        # through — so the persisted result is clean, canonical A2UI
                        # JSON instead of being salvaged per-render on every client.
                        # Content-gated: a non-A2UI result returns None and is stored
                        # verbatim, and any error leaves the result untouched.
                        try:
                            from src.engines.crewai.exporters.ui_document import (
                                normalize_ui_document,
                            )

                            canonical = normalize_ui_document(stored_result)
                            if canonical is not None:
                                stored_result = canonical
                        except Exception as norm_err:  # noqa: BLE001 — never affect persistence
                            logger.warning(
                                f"[ExecutionStatusService] UI-document normalization "
                                f"skipped for job_id {job_id}: {norm_err}"
                            )

                        update_data["result"] = stored_result
                        logger.info(f"[ExecutionStatusService] Successfully processed result for job_id: {job_id}")
                    except Exception as json_err:
                        logger.error(f"[ExecutionStatusService] Error processing result for job_id: {job_id}: {str(json_err)}")
                        # Still add the result as a string if JSON serialization fails
                        update_data["result"] = str(result)

                # Set completed_at if status is a terminal status
                if status in [ExecutionStatus.COMPLETED.value, ExecutionStatus.FAILED.value, ExecutionStatus.CANCELLED.value]:
                    from datetime import datetime
                    # Always set completed_at to current UTC time for terminal statuses
                    # Must use utcnow() to match created_at which also uses utcnow()
                    update_data["completed_at"] = datetime.utcnow()
                    logger.info(f"[ExecutionStatusService] Setting completed_at for terminal status {status} on job {job_id}")

                logger.info(f"[ExecutionStatusService] Update data keys: {', '.join(update_data.keys())}")

                # Call the repository update method using the integer record_id
                logger.debug(f"[ExecutionStatusService] Calling repo.update_execution for record_id: {record_id} with status: {status}")
                updated_execution = await repo.update_execution(
                    execution_id=record_id, # Use the integer ID here
                    data=update_data
                )

                # Explicitly flush and commit the session to catch potential DB errors early
                if updated_execution:
                    logger.debug(f"[ExecutionStatusService] Flushing session after updating record_id: {record_id} for job_id: {job_id}")
                    await session.flush() # Send the UPDATE to the DB
                    logger.debug(f"[ExecutionStatusService] Committing transaction after flushing update for record_id: {record_id}")
                    await session.commit() # Attempt to COMMIT the transaction
                    logger.info(f"[ExecutionStatusService] Successfully committed status update for job_id: {job_id} (record_id: {record_id}) to {status}.")

                    # Broadcast SSE event for real-time updates
                    # Skip SSE broadcast in subprocess mode - subprocess has its own SSE manager with no clients
                    import os
                    is_subprocess = os.environ.get('CREW_SUBPROCESS_MODE') == 'true'
                    if is_subprocess:
                        logger.debug(f"[ExecutionStatusService] Skipping SSE broadcast in subprocess mode for job_id={job_id}")
                    else:
                        try:
                            from datetime import datetime as dt
                            event_data = {
                                "job_id": job_id,
                                "status": status,
                                "message": message,
                                "updated_at": dt.now().isoformat(),  # Use current timestamp since model has no updated_at
                                "group_id": updated_execution.group_id  # Include group_id for filtering
                            }
                            if result is not None:
                                event_data["result"] = result
                            if status in [ExecutionStatus.COMPLETED.value, ExecutionStatus.FAILED.value, ExecutionStatus.CANCELLED.value]:
                                event_data["completed_at"] = updated_execution.completed_at.isoformat() if updated_execution.completed_at else None

                            event = SSEEvent(
                                data=event_data,
                                event="execution_update",
                                id=f"{job_id}_{status}_{record_id}"
                            )
                            await sse_manager.broadcast_to_job(job_id, event)
                            logger.debug(f"[ExecutionStatusService] Broadcasted SSE event for job_id: {job_id}")
                        except Exception as sse_error:
                            # Don't fail the update if SSE fails
                            logger.warning(f"[ExecutionStatusService] Failed to broadcast SSE event: {sse_error}")

                    return True
                else:
                    logger.error(f"[ExecutionStatusService] Failed to update execution for job_id: {job_id} (record_id: {record_id}). Update method returned None.")
                    # Rollback might be appropriate here if update returned None unexpectedly
                    await session.rollback()
                    return False

            # Execute the operation with a provided session if available; otherwise fall back.
            # Use execute_db_operation_smart so that when Lakebase is active the
            # execution_history record (which lives in Lakebase) can be found.
            if session is not None:
                return await _update_operation(session)
            return await execute_db_operation_smart(_update_operation)

        except Exception as e:
            logger.error(f"[ExecutionStatusService] Error during update/flush/commit for job_id {job_id}: {str(e)}", exc_info=True)
            return False

    @staticmethod
    async def update_mlflow_trace_id(
        job_id: str,
        trace_id: str,
        experiment_name: Optional[str] = None,
        session: AsyncSession | None = None
    ) -> bool:
        """
        Update the MLflow trace ID for an execution.

        Args:
            job_id: Execution ID (string UUID, maps to job_id field)
            trace_id: MLflow trace ID to store
            experiment_name: Optional MLflow experiment name

        Returns:
            True if successful, False otherwise
        """
        # Validate job_id and trace_id
        if not job_id or not isinstance(job_id, str):
            logger.error(f"[ExecutionStatusService] Invalid job_id: {job_id}")
            return False

        if not trace_id or not isinstance(trace_id, str):
            logger.error(f"[ExecutionStatusService] Invalid trace_id: {trace_id}")
            return False

        try:
            # Define the database operation
            async def _update_trace_operation(session):
                repo = ExecutionRepository(session)

                # Find the execution record by job_id
                logger.debug(f"[ExecutionStatusService] Finding execution by job_id: {job_id} to update MLflow trace ID.")
                execution_record = await repo.get_execution_by_job_id(job_id=job_id)

                if not execution_record:
                    logger.error(f"[ExecutionStatusService] Execution record not found for job_id: {job_id}. Cannot update MLflow trace ID.")
                    return False

                # Get the integer primary key (id) from the record
                record_id = execution_record.id
                logger.debug(f"[ExecutionStatusService] Found record_id: {record_id} for job_id: {job_id}. Updating MLflow trace ID.")

                # Prepare update data
                update_data = {
                    "mlflow_trace_id": trace_id
                }

                if experiment_name:
                    update_data["mlflow_experiment_name"] = experiment_name

                logger.info(f"[ExecutionStatusService] Updating MLflow trace ID {trace_id} for job_id: {job_id}")

                # Call the repository update method
                updated_execution = await repo.update_execution(
                    execution_id=record_id,
                    data=update_data
                )

                # Flush and commit
                if updated_execution:
                    await session.flush()
                    await session.commit()
                    logger.info(f"[ExecutionStatusService] Successfully updated MLflow trace ID for job_id: {job_id}")
                    return True
                else:
                    logger.error(f"[ExecutionStatusService] Failed to update MLflow trace ID for job_id: {job_id}")
                    await session.rollback()
                    return False

            # Execute the operation with a provided session if available; otherwise fall back
            if session is not None:
                return await _update_trace_operation(session)
            return await execute_db_operation_smart(_update_trace_operation)

        except Exception as e:
            logger.error(f"[ExecutionStatusService] Error updating MLflow trace ID for job_id {job_id}: {str(e)}", exc_info=True)
            return False

    @staticmethod
    async def update_mlflow_evaluation_run_id(
        session: AsyncSession,
        job_id: str,
        evaluation_run_id: str
    ) -> bool:
        """
        Update the MLflow evaluation run ID for an execution.

        Args:
            session: Async SQLAlchemy session (from router dependency)
            job_id: Execution ID (string UUID, maps to job_id field)
            evaluation_run_id: MLflow evaluation run ID to store

        Returns:
            True if successful, False otherwise
        """
        logger.info(f"[ExecutionStatusService] Updating MLflow evaluation run ID for job_id: {job_id}, evaluation_run_id: {evaluation_run_id}")

        try:
            # Use repository to find and update the execution by job_id
            repo = ExecutionRepository(session)
            execution_record = await repo.get_execution_by_job_id(job_id=job_id)

            if not execution_record:
                logger.warning(f"[ExecutionStatusService] No execution found with job_id: {job_id}")
                return False

            # Update the MLflow evaluation run ID using repository update
            record_id = execution_record.id
            await repo.update_execution(
                execution_id=record_id,
                data={"mlflow_evaluation_run_id": evaluation_run_id}
            )

            # Commit the changes
            await session.commit()
            logger.info(f"[ExecutionStatusService] Successfully updated MLflow evaluation run ID for job_id: {job_id}")
            return True

        except Exception as e:
            logger.error(f"[ExecutionStatusService] Error updating MLflow evaluation run ID for job_id {job_id}: {str(e)}", exc_info=True)
            return False

    @staticmethod
    async def get_status(execution_id: str, session: AsyncSession | None = None) -> Optional[Any]:
        """
        Get the status of an execution from the database.

        Args:
            execution_id: Execution ID

        Returns:
            Execution object or None if not found
        """
        # Validate execution_id
        if not execution_id or not isinstance(execution_id, str):
            logger.error(f"[ExecutionStatusService] Invalid execution_id: {execution_id}")
            return None

        try:
            # Define the database operation
            async def _get_operation(session):
                repo = ExecutionRepository(session)
                return await repo.get_execution_by_job_id(job_id=execution_id)

            # Execute the operation with a provided session if available; otherwise fall back
            if session is not None:
                return await _get_operation(session)
            return await execute_db_operation_smart(_get_operation)

        except Exception as e:
            logger.error(f"Error getting execution status: {str(e)}")
            return None

    @staticmethod
    async def create_execution(execution_data: Dict[str, Any], group_context=None, session: AsyncSession | None = None) -> bool:
        """
        Create a new execution record in the database with group context.

        Args:
            execution_data: Dictionary with execution data
            group_context: Group context for multi-group data isolation

        Returns:
            True if successful, False otherwise
        """
        from src.db.session import get_isolated_db_session
        from src.repositories.execution_repository import ExecutionRepository

        # Validate job_id
        job_id = execution_data.get('job_id')
        if not job_id or not isinstance(job_id, str):
            logger.error(f"[ExecutionStatusService] Invalid job_id in execution data: {job_id}")
            return False

        try:
            # Add group information to execution data if group context is provided
            if group_context:
                execution_data["group_id"] = group_context.primary_group_id
                execution_data["group_email"] = group_context.group_email
                logger.info(f"[ExecutionStatusService] Adding group context to execution: group_id={group_context.primary_group_id}, groups={group_context.group_ids}, email={group_context.group_email}")

            # Prefer using a provided session (router-injected). Fallback to internal factory for backward compatibility.
            if session is not None:
                repo = ExecutionRepository(session)

                # Check if record already exists (with group filtering if available)
                group_ids = group_context.group_ids if group_context else None
                existing = await repo.get_execution_by_job_id(job_id=job_id, group_ids=group_ids)
                if existing:
                    logger.info(f"[ExecutionStatusService] Execution record with job_id: {job_id} already exists, skipping creation")
                    return True

                # Create execution record
                logger.debug(f"[ExecutionStatusService] Creating execution record with job_id: {job_id}")
                await repo.create_execution(data=execution_data)

                # Explicitly commit transaction
                await session.commit()

                logger.info(f"[ExecutionStatusService] Successfully created execution record with job_id: {job_id}")

                # Broadcast SSE event so frontend shows the new job immediately
                await ExecutionStatusService._broadcast_execution_created(execution_data)

                return True
            else:
                # No caller-supplied session: write the parent row on a PRIVATE
                # connection (get_isolated_db_session = a dedicated NullPool engine
                # on SQLite). The previous request_scoped_session() rode the shared
                # StaticPool connection, so a concurrent run's commit/rollback — or
                # the request session being closed when the response returned, while
                # this still ran in a background task ("sqlite3.ProgrammingError:
                # Cannot operate on a closed database") — could silently discard the
                # INSERT. The crew subprocess then kept writing execution_logs on its
                # own connection, leaving an orphaned run (logs, no executionhistory
                # row) that every status poll 404s on forever. A private connection
                # commits this row independently and durably.
                async with get_isolated_db_session() as session:
                    # Create repository instance
                    repo = ExecutionRepository(session)

                    # Check if record already exists (with group filtering if available)
                    group_ids = group_context.group_ids if group_context else None
                    existing = await repo.get_execution_by_job_id(job_id=job_id, group_ids=group_ids)
                    if existing:
                        logger.info(f"[ExecutionStatusService] Execution record with job_id: {job_id} already exists, skipping creation")
                        return True

                    # Create execution record
                    logger.debug(f"[ExecutionStatusService] Creating execution record with job_id: {job_id}")
                    await repo.create_execution(data=execution_data)

                    # Explicitly commit transaction
                    await session.commit()

                    logger.info(f"[ExecutionStatusService] Successfully created execution record with job_id: {job_id}")

                    # Broadcast SSE event so frontend shows the new job immediately
                    await ExecutionStatusService._broadcast_execution_created(execution_data)

                    return True
        except Exception as e:
            logger.error(f"[ExecutionStatusService] Error creating execution record: {e}", exc_info=True)
            return False

    @staticmethod
    async def _broadcast_execution_created(execution_data: Dict[str, Any]) -> None:
        """Broadcast an SSE event when a new execution is created."""
        try:
            from datetime import datetime as dt

            job_id = execution_data.get("job_id", "")
            created_at = execution_data.get("created_at")
            if hasattr(created_at, "isoformat"):
                created_at = created_at.isoformat()
            elif not isinstance(created_at, str):
                created_at = dt.now().isoformat()

            event_data = {
                "job_id": job_id,
                "status": execution_data.get("status", "RUNNING"),
                "run_name": execution_data.get("run_name", ""),
                "execution_type": execution_data.get("execution_type", "crew"),
                "created_at": created_at,
                "updated_at": dt.now().isoformat(),
                "group_id": execution_data.get("group_id"),
                "planning": execution_data.get("planning", False),
            }

            event = SSEEvent(
                data=event_data,
                event="execution_update",
                id=f"{job_id}_created",
            )
            sent_count = await sse_manager.broadcast_to_job(job_id, event)
            logger.info(
                f"[ExecutionStatusService] Broadcasted execution_created SSE for job_id={job_id} to {sent_count} clients"
            )
        except Exception as sse_error:
            logger.warning(f"[ExecutionStatusService] Failed to broadcast execution_created SSE: {sse_error}")