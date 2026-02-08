"""Trace persistence mixin for writing execution traces to queue/database.

Provides _enqueue_trace, _write_trace_to_db_async, and _write_trace_to_db
methods that handle both main-process (queue) and subprocess (direct DB) modes.
"""

import json
import logging
import os
from datetime import datetime, timezone
from uuid import UUID

from src.services.execution_logs_queue import enqueue_log

logger = logging.getLogger(__name__)


class TracePersistenceMixin:
    """Mixin providing trace enqueueing and database persistence.

    Requires the consuming class to have:
        - self.job_id: str
        - self.group_context: optional group context
        - self._queue: trace queue instance
        - self._init_time: datetime
        - self._task_event_queue: optional multiprocessing.Queue
    """

    def _enqueue_trace(
        self,
        event_source: str,
        event_context: str,
        event_type: str,
        output_content: str,
        extra_data: dict = None,
    ) -> None:
        """Enqueue trace data for asynchronous processing.

        Creates a structured trace entry and either enqueues it to the shared
        queue (main process) or writes directly to database (subprocess).

        Args:
            event_source: Source of the event (agent name, "crew", etc.)
            event_context: Context information (task name, tool name, etc.)
            event_type: Type of event ("agent_execution", "tool_usage", etc.)
            output_content: The actual output/content from the event
            extra_data: Optional dictionary with additional metadata
        """
        log_prefix = f"[AgentTraceEventListener][{self.job_id}]"
        try:
            timestamp = datetime.now(timezone.utc)
            time_since_init = (timestamp - self._init_time).total_seconds()

            logger.debug(
                f"[TRACE_DEBUG] _enqueue_trace called: event_type={event_type}, source={event_source}"
            )

            logger.debug(
                f"{log_prefix} Enqueuing {event_type} trace | Source: {event_source} | Context: {event_context[:30] if event_context else 'None'}..."
            )

            # Create trace data for database/queue
            trace_data = {
                "job_id": self.job_id,
                "event_source": event_source,
                "event_context": event_context,
                "event_type": event_type,
                "created_at": timestamp.replace(tzinfo=None),
                "output": {
                    "content": output_content,
                    "time_since_init": time_since_init,
                    "extra_data": extra_data or {},
                },
                "trace_metadata": extra_data or {},
            }

            # Add group context if available
            if self.group_context:
                trace_data["group_id"] = self.group_context.primary_group_id
                trace_data["group_email"] = self.group_context.group_email

            is_subprocess = os.environ.get("CREW_SUBPROCESS_MODE") == "true"

            if is_subprocess:
                logger.debug(
                    f"[TRACE_DEBUG] Subprocess mode detected, writing directly to database"
                )
                logger.debug(
                    f"{log_prefix} Writing trace directly to database (subprocess mode)"
                )
                self._write_trace_to_db_async(trace_data)

                # Relay task lifecycle events to main process for SSE broadcasting
                if self._task_event_queue and event_type in (
                    "task_started", "task_completed", "task_failed"
                ):
                    try:
                        self._task_event_queue.put_nowait(trace_data)
                        logger.info(
                            f"{log_prefix} Relayed {event_type} to main process via task_event_queue"
                        )
                    except Exception as relay_err:
                        logger.warning(
                            f"{log_prefix} Failed to relay {event_type} to main process: {relay_err}"
                        )
            else:
                logger.debug(f"[TRACE_DEBUG] Main process mode, putting trace on queue")
                logger.debug(
                    f"[TRACE_DEBUG] Queue before put: size={self._queue.qsize()}"
                )
                logger.debug(f"{log_prefix} Putting trace on queue (main process mode)")
                self._queue.put(trace_data)
                logger.debug(
                    f"[TRACE_DEBUG] Queue after put: size={self._queue.qsize()}"
                )

                # Also enqueue to job output queue for real-time streaming
                enqueue_log(
                    self.job_id,
                    {"content": output_content, "timestamp": timestamp.isoformat()},
                )

        except Exception as e:
            logger.error(f"{log_prefix} Error in _enqueue_trace: {e}", exc_info=True)

    def _write_trace_to_db_async(self, trace_data: dict) -> None:
        """Write trace to PostgreSQL database using async operations.

        Handles asynchronous database writes in subprocess mode, creating
        a new event loop and thread to avoid blocking the main execution.

        Args:
            trace_data: Dictionary containing trace information to persist
        """
        log_prefix = f"[AgentTraceEventListener][{self.job_id}]"
        try:
            import asyncio
            from concurrent.futures import ThreadPoolExecutor

            logger.info(
                f"{log_prefix} Writing trace to PostgreSQL - Event: {trace_data.get('event_type')} | Source: {trace_data.get('event_source')}"
            )

            class UUIDEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, UUID):
                        return str(obj)
                    return super().default(obj)

            async def write_async():
                try:
                    from src.services.execution_trace_service import ExecutionTraceService
                    from src.db.database_router import get_smart_db_session

                    output_data = trace_data.get("output", {})
                    if output_data:
                        cleaned_output = json.loads(
                            json.dumps(output_data, cls=UUIDEncoder)
                        )
                    else:
                        cleaned_output = {}

                    if isinstance(cleaned_output, dict):
                        extra_data = cleaned_output.get("extra_data", {})
                    else:
                        extra_data = {}

                    async for session in get_smart_db_session():
                        trace_service = ExecutionTraceService(session)
                        await trace_service.create_trace(
                            {
                                "job_id": trace_data.get("job_id"),
                                "event_source": trace_data.get("event_source"),
                                "event_context": trace_data.get("event_context"),
                                "event_type": trace_data.get("event_type"),
                                "output": cleaned_output,
                                "trace_metadata": extra_data,
                                "group_id": trace_data.get("group_id"),
                                "group_email": trace_data.get("group_email"),
                            }
                        )
                    logger.info(
                        f"{log_prefix} Trace written to PostgreSQL successfully"
                    )
                    return True
                except ValueError as e:
                    logger.debug(f"{log_prefix} Trace skipped (job doesn't exist): {e}")
                    return False
                except Exception as e:
                    logger.error(
                        f"{log_prefix} Failed to write trace to PostgreSQL: {e}",
                        exc_info=True,
                    )
                    return False

            def run_in_thread():
                logger.info(f"{log_prefix} Running async write in thread")
                result = asyncio.run(write_async())
                if result:
                    logger.info(f"{log_prefix} Thread completed successfully")
                else:
                    logger.error(f"{log_prefix} Thread completed with errors")
                return result

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(run_in_thread)
                logger.info(f"{log_prefix} Submitted async write to thread pool")

        except Exception as e:
            logger.error(
                f"{log_prefix} Error in _write_trace_to_db_async: {e}", exc_info=True
            )

    def _write_trace_to_db(self, trace_data: dict) -> None:
        """Write trace directly to database (for subprocess mode).

        Synchronous database write operation used when running in subprocess
        mode where queue sharing is not available.

        Args:
            trace_data: Dictionary containing trace information to persist
        """
        log_prefix = f"[AgentTraceEventListener][{self.job_id}]"
        try:
            from src.models.execution_trace import ExecutionTrace

            logger.debug(f"{log_prefix} Writing trace to database using SessionLocal")

            from src.db.session import async_session_factory
            import asyncio

            async def write_trace_async():
                async with async_session_factory() as db:
                    try:
                        trace = ExecutionTrace(**trace_data)
                        db.add(trace)
                        await db.commit()
                        logger.info(
                            f"{log_prefix} Trace written to database successfully"
                        )
                    except Exception as e:
                        await db.rollback()
                        logger.error(f"{log_prefix} Database error: {e}", exc_info=True)
                        raise

            try:
                asyncio.run(write_trace_async())
            except Exception as e:
                logger.error(f"{log_prefix} Failed to write trace: {e}")

                if "no such table" in str(e).lower():
                    logger.warning(
                        f"{log_prefix} Table execution_trace doesn't exist, attempting to create it..."
                    )
                    try:
                        from src.models.execution_trace import Base
                        from sqlalchemy import inspect

                        if "sqlite" in str(db.bind.url):
                            inspector = inspect(db.bind)
                            if "execution_trace" not in inspector.get_table_names():
                                Base.metadata.tables["execution_trace"].create(db.bind)
                                logger.info(
                                    f"{log_prefix} Created execution_trace table in SQLite"
                                )

                                trace = ExecutionTrace(**trace_data)
                                db.add(trace)
                                db.commit()
                                logger.info(
                                    f"{log_prefix} Trace written after creating table"
                                )
                    except Exception as create_error:
                        logger.error(
                            f"{log_prefix} Failed to create table: {create_error}",
                            exc_info=True,
                        )
                raise
            finally:
                db.close()

        except Exception as e:
            logger.error(
                f"{log_prefix} Error writing trace to database: {e}", exc_info=True
            )
