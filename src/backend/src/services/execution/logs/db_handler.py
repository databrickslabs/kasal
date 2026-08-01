"""
The bridge from python logging into the execution_logs table.

Runs inside the crew/flow subprocess, where the parent's async session is not
available — hence the sync path with an async fallback.
"""

import asyncio
import json
import logging
import os
import sys
import threading
import traceback
from datetime import UTC, datetime
from typing import Any, Optional

from src.services.execution.logs.context import _execution_context


class ExecutionLogsDatabaseHandler(logging.Handler):
    """
    Custom logging handler that writes logs directly to the execution_logs database.
    Uses synchronous database operations since we're in a subprocess.

    ⚠️ IMPORTANT: This handler is for execution_logs ONLY, not execution_trace!

    execution_logs vs execution_trace:
    - execution_logs: Raw logs from subprocess (crew.log, stdout, stderr)
                     Written by this handler from within the subprocess
                     Contains unstructured log messages

    - execution_trace: Structured events from CrewAI event bus
                      Written by the OTel bridge (see services/otel_tracing/)
                      Contains structured events like task_started, agent_execution
                      Uses the original GroupContext object

    DO NOT mix these two systems or try to unify their implementations!
    """

    def __init__(
        self, execution_id: str, group_context: Any = None, log_queue: Any = None
    ):
        """
        Initialize the handler.

        Args:
            execution_id: The execution ID for this handler
            group_context: Optional group context for multi-tenant isolation
            log_queue: Optional multiprocessing Queue for sending logs to main process
        """
        super().__init__()
        self.execution_id = execution_id
        self.group_context = group_context
        self.log_queue = log_queue  # Queue for sending logs to main process
        self._db_url = None

        # Debug log the initialization
        from src.core.logger import get_logger

        logger = get_logger("crew")
        logger.info(f"[DB_HANDLER] Initialized with execution_id={execution_id}")
        logger.info(f"[DB_HANDLER] log_queue provided: {log_queue is not None}")
        if group_context:
            logger.info(
                f"[DB_HANDLER] Group context provided - type: {type(group_context)}"
            )
            if hasattr(group_context, "__dict__"):
                logger.info(
                    f"[DB_HANDLER] Group context attributes: {group_context.__dict__}"
                )
        else:
            logger.warning(
                f"[DB_HANDLER] No group context provided for execution {execution_id}"
            )

        self._init_db()

    def _init_db(self):
        """Initialize database connection settings."""
        import os
        import pathlib

        from src.core.logger import get_logger

        logger = get_logger("crew")

        # Log environment variables for debugging
        db_type_env = os.environ.get("DATABASE_TYPE", "not set")
        logger.info(f"[DB_HANDLER] DATABASE_TYPE env var: {db_type_env}")

        # First check for DATABASE_URL environment variable
        self._db_url = os.environ.get("DATABASE_URL")

        if not self._db_url:
            # Derive the sync database URL from settings.DATABASE_URI.
            # This ensures we use the exact same connection string as the
            # rest of the application, avoiding mismatches when individual
            # fields (POSTGRES_SERVER, etc.) are not set but DATABASE_URI is.
            try:
                from src.config.settings import settings

                # settings.DATABASE_URI is the canonical source of truth,
                # already assembled from env vars or individual fields.
                # Strip async driver prefixes to get a sync-compatible URL
                # (_write_to_db_async re-adds +asyncpg when needed).
                db_uri = str(settings.DATABASE_URI)
                self._db_url = db_uri.replace("+asyncpg", "").replace("+aiosqlite", "")
                logger.info(
                    f"[DB_HANDLER] Using database URL derived from settings.DATABASE_URI"
                )

            except ImportError as e:
                # If settings cannot be imported, use SQLite fallback
                logger.warning(f"[DB_HANDLER] Could not import settings: {e}")
                backend_root = pathlib.Path(__file__).parent.parent.parent.parent
                db_path = backend_root / "app.db"
                self._db_url = f"sqlite:///{db_path.absolute()}"
                logger.info(f"[DB_HANDLER] Using SQLite fallback due to import error")
        else:
            # DATABASE_URL is set in environment
            logger.info(f"[DB_HANDLER] Using DATABASE_URL from environment")

    def emit(self, record):
        """
        Emit a log record by writing directly to the database.

        Args:
            record: The log record to emit
        """
        try:
            # Debug: Print that emit was called
            print(
                f"[DB_HANDLER EMIT] Called for {self.execution_id[:8]}, queue={self.log_queue is not None}"
            )

            # Skip database handler's own logs to avoid recursion
            if record.name == "crew" and "[DB_HANDLER]" in record.getMessage():
                return

            # Format the log message
            msg = self.format(record)

            # Write directly to database using synchronous operations
            self._write_to_db_sync(msg)
        except Exception as e:
            # Log errors to crew.log instead of database to avoid recursion
            import logging

            logger = logging.getLogger("crew")
            # Only log to file handler, not database handler
            for handler in logger.handlers[:]:
                if not isinstance(handler, ExecutionLogsDatabaseHandler):
                    handler.emit(
                        logging.LogRecord(
                            name="crew",
                            level=logging.ERROR,
                            pathname="",
                            lineno=0,
                            msg=f"[DB_HANDLER] Error writing to database: {str(e)}",
                            args=(),
                            exc_info=None,
                        )
                    )

    def _write_to_db_sync(self, content: str):
        """
        Write log to database or queue.
        If log_queue is available (subprocess mode), send to queue.
        Otherwise, write directly to database (main process mode).

        Args:
            content: The log content to write
        """
        try:
            import asyncio
            from datetime import datetime

            # Prepare the data
            log_data = {
                "execution_id": self.execution_id,
                "content": content,
                "timestamp": datetime.utcnow(),
            }

            # Add group context if available
            # ⚠️ WARNING: group_context handling is critical for execution_trace
            # DO NOT change how group_context is passed or accessed here
            # It MUST support both dict and object forms for compatibility
            if self.group_context:
                # Handle both dict and object forms of group_context
                if isinstance(self.group_context, dict):
                    # If it's a dict, get values directly
                    log_data["group_id"] = self.group_context.get(
                        "primary_group_id"
                    ) or self.group_context.get("group_id")
                    log_data["group_email"] = self.group_context.get("group_email")
                else:
                    # If it's an object (GroupContext), use getattr
                    # This is the normal case when execution_trace is working
                    log_data["group_id"] = getattr(
                        self.group_context, "primary_group_id", None
                    ) or getattr(self.group_context, "group_id", None)
                    log_data["group_email"] = getattr(
                        self.group_context, "group_email", None
                    )

                # Debug logging to verify group context
                if not log_data["group_id"] and not log_data["group_email"]:
                    import logging

                    debug_logger = logging.getLogger("crew")
                    debug_logger.warning(
                        f"[DB_HANDLER] Group context exists but values are None - group_context: {self.group_context}, type: {type(self.group_context)}"
                    )
                    if hasattr(self.group_context, "__dict__"):
                        debug_logger.warning(
                            f"[DB_HANDLER] Group context attributes: {self.group_context.__dict__}"
                        )
            else:
                log_data["group_id"] = None
                log_data["group_email"] = None

            # If we have a log_queue (subprocess mode), use it instead of direct DB write
            # This follows the same pattern as execution_trace - collect in subprocess, write in main
            if self.log_queue is not None:
                try:
                    # Put log data in queue for main process to write
                    self.log_queue.put_nowait(log_data)
                    print(
                        f"[SUBPROCESS LOG QUEUE] Added log to queue for {self.execution_id[:8]}, content: {log_data.get('content', '')[:50]}..."
                    )

                    # Debug log to confirm queue usage
                    from src.core.logger import get_logger

                    logger = get_logger("crew")
                    for handler in logger.handlers[:]:
                        if not isinstance(handler, ExecutionLogsDatabaseHandler):
                            handler.emit(
                                logging.LogRecord(
                                    name="crew",
                                    level=logging.DEBUG,
                                    pathname="",
                                    lineno=0,
                                    msg=f"[DB_HANDLER] Queued log for {self.execution_id[:8]} (queue size approx: {self.log_queue.qsize()})",
                                    args=(),
                                    exc_info=None,
                                )
                            )
                            break
                    return  # Exit early - main process will handle DB write
                except Exception as queue_error:
                    # If queue fails, fall back to direct DB write
                    from src.core.logger import get_logger

                    logger = get_logger("crew")
                    logger.warning(
                        f"[DB_HANDLER] Failed to queue log: {queue_error}, falling back to direct write"
                    )

            # Check if we're in SQLite or PostgreSQL mode
            if "sqlite" in self._db_url:
                # Use synchronous SQLite operations
                import sqlite3
                from urllib.parse import urlparse

                # Extract the database path from the URL
                parsed = urlparse(self._db_url)
                db_path = parsed.path.lstrip("/")

                # Connect and insert
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO execution_logs (execution_id, content, timestamp, group_id, group_email)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (
                        log_data["execution_id"],
                        log_data["content"],
                        log_data["timestamp"],
                        log_data["group_id"],
                        log_data["group_email"],
                    ),
                )
                conn.commit()

                # Debug: Log successful write (but avoid recursion)
                if "[DB_WRITE_SUCCESS]" not in log_data["content"]:
                    from src.core.logger import get_logger

                    file_logger = get_logger("crew")
                    for handler in file_logger.handlers[:]:
                        if not isinstance(handler, ExecutionLogsDatabaseHandler):
                            handler.emit(
                                logging.LogRecord(
                                    name="crew",
                                    level=logging.DEBUG,
                                    pathname="",
                                    lineno=0,
                                    msg=f"[DB_WRITE_SUCCESS] Written log to execution_logs for {log_data['execution_id'][:8]}",
                                    args=(),
                                    exc_info=None,
                                )
                            )
                            break

                conn.close()
            else:
                # For PostgreSQL, check if we're already in an async context
                try:
                    loop = asyncio.get_running_loop()
                    # We're already in an async context, create a task
                    task = loop.create_task(self._write_to_db_async(log_data))
                    # Don't wait for it to complete (fire and forget)
                except RuntimeError:
                    # No running loop, use asyncio.run()
                    asyncio.run(self._write_to_db_async(log_data))

        except Exception as e:
            # Log error to crew.log
            from src.core.logger import get_logger

            logger = get_logger("crew")
            # Find file handler only
            for handler in logger.handlers[:]:
                if not isinstance(handler, ExecutionLogsDatabaseHandler):
                    handler.emit(
                        logging.LogRecord(
                            name="crew",
                            level=logging.ERROR,
                            pathname="",
                            lineno=0,
                            msg=f"[DB_HANDLER] Database write error: {str(e)}",
                            args=(),
                            exc_info=None,
                        )
                    )

    async def _write_to_db_async(self, log_data: dict):
        """
        Async helper for PostgreSQL writes.

        Args:
            log_data: The log data dictionary to write
        """
        try:
            from sqlalchemy import text
            from sqlalchemy.ext.asyncio import create_async_engine

            # Convert the sync PostgreSQL URL to async
            async_url = self._db_url
            if "postgresql+pg8000" in async_url:
                async_url = async_url.replace("postgresql+pg8000", "postgresql+asyncpg")
            elif "postgresql://" in async_url:
                async_url = async_url.replace("postgresql://", "postgresql+asyncpg://")

            # Create async engine
            engine = create_async_engine(async_url)

            # Execute the insert
            async with engine.begin() as conn:
                await conn.execute(
                    text("""
                    INSERT INTO execution_logs (execution_id, content, timestamp, group_id, group_email)
                    VALUES (:execution_id, :content, :timestamp, :group_id, :group_email)
                """),
                    log_data,
                )

            await engine.dispose()
        except Exception as e:
            raise Exception(f"Async database write failed: {str(e)}")
