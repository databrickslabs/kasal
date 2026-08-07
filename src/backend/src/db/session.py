import asyncio
import logging
import os
import re
import sys
import time
from contextlib import asynccontextmanager
from contextvars import Context, ContextVar, copy_context
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import AsyncGenerator, Generator, Optional

from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.config.settings import settings
from src.core.logger import LoggerManager
from src.db.base import Base

# SQL identifier validation to prevent injection in dynamic SQL
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, kind: str = "identifier") -> str:
    """Validate a SQL identifier against a safe pattern to prevent injection."""
    if not name or not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL {kind}: {name!r}")
    return name


# Configure logging using LoggerManager
logger_manager = LoggerManager.get_instance()
if not logger_manager._initialized or not logger_manager._log_dir:
    # Initialize with environment variable if available
    log_dir = os.environ.get("LOG_DIR")
    if log_dir:
        logger_manager.initialize(log_dir)
    else:
        logger_manager.initialize()

# Get module logger
logger = logging.getLogger(__name__)

# Check if SQL debugging is enabled via environment variable or debug all
SQL_DEBUG = (
    os.environ.get("SQL_DEBUG", "false").lower() == "true"
    or os.environ.get("KASAL_DEBUG_ALL", "false").lower() == "true"
    or os.environ.get("KASAL_LOG_DATABASE", "").upper() == "DEBUG"
)
if SQL_DEBUG:
    logger.warning("=" * 80)
    logger.warning("SQL_DEBUG is ENABLED - All SQL queries will be logged!")
    logger.warning("This WILL impact performance. Disable when done debugging.")
    logger.warning("To disable: unset SQL_DEBUG or export SQL_DEBUG=false")
    logger.warning("=" * 80)


# Database retry decorator for handling SQLite locks
def retry_db_operation(max_retries: int = 3, delay: float = 0.1, backoff: float = 2.0):
    """Decorator to retry database operations when SQLite is locked."""

    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except OperationalError as e:
                    last_exception = e
                    if (
                        "database is locked" in str(e).lower()
                        and attempt < max_retries - 1
                    ):
                        wait_time = delay * (backoff**attempt)
                        logger.warning(
                            f"Database locked, retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    raise
                except Exception as e:
                    # For non-lock related errors, don't retry
                    raise
            raise last_exception

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except OperationalError as e:
                    last_exception = e
                    if (
                        "database is locked" in str(e).lower()
                        and attempt < max_retries - 1
                    ):
                        wait_time = delay * (backoff**attempt)
                        logger.warning(
                            f"Database locked, retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait_time)
                        continue
                    raise
                except Exception as e:
                    # For non-lock related errors, don't retry
                    raise
            raise last_exception

        # Return the appropriate wrapper based on whether the function is async
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


# Create a SQLAlchemy logger using the LoggerManager
class SQLAlchemyLogger:
    def __init__(self):
        self.formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.log_dir = logger_manager._log_dir
        self.setup_logger()

    def setup_logger(self):
        # Create sqlalchemy.log file handler
        sqlalchemy_log_file = self.log_dir / "sqlalchemy.log"

        # Get the sqlalchemy engine logger
        engine_logger = logging.getLogger("sqlalchemy.engine")

        # When SQL_DEBUG is enabled, we want both console and file output
        if SQL_DEBUG:
            engine_logger.setLevel(logging.INFO)
            # Clear existing handlers to avoid duplicates
            engine_logger.handlers = []
            engine_logger.propagate = False

            # Add console handler for immediate visibility
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter("[SQL] %(message)s"))
            engine_logger.addHandler(console_handler)

            # Also add file handler for persistent logging
            file_handler = logging.handlers.RotatingFileHandler(
                sqlalchemy_log_file,
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5,
                encoding="utf-8",
            )
            file_handler.setFormatter(self.formatter)
            engine_logger.addHandler(file_handler)
        else:
            # In non-debug mode, respect centralized configuration
            # but ensure file handler exists
            engine_logger.propagate = False

        # Ensure handlers are set up properly
        if not engine_logger.handlers:
            # Create file handler if not already configured elsewhere
            file_handler = logging.handlers.RotatingFileHandler(
                sqlalchemy_log_file,
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5,
                encoding="utf-8",
            )
            file_handler.setFormatter(self.formatter)
            engine_logger.addHandler(file_handler)

        # Log that the database logger has been configured
        logger.info(f"SQLAlchemy logs will be written to {sqlalchemy_log_file}")
        if SQL_DEBUG:
            logger.info("SQL_DEBUG enabled: SQL queries will also be shown in console")


# Initialize SQLAlchemy logging
sql_logger = SQLAlchemyLogger()

# Import pool classes
from sqlalchemy.pool import AsyncAdaptedQueuePool, NullPool, StaticPool

# Determine if we should use NullPool for event loop isolation
# This is necessary when running in environments with multiple event loops
# such as with CrewAI memory backends or during testing
use_nullpool = os.environ.get("USE_NULLPOOL", "false").lower() == "true"

# Track the main event loop for intelligent engine selection
main_event_loop = None


def set_main_event_loop():
    """
    Capture the main event loop when the FastAPI app starts.
    This should be called from the lifespan/startup event.
    """
    global main_event_loop
    try:
        main_event_loop = asyncio.get_running_loop()
        logger.info(f"Main event loop captured: {id(main_event_loop)}")
    except RuntimeError:
        logger.warning("Failed to capture main event loop")


# Try to detect early if we're in an async context
try:
    main_event_loop = asyncio.get_running_loop()
    logger.info(f"Main event loop detected during import: {id(main_event_loop)}")
except RuntimeError:
    # No event loop running yet - will be set later by set_main_event_loop()
    logger.info("No event loop during module import - will capture during app startup")


# Determine isolation level based on database type
def get_isolation_level(database_uri: str) -> str:
    """Get appropriate isolation level based on database type."""
    if database_uri.startswith("sqlite"):
        # SQLite with SQLAlchemy: Use None for autocommit behavior with async
        # This allows SQLite to handle transactions automatically
        return None
    else:
        # PostgreSQL supports: READ COMMITTED, READ UNCOMMITTED, REPEATABLE READ, SERIALIZABLE
        return "READ COMMITTED"


def get_sqlite_connect_args(database_uri: str) -> dict:
    """Get SQLite-specific connection arguments for better concurrent access."""
    if database_uri.startswith("sqlite"):
        return {
            "check_same_thread": False,  # Allow SQLite to be used across threads
            "timeout": 60,  # Increase to 60 seconds for heavy operations
            # Note: isolation_level is set at engine level, not in connect_args for SQLite
        }
    return {}


def get_sqlite_poolclass():
    """Pool class for the SQLite engine: StaticPool.

    NullPool opens a FRESH aiosqlite connection per checkout, so every
    concurrent writer (API loop, trace/logs writer, OTel exporter thread,
    flow/crew subprocess) gets its own connection. On SQLite — a single-writer
    database — those connections contend for the one write lock and, once write
    load grows (memory/chat-session writes, multi-crew HITL flow resumes),
    callers exceed busy_timeout and fail with "database is locked".

    StaticPool shares ONE connection so writes serialize through it, which
    eliminates that contention. WAL mode + busy_timeout (configure_sqlite) plus
    check_same_thread=False keep the shared connection safe across threads. This
    reverts the StaticPool->NullPool switch from 205b5f57; that switch was made
    to avoid MissingGreenlet / "Cannot operate on a closed database" at
    cross-loop teardown, so watch for those if connection lifetimes change.
    """
    return StaticPool


isolation_level = get_isolation_level(str(settings.DATABASE_URI))
connect_args = get_sqlite_connect_args(str(settings.DATABASE_URI))

# Create intelligent dual-engine setup for optimal performance
# Strategy: Use pooled connections for main app, NullPool for background tasks

if str(settings.DATABASE_URI).startswith("sqlite"):
    # SQLite: StaticPool — ONE shared aiosqlite connection so concurrent writers
    # serialize through it instead of each opening its own connection and
    # contending for SQLite's single write lock ("database is locked"). WAL mode
    # + busy_timeout (configure_sqlite, applied via the "connect" event) and
    # check_same_thread=False keep the shared connection safe across threads.
    # See get_sqlite_poolclass() for the NullPool trade-off (205b5f57).
    logger.info(
        "SQLite detected - using StaticPool (single shared connection) to "
        "serialize writes and avoid 'database is locked'"
    )
    engine = create_async_engine(
        str(settings.DATABASE_URI),
        echo=SQL_DEBUG,
        future=True,
        poolclass=get_sqlite_poolclass(),
        connect_args={
            **connect_args,
            "check_same_thread": False,
        },
    )
    # For SQLite, both engines are the same
    pooled_engine = engine
    nullpool_engine = engine

else:
    # PostgreSQL: Create TWO engines for different contexts
    logger.info("=" * 80)
    logger.info(
        "PostgreSQL detected - creating dual-engine setup for optimal performance"
    )
    logger.info(
        "Main app will use pooled connections, background tasks will use NullPool"
    )
    logger.info("=" * 80)

    # 1. Create POOLED engine for main FastAPI application (best performance)
    pooled_engine_opts = {
        "echo": SQL_DEBUG,
        "future": True,
        "poolclass": AsyncAdaptedQueuePool,
        "pool_size": 20,  # Keep 20 connections ready for web requests
        "max_overflow": 10,  # Allow 10 more during peak
        "pool_pre_ping": True,  # Check connection health
        "pool_recycle": 3600,  # Recycle after 1 hour
        "echo_pool": SQL_DEBUG,
        "connect_args": connect_args,
        "isolation_level": isolation_level,
    }
    pooled_engine = create_async_engine(
        str(settings.DATABASE_URI), **pooled_engine_opts
    )
    logger.info("Created pooled engine for main FastAPI app (20x better performance)")

    # 2. Create NULLPOOL engine for background tasks/CrewAI (event loop isolation)
    nullpool_engine_opts = {
        "echo": SQL_DEBUG,
        "future": True,
        "poolclass": NullPool,  # No pooling - new connection per query
        "connect_args": connect_args,
        "isolation_level": isolation_level,
    }
    nullpool_engine = create_async_engine(
        str(settings.DATABASE_URI), **nullpool_engine_opts
    )
    logger.info("Created NullPool engine for background tasks (CrewAI compatibility)")

    # Default engine selection
    # When USE_NULLPOOL=true, default to NullPool to avoid cross-loop issues under reload
    # get_db() still routes to pooled sessions where safe (main app loop) when USE_NULLPOOL=false
    if use_nullpool:
        logger.info(
            "USE_NULLPOOL=true - Event loop isolation mode enabled (defaulting to NullPool engine)"
        )
        engine = nullpool_engine
    else:
        logger.info(
            "USE_NULLPOOL=false - Full pooling mode enabled for maximum performance"
        )
        engine = pooled_engine

# Configure SQLite for better concurrent access.
# With NullPool this hook fires for EVERY new connection (i.e. constantly), so
# the success line logs at INFO only once and DEBUG afterwards.
_sqlite_configured_logged = False


def configure_sqlite(dbapi_connection, connection_record):
    """Configure SQLite connection for better performance and concurrency."""
    global _sqlite_configured_logged
    if str(settings.DATABASE_URI).startswith("sqlite"):
        try:
            # Set busy timeout - CRITICAL for handling locks (20 seconds based on best practices)
            dbapi_connection.execute("PRAGMA busy_timeout=20000")
            # Enable foreign keys
            dbapi_connection.execute("PRAGMA foreign_keys=ON")
            # Use WAL mode for better concurrency - MOST IMPORTANT optimization
            dbapi_connection.execute("PRAGMA journal_mode=WAL")
            # Optimize for faster writes with NORMAL synchronous (safe with WAL)
            dbapi_connection.execute("PRAGMA synchronous=NORMAL")
            # Increase cache for better performance
            dbapi_connection.execute("PRAGMA cache_size=-64000")  # 64MB cache
            # Store temp tables in memory to reduce disk I/O
            dbapi_connection.execute("PRAGMA temp_store=MEMORY")
            # Enable memory-mapped I/O for better performance
            dbapi_connection.execute("PRAGMA mmap_size=268435456")  # 256MB mmap
            # Auto checkpoint at 1000 pages to balance performance and WAL size
            dbapi_connection.execute("PRAGMA wal_autocheckpoint=1000")
            # Optimize page size for better performance
            dbapi_connection.execute("PRAGMA page_size=4096")
            if not _sqlite_configured_logged:
                logger.info(
                    "SQLite configured with WAL mode and optimizations (per-connection; further lines at DEBUG)"
                )
                _sqlite_configured_logged = True
            else:
                logger.debug("SQLite connection configured (WAL + optimizations)")
        except Exception as e:
            logger.error(f"Failed to configure SQLite connection: {e}")


# Apply SQLite configuration to all engines
if str(settings.DATABASE_URI).startswith("sqlite"):
    # For async engine, we need to listen to the sync_engine property
    event.listen(engine.sync_engine, "connect", configure_sqlite)
    logger.info("Applied SQLite configuration event listener to main engine")

    # Also apply to pooled and nullpool engines if they exist (PostgreSQL only)
    if "pooled_engine" in locals():
        event.listen(pooled_engine.sync_engine, "connect", configure_sqlite)
        logger.info("Applied SQLite configuration to pooled engine")
    if "nullpool_engine" in locals():
        event.listen(nullpool_engine.sync_engine, "connect", configure_sqlite)
        logger.info("Applied SQLite configuration to nullpool engine")

# Sync engine removed - everything is async now
# All database operations must use async sessions

# Create session factories for both engines (avoid recreating on each request)
# Main session factory (uses default engine based on USE_NULLPOOL setting)
_local_session_factory = async_sessionmaker(
    engine,
    expire_on_commit=False,
    autoflush=False,  # Disable autoflush to prevent SQLite locking issues
    autocommit=False,  # Explicit transaction control
)


class _SwappableSessionFactory:
    """Wrapper around async_sessionmaker that can be hot-swapped to Lakebase.

    Every module that does ``from src.db.session import async_session_factory``
    receives a reference to the **same** mutable instance.  Calling
    ``async_session_factory.activate_lakebase(lakebase_sessionmaker)`` replaces
    the underlying factory so that *all* existing callers automatically start
    producing Lakebase sessions — zero call-site changes required.
    """

    def __init__(self, default_factory):
        self._factory = default_factory
        self._is_lakebase = False
        self._on_swap_callbacks = []

    # --- async_sessionmaker-compatible interface ---

    def __call__(self):
        """Return a new AsyncSession (same as async_sessionmaker.__call__)."""
        return self._factory()

    # --- swap-invalidation hooks ---

    def register_on_swap(self, callback):
        """Register a zero-arg callback fired whenever the active DB is swapped
        (Lakebase activate/deactivate). Use it to flush in-memory caches keyed to
        the OLD database — e.g. ExecutionService's execution registry — so a
        status lookup after a swap doesn't serve/poll rows that only existed in
        the previous DB (the 'Execution not found' 404 storm)."""
        self._on_swap_callbacks.append(callback)

    def _fire_swap_callbacks(self):
        for cb in self._on_swap_callbacks:
            try:
                cb()
            except Exception as e:
                logger.warning(f"[SESSION FACTORY] on-swap cache hook failed: {e}")

    # --- hot-swap API ---

    def activate_lakebase(self, lakebase_factory):
        """Replace the underlying factory with a Lakebase-backed one."""
        changed = not self._is_lakebase
        self._factory = lakebase_factory
        self._is_lakebase = True
        logger.info("[SESSION FACTORY] Swapped to Lakebase engine")
        # Only on an actual transition — activate_lakebase can be called more
        # than once (startup + per-request router); firing every time would
        # wipe live caches needlessly.
        if changed:
            self._fire_swap_callbacks()

    def deactivate_lakebase(self):
        """Revert to the local (SQLite / PG) factory."""
        changed = self._is_lakebase
        self._factory = _local_session_factory
        self._is_lakebase = False
        logger.info("[SESSION FACTORY] Reverted to local engine")
        if changed:
            self._fire_swap_callbacks()

    @property
    def is_lakebase(self) -> bool:
        return self._is_lakebase


async_session_factory = _SwappableSessionFactory(_local_session_factory)

# ContextVar holding the current request-scoped session (set by DI providers)
_request_session: ContextVar[Optional[AsyncSession]] = ContextVar(
    "_request_session", default=None
)


def _usable_for_more_sql(session: AsyncSession) -> bool:
    """Whether ``session`` can still take a query, i.e. is not mid-commit.

    Reusing the request session is the whole point of branch 1, but a session
    COMMITTING is briefly in ``PREPARED`` state, and SQLAlchemy refuses further SQL
    on it: "This session is in 'prepared' state; no further SQL can be emitted
    within this transaction."

    That is reachable because ``commit()`` -> ``_prepare_impl()`` -> ``flush()``, and
    a flush can run application code that reads the database again — the light-agent
    path did exactly this, committing a terminal run status and then building its
    embedder config, whose API-key lookup landed back on the same session. The read
    failed, the Databricks key read as absent, embeddings fell back to a local Ollama
    that was not running, and memory silently saved with no vector.

    Falling through to a NEW session is correct here: a nested read that cannot join
    the in-flight transaction is better served by its own, and it sees the committed
    state either way.
    """
    from sqlalchemy.orm.session import SessionTransactionState

    try:
        transaction = session.sync_session._transaction
    except Exception:  # noqa: BLE001 - a test double, or no sync_session
        return True
    if transaction is None:
        return True
    state = getattr(transaction, "_state", None)
    # Only REFUSE on a state we positively recognise as unusable. A MagicMock's
    # attribute access yields another Mock, so an `is ACTIVE` test would reject every
    # test double and every future SQLAlchemy state name.
    return state not in (
        SessionTransactionState.PREPARED,
        SessionTransactionState.COMMITTED,
        SessionTransactionState.CLOSED,
        SessionTransactionState.DEACTIVE,
    )


@asynccontextmanager
async def routed_scoped_session():
    """The one way to get a session outside an HTTP request.

    Three branches, in order:

    1. **Inside a request** — yield THAT session, so the caller joins the single
       request transaction rather than opening a competing one.
    2. **Already resolving auth** — use the raw factory. This is the recursion
       break; see below.
    3. **Otherwise** — go through the database ROUTER, which re-reads
       ``is_lakebase_enabled()`` on every call.

    Branch 3 is the point. ``async_session_factory`` is a per-process SNAPSHOT that
    only a SUBPROCESS ever swaps to Lakebase (``activate_lakebase_in_subprocess``);
    the main process never does. So anything running IN-PROCESS outside a request —
    a FastAPI ``BackgroundTask``, which is where the whole Chat path runs — read
    local SQLite while the crew and flow subprocesses read Lakebase. Same config,
    opposite answers, no error: an MCP server enabled for the workspace produced
    "Added 1 explicit MCP servers" in Agent Builder and "Added 0" in Chat.

    Branch 2 is why this helper cannot simply always route. The router needs a
    credential to reach Lakebase and resolves one via ``get_auth_context`` →
    ``ApiKeysService`` — itself a caller of this function. Routing that read closes
    the loop, which the deployed app logged 1,287 times as "maximum recursion depth
    exceeded", killing every crew and flow subprocess. ``_RESOLVING_AUTH`` is set
    for the duration of an auth resolution (by ``get_auth_context`` on its
    outermost entry, and by the router before it calls auth), and it is a
    ContextVar rather than a bool so one task's lookup cannot disable routing for
    another's. The bootstrap read wants the local database anyway: the Lakebase
    config row lives there by design.

    This replaced ``request_scoped_session``, which had branch 1 and then went
    straight to the snapshot — unconditionally, whether or not auth was involved.
    That looked safe (the name says "request-scoped") and was the same silent split
    as the raw factory in 33 of its 37 call sites.
    """
    existing = _request_session.get(None)
    if existing is not None and _usable_for_more_sql(existing):
        yield existing
        return

    # Imported here, not at module scope: both modules import THIS one.
    from src.utils.databricks_auth import _RESOLVING_AUTH

    if _RESOLVING_AUTH.get():
        async with async_session_factory() as session:
            yield session
        return

    from src.db.database_router import get_smart_db_session

    async for session in get_smart_db_session():
        yield session
        break


def detach_request_session() -> None:
    """Clear the request-scoped session for the CURRENT context.

    A task spawned with ``asyncio.create_task`` during an HTTP request inherits
    a COPY of that request's context — including ``_request_session`` pointing at
    the request-scoped DB session, which FastAPI closes the instant the response
    returns. Such a background task must call this FIRST so that any later
    ``request_scoped_session()`` (e.g. the model-config read inside
    ``LLMManager.configure_kasal_llm``) opens a fresh standalone session instead
    of operating on the closed one (``sqlite3.ProgrammingError: Cannot operate on
    a closed database``). Setting the var here only affects this task's copied
    context, never the originating request.
    """
    _request_session.set(None)


def background_task_context() -> Context:
    """A copy of the current context with the request-scoped DB session cleared.

    Pass to ``asyncio.create_task(coro, context=background_task_context())`` when
    spawning a job that OUTLIVES the request that started it. The task keeps the
    request's other contextvars (group id, OBO token) but never inherits
    ``_request_session`` — the session FastAPI closes at response end — so every
    ``request_scoped_session()`` inside it opens a fresh standalone session.

    This is the spawn-side complement to :func:`detach_request_session` (which
    clears the var from INSIDE an already-running task): declaring the clean
    context at the ``create_task`` boundary means the task can never observe the
    dead session at all, so there is no "detach first or crash" ordering to
    forget. The db layer owns ``_request_session``, so it owns this helper too.
    """
    ctx = copy_context()
    ctx.run(_request_session.set, None)
    return ctx


# Create separate session factories for pooled and nullpool engines
pooled_session_factory = None
nullpool_session_factory = None

if not str(settings.DATABASE_URI).startswith("sqlite"):
    # For PostgreSQL, create session factories for both engines
    pooled_session_factory = async_sessionmaker(
        pooled_engine,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )

    nullpool_session_factory = async_sessionmaker(
        nullpool_engine,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )


# (was: ``safe_async_session`` — a third session helper, now DELETED.)
# It wrapped the raw ``async_session_factory`` and swallowed errors from
# ``session.close()``, for crew/flow subprocess work where a SQLite connection
# went stale after the greenlet context was lost. It had no caller left in
# ``src/`` — only its own tests, plus eleven ``patch("src.db.session.
# safe_async_session", ...)`` in the flow-executor tests that were patching a
# name the code under test no longer touched, i.e. asserting nothing. That is
# the same failure mode ``request_scoped_session`` had: a helper that reads as
# the careful option, keeps a per-process SNAPSHOT of the factory underneath,
# and stays alive because its tests still pass. Two helpers remain by design —
# ``routed_scoped_session`` (the default) and ``get_isolated_db_session``
# (connection isolation on SQLite). Do not add a third.


# ── Isolated DB session on a PRIVATE connection ───────────────────────────
# SQLite runs on a SINGLE shared connection (StaticPool — see
# get_sqlite_poolclass). SQLite transactions are per-connection, but every
# AsyncSession in the process checks out that one connection, so concurrent
# sessions corrupt each other's transaction boundaries: a concurrent
# rollback/close can silently discard another session's just-committed (or
# pending) writes. That bit progressive crew generation — across the
# seconds-long per-task LLM calls, a concurrent request's session on the shared
# connection clobbered a committed agent row, so the next task INSERT failed the
# agent_id foreign key ("FOREIGN KEY constraint failed"). A session on its OWN
# connection (its own aiosqlite queue) is immune to that interference.
# PostgreSQL/Lakebase already give each pooled checkout a private connection, so
# this only needs special handling for SQLite.
_isolated_sqlite_engine = None
_isolated_sqlite_session_factory = None


def _get_isolated_sqlite_session_factory():
    """Lazily build a NullPool engine + sessionmaker on a private connection.

    NullPool hands out a FRESH aiosqlite connection per checkout (closed on
    return), so the caller's whole unit of work runs on a connection no other
    session touches. Only one such connection is added per crew-generation run
    (not the per-query NullPool storm that the 205b5f57 StaticPool switch was
    reverting), and it sits idle/lock-free between the per-entity commits, so
    WAL + busy_timeout absorb the brief write-lock overlap with the shared
    connection.
    """
    global _isolated_sqlite_engine, _isolated_sqlite_session_factory
    if _isolated_sqlite_session_factory is None:
        _isolated_sqlite_engine = create_async_engine(
            str(settings.DATABASE_URI),
            echo=SQL_DEBUG,
            future=True,
            poolclass=NullPool,
            connect_args={**connect_args, "check_same_thread": False},
        )
        # Apply the same PRAGMAs (foreign_keys=ON, WAL, busy_timeout) as the main engine.
        event.listen(_isolated_sqlite_engine.sync_engine, "connect", configure_sqlite)
        _isolated_sqlite_session_factory = async_sessionmaker(
            _isolated_sqlite_engine,
            expire_on_commit=False,
            autoflush=False,
            autocommit=False,
        )
    return _isolated_sqlite_session_factory


@asynccontextmanager
async def get_isolated_db_session():
    """Yield a session whose connection is NOT shared with any other session.

    Use for a multi-step unit of work that interleaves DB writes with long
    awaits (e.g. progressive crew generation: commit an agent, then make a
    seconds-long LLM call, then insert a task referencing it). On the shared
    SQLite connection a concurrent session's commit/rollback in that window can
    silently discard the committed agent and break the task's foreign key; a
    private connection removes that hazard. For SQLite this uses a dedicated
    NullPool engine (private connection); for PostgreSQL/Lakebase, where each
    pooled checkout is already private, it falls through to the normal (possibly
    Lakebase-swapped) factory.
    """
    if (
        str(settings.DATABASE_URI).startswith("sqlite")
        and not async_session_factory.is_lakebase
    ):
        # The global Lakebase swap (main.py lifespan / activate_lakebase_in_subprocess)
        # is PER-PROCESS and can fail or lag, leaving THIS process's
        # async_session_factory on local SQLite even though Lakebase is enabled in
        # the DB config. Reads route to Lakebase off is_lakebase_enabled() via
        # get_smart_db_session — INDEPENDENT of the swap state. So if we wrote to the
        # private SQLite engine here while Lakebase is enabled, the row would land in
        # local SQLite while every read looks in Lakebase, i.e. "Execution not found"
        # 404 storms (e751d923 moved create_execution's parent-row write onto this
        # helper and regressed exactly that). Follow the SAME signal as reads: when
        # Lakebase is enabled, write through the same Lakebase factory the reads use.
        from src.db.database_router import (
            get_lakebase_config_from_db,
            is_lakebase_enabled,
        )

        if await is_lakebase_enabled():
            from src.db.lakebase_session import get_lakebase_session

            config = await get_lakebase_config_from_db()
            instance_name = (config or {}).get("instance_name")
            async with get_lakebase_session(instance_name) as session:
                yield session
            return
        factory = _get_isolated_sqlite_session_factory()
        async with factory() as session:
            yield session
    else:
        # PostgreSQL or a Lakebase-swapped factory: pooled connections are
        # already per-checkout, so there is no shared-connection hazard.
        async with async_session_factory() as session:
            yield session


# Sync session factory for non-async contexts (e.g. CrewAI guardrail callbacks).
# Uses the sync_engine underlying the async engine.
from sqlalchemy.orm import sessionmaker as sync_sessionmaker

sync_session_factory = sync_sessionmaker(
    engine.sync_engine,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


# Database initialization
def _conn_is_sqlite(conn) -> bool:
    """Whether THIS connection is SQLite — asked of the connection, not the env.

    Every ``_ensure_*`` helper branches on dialect, and reading
    ``settings.DATABASE_URI`` to decide is wrong on the path that matters most.
    ``run_schema_self_heal`` is called TWICE: once in ``init_db`` against the
    local engine, and again from the ``main.py`` lifespan against the freshly
    activated LAKEBASE engine. On that second call ``DATABASE_URI`` still says
    ``sqlite`` — it describes the configured default, not the connection in hand —
    so the helpers took the SQLite branch and issued SQLite-flavoured DDL at
    PostgreSQL.

    The failure was invisible because each helper swallows its exception with a
    warning. Columns added BEFORE a Lakebase was provisioned were unaffected
    (``create_all`` had made the whole table), so this stayed latent until the
    first column added AFTER one existed: `modelconfig.thinking_budget_tokens`
    never landed, and every read of the model catalogue — which is every LLM call
    — 500'd with "column modelconfig.thinking_budget_tokens does not exist".

    Asking the connection removes the coupling entirely.
    """
    try:
        return conn.engine.dialect.name == "sqlite"
    except Exception:  # noqa: BLE001 — fall back to the configured default
        return str(settings.DATABASE_URI).startswith("sqlite")


async def _pg_columns(conn, table: str) -> set[str]:
    """Column names of ``table`` on PostgreSQL; empty set if it does not exist.

    The Postgres counterpart of ``PRAGMA table_info``. Reading the catalogue
    before altering matters for more than efficiency: Postgres checks OWNERSHIP
    before it checks existence, so ``ADD COLUMN IF NOT EXISTS`` raises 42501
    "must be owner" on a table this role does not own EVEN WHEN the column is
    already there and the statement would do nothing.

    Unqualified so it follows ``search_path``, matching the DDL that uses it.
    """
    res = await conn.exec_driver_sql(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table}' "
        "AND table_schema = ANY (current_schemas(false))"
    )
    return {row[0] for row in res.fetchall()}


async def _ensure_documentation_embeddings_columns(conn) -> None:
    """Idempotently add group_id/file_path to documentation_embeddings.

    create_all never ALTERs an existing table, so app DBs created before these
    columns existed are missing them — which breaks knowledge ingest fallback,
    built-in doc seeding, and group-scoped search (all reference the columns).
    Safe to run on every startup; the embedding column is unchanged here.
    """
    is_sqlite = _conn_is_sqlite(conn)
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql(
                "PRAGMA table_info(documentation_embeddings)"
            )
            cols = {row[1] for row in res.fetchall()}
            if not cols:
                return  # table not created yet
            if "group_id" not in cols:
                await conn.exec_driver_sql(
                    "ALTER TABLE documentation_embeddings ADD COLUMN group_id VARCHAR(100)"
                )
            if "file_path" not in cols:
                await conn.exec_driver_sql(
                    "ALTER TABLE documentation_embeddings ADD COLUMN file_path VARCHAR"
                )
        else:
            # Ask before altering. `ADD COLUMN IF NOT EXISTS` is a no-op when the
            # column is there, but Postgres checks OWNERSHIP before it checks
            # existence — so on a table owned by a previous deploy's service
            # principal it raises 42501 "must be owner" for work that did not need
            # doing. That error is what aborted the shared transaction and took
            # the other 23 self-heal steps down with it, leaving
            # agents.thinking_budget_tokens missing. Skipping the ALTER when the
            # columns already exist keeps the common case off the failure path.
            existing = await _pg_columns(conn, "documentation_embeddings")
            if not existing:
                return  # table not created yet
            for name, ddl_type in (
                ("group_id", "VARCHAR(100)"),
                ("file_path", "VARCHAR"),
            ):
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE documentation_embeddings "
                        f"ADD COLUMN IF NOT EXISTS {name} {ddl_type}"
                    )
        logger.info("Ensured documentation_embeddings group_id/file_path columns")
    except Exception as e:
        logger.warning(f"Could not ensure documentation_embeddings columns: {e}")

    # Ensure the dedicated knowledge_embeddings table exists (uploaded-knowledge
    # fallback when no Lakebase backend is active). create_all is skipped on
    # existing DBs, so create it explicitly here.
    try:
        from src.models.documentation_embedding import KnowledgeEmbedding

        def _create_knowledge_table(sync_conn):
            KnowledgeEmbedding.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_knowledge_table)
        logger.info("Ensured knowledge_embeddings table exists")
    except Exception as e:
        logger.warning(f"Could not ensure knowledge_embeddings table: {e}")

    # Self-heal: knowledge_embeddings.created_by (per-user isolation of
    # uploaded knowledge). checkfirst-create above never ALTERs a table that
    # already exists, so pre-existing DBs need the column added here.
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(knowledge_embeddings)")
            cols = {row[1] for row in res.fetchall()}
            if cols and "created_by" not in cols:
                await conn.exec_driver_sql(
                    "ALTER TABLE knowledge_embeddings ADD COLUMN created_by VARCHAR(255)"
                )
                logger.info(
                    "Added knowledge_embeddings.created_by column (SQLite self-heal)"
                )
        else:
            await conn.exec_driver_sql(
                "ALTER TABLE knowledge_embeddings ADD COLUMN IF NOT EXISTS created_by VARCHAR(255)"
            )
        logger.info("Ensured knowledge_embeddings.created_by column")
    except Exception as e:
        logger.warning(f"Could not ensure knowledge_embeddings.created_by column: {e}")

    await _ensure_pgvector_embedding_columns(conn)


def _vector_embedding_tables() -> tuple[str, ...]:
    """Tables whose ``embedding vector(1024)`` column is added AFTER the table.

    DERIVED from the models, not listed. The first version of this helper hardcoded
    ``("documentation_embeddings", "knowledge_embeddings")`` and so missed
    ``workflow_recipes``, which carries the same ``Vector(1024)`` column — the
    deployed app then failed every recipe lookup with

        column workflow_recipes.embedding does not exist

    That is the third time a hand-maintained list of models has drifted from
    reality here. Asking the metadata covers a new vector column on the day it is
    added.

    Computed locally rather than importing the equivalent helper from
    ``services/databricks/lakebase/schema.py``: ``db`` sits below ``services`` and
    must not reach up into it.
    """
    from src.models.documentation_embedding import Vector

    return tuple(
        sorted(
            table.name
            for table in Base.metadata.sorted_tables
            if any(isinstance(column.type, Vector) for column in table.columns)
        )
    )


async def _ensure_pgvector_embedding_columns(conn) -> None:
    """Add back the ``embedding`` column on PostgreSQL when pgvector is present.

    Vector tables are created WITHOUT their vector column, because a deployed app
    cannot install pgvector (``CREATE EXTENSION`` needs ``databricks_superuser``)
    and ``CREATE TABLE ... vector(1024)`` fails outright without it. That keeps the
    rest of the table usable — but nothing added the column back once an instance
    owner HAD enabled the extension, so the ORM kept inserting a column that did
    not exist::

        column "embedding" of relation "knowledge_embeddings" does not exist

    which broke knowledge upload after the text was extracted and 56 chunks were
    embedded — the work was done and then discarded.

    SQLite is skipped: there the column is TEXT holding JSON and ``create_all``
    makes it with the table, and the repository uses a Python-side similarity path.
    """
    if _conn_is_sqlite(conn):
        return
    # Which schema holds the extension. pgvector installs its `vector` type and
    # its operator classes into ONE schema — usually `public` — and this
    # connection's search_path is not guaranteed to include it. The first version
    # of this helper used the bare name and every statement failed with
    # `type "vector" does not exist` / `operator class "vector_cosine_ops" does
    # not exist` even though the extension WAS installed. Qualifying removes the
    # dependency on search_path entirely.
    try:
        result = await conn.exec_driver_sql(
            "SELECT n.nspname FROM pg_extension e "
            "JOIN pg_namespace n ON n.oid = e.extnamespace "
            "WHERE e.extname IN ('vector', 'pgvector')"
        )
        row = result.fetchone()
        if row is None:
            logger.info(
                "pgvector not enabled; embedding columns skipped. An instance owner "
                "can run 'CREATE EXTENSION IF NOT EXISTS vector;' and restart to "
                "enable similarity search."
            )
            return
        ext_schema = row[0]
    except Exception as e:  # noqa: BLE001 — never block startup on the probe
        logger.warning(f"Could not check for pgvector: {e}")
        return

    applied = True
    for table in _vector_embedding_tables():
        # Each statement in its own SAVEPOINT: an orphaned-owner table (42501)
        # must not abort the surrounding self-heal transaction.
        for stmt in (
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS embedding "
            f"{ext_schema}.vector(1024)",
            f"CREATE INDEX IF NOT EXISTS idx_{table}_embedding ON {table} "
            f"USING hnsw (embedding {ext_schema}.vector_cosine_ops)",
        ):
            try:
                async with conn.begin_nested():
                    await conn.exec_driver_sql(stmt)
            except Exception as e:  # noqa: BLE001 — best-effort per statement
                applied = False
                logger.warning(f"Could not apply '{stmt[:60]}...': {e}")
    if applied:
        logger.info("Ensured pgvector embedding columns + HNSW indexes")
    else:
        # Do NOT log success when nothing was applied. The first version did, so
        # "Ensured pgvector embedding columns" appeared in the logs while every
        # ALTER had failed and knowledge upload stayed broken.
        logger.warning(
            "pgvector embedding columns INCOMPLETE — knowledge/document similarity "
            "search will not work until the warnings above are resolved"
        )


async def _ensure_chat_sessions_table(conn) -> None:
    """Idempotently create the chat_sessions table (named chat-mode sessions).

    create_all is skipped on existing DBs, so DBs created before this table
    was added need it created explicitly here.
    """
    try:
        from src.models.chat_session import ChatSession

        def _create_chat_sessions_table(sync_conn):
            ChatSession.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_chat_sessions_table)
        logger.info("Ensured chat_sessions table exists")
    except Exception as e:
        logger.warning(f"Could not ensure chat_sessions table: {e}")


async def _ensure_workflow_recipes_table(conn) -> None:
    """Idempotently create the workflow_recipes table (executed crews kept for reuse).

    create_all is skipped on existing DBs, so this is how the table reaches
    already-deployed installs. No alembic revision: the migration graph
    currently has many heads, and a new table needs no ALTER — checkfirst-create
    covers SQLite, PostgreSQL and Lakebase identically.
    """
    try:
        from src.models.workflow_recipe import WorkflowRecipe

        def _create_workflow_recipes_table(sync_conn):
            WorkflowRecipe.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_workflow_recipes_table)
        logger.info("Ensured workflow_recipes table exists")
    except Exception as e:
        logger.warning(f"Could not ensure workflow_recipes table: {e}")


async def _ensure_workflow_recipe_trials_table(conn) -> None:
    """Idempotently create workflow_recipe_trials (the reuse measurement ledger).

    Same reasoning as the recipes table above: a brand-new table needs no ALTER,
    so a checkfirst-create reaches already-deployed installs identically on
    SQLite, PostgreSQL and Lakebase. Missing this table must never break
    generation — the recording path swallows its own errors — but without it
    every trial write is a silent no-op and the effectiveness report stays empty.
    """
    try:
        from src.models.workflow_recipe_trial import WorkflowRecipeTrial

        def _create_workflow_recipe_trials_table(sync_conn):
            WorkflowRecipeTrial.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_workflow_recipe_trials_table)
        logger.info("Ensured workflow_recipe_trials table exists")
    except Exception as e:
        logger.warning(f"Could not ensure workflow_recipe_trials table: {e}")


async def _ensure_mlflow_config_table(conn) -> None:
    """Idempotently create the mlflowconfig table (per-group MLflow settings).

    create_all is skipped on existing DBs, so a database created before this
    model landed (the common local-dev case) never gets the table and every
    ``GET /mlflow/settings`` 500s with "no such table: mlflowconfig". New table,
    no ALTER, so a checkfirst-create reaches SQLite, PostgreSQL and Lakebase
    identically — same pattern as _ensure_prompt_optimization_runs_table.
    """
    try:
        from src.models.mlflow_config import MLflowConfig

        def _create(sync_conn):
            MLflowConfig.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create)
        logger.info("Ensured mlflowconfig table exists")
    except Exception as e:
        logger.warning(f"Could not ensure mlflowconfig table: {e}")


async def _ensure_a2a_push_configs_table(conn) -> None:
    """Idempotently create a2a_push_configs (A2A webhook registrations).

    New table, so a checkfirst-create reaches deployed installs identically on
    SQLite, PostgreSQL and Lakebase — same as its neighbours here.
    """
    try:
        from src.models.a2a_push_config import A2APushConfig

        def _create(sync_conn):
            A2APushConfig.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create)
        logger.info("Ensured a2a_push_configs table exists")
    except Exception as e:
        logger.warning(f"Could not ensure a2a_push_configs table: {e}")


async def _ensure_skills_tables(conn) -> None:
    """Idempotently create skills + skill_files (Agent Skills storage).

    New tables, so a checkfirst-create reaches deployed installs identically on
    SQLite, PostgreSQL and Lakebase — same as its neighbours here. Files after
    skills: the child carries the foreign key.
    """
    try:
        from src.models.skill import Skill, SkillFile

        def _create(sync_conn):
            Skill.__table__.create(sync_conn, checkfirst=True)
            SkillFile.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create)
        logger.info("Ensured skills tables exist")
    except Exception as e:
        logger.warning(f"Could not ensure skills tables: {e}")


async def _ensure_a2a_agents_table(conn) -> None:
    """Idempotently create a2a_agents (remote agents Kasal can call).

    New table, so a checkfirst-create reaches deployed installs identically on
    SQLite, PostgreSQL and Lakebase — same as its neighbours here.
    """
    try:
        from src.models.a2a_agent import A2AAgent

        def _create(sync_conn):
            A2AAgent.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create)
        logger.info("Ensured a2a_agents table exists")
    except Exception as e:
        logger.warning(f"Could not ensure a2a_agents table: {e}")


async def _ensure_crew_publications_table(conn) -> None:
    """Idempotently create `publications` (the external-publication registry).

    A brand-new table, so a checkfirst-create reaches already-deployed installs
    identically on SQLite, PostgreSQL and Lakebase — same reasoning as the
    tables above, and the Alembic migration carries the same guard.

    Without it the MCP tool list and the A2A Agent Card both fail: they read
    this table to answer "what has this workspace published", and an unhandled
    UndefinedTable turns a discovery request into a 500.
    """
    try:
        from src.models.crew_publication import Publication

        def _create_crew_publications_table(sync_conn):
            Publication.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_crew_publications_table)
        logger.info("Ensured publications table exists")
    except Exception as e:
        logger.warning(f"Could not ensure publications table: {e}")


async def _ensure_chat_sessions_columns(conn) -> None:
    """Idempotently add the running_job_id + preview_* columns to chat_sessions.

    These back the refresh-reconnect marker and the per-session preview, which
    moved off browser IndexedDB onto the server. create_all never ALTERs an
    existing table, so DBs created before these columns existed (e.g. customer
    instances we can't migrate manually) are missing them — which would break
    saving/reading previews and the running-job marker. Safe to run every
    startup; all columns are nullable with no default.
    """
    is_sqlite = _conn_is_sqlite(conn)
    columns = [
        ("running_job_id", "VARCHAR"),
        ("preview_type", "VARCHAR(50)"),
        ("preview_data", "TEXT"),
        ("preview_title", "VARCHAR(512)"),
        ("context_summary", "TEXT"),
        ("context_summary_upto", "TIMESTAMP"),
    ]
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(chat_sessions)")
            existing = {row[1] for row in res.fetchall()}
            if not existing:
                return  # table not created yet (handled by _ensure_chat_sessions_table)
            for name, ddl_type in columns:
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE chat_sessions ADD COLUMN {name} {ddl_type}"
                    )
                    logger.info(f"Added chat_sessions.{name} column (SQLite self-heal)")
        else:
            for name, ddl_type in columns:
                await conn.exec_driver_sql(
                    f"ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS {name} {ddl_type}"
                )
            logger.info("Ensured chat_sessions preview/running_job_id columns")
    except Exception as e:
        logger.warning(f"Could not ensure chat_sessions columns: {e}")


async def _ensure_agent_columns(conn) -> None:
    """Idempotently add the agents columns added after the table shipped.

    ``create_all`` never ALTERs an existing table, so a database created before a
    column existed would accept the field from the UI and silently drop it on
    save — the failure reads as "my selection did not persist", with nothing
    anywhere saying why. All nullable, safe every startup."""
    is_sqlite = _conn_is_sqlite(conn)
    # (name, sqlite type, postgres type)
    columns = [
        ("skills", "TEXT", "JSONB"),
        # Per-agent thinking overrides; NULL inherits the model row.
        ("thinking_budget_tokens", "INTEGER", "INTEGER"),
        ("reasoning_effort", "TEXT", "VARCHAR"),
    ]
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(agents)")
            existing = {row[1] for row in res.fetchall()}
            if not existing:
                return  # table not created yet (create_all handles fresh DBs)
            for name, sqlite_type, _pg_type in columns:
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE agents ADD COLUMN {name} {sqlite_type}"
                    )
                    logger.info(f"Added agents.{name} column (SQLite self-heal)")
        else:
            # Read the catalogue first — see _pg_columns: an ALTER on a table this
            # role does not own raises "must be owner" even when the column is
            # already present, and that error aborts the whole self-heal
            # transaction.
            existing = await _pg_columns(conn, "agents")
            if not existing:
                return  # table not created yet
            for name, _sqlite_type, pg_type in columns:
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE agents ADD COLUMN IF NOT EXISTS {name} {pg_type}"
                    )
                    logger.info(f"Added agents.{name} column")
            logger.info("Ensured agents columns (skills, thinking overrides)")
    except Exception as e:
        logger.warning(f"Could not ensure agents columns: {e}")


async def _ensure_execution_history_columns(conn) -> None:
    """Idempotently add executionhistory.resumed_from_execution_id.

    Resuming a run creates a NEW execution pointing at the one it came from.
    ``create_all`` never ALTERs an existing table, so a database created before
    that column existed keeps a schema the model no longer matches — and because
    SQLAlchemy selects every mapped column, EVERY read of executionhistory fails
    with "no such column", not just the resume path. There is an Alembic
    migration for this too; this covers dev databases that were built from the
    models and have no alembic_version at all.
    """
    is_sqlite = _conn_is_sqlite(conn)
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(executionhistory)")
            existing = {row[1] for row in res.fetchall()}
            if not existing:
                return  # table not created yet (create_all handles fresh DBs)
            if "resumed_from_execution_id" not in existing:
                await conn.exec_driver_sql(
                    "ALTER TABLE executionhistory "
                    "ADD COLUMN resumed_from_execution_id INTEGER"
                )
                logger.info(
                    "Added executionhistory.resumed_from_execution_id (SQLite self-heal)"
                )
        else:
            await conn.exec_driver_sql(
                "ALTER TABLE executionhistory "
                "ADD COLUMN IF NOT EXISTS resumed_from_execution_id INTEGER"
            )
            logger.info("Ensured executionhistory.resumed_from_execution_id column")
    except Exception as e:
        logger.warning(
            f"Could not ensure executionhistory.resumed_from_execution_id: {e}"
        )


async def _ensure_crew_columns(conn) -> None:
    """Idempotently add reasoning_config to crews. create_all never ALTERs an
    existing table, so DBs created before this column existed (e.g. deployed
    customer instances) would silently drop the saved reasoning budget
    ({"reasoning_effort": ...}) on save/reload. Safe to run every startup; column
    is nullable JSON/TEXT.

    NOTE: the removed planner columns (crews.planning / crews.planning_llm /
    schedule.planning) are dropped by the alembic migration
    20260725_drop_planner_columns. They are deliberately NOT dropped here — a
    self-healing deployed DB keeps the orphan columns harmlessly (nullable and no
    longer mapped), and running DROP COLUMN on every startup is riskier than
    leaving them behind."""
    is_sqlite = _conn_is_sqlite(conn)
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(crews)")
            existing = {row[1] for row in res.fetchall()}
            if not existing:
                return  # table not created yet (create_all handles fresh DBs)
            if "reasoning_config" not in existing:
                await conn.exec_driver_sql(
                    "ALTER TABLE crews ADD COLUMN reasoning_config TEXT"
                )
                logger.info("Added crews.reasoning_config column (SQLite self-heal)")
        else:
            await conn.exec_driver_sql(
                "ALTER TABLE crews ADD COLUMN IF NOT EXISTS reasoning_config JSONB"
            )
            logger.info("Ensured crews.reasoning_config column")
    except Exception as e:
        logger.warning(f"Could not ensure crews.reasoning_config column: {e}")


async def _disable_bi_specialist_crew_memory(conn) -> None:
    """Idempotently disable crew/agent memory for the pre-seeded 'bi-specialist'
    workspace. These are deterministic ETL crews (PBI config extraction → UCMV
    generation → validation → deploy) that pass all data via the flow handoff and
    tool configs, NOT via cross-run semantic recall. With memory on, CrewAI
    auto-saves each task's output (incl. the ~174K-char pipeline-config JSON) and
    recalls it workspace-wide into every later agent prompt — overflowing the
    model's context window (200K), which forces a fallback to a larger model and
    stalls the HITL approval gate. The seed now ships memory=False, but the seeder
    is insert-only (it skips a group that already exists), so DBs seeded before
    this change keep memory on. This self-heals them. Safe to run every startup —
    it only flips rows that are still True, scoped to the bi-specialist group."""
    is_sqlite = _conn_is_sqlite(conn)
    # SQLite stores booleans as 0/1; Postgres uses true/false. exec_driver_sql
    # with a literal keeps this dialect-agnostic enough for both.
    true_val = "1" if is_sqlite else "true"
    false_val = "0" if is_sqlite else "false"
    try:
        for table in ("crews", "agents"):
            if is_sqlite:
                res = await conn.exec_driver_sql(f"PRAGMA table_info({table})")
                cols = {row[1] for row in res.fetchall()}
                if "memory" not in cols or "group_id" not in cols:
                    continue  # table/columns not present yet (fresh DB handled by seed)
            await conn.exec_driver_sql(
                f"UPDATE {table} SET memory = {false_val} "
                f"WHERE group_id = 'bi-specialist' AND memory = {true_val}"
            )
        logger.info("Ensured bi-specialist crews/agents have memory disabled")
    except Exception as e:
        logger.warning(f"Could not disable bi-specialist crew memory: {e}")


async def _ensure_ui_config_columns(conn) -> None:
    """Idempotently create ui_config and add its Predefined-UI columns.

    The TABLE was previously left to ``create_all`` — but ``init_db`` skips
    ``create_all`` entirely once the database has more than one table
    ("Tables already exist"), so on any install created before ui_config shipped the
    table simply never appeared. Opening the UI Configurator then 500'd on every
    request with ``no such table: ui_config``, which is why every other
    later-than-the-DB table in this module has its own checkfirst-create.

    The COLUMNS still need the ALTER pass: create_all never ALTERs an existing
    table, so DBs created before catalog_json/style_json existed would silently
    drop a workspace's A2UI catalog + branding on save/reload.

    Safe to run every startup (checkfirst-create, nullable TEXT columns).
    """
    try:
        from src.models.ui_config import UIConfig

        def _create_ui_config_table(sync_conn):
            UIConfig.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_ui_config_table)
        logger.info("Ensured ui_config table exists")
    except Exception as e:
        logger.warning(f"Could not ensure ui_config table: {e}")
    is_sqlite = _conn_is_sqlite(conn)
    # `disabled_components` arrived with the per-component toggles: create_all
    # never ALTERs an existing table, so without it every database provisioned
    # before the toggles would raise on the first read of a UI config.
    columns = ("catalog_json", "style_json", "disabled_components")
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(ui_config)")
            existing = {row[1] for row in res.fetchall()}
            if not existing:
                return  # table not created yet (create_all handles fresh DBs)
            for col in columns:
                if col not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE ui_config ADD COLUMN {col} TEXT"
                    )
                    logger.info(f"Added ui_config.{col} column (SQLite self-heal)")
        else:
            for col in columns:
                await conn.exec_driver_sql(
                    f"ALTER TABLE ui_config ADD COLUMN IF NOT EXISTS {col} TEXT"
                )
            logger.info(
                "Ensured ui_config catalog_json/style_json/disabled_components columns"
            )
    except Exception as e:
        logger.warning(f"Could not ensure ui_config columns: {e}")


async def _ensure_modelconfig_columns(conn) -> None:
    """Idempotently add the modelconfig columns added after the table shipped.

    ``create_all`` never ALTERs an existing table and Alembic does not run at
    startup here, so without this every database provisioned before these
    columns existed would raise on the first SELECT of the model catalogue —
    which is every LLM call. All nullable with no default, safe every startup.

    JSON rather than TEXT: SQLAlchemy's JSON type reads a TEXT column fine on
    SQLite (it stores JSON as text anyway), and on PostgreSQL the native type is
    what the ORM expects to decode.
    """
    is_sqlite = _conn_is_sqlite(conn)
    columns = [
        ("params", "JSON"),
        ("unsupported_params", "JSON"),
        # Anthropic thinking depth. Which of the two applies is decided by
        # transport.thinking_mode(); see models/model_config.py.
        ("thinking_budget_tokens", "INTEGER"),
        ("reasoning_effort", "VARCHAR"),
    ]
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(modelconfig)")
            existing = {row[1] for row in res.fetchall()}
            if not existing:
                return  # table not created yet (create_all handles fresh DBs)
            for name, ddl_type in columns:
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE modelconfig ADD COLUMN {name} {ddl_type}"
                    )
                    logger.info(f"Added modelconfig.{name} column (SQLite self-heal)")
        else:
            # Catalogue first — see _pg_columns and _ensure_agent_columns.
            existing = await _pg_columns(conn, "modelconfig")
            if not existing:
                return  # table not created yet
            for name, ddl_type in columns:
                if name not in existing:
                    await conn.exec_driver_sql(
                        f"ALTER TABLE modelconfig ADD COLUMN IF NOT EXISTS {name} {ddl_type}"
                    )
                    logger.info(f"Added modelconfig.{name} column")
            logger.info("Ensured modelconfig params/unsupported_params columns")
    except Exception as e:
        logger.warning(f"Could not ensure modelconfig columns: {e}")


async def _ensure_hot_polling_indexes(conn) -> None:
    """Idempotently add the indexes the run-polling queries filter/sort on.

    create_all only creates indexes for NEW tables, so existing deployed DBs
    sequential-scan/sort the two biggest, fastest-growing tables on every 2s
    poll: executionhistory (list: group_id + ORDER BY created_at DESC; trace
    broadcaster: status IN ('RUNNING', ...) every second) and execution_trace
    (run-scoped reads/deletes on run_id, ordered reads on created_at).
    CREATE INDEX IF NOT EXISTS is valid on both SQLite and PostgreSQL."""
    statements = (
        "CREATE INDEX IF NOT EXISTS idx_executionhistory_group_created "
        "ON executionhistory (group_id, created_at)",
        "CREATE INDEX IF NOT EXISTS ix_executionhistory_status "
        "ON executionhistory (status)",
        "CREATE INDEX IF NOT EXISTS ix_executionhistory_created_at "
        "ON executionhistory (created_at)",
        "CREATE INDEX IF NOT EXISTS ix_execution_trace_run_id "
        "ON execution_trace (run_id)",
        "CREATE INDEX IF NOT EXISTS ix_execution_trace_created_at "
        "ON execution_trace (created_at)",
    )
    for stmt in statements:
        try:
            await conn.exec_driver_sql(stmt)
        except Exception as e:
            logger.warning(
                f"Could not ensure polling index ({stmt.split(' ON ', 1)[0]}): {e}"
            )
    logger.info("Ensured hot-polling indexes on executionhistory/execution_trace")


async def _heal_personal_group_names(conn) -> None:
    """One-time data heal for the workspace→teamspace rename: auto-created
    personal groups persisted the old display name ("Personal Workspace - …")
    in groups.name, which surfaces in the admin group list. The personal tenant
    is now the "Personal Space" (it is not a teamspace), so rewrite the prefix
    in place. Idempotent (the WHERE prefix no longer matches after the first
    run) and DML-only, so it also runs on deployments where DDL is unavailable."""
    try:
        res = await conn.exec_driver_sql(
            "UPDATE \"groups\" SET name = REPLACE(name, 'Personal Workspace', 'Personal Space') "
            "WHERE name LIKE 'Personal Workspace%'"
        )
        renamed = getattr(res, "rowcount", 0) or 0
        if renamed > 0:
            logger.info(f"Renamed {renamed} personal group(s) to 'Personal Space'")
    except Exception as e:
        logger.warning(f"Could not heal personal group names: {e}")


async def _heal_engine_config_names(conn) -> None:
    """One-time data heal for the crewai→kasal engine rename: engine_configs
    rows persisted engine_name='crewai' (the legacy name of the now-native
    kasal engine). Rewrite in place so lookups keyed on 'kasal' find them.
    Idempotent (the WHERE no longer matches after the first run) and DML-only,
    so it also runs on deployments where DDL is unavailable."""
    try:
        res = await conn.exec_driver_sql(
            "UPDATE engineconfig SET engine_name = 'kasal' "
            "WHERE engine_name = 'crewai'"
        )
        renamed = getattr(res, "rowcount", 0) or 0
        if renamed > 0:
            logger.info(
                f"Renamed {renamed} engineconfig row(s) from 'crewai' to 'kasal'"
            )
    except Exception as e:
        logger.warning(f"Could not heal engine_config engine names: {e}")


async def _ensure_crew_feedback_table(conn) -> None:
    """Idempotently create the crew_feedback table (thumbs feedback on
    cataloged crews). create_all is skipped on existing DBs."""
    try:
        from src.models.crew_feedback import CrewFeedback

        def _create_crew_feedback_table(sync_conn):
            CrewFeedback.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_crew_feedback_table)
        logger.info("Ensured crew_feedback table exists")
    except Exception as e:
        logger.warning(f"Could not ensure crew_feedback table: {e}")


async def _ensure_powerbi_extraction_table(conn) -> None:
    """Idempotently create the powerbi_extraction table (raw Power BI extraction
    artifacts persisted per Pipeline Config Generator run, for SQL querying).
    create_all is skipped on existing DBs, so DBs created before this table
    existed need this self-heal."""
    try:
        from src.models.powerbi_extraction import PowerBIExtraction

        def _create_powerbi_extraction_table(sync_conn):
            PowerBIExtraction.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_powerbi_extraction_table)
        logger.info("Ensured powerbi_extraction table exists")
    except Exception as e:
        logger.warning(f"Could not ensure powerbi_extraction table: {e}")


async def _ensure_prompt_optimization_runs_table(conn) -> None:
    """Idempotently create the prompt_optimization_runs table (durable GEPA
    optimization runs, including the before-image an apply can be reverted
    from). create_all is skipped on existing DBs, so DBs created before this
    table existed need this self-heal."""
    try:
        from src.models.prompt_optimization_run import PromptOptimizationRun

        def _create_prompt_optimization_runs_table(sync_conn):
            PromptOptimizationRun.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_prompt_optimization_runs_table)
        logger.info("Ensured prompt_optimization_runs table exists")
    except Exception as e:
        logger.warning(f"Could not ensure prompt_optimization_runs table: {e}")


async def _ensure_memory_maintenance_table(conn) -> None:
    """Idempotently create memory_maintenance_watermarks.

    Per-scope record of when memory maintenance last ran, which is what makes
    the scheduled sweep's coverage depend on store size rather than on how often
    someone happens to run a crew. Same reasoning as the tables above: a brand
    new table needs no ALTER, so a checkfirst-create reaches already-deployed
    installs identically on SQLite, PostgreSQL and Lakebase. A missing table
    must never break anything — the sweep swallows its own errors — but without
    it every tick would re-maintain the same scopes."""
    try:
        from src.models.memory_maintenance import MemoryMaintenanceWatermark

        def _create_memory_maintenance_table(sync_conn):
            MemoryMaintenanceWatermark.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(_create_memory_maintenance_table)
        logger.info("Ensured memory_maintenance_watermarks table exists")
    except Exception as e:
        logger.warning(f"Could not ensure memory_maintenance_watermarks table: {e}")


async def _ensure_databricks_config_columns(conn) -> None:
    """Idempotently add ai_gateway_enabled to databricksconfig.

    create_all never ALTERs an existing table, so app DBs created before this
    column existed (e.g. customer instances deployed from the marketplace that
    we cannot migrate manually) are missing it — which breaks reading/writing
    the Databricks configuration. Safe to run on every startup; defaults to
    false (serving-endpoints routing) to preserve existing behavior.
    """
    is_sqlite = _conn_is_sqlite(conn)
    try:
        if is_sqlite:
            res = await conn.exec_driver_sql("PRAGMA table_info(databricksconfig)")
            cols = {row[1] for row in res.fetchall()}
            if not cols:
                return  # table not created yet
            if "ai_gateway_enabled" not in cols:
                await conn.exec_driver_sql(
                    "ALTER TABLE databricksconfig ADD COLUMN ai_gateway_enabled BOOLEAN DEFAULT 0"
                )
                logger.info(
                    "Added databricksconfig.ai_gateway_enabled column (SQLite self-heal)"
                )
        else:
            await conn.exec_driver_sql(
                "ALTER TABLE databricksconfig ADD COLUMN IF NOT EXISTS ai_gateway_enabled BOOLEAN DEFAULT false"
            )
            logger.info("Ensured databricksconfig.ai_gateway_enabled column")
    except Exception as e:
        logger.warning(
            f"Could not ensure databricksconfig.ai_gateway_enabled column: {e}"
        )


async def run_schema_self_heal(conn) -> None:
    """Create missing tables and add missing columns on an existing DB.

    ``create_all`` fully creates a NEW table but never ALTERs an existing one, so
    DBs provisioned before a table/column was added won't have it. Each ``_ensure_*``
    helper here is idempotent and NON-DESTRUCTIVE — ``CREATE TABLE IF NOT EXISTS``
    for tables, ``ADD COLUMN IF NOT EXISTS`` for columns — so this is safe to run
    on every startup and against any engine's connection.

    Runs against BOTH the local/default engine (in ``init_db``) AND, critically,
    the active Lakebase engine after the runtime hot-swap (``main.py`` lifespan) —
    the latter is the only path that heals a customer's PRE-EXISTING Lakebase,
    which ``init_db`` alone misses because it fires before Lakebase activation.

    Each step runs in its own SAVEPOINT. Every helper already catches its own
    exception and logs a warning, which LOOKS like isolation but is not: on
    PostgreSQL a failed statement aborts the whole transaction, so every
    subsequent statement on the same connection dies with
    ``InFailedSQLTransactionError`` — "current transaction is aborted, commands
    ignored until end of transaction block". Swallowing the first error therefore
    converted one skippable failure into a silent, total no-op.

    That is not hypothetical. On the deployed app ``documentation_embeddings`` was
    left owned by a PREVIOUS deploy's service principal, so its ALTER failed with
    ``must be owner of table`` — and because it runs FIRST, it took the other 23
    steps with it. ``agents.thinking_budget_tokens`` was never added, and agent
    creation failed with ``column "thinking_budget_tokens" of relation "agents"
    does not exist``. Rolling back to a savepoint makes each step independently
    skippable, which is what the per-helper try/except was always meant to give.
    """
    steps = (
        _ensure_documentation_embeddings_columns,
        _ensure_databricks_config_columns,
        _ensure_chat_sessions_table,
        _ensure_chat_sessions_columns,
        _ensure_workflow_recipes_table,
        _ensure_workflow_recipe_trials_table,
        _ensure_crew_publications_table,
        _ensure_a2a_push_configs_table,
        _ensure_a2a_agents_table,
        _ensure_skills_tables,
        _ensure_crew_feedback_table,
        _ensure_powerbi_extraction_table,
        _ensure_prompt_optimization_runs_table,
        _ensure_mlflow_config_table,
        _ensure_memory_maintenance_table,
        _ensure_agent_columns,
        _ensure_crew_columns,
        _ensure_execution_history_columns,
        _ensure_ui_config_columns,
        _ensure_modelconfig_columns,
        _ensure_hot_polling_indexes,
        _heal_personal_group_names,
        _heal_engine_config_names,
        _disable_bi_specialist_crew_memory,
    )
    for step in steps:
        await _run_self_heal_step(conn, step)


async def _run_self_heal_step(conn, step) -> None:
    """Run one self-heal step so its failure cannot abort the ones after it.

    ``conn.begin_nested()`` is a SAVEPOINT: releasing it on success keeps the
    work, rolling it back on failure returns the transaction to a usable state.
    Without it a single failed DDL poisons every later step (see
    ``run_schema_self_heal``).

    Falls back to calling the step directly if the connection cannot nest — a
    mock in tests, or a driver without savepoint support. The helpers still log
    their own warnings, so behaviour there is exactly what it was before.
    """
    try:
        nested = conn.begin_nested()
    except Exception:  # noqa: BLE001 — no savepoint support; degrade, don't fail
        await step(conn)
        return
    try:
        async with nested:
            await step(conn)
    except Exception as exc:  # noqa: BLE001 — one broken step must not stop the rest
        # The step logged the cause; this records that it was ISOLATED, which is
        # the difference between "one table skipped" and "nothing healed".
        logger.warning(
            f"Schema self-heal step {step.__name__} rolled back, continuing: {exc}"
        )


async def init_db() -> None:
    """Initialize database tables if they don't exist."""
    try:
        # Import all models to ensure they're registered
        import importlib

        import src.db.all_models

        importlib.reload(src.db.all_models)  # Ensure models are freshly loaded
        from src.db.all_models import Base

        # For PostgreSQL, check if database exists and create if not
        if str(settings.DATABASE_URI).startswith("postgresql"):
            import asyncpg

            # Extract connection parameters
            db_name = settings.POSTGRES_DB
            host = settings.POSTGRES_SERVER
            port = settings.POSTGRES_PORT
            user = settings.POSTGRES_USER
            password = settings.POSTGRES_PASSWORD

            # Defense-in-depth: validate db_name even though it comes from
            # environment settings, to prevent SQL injection via CREATE DATABASE.
            _validate_identifier(db_name, "database name")

            try:
                # First, try to connect to the specified database
                test_conn = await asyncpg.connect(
                    host=host, port=port, user=user, password=password, database=db_name
                )
                await test_conn.close()
                logger.info(f"Database '{db_name}' exists and is accessible")
            except asyncpg.InvalidCatalogNameError:
                # Database doesn't exist, create it
                logger.info(f"Database '{db_name}' does not exist. Creating it...")

                # Connect to postgres database to create the new database
                admin_conn = await asyncpg.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    database="postgres",  # Connect to default postgres database
                )

                try:
                    # Create the database
                    await admin_conn.execute(f'CREATE DATABASE "{db_name}"')
                    logger.info(f"Database '{db_name}' created successfully")
                except asyncpg.DuplicateDatabaseError:
                    logger.info(f"Database '{db_name}' already exists")
                except Exception as e:
                    logger.error(f"Error creating database: {e}")
                    raise
                finally:
                    await admin_conn.close()

        # For SQLite, ensure database file exists
        if str(settings.DATABASE_URI).startswith("sqlite"):
            db_path = settings.SQLITE_DB_PATH

            # Get absolute path if relative
            if not os.path.isabs(db_path):
                # If it's a relative path, make it absolute from current directory
                db_path = os.path.abspath(db_path)

            logger.info(f"Database path: {db_path}")

            # Create directory if it doesn't exist
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                logger.info(f"Creating database directory: {db_dir}")
                os.makedirs(db_dir, exist_ok=True)

            # Create empty database file if it doesn't exist
            if not os.path.exists(db_path):
                logger.info(f"Creating new SQLite database file: {db_path}")
                # Create the file and initialize it
                with open(db_path, "w") as f:
                    pass  # Create empty file

                # Initialize it as a sqlite database
                import sqlite3

                conn = sqlite3.connect(db_path)
                conn.close()
                logger.info(f"Empty database file created at {db_path}")

        # Create all tables in a completely separate, isolated transaction
        logger.info("Creating database tables...")

        # For SQLite, we can verify if tables already exist first
        tables_exist = False
        if str(settings.DATABASE_URI).startswith("sqlite"):
            try:
                import sqlite3

                conn = sqlite3.connect(os.path.abspath(settings.SQLITE_DB_PATH))
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                conn.close()

                if len(tables) > 1:  # SQLite has a sqlite_master table by default
                    logger.info(
                        f"Tables already exist: {', '.join([t[0] for t in tables])}"
                    )
                    tables_exist = True
            except Exception as e:
                logger.error(f"Error checking existing tables: {e}")

        # Only create tables if they don't already exist
        if not tables_exist:
            # Use a fresh engine for initialization with settings optimized for table creation
            init_engine_opts = {
                "echo": SQL_DEBUG,  # Control SQL logging via SQL_DEBUG env var
                "future": True,
            }

            # For SQLite, don't set isolation_level to avoid errors
            if not str(settings.DATABASE_URI).startswith("sqlite"):
                init_engine_opts["isolation_level"] = (
                    "AUTOCOMMIT"  # Use AUTOCOMMIT for table creation
                )
                init_engine_opts["poolclass"] = (
                    NullPool  # Avoid pooling during init to isolate loop
                )

            # Create a dedicated engine just for initialization
            engine_for_init = create_async_engine(
                str(settings.DATABASE_URI), **init_engine_opts
            )

            # First ensure connection works
            async with engine_for_init.connect() as conn:
                logger.info("Database connection established")

            # Ensure pgvector extension (PostgreSQL) before table creation
            if not str(settings.DATABASE_URI).startswith("sqlite"):
                try:
                    logger.info("Checking pgvector extension...")
                    async with engine_for_init.connect() as conn:
                        result = await conn.execute(
                            text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
                        )
                        extension_exists = result.fetchone() is not None
                        if not extension_exists:
                            logger.info("Installing pgvector extension...")
                            await conn.execute(
                                text("CREATE EXTENSION IF NOT EXISTS vector")
                            )
                            await conn.commit()
                            logger.info("pgvector extension installed successfully")
                        else:
                            logger.info("pgvector extension already installed")
                except Exception as e:
                    logger.warning(f"Could not install pgvector extension: {e}")
                    logger.warning(
                        "The database will work but documentation embeddings table will not be created"
                    )

            # Then create tables
            try:
                async with engine_for_init.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                    logger.info("Tables created successfully")
            except Exception as table_error:
                logger.error(f"Error creating tables: {table_error}")
                import traceback

                logger.error(traceback.format_exc())
                raise

            # Close the engine after use
            await engine_for_init.dispose()

            logger.info("Database tables initialized successfully")

        # Self-heal: create missing tables and add missing columns even when the
        # tables already existed (create_all is skipped then). DBs created before
        # these tables/columns were added would otherwise be missing them.
        try:
            ensure_engine = create_async_engine(
                str(settings.DATABASE_URI), future=True, echo=SQL_DEBUG
            )
            try:
                async with ensure_engine.begin() as conn:
                    await run_schema_self_heal(conn)
            finally:
                await ensure_engine.dispose()
        except Exception as ensure_err:
            logger.warning(f"column ensure skipped: {ensure_err}")

        # Verify tables were created for SQLite
        if str(settings.DATABASE_URI).startswith("sqlite"):

            import sqlite3

            try:
                db_path_to_check = os.path.abspath(settings.SQLITE_DB_PATH)
                logger.info(f"Verifying tables in: {db_path_to_check}")

                if (
                    os.path.exists(db_path_to_check)
                    and os.path.getsize(db_path_to_check) > 0
                ):
                    conn = sqlite3.connect(db_path_to_check)
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    tables = cursor.fetchall()
                    conn.close()

                    table_count = len(tables)
                    logger.info(
                        f"Verified {table_count} tables in database: {', '.join([t[0] for t in tables])}"
                    )
                    if table_count == 0:
                        logger.error("No tables were created in the database!")
                else:
                    logger.error(
                        f"Database file not found or empty after initialization: {db_path_to_check}"
                    )
            except Exception as e:
                logger.error(f"Error verifying tables: {e}")
                import traceback

                logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        # Print full traceback for debugging
        import traceback

        logger.error(traceback.format_exc())
        raise


def get_smart_engine():
    """
    Intelligently select the right engine based on the current context.

    Returns:
        - Pooled engine for main FastAPI app (best performance)
        - NullPool engine for background tasks/CrewAI (event loop isolation)
    """
    # If SQLite, always return the same engine
    if str(settings.DATABASE_URI).startswith("sqlite"):
        return engine

    # For PostgreSQL, check if we can detect the context
    try:
        current_loop = asyncio.get_running_loop()

        # Check if we're in a background task (different event loop)
        if main_event_loop and current_loop != main_event_loop:
            logger.debug(
                f"Background task detected (loop {id(current_loop)} != main {id(main_event_loop)}) - using NullPool"
            )
            return nullpool_engine
        else:
            # We're in the main event loop - use pooled engine for performance
            logger.debug(
                f"Main app context detected (loop {id(current_loop)}) - using pooled engine"
            )
            return pooled_engine
    except RuntimeError:
        # No event loop running - probably sync context
        logger.debug("No event loop detected - using NullPool for safety")
        return nullpool_engine
    except Exception as e:
        logger.warning(f"Error detecting context: {e} - falling back to default engine")
        return engine


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency function that yields db sessions with smart engine selection.

    Uses the appropriate session factory based on context:
    - Pooled session for FastAPI requests (best performance)
    - NullPool session for background tasks (event loop isolation)

    Yields:
        AsyncSession: SQLAlchemy async session
    """
    # Default to async_session_factory; in tests (pytest), this allows monkeypatching
    smart_session_factory = async_session_factory

    # In normal runtime (not under pytest), select appropriate factory by context
    if not os.environ.get("PYTEST_CURRENT_TEST"):
        if str(settings.DATABASE_URI).startswith("sqlite"):
            # SQLite always uses the same session factory
            smart_session_factory = async_session_factory
        else:
            # PostgreSQL: ALWAYS use smart selection regardless of USE_NULLPOOL setting
            try:
                current_loop = asyncio.get_running_loop()

                # Check if we're in a background task (different event loop)
                if main_event_loop and current_loop != main_event_loop:
                    logger.debug(
                        f"Background context (loop {id(current_loop)}) - using NullPool session"
                    )
                    smart_session_factory = nullpool_session_factory
                else:
                    # We're in the main event loop - ALWAYS use pooled sessions for performance!
                    logger.debug(
                        f"Main app context (loop {id(current_loop)}) - using POOLED session"
                    )
                    # CRITICAL: Always use pooled_session_factory for main app, not async_session_factory
                    smart_session_factory = pooled_session_factory
            except RuntimeError:
                # No event loop running - use NullPool for safety
                logger.debug("No event loop - using NullPool session")
                smart_session_factory = nullpool_session_factory
            except Exception as e:
                logger.warning(
                    f"Error detecting context: {e} - using NullPool for safety"
                )
                # Fallback to NullPool for safety (not the default factory which might be wrong)
                smart_session_factory = nullpool_session_factory

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Use the selected session factory
            async with smart_session_factory() as session:
                # Publish session into ContextVar so that
                # request_scoped_session() returns the same session
                token = _request_session.set(session)
                try:
                    yield session
                    # Commit the transaction if no exception occurred
                    await session.commit()
                    return  # Success, exit retry loop
                except OperationalError as e:
                    # Rollback on any exception
                    await session.rollback()
                    if (
                        "database is locked" in str(e).lower()
                        and attempt < max_retries - 1
                    ):
                        wait_time = 0.1 * (2.0**attempt)
                        logger.warning(
                            f"Database locked in session, retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    raise
                except Exception:
                    # Rollback on any exception
                    await session.rollback()
                    raise
                finally:
                    try:
                        _request_session.reset(token)
                    except ValueError:
                        # Token created in a different async context (generator
                        # GC'd or cancelled across tasks). Safe to ignore.
                        pass
                    # Ensure session is properly closed
                    await session.close()
        except OperationalError as e:
            if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                wait_time = 0.1 * (2.0**attempt)
                logger.warning(
                    f"Database locked creating session, retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(wait_time)
                continue
            raise


async def get_local_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield a session connected to the LOCAL database (SQLite/PG),
    bypassing the Lakebase swap.  Used for bootstrap config tables
    like database_configs that are never migrated to Lakebase."""
    async with _local_session_factory() as session:
        token = _request_session.set(session)
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            try:
                _request_session.reset(token)
            except ValueError:
                pass
            await session.close()


# get_sync_db removed - use get_db() instead
# All database operations must be async


# Graceful engine disposal to avoid event-loop mismatch on shutdown
async def dispose_engines() -> None:
    """
    Dispose all async engines/pools while the FastAPI event loop is still alive.
    This prevents asyncpg from attempting to close connections on a different
    loop during interpreter shutdown ("Future attached to a different loop").

    Also disposes the global Lakebase session factory to ensure a clean
    switch between database backends (SQLite/PG <-> Lakebase).
    """
    try:
        engines = []
        try:
            engines.append(engine)
        except Exception:
            pass
        try:
            if "pooled_engine" in globals():
                engines.append(pooled_engine)
        except Exception:
            pass
        try:
            if "nullpool_engine" in globals():
                engines.append(nullpool_engine)
        except Exception:
            pass
        try:
            if _isolated_sqlite_engine is not None:
                engines.append(_isolated_sqlite_engine)
        except Exception:
            pass

        # De-duplicate in case some references are the same (e.g., SQLite)
        seen = set()
        unique_engines = []
        for eng in engines:
            if eng is not None and id(eng) not in seen:
                seen.add(id(eng))
                unique_engines.append(eng)

        for eng in unique_engines:
            try:
                logger.info(f"Disposing SQLAlchemy engine {eng}...")
                await eng.dispose()
            except Exception as e:
                logger.warning(f"Error disposing engine {eng}: {e}")

        # Also dispose the Lakebase factory to force fresh connections
        # on the next request after a backend switch
        try:
            from src.db.lakebase_session import dispose_lakebase_factory

            await dispose_lakebase_factory()
        except Exception as e:
            logger.warning(f"Error disposing Lakebase factory: {e}")
    except Exception as outer_e:
        logger.warning(f"dispose_engines encountered an error: {outer_e}")
