from typing import AsyncGenerator, Generator
import os
import logging
from pathlib import Path
from datetime import datetime, timezone
import sys
import asyncio
import time
from functools import wraps

from sqlalchemy import text, event
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.exc import OperationalError

from src.config.settings import settings
from src.db.base import Base
from src.core.logger import LoggerManager

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
    os.environ.get("SQL_DEBUG", "false").lower() == "true" or
    os.environ.get("KASAL_DEBUG_ALL", "false").lower() == "true" or
    os.environ.get("KASAL_LOG_DATABASE", "").upper() == "DEBUG"
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
                    if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                        wait_time = delay * (backoff ** attempt)
                        logger.warning(f"Database locked, retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})")
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
                    if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                        wait_time = delay * (backoff ** attempt)
                        logger.warning(f"Database locked, retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})")
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
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.log_dir = logger_manager._log_dir
        self.setup_logger()

    def setup_logger(self):
        # Create sqlalchemy.log file handler
        sqlalchemy_log_file = self.log_dir / "sqlalchemy.log"

        # Get the sqlalchemy engine logger
        engine_logger = logging.getLogger('sqlalchemy.engine')

        # When SQL_DEBUG is enabled, we want both console and file output
        if SQL_DEBUG:
            engine_logger.setLevel(logging.INFO)
            # Clear existing handlers to avoid duplicates
            engine_logger.handlers = []
            engine_logger.propagate = False

            # Add console handler for immediate visibility
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter('[SQL] %(message)s'))
            engine_logger.addHandler(console_handler)

            # Also add file handler for persistent logging
            file_handler = logging.handlers.RotatingFileHandler(
                sqlalchemy_log_file,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
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
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
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
from sqlalchemy.pool import NullPool, AsyncAdaptedQueuePool, StaticPool

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
    if database_uri.startswith('sqlite'):
        # SQLite with SQLAlchemy: Use None for autocommit behavior with async
        # This allows SQLite to handle transactions automatically
        return None
    else:
        # PostgreSQL supports: READ COMMITTED, READ UNCOMMITTED, REPEATABLE READ, SERIALIZABLE
        return "READ COMMITTED"

def get_sqlite_connect_args(database_uri: str) -> dict:
    """Get SQLite-specific connection arguments for better concurrent access."""
    if database_uri.startswith('sqlite'):
        return {
            "check_same_thread": False,  # Allow SQLite to be used across threads
            "timeout": 60,  # Increase to 60 seconds for heavy operations
            # Note: isolation_level is set at engine level, not in connect_args for SQLite
        }
    return {}

isolation_level = get_isolation_level(str(settings.DATABASE_URI))
connect_args = get_sqlite_connect_args(str(settings.DATABASE_URI))

# Create intelligent dual-engine setup for optimal performance
# Strategy: Use pooled connections for main app, NullPool for background tasks

if str(settings.DATABASE_URI).startswith('sqlite'):
    # SQLite: Use StaticPool for both contexts (single connection)
    logger.info("SQLite detected - using StaticPool for single connection reuse")
    engine = create_async_engine(
        str(settings.DATABASE_URI),
        echo=SQL_DEBUG,
        future=True,
        poolclass=StaticPool,
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
    logger.info("PostgreSQL detected - creating dual-engine setup for optimal performance")
    logger.info("Main app will use pooled connections, background tasks will use NullPool")
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
        "isolation_level": isolation_level
    }
    pooled_engine = create_async_engine(str(settings.DATABASE_URI), **pooled_engine_opts)
    logger.info("Created pooled engine for main FastAPI app (20x better performance)")

    # 2. Create NULLPOOL engine for background tasks/CrewAI (event loop isolation)
    nullpool_engine_opts = {
        "echo": SQL_DEBUG,
        "future": True,
        "poolclass": NullPool,  # No pooling - new connection per query
        "connect_args": connect_args,
        "isolation_level": isolation_level
    }
    nullpool_engine = create_async_engine(str(settings.DATABASE_URI), **nullpool_engine_opts)
    logger.info("Created NullPool engine for background tasks (CrewAI compatibility)")

    # Default engine selection
    # When USE_NULLPOOL=true, default to NullPool to avoid cross-loop issues under reload
    # get_db() still routes to pooled sessions where safe (main app loop) when USE_NULLPOOL=false
    if use_nullpool:
        logger.info("USE_NULLPOOL=true - Event loop isolation mode enabled (defaulting to NullPool engine)")
        engine = nullpool_engine
    else:
        logger.info("USE_NULLPOOL=false - Full pooling mode enabled for maximum performance")
        engine = pooled_engine

# Configure SQLite for better concurrent access
def configure_sqlite(dbapi_connection, connection_record):
    """Configure SQLite connection for better performance and concurrency."""
    if str(settings.DATABASE_URI).startswith('sqlite'):
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
            logger.info("SQLite configured with WAL mode and optimizations")
        except Exception as e:
            logger.error(f"Failed to configure SQLite connection: {e}")

# Apply SQLite configuration to all engines
if str(settings.DATABASE_URI).startswith('sqlite'):
    # For async engine, we need to listen to the sync_engine property
    event.listen(engine.sync_engine, "connect", configure_sqlite)
    logger.info("Applied SQLite configuration event listener to main engine")

    # Also apply to pooled and nullpool engines if they exist (PostgreSQL only)
    if 'pooled_engine' in locals():
        event.listen(pooled_engine.sync_engine, "connect", configure_sqlite)
        logger.info("Applied SQLite configuration to pooled engine")
    if 'nullpool_engine' in locals():
        event.listen(nullpool_engine.sync_engine, "connect", configure_sqlite)
        logger.info("Applied SQLite configuration to nullpool engine")

# Sync engine removed - everything is async now
# All database operations must use async sessions

# Create session factories for both engines (avoid recreating on each request)
# Main session factory (uses default engine based on USE_NULLPOOL setting)
async_session_factory = async_sessionmaker(
    engine,
    expire_on_commit=False,
    autoflush=False,  # Disable autoflush to prevent SQLite locking issues
    autocommit=False,  # Explicit transaction control
)

# Create separate session factories for pooled and nullpool engines
pooled_session_factory = None
nullpool_session_factory = None

if not str(settings.DATABASE_URI).startswith('sqlite'):
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

# SessionLocal removed - use async_session_factory instead
# All database operations must be async

# Database initialization
async def init_db() -> None:
    """Initialize database tables if they don't exist."""
    try:
        # Import all models to ensure they're registered
        import importlib
        import src.db.all_models
        importlib.reload(src.db.all_models)  # Ensure models are freshly loaded
        from src.db.all_models import Base

        # For PostgreSQL, check if database exists and create if not
        if str(settings.DATABASE_URI).startswith('postgresql'):
            import asyncpg

            # Extract connection parameters
            db_name = settings.POSTGRES_DB
            host = settings.POSTGRES_SERVER
            port = settings.POSTGRES_PORT
            user = settings.POSTGRES_USER
            password = settings.POSTGRES_PASSWORD

            try:
                # First, try to connect to the specified database
                test_conn = await asyncpg.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    database=db_name
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
                    database='postgres'  # Connect to default postgres database
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
        if str(settings.DATABASE_URI).startswith('sqlite'):
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
                with open(db_path, 'w') as f:
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
        if str(settings.DATABASE_URI).startswith('sqlite'):
            try:
                import sqlite3
                conn = sqlite3.connect(os.path.abspath(settings.SQLITE_DB_PATH))
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                conn.close()

                if len(tables) > 1:  # SQLite has a sqlite_master table by default
                    logger.info(f"Tables already exist: {', '.join([t[0] for t in tables])}")
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
            if not str(settings.DATABASE_URI).startswith('sqlite'):
                init_engine_opts['isolation_level'] = 'AUTOCOMMIT'  # Use AUTOCOMMIT for table creation
                init_engine_opts['poolclass'] = NullPool  # Avoid pooling during init to isolate loop

            # Create a dedicated engine just for initialization
            engine_for_init = create_async_engine(
                str(settings.DATABASE_URI),
                **init_engine_opts
            )

            # First ensure connection works
            async with engine_for_init.connect() as conn:
                logger.info("Database connection established")

            # Ensure pgvector extension (PostgreSQL) before table creation
            if not str(settings.DATABASE_URI).startswith('sqlite'):
                try:
                    logger.info("Checking pgvector extension...")
                    async with engine_for_init.connect() as conn:
                        result = await conn.execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector'"))
                        extension_exists = result.fetchone() is not None
                        if not extension_exists:
                            logger.info("Installing pgvector extension...")
                            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                            await conn.commit()
                            logger.info("pgvector extension installed successfully")
                        else:
                            logger.info("pgvector extension already installed")
                except Exception as e:
                    logger.warning(f"Could not install pgvector extension: {e}")
                    logger.warning("The database will work but documentation embeddings table will not be created")

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

        # Verify tables were created for SQLite
        if str(settings.DATABASE_URI).startswith('sqlite'):

            import sqlite3
            try:
                db_path_to_check = os.path.abspath(settings.SQLITE_DB_PATH)
                logger.info(f"Verifying tables in: {db_path_to_check}")

                if os.path.exists(db_path_to_check) and os.path.getsize(db_path_to_check) > 0:
                    conn = sqlite3.connect(db_path_to_check)
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    tables = cursor.fetchall()
                    conn.close()

                    table_count = len(tables)
                    logger.info(f"Verified {table_count} tables in database: {', '.join([t[0] for t in tables])}")
                    if table_count == 0:
                        logger.error("No tables were created in the database!")
                else:
                    logger.error(f"Database file not found or empty after initialization: {db_path_to_check}")
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
    if str(settings.DATABASE_URI).startswith('sqlite'):
        return engine

    # For PostgreSQL, check if we can detect the context
    try:
        current_loop = asyncio.get_running_loop()

        # Check if we're in a background task (different event loop)
        if main_event_loop and current_loop != main_event_loop:
            logger.debug(f"Background task detected (loop {id(current_loop)} != main {id(main_event_loop)}) - using NullPool")
            return nullpool_engine
        else:
            # We're in the main event loop - use pooled engine for performance
            logger.debug(f"Main app context detected (loop {id(current_loop)}) - using pooled engine")
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
        if str(settings.DATABASE_URI).startswith('sqlite'):
            # SQLite always uses the same session factory
            smart_session_factory = async_session_factory
        else:
            # PostgreSQL: ALWAYS use smart selection regardless of USE_NULLPOOL setting
            try:
                current_loop = asyncio.get_running_loop()

                # Check if we're in a background task (different event loop)
                if main_event_loop and current_loop != main_event_loop:
                    logger.debug(f"Background context (loop {id(current_loop)}) - using NullPool session")
                    smart_session_factory = nullpool_session_factory
                else:
                    # We're in the main event loop - ALWAYS use pooled sessions for performance!
                    logger.debug(f"Main app context (loop {id(current_loop)}) - using POOLED session")
                    # CRITICAL: Always use pooled_session_factory for main app, not async_session_factory
                    smart_session_factory = pooled_session_factory
            except RuntimeError:
                # No event loop running - use NullPool for safety
                logger.debug("No event loop - using NullPool session")
                smart_session_factory = nullpool_session_factory
            except Exception as e:
                logger.warning(f"Error detecting context: {e} - using NullPool for safety")
                # Fallback to NullPool for safety (not the default factory which might be wrong)
                smart_session_factory = nullpool_session_factory

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Use the selected session factory
            async with smart_session_factory() as session:
                try:
                    yield session
                    # Commit the transaction if no exception occurred
                    await session.commit()
                    return  # Success, exit retry loop
                except OperationalError as e:
                    # Rollback on any exception
                    await session.rollback()
                    if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                        wait_time = 0.1 * (2.0 ** attempt)
                        logger.warning(f"Database locked in session, retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    raise
                except Exception:
                    # Rollback on any exception
                    await session.rollback()
                    raise
                finally:
                    # Ensure session is properly closed
                    await session.close()
        except OperationalError as e:
            if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                wait_time = 0.1 * (2.0 ** attempt)
                logger.warning(f"Database locked creating session, retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(wait_time)
                continue
            raise

# get_sync_db removed - use get_db() instead
# All database operations must be async

# Graceful engine disposal to avoid event-loop mismatch on shutdown
async def dispose_engines() -> None:
    """
    Dispose all async engines/pools while the FastAPI event loop is still alive.
    This prevents asyncpg from attempting to close connections on a different
    loop during interpreter shutdown ("Future attached to a different loop").
    """
    try:
        engines = []
        try:
            engines.append(engine)
        except Exception:
            pass
        try:
            if 'pooled_engine' in globals():
                engines.append(pooled_engine)
        except Exception:
            pass
        try:
            if 'nullpool_engine' in globals():
                engines.append(nullpool_engine)
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
    except Exception as outer_e:
        logger.warning(f"dispose_engines encountered an error: {outer_e}")
