from typing import AsyncGenerator, Generator
import os
import logging
from pathlib import Path
from datetime import datetime, timezone
import sys
import asyncio
import time
from functools import wraps

from sqlalchemy import create_engine, text, event
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import sessionmaker
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

# Get a logger from the LoggerManager system
logger = logging.getLogger(__name__)

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
        
        # Ensure the sqlalchemy engine logger doesn't propagate logs to stdout
        engine_logger = logging.getLogger('sqlalchemy.engine')
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
        
# Initialize SQLAlchemy logging
sql_logger = SQLAlchemyLogger()

# Import pool classes
from sqlalchemy.pool import NullPool, AsyncAdaptedQueuePool, StaticPool

# Determine if we should use NullPool for event loop isolation
# This is necessary when running in environments with multiple event loops
# such as with CrewAI memory backends or during testing
use_nullpool = os.environ.get("USE_NULLPOOL", "false").lower() == "true"

# Determine isolation level based on database type
def get_isolation_level(database_uri: str) -> str:
    """Get appropriate isolation level based on database type."""
    if database_uri.startswith('sqlite'):
        # SQLite supports: READ UNCOMMITTED, SERIALIZABLE, AUTOCOMMIT
        return "SERIALIZABLE"
    else:
        # PostgreSQL supports: READ COMMITTED, READ UNCOMMITTED, REPEATABLE READ, SERIALIZABLE
        return "READ COMMITTED"

def get_sqlite_connect_args(database_uri: str) -> dict:
    """Get SQLite-specific connection arguments for better concurrent access."""
    if database_uri.startswith('sqlite'):
        return {
            "check_same_thread": False,  # Allow SQLite to be used across threads
            "timeout": 30,  # 30 second timeout for locked database
            "isolation_level": None,  # Use autocommit mode to reduce lock time
        }
    return {}

isolation_level = get_isolation_level(str(settings.DATABASE_URI))
connect_args = get_sqlite_connect_args(str(settings.DATABASE_URI))

# Create async engine for the database
# Force NullPool for SQLite to prevent locking issues
if str(settings.DATABASE_URI).startswith('sqlite') or use_nullpool:
    logger.info("Using NullPool for SQLite to prevent database locking issues")
    engine = create_async_engine(
        str(settings.DATABASE_URI),
        echo=False,  # Disable SQL logging for better performance
        future=True,
        poolclass=NullPool,  # Disable connection pooling to prevent locking conflicts
        # Set isolation level based on database type
        isolation_level=isolation_level,
        echo_pool=False,  # Disable pool logging for better performance
        connect_args=connect_args,  # SQLite-specific connection arguments
    )
else:
    # Use the default async pool (AsyncAdaptedQueuePool is automatically used)
    engine = create_async_engine(
        str(settings.DATABASE_URI),
        echo=False,  # Disable SQL logging for better performance
        future=True,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        # Set isolation level based on database type
        isolation_level=isolation_level,
        echo_pool=False,  # Disable pool logging for better performance
        connect_args=connect_args,  # SQLite-specific connection arguments
    )

# Configure SQLite for better concurrent access
def configure_sqlite(dbapi_connection, connection_record):
    """Configure SQLite connection for better performance and concurrency."""
    if str(settings.DATABASE_URI).startswith('sqlite'):
        try:
            # Enable WAL mode for better concurrent access
            dbapi_connection.execute("PRAGMA journal_mode=WAL")
            # Set busy timeout - CRITICAL for handling locks
            dbapi_connection.execute("PRAGMA busy_timeout=30000")
            # Enable foreign keys
            dbapi_connection.execute("PRAGMA foreign_keys=ON")
            # Optimize for concurrent access
            dbapi_connection.execute("PRAGMA synchronous=NORMAL")
            dbapi_connection.execute("PRAGMA cache_size=10000")
            dbapi_connection.execute("PRAGMA temp_store=memory")
            # Additional concurrency settings
            dbapi_connection.execute("PRAGMA wal_autocheckpoint=1000")
            dbapi_connection.execute("PRAGMA wal_checkpoint_timeout=5000")
            logger.info("SQLite connection configured with WAL mode and 30s timeout")
        except Exception as e:
            logger.error(f"Failed to configure SQLite connection: {e}")

# Apply SQLite configuration to both engines
if str(settings.DATABASE_URI).startswith('sqlite'):
    # For async engine, we need to listen to the sync_engine property
    event.listen(engine.sync_engine, "connect", configure_sqlite)
    logger.info("Applied SQLite configuration event listener to async engine")

# Create sync engine for backwards compatibility if needed
# Since asyncpg is async-only and cannot be used for sync operations,
# we'll use SQLite for sync operations when using PostgreSQL+asyncpg for async
if str(settings.SYNC_DATABASE_URI).startswith("postgresql+asyncpg://"):
    # asyncpg cannot be used for sync operations, use SQLite instead
    logger.info("Using SQLite for sync operations since asyncpg is async-only")
    sync_sqlite_uri = f"sqlite:///{settings.SQLITE_DB_PATH}"
    sync_engine = create_engine(
        sync_sqlite_uri,
        echo=False,
        future=True,
        poolclass=NullPool,  # Use NullPool for SQLite
        connect_args=connect_args,
    )
else:
    # For other database configurations, use the configured sync URI
    # For SQLite sync operations, also use NullPool
    if str(settings.SYNC_DATABASE_URI).startswith('sqlite'):
        sync_engine = create_engine(
            str(settings.SYNC_DATABASE_URI),
            echo=False,
            future=True,
            poolclass=NullPool,  # Use NullPool for SQLite
            connect_args=connect_args,
        )
    else:
        sync_engine = create_engine(
            str(settings.SYNC_DATABASE_URI),
            echo=False,
            future=True,
            connect_args=connect_args,
        )

# Apply SQLite configuration to sync engine as well
if str(settings.SYNC_DATABASE_URI).startswith('sqlite') or str(settings.SYNC_DATABASE_URI).startswith("postgresql+asyncpg://"):
    event.listen(sync_engine, "connect", configure_sqlite)
    logger.info("Applied SQLite configuration event listener to sync engine")

# Create async session factory
async_session_factory = async_sessionmaker(
    engine,
    expire_on_commit=False,
    autoflush=True,  # Enable autoflush to ensure changes are flushed to DB before queries
)

# Create sync session factory for backward compatibility
SessionLocal = sessionmaker(
    bind=sync_engine,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)

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
            
            # Ensure pgvector extension is installed
            try:
                logger.info("Checking pgvector extension...")
                async with engine.connect() as conn:
                    # Check if pgvector extension exists
                    result = await conn.execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector'"))
                    extension_exists = result.fetchone() is not None
                    
                    if not extension_exists:
                        logger.info("Installing pgvector extension...")
                        # Create the extension
                        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                        await conn.commit()
                        logger.info("pgvector extension installed successfully")
                    else:
                        logger.info("pgvector extension already installed")
            except Exception as e:
                logger.warning(f"Could not install pgvector extension: {e}")
                logger.warning("The database will work but documentation embeddings table will not be created")
        
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
                "isolation_level": "AUTOCOMMIT",  # Use AUTOCOMMIT for table creation
                "echo": False,  # Disable detailed logging for better performance
                "future": True,
            }
            
            # Create a dedicated engine just for initialization
            engine_for_init = create_async_engine(
                str(settings.DATABASE_URI),
                **init_engine_opts
            )
            
            # First ensure connection works
            async with engine_for_init.connect() as conn:
                logger.info("Database connection established")
            
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

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency function that yields db sessions with retry logic for SQLite locks.

    Yields:
        AsyncSession: SQLAlchemy async session
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # async_session_factory is a sessionmaker, call it to get a session
            # Using async with ensures proper cleanup
            async with async_session_factory() as session:
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
                # Session will auto-close when exiting the async context manager
        except OperationalError as e:
            if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                wait_time = 0.1 * (2.0 ** attempt)
                logger.warning(f"Database locked creating session, retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(wait_time)
                continue
            raise

# Keep the sync version for backward compatibility
def get_sync_db() -> Generator[SessionLocal, None, None]:
    """Synchronous database session for backward compatibility"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 