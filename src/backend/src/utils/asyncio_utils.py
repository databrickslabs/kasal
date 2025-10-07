"""
Utilities for event loop management and handling asyncio operations across threads.
"""
import asyncio
import logging
from typing import Any, Callable, List, TypeVar, Coroutine

from src.core.logger import LoggerManager
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import StaticPool, NullPool
from sqlalchemy import event

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system

# Type variable for the return value of the database operation
T = TypeVar('T')

async def execute_db_operation_with_fresh_engine(operation: Callable[[AsyncSession], Coroutine[Any, Any, T]]) -> T:
    """
    Execute a database operation with a fresh engine and session to avoid event loop issues.

    IMPORTANT: Uses the same database configuration as the main application to prevent
    SQLite locking issues and disk I/O errors. For SQLite, this means:
    - StaticPool for single connection reuse
    - WAL mode for better concurrency
    - Increased timeout (60s) for heavy operations

    Args:
        operation: A callable that takes an AsyncSession and returns a coroutine

    Returns:
        The result of the operation

    Example:
        ```
        async def get_user(session, user_id):
            # Database operation
            return await session.execute(...)

        result = await execute_db_operation_with_fresh_engine(
            lambda session: get_user(session, 123)
        )
        ```
    """
    # Import here to avoid circular imports
    from src.config.settings import settings
    from src.db.session import get_isolation_level, get_sqlite_connect_args

    database_uri = str(settings.DATABASE_URI)
    is_sqlite = database_uri.startswith('sqlite')

    # Get database-specific configuration
    isolation_level = get_isolation_level(database_uri)
    connect_args = get_sqlite_connect_args(database_uri)

    # Create engine with proper configuration based on database type
    if is_sqlite:
        # SQLite: Use StaticPool to match main application configuration
        # This prevents locking issues and disk I/O errors by reusing a single connection
        logger.debug("Creating fresh SQLite engine with StaticPool and WAL mode")
        engine = create_async_engine(
            database_uri,
            echo=False,
            future=True,
            poolclass=StaticPool,  # Single connection reuse - critical for SQLite
            connect_args={
                **connect_args,
                "check_same_thread": False,  # Allow cross-thread access
            },
        )

        # Configure SQLite connection with WAL mode and optimizations
        # This matches the configuration in session.py
        @event.listens_for(engine.sync_engine, "connect")
        def configure_sqlite_connection(dbapi_connection, connection_record):
            """Configure SQLite connection with WAL mode and performance optimizations."""
            try:
                # Enable foreign keys
                dbapi_connection.execute("PRAGMA foreign_keys=ON")
                # Use WAL mode for better concurrency
                dbapi_connection.execute("PRAGMA journal_mode=WAL")
                # Optimize for faster writes with NORMAL synchronous (safe with WAL)
                dbapi_connection.execute("PRAGMA synchronous=NORMAL")
                # Increase cache for better performance
                dbapi_connection.execute("PRAGMA cache_size=-64000")  # 64MB cache
                # Enable memory-mapped I/O for better performance
                dbapi_connection.execute("PRAGMA mmap_size=268435456")  # 256MB mmap
                # Auto checkpoint at 1000 pages to balance performance and WAL size
                dbapi_connection.execute("PRAGMA wal_autocheckpoint=1000")
                logger.debug("SQLite connection configured with WAL mode and optimizations")
            except Exception as e:
                logger.error(f"Failed to configure SQLite connection: {e}")
    else:
        # PostgreSQL: Use NullPool for event loop isolation
        # NullPool creates a new connection for each checkout, avoiding cross-loop issues
        logger.debug("Creating fresh PostgreSQL engine with NullPool")
        engine = create_async_engine(
            database_uri,
            echo=False,
            future=True,
            poolclass=NullPool,  # No pooling - new connection per query
            connect_args=connect_args,
            isolation_level=isolation_level,
        )

    # Create a new session factory
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autoflush=False,  # Disable autoflush to prevent SQLite locking issues
    )

    try:
        # Create a session and execute the operation
        async with session_factory() as session:
            result = await operation(session)
            return result
    except Exception as e:
        logger.error(f"Error executing DB operation with fresh engine: {str(e)}", exc_info=True)
        raise
    finally:
        # Always dispose the engine
        await engine.dispose()

def create_and_run_loop(coroutine: Any) -> Any:
    """Create a new event loop, run the coroutine, and clean up properly."""
    new_loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(new_loop)
        result = new_loop.run_until_complete(coroutine)
        return result
    finally:
        # Properly clean up the event loop
        try:
            # Close all running event loop tasks
            pending = asyncio.all_tasks(new_loop) if hasattr(asyncio, 'all_tasks') else []
            for task in pending:
                task.cancel()
            # Run the event loop until all tasks are canceled
            if pending:
                new_loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            # Remove the loop from the current context and close it
            asyncio.set_event_loop(None)
            new_loop.close()
        except Exception as e:
            logger.error(f"Error cleaning up event loop: {str(e)}")

def create_task_lifecycle_callback(loop_handler: Callable, callbacks: List, task_key: str) -> Callable:
    """Create a callback for task lifecycle events with proper event loop handling."""
    def callback_function(task_obj, success=True):
        logger.info(f"Task event for {task_key} (success: {success})")
        # Create a new event loop for the callback
        new_loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(new_loop)
            for callback in callbacks:
                try:
                    if hasattr(callback, loop_handler):
                        logger.info(f"Calling {loop_handler} for {callback.__class__.__name__}")
                        handler = getattr(callback, loop_handler)
                        if loop_handler == 'on_task_end':
                            new_loop.run_until_complete(handler(task_obj, success))
                        else:
                            new_loop.run_until_complete(handler(task_obj))
                except Exception as callback_error:
                    logger.error(f"Error in {loop_handler}: {callback_error}")
                    logger.error("Stack trace:", exc_info=True)
                    # Continue with other callbacks even if one fails
        finally:
            # Properly clean up the event loop
            try:
                # Close all running event loop tasks
                pending = asyncio.all_tasks(new_loop) if hasattr(asyncio, 'all_tasks') else []
                for task in pending:
                    task.cancel()
                # Run the event loop until all tasks are canceled
                if pending:
                    new_loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                # Remove the loop from the current context and close it
                asyncio.set_event_loop(None)
                new_loop.close()
            except Exception as e:
                logger.error(f"Error cleaning up {loop_handler} event loop: {str(e)}")
    
    return callback_function

def run_in_thread_with_loop(func: Callable, *args, **kwargs) -> Any:
    """Run a function in a thread with a properly managed event loop."""
    # Track whether we created a new event loop
    created_loop = False
    loop = None
    
    try:
        # Set up event loop for this thread
        try:
            # Try to get the current event loop
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # If there's no event loop in this thread, create one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            created_loop = True
            logger.info("Created new event loop for thread execution")

        # Execute the function
        return func(*args, **kwargs)
    
    finally:
        # Clean up the event loop only if we created it
        if created_loop and loop is not None:
            try:
                # Only close the loop if we created it
                asyncio.set_event_loop(None)
                loop.close()
                logger.info("Successfully closed the event loop created for this thread")
            except Exception as e:
                logger.error(f"Error cleaning up event loop: {str(e)}") 