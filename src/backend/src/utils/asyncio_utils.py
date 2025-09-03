"""
Utilities for event loop management and handling asyncio operations across threads.
"""
import asyncio
import logging
from typing import Any, Callable, List, TypeVar, Coroutine

from src.core.logger import LoggerManager
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system

# Type variable for the return value of the database operation
T = TypeVar('T')

async def execute_db_operation_with_fresh_engine(operation: Callable[[AsyncSession], Coroutine[Any, Any, T]]) -> T:
    """
    Execute a database operation with a fresh engine and session to avoid event loop issues.
    
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
    
    # Create a new engine for this operation
    engine = create_async_engine(
        str(settings.DATABASE_URI),
        echo=False,
        future=True,
        pool_pre_ping=True,
    )
    
    # Create a new session factory
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autoflush=False,
    )
    
    try:
        # Create a session and execute the operation
        async with session_factory() as session:
            result = await operation(session)
            return result
    except Exception as e:
        logger.error(f"Error executing DB operation with fresh engine: {str(e)}")
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