"""
Database router that automatically selects between regular database (PostgreSQL/SQLite) and Lakebase.

This module provides a routing mechanism to dynamically choose the appropriate database
backend based on configuration stored in the database itself.
"""
import os
import logging
from typing import AsyncGenerator, Optional, Dict, Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.db.session import async_session_factory
from src.db.lakebase_session import get_lakebase_session
from src.core.logger import LoggerManager

logger_manager = LoggerManager.get_instance()
logger = logger_manager.database


async def get_lakebase_config_from_db() -> Optional[Dict[str, Any]]:
    """
    Get Lakebase configuration from the database.

    Returns:
        Lakebase configuration dictionary or None if not found
    """
    try:
        # Use direct session factory to avoid circular dependency
        from src.db.session import get_db
        async for session in get_db():
            from src.models.database_config import LakebaseConfig

            # Query for Lakebase configuration
            stmt = select(LakebaseConfig).where(
                LakebaseConfig.key == "lakebase"
            )
            result = await session.execute(stmt)
            config_entry = result.scalar_one_or_none()

            if config_entry and config_entry.value:
                return config_entry.value

            # No configuration found means Lakebase is disabled
            return None
            # Note: The async for loop will automatically close the session
    except Exception as e:
        logger.debug(f"Could not read Lakebase config from database: {e}")
        return None


async def is_lakebase_enabled() -> bool:
    """
    Check if Lakebase is enabled and configured.
    """
    # Check environment variable first (highest priority)
    if os.environ.get("USE_LAKEBASE", "").lower() == "true":
        return True

    if os.environ.get("DISABLE_LAKEBASE", "").lower() == "true":
        return False

    # For fresh deployments or when database tables don't exist yet,
    # default to regular database to avoid circular dependency
    try:
        # Get configuration from database
        config = await get_lakebase_config_from_db()

        if config:
            return (
                config.get("enabled", False) and
                config.get("endpoint") and
                config.get("migration_completed", False)
            )
    except Exception as e:
        # If we can't read the database config (e.g., tables don't exist yet),
        # default to regular database
        logger.debug(f"Cannot read Lakebase config from database, using regular DB: {e}")

    return False


async def get_smart_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Database router that automatically selects between regular DB and Lakebase.

    This function acts as a router, checking the configuration and routing
    the database session to either Lakebase (when enabled and configured)
    or the regular database (PostgreSQL/SQLite).

    Yields:
        AsyncSession from either regular database or Lakebase
    """
    # Check if Lakebase should be used
    if await is_lakebase_enabled():
        logger.debug("🔄 DATABASE ROUTER: Connecting to LAKEBASE")

        # Get configuration to extract instance name
        config = await get_lakebase_config_from_db()
        instance_name = None

        if config:
            instance_name = config.get("instance_name")

        if not instance_name:
            instance_name = os.environ.get("LAKEBASE_INSTANCE_NAME", "kasal-lakebase")

        # Try to get user token and email from environment or context
        user_token = os.environ.get("DATABRICKS_USER_TOKEN")
        user_email = os.environ.get("DATABRICKS_USER_EMAIL")

        try:
            # Simply delegate to lakebase session provider
            # It will handle its own session lifecycle
            logger.debug(f"  • Instance: {instance_name}")
            logger.debug(f"  • Endpoint: {config.get('endpoint') if config else 'N/A'}")
            async for session in get_lakebase_session(instance_name, user_token, user_email):
                yield session
            return
        except Exception as e:
            logger.error(f"⚠️ Failed to get Lakebase session, falling back to regular DB: {e}")
            # Fall through to regular database

    # Use regular database session with proper lifecycle management
    logger.debug("🔄 DATABASE ROUTER: Using PostgreSQL/SQLite")
    # Use async context manager for proper session lifecycle
    async with async_session_factory() as session:
        try:
            # logger.debug(f"[DB ROUTER] Created session {id(session)}")
            yield session
            # Commit the transaction if no exception occurred
            await session.commit()
        except Exception as e:
            # Rollback on any exception
            logger.error(f"[DB ROUTER] Rolling back session {id(session)} due to exception: {e}")
            await session.rollback()
            raise
        finally:
            # Ensure session is closed even if not explicitly committed/rolled back
            await session.close()