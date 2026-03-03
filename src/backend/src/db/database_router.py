"""
Database router that automatically selects between regular database (PostgreSQL/SQLite) and Lakebase.

This module provides a routing mechanism to dynamically choose the appropriate database
backend based on configuration stored in the database itself.
"""
import asyncio
import os
import logging
from typing import AsyncGenerator, Optional, Dict, Any
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.db.session import async_session_factory, _request_session
from src.db.lakebase_session import get_lakebase_session
from src.db.lakebase_state import is_fallback_allowed, record_successful_connection
from src.core.exceptions import LakebaseUnavailableError
from src.core.logger import LoggerManager

logger_manager = LoggerManager.get_instance()
logger = logger_manager.database


async def get_lakebase_config_from_db() -> Optional[Dict[str, Any]]:
    """
    Get Lakebase configuration from the database.

    IMPORTANT: Always reads from the local SQLite fallback database, not the
    main session factory.  The lakebase config is written to SQLite during
    setup; once migration succeeds the main session factory points at the
    Lakebase PostgreSQL instance which doesn't contain the config row.
    Reading from SQLite avoids the chicken-and-egg problem.

    Returns:
        Lakebase configuration dictionary or None if not found
    """
    try:
        import json
        import sqlite3
        from src.db.session import settings

        db_path = settings.SQLITE_DB_PATH or "./app.db"
        if not os.path.isabs(db_path):
            db_path = os.path.abspath(db_path)

        if not os.path.exists(db_path):
            return None

        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT value FROM database_configs WHERE key = ?",
                ("lakebase",),
            )
            row = cursor.fetchone()
            if row and row[0]:
                return json.loads(row[0]) if isinstance(row[0], str) else row[0]
            return None
        finally:
            conn.close()
    except Exception as e:
        logger.debug(f"Could not read Lakebase config from database: {e}")
        return None


async def is_lakebase_enabled() -> bool:
    """
    Check if Lakebase is enabled and configured.
    Database is the single source of truth - no environment variable overrides.
    """
    # For fresh deployments or when database tables don't exist yet,
    # default to regular database to avoid circular dependency
    try:
        # Get configuration from database - this is the ONLY source of truth
        config = await get_lakebase_config_from_db()

        if not config:
            logger.debug("🔴 Lakebase DISABLED - No configuration found in database")
            return False

        is_enabled = (
            config.get("enabled", False) and
            config.get("endpoint") and
            (
                config.get("migration_completed", False) or
                config.get("database_type") == "lakebase" or
                config.get("instance_status") == "READY"
            )
        )

        if is_enabled:
            logger.info(f"🔵 Lakebase ENABLED via database config (endpoint: {config.get('endpoint')})")
        else:
            logger.debug(f"🔴 Lakebase DISABLED - Config incomplete: "
                        f"enabled={config.get('enabled')}, "
                        f"has_endpoint={bool(config.get('endpoint'))}, "
                        f"migration_completed={config.get('migration_completed')}")

        return is_enabled

    except Exception as e:
        # If we can't read the database config (e.g., tables don't exist yet),
        # default to regular database
        logger.debug(f"🔴 Lakebase DISABLED - Cannot read config from database: {e}")

    return False


async def get_smart_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Database router that automatically selects between regular DB and Lakebase.

    This function acts as a router, checking the configuration and routing
    the database session to either Lakebase (when enabled and configured)
    or the regular database (PostgreSQL/SQLite).

    IMPORTANT: This is an async generator used as a FastAPI dependency.
    It must yield EXACTLY ONCE to avoid 'generator didn't stop after athrow()'.
    The Lakebase vs regular DB decision must be made BEFORE the single yield.

    Yields:
        AsyncSession from either regular database or Lakebase
    """
    # Decide which session provider to use BEFORE yielding
    use_lakebase = False
    instance_name = None
    user_token = None
    user_email = None
    config = None

    if await is_lakebase_enabled():
        logger.debug("🔄 DATABASE ROUTER: Connecting to LAKEBASE")

        config = await get_lakebase_config_from_db()
        if config:
            instance_name = config.get("instance_name")
        if not instance_name:
            instance_name = os.environ.get("LAKEBASE_INSTANCE_NAME", "kasal-lakebase")

        # Get user token and email from unified auth
        try:
            from src.utils.databricks_auth import get_auth_context
            auth = await get_auth_context()
            if auth:
                user_token = auth.token
                user_email = auth.user_identity
                logger.debug(f"Using unified {auth.auth_method} auth for Lakebase session")
        except Exception as e:
            logger.warning(f"Failed to get unified auth for Lakebase: {e}")

        use_lakebase = True

    if use_lakebase:
        session_yielded = False
        last_error: Optional[Exception] = None
        max_retries = 3
        backoff_delays = [0.5, 1.0, 2.0]

        for attempt in range(max_retries):
            try:
                logger.debug(f"  • Instance: {instance_name} (attempt {attempt + 1}/{max_retries})")
                logger.debug(f"  • Endpoint: {config.get('endpoint') if config else 'N/A'}")
                async with get_lakebase_session(instance_name, user_token, user_email) as session:
                    record_successful_connection()
                    token = _request_session.set(session)
                    try:
                        session_yielded = True
                        yield session
                    finally:
                        try:
                            _request_session.reset(token)
                        except ValueError:
                            pass
                return
            except GeneratorExit:
                return
            except Exception as e:
                if session_yielded:
                    raise
                last_error = e
                if attempt < max_retries - 1:
                    delay = backoff_delays[attempt]
                    logger.warning(
                        f"Lakebase connection attempt {attempt + 1}/{max_retries} failed: {e}. "
                        f"Retrying in {delay}s..."
                    )
                    await asyncio.sleep(delay)

        # All retries exhausted
        if is_fallback_allowed():
            logger.warning(
                f"Lakebase unavailable during startup after {max_retries} attempts, "
                f"falling back to local DB: {last_error}"
            )
            use_lakebase = False
        else:
            logger.error(
                f"Lakebase unavailable after {max_retries} attempts "
                f"(fallback disabled — data was written to Lakebase): {last_error}"
            )
            raise LakebaseUnavailableError(
                f"Lakebase database unreachable after {max_retries} retries: {last_error}"
            )

    # Use regular database session with proper lifecycle management
    logger.debug("🔄 DATABASE ROUTER: Using PostgreSQL/SQLite")
    async with async_session_factory() as session:
        token = _request_session.set(session)
        try:
            yield session
            await session.commit()
        except Exception as e:
            logger.error(f"[DB ROUTER] Rolling back session {id(session)} due to exception: {e}")
            await session.rollback()
            raise
        finally:
            try:
                _request_session.reset(token)
            except ValueError:
                pass
            await session.close()