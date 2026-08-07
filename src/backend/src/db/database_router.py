"""
Database router that automatically selects between regular database (PostgreSQL/SQLite) and Lakebase.

This module provides a routing mechanism to dynamically choose the appropriate database
backend based on configuration stored in the database itself.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exceptions import LakebaseUnavailableError
from src.core.logger import LoggerManager
from src.db.lakebase_session import (
    _is_disposed_connection_error,
    get_lakebase_session,
)
from src.db.lakebase_state import is_fallback_allowed, record_successful_connection
from src.db.session import _request_session, async_session_factory

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
            config.get("enabled", False)
            and config.get("endpoint")
            and (
                config.get("migration_completed", False)
                or config.get("database_type") == "lakebase"
                or config.get("instance_status") == "READY"
            )
        )

        if is_enabled:
            logger.info(
                f"🔵 Lakebase ENABLED via database config (endpoint: {config.get('endpoint')})"
            )
        else:
            logger.debug(
                f"🔴 Lakebase DISABLED - Config incomplete: "
                f"enabled={config.get('enabled')}, "
                f"has_endpoint={bool(config.get('endpoint'))}, "
                f"migration_completed={config.get('migration_completed')}"
            )

        return is_enabled

    except Exception as e:
        # If we can't read the database config (e.g., tables don't exist yet),
        # default to regular database
        logger.debug(f"🔴 Lakebase DISABLED - Cannot read config from database: {e}")

    return False


async def activate_lakebase_in_subprocess() -> bool:
    """Activate Lakebase on async_session_factory inside a spawned subprocess.

    On macOS ``multiprocessing.spawn`` creates a fresh interpreter where the
    global ``async_session_factory`` has NOT been hot-swapped.  This helper
    mirrors the activation logic from ``main.py`` so that **all** existing
    callers of ``async_session_factory()`` (FlowRunnerService, tools, etc.)
    automatically produce Lakebase sessions.

    Returns True if Lakebase was activated, False otherwise.
    """
    try:
        if not await is_lakebase_enabled():
            return False

        config = await get_lakebase_config_from_db()
        instance_name = (config or {}).get("instance_name") or os.environ.get(
            "LAKEBASE_INSTANCE_NAME", "kasal-lakebase"
        )

        from src.db.lakebase_session import LakebaseSessionFactory

        lb_factory = LakebaseSessionFactory(instance_name)
        await lb_factory.create_engine()
        async_session_factory.activate_lakebase(lb_factory._session_factory)

        from src.db.lakebase_state import mark_lakebase_activated

        mark_lakebase_activated()

        logger.info(
            f"[SUBPROCESS] Activated Lakebase session factory (instance: {instance_name})"
        )
        return True
    except Exception as e:
        logger.warning(f"[SUBPROCESS] Lakebase activation skipped: {e}")
        return False


async def deactivate_lakebase_in_process() -> None:
    """Point this process's GLOBAL session factory back at the local database.

    Deleting the config row is already enough for ROUTED sessions: both
    ``get_smart_db_session`` (``SessionDep``) and ``routed_scoped_session``
    re-read :func:`is_lakebase_enabled` on every call, so new requests go local
    the moment the row is gone.

    It is NOT enough for the raw ``async_session_factory``. That global is
    hot-swapped to Lakebase by ``main.py``'s lifespan (and by
    :func:`activate_lakebase_in_subprocess`) and nothing ever swapped it back, so
    every holder of the raw factory — ``utils/databricks_auth`` and, through it,
    ``routed_scoped_session``'s reentrant ``_RESOLVING_AUTH`` branch — kept
    producing Lakebase sessions until the process restarted. That split is what
    made "disable" look like it had not taken effect, and it is what this closes.

    Deliberately disposes ONLY the Lakebase factory. ``dispose_engines()`` also
    disposes the local SQLite/PG engines, and those are the backend being
    switched TO.

    Safe to call when Lakebase was never active: ``deactivate_lakebase`` is a
    no-op that skips its swap callbacks unless the state actually changed, and
    ``dispose_lakebase_factory`` returns immediately when no factory was built.
    """
    from src.db.lakebase_session import dispose_lakebase_factory
    from src.db.lakebase_state import mark_lakebase_deactivated

    # Reverts the global factory AND fires the registered on-swap callbacks, which
    # is how caches keyed to the old backend (e.g. ExecutionService's in-memory
    # cache) get cleared. Doing this by hand would miss them.
    async_session_factory.deactivate_lakebase()

    await dispose_lakebase_factory()

    # Only correct because this is a deliberate disable — see the docstring on
    # mark_lakebase_deactivated before reusing it anywhere else.
    mark_lakebase_deactivated()

    logger.info("Lakebase deactivated — session factory reverted to the local database")


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

        # Lakebase authenticates as the APP'S SERVICE PRINCIPAL, from environment
        # variables — see LakebaseConnectionService.get_workspace_client, which
        # requires DATABRICKS_CLIENT_ID/SECRET/HOST for the `postgres` scope and
        # deliberately STRIPS DATABRICKS_TOKEN/API_KEY first because a PAT
        # conflicts with SPN ("more than one authorization method"). get_username()
        # likewise prefers the SPN client_id. So on a deployed app the PAT chain
        # contributes nothing here.
        #
        # It also cannot be used here. get_auth_context() reads the `apikey` table,
        # and once that read is routed the router calls auth to reach Lakebase
        # while auth calls the router to read the key:
        #
        #     get_auth_context → get_smart_db_session → get_auth_context → …
        #
        # which the deployed app logged 1,287 times as "maximum recursion depth
        # exceeded", killing every crew and flow subprocess (Chat survived on a
        # warm per-process PAT cache — that asymmetry is what identified it).
        #
        # Only the LOCAL-DEV fallback needs an identity, and only when no SPN is
        # configured; that path has no Lakebase config row to fetch, so there is no
        # cycle to close.
        if not os.environ.get("DATABRICKS_CLIENT_ID"):
            try:
                from src.utils.databricks_auth import _RESOLVING_AUTH, get_auth_context

                # Mark the auth resolution as IN PROGRESS before calling it. The
                # flag is what makes auth's own DB read use the raw factory instead
                # of routing; auth sets it itself, but only on ITS outermost entry —
                # and here the ROUTER is the outermost caller, so without this the
                # first entry routes and re-enters this very function. Measured: 2
                # simultaneous get_smart_db_session frames before this line existed.
                # One level is survivable, but it is the same loop that produced
                # 1,287 "maximum recursion depth exceeded", so close it here rather
                # than rely on it staying shallow.
                _auth_token = _RESOLVING_AUTH.set(True)
                try:
                    auth = await get_auth_context()
                finally:
                    _RESOLVING_AUTH.reset(_auth_token)
                if auth:
                    user_token = auth.token
                    user_email = auth.user_identity
                    logger.debug(
                        f"Using unified {auth.auth_method} auth for Lakebase session"
                    )
            except Exception as e:
                logger.warning(f"Failed to get unified auth for Lakebase: {e}")
        else:
            logger.debug(
                "Lakebase session using the app service principal (SPN env vars); "
                "skipping the PAT chain"
            )

        use_lakebase = True

    if use_lakebase:
        session_yielded = False
        last_error: Optional[Exception] = None
        max_retries = 3
        backoff_delays = [0.5, 1.0, 2.0]

        for attempt in range(max_retries):
            try:
                logger.debug(
                    f"  • Instance: {instance_name} (attempt {attempt + 1}/{max_retries})"
                )
                logger.debug(
                    f"  • Endpoint: {config.get('endpoint') if config else 'N/A'}"
                )
                async with get_lakebase_session(
                    instance_name, user_token, user_email
                ) as session:
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
            # A Lakebase migrate/enable disposes the shared SQLite/PG engine
            # mid-flight (dispose_engines(), to switch backends). That closes the
            # connection underneath any CONCURRENT request still holding this
            # session, so this commit then raises "no active connection". Nothing
            # remains to commit or roll back on a disposed connection, so treat
            # that specific teardown race as non-fatal instead of surfacing a raw
            # 500 to the (usually polling) client.
            if _is_disposed_connection_error(e):
                logger.warning(
                    f"[DB ROUTER] Session {id(session)} connection disposed "
                    f"(backend switch in progress); ignoring: {e}"
                )
            else:
                logger.error(
                    f"[DB ROUTER] Rolling back session {id(session)} due to exception: {e}"
                )
                await session.rollback()
                raise
        finally:
            try:
                _request_session.reset(token)
            except ValueError:
                pass
            await session.close()
