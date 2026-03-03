"""
Lakebase session factory for managing database connections when using Databricks Lakebase.

Uses the do_connect event pattern per Databricks Apps Cookbook for token injection,
with a background task that refreshes tokens every 50 minutes (tokens expire at 60 min).
The PG username is the SPN client_id (DATABRICKS_CLIENT_ID env var).

Authentication for Lakebase memory operations:
  - Deployed (Databricks Apps): SPN (client_id + client_secret)
  - Local dev: PAT (Personal Access Token from API Keys or environment)
  - OBO is NEVER used for Lakebase connections
"""
import os
import uuid
import time
import asyncio
import logging
import threading
from typing import AsyncGenerator, Optional
from contextlib import asynccontextmanager

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from databricks.sdk import WorkspaceClient

from src.core.logger import LoggerManager

logger_manager = LoggerManager.get_instance()
logger = logging.getLogger(__name__)

# Token refresh interval: 50 minutes (tokens expire at 60 min, 10 min safety margin)
TOKEN_REFRESH_INTERVAL_SECONDS = 50 * 60


class LakebaseSessionFactory:
    """Factory for creating Lakebase database sessions with do_connect token injection."""

    def __init__(self, instance_name: str = "kasal-lakebase", user_token: Optional[str] = None, user_email: Optional[str] = None, group_id: Optional[str] = None):
        """
        Initialize Lakebase session factory.

        Args:
            instance_name: Name of the Lakebase instance
            user_token: Unused (kept for API compat). Auth uses SPN or PAT only.
            user_email: Optional user email for Lakebase authentication
            group_id: Optional group_id for PAT lookup in background threads
                     where UserContext is not available.
        """
        self.instance_name = instance_name
        self.user_token = user_token
        self.user_email = user_email
        self.group_id = group_id
        self._workspace_client = None
        self._engine = None
        self._session_factory = None
        self._token_holder: dict = {"token": "", "refreshed_at": 0.0}
        self._refresh_task: Optional[asyncio.Task] = None
        self._engine_loop_id: Optional[int] = None  # Track which event loop owns the engine

    async def _get_workspace_client(self) -> WorkspaceClient:
        """
        Get or create Databricks workspace client for Lakebase.

        Authentication priority (OBO is NEVER used):
        1. SPN OAuth (client credentials) — for deployed Databricks Apps
        2. PAT — from API Keys Service or Databricks CLI profile

        Returns:
            WorkspaceClient configured with appropriate authentication
        """
        if self._workspace_client:
            return self._workspace_client

        try:
            # Priority 1: SPN OAuth — required when deployed as a Databricks App
            client_id = os.getenv("DATABRICKS_CLIENT_ID")
            client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
            host = os.getenv("DATABRICKS_HOST")

            if client_id and client_secret and host:
                logger.info("[LAKEBASE SESSION] Using SPN OAuth (client credentials)")
                # Strip PAT vars to prevent SDK dual-auth conflict (oauth + pat)
                # Same pattern as mlflow_setup.py
                _pat_backup = {}
                for _k in ("DATABRICKS_TOKEN", "DATABRICKS_API_KEY"):
                    if _k in os.environ:
                        _pat_backup[_k] = os.environ.pop(_k)
                try:
                    self._workspace_client = WorkspaceClient(
                        host=host,
                        client_id=client_id,
                        client_secret=client_secret,
                    )
                finally:
                    os.environ.update(_pat_backup)
                logger.info("[LAKEBASE SESSION] SPN WorkspaceClient created successfully")
                return self._workspace_client

            # Priority 2: PAT — pass user_token=None to skip OBO entirely
            # Pass group_id explicitly so PAT lookup works in background threads
            # where UserContext is not available.
            logger.info(f"[LAKEBASE SESSION] No SPN credentials, trying PAT (local dev, group_id={self.group_id})")
            from src.utils.databricks_auth import get_workspace_client
            self._workspace_client = await get_workspace_client(user_token=None, group_id=self.group_id)

            if not self._workspace_client:
                raise ValueError(
                    "Failed to create workspace client for Lakebase. "
                    "For deployed apps: set DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET. "
                    "For local dev: configure a PAT via API Keys (Configuration → API Keys) or Databricks CLI profile."
                )

            logger.info("[LAKEBASE SESSION] Local dev WorkspaceClient created via PAT")
            return self._workspace_client

        except Exception as e:
            logger.error(f"Failed to create workspace client: {e}")
            raise

    async def _get_username(self) -> str:
        """
        Determine the PostgreSQL username for Lakebase connections.

        Priority:
        1. SPN client_id (DATABRICKS_CLIENT_ID env var) - production / deployed apps
        2. User email if provided (from request context)
        3. Current user from workspace client (local development)

        Returns:
            Username string for PG connections

        Raises:
            ValueError: If no username can be determined
        """
        # 1. SPN client_id (production / deployed Databricks Apps)
        client_id = os.getenv("DATABRICKS_CLIENT_ID")
        if client_id:
            logger.info(f"Using SPN client_id as PG username: {client_id}")
            return client_id

        # 2. User email if provided
        if self.user_email:
            logger.info(f"Using provided email for Lakebase: {self.user_email}")
            return self.user_email

        # 3. Try to get current user from workspace client (local dev)
        try:
            w = await self._get_workspace_client()
            current_user = w.current_user.me()
            if current_user and hasattr(current_user, 'user_name') and current_user.user_name:
                logger.info(f"Using workspace current user as PG username: {current_user.user_name}")
                return current_user.user_name
        except Exception as e:
            logger.warning(f"Could not get current user from workspace client: {e}")

        raise ValueError(
            "Cannot determine PG username for Lakebase. "
            "Set DATABRICKS_CLIENT_ID environment variable or ensure Databricks authentication is configured."
        )

    async def _refresh_token(self) -> str:
        """
        Generate a fresh database credential token.

        Returns:
            Fresh token string
        """
        w = await self._get_workspace_client()
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[self.instance_name]
        )
        self._token_holder["token"] = cred.token
        self._token_holder["refreshed_at"] = time.time()
        logger.info(f"Refreshed Lakebase token for instance {self.instance_name} (length: {len(cred.token)})")
        return cred.token

    async def _schedule_token_refresh(self):
        """Background task that refreshes the token every 50 minutes."""
        while True:
            try:
                await asyncio.sleep(TOKEN_REFRESH_INTERVAL_SECONDS)
                await self._refresh_token()
                logger.info(f"Background token refresh completed for {self.instance_name}")
            except asyncio.CancelledError:
                logger.info(f"Token refresh task cancelled for {self.instance_name}")
                break
            except Exception as e:
                logger.error(f"Token refresh failed for {self.instance_name}: {e}")
                # Retry after 60 seconds on failure
                await asyncio.sleep(60)

    async def get_connection_string(self) -> str:
        """
        Generate a connection string with fresh token for Lakebase.

        The connection URL uses a placeholder password — the actual token is
        injected via the do_connect event listener.

        Returns:
            PostgreSQL connection string for Lakebase
        """
        try:
            w = await self._get_workspace_client()
            instance = w.database.get_database_instance(name=self.instance_name)

            # Check if instance is ready
            state_str = str(instance.state).upper()
            if "AVAILABLE" not in state_str and "READY" not in state_str:
                raise ValueError(f"Lakebase instance {self.instance_name} is not ready (state: {instance.state})")

            username = await self._get_username()

            # Refresh token into the holder
            await self._refresh_token()

            # Build URL with placeholder password (do_connect injects real token)
            connection_url = (
                f"postgresql+asyncpg://{username}:placeholder@"
                f"{instance.read_write_dns}:5432/databricks_postgres"
            )

            return connection_url

        except Exception as e:
            logger.error(f"Error getting Lakebase connection string: {e}")
            raise

    async def create_engine(self):
        """Create or refresh the SQLAlchemy engine with do_connect token injection."""
        try:
            # Dispose of old engine if exists
            if self._engine:
                try:
                    await self._engine.dispose()
                except Exception:
                    pass  # Engine may belong to a closed event loop

            # Cancel old refresh task if exists
            if self._refresh_task and not self._refresh_task.done():
                self._refresh_task.cancel()

            # Reset cached workspace client to force re-authentication
            self._workspace_client = None

            # Get fresh connection string (also refreshes token into holder)
            connection_url = await self.get_connection_string()

            # Check if NullPool is requested (crew threads set USE_NULLPOOL=true)
            use_nullpool = os.environ.get("USE_NULLPOOL", "").lower() == "true"

            pool_kwargs = {}
            if use_nullpool:
                from sqlalchemy.pool import NullPool
                pool_kwargs["poolclass"] = NullPool
                logger.info("[LAKEBASE SESSION] Using NullPool (crew thread mode)")
            else:
                pool_kwargs["pool_size"] = 5
                pool_kwargs["max_overflow"] = 10
                pool_kwargs["pool_recycle"] = 3600  # Aligned with 1-hour token expiry

            # Create engine with pool_pre_ping=False (required for do_connect pattern)
            self._engine = create_async_engine(
                connection_url,
                echo=False,
                future=True,
                pool_pre_ping=False,   # Required: conflicts with do_connect token injection
                connect_args={
                    "ssl": "require",
                    "server_settings": {
                        "jit": "off",
                        "search_path": "kasal"
                    }
                },
                **pool_kwargs
            )

            # Attach do_connect event listener to inject token from holder
            token_holder = self._token_holder

            @event.listens_for(self._engine.sync_engine, "do_connect")
            def inject_token(dialect, conn_rec, cargs, cparams):
                cparams["password"] = token_holder["token"]

            # Create session factory
            self._session_factory = async_sessionmaker(
                self._engine,
                expire_on_commit=False,
                autoflush=False,
                class_=AsyncSession
            )

            # Track which event loop owns this engine
            try:
                self._engine_loop_id = id(asyncio.get_running_loop())
            except RuntimeError:
                self._engine_loop_id = None

            # Start background token refresh task (only for long-lived pools)
            if not use_nullpool:
                self._refresh_task = asyncio.create_task(self._schedule_token_refresh())
            else:
                self._refresh_task = None

            logger.info(f"Created Lakebase engine for instance: {self.instance_name} (nullpool={use_nullpool})")

        except Exception as e:
            logger.error(f"Error creating Lakebase engine: {e}")
            raise

    def _is_engine_loop_stale(self) -> bool:
        """Check if the engine was created in a different event loop."""
        if not self._engine or self._engine_loop_id is None:
            return True
        try:
            current_loop_id = id(asyncio.get_running_loop())
            return current_loop_id != self._engine_loop_id
        except RuntimeError:
            return True

    @asynccontextmanager
    async def get_session(self):
        """
        Get a database session with Lakebase as an async context manager.
        Transaction management should be handled by the caller.

        Yields:
            AsyncSession connected to Lakebase
        """
        # Recreate engine if not exists, or if event loop changed (crew thread)
        if not self._engine or not self._session_factory or self._is_engine_loop_stale():
            try:
                if self._is_engine_loop_stale():
                    logger.info("[LAKEBASE SESSION] Event loop changed, recreating engine")
                await self.create_engine()
            except Exception as e:
                logger.error(f"Error creating Lakebase session: {e}")
                raise

        try:
            async with self._session_factory() as session:
                yield session
        except GeneratorExit:
            # Client disconnected — nothing to clean up, the session
            # factory context manager will handle connection return.
            pass
        except Exception as e:
            # Check if this is a token/auth error that might be fixable by refreshing
            err_str = str(e).lower()
            if "token" in err_str or "authentication" in err_str or "password" in err_str:
                logger.info("Token may have expired, refreshing and recreating engine...")
                await self.create_engine()
                # Re-raise so the caller can retry with a fresh session
                raise
            else:
                logger.error(f"Error in Lakebase session: {e}")
                raise

    async def dispose(self):
        """Dispose of the engine, cancel refresh task, and clean up resources."""
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None

        if self._engine:
            try:
                await self._engine.dispose()
            except Exception:
                pass  # Engine may belong to a closed event loop
            self._engine = None
            self._session_factory = None
            self._engine_loop_id = None
            logger.info(f"Disposed Lakebase engine for instance {self.instance_name}")


# Global instance for the main event loop (API endpoints)
_lakebase_factory: Optional[LakebaseSessionFactory] = None

# Thread-local factory for crew threads (each thread gets its own factory/engine)
_thread_local = threading.local()


async def dispose_lakebase_factory() -> None:
    """
    Dispose the global Lakebase session factory and reset it.

    This must be called when:
    - Switching FROM Lakebase to regular DB (disable)
    - Switching TO Lakebase (enable) to force fresh connection
    - Any config change that affects Lakebase connection parameters

    Without this, stale engines/tokens persist and new requests
    continue using the old backend.
    """
    global _lakebase_factory
    if _lakebase_factory:
        try:
            await _lakebase_factory.dispose()
            logger.info("Disposed global Lakebase session factory")
        except Exception as e:
            logger.warning(f"Error disposing Lakebase factory: {e}")
        finally:
            _lakebase_factory = None
    else:
        logger.debug("No Lakebase factory to dispose")


def _is_crew_thread() -> bool:
    """Check if we're running in a crew thread (USE_NULLPOOL indicates sync-async bridge)."""
    return os.environ.get("USE_NULLPOOL", "").lower() == "true"


@asynccontextmanager
async def get_lakebase_session(
    instance_name: str = None,
    user_token: Optional[str] = None,
    user_email: Optional[str] = None,
    group_id: Optional[str] = None,
) -> AsyncGenerator[AsyncSession, None]:
    """
    Get a Lakebase database session with proper transaction management.

    Authentication: Uses SPN (deployed) or PAT (local dev). OBO is never used.
    The user_token parameter is accepted for API compatibility but not used for auth.

    For crew threads (USE_NULLPOOL=true), uses a thread-local factory to avoid
    event loop conflicts with the main application's factory.

    Args:
        instance_name: Optional instance name override
        user_token: Accepted for API compat, not used for authentication
        user_email: Optional user email for Lakebase authentication
        group_id: Optional group_id for PAT lookup in background threads
                 where UserContext is not available.

    Yields:
        AsyncSession connected to Lakebase
    """
    global _lakebase_factory

    # Get instance name from config if not provided
    if not instance_name:
        instance_name = os.getenv("LAKEBASE_INSTANCE_NAME", "kasal-lakebase")

    # Crew threads: use thread-local factory to avoid event loop conflicts
    if _is_crew_thread():
        factory = getattr(_thread_local, 'factory', None)
        if not factory or factory.instance_name != instance_name:
            factory = LakebaseSessionFactory(instance_name, user_email=user_email, group_id=group_id)
            _thread_local.factory = factory
            logger.info(f"[LAKEBASE SESSION] Created thread-local factory for crew thread (instance: {instance_name}, group_id={group_id})")

        async with factory.get_session() as session:
            try:
                yield session
                await session.commit()
            except GeneratorExit:
                pass
            except Exception:
                try:
                    await session.rollback()
                except Exception:
                    pass
                raise
            finally:
                try:
                    await session.close()
                except Exception:
                    pass
        return

    # Main event loop: use global factory
    if not _lakebase_factory or _lakebase_factory.instance_name != instance_name:
        _lakebase_factory = LakebaseSessionFactory(instance_name, user_email=user_email, group_id=group_id)

    if user_email and _lakebase_factory.user_email != user_email:
        _lakebase_factory.user_email = user_email
        # Force engine recreation with new email
        await _lakebase_factory.create_engine()

    # Get session and manage its lifecycle using async context manager
    async with _lakebase_factory.get_session() as session:
        try:
            yield session
            await session.commit()
        except GeneratorExit:
            # Client disconnected — skip commit/rollback to avoid
            # asyncpg "another operation is in progress" errors.
            pass
        except Exception:
            try:
                await session.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                await session.close()
            except Exception:
                # Connection may already be closed or in a broken state
                # (e.g. asyncpg InterfaceError during concurrent cleanup).
                # Swallow to prevent masking the original error.
                pass
