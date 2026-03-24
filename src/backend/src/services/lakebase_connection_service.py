"""
Lakebase Connection Service for managing database connections and authentication.

This service handles all connection-related operations for Databricks Lakebase instances:
- Credential generation using SPN (Service Principal) client_id as PG username
- Deterministic connection (no trial-and-error)
- Engine creation for both async and sync operations with do_connect token injection
- Workspace client management
"""
import os
import uuid
import logging
import time
from typing import Optional, Tuple, Dict, Any

from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, text, event
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy.pool import NullPool
from sqlalchemy.engine import Engine

from src.core.base_service import BaseService
from src.utils.databricks_auth import get_workspace_client

logger = logging.getLogger(__name__)


class LakebaseConnectionService(BaseService):
    """Service for managing Lakebase database connections and authentication.

    Uses the SPN (Service Principal) client_id as the PostgreSQL username,
    per Databricks Apps Cookbook. No trial-and-error connection guessing.
    """

    def __init__(self, user_token: Optional[str] = None, user_email: Optional[str] = None):
        """
        Initialize Lakebase connection service.

        Args:
            user_token: Optional user token for local-dev fallback auth.
            user_email: Optional user email (local dev fallback for PG username).
        """
        # Don't call super().__init__ since we don't have a session
        self.user_token = user_token
        self.user_email = user_email
        self._workspace_client = None

    async def get_workspace_client(self) -> WorkspaceClient:
        """
        Get or create Databricks workspace client for Lakebase.

        When deployed as a Databricks App, Lakebase requires an OAuth token
        with the 'postgres' scope. Only SPN OAuth (client credentials)
        provides this scope, so SPN is used when available.

        For local development (no SPN env vars), falls back to the generic
        auth chain (PAT / OBO) which works when connecting from a developer
        machine that already has a Lakebase role.

        Returns:
            WorkspaceClient configured with appropriate credentials

        Raises:
            ValueError: If no authentication method is available
        """
        if not self._workspace_client:
            client_id = os.getenv("DATABRICKS_CLIENT_ID")
            client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
            host = os.getenv("DATABRICKS_HOST")

            # SPN OAuth — required when deployed as a Databricks App.
            # The platform also injects DATABRICKS_TOKEN (PAT) which
            # conflicts with SPN in the SDK ("more than one authorization
            # method").  Strip PAT vars before the SDK call and restore
            # after — same pattern as mlflow_setup.py.
            if client_id and client_secret and host:
                logger.info("[LAKEBASE AUTH] Using SPN OAuth (client credentials) — required for postgres scope")
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
                logger.info("[LAKEBASE AUTH] SPN WorkspaceClient created successfully")
                return self._workspace_client

            # Local dev fallback — PAT/OBO via generic auth chain
            logger.info("[LAKEBASE AUTH] No SPN credentials, falling back to PAT/OBO (local dev)")
            self._workspace_client = await get_workspace_client(self.user_token)
            if not self._workspace_client:
                raise ValueError(
                    "Failed to create WorkspaceClient for Lakebase. "
                    "For deployed apps: set DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET. "
                    "For local dev: configure a PAT via API Keys or Databricks CLI profile."
                )
            logger.info("[LAKEBASE AUTH] Local dev WorkspaceClient created via PAT/OBO")
        return self._workspace_client

    def get_spn_username(self) -> Optional[str]:
        """
        Get the Service Principal client_id from environment for use as PG username.

        Per Databricks docs, when a Databricks App connects to Lakebase,
        the PG username IS the service principal's client_id (UUID format).

        Returns:
            The DATABRICKS_CLIENT_ID value or None if not set
        """
        client_id = os.getenv("DATABRICKS_CLIENT_ID")
        if client_id:
            logger.info(f"Using SPN client_id as PG username: {client_id}")
        return client_id

    async def get_username(self) -> str:
        """
        Determine the PostgreSQL username for Lakebase connections.

        Priority:
        1. SPN client_id (deployed Databricks App) - DATABRICKS_CLIENT_ID env var
        2. Current authenticated user email (local development)

        Returns:
            Username string for PG connections

        Raises:
            ValueError: If no username can be determined
        """
        # 1. Try SPN client_id (production / deployed apps)
        spn_username = self.get_spn_username()
        if spn_username:
            return spn_username

        # 2. Try user email if provided (e.g. from request context)
        if self.user_email:
            logger.info(f"Using provided user email as PG username: {self.user_email}")
            return self.user_email

        # 3. Try to get current user from workspace client (local dev)
        try:
            w = await self.get_workspace_client()
            current_user = w.current_user.me()
            if current_user and hasattr(current_user, 'user_name') and current_user.user_name:
                logger.info(f"Using workspace user as PG username: {current_user.user_name}")
                return current_user.user_name
        except Exception as e:
            logger.warning(f"Could not get current user from workspace client: {e}")

        raise ValueError(
            "Cannot determine PostgreSQL username for Lakebase. "
            "Set DATABRICKS_CLIENT_ID environment variable or ensure Databricks authentication is configured."
        )

    async def generate_credentials(self, instance_name: str) -> Any:
        """
        Generate database credentials for a Lakebase instance or autoscaling project.

        Tries the provisioned DatabaseAPI first, then falls back to the
        autoscaling PostgresAPI.

        Args:
            instance_name: Name of the Lakebase instance or project

        Returns:
            Database credential object with token attribute

        Raises:
            Exception: If credential generation fails
        """
        w = await self.get_workspace_client()
        logger.info(f"Generating database credentials for instance: {instance_name}")

        # Try provisioned instance first
        try:
            cred = w.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[instance_name]
            )
            logger.info(f"Generated provisioned credential, token length: {len(cred.token)}")
            return cred
        except Exception as e:
            if "not found" not in str(e).lower() and "not_found" not in str(e).lower():
                raise
            logger.info(f"Instance {instance_name} not found in DatabaseAPI, trying PostgresAPI")

        # Fall back to autoscaling project
        # Find the primary endpoint on the production branch
        try:
            endpoints = list(w.postgres.list_endpoints(
                parent=f"projects/{instance_name}/branches/production"
            ))
            endpoint_name = None
            for ep in endpoints:
                endpoint_name = ep.name
                break

            if not endpoint_name:
                raise ValueError(f"No endpoints found for project {instance_name}")

            cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
            logger.info(f"Generated autoscaling credential, token length: {len(cred.token)}")
            return cred
        except Exception as e:
            logger.error(f"Failed to generate credentials for {instance_name}: {e}")
            raise

    async def test_connection(
        self,
        endpoint: str,
        instance_name: str
    ) -> Dict[str, Any]:
        """
        Test connection to Lakebase with deterministic SPN auth (single attempt).

        Args:
            endpoint: Lakebase endpoint (DNS name)
            instance_name: Lakebase instance name for credential generation

        Returns:
            Dict with success status, connected_user, and version info
        """
        username = await self.get_username()
        cred = await self.generate_credentials(instance_name)

        logger.info(f"Testing Lakebase connection as '{username}' to {endpoint}")

        connection_url = (
            f"postgresql+asyncpg://{username}:{cred.token}@"
            f"{endpoint}:5432/databricks_postgres"
        )

        test_engine = create_async_engine(
            connection_url,
            echo=False,
            connect_args={
                "ssl": "require",
                "server_settings": {"jit": "off"}
            }
        )

        try:
            async with test_engine.connect() as conn:
                result = await conn.execute(text("SELECT current_user, version()"))
                current_user, version = result.fetchone()
                logger.info(f"Connected to Lakebase as: {current_user}, version: {version}")
                return {
                    "success": True,
                    "connected_user": current_user,
                    "version": version,
                    "username_used": username
                }
        except Exception as e:
            logger.error(f"Lakebase connection failed as '{username}': {e}")
            return {
                "success": False,
                "error": str(e),
                "username_used": username
            }
        finally:
            await test_engine.dispose()

    def create_engine_with_token_refresh(
        self,
        endpoint: str,
        username: str,
        token_holder: Dict[str, Any],
        driver: str = "asyncpg"
    ) -> Any:
        """
        Create a SQLAlchemy engine with do_connect event listener for token injection.

        Per Databricks Apps Cookbook, uses do_connect to inject fresh tokens
        from a mutable token_holder dict that a background refresh task updates.

        Args:
            endpoint: Lakebase endpoint (DNS name)
            username: PG username (SPN client_id)
            token_holder: Mutable dict with {"token": str, "refreshed_at": float}
            driver: SQLAlchemy driver ("asyncpg" or "pg8000")

        Returns:
            Engine (async or sync depending on driver)
        """
        # Build URL with placeholder password — do_connect will inject the real token
        if driver == "asyncpg":
            connection_url = (
                f"postgresql+asyncpg://{username}:placeholder@"
                f"{endpoint}:5432/databricks_postgres"
            )
            engine = create_async_engine(
                connection_url,
                echo=False,
                pool_pre_ping=False,  # Required: conflicts with do_connect token injection
                pool_recycle=3600,    # Aligned with 1-hour token expiry
                pool_size=5,
                max_overflow=10,
                connect_args={
                    "ssl": "require",
                    "server_settings": {
                        "jit": "off",
                        "search_path": "kasal"
                    }
                }
            )

            # Attach do_connect listener on the sync_engine (required by SQLAlchemy)
            @event.listens_for(engine.sync_engine, "do_connect")
            def inject_token(dialect, conn_rec, cargs, cparams):
                cparams["password"] = token_holder["token"]

            logger.info(f"Created async engine with do_connect token injection for {endpoint}")
            return engine

        else:
            # Sync engine (pg8000) for streaming operations
            connection_url = (
                f"postgresql+pg8000://{username}:placeholder@"
                f"{endpoint}:5432/databricks_postgres"
            )
            engine = create_engine(
                connection_url,
                echo=False,
                pool_pre_ping=False,
                poolclass=NullPool,
                connect_args={"ssl_context": True}
            )

            @event.listens_for(engine, "do_connect")
            def inject_token_sync(dialect, conn_rec, cargs, cparams):
                cparams["password"] = token_holder["token"]

            logger.info(f"Created sync engine with do_connect token injection for {endpoint}")
            return engine

    async def create_lakebase_engine_async(
        self,
        endpoint: str,
        username: str,
        token: str
    ) -> AsyncEngine:
        """
        Create async SQLAlchemy engine for Lakebase with SSL configuration.

        Args:
            endpoint: Lakebase endpoint (DNS name)
            username: PostgreSQL username (SPN client_id)
            token: Database authentication token

        Returns:
            Configured AsyncEngine with asyncpg driver and SSL
        """
        connection_url = (
            f"postgresql+asyncpg://{username}:{token}@"
            f"{endpoint}:5432/databricks_postgres"
        )

        engine = create_async_engine(
            connection_url,
            echo=False,
            pool_pre_ping=False,
            pool_recycle=3600,
            connect_args={
                "ssl": "require",
                "server_settings": {
                    "jit": "off"
                }
            }
        )

        logger.info(f"Created async Lakebase engine for {endpoint}")
        return engine

    def create_lakebase_engine_sync(
        self,
        endpoint: str,
        username: str,
        token: str,
        statement_timeout_ms: int = 0
    ) -> Engine:
        """
        Create sync SQLAlchemy engine for Lakebase with SSL configuration.

        Uses pg8000 driver for sync operations (streaming contexts).

        Args:
            endpoint: Lakebase endpoint (DNS name)
            username: PostgreSQL username (SPN client_id)
            token: Database authentication token
            statement_timeout_ms: PostgreSQL statement_timeout in milliseconds.
                If > 0, a ``connect`` event listener will run
                ``SET statement_timeout = '<ms>'`` on every new DBAPI connection
                so the server kills long-running queries automatically.

        Returns:
            Configured Engine with pg8000 driver and SSL
        """
        connection_url = (
            f"postgresql+pg8000://{username}:{token}@"
            f"{endpoint}:5432/databricks_postgres"
        )

        engine = create_engine(
            connection_url,
            echo=False,
            poolclass=NullPool,
            connect_args={
                "ssl_context": True
            }
        )

        if statement_timeout_ms > 0:
            @event.listens_for(engine, "connect")
            def _set_statement_timeout(dbapi_conn, connection_record):
                cursor = dbapi_conn.cursor()
                cursor.execute(f"SET statement_timeout = '{statement_timeout_ms}'")
                cursor.close()

            logger.info(
                f"Created sync Lakebase engine for {endpoint} "
                f"with statement_timeout={statement_timeout_ms}ms"
            )
        else:
            logger.info(f"Created sync Lakebase engine for {endpoint}")

        return engine

    async def get_connected_engine_async(
        self,
        instance_name: str,
        endpoint: str
    ) -> Tuple[str, AsyncEngine]:
        """
        Generate credentials and create a connected async engine using SPN username.

        Args:
            instance_name: Name of the Lakebase instance
            endpoint: Lakebase endpoint (DNS name)

        Returns:
            Tuple of (username, async_engine)

        Raises:
            Exception: If connection fails
        """
        username = await self.get_username()
        cred = await self.generate_credentials(instance_name)
        engine = await self.create_lakebase_engine_async(endpoint, username, cred.token)

        # Verify connection
        try:
            async with engine.connect() as conn:
                result = await conn.execute(text("SELECT current_user"))
                connected_user = result.scalar()
                logger.info(f"Connected to Lakebase as: {connected_user}")
        except Exception as e:
            await engine.dispose()
            raise Exception(f"Failed to connect to Lakebase as '{username}': {e}")

        return username, engine

    def get_connected_engine_sync(
        self,
        endpoint: str,
        username: str,
        token: str
    ) -> Tuple[Engine, str]:
        """
        Create and verify a sync engine connection using SPN username.

        Args:
            endpoint: Lakebase endpoint (DNS name)
            username: PostgreSQL username (SPN client_id)
            token: Database authentication token

        Returns:
            Tuple of (sync_engine, connected_user)

        Raises:
            Exception: If connection fails
        """
        engine = self.create_lakebase_engine_sync(endpoint, username, token)

        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT current_user"))
                connected_user = result.scalar()
                logger.info(f"[SYNC] Connected to Lakebase as: {connected_user}")
        except Exception as e:
            engine.dispose()
            raise Exception(f"[SYNC] Failed to connect to Lakebase as '{username}': {e}")

        return engine, connected_user
