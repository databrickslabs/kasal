"""
Lakebase session factory for managing database connections when using Databricks Lakebase.
"""
import os
import uuid
import logging
from typing import AsyncGenerator, Optional
from contextlib import asynccontextmanager
from urllib.parse import quote

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from databricks.sdk import WorkspaceClient

from src.core.logger import LoggerManager
from src.utils.databricks_auth import get_current_databricks_user, get_workspace_client

logger_manager = LoggerManager.get_instance()
logger = logging.getLogger(__name__)


class LakebaseSessionFactory:
    """Factory for creating Lakebase database sessions with token-based authentication."""

    def __init__(self, instance_name: str = "kasal-lakebase", user_token: Optional[str] = None, user_email: Optional[str] = None):
        """
        Initialize Lakebase session factory.

        Args:
            instance_name: Name of the Lakebase instance
            user_token: Optional user token for authentication
            user_email: Optional user email for Lakebase authentication
        """
        self.instance_name = instance_name
        self.user_token = user_token
        self.user_email = user_email
        self._workspace_client = None
        self._engine = None
        self._session_factory = None

    async def _get_workspace_client(self) -> WorkspaceClient:
        """
        Get or create Databricks workspace client with proper authentication hierarchy.

        Authentication priority:
        1. OBO (On-Behalf-Of) with user token
        2. PAT from API keys service
        3. PAT from environment variables (DATABRICKS_TOKEN)

        Uses the centralized get_workspace_client() function which properly handles
        authentication without mixing OAuth and PAT methods.

        Returns:
            WorkspaceClient configured with appropriate authentication
        """
        if self._workspace_client:
            return self._workspace_client

        try:
            # Use centralized workspace client creation from databricks_auth
            # This handles the authentication hierarchy correctly and avoids
            # mixing OAuth (SPN) with token authentication
            self._workspace_client = await get_workspace_client(user_token=self.user_token)

            if not self._workspace_client:
                raise ValueError("Failed to create workspace client - no authentication available")

            logger.info("Successfully created workspace client using centralized auth")
            return self._workspace_client

        except Exception as e:
            logger.error(f"Failed to create workspace client: {e}")
            raise


    async def get_connection_string(self) -> str:
        """
        Generate a connection string with a fresh token for Lakebase.

        Returns:
            PostgreSQL connection string for Lakebase
        """
        try:
            # Get instance details
            w = await self._get_workspace_client()
            instance = w.database.get_database_instance(name=self.instance_name)

            # Check if instance is ready - handle both string and enum states
            state_str = str(instance.state).upper()
            if "AVAILABLE" in state_str or "READY" in state_str:
                # Instance is ready
                pass
            else:
                raise ValueError(f"Lakebase instance {self.instance_name} is not ready (state: {instance.state})")

            # Generate temporary token
            cred = w.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[self.instance_name]
            )

            # Build connection string
            # For asyncpg, don't use sslmode in URL - use connect_args instead
            # Determine username:
            # - If user email provided (e.g., from request context), use it
            # - Otherwise: Get the actual authenticated user's identity
            if self.user_email:
                username = quote(self.user_email)
                logger.info(f"Using provided email for Lakebase: {self.user_email}")
            else:
                # Get the actual authenticated user's identity using centralized method
                # Pass the user token if we have it, otherwise it will use PAT
                current_user_identity, error = await get_current_databricks_user(self.user_token)
                if error or not current_user_identity:
                    logger.error(f"Failed to get current user identity: {error}")
                    raise Exception(f"Cannot determine Databricks user identity: {error}")
                else:
                    username = quote(current_user_identity)
                    logger.info(f"Using authenticated user identity for Lakebase: {current_user_identity}")

            connection_url = (
                f"postgresql+asyncpg://{username}:{cred.token}@"
                f"{instance.read_write_dns}:5432/databricks_postgres"
            )

            return connection_url

        except Exception as e:
            logger.error(f"Error getting Lakebase connection string: {e}")
            raise

    async def create_engine(self):
        """Create or refresh the SQLAlchemy engine with a new token."""
        try:
            # Dispose of old engine if exists
            if self._engine:
                await self._engine.dispose()

            # Get fresh connection string
            connection_url = await self.get_connection_string()

            # Create new engine with SSL configuration for asyncpg
            self._engine = create_async_engine(
                connection_url,
                echo=False,
                future=True,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                # Token expires, so we need to recreate connections periodically
                pool_recycle=1800,  # Recycle connections every 30 minutes
                connect_args={
                    "ssl": "require",  # Enable SSL for asyncpg
                    "server_settings": {
                        "jit": "off",  # Disable JIT for compatibility
                        "search_path": "kasal"  # Set schema search path to kasal only
                    }
                }
            )

            # Create session factory
            self._session_factory = async_sessionmaker(
                self._engine,
                expire_on_commit=False,
                autoflush=False,
                class_=AsyncSession
            )

            logger.info(f"✅ Created Lakebase engine for instance: {self.instance_name}")

        except Exception as e:
            logger.error(f"Error creating Lakebase engine: {e}")
            raise

    @asynccontextmanager
    async def get_session(self):
        """
        Get a database session with Lakebase as an async context manager.
        Transaction management should be handled by the caller.

        Yields:
            AsyncSession connected to Lakebase
        """
        try:
            # Create engine if not exists or refresh if needed
            if not self._engine or not self._session_factory:
                await self.create_engine()

            # Simply provide the session, let caller handle transactions
            async with self._session_factory() as session:
                yield session

        except Exception as e:
            logger.error(f"Error creating Lakebase session: {e}")
            # Try to recreate engine on connection errors
            if "token" in str(e).lower() or "authentication" in str(e).lower():
                logger.info("Token may have expired, recreating engine...")
                await self.create_engine()
                # Retry once with new engine
                async with self._session_factory() as session:
                    yield session
            else:
                raise

    async def dispose(self):
        """Dispose of the engine and clean up resources."""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
            logger.info(f"Disposed Lakebase engine for instance {self.instance_name}")


# Global instance for reuse
_lakebase_factory: Optional[LakebaseSessionFactory] = None


@asynccontextmanager
async def get_lakebase_session(
    instance_name: str = None,
    user_token: Optional[str] = None,
    user_email: Optional[str] = None
) -> AsyncGenerator[AsyncSession, None]:
    """
    Get a Lakebase database session with proper transaction management.

    This function handles the complete session lifecycle including
    commit, rollback, and close operations.

    Args:
        instance_name: Optional instance name override
        user_token: Optional user token for authentication
        user_email: Optional user email for Lakebase authentication

    Yields:
        AsyncSession connected to Lakebase
    """
    global _lakebase_factory

    # Get instance name from config if not provided
    if not instance_name:
        # Try to get from environment or config
        instance_name = os.getenv("LAKEBASE_INSTANCE_NAME", "kasal-lakebase")

    # Create or update factory
    if not _lakebase_factory or _lakebase_factory.instance_name != instance_name:
        _lakebase_factory = LakebaseSessionFactory(instance_name, user_token, user_email)

    # Update token and email if provided
    if user_token and _lakebase_factory.user_token != user_token:
        _lakebase_factory.user_token = user_token
        # Force engine recreation with new token
        await _lakebase_factory.create_engine()

    if user_email and _lakebase_factory.user_email != user_email:
        _lakebase_factory.user_email = user_email
        # Force engine recreation with new email
        await _lakebase_factory.create_engine()

    # Get session and manage its lifecycle using async context manager
    async with _lakebase_factory.get_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()