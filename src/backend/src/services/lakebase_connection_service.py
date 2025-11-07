"""
Lakebase Connection Service for managing database connections and authentication.

This service handles all connection-related operations for Databricks Lakebase instances:
- Credential generation
- Connection testing with multiple authentication approaches
- Engine creation for both async and sync operations
- Workspace client management
- User identity resolution
"""
import uuid
import logging
from typing import Optional, Tuple, Dict, Any
from urllib.parse import quote

from databricks.sdk import WorkspaceClient
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy.pool import NullPool
from sqlalchemy.engine import Engine

from src.core.base_service import BaseService
from src.utils.databricks_auth import get_workspace_client

logger = logging.getLogger(__name__)


class LakebaseConnectionService(BaseService):
    """Service for managing Lakebase database connections and authentication."""

    def __init__(self, user_token: Optional[str] = None, user_email: Optional[str] = None):
        """
        Initialize Lakebase connection service.

        Args:
            user_token: Optional user token for Databricks authentication (OBO)
            user_email: Optional user email for Lakebase authentication
        """
        # Don't call super().__init__ since we don't have a session
        self.user_token = user_token
        self.user_email = user_email
        self._workspace_client = None

    async def get_workspace_client(self) -> WorkspaceClient:
        """
        Get or create Databricks workspace client for Lakebase.

        Uses Lakebase-specific authentication priority (OBO → PAT → SPN):
        1. OBO (On-Behalf-Of) - User token from request headers (if available)
        2. PAT (Personal Access Token) - From environment/API keys service
        3. SPN (Service Principal) - OAuth with client ID/secret

        This enables user-specific operations when OBO is available, with graceful
        fallback to shared credentials (PAT/SPN) when OBO is not available or fails.

        Returns:
            WorkspaceClient configured with appropriate credentials

        Raises:
            ValueError: If WorkspaceClient creation fails
        """
        if not self._workspace_client:
            # Use unified auth priority: OBO → PAT → SPN
            # This is lazy initialization - only creates client when actually needed
            logger.info("Creating WorkspaceClient for Lakebase (lazy initialization)")
            self._workspace_client = await get_workspace_client(self.user_token)
            if not self._workspace_client:
                error_msg = (
                    "Failed to create WorkspaceClient for Lakebase operations. "
                    "No authentication method available. Please configure Databricks authentication:\n"
                    "  1. OBO: User token from request headers (automatic when authenticated)\n"
                    "  2. PAT: Configure via API Keys Service (Configuration → API Keys)\n"
                    "  3. SPN: Configure via Databricks authentication settings"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            logger.info("Successfully created WorkspaceClient for Lakebase")
        return self._workspace_client

    async def generate_credentials(self, instance_name: str) -> Any:
        """
        Generate database credentials for Lakebase instance.

        When OBO authentication is used, credentials are generated for the specific user.
        When PAT/SPN authentication is used, credentials are generated for the service account.

        Args:
            instance_name: Name of the Lakebase instance

        Returns:
            Database credential object with token and expiration_time

        Raises:
            Exception: If credential generation fails
        """
        w = await self.get_workspace_client()

        logger.info(f"Generating database credentials for instance: {instance_name}")
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[instance_name]
        )

        logger.info(f"✅ Generated database credential, token length: {len(cred.token)}")

        # Note: DatabaseCredential only has 'token' and 'expiration_time' attributes
        # There is NO 'user' attribute - we determine the user by testing connections
        # When OBO is used, the token is for the specific user; otherwise it's for the service account

        return cred

    async def test_connections_async(
        self,
        endpoint: str,
        cred: Any
    ) -> Tuple[Optional[AsyncEngine], Optional[str]]:
        """
        Test async connections to Lakebase with multiple authentication approaches.

        Tries different connection methods in order:
        1. Token-only (no username)
        2. Admin user with token
        3. Postgres user with token
        4. Credential user with token

        Returns the first successful engine and connected user.

        Args:
            endpoint: Lakebase endpoint (DNS name)
            cred: Database credential object with user and token

        Returns:
            Tuple of (AsyncEngine, connected_user) if successful, (None, None) if all failed
        """
        logger.info(f"[MIGRATION DEBUG] ========== ASYNC CONNECTION ATTEMPTS ==========")
        logger.info(f"[MIGRATION DEBUG] Endpoint: {endpoint}")
        logger.info(f"[MIGRATION DEBUG] Token length: {len(cred.token)}")

        # Try different connection approaches - credential has no 'user' attribute
        connection_attempts = [
            ("token-only", f"postgresql+asyncpg://:{cred.token}@{endpoint}:5432/databricks_postgres"),
            ("admin", f"postgresql+asyncpg://admin:{cred.token}@{endpoint}:5432/databricks_postgres"),
            ("postgres", f"postgresql+asyncpg://postgres:{cred.token}@{endpoint}:5432/databricks_postgres")
        ]

        connected_engine = None
        connected_user = None

        for attempt_name, connection_url in connection_attempts:
            logger.info(f"[MIGRATION DEBUG] Attempting async connection as '{attempt_name}'...")
            # Mask the token in the log
            masked_url = connection_url.replace(cred.token, "***TOKEN***")
            logger.info(f"[MIGRATION DEBUG] Connection URL (masked): {masked_url}")

            test_engine = create_async_engine(
                connection_url,
                echo=False,
                connect_args={
                    "ssl": "require",
                    "server_settings": {"jit": "off"}
                }
            )

            try:
                async with test_engine.connect() as test_conn:
                    result = await test_conn.execute(text("SELECT current_user, version()"))
                    current_user, version = result.fetchone()
                    logger.info(f"[MIGRATION DEBUG] ✅✅✅ SUCCESS! Connected as: {current_user}")
                    logger.info(f"[MIGRATION DEBUG] Database version: {version}")
                    logger.info(f"✅ SUCCESS! Connected as: {current_user}")
                    logger.info(f"✅ Database version: {version}")
                    connected_engine = test_engine
                    connected_user = current_user
                    break
            except Exception as conn_error:
                logger.info(f"[MIGRATION DEBUG] ❌ FAILED '{attempt_name}': {str(conn_error)[:200]}")
                logger.info(f"Connection attempt '{attempt_name}' failed: {conn_error}")
                await test_engine.dispose()

        logger.info(f"[MIGRATION DEBUG] ========================================")

        if not connected_engine:
            logger.info("[MIGRATION DEBUG] ❌❌❌ ALL ASYNC CONNECTION ATTEMPTS FAILED!")

        return connected_engine, connected_user

    def test_connections_sync(
        self,
        endpoint: str,
        cred: Any
    ) -> Tuple[Optional[Engine], Optional[str]]:
        """
        Test sync connections to Lakebase with multiple authentication approaches.

        Tries different connection methods in order:
        1. Current authenticated user identity (from workspace client)
        2. User email from OBO (X-Forwarded-Email)
        3. Common Databricks/Lakebase usernames as fallback

        Returns the first successful engine and connected user.
        Uses pg8000 driver for sync operations (streaming contexts).

        Args:
            endpoint: Lakebase endpoint (DNS name)
            cred: Database credential object with token

        Returns:
            Tuple of (Engine, connected_user) if successful, (None, None) if all failed
        """
        logger.info(f"[STREAM] ========== SYNC CONNECTION ATTEMPTS ==========")
        logger.info(f"[STREAM] Endpoint: {endpoint}")
        logger.info(f"[STREAM] Token length: {len(cred.token)}")

        # Build list of usernames to try
        # Note: pg8000 requires a username, we try authenticated identity first
        connection_attempts = []

        # Try to get the current authenticated user identity (Service Principal or User)
        # Use sync approach: create workspace client and get current user
        try:
            from src.utils.databricks_auth import get_workspace_client
            import asyncio
            from concurrent.futures import ThreadPoolExecutor

            # Run async function in a new thread to avoid event loop conflict
            def get_identity_sync():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    # Get workspace client with service-level auth (PAT/SPN, no OBO)
                    w = loop.run_until_complete(get_workspace_client(user_token=None))
                    if w:
                        current_user = w.current_user.me()
                        if current_user:
                            # Try user_name first, then application_id (for service principals)
                            if hasattr(current_user, 'user_name') and current_user.user_name:
                                return current_user.user_name, None
                            elif hasattr(current_user, 'application_id') and current_user.application_id:
                                return current_user.application_id, None
                            elif hasattr(current_user, 'display_name') and current_user.display_name:
                                return current_user.display_name, None
                    return None, "Could not get authenticated user"
                except Exception as e:
                    return None, str(e)
                finally:
                    loop.close()

            with ThreadPoolExecutor() as executor:
                future = executor.submit(get_identity_sync)
                user_identity, error = future.result(timeout=10)

            if user_identity and not error:
                logger.info(f"[STREAM] Will try authenticated identity: {user_identity}")
                connection_attempts.append(
                    ("authenticated_identity", f"postgresql+pg8000://{quote(user_identity)}:{cred.token}@{endpoint}:5432/databricks_postgres")
                )
            else:
                logger.warning(f"[STREAM] Could not get authenticated identity: {error}")
        except Exception as e:
            logger.warning(f"[STREAM] Error getting authenticated identity: {e}")

        # If we have user_email from OBO (X-Forwarded-Email), try it as well
        if self.user_email:
            logger.info(f"[STREAM] Will try user email (OBO): {self.user_email}")
            connection_attempts.append(
                ("user_email_obo", f"postgresql+pg8000://{quote(self.user_email)}:{cred.token}@{endpoint}:5432/databricks_postgres")
            )

        # Add common fallback usernames
        connection_attempts.extend([
            ("databricks_superuser", f"postgresql+pg8000://databricks_superuser:{cred.token}@{endpoint}:5432/databricks_postgres"),
            ("admin", f"postgresql+pg8000://admin:{cred.token}@{endpoint}:5432/databricks_postgres"),
            ("postgres", f"postgresql+pg8000://postgres:{cred.token}@{endpoint}:5432/databricks_postgres")
        ])

        connected_user = None
        test_engine = None

        for attempt_name, connection_url in connection_attempts:
            logger.info(f"[STREAM] Trying connection as '{attempt_name}'...")
            try:
                test_engine = create_engine(
                    connection_url,
                    echo=False,
                    poolclass=NullPool,
                    connect_args={"ssl_context": True}
                )

                with test_engine.connect() as test_conn:
                    result = test_conn.execute(text("SELECT current_user"))
                    connected_user = result.scalar()
                    logger.info(f"[STREAM] ✅ Connected as: {connected_user}")
                    # Keep this engine for the actual migration
                    break

            except Exception as e:
                logger.info(f"[STREAM] ❌ Failed '{attempt_name}': {str(e)[:100]}")
                if test_engine:
                    test_engine.dispose()
                    test_engine = None

        logger.info(f"[STREAM] ========================================")

        if not test_engine or not connected_user:
            logger.info("[STREAM] ❌ ALL SYNC CONNECTION ATTEMPTS FAILED!")

        return test_engine, connected_user

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
            username: PostgreSQL username (URL-encoded)
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
            connect_args={
                "ssl": "require",  # Enable SSL for asyncpg
                "server_settings": {
                    "jit": "off"  # Disable JIT for compatibility
                }
            }
        )

        logger.info(f"Created async Lakebase engine for {endpoint}")
        return engine

    def create_lakebase_engine_sync(
        self,
        endpoint: str,
        username: str,
        token: str
    ) -> Engine:
        """
        Create sync SQLAlchemy engine for Lakebase with SSL configuration.

        Uses pg8000 driver for sync operations (streaming contexts).

        Args:
            endpoint: Lakebase endpoint (DNS name)
            username: PostgreSQL username (URL-encoded)
            token: Database authentication token

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
                "ssl_context": True  # pg8000 uses ssl_context instead of sslmode
            }
        )

        logger.info(f"Created sync Lakebase engine for {endpoint}")
        return engine

    async def get_connected_user_identity(
        self,
        instance_name: str,
        endpoint: str
    ) -> Tuple[str, AsyncEngine]:
        """
        Generate credentials, test connections, and return the working user identity and engine.

        This is a convenience method that combines credential generation and connection testing.

        Args:
            instance_name: Name of the Lakebase instance
            endpoint: Lakebase endpoint (DNS name)

        Returns:
            Tuple of (user_identity, async_engine) that successfully connected

        Raises:
            Exception: If all connection attempts fail
        """
        # Generate credentials
        cred = await self.generate_credentials(instance_name)

        # Test connections
        engine, connected_user = await self.test_connections_async(endpoint, cred)

        if not engine or not connected_user:
            raise Exception("Failed to connect to Lakebase with any credentials")

        logger.info(f"Successfully connected to Lakebase as: {connected_user}")
        return connected_user, engine

    def resolve_postgresql_user(self, cred_user: str, connected_user: Optional[str] = None) -> str:
        """
        Resolve which PostgreSQL user to use for GRANT statements.

        Prefers the actual connected user over the credential user.

        Args:
            cred_user: User from database credential
            connected_user: User that successfully connected (if known)

        Returns:
            PostgreSQL username to use for permissions
        """
        if connected_user:
            logger.info(f"Using connected user for PostgreSQL role: {connected_user}")
            return connected_user
        else:
            logger.info(f"Using credential user for PostgreSQL role: {cred_user}")
            return cred_user
