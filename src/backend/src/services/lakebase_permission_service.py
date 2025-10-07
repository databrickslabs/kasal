"""
Lakebase Permission Service for managing database permissions.

This service handles all permission-related operations for Lakebase instances,
including schema permissions, default privileges, and error handling.
"""
import logging
from typing import Optional
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.engine import Connection

from src.core.base_service import BaseService

logger = logging.getLogger(__name__)


class LakebasePermissionService(BaseService):
    """Service for managing Lakebase database permissions."""

    def __init__(self):
        """
        Initialize Lakebase permission service.

        Note: This service doesn't require a database session as it operates
        directly on engine connections for permission management.
        """
        # Don't call super().__init__() as we don't need a session
        pass

    async def grant_schema_permissions_async(
        self,
        engine: AsyncEngine,
        user_email: str
    ) -> None:
        """
        Grant schema permissions to a user asynchronously.

        This method grants ALL privileges on the kasal and public schemas
        to the specified user. Permission errors are logged as warnings
        but do not cause the operation to fail.

        Args:
            engine: AsyncEngine connected to the Lakebase instance
            user_email: Email/username of the user to grant permissions to

        Note:
            This method handles exceptions gracefully - permission errors
            are logged but don't fail the migration process.
        """
        try:
            async with engine.begin() as conn:
                # Grant all privileges on kasal schema
                await conn.execute(
                    text(f'GRANT ALL ON SCHEMA kasal TO "{user_email}"')
                )
                # Grant all privileges on public schema
                await conn.execute(
                    text(f'GRANT ALL ON SCHEMA public TO "{user_email}"')
                )
                logger.info(f"✅ Granted schema permissions to {user_email}")
        except Exception as grant_error:
            # Log but don't fail - user might already have permissions
            # or permissions might be set differently in the environment
            logger.warning(
                f"Permission grant warning for {user_email} (may be ok): {grant_error}"
            )

    def grant_schema_permissions_sync(
        self,
        connection: Connection,
        user_email: str
    ) -> None:
        """
        Grant schema permissions to a user synchronously.

        This method grants ALL privileges on the kasal and public schemas
        to the specified user. Permission errors are logged as warnings
        but do not cause the operation to fail.

        Args:
            connection: Active database connection
            user_email: Email/username of the user to grant permissions to

        Note:
            This method handles exceptions gracefully - permission errors
            are logged but don't fail the migration process.
        """
        try:
            # Grant all privileges on kasal schema
            connection.execute(
                text(f'GRANT ALL ON SCHEMA kasal TO "{user_email}"')
            )
            # Grant all privileges on public schema
            connection.execute(
                text(f'GRANT ALL ON SCHEMA public TO "{user_email}"')
            )
            logger.info(f"✅ Granted schema permissions to {user_email}")
        except Exception as grant_error:
            # Log but don't fail - user might already have permissions
            # or permissions might be set differently in the environment
            logger.warning(
                f"Permission grant warning for {user_email} (may be ok): {grant_error}"
            )

    async def grant_default_privileges_async(
        self,
        engine: AsyncEngine,
        user_email: str
    ) -> None:
        """
        Set default privileges for future objects asynchronously.

        This method configures default privileges so that any tables or
        sequences created in the kasal schema will automatically grant
        ALL privileges to the specified user.

        Args:
            engine: AsyncEngine connected to the Lakebase instance
            user_email: Email/username of the user to grant default privileges to

        Note:
            This method handles exceptions gracefully - privilege errors
            are logged but don't fail the migration process.
        """
        try:
            async with engine.begin() as conn:
                # Set default privileges for tables
                await conn.execute(
                    text(
                        f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal '
                        f'GRANT ALL ON TABLES TO "{user_email}"'
                    )
                )
                # Set default privileges for sequences
                await conn.execute(
                    text(
                        f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal '
                        f'GRANT ALL ON SEQUENCES TO "{user_email}"'
                    )
                )
                logger.info(f"✅ Set default privileges for {user_email}")
        except Exception as privilege_error:
            # Log but don't fail - default privileges might be set differently
            # or the user might not have permission to alter default privileges
            logger.warning(
                f"Default privilege warning for {user_email} (may be ok): {privilege_error}"
            )

    def grant_default_privileges_sync(
        self,
        connection: Connection,
        user_email: str
    ) -> None:
        """
        Set default privileges for future objects synchronously.

        This method configures default privileges so that any tables or
        sequences created in the kasal schema will automatically grant
        ALL privileges to the specified user.

        Args:
            connection: Active database connection
            user_email: Email/username of the user to grant default privileges to

        Note:
            This method handles exceptions gracefully - privilege errors
            are logged but don't fail the migration process.
        """
        try:
            # Set default privileges for tables
            connection.execute(
                text(
                    f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal '
                    f'GRANT ALL ON TABLES TO "{user_email}"'
                )
            )
            # Set default privileges for sequences
            connection.execute(
                text(
                    f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal '
                    f'GRANT ALL ON SEQUENCES TO "{user_email}"'
                )
            )
            logger.info(f"✅ Set default privileges for {user_email}")
        except Exception as privilege_error:
            # Log but don't fail - default privileges might be set differently
            # or the user might not have permission to alter default privileges
            logger.warning(
                f"Default privilege warning for {user_email} (may be ok): {privilege_error}"
            )

    async def grant_all_permissions_async(
        self,
        engine: AsyncEngine,
        user_email: str
    ) -> None:
        """
        Grant all permissions (schema + default privileges) asynchronously.

        This is a convenience method that combines schema permissions and
        default privileges in a single call.

        Args:
            engine: AsyncEngine connected to the Lakebase instance
            user_email: Email/username of the user to grant permissions to
        """
        await self.grant_schema_permissions_async(engine, user_email)
        await self.grant_default_privileges_async(engine, user_email)

    def grant_all_permissions_sync(
        self,
        connection: Connection,
        user_email: str
    ) -> None:
        """
        Grant all permissions (schema + default privileges) synchronously.

        This is a convenience method that combines schema permissions and
        default privileges in a single call.

        Args:
            connection: Active database connection
            user_email: Email/username of the user to grant permissions to
        """
        self.grant_schema_permissions_sync(connection, user_email)
        self.grant_default_privileges_sync(connection, user_email)
