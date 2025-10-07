"""
Lakebase Schema Service for managing database schema operations.

This service handles:
- Schema creation (CREATE SCHEMA IF NOT EXISTS kasal)
- Schema deletion (DROP SCHEMA IF EXISTS kasal CASCADE)
- Table creation from SQLAlchemy metadata
- Search path configuration (SET search_path TO kasal)
- Special handling for tables with vector columns (documentation_embeddings)
"""
import logging
from typing import Optional, AsyncGenerator, Dict, Any
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.engine import Engine

from src.core.base_service import BaseService
from src.db.base import Base

logger = logging.getLogger(__name__)


class LakebaseSchemaService(BaseService):
    """Service for managing Lakebase database schema operations."""

    def __init__(self):
        """
        Initialize Lakebase schema service.

        Note: This service does not require a database session as it operates
        directly on engines passed to its methods.
        """
        # No session needed for schema operations
        pass

    async def create_schema_async(
        self,
        engine: AsyncEngine,
        user_email: str,
        recreate: bool = False
    ) -> None:
        """
        Create kasal schema in Lakebase database (async version).

        Args:
            engine: AsyncEngine for Lakebase connection
            user_email: User email for permission grants
            recreate: If True, drop existing schema before creating

        Raises:
            Exception: If schema creation fails
        """
        try:
            # Handle schema recreation if requested
            if recreate:
                try:
                    async with engine.begin() as conn:
                        logger.info("🗑️ Dropping existing kasal schema (if exists)...")
                        await conn.execute(text("DROP SCHEMA IF EXISTS kasal CASCADE"))
                        logger.info("✅ Dropped kasal schema")
                except Exception as drop_error:
                    logger.warning(
                        f"Could not drop schema (may be owned by different role): {drop_error}"
                    )
                    logger.info("Proceeding with CREATE SCHEMA IF NOT EXISTS...")
                    # Transaction was aborted, but that's ok - we'll create schema in next transaction

            # Create schema in a fresh transaction
            async with engine.begin() as conn:
                # Create kasal schema if it doesn't exist
                await conn.execute(text("CREATE SCHEMA IF NOT EXISTS kasal"))
                logger.info("✅ Created kasal schema in Lakebase")

                # Grant schema permissions
                try:
                    await conn.execute(text(f'GRANT ALL ON SCHEMA kasal TO "{user_email}"'))
                    await conn.execute(text(f'GRANT ALL ON SCHEMA public TO "{user_email}"'))
                    logger.info(f"✅ Granted schema permissions to {user_email}")
                except Exception as grant_error:
                    # Log but don't fail - user might already have permissions
                    logger.warning(f"Permission grant warning (may be ok): {grant_error}")

                # Set default privileges for future objects
                try:
                    await conn.execute(
                        text(
                            f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal '
                            f'GRANT ALL ON TABLES TO "{user_email}"'
                        )
                    )
                    await conn.execute(
                        text(
                            f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal '
                            f'GRANT ALL ON SEQUENCES TO "{user_email}"'
                        )
                    )
                    logger.info(f"✅ Set default privileges for {user_email}")
                except Exception as privilege_error:
                    logger.warning(f"Default privilege warning (may be ok): {privilege_error}")

        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            raise

    def create_schema_sync(
        self,
        engine: Engine,
        user_email: str,
        recreate: bool = False
    ) -> None:
        """
        Create kasal schema in Lakebase database (sync version).

        Args:
            engine: Sync Engine for Lakebase connection
            user_email: User email for permission grants
            recreate: If True, drop existing schema before creating

        Raises:
            Exception: If schema creation fails
        """
        try:
            # Handle schema recreation if requested
            if recreate:
                try:
                    with engine.begin() as conn:
                        logger.info("🗑️ Dropping existing kasal schema (if exists)...")
                        conn.execute(text("DROP SCHEMA IF EXISTS kasal CASCADE"))
                        logger.info("✅ Dropped kasal schema")
                except Exception as drop_error:
                    logger.warning(
                        f"Could not drop schema (may be owned by different role): {drop_error}"
                    )
                    logger.info("Proceeding with CREATE SCHEMA IF NOT EXISTS...")

            # Create schema in a fresh transaction
            with engine.begin() as conn:
                # Create kasal schema if it doesn't exist
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS kasal"))
                logger.info("✅ Created kasal schema")

                # Grant schema permissions
                try:
                    conn.execute(text(f'GRANT ALL ON SCHEMA kasal TO "{user_email}"'))
                    conn.execute(
                        text(
                            f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal '
                            f'GRANT ALL ON TABLES TO "{user_email}"'
                        )
                    )
                    conn.execute(
                        text(
                            f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal '
                            f'GRANT ALL ON SEQUENCES TO "{user_email}"'
                        )
                    )
                    logger.info(f"✅ Granted schema permissions to {user_email}")
                except Exception as grant_error:
                    logger.warning(f"Permission grant warning: {grant_error}")

        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            raise

    async def create_tables_async(self, engine: AsyncEngine) -> None:
        """
        Create all tables from SQLAlchemy metadata in kasal schema (async version).

        Handles special case for documentation_embeddings table which contains
        vector columns not supported by Lakebase.

        Args:
            engine: AsyncEngine for Lakebase connection

        Raises:
            Exception: If table creation fails
        """
        try:
            async with engine.begin() as conn:
                # Set kasal as the default schema for this connection
                await conn.execute(text("SET search_path TO kasal, public"))
                logger.info("✅ Set kasal schema as default search path")

                # Tables with vector columns that need special handling
                tables_to_skip = ['documentation_embeddings']

                # Get all table objects from metadata
                for table in Base.metadata.sorted_tables:
                    if table.name in tables_to_skip:
                        logger.info(f"Skipping table {table.name} (contains vector column)")
                        # Create a modified version without vector column
                        if table.name == 'documentation_embeddings':
                            # Create table without the embedding column
                            create_sql = """
                            CREATE TABLE IF NOT EXISTS documentation_embeddings (
                                id SERIAL PRIMARY KEY,
                                source VARCHAR NOT NULL,
                                title VARCHAR NOT NULL,
                                content TEXT NOT NULL,
                                doc_metadata JSON,
                                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
                            )
                            """
                            await conn.execute(text(create_sql))
                            logger.info("Created documentation_embeddings table without vector column")
                    else:
                        # Create table normally using SQLAlchemy metadata
                        await conn.run_sync(table.create, checkfirst=True)
                        logger.info(f"Created table {table.name}")

                logger.info("✅ Created table structure in Lakebase")

        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise

    def create_tables_sync(self, engine: Engine) -> None:
        """
        Create all tables from SQLAlchemy metadata in kasal schema (sync version).

        Handles special case for documentation_embeddings table which contains
        vector columns not supported by Lakebase.

        Args:
            engine: Sync Engine for Lakebase connection

        Raises:
            Exception: If table creation fails
        """
        try:
            with engine.begin() as conn:
                # Set kasal as the default schema for this connection
                conn.execute(text("SET search_path TO kasal"))
                logger.info("✅ Set kasal schema as default search path")

                # Tables with vector columns that need special handling
                tables_to_skip = ['documentation_embeddings']

                # Get all table objects from metadata
                for table in Base.metadata.sorted_tables:
                    if table.name in tables_to_skip:
                        logger.info(f"Skipping table {table.name} (contains vector column)")
                        # Create a modified version without vector column
                        if table.name == 'documentation_embeddings':
                            # Create table without the embedding column
                            create_sql = """
                            CREATE TABLE IF NOT EXISTS documentation_embeddings (
                                id SERIAL PRIMARY KEY,
                                source VARCHAR NOT NULL,
                                title VARCHAR NOT NULL,
                                content TEXT NOT NULL,
                                doc_metadata JSON,
                                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
                            )
                            """
                            conn.execute(text(create_sql))
                            logger.info("Created documentation_embeddings table without vector column")
                    else:
                        # Create table normally using SQLAlchemy metadata
                        table.create(conn, checkfirst=True)
                        logger.info(f"Created table {table.name}")

                logger.info("✅ Created table structure in Lakebase")

        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise

    async def create_tables_async_stream(
        self,
        engine: AsyncEngine
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Create all tables from SQLAlchemy metadata with streaming progress (async version).

        Yields progress events as tables are created.

        Args:
            engine: AsyncEngine for Lakebase connection

        Yields:
            Dict with event type and progress information
        """
        try:
            async with engine.begin() as conn:
                # Set kasal as the default schema for this connection
                await conn.execute(text("SET search_path TO kasal, public"))
                yield {"type": "success", "message": "✅ Set kasal schema as default search path"}

                # Tables with vector columns that need special handling
                tables_to_skip = ['documentation_embeddings']

                # Get all table objects from metadata
                for table in Base.metadata.sorted_tables:
                    if table.name in tables_to_skip:
                        yield {
                            "type": "info",
                            "message": f"Skipping table {table.name} (contains vector column)"
                        }
                        # Create a modified version without vector column
                        if table.name == 'documentation_embeddings':
                            create_sql = """
                            CREATE TABLE IF NOT EXISTS documentation_embeddings (
                                id SERIAL PRIMARY KEY,
                                source VARCHAR NOT NULL,
                                title VARCHAR NOT NULL,
                                content TEXT NOT NULL,
                                doc_metadata JSON,
                                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
                            )
                            """
                            await conn.execute(text(create_sql))
                            yield {
                                "type": "success",
                                "message": f"Created {table.name} without vector column"
                            }
                    else:
                        # Create table normally
                        await conn.run_sync(table.create, checkfirst=True)
                        yield {"type": "success", "message": f"Created table {table.name}"}

                yield {"type": "success", "message": "✅ Created table structure in Lakebase"}

        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            yield {"type": "error", "message": f"Error creating tables: {e}"}
            raise

    def create_tables_sync_stream(self, engine: Engine) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Create all tables from SQLAlchemy metadata with streaming progress (sync version).

        Yields progress events as tables are created.

        Args:
            engine: Sync Engine for Lakebase connection

        Yields:
            Dict with event type and progress information
        """
        try:
            with engine.begin() as conn:
                # Set kasal as the default schema for this connection
                conn.execute(text("SET search_path TO kasal"))
                yield {"type": "success", "message": "✅ Set kasal schema as default search path"}

                # Tables with vector columns that need special handling
                tables_to_skip = ['documentation_embeddings']

                # Get all table objects from metadata
                for table in Base.metadata.sorted_tables:
                    if table.name in tables_to_skip:
                        yield {
                            "type": "info",
                            "message": f"Skipping table {table.name} (contains vector column)"
                        }
                        # Create a modified version without vector column
                        if table.name == 'documentation_embeddings':
                            create_sql = """
                            CREATE TABLE IF NOT EXISTS documentation_embeddings (
                                id SERIAL PRIMARY KEY,
                                source VARCHAR NOT NULL,
                                title VARCHAR NOT NULL,
                                content TEXT NOT NULL,
                                doc_metadata JSON,
                                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
                            )
                            """
                            conn.execute(text(create_sql))
                            yield {
                                "type": "success",
                                "message": f"Created {table.name} without vector column"
                            }
                    else:
                        # Create table normally
                        table.create(conn, checkfirst=True)
                        yield {"type": "success", "message": f"Created table {table.name}"}

                yield {"type": "success", "message": "✅ Created table structure in Lakebase"}

        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            yield {"type": "error", "message": f"Error creating tables: {e}"}
            raise

    async def set_search_path_async(self, connection, schema: str = "kasal") -> None:
        """
        Set the search path for a database connection (async version).

        Args:
            connection: Async database connection
            schema: Schema name to set as search path (default: kasal)

        Raises:
            Exception: If setting search path fails
        """
        try:
            await connection.execute(text(f"SET search_path TO {schema}, public"))
            logger.debug(f"Set search path to {schema}")
        except Exception as e:
            logger.error(f"Error setting search path: {e}")
            raise

    def set_search_path_sync(self, connection, schema: str = "kasal") -> None:
        """
        Set the search path for a database connection (sync version).

        Args:
            connection: Sync database connection
            schema: Schema name to set as search path (default: kasal)

        Raises:
            Exception: If setting search path fails
        """
        try:
            connection.execute(text(f"SET search_path TO {schema}"))
            logger.debug(f"Set search path to {schema}")
        except Exception as e:
            logger.error(f"Error setting search path: {e}")
            raise
