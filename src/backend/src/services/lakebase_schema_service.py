"""
Lakebase Schema Service for managing database schema operations.

This service handles:
- Schema creation (CREATE SCHEMA IF NOT EXISTS kasal)
- Schema deletion (DROP SCHEMA IF EXISTS kasal CASCADE)
- Table creation from SQLAlchemy metadata
- Search path configuration (SET search_path TO kasal)
- Special handling for tables with vector columns (documentation_embeddings)
"""
import asyncio
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Tuple, Generator, AsyncGenerator, Dict, Any
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.engine import Engine

from src.core.base_service import BaseService
from src.db.base import Base

logger = logging.getLogger(__name__)

# --- SQL injection prevention helpers ---
_SAFE_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _validate_identifier(name: str, kind: str = "identifier") -> str:
    """Validate a SQL identifier (schema name, table name) to prevent injection.

    Only allows simple identifiers matching ``^[A-Za-z_][A-Za-z0-9_]*$``.

    Raises:
        ValueError: If the name does not match the safe pattern.
    """
    if not name or not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL {kind}: {name!r}")
    return name


def _quote_pg_role(identifier: str) -> str:
    """Safely quote a PostgreSQL role name.

    Accepts either an email address (local dev) or a UUID client_id
    (SPN in deployed Databricks Apps). Escapes embedded double-quotes
    so the result is safe for use as a quoted identifier in GRANT /
    ALTER DEFAULT PRIVILEGES statements.

    Raises:
        ValueError: If the identifier does not match expected formats.
    """
    # Email pattern (local dev)
    _EMAIL_RE = re.compile(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$')
    # UUID pattern (SPN client_id)
    _UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)

    if not identifier or not (_EMAIL_RE.match(identifier) or _UUID_RE.match(identifier)):
        raise ValueError(f"Invalid PostgreSQL role identifier: {identifier!r}")
    return '"' + identifier.replace('"', '""') + '"'


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
            safe_role = _quote_pg_role(user_email)

            # Handle schema recreation if requested
            if recreate:
                try:
                    async with engine.begin() as conn:
                        await conn.execute(text(f'ALTER SCHEMA kasal OWNER TO {safe_role}'))
                except Exception:
                    pass  # Schema may not exist yet
                try:
                    async with engine.begin() as conn:
                        logger.info("Dropping existing kasal schema (if exists)...")
                        await conn.execute(text("DROP SCHEMA IF EXISTS kasal CASCADE"))
                        logger.info("Dropped kasal schema")
                except Exception as drop_error:
                    logger.warning(f"Could not drop schema: {drop_error}")
                    logger.info("Proceeding with CREATE SCHEMA IF NOT EXISTS...")
                    # Transaction was aborted, but that's ok - we'll create schema in next transaction

            # Create schema in a fresh transaction
            async with engine.begin() as conn:
                # Create kasal schema if it doesn't exist
                await conn.execute(text("CREATE SCHEMA IF NOT EXISTS kasal"))
                logger.info("Created kasal schema in Lakebase")

                # Grant schema permissions
                try:
                    await conn.execute(text(f'GRANT ALL ON SCHEMA kasal TO {safe_role}'))
                    await conn.execute(text(f'GRANT ALL ON SCHEMA public TO {safe_role}'))
                    logger.info(f"Granted schema permissions to {user_email}")
                except Exception as grant_error:
                    # Log but don't fail - user might already have permissions
                    logger.warning(f"Permission grant warning (may be ok): {grant_error}")

                # Set default privileges for future objects
                try:
                    await conn.execute(
                        text(
                            f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal '
                            f'GRANT ALL ON TABLES TO {safe_role}'
                        )
                    )
                    await conn.execute(
                        text(
                            f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal '
                            f'GRANT ALL ON SEQUENCES TO {safe_role}'
                        )
                    )
                    logger.info(f"Set default privileges for {user_email}")
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
            safe_role = _quote_pg_role(user_email)

            # Handle schema recreation if requested
            if recreate:
                try:
                    with engine.begin() as conn:
                        conn.execute(text(f'ALTER SCHEMA kasal OWNER TO {safe_role}'))
                except Exception:
                    pass  # Schema may not exist yet
                try:
                    with engine.begin() as conn:
                        logger.info("Dropping existing kasal schema (if exists)...")
                        conn.execute(text("DROP SCHEMA IF EXISTS kasal CASCADE"))
                        logger.info("Dropped kasal schema")
                except Exception as drop_error:
                    logger.warning(f"Could not drop schema: {drop_error}")
                    logger.info("Proceeding with CREATE SCHEMA IF NOT EXISTS...")

            # Create schema in a fresh transaction
            with engine.begin() as conn:
                # Create kasal schema if it doesn't exist
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS kasal"))
                logger.info("Created kasal schema")

                # Grant schema permissions
                try:
                    conn.execute(text(f'GRANT ALL ON SCHEMA kasal TO {safe_role}'))
                    conn.execute(
                        text(
                            f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal '
                            f'GRANT ALL ON TABLES TO {safe_role}'
                        )
                    )
                    conn.execute(
                        text(
                            f'ALTER DEFAULT PRIVILEGES IN SCHEMA kasal '
                            f'GRANT ALL ON SEQUENCES TO {safe_role}'
                        )
                    )
                    logger.info(f"Granted schema permissions to {user_email}")
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
                logger.info("Set kasal schema as default search path")

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

                logger.info("Created table structure in Lakebase")

        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise

    def create_tables_sync(self, engine: Engine) -> None:
        """
        Create all tables from SQLAlchemy metadata in kasal schema (sync version).

        Uses parallel dependency waves for faster creation on remote Lakebase.

        Args:
            engine: Sync Engine for Lakebase connection

        Raises:
            Exception: If table creation fails
        """
        try:
            tables_to_skip = {'documentation_embeddings'}
            all_tables = Base.metadata.sorted_tables
            waves, table_map = self._get_dependency_waves(all_tables)
            max_parallel = 10

            logger.info(f"Creating {len(all_tables)} tables in {len(waves)} waves")

            for wave_table_names in waves:
                normal = [n for n in wave_table_names if n not in tables_to_skip]
                special = [n for n in wave_table_names if n in tables_to_skip]

                if normal:
                    if len(normal) <= 2:
                        self._create_tables_batch_sync(engine, normal, table_map)
                        for name in normal:
                            logger.info(f"Created table {name}")
                    else:
                        n_workers = min(len(normal), max_parallel)
                        chunks: List[List[str]] = [[] for _ in range(n_workers)]
                        for i, name in enumerate(normal):
                            chunks[i % n_workers].append(name)

                        with ThreadPoolExecutor(max_workers=n_workers) as executor:
                            futures = [
                                executor.submit(
                                    self._create_tables_batch_sync, engine, chunk, table_map
                                )
                                for chunk in chunks if chunk
                            ]
                            for future in as_completed(futures):
                                results = future.result()
                                for name, success, error in results:
                                    if success:
                                        logger.info(f"Created table {name}")
                                    else:
                                        logger.error(f"Error creating table {name}: {error}")

                for name in special:
                    if name == 'documentation_embeddings':
                        logger.info(f"Skipping table {name} (contains vector column)")
                        self._create_doc_embeddings_sync(engine)
                        logger.info("Created documentation_embeddings without vector column")

            logger.info("Created table structure in Lakebase")

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
                yield {"type": "success", "message": "Set kasal schema as default search path"}

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

                yield {"type": "success", "message": "Created table structure in Lakebase"}

        except (asyncio.CancelledError, GeneratorExit):
            logger.warning("Async table creation stream cancelled (client disconnected)")
            return
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            try:
                yield {"type": "error", "message": f"Error creating tables: {e}"}
            except (GeneratorExit, asyncio.CancelledError):
                return
            raise

    @staticmethod
    def _get_dependency_waves(tables) -> Tuple[List[List[str]], Dict[str, Any]]:
        """Group tables into parallel waves based on FK dependencies.

        Tables in the same wave have no FK dependencies on each other,
        so they can be created or populated concurrently.

        Returns:
            Tuple of (waves, table_map) where waves is a list of lists
            of table names, and table_map maps names to Table objects.
        """
        table_map = {t.name: t for t in tables}

        # Build dependency map: table_name -> set of referenced table names
        deps: Dict[str, set] = {}
        for table in tables:
            fk_deps = set()
            for fk in table.foreign_keys:
                ref_table = fk.column.table.name
                if ref_table != table.name:  # skip self-references
                    fk_deps.add(ref_table)
            deps[table.name] = fk_deps

        waves: List[List[str]] = []
        assigned: set = set()

        while len(assigned) < len(deps):
            # Tables whose dependencies are all in already-assigned waves
            wave = [
                name for name, d in deps.items()
                if name not in assigned and d.issubset(assigned)
            ]
            if not wave:
                # Circular deps or unresolvable — force remaining into final wave
                wave = [n for n in deps if n not in assigned]
            waves.append(wave)
            assigned.update(wave)

        return waves, table_map

    def _create_tables_batch_sync(
        self, engine: Engine, table_names: List[str], table_map: Dict[str, Any]
    ) -> List[Tuple[str, bool, Optional[str]]]:
        """Create multiple tables on a single connection. Thread-safe.

        Each call opens its own connection (NullPool creates a fresh one),
        sets search_path, then creates all tables in the batch sequentially.

        Returns:
            List of (table_name, success, error_message) tuples.
        """
        results = []
        with engine.begin() as conn:
            conn.execute(text("SET search_path TO kasal"))
            for name in table_names:
                try:
                    table_map[name].create(conn, checkfirst=True)
                    results.append((name, True, None))
                except Exception as e:
                    results.append((name, False, str(e)))
        return results

    def _create_doc_embeddings_sync(self, engine: Engine) -> None:
        """Create documentation_embeddings table without vector column."""
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
        with engine.begin() as conn:
            conn.execute(text("SET search_path TO kasal"))
            conn.execute(text(create_sql))

    def create_tables_sync_stream(self, engine: Engine) -> Generator[Dict[str, Any], None, None]:
        """Create all tables with streaming progress using parallel dependency waves.

        Groups tables by FK dependency depth and creates each wave in parallel
        using ThreadPoolExecutor. Tables within the same wave have no FK deps
        on each other, so they can be safely created concurrently on separate
        connections.

        Args:
            engine: Sync Engine for Lakebase connection

        Yields:
            Dict with event type and progress information
        """
        try:
            tables_to_skip = {'documentation_embeddings'}
            all_tables = Base.metadata.sorted_tables
            waves, table_map = self._get_dependency_waves(all_tables)

            total_tables = len(all_tables)
            created_count = 0
            max_parallel = 10  # max concurrent connections to Lakebase

            logger.info(
                f"Creating {total_tables} tables in {len(waves)} dependency waves"
            )

            for wave_idx, wave_table_names in enumerate(waves):
                normal = [n for n in wave_table_names if n not in tables_to_skip]
                special = [n for n in wave_table_names if n in tables_to_skip]

                if normal:
                    if len(normal) <= 2:
                        # Small wave — single connection, no threading overhead
                        results = self._create_tables_batch_sync(engine, normal, table_map)
                        for name, success, error in results:
                            if success:
                                created_count += 1
                                yield {"type": "success", "message": f"Created table {name}"}
                            else:
                                yield {"type": "error", "message": f"Error creating table {name}: {error}"}
                    else:
                        # Split across parallel connections
                        n_workers = min(len(normal), max_parallel)
                        chunks: List[List[str]] = [[] for _ in range(n_workers)]
                        for i, name in enumerate(normal):
                            chunks[i % n_workers].append(name)

                        with ThreadPoolExecutor(max_workers=n_workers) as executor:
                            futures = {
                                executor.submit(
                                    self._create_tables_batch_sync, engine, chunk, table_map
                                ): chunk
                                for chunk in chunks if chunk
                            }
                            for future in as_completed(futures):
                                try:
                                    results = future.result()
                                    for name, success, error in results:
                                        if success:
                                            created_count += 1
                                            yield {"type": "success", "message": f"Created table {name}"}
                                        else:
                                            yield {"type": "error", "message": f"Error creating table {name}: {error}"}
                                except Exception as e:
                                    chunk = futures[future]
                                    for name in chunk:
                                        yield {"type": "error", "message": f"Error creating table {name}: {e}"}

                # Handle special tables (need custom DDL)
                for name in special:
                    yield {"type": "info", "message": f"Skipping table {name} (contains vector column)"}
                    if name == 'documentation_embeddings':
                        self._create_doc_embeddings_sync(engine)
                        created_count += 1
                        yield {"type": "success", "message": f"Created {name} without vector column"}

            yield {"type": "success", "message": f"Created {created_count} tables in Lakebase ({len(waves)} waves, parallel)"}

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
            ValueError: If schema name is not a valid identifier
            Exception: If setting search path fails
        """
        try:
            safe_schema = _validate_identifier(schema, "schema name")
            await connection.execute(text(f"SET search_path TO {safe_schema}, public"))
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
            ValueError: If schema name is not a valid identifier
            Exception: If setting search path fails
        """
        try:
            safe_schema = _validate_identifier(schema, "schema name")
            connection.execute(text(f"SET search_path TO {safe_schema}"))
            logger.debug(f"Set search path to {schema}")
        except Exception as e:
            logger.error(f"Error setting search path: {e}")
            raise
