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


def _is_not_owner_error(exc: Exception) -> bool:
    """True for PostgreSQL 'must be owner of …' (SQLSTATE 42501).

    The orphaned-owner case: a table was CREATEd by a previous deploy's service
    principal, so the role running the migration today doesn't own it and can't
    ALTER it. We tolerate this specific error rather than aborting the whole
    migration over one un-ownable table.
    """
    s = str(exc)
    return "42501" in s or "must be owner" in s.lower()


def _owner_remediation(stmt: str) -> str:
    """Actionable message for an ownership-blocked ALTER."""
    m = re.search(r'\bALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?([A-Za-z_][\w."]*)', stmt or "", re.IGNORECASE)
    table = m.group(1) if m else "the table"
    return (
        f"Cannot apply DDL — this role does not own {table} (Postgres 42501 'must be owner'). "
        f"It was created by a previous deploy's service principal (orphaned owner). A Lakebase "
        f"instance owner / superuser must reassign ownership, then re-run the migration:\n"
        f"    REASSIGN OWNED BY <old_owner_role> TO CURRENT_USER;\n"
        f"  (or per-table:  ALTER TABLE {table} OWNER TO \"<this_app_role>\";)\n"
        f"Skipped statement: {stmt}"
    )


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
                tables_to_skip = ['documentation_embeddings', 'knowledge_embeddings']

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
                                group_id VARCHAR(100),
                                file_path VARCHAR,
                                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
                            )
                            """
                            await conn.execute(text(create_sql))
                            await self._ensure_doc_embeddings_columns_async(conn)
                            logger.info("Created documentation_embeddings table (pgvector embedding + scoping columns ensured)")
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
            tables_to_skip = {'documentation_embeddings', 'knowledge_embeddings'}
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
                tables_to_skip = ['documentation_embeddings', 'knowledge_embeddings']

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
                                group_id VARCHAR(100),
                                file_path VARCHAR,
                                created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
                                updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
                            )
                            """
                            await conn.execute(text(create_sql))
                            await self._ensure_doc_embeddings_columns_async(conn)
                            yield {
                                "type": "success",
                                "message": f"Created {table.name} (pgvector embedding + scoping columns ensured)"
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

    # SQL statements that bring an existing documentation_embeddings table up to
    # the current schema. Idempotent (IF NOT EXISTS), so safe to run on every
    # init. group_id/file_path scope uploaded knowledge per workspace; the
    # embedding column + HNSW index back pgvector similarity search (this table
    # was historically created WITHOUT the vector column when Databricks Vector
    # Search was the document-embedding backend).
    _DOC_EMB_PLAIN_DDL = (
        "ALTER TABLE documentation_embeddings ADD COLUMN IF NOT EXISTS group_id VARCHAR(100)",
        "ALTER TABLE documentation_embeddings ADD COLUMN IF NOT EXISTS file_path VARCHAR",
        "CREATE INDEX IF NOT EXISTS idx_doc_emb_group_id ON documentation_embeddings (group_id)",
        "CREATE INDEX IF NOT EXISTS idx_doc_emb_file_path ON documentation_embeddings (file_path)",
    )
    _DOC_EMB_VECTOR_DDL = (
        "ALTER TABLE documentation_embeddings ADD COLUMN IF NOT EXISTS embedding vector(1024)",
        "CREATE INDEX IF NOT EXISTS idx_doc_emb_embedding ON documentation_embeddings "
        "USING hnsw (embedding vector_cosine_ops)",
    )
    _DOC_EMB_PGVECTOR_CHECK = (
        "SELECT 1 FROM pg_extension WHERE extname IN ('vector', 'pgvector')"
    )

    @staticmethod
    async def _exec_ddl_tolerant_async(conn, stmt: str) -> None:
        """Run one idempotent DDL statement inside a SAVEPOINT.

        The savepoint keeps an orphaned-owner failure (Postgres 42501 'must be
        owner') from poisoning the surrounding transaction; that specific error
        is logged with remediation and skipped so it doesn't abort the whole
        migration. Any other error still propagates.
        """
        try:
            async with conn.begin_nested():
                await conn.execute(text(stmt))
        except Exception as e:
            if _is_not_owner_error(e):
                logger.error(_owner_remediation(stmt))
                return
            raise

    @staticmethod
    def _exec_ddl_tolerant_sync(conn, stmt: str) -> None:
        """Sync counterpart of _exec_ddl_tolerant_async (savepoint-isolated)."""
        try:
            with conn.begin_nested():
                conn.execute(text(stmt))
        except Exception as e:
            if _is_not_owner_error(e):
                logger.error(_owner_remediation(stmt))
                return
            raise

    async def _ensure_doc_embeddings_columns_async(self, conn) -> None:
        """Bring the documentation_embeddings table up to the pgvector schema (async).

        Each DDL runs in its own savepoint and tolerates the orphaned-owner case
        (see _exec_ddl_tolerant_async), so a table owned by a previous deploy's
        service principal logs a clear remediation instead of aborting the
        migration with 'Error creating tables: must be owner of table'.
        """
        for stmt in self._DOC_EMB_PLAIN_DDL:
            await self._exec_ddl_tolerant_async(conn, stmt)
        # The vector column requires the pgvector extension; only add it when the
        # extension is present so we don't poison the surrounding transaction.
        result = await conn.execute(text(self._DOC_EMB_PGVECTOR_CHECK))
        if result.fetchone():
            for stmt in self._DOC_EMB_VECTOR_DDL:
                await self._exec_ddl_tolerant_async(conn, stmt)
        else:
            logger.warning(
                "pgvector extension not found; documentation_embeddings.embedding "
                "column not added. Have the instance owner run "
                "'CREATE EXTENSION IF NOT EXISTS vector;' then re-run initialization."
            )

    def _ensure_doc_embeddings_columns_sync(self, conn) -> None:
        """Bring the documentation_embeddings table up to the pgvector schema (sync)."""
        for stmt in self._DOC_EMB_PLAIN_DDL:
            self._exec_ddl_tolerant_sync(conn, stmt)
        result = conn.execute(text(self._DOC_EMB_PGVECTOR_CHECK))
        if result.fetchone():
            for stmt in self._DOC_EMB_VECTOR_DDL:
                self._exec_ddl_tolerant_sync(conn, stmt)
        else:
            logger.warning(
                "pgvector extension not found; documentation_embeddings.embedding "
                "column not added. Have the instance owner run "
                "'CREATE EXTENSION IF NOT EXISTS vector;' then re-run initialization."
            )

    def _create_doc_embeddings_sync(self, engine: Engine) -> None:
        """Create documentation_embeddings table and ensure pgvector schema."""
        create_sql = """
        CREATE TABLE IF NOT EXISTS documentation_embeddings (
            id SERIAL PRIMARY KEY,
            source VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            content TEXT NOT NULL,
            doc_metadata JSON,
            group_id VARCHAR(100),
            file_path VARCHAR,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
        )
        """
        with engine.begin() as conn:
            conn.execute(text("SET search_path TO kasal"))
            conn.execute(text(create_sql))
            self._ensure_doc_embeddings_columns_sync(conn)

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
            tables_to_skip = {'documentation_embeddings', 'knowledge_embeddings'}
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
