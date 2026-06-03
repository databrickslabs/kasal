"""
Lakebase memory service for managing pgvector memory tables.

Handles table initialization, connection testing, and statistics
for the Lakebase pgvector memory backend.
"""

import logging
from typing import Any, Dict, Optional

from src.core.logger import LoggerManager
from src.db.lakebase_session import get_lakebase_session

logger = LoggerManager.get_instance().system

# pgvector ("vector") is NOT a trusted PostgreSQL extension, so CREATE EXTENSION
# requires the databricks_superuser role. A deployed Databricks App's service
# principal only has CONNECT/CREATE/DML on the database (not superuser), so it
# cannot create the extension itself — it must be pre-created once by the
# Lakebase instance owner. Everything else (schema, tables, indexes) the app's
# service principal CAN create on its own.
PGVECTOR_ADMIN_INSTRUCTIONS = (
    "The pgvector extension is not enabled on this Lakebase instance, and the "
    "app's service principal does not have permission to create it "
    "(CREATE EXTENSION requires the databricks_superuser role).\n\n"
    "To fix this, the Lakebase instance owner must enable the extension ONCE. "
    "Connect to the instance (Databricks SQL editor, psql, or any client) as the "
    "owner and run:\n\n"
    "    CREATE EXTENSION IF NOT EXISTS vector;\n\n"
    "Then click 'Initialize Tables' again — the app will create the kasal schema, "
    "the memory table, and its indexes automatically."
)


class LakebaseMemoryService:
    """Service for managing Lakebase pgvector memory infrastructure."""

    def __init__(
        self,
        user_token: Optional[str] = None,
        instance_name: Optional[str] = None,
    ):
        self.user_token = user_token
        self.instance_name = instance_name

    async def test_connection(self) -> Dict[str, Any]:
        """
        Test connection to Lakebase and verify pgvector extension is available.

        Returns:
            Dict with success status and details
        """
        try:
            async with get_lakebase_session(
                instance_name=self.instance_name
            ) as session:
                from sqlalchemy import text

                # Get version info
                result = await session.execute(text("SELECT version()"))
                pg_version = result.scalar() or "unknown"

                # Check if pgvector extension is already enabled
                result = await session.execute(
                    text("SELECT extname FROM pg_extension WHERE extname IN ('vector', 'pgvector')")
                )
                row = result.fetchone()
                has_pgvector = row is not None

                details = {
                    "pgvector_available": has_pgvector,
                    "pg_version": pg_version,
                }
                if has_pgvector:
                    message = "Successfully connected to Lakebase with pgvector support"
                else:
                    # pgvector may need to be enabled by the instance owner before
                    # Initialize Tables can succeed (the app's service principal
                    # cannot create the extension itself). Surface the exact SQL.
                    message = (
                        "Successfully connected to Lakebase, but the pgvector "
                        "extension is not enabled. If 'Initialize Tables' fails, "
                        "the instance owner must run: "
                        "CREATE EXTENSION IF NOT EXISTS vector;"
                    )
                    details["pgvector_setup_instructions"] = (
                        PGVECTOR_ADMIN_INSTRUCTIONS
                    )
                    details["pgvector_setup_sql"] = (
                        "CREATE EXTENSION IF NOT EXISTS vector;"
                    )

                return {
                    "success": True,
                    "message": message,
                    "details": details,
                }

        except Exception as e:
            logger.error(f"Lakebase connection test failed: {e}")
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}",
                "details": {"error": str(e)},
            }

    async def initialize_tables(
        self,
        embedding_dimension: int = 1024,
        memory_table: str = "crew_memory",
    ) -> Dict[str, Any]:
        """
        Create pgvector extension and the unified memory table with indexes.

        CrewAI 1.10+ uses a single unified ``Memory`` class over one table.
        Uses HNSW indexes (work on empty tables, no periodic rebuilds needed).
        """
        tables = {"memory": memory_table}
        results = {}

        try:
            async with get_lakebase_session(
                instance_name=self.instance_name
            ) as session:
                from sqlalchemy import text

                # Enable pgvector extension
                # Lakebase documents the extension as 'vector'; some environments
                # also accept 'pgvector'. Check if already installed first — the
                # SPN used by deployed Databricks Apps often lacks CREATE EXTENSION
                # privileges, but can use an extension the instance owner pre-created.
                pgvector_enabled = False

                result = await session.execute(
                    text(
                        "SELECT extname FROM pg_extension "
                        "WHERE extname IN ('vector', 'pgvector')"
                    )
                )
                existing_ext = result.scalar()
                if existing_ext:
                    pgvector_enabled = True
                    logger.info(f"Extension '{existing_ext}' already enabled")
                else:
                    # Try to create — will succeed for instance owners / superusers
                    for ext_name in ("vector", "pgvector"):
                        try:
                            await session.execute(text("SAVEPOINT ext_attempt"))
                            await session.execute(text(f"CREATE EXTENSION IF NOT EXISTS {ext_name}"))
                            await session.execute(text("RELEASE SAVEPOINT ext_attempt"))
                            pgvector_enabled = True
                            logger.info(f"Extension '{ext_name}' created successfully")
                            break
                        except Exception as ext_err:
                            await session.execute(text("ROLLBACK TO SAVEPOINT ext_attempt"))
                            logger.debug(f"Extension '{ext_name}' not available: {ext_err}")

                if not pgvector_enabled:
                    return {
                        "success": False,
                        "message": PGVECTOR_ADMIN_INSTRUCTIONS,
                        "tables": results,
                    }

                # Ensure the kasal schema exists before creating tables in it.
                # The app's service principal has CREATE on the database, so it
                # can create the schema itself (unlike the extension above).
                # check_tables_initialized() queries table_schema='kasal', so the
                # table MUST live in kasal — qualify all DDL explicitly rather
                # than relying solely on search_path.
                await session.execute(text("CREATE SCHEMA IF NOT EXISTS kasal"))

                for memory_type, table_name in tables.items():
                    try:
                        qualified_table = f"kasal.{table_name}"
                        # Unified memory schema. Cognitive fields (scope,
                        # categories, importance, source, private) are kept
                        # inside the ``metadata`` JSONB column; session_id
                        # remains a top-level column for cheap run-scoped
                        # filtering.
                        create_sql = text(f"""
                            CREATE TABLE IF NOT EXISTS {qualified_table} (
                                id TEXT PRIMARY KEY,
                                crew_id TEXT NOT NULL,
                                group_id TEXT NOT NULL DEFAULT '',
                                session_id TEXT NOT NULL DEFAULT '',
                                agent TEXT NOT NULL DEFAULT '',
                                content TEXT NOT NULL,
                                metadata JSONB DEFAULT '{{}}'::jsonb,
                                score FLOAT,
                                embedding vector({embedding_dimension}),
                                created_at TIMESTAMPTZ DEFAULT NOW(),
                                updated_at TIMESTAMPTZ DEFAULT NOW()
                            )
                        """)
                        await session.execute(create_sql)

                        # HNSW index on embedding column for cosine similarity.
                        await session.execute(text(f"""
                            CREATE INDEX IF NOT EXISTS idx_{table_name}_embedding
                            ON {qualified_table}
                            USING hnsw (embedding vector_cosine_ops)
                        """))

                        # B-tree indexes for tenant / session filtering.
                        await session.execute(text(f"""
                            CREATE INDEX IF NOT EXISTS idx_{table_name}_crew_id
                            ON {qualified_table} (crew_id)
                        """))
                        await session.execute(text(f"""
                            CREATE INDEX IF NOT EXISTS idx_{table_name}_group_id
                            ON {qualified_table} (group_id)
                        """))
                        await session.execute(text(f"""
                            CREATE INDEX IF NOT EXISTS idx_{table_name}_session_id
                            ON {qualified_table} (session_id)
                        """))

                        # GIN index on metadata for scope / category filters.
                        await session.execute(text(f"""
                            CREATE INDEX IF NOT EXISTS idx_{table_name}_metadata
                            ON {qualified_table}
                            USING gin (metadata)
                        """))

                        results[memory_type] = {
                            "success": True,
                            "table_name": table_name,
                            "message": f"Table kasal.{table_name} initialized with HNSW + GIN indexes",
                        }
                        logger.info(
                            f"Initialized {memory_type} memory table: {table_name}"
                        )

                    except Exception as e:
                        results[memory_type] = {
                            "success": False,
                            "table_name": table_name,
                            "message": f"Failed to initialize: {str(e)}",
                        }
                        logger.error(
                            f"Failed to initialize {memory_type} table {table_name}: {e}"
                        )

            all_success = all(r["success"] for r in results.values())
            return {
                "success": all_success,
                "message": (
                    "All tables initialized"
                    if all_success
                    else "Some tables failed to initialize"
                ),
                "tables": results,
            }

        except Exception as e:
            logger.error(f"Table initialization failed: {e}")
            return {
                "success": False,
                "message": f"Initialization failed: {str(e)}",
                "tables": results,
            }

    async def check_tables_initialized(
        self,
        memory_table: str = "crew_memory",
    ) -> Dict[str, bool]:
        """Check whether the unified memory table exists."""
        tables = {"memory": memory_table}
        status = {}

        try:
            async with get_lakebase_session(
                instance_name=self.instance_name
            ) as session:
                from sqlalchemy import text

                for memory_type, table_name in tables.items():
                    result = await session.execute(
                        text("""
                            SELECT EXISTS (
                                SELECT 1 FROM information_schema.tables
                                WHERE table_schema = 'kasal'
                                  AND table_name = :table_name
                            )
                        """),
                        {"table_name": table_name},
                    )
                    status[memory_type] = result.scalar() or False

        except Exception as e:
            logger.error(f"Failed to check table status: {e}")
            for memory_type in tables:
                status[memory_type] = False

        return status

    async def get_table_stats(
        self,
        memory_table: str = "crew_memory",
    ) -> Dict[str, Any]:
        """Get row-count statistics for the unified memory table."""
        tables = {"memory": memory_table}
        stats = {}

        try:
            async with get_lakebase_session(
                instance_name=self.instance_name
            ) as session:
                from sqlalchemy import text

                for memory_type, table_name in tables.items():
                    try:
                        # Check existence
                        exists_result = await session.execute(
                            text("""
                                SELECT EXISTS (
                                    SELECT 1 FROM information_schema.tables
                                    WHERE table_name = :table_name
                                )
                            """),
                            {"table_name": table_name},
                        )
                        exists = exists_result.scalar() or False

                        row_count = 0
                        if exists:
                            count_result = await session.execute(
                                text(f"SELECT COUNT(*) FROM {table_name}")
                            )
                            row_count = count_result.scalar() or 0

                        stats[memory_type] = {
                            "table_name": table_name,
                            "exists": exists,
                            "row_count": row_count,
                        }
                    except Exception as e:
                        stats[memory_type] = {
                            "table_name": table_name,
                            "exists": False,
                            "row_count": 0,
                            "error": str(e),
                        }

        except Exception as e:
            logger.error(f"Failed to get table stats: {e}")
            for memory_type, table_name in tables.items():
                stats[memory_type] = {
                    "table_name": table_name,
                    "exists": False,
                    "row_count": 0,
                    "error": str(e),
                }

        return stats

    async def get_table_data(
        self,
        table_name: str,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        Fetch rows from a Lakebase memory table.

        Args:
            table_name: Name of the memory table to query
            limit: Maximum number of rows to return

        Returns:
            Dict with success, documents list, and total count
        """
        # Whitelist allowed table names to prevent SQL injection
        allowed_tables = {
            "crew_short_term_memory",
            "crew_long_term_memory",
            "crew_entity_memory",
        }
        if table_name not in allowed_tables:
            return {
                "success": False,
                "message": f"Invalid table name: {table_name}",
                "documents": [],
            }

        try:
            async with get_lakebase_session(
                instance_name=self.instance_name
            ) as session:
                from sqlalchemy import text

                # Get total count
                count_result = await session.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                )
                total = count_result.scalar() or 0

                # Fetch rows (exclude embedding column — too large)
                result = await session.execute(
                    text(f"""
                        SELECT id, crew_id, group_id, session_id, agent,
                               content, metadata, score, created_at, updated_at
                        FROM {table_name}
                        ORDER BY created_at DESC
                        LIMIT :limit
                    """),
                    {"limit": limit},
                )
                rows = result.fetchall()

                documents = []
                for row in rows:
                    metadata_val = row[6]
                    if isinstance(metadata_val, str):
                        try:
                            import json
                            metadata_val = json.loads(metadata_val)
                        except (ValueError, TypeError):
                            metadata_val = {}

                    documents.append({
                        "id": row[0],
                        "crew_id": row[1],
                        "group_id": row[2],
                        "session_id": row[3],
                        "agent": row[4],
                        "text": row[5],
                        "metadata": metadata_val or {},
                        "score": row[7],
                        "created_at": str(row[8]) if row[8] else None,
                        "updated_at": str(row[9]) if row[9] else None,
                    })

                return {
                    "success": True,
                    "documents": documents,
                    "total": total,
                }

        except Exception as e:
            logger.error(f"Failed to fetch table data from {table_name}: {e}")
            return {
                "success": False,
                "message": f"Failed to fetch data: {str(e)}",
                "documents": [],
            }

    async def get_entity_data(
        self,
        memory_table: str = "crew_memory",
        limit: int = 200,
    ) -> Dict[str, Any]:
        """
        Fetch entity-like memory records and format them for graph visualization.

        CrewAI 1.10+ no longer stores entities in a dedicated table — this
        method now reads from the unified ``crew_memory`` table and expects
        any entities to be tagged with an "entity" category by the caller
        (or their extraction pipeline).

        Returns:
            Dict with entities list and relationships list
        """
        allowed_tables = {
            "crew_memory",
            # Legacy names kept so saved configs don't break.
            "crew_entity_memory",
        }
        if memory_table not in allowed_tables:
            return {"entities": [], "relationships": []}
        entity_table = memory_table  # local alias preserved for the SQL below

        try:
            async with get_lakebase_session(
                instance_name=self.instance_name
            ) as session:
                from sqlalchemy import text
                import json

                result = await session.execute(
                    text(f"""
                        SELECT id, crew_id, agent, content, metadata, score
                        FROM {entity_table}
                        ORDER BY created_at DESC
                        LIMIT :limit
                    """),
                    {"limit": limit},
                )
                rows = result.fetchall()

                entities = []
                relationships = []
                seen_entity_ids = set()

                for row in rows:
                    record_id = row[0]
                    # SELECT id(0), crew_id(1), agent(2), content(3), metadata(4), score(5)
                    content = row[3] or ""
                    metadata_val = row[4]

                    if isinstance(metadata_val, str):
                        try:
                            metadata_val = json.loads(metadata_val)
                        except (ValueError, TypeError):
                            metadata_val = {}
                    metadata_val = metadata_val or {}

                    # Extract entity name — try metadata first, fall back to content
                    entity_name = (
                        metadata_val.get("entity_name")
                        or metadata_val.get("name")
                        or content[:80]
                    )
                    entity_type = (
                        metadata_val.get("entity_type")
                        or metadata_val.get("type")
                        or "entity"
                    )

                    entity_id = entity_name or record_id
                    if entity_id not in seen_entity_ids:
                        seen_entity_ids.add(entity_id)
                        entities.append({
                            "id": entity_id,
                            "name": entity_name,
                            "type": entity_type,
                            "attributes": {
                                "agent": row[2],
                                "crew_id": row[1],
                                "content": content,
                                "score": row[5],
                                **{k: v for k, v in metadata_val.items()
                                   if k not in ("entity_name", "name", "entity_type", "type")},
                            },
                        })

                    # Extract relationships from metadata
                    related_to = metadata_val.get("related_to") or metadata_val.get("relationships") or []
                    if isinstance(related_to, str):
                        related_to = [r.strip() for r in related_to.split(",") if r.strip()]
                    if isinstance(related_to, list):
                        for target in related_to:
                            target_name = target if isinstance(target, str) else str(target)
                            relationships.append({
                                "source": entity_id,
                                "target": target_name,
                                "type": "related_to",
                            })
                            # Ensure target node exists
                            if target_name not in seen_entity_ids:
                                seen_entity_ids.add(target_name)
                                entities.append({
                                    "id": target_name,
                                    "name": target_name,
                                    "type": "entity",
                                    "attributes": {},
                                })

                return {
                    "entities": entities,
                    "relationships": relationships,
                }

        except Exception as e:
            logger.error(f"Failed to fetch entity data from {entity_table}: {e}")
            return {"entities": [], "relationships": []}
