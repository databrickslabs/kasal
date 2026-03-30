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
                    text("SELECT extname FROM pg_extension WHERE extname IN ('pgvector', 'vector')")
                )
                row = result.fetchone()
                has_pgvector = row is not None

                return {
                    "success": True,
                    "message": (
                        "Successfully connected to Lakebase with pgvector support"
                        if has_pgvector
                        else "Successfully connected to Lakebase. Click 'Initialize Tables' to enable pgvector and create memory tables."
                    ),
                    "details": {
                        "pgvector_available": has_pgvector,
                        "pg_version": pg_version,
                    },
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
        short_term_table: str = "crew_short_term_memory",
        long_term_table: str = "crew_long_term_memory",
        entity_table: str = "crew_entity_memory",
    ) -> Dict[str, Any]:
        """
        Create pgvector extension and memory tables with indexes.

        Uses HNSW indexes (work on empty tables, no periodic rebuilds needed).
        """
        tables = {
            "short_term": short_term_table,
            "long_term": long_term_table,
            "entity": entity_table,
        }
        results = {}

        try:
            async with get_lakebase_session(
                instance_name=self.instance_name
            ) as session:
                from sqlalchemy import text

                # Enable pgvector extension
                # Databricks Lakebase uses 'pgvector', standard PostgreSQL uses 'vector'
                # Use SAVEPOINT so a failed attempt doesn't poison the transaction
                pgvector_enabled = False
                for ext_name in ("pgvector", "vector"):
                    try:
                        await session.execute(text("SAVEPOINT ext_attempt"))
                        await session.execute(text(f"CREATE EXTENSION IF NOT EXISTS {ext_name}"))
                        await session.execute(text("RELEASE SAVEPOINT ext_attempt"))
                        pgvector_enabled = True
                        logger.info(f"Extension '{ext_name}' enabled successfully")
                        break
                    except Exception as ext_err:
                        await session.execute(text("ROLLBACK TO SAVEPOINT ext_attempt"))
                        logger.debug(f"Extension '{ext_name}' not available: {ext_err}")

                if not pgvector_enabled:
                    return {
                        "success": False,
                        "message": "Could not enable pgvector extension. Ensure pgvector is available in your Lakebase project.",
                        "tables": results,
                    }

                for memory_type, table_name in tables.items():
                    try:
                        # Create table
                        create_sql = text(f"""
                            CREATE TABLE IF NOT EXISTS {table_name} (
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

                        # Create HNSW index on embedding column
                        await session.execute(text(f"""
                            CREATE INDEX IF NOT EXISTS idx_{table_name}_embedding
                            ON {table_name}
                            USING hnsw (embedding vector_cosine_ops)
                        """))

                        # Create B-tree indexes for filtering
                        await session.execute(text(f"""
                            CREATE INDEX IF NOT EXISTS idx_{table_name}_crew_id
                            ON {table_name} (crew_id)
                        """))
                        await session.execute(text(f"""
                            CREATE INDEX IF NOT EXISTS idx_{table_name}_group_id
                            ON {table_name} (group_id)
                        """))

                        if memory_type == "short_term":
                            await session.execute(text(f"""
                                CREATE INDEX IF NOT EXISTS idx_{table_name}_session_id
                                ON {table_name} (session_id)
                            """))

                        results[memory_type] = {
                            "success": True,
                            "table_name": table_name,
                            "message": f"Table {table_name} initialized with HNSW index",
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
        short_term_table: str = "crew_short_term_memory",
        long_term_table: str = "crew_long_term_memory",
        entity_table: str = "crew_entity_memory",
    ) -> Dict[str, bool]:
        """
        Check if all required memory tables exist.

        Returns:
            Dict mapping table name to existence boolean
        """
        tables = {
            "short_term": short_term_table,
            "long_term": long_term_table,
            "entity": entity_table,
        }
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
        short_term_table: str = "crew_short_term_memory",
        long_term_table: str = "crew_long_term_memory",
        entity_table: str = "crew_entity_memory",
    ) -> Dict[str, Any]:
        """
        Get row counts and existence info for all memory tables.

        Returns:
            Dict with per-table statistics
        """
        tables = {
            "short_term": short_term_table,
            "long_term": long_term_table,
            "entity": entity_table,
        }
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
        entity_table: str = "crew_entity_memory",
        limit: int = 200,
    ) -> Dict[str, Any]:
        """
        Fetch entity memory data and format it for graph visualization.

        Parses entity content and metadata to extract nodes and relationships
        in the same format used by the Databricks entity-data endpoint.

        Returns:
            Dict with entities list and relationships list
        """
        allowed_tables = {
            "crew_entity_memory",
        }
        if entity_table not in allowed_tables:
            return {"entities": [], "relationships": []}

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
