"""
Lakebase pgvector storage backend for CrewAI memory.

This module provides async storage using pgvector on the existing
Lakebase PostgreSQL instance, eliminating the need for separate
Databricks Vector Search infrastructure.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.core.logger import LoggerManager
from src.db.lakebase_session import get_lakebase_session

logger = LoggerManager.get_instance().crew


class LakebasePgVectorStorage:
    """
    Async storage backend using pgvector on Lakebase PostgreSQL.

    Uses raw SQL with the pgvector cosine distance operator (<=>) for
    similarity search. Tables are created in the kasal schema alongside
    the application tables.
    """

    def __init__(
        self,
        table_name: str,
        memory_type: str,
        crew_id: str,
        group_id: Optional[str] = None,
        job_id: Optional[str] = None,
        embedding_dimension: int = 1024,
        user_token: Optional[str] = None,
        instance_name: Optional[str] = None,
    ):
        self.table_name = table_name
        self.memory_type = memory_type
        self.crew_id = crew_id
        self.group_id = group_id
        self.job_id = job_id
        self.embedding_dimension = embedding_dimension
        # user_token kept for API compat but NOT used for auth.
        # Lakebase auth uses SPN (deployed) or PAT (local dev) only.
        self.user_token = user_token
        self.instance_name = instance_name

    async def save(
        self,
        record_id: str,
        content: str,
        embedding: List[float],
        metadata: Optional[Dict[str, Any]] = None,
        agent: Optional[str] = None,
        score: Optional[float] = None,
    ) -> None:
        """
        Save a memory record with its embedding vector.

        Uses INSERT ... ON CONFLICT for upserts.
        """
        if not record_id:
            record_id = str(uuid.uuid4())

        metadata = metadata or {}
        now = datetime.now(timezone.utc)

        embedding_str = "[" + ",".join(str(v) for v in embedding) + "]"

        async with get_lakebase_session(instance_name=self.instance_name, group_id=self.group_id) as session:
            from sqlalchemy import text

            sql = text(f"""
                INSERT INTO {self.table_name}
                    (id, crew_id, group_id, session_id, agent, content, metadata, score, embedding, created_at, updated_at)
                VALUES
                    (:id, :crew_id, :group_id, :session_id, :agent, :content,
                     CAST(:metadata AS jsonb), :score, CAST(:embedding AS vector), :created_at, :updated_at)
                ON CONFLICT (id) DO UPDATE SET
                    content = EXCLUDED.content,
                    metadata = EXCLUDED.metadata,
                    score = EXCLUDED.score,
                    embedding = EXCLUDED.embedding,
                    updated_at = EXCLUDED.updated_at
            """)

            await session.execute(
                sql,
                {
                    "id": record_id,
                    "crew_id": self.crew_id,
                    "group_id": self.group_id or "",
                    "session_id": self.job_id or "",
                    "agent": agent or "",
                    "content": content,
                    "metadata": json.dumps(metadata),
                    "score": score,
                    "embedding": embedding_str,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            logger.debug(
                f"[LakebasePgVector] Saved {self.memory_type} record {record_id}"
            )

    async def search(
        self,
        query_embedding: List[float],
        k: int = 5,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search for similar memory records using cosine distance.

        Automatically filters by crew_id. For short-term memory,
        also filters by session_id (job_id).
        """
        embedding_str = "[" + ",".join(str(v) for v in query_embedding) + "]"

        where_clauses = ["crew_id = :crew_id"]
        params: Dict[str, Any] = {"crew_id": self.crew_id, "k": k}

        # Short-term memory: scope to current session
        if self.memory_type == "short_term" and self.job_id:
            where_clauses.append("session_id = :session_id")
            params["session_id"] = self.job_id

        # Apply additional filters
        if filters:
            for key, value in filters.items():
                if key not in ("crew_id", "session_id"):
                    param_name = f"filter_{key}"
                    where_clauses.append(f"{key} = :{param_name}")
                    params[param_name] = value

        where_sql = " AND ".join(where_clauses)

        async with get_lakebase_session(instance_name=self.instance_name, group_id=self.group_id) as session:
            from sqlalchemy import text

            sql = text(f"""
                SELECT id, content, metadata, score, agent,
                       embedding <=> CAST(:query_embedding AS vector) AS distance
                FROM {self.table_name}
                WHERE {where_sql}
                ORDER BY distance ASC
                LIMIT :k
            """)

            params["query_embedding"] = embedding_str
            result = await session.execute(sql, params)
            rows = result.fetchall()

            results = []
            for row in rows:
                metadata_val = row[2]
                if isinstance(metadata_val, str):
                    try:
                        metadata_val = json.loads(metadata_val)
                    except (json.JSONDecodeError, TypeError):
                        metadata_val = {}

                results.append(
                    {
                        "id": row[0],
                        "content": row[1],
                        "metadata": metadata_val or {},
                        "score": row[3],
                        "agent": row[4],
                        "distance": float(row[5]) if row[5] is not None else None,
                    }
                )

            logger.debug(
                f"[LakebasePgVector] {self.memory_type} search returned {len(results)} results"
            )
            return results

    async def delete(self, record_id: str) -> None:
        """Delete a single memory record by ID."""
        async with get_lakebase_session(instance_name=self.instance_name, group_id=self.group_id) as session:
            from sqlalchemy import text

            sql = text(
                f"DELETE FROM {self.table_name} WHERE id = :id AND crew_id = :crew_id"
            )
            await session.execute(sql, {"id": record_id, "crew_id": self.crew_id})
            logger.debug(f"[LakebasePgVector] Deleted record {record_id}")

    async def clear(self) -> None:
        """Clear all memory records for this crew (and session for short-term)."""
        async with get_lakebase_session(instance_name=self.instance_name, group_id=self.group_id) as session:
            from sqlalchemy import text

            if self.memory_type == "short_term" and self.job_id:
                sql = text(
                    f"DELETE FROM {self.table_name} "
                    f"WHERE crew_id = :crew_id AND session_id = :session_id"
                )
                await session.execute(
                    sql, {"crew_id": self.crew_id, "session_id": self.job_id}
                )
            else:
                sql = text(f"DELETE FROM {self.table_name} WHERE crew_id = :crew_id")
                await session.execute(sql, {"crew_id": self.crew_id})

            logger.info(
                f"[LakebasePgVector] Cleared {self.memory_type} memory for crew {self.crew_id}"
            )

    async def get_stats(self) -> Dict[str, Any]:
        """Get statistics for this memory table."""
        async with get_lakebase_session(instance_name=self.instance_name, group_id=self.group_id) as session:
            from sqlalchemy import text

            sql = text(
                f"SELECT COUNT(*) FROM {self.table_name} WHERE crew_id = :crew_id"
            )
            result = await session.execute(sql, {"crew_id": self.crew_id})
            count = result.scalar() or 0

            return {
                "table_name": self.table_name,
                "memory_type": self.memory_type,
                "crew_id": self.crew_id,
                "record_count": count,
            }
