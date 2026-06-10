"""Lakebase pgvector implementation of CrewAI's unified ``StorageBackend`` protocol.

Introduced with the CrewAI 1.10+ cognitive memory system. Replaces the legacy
``crewai_lakebase_wrapper.py`` which implemented the old per-memory-type
interfaces.

The existing ``kasal.memory_*`` tables predate the unified schema, so cognitive
fields (``scope``, ``categories``, ``importance``, ``source``, ``private``) are
stored inside the ``metadata`` JSONB column and queried with ``->>`` accessors.
A future migration can promote them to first-class columns if query patterns
demand it.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from crewai.memory.types import MemoryRecord, ScopeInfo
from sqlalchemy import text

from src.core.logger import LoggerManager
from src.db.lakebase_session import get_lakebase_session

# SECURITY: ``table_name`` (from the LakebaseMemoryConfig.memory_table config
# field) is interpolated into raw SQL throughout this backend. Validate it as a
# strict SQL identifier so a crafted value cannot inject SQL / comment out the
# appended tenant (group_id) filters.
_SAFE_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_table_name(name: str) -> str:
    if not name or not _SAFE_TABLE_NAME.match(str(name)):
        raise ValueError(f"Invalid memory table name: {name!r}")
    return name

# CrewAI 1.10+ runs memory saves on a background thread pool; each save runs its
# coroutine in a *fresh* event loop (see ``_run_sync``). A pooled async engine
# would bind connections to that short-lived loop, and SQLAlchemy's reset/rollback
# then fails ("await_ ... rollback() ... without a greenlet") when the loop is
# gone — stalling crew teardown so the job never finalizes. NullPool gives each
# save its own connection, opened and closed inside its own loop. Matches the
# Databricks backend. Set before any Lakebase session is created in this process.
if not os.environ.get("USE_NULLPOOL"):
    os.environ["USE_NULLPOOL"] = "true"


logger = LoggerManager.get_instance().crew


class LakebaseStorageBackend:
    """Unified-memory storage backed by Lakebase (Postgres + pgvector).

    Implements CrewAI's ``StorageBackend`` protocol. All reads and writes are
    filtered by ``crew_id`` + ``group_id`` so one tenant cannot observe
    another's memory. Short-term-style scoping uses ``session_id``.
    """

    def __init__(
        self,
        *,
        table_name: str,
        crew_id: str,
        group_id: str,
        session_id: str | None = None,
        embedder: Any = None,
        embedding_dimension: int = 1024,
        instance_name: str | None = None,
        workspace_wide: bool = True,
    ) -> None:
        # SECURITY: validate before it reaches any interpolated raw SQL.
        self.table_name = _validate_table_name(table_name)
        self.crew_id = crew_id
        self.group_id = group_id
        self.session_id = session_id
        self.embedder = embedder
        self.embedding_dimension = embedding_dimension
        self.instance_name = instance_name
        # Default READ scope: True = workspace-wide (group_id), False = this
        # chat session only (session_id). Toggled per execution from the chat
        # "Workspace memory" switch. crew_id is NOT a scoping key — it only tags
        # rows for tracing. Deletes/consolidation stay crew-scoped.
        self.workspace_wide = workspace_wide

        logger.info(
            "LakebaseStorageBackend initialized (table=%s, crew_id=%s, "
            "session_id=%s, group_id=%s, workspace_wide=%s)",
            table_name,
            crew_id,
            session_id,
            group_id,
            workspace_wide,
        )

    # ------------------------------------------------------------------
    # StorageBackend protocol — synchronous methods
    # ------------------------------------------------------------------

    def save(self, records: list[MemoryRecord]) -> None:
        self._run_sync(self.asave(records))

    def search(
        self,
        query_embedding: list[float],
        scope_prefix: str | None = None,
        categories: list[str] | None = None,
        metadata_filter: dict[str, Any] | None = None,
        limit: int = 10,
        min_score: float = 0.0,
    ) -> list[tuple[MemoryRecord, float]]:
        return self._run_sync(
            self.asearch(
                query_embedding=query_embedding,
                scope_prefix=scope_prefix,
                categories=categories,
                metadata_filter=metadata_filter,
                limit=limit,
                min_score=min_score,
            )
        )

    def delete(
        self,
        scope_prefix: str | None = None,
        categories: list[str] | None = None,
        record_ids: list[str] | None = None,
        older_than: datetime | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> int:
        return self._run_sync(
            self.adelete(
                scope_prefix=scope_prefix,
                categories=categories,
                record_ids=record_ids,
                older_than=older_than,
                metadata_filter=metadata_filter,
            )
        )

    def update(self, record: MemoryRecord) -> None:
        self._run_sync(self.asave([record]))

    def get_record(self, record_id: str) -> MemoryRecord | None:
        async def _fetch() -> MemoryRecord | None:
            async with get_lakebase_session(
                instance_name=self.instance_name, group_id=self.group_id
            ) as session:
                # Workspace-wide read: fetch by id within the workspace,
                # regardless of which crew wrote it.
                sql = text(
                    f"SELECT id, content, metadata, created_at, updated_at, agent "
                    f"FROM {self.table_name} "
                    f"WHERE id = :id AND group_id = :group_id"
                )
                result = await session.execute(
                    sql,
                    {
                        "id": record_id,
                        "group_id": self.group_id,
                    },
                )
                row = result.fetchone()
                return self._row_to_record(row) if row else None
        return self._run_sync(_fetch())

    def list_records(
        self,
        scope_prefix: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[MemoryRecord]:
        async def _list() -> list[MemoryRecord]:
            where, params = self._tenant_where()
            if scope_prefix:
                where.append("metadata->>'scope' LIKE :scope_prefix")
                params["scope_prefix"] = f"{scope_prefix}%"
            async with get_lakebase_session(
                instance_name=self.instance_name, group_id=self.group_id
            ) as session:
                sql = text(
                    f"SELECT id, content, metadata, created_at, updated_at, agent "
                    f"FROM {self.table_name} "
                    f"WHERE {' AND '.join(where)} "
                    f"ORDER BY created_at DESC "
                    f"LIMIT :limit OFFSET :offset"
                )
                params["limit"] = limit
                params["offset"] = offset
                result = await session.execute(sql, params)
                return [self._row_to_record(row) for row in result.fetchall()]
        return self._run_sync(_list())

    def get_scope_info(self, scope: str) -> ScopeInfo:
        async def _info() -> ScopeInfo:
            where, params = self._tenant_where()
            where.append("metadata->>'scope' = :scope")
            params["scope"] = scope
            async with get_lakebase_session(
                instance_name=self.instance_name, group_id=self.group_id
            ) as session:
                sql = text(
                    f"SELECT metadata, created_at FROM {self.table_name} "
                    f"WHERE {' AND '.join(where)}"
                )
                result = await session.execute(sql, params)
                rows = result.fetchall()
                categories: set[str] = set()
                oldest: datetime | None = None
                newest: datetime | None = None
                for metadata_val, created_at in rows:
                    md = metadata_val if isinstance(metadata_val, dict) else _loads_or_empty(metadata_val)
                    categories.update(md.get("categories") or [])
                    if oldest is None or created_at < oldest:
                        oldest = created_at
                    if newest is None or created_at > newest:
                        newest = created_at
            children = await self._list_child_scopes(scope)
            return ScopeInfo(
                path=scope,
                record_count=len(rows),
                categories=sorted(categories),
                oldest_record=oldest,
                newest_record=newest,
                child_scopes=children,
            )
        return self._run_sync(_info())

    def list_scopes(self, parent: str = "/") -> list[str]:
        return self._run_sync(self._list_child_scopes(parent))

    def list_categories(self, scope_prefix: str | None = None) -> dict[str, int]:
        async def _categories() -> dict[str, int]:
            where, params = self._tenant_where()
            if scope_prefix:
                where.append("metadata->>'scope' LIKE :scope_prefix")
                params["scope_prefix"] = f"{scope_prefix}%"
            async with get_lakebase_session(
                instance_name=self.instance_name, group_id=self.group_id
            ) as session:
                sql = text(
                    f"SELECT metadata FROM {self.table_name} WHERE {' AND '.join(where)}"
                )
                result = await session.execute(sql, params)
                counts: dict[str, int] = {}
                for (metadata_val,) in result.fetchall():
                    md = metadata_val if isinstance(metadata_val, dict) else _loads_or_empty(metadata_val)
                    for category in md.get("categories") or []:
                        counts[category] = counts.get(category, 0) + 1
                return counts
        return self._run_sync(_categories())

    def count(self, scope_prefix: str | None = None) -> int:
        async def _count() -> int:
            where, params = self._tenant_where()
            if scope_prefix:
                where.append("metadata->>'scope' LIKE :scope_prefix")
                params["scope_prefix"] = f"{scope_prefix}%"
            async with get_lakebase_session(
                instance_name=self.instance_name, group_id=self.group_id
            ) as session:
                sql = text(f"SELECT COUNT(*) FROM {self.table_name} WHERE {' AND '.join(where)}")
                result = await session.execute(sql, params)
                return int(result.scalar() or 0)
        return self._run_sync(_count())

    def reset(self, scope_prefix: str | None = None) -> None:
        self.delete(scope_prefix=scope_prefix)

    # ------------------------------------------------------------------
    # StorageBackend protocol — async methods
    # ------------------------------------------------------------------

    async def asave(self, records: list[MemoryRecord]) -> None:
        if not records:
            return
        async with get_lakebase_session(
            instance_name=self.instance_name, group_id=self.group_id
        ) as session:
            for record in records:
                embedding = record.embedding
                if embedding is None:
                    embedding = self._embed_sync(record.content)
                embedding_str = _vector_to_pg(list(embedding))
                metadata = dict(record.metadata or {})
                metadata.update(
                    {
                        "scope": record.scope,
                        "categories": list(record.categories or []),
                        "importance": float(record.importance),
                        "source": record.source,
                        "private": bool(record.private),
                        "last_accessed": record.last_accessed.isoformat(),
                    }
                )
                sql = text(
                    f"""
                    INSERT INTO {self.table_name}
                        (id, crew_id, group_id, session_id, agent, content, metadata,
                         score, embedding, created_at, updated_at)
                    VALUES
                        (:id, :crew_id, :group_id, :session_id, :agent, :content,
                         CAST(:metadata AS jsonb), :score, CAST(:embedding AS vector),
                         :created_at, :updated_at)
                    ON CONFLICT (id) DO UPDATE SET
                        content = EXCLUDED.content,
                        metadata = EXCLUDED.metadata,
                        score = EXCLUDED.score,
                        embedding = EXCLUDED.embedding,
                        updated_at = EXCLUDED.updated_at
                    """
                )
                await session.execute(
                    sql,
                    {
                        "id": record.id or str(uuid.uuid4()),
                        "crew_id": self.crew_id,
                        "group_id": self.group_id,
                        "session_id": self.session_id or "",
                        "agent": record.source or "",
                        "content": record.content,
                        "metadata": json.dumps(metadata),
                        "score": float(record.importance),
                        "embedding": embedding_str,
                        "created_at": record.created_at,
                        "updated_at": record.last_accessed,
                    },
                )

    async def asearch(
        self,
        query_embedding: list[float],
        scope_prefix: str | None = None,
        categories: list[str] | None = None,
        metadata_filter: dict[str, Any] | None = None,
        limit: int = 10,
        min_score: float = 0.0,
    ) -> list[tuple[MemoryRecord, float]]:
        where, params = self._tenant_where()
        if scope_prefix:
            where.append("metadata->>'scope' LIKE :scope_prefix")
            params["scope_prefix"] = f"{scope_prefix}%"
        if categories:
            # Match any overlap: metadata->'categories' ?| array[...]
            where.append("metadata->'categories' ?| :categories")
            params["categories"] = list(categories)
        if metadata_filter:
            for index, (key, value) in enumerate(metadata_filter.items()):
                placeholder = f"mf_{index}"
                where.append(f"metadata->>'{key}' = :{placeholder}")
                params[placeholder] = str(value)

        params["query_embedding"] = _vector_to_pg(query_embedding)
        params["limit"] = limit

        async with get_lakebase_session(
            instance_name=self.instance_name, group_id=self.group_id
        ) as session:
            sql = text(
                f"""
                SELECT id, content, metadata, created_at, updated_at, agent,
                       1.0 - (embedding <=> CAST(:query_embedding AS vector)) AS score
                FROM {self.table_name}
                WHERE {' AND '.join(where)}
                ORDER BY embedding <=> CAST(:query_embedding AS vector) ASC
                LIMIT :limit
                """
            )
            result = await session.execute(sql, params)
            out: list[tuple[MemoryRecord, float]] = []
            for row in result.fetchall():
                score = float(row[-1] or 0.0)
                if score < min_score:
                    continue
                record = self._row_to_record(row)
                if record is None:
                    continue
                if record.private and record.source not in (self.session_id, self.crew_id):
                    continue
                out.append((record, score))
            return out

    async def adelete(
        self,
        scope_prefix: str | None = None,
        categories: list[str] | None = None,
        record_ids: list[str] | None = None,
        older_than: datetime | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> int:
        # Crew-scoped: a crew only deletes/consolidates memories it wrote, even
        # though it can READ the whole workspace (or just its chat session).
        where, params = self._crew_where()
        if record_ids:
            where.append("id = ANY(:record_ids)")
            params["record_ids"] = list(record_ids)
        if scope_prefix:
            where.append("metadata->>'scope' LIKE :scope_prefix")
            params["scope_prefix"] = f"{scope_prefix}%"
        if categories:
            where.append("metadata->'categories' ?| :categories")
            params["categories"] = list(categories)
        if older_than is not None:
            where.append("created_at < :older_than")
            params["older_than"] = older_than
        if metadata_filter:
            for index, (key, value) in enumerate(metadata_filter.items()):
                placeholder = f"mf_{index}"
                where.append(f"metadata->>'{key}' = :{placeholder}")
                params[placeholder] = str(value)

        async with get_lakebase_session(
            instance_name=self.instance_name, group_id=self.group_id
        ) as session:
            sql = text(f"DELETE FROM {self.table_name} WHERE {' AND '.join(where)}")
            result = await session.execute(sql, params)
            return int(getattr(result, "rowcount", 0) or 0)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _tenant_where(self, workspace_wide: bool | None = None) -> tuple[list[str], dict[str, Any]]:
        """WHERE fragment + params scoping a READ to this tenant.

        Uses ``self.workspace_wide`` (the per-execution default from the chat
        "Workspace memory" toggle): True = WORKSPACE-WIDE (group_id only) so any
        crew recalls ALL context in the workspace; False = this chat
        ``session_id`` only, so recall is confined to the current conversation.
        group_id remains the tenant-isolation boundary either way.

        NOTE: ``crew_id`` is deliberately NOT a scoping key. It is the
        deterministic per-crew-structure hash used for tracing/identity and
        changes every time the crew structure changes (e.g. each chat prompt),
        so scoping reads by it would wall every run off from the rest of the
        workspace. Memory partitioning is workspace-vs-session only.
        """
        if workspace_wide is None:
            workspace_wide = self.workspace_wide
        if workspace_wide:
            return ["group_id = :group_id"], {"group_id": self.group_id}
        return (
            ["session_id = :session_id", "group_id = :group_id"],
            {"session_id": self.session_id or "", "group_id": self.group_id},
        )

    def _crew_where(self) -> tuple[list[str], dict[str, Any]]:
        """WHERE fragment + params scoping a WRITE/DELETE to this crew.

        Deletes/consolidation are crew-scoped so a crew only prunes or merges
        memories IT wrote — one crew's (or session's) consolidation must not
        delete another's, even though reads can span the whole workspace.
        """
        return (
            ["crew_id = :crew_id", "group_id = :group_id"],
            {"crew_id": self.crew_id, "group_id": self.group_id},
        )

    async def _list_child_scopes(self, parent: str) -> list[str]:
        where, params = self._tenant_where()
        prefix = parent if parent.endswith("/") else f"{parent}/"
        where.append("metadata->>'scope' LIKE :prefix")
        params["prefix"] = f"{prefix}%"
        async with get_lakebase_session(
            instance_name=self.instance_name, group_id=self.group_id
        ) as session:
            sql = text(
                f"SELECT DISTINCT metadata->>'scope' AS scope "
                f"FROM {self.table_name} WHERE {' AND '.join(where)}"
            )
            result = await session.execute(sql, params)
            children: set[str] = set()
            for (scope_val,) in result.fetchall():
                if not scope_val or not scope_val.startswith(prefix):
                    continue
                remainder = scope_val[len(prefix):]
                first_segment = remainder.split("/", 1)[0]
                if first_segment:
                    children.add(f"{prefix}{first_segment}")
            return sorted(children)

    def _row_to_record(self, row: Any) -> MemoryRecord | None:
        if row is None:
            return None
        id_val, content, metadata_val, created_at, updated_at, agent, *_ = list(row) + [None] * 6
        metadata = metadata_val if isinstance(metadata_val, dict) else _loads_or_empty(metadata_val)
        scope = metadata.pop("scope", "/") or "/"
        categories = metadata.pop("categories", []) or []
        importance = float(metadata.pop("importance", 0.5) or 0.5)
        source = metadata.pop("source", agent) or None
        private = bool(metadata.pop("private", False))
        last_accessed_raw = metadata.pop("last_accessed", None)
        last_accessed = _parse_datetime(last_accessed_raw) if last_accessed_raw else (
            updated_at or created_at or datetime.utcnow()
        )
        # CrewAI's recency scoring does ``datetime.utcnow() - record.created_at``
        # (offset-naive). Postgres ``timestamptz`` columns come back offset-aware,
        # so normalise to naive UTC to avoid "can't subtract offset-naive and
        # offset-aware datetimes" in RecallFlow.search_chunks.
        return MemoryRecord(
            id=str(id_val) if id_val is not None else str(uuid.uuid4()),
            content=content or "",
            scope=scope,
            categories=list(categories),
            importance=importance,
            source=source,
            private=private,
            metadata=metadata,
            created_at=_to_naive_utc(created_at) if created_at else datetime.utcnow(),
            last_accessed=_to_naive_utc(last_accessed),
        )

    def _embed_sync(self, text_content: str) -> list[float]:
        if self.embedder is None:
            raise ValueError("No embedder configured on LakebaseStorageBackend")
        embedder = self.embedder
        if isinstance(embedder, dict):
            inner = embedder.get("config", {}).get("embedder", embedder)
            if callable(inner):
                result = inner([text_content])
            elif hasattr(inner, "embed_documents"):
                result = inner.embed_documents([text_content])
            else:
                raise TypeError(f"Unsupported embedder dict shape: {embedder!r}")
        elif callable(embedder):
            result = embedder([text_content])
        elif hasattr(embedder, "embed_documents"):
            result = embedder.embed_documents([text_content])
        else:
            raise TypeError(f"Unsupported embedder type: {type(embedder).__name__}")
        if not result:
            raise RuntimeError("Embedder returned no vectors")
        vector = result[0]
        if hasattr(vector, "tolist"):
            return list(vector.tolist())
        return list(vector)

    def _run_sync(self, coro: Any) -> Any:
        if not asyncio.iscoroutine(coro):
            return coro
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)

        def _runner(c: Any) -> Any:
            new_loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(new_loop)
                return new_loop.run_until_complete(c)
            finally:
                asyncio.set_event_loop(None)
                new_loop.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(_runner, coro).result()


# ----------------------------------------------------------------------
# Module helpers
# ----------------------------------------------------------------------


def _vector_to_pg(vector: list[float]) -> str:
    return "[" + ",".join(str(float(v)) for v in vector) + "]"


def _loads_or_empty(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def _to_naive_utc(dt: datetime) -> datetime:
    """Coerce a datetime to offset-naive UTC.

    CrewAI's recency math (``datetime.utcnow() - record.created_at``) is
    offset-naive, so every datetime we hand back on a ``MemoryRecord`` must be
    naive UTC — otherwise mixing with offset-aware values (e.g. Postgres
    ``timestamptz``) raises ``can't subtract offset-naive and offset-aware``.
    """
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _to_naive_utc(value)
    if not value:
        return datetime.utcnow()
    try:
        return _to_naive_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return datetime.utcnow()
