"""Lakebase pgvector implementation of the unified ``StorageBackend`` protocol.

The primary memory backend, and since Databricks Vector Search memory was
retired, the only remote one.

The ``kasal.memory_*`` tables predate the unified schema, so most
fields (``scope``, ``categories``, ``importance``, ``source``, ``private``) live
inside the ``metadata`` JSONB column and are read with ``->>`` accessors.

``kind`` and the validity window (``valid_from`` / ``valid_to`` /
``superseded_by``) are REAL COLUMNS. They earned promotion out of JSONB by the
rule this module's original note anticipated — query patterns demanded it. Every
single recall filters on ``valid_to`` and branches its recency decay on
``kind``, and a ``->>`` accessor in the hot scoring path cannot use an index.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from src.core.logger import LoggerManager
from src.db.lakebase_session import get_lakebase_session
from src.services.memory.bridge_loop import run_on_bridge_loop
from src.services.memory.engine import MemoryRecord, ScopeInfo
from src.services.memory.lakebase_schema import ensure_memory_columns, needs_check
from src.services.memory.pg_codec import (
    loads_or_empty,
    parse_datetime,
    to_aware_utc,
    to_aware_utc_or_none,
    to_naive_utc,
    vector_to_pg,
)

# SECURITY: ``table_name`` (from the LakebaseMemoryConfig.memory_table config
# field) is interpolated into raw SQL throughout this backend. Validate it as a
# strict SQL identifier so a crafted value cannot inject SQL / comment out the
# appended tenant (group_id) filters.
_SAFE_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_table_name(name: str) -> str:
    if not name or not _SAFE_TABLE_NAME.match(str(name)):
        raise ValueError(f"Invalid memory table name: {name!r}")
    return name


# Every row-returning query selects exactly this, in this order, because
# ``_row_to_record`` unpacks positionally. One constant so a column added to the
# search query cannot silently shift the parse of the others.
_RECORD_COLUMNS = (
    "id, content, metadata, created_at, updated_at, agent, "
    "kind, valid_from, valid_to, superseded_by"
)


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

    Implements the unified ``StorageBackend`` protocol. There is one memory pool
    per tenant, and ONE scoping rule for every operation that touches it —
    ``_tenant_where``:

    * ``group_id`` alone when ``workspace_wide`` (the default), so a run reads
      and prunes everything in the workspace.
    * ``session_id`` + ``group_id`` when the chat "Workspace memory" toggle is
      off, confining both to the current conversation.

    ``group_id`` is present either way; it is the tenant boundary, so one
    workspace can never observe or alter another's memory.

    **``crew_id`` filters nothing.** It is written onto each row as provenance —
    useful for tracing which run produced a memory — and that is all. It is a
    hash of crew STRUCTURE and changes every time that structure does, including
    with each chat prompt, so any query scoped by it walls a run off from its own
    history. The retired Databricks Vector Search backend scoped reads by it,
    which made chat memory there close to unrecallable across turns; deletes were
    scoped by it here, which made ``reset`` clear only part of a workspace.
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
        semantic_weight: float = 0.6,
        keyword_weight: float = 0.15,
        recency_weight: float = 0.15,
        importance_weight: float = 0.10,
        recency_half_life_days: float = 30.0,
        relevance_threshold: float = 0.35,
    ) -> None:
        # SECURITY: validate before it reaches any interpolated raw SQL.
        self.table_name = _validate_table_name(table_name)
        self.crew_id = crew_id
        self.group_id = group_id
        self.session_id = session_id
        self.embedder = embedder
        self.embedding_dimension = embedding_dimension
        self.instance_name = instance_name
        # Hybrid-scoring weights (see asearch): semantic similarity dominates,
        # keyword/recency/importance re-rank the candidate pool.
        self.semantic_weight = semantic_weight
        self.keyword_weight = keyword_weight
        self.recency_weight = recency_weight
        self.importance_weight = importance_weight
        self.recency_half_life_days = recency_half_life_days
        # Semantic gate applied BEFORE blending: recency/importance rank among
        # RELEVANT candidates — they must never rescue unrelated memories into
        # the context (e.g. Swiss-news records recalled for a database job).
        self.relevance_threshold = relevance_threshold
        # Default READ scope: True = workspace-wide (group_id), False = this
        # chat session only (session_id). Toggled per execution from the chat
        # "Workspace memory" switch. crew_id is NOT a scoping key anywhere —
        # it only tags rows for tracing.
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
        query_text: str | None = None,
    ) -> list[tuple[MemoryRecord, float]]:
        return self._run_sync(
            self.asearch(
                query_embedding=query_embedding,
                scope_prefix=scope_prefix,
                categories=categories,
                metadata_filter=metadata_filter,
                limit=limit,
                min_score=min_score,
                query_text=query_text,
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
            async with self._session() as session:
                # Workspace-wide read: fetch by id within the workspace,
                # regardless of which crew wrote it.
                sql = text(
                    f"SELECT {_RECORD_COLUMNS} "
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
            async with self._session() as session:
                sql = text(
                    f"SELECT {_RECORD_COLUMNS} "
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
            async with self._session() as session:
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
                    md = (
                        metadata_val
                        if isinstance(metadata_val, dict)
                        else loads_or_empty(metadata_val)
                    )
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
            async with self._session() as session:
                sql = text(
                    f"SELECT metadata FROM {self.table_name} WHERE {' AND '.join(where)}"
                )
                result = await session.execute(sql, params)
                counts: dict[str, int] = {}
                for (metadata_val,) in result.fetchall():
                    md = (
                        metadata_val
                        if isinstance(metadata_val, dict)
                        else loads_or_empty(metadata_val)
                    )
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
            async with self._session() as session:
                sql = text(
                    f"SELECT COUNT(*) FROM {self.table_name} WHERE {' AND '.join(where)}"
                )
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
        async with self._session() as session:
            for record in records:
                embedding = record.embedding
                if embedding is None:
                    embedding = self._embed_sync(record.content)
                embedding_str = vector_to_pg(list(embedding))
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
                sql = text(f"""
                    INSERT INTO {self.table_name}
                        (id, crew_id, group_id, session_id, agent, content, metadata,
                         score, embedding, kind, valid_from, valid_to, superseded_by,
                         created_at, updated_at)
                    VALUES
                        (:id, :crew_id, :group_id, :session_id, :agent, :content,
                         CAST(:metadata AS jsonb), :score, CAST(:embedding AS vector),
                         :kind, :valid_from, :valid_to, :superseded_by,
                         :created_at, :updated_at)
                    ON CONFLICT (id) DO UPDATE SET
                        content = EXCLUDED.content,
                        metadata = EXCLUDED.metadata,
                        score = EXCLUDED.score,
                        embedding = EXCLUDED.embedding,
                        kind = EXCLUDED.kind,
                        valid_from = EXCLUDED.valid_from,
                        -- Retiring a fact IS an update through this path
                        -- (``update`` upserts the whole record), so these must
                        -- be carried across or supersession would never persist.
                        valid_to = EXCLUDED.valid_to,
                        superseded_by = EXCLUDED.superseded_by,
                        updated_at = EXCLUDED.updated_at
                    """)
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
                        "kind": record.kind,
                        "valid_from": to_aware_utc_or_none(record.valid_from),
                        "valid_to": to_aware_utc_or_none(record.valid_to),
                        "superseded_by": record.superseded_by,
                        # MUST be offset-AWARE UTC. These bind to TIMESTAMPTZ
                        # columns, and asyncpg's encoder does obj.astimezone(utc)
                        # — which treats a NAIVE datetime as MACHINE-LOCAL time
                        # and silently shifts it by the host's UTC offset. CrewAI
                        # hands us naive datetime.utcnow() values, so without this
                        # coercion every created_at lands hours off true UTC and
                        # the Memory Browser's per-run time window (built
                        # from the run's correctly-stored completed_at) rejects all
                        # of a run's records.
                        "created_at": to_aware_utc(record.created_at),
                        "updated_at": to_aware_utc(record.last_accessed),
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
        query_text: str | None = None,
    ) -> list[tuple[MemoryRecord, float]]:
        """Hybrid-scored search: semantic + keyword + recency + importance.

        Two stages so the pgvector HNSW index stays in play: the inner query
        pulls the top candidates by pure vector distance (index-accelerated),
        the outer query re-ranks that small set with the blended score
        (mem0-style fused scoring, native to Postgres — ts_rank_cd for
        keywords, exponential recency decay, stored importance).

        Two policies the record's ``kind`` selects between:

        * **Only currently-valid records are returned.** A fact that has been
          superseded (``valid_to`` set) stays in the table — "what did we
          believe on date X" remains answerable — but never re-enters a prompt.
        * **Recency decay applies to EPISODIC records only.** A 30-day half-life
          is about right for "what happened in run 47" and actively wrong for
          "the user prefers Databricks SQL": a stable preference learned two
          months ago is not less true than one learned yesterday. Semantic and
          procedural records take the full recency term instead of a decayed
          one, so age never pushes a current fact out of the recall budget.
        """
        where, params = self._tenant_where()
        # Superseded records are history, not context.
        where.append("(valid_to IS NULL OR valid_to > NOW())")
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

        params["query_embedding"] = vector_to_pg(query_embedding)
        params["limit"] = limit
        # Re-rank pool: enough candidates that keyword/recency can promote a
        # non-top-cosine hit, small enough that the outer pass is negligible.
        params["candidate_limit"] = max(limit * 5, 25)
        params["relevance_threshold"] = float(self.relevance_threshold)
        params["half_life_days"] = float(self.recency_half_life_days)
        params["w_semantic"] = float(self.semantic_weight)
        params["w_recency"] = float(self.recency_weight)
        params["w_importance"] = float(self.importance_weight)

        keyword_term = "0.0"
        text_query = " ".join((query_text or "").split())
        if text_query and self.keyword_weight > 0:
            params["query_text"] = text_query[:500]
            params["w_keyword"] = float(self.keyword_weight)
            keyword_term = (
                ":w_keyword * LEAST(ts_rank_cd(to_tsvector('simple', c.content), "
                "plainto_tsquery('simple', :query_text)), 1.0)"
            )

        async with self._session() as session:
            sql = text(f"""
                SELECT c.id, c.content, c.metadata, c.created_at, c.updated_at,
                       c.agent, c.kind, c.valid_from, c.valid_to, c.superseded_by,
                       (
                           :w_semantic * c.semantic
                           + {keyword_term}
                           + :w_recency * CASE
                               WHEN c.kind = 'episodic' THEN EXP(
                                   -0.6931471805599453
                                   * GREATEST(
                                       EXTRACT(EPOCH FROM (NOW() - c.created_at)), 0
                                     )
                                   / (:half_life_days * 86400.0)
                               )
                               ELSE 1.0
                           END
                           + :w_importance
                             * COALESCE((c.metadata->>'importance')::float, 0.5)
                       ) AS score
                FROM (
                    SELECT id, content, metadata, created_at, updated_at, agent,
                           kind, valid_from, valid_to, superseded_by,
                           1.0 - (embedding <=> CAST(:query_embedding AS vector))
                               AS semantic
                    FROM {self.table_name}
                    WHERE {' AND '.join(where)}
                    ORDER BY embedding <=> CAST(:query_embedding AS vector) ASC
                    LIMIT :candidate_limit
                ) AS c
                WHERE c.semantic >= :relevance_threshold
                ORDER BY score DESC
                LIMIT :limit
                """)
            result = await session.execute(sql, params)
            out: list[tuple[MemoryRecord, float]] = []
            for row in result.fetchall():
                score = float(row[-1] or 0.0)
                if score < min_score:
                    continue
                record = self._row_to_record(row)
                if record is None:
                    continue
                if record.private and record.source not in (
                    self.session_id,
                    self.crew_id,
                ):
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
        # Deletes are scoped exactly like reads: the workspace, or one chat
        # session. ``crew_id`` is NOT a filter here or anywhere else.
        #
        # It used to scope filter-shaped deletes, on the reasoning that one
        # crew's pruning should not sweep away another's. That made crew a
        # boundary for deletes and for nothing else, in a store that is one pool
        # per workspace — and since ``crew_id`` is a hash of crew structure that
        # changes with every chat prompt, it meant a chat turn's maintenance
        # could only ever see that ONE turn's write, and ``reset`` cleared only
        # what the current crew happened to have written.
        where, params = self._tenant_where()
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

        async with self._session() as session:
            sql = text(f"DELETE FROM {self.table_name} WHERE {' AND '.join(where)}")
            result = await session.execute(sql, params)
            return int(getattr(result, "rowcount", 0) or 0)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def _session(self) -> AsyncIterator[Any]:
        """A Lakebase session with this table's schema guaranteed current.

        EVERY database operation in this backend goes through here, which is the
        point: the table's DDL is only ever run by an admin-only endpoint, so a
        column added to it reaches new workspaces and no others. A workspace
        whose table predates the column would fail every insert and every select
        — silently, because memory swallows its own errors — until somebody
        happened to re-initialize the table by hand.

        The repair runs in a SEPARATE session, before the caller's. Sessions here
        roll back on exception and Postgres DDL is transactional, so sharing one
        would let a failure in the caller's own SQL quietly undo the repair while
        the cache recorded it as done. Costs one extra connection on the first
        memory operation in a process, and a set lookup for every one after.
        """
        if needs_check(self.table_name):
            async with get_lakebase_session(
                instance_name=self.instance_name, group_id=self.group_id
            ) as schema_session:
                await ensure_memory_columns(schema_session, self.table_name)
        async with get_lakebase_session(
            instance_name=self.instance_name, group_id=self.group_id
        ) as session:
            yield session

    def _tenant_where(
        self, workspace_wide: bool | None = None
    ) -> tuple[list[str], dict[str, Any]]:
        """WHERE fragment + params scoping an operation to this tenant.

        Used by reads AND deletes — ``crew_id`` filters nothing anywhere.

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

    async def _list_child_scopes(self, parent: str) -> list[str]:
        where, params = self._tenant_where()
        prefix = parent if parent.endswith("/") else f"{parent}/"
        where.append("metadata->>'scope' LIKE :prefix")
        params["prefix"] = f"{prefix}%"
        async with self._session() as session:
            sql = text(
                f"SELECT DISTINCT metadata->>'scope' AS scope "
                f"FROM {self.table_name} WHERE {' AND '.join(where)}"
            )
            result = await session.execute(sql, params)
            children: set[str] = set()
            for (scope_val,) in result.fetchall():
                if not scope_val or not scope_val.startswith(prefix):
                    continue
                remainder = scope_val[len(prefix) :]
                first_segment = remainder.split("/", 1)[0]
                if first_segment:
                    children.add(f"{prefix}{first_segment}")
            return sorted(children)

    def _row_to_record(self, row: Any) -> MemoryRecord | None:
        if row is None:
            return None
        (
            id_val,
            content,
            metadata_val,
            created_at,
            updated_at,
            agent,
            kind,
            valid_from,
            valid_to,
            superseded_by,
            *_,
        ) = (
            list(row) + [None] * 10
        )
        metadata = (
            metadata_val
            if isinstance(metadata_val, dict)
            else loads_or_empty(metadata_val)
        )
        scope = metadata.pop("scope", "/") or "/"
        categories = metadata.pop("categories", []) or []
        importance = float(metadata.pop("importance", 0.5) or 0.5)
        source = metadata.pop("source", agent) or None
        private = bool(metadata.pop("private", False))
        last_accessed_raw = metadata.pop("last_accessed", None)
        last_accessed = (
            parse_datetime(last_accessed_raw)
            if last_accessed_raw
            else (updated_at or created_at or datetime.utcnow())
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
            created_at=to_naive_utc(created_at) if created_at else datetime.utcnow(),
            last_accessed=to_naive_utc(last_accessed),
            # ``kind`` validates unknown/NULL to episodic — which is the right
            # reading of any row written before the column existed.
            kind=kind,
            valid_from=to_naive_utc(valid_from) if valid_from else None,
            valid_to=to_naive_utc(valid_to) if valid_to else None,
            superseded_by=superseded_by,
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
        """Run a coroutine on the shared long-lived bridge loop (PERF-013).

        A fresh loop per call made _is_engine_loop_stale() trip on EVERY
        memory operation, forcing engine recreation + ~3 Databricks
        control-plane calls (credential, instance lookup, username) before
        each <10ms pgvector query. A stable loop keeps the engine cached;
        token freshness is handled lazily by get_session.
        """
        return run_on_bridge_loop(coro)
