"""Lakebase graph memory — entities and facts in Postgres, beside the record
table.

The production store. Two tables named after the configured memory table
(``<table>_entities`` / ``<table>_facts``): pgvector with an HNSW index for the
embeddings and a stored ``tsvector`` with a GIN index for keyword search. No
graph database — Lakebase stays the only
production data store.

``group_id`` is on every statement; it is the tenant boundary, as in
``LakebaseStorageBackend``. This store never opens a session itself: it is
handed the record backend's opener (``open_session``), so memory has one place
that reaches Lakebase. The tables are created on first use, in their own
session, because nothing else runs this DDL for an existing workspace (see
``lakebase_schema`` for why that matters).

Keyword ranking is Postgres full-text (``ts_rank_cd``), not true BM25 — close
enough to lift an exact name or identifier, which is what it is here for.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Callable, Coroutine, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import TypeVar

from sqlalchemy import text as sql
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logger import LoggerManager
from src.services.memory.engine.graph_types import (
    DEFAULT_NOTE_RETENTION_DAYS,
    DEFAULT_STRICT_MIN_SIMILARITY,
    FACT_EPISODIC,
    FACT_PROCEDURAL,
    MAX_STANDING,
    Entity,
    ExtractedFact,
    Fact,
    drop_weak,
    entity_key,
    fact_kind,
    keyword_tokens,
    plan_write,
    qualifiers_of,
    rank_hits,
)
from src.services.memory.storage.adapter import embed_text, embed_texts
from src.services.memory.storage.bridge_loop import get_bridge_loop
from src.services.memory.storage.pg_codec import (
    loads_or_empty,
    to_naive_utc,
    vector_to_pg,
)

logger = LoggerManager.get_instance().crew

_T = TypeVar("_T")

_SAFE_TABLE_NAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"


def validate_table_name(name: str) -> str:
    """A strict SQL identifier: the table name is interpolated into raw SQL, so
    a crafted value must not be able to comment out the tenant filter."""
    if not name or name[0].isdigit() or any(ch not in _SAFE_TABLE_NAME for ch in name):
        raise ValueError(f"Invalid memory table name: {name!r}")
    return name


_FACT_COLUMNS = (
    "f.id, f.subject_id, f.relation, f.object_id, f.object, f.statement, "
    "f.source, f.metadata, f.created_at, f.valid_to, f.superseded_by, e.name, "
    "f.kind"
)

# Table pairs whose DDL has run in this process (see ``_session``).
_ready: set[str] = set()


class _LostRace(Exception):
    """Another writer stored the same fact first; the transaction is undone."""


def reset_graph_schema_cache() -> None:
    """Forget which tables have been created. For tests and process reuse."""
    _ready.clear()


def graph_ddl(base_table: str, embedding_dimension: int) -> list[str]:
    """Idempotent DDL for the two graph tables of ``base_table``."""
    entities, facts = f"{base_table}_entities", f"{base_table}_facts"
    return [
        f"""CREATE TABLE IF NOT EXISTS {entities} (
            id TEXT PRIMARY KEY,
            group_id TEXT NOT NULL,
            name TEXT NOT NULL,
            name_key TEXT NOT NULL,
            type TEXT NOT NULL DEFAULT 'concept',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (group_id, name_key)
        )""",
        f"""CREATE TABLE IF NOT EXISTS {facts} (
            id TEXT PRIMARY KEY,
            group_id TEXT NOT NULL,
            session_id TEXT,
            subject_id TEXT NOT NULL,
            relation TEXT NOT NULL,
            object_id TEXT,
            object TEXT NOT NULL,
            object_key TEXT NOT NULL,
            statement TEXT NOT NULL,
            embedding vector({int(embedding_dimension)}),
            tsv tsvector GENERATED ALWAYS AS
                (to_tsvector('simple', statement)) STORED,
            source TEXT,
            metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            valid_to TIMESTAMPTZ,
            superseded_by TEXT,
            kind TEXT NOT NULL DEFAULT 'semantic'
        )""",
        # A table created before ``kind`` existed keeps its old shape otherwise.
        f"ALTER TABLE {facts} ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL "
        f"DEFAULT 'semantic'",
        f"CREATE INDEX IF NOT EXISTS idx_{facts}_embedding ON {facts} "
        f"USING hnsw (embedding vector_cosine_ops)",
        f"CREATE INDEX IF NOT EXISTS idx_{facts}_tsv ON {facts} USING gin (tsv)",
        f"CREATE INDEX IF NOT EXISTS idx_{facts}_subject ON {facts} "
        f"(group_id, subject_id, relation)",
        f"CREATE INDEX IF NOT EXISTS idx_{facts}_object ON {facts} "
        f"(group_id, object_id)",
        # One CURRENT copy of a fact, however many writers race.
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{facts}_current ON {facts} "
        f"(group_id, subject_id, relation, object_key) WHERE valid_to IS NULL",
    ]


class LakebaseGraphStore:
    """Postgres-backed entity/fact store for one teamspace."""

    def __init__(
        self,
        *,
        table_name: str,
        group_id: str,
        open_session: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        session_id: str | None = None,
        embedder: object = None,
        embedding_dimension: int = 1024,
        workspace_wide: bool = True,
        note_retention_days: float = DEFAULT_NOTE_RETENTION_DAYS,
    ) -> None:
        # SECURITY: validated before it reaches any interpolated SQL.
        self.table_name = validate_table_name(table_name)
        self.entities = f"{self.table_name}_entities"
        self.facts = f"{self.table_name}_facts"
        self.group_id = group_id
        self.session_id = session_id
        self.embedder = embedder
        self.embedding_dimension = embedding_dimension
        self.workspace_wide = workspace_wide
        self.note_retention_days = float(note_retention_days)
        self.strict_min_similarity = DEFAULT_STRICT_MIN_SIMILARITY
        self._open_session = open_session

    def embed(self, text: str) -> list[float]:
        return embed_text(self.embedder, text)

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        return embed_texts(self.embedder, texts)

    # ------------------------------------------------------------------
    # sync surface (the memory layer is synchronous; see bridge_loop)
    # ------------------------------------------------------------------

    def add_fact(
        self,
        fact: ExtractedFact,
        embedding: list[float] | None,
        *,
        source: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> Fact | None:
        return self._run_sync(self.aadd_fact(fact, embedding, source, metadata))

    def search(
        self,
        query_text: str,
        query_embedding: list[float] | None,
        limit: int = 10,
        min_similarity: float = 0.0,
    ) -> list[tuple[Fact, float | None]]:
        return self._run_sync(
            self.asearch(query_text, query_embedding, limit, min_similarity)
        )

    def list_facts(self, limit: int = 100, include_retired: bool = False) -> list[Fact]:
        return self._run_sync(self.alist_facts(limit, include_retired))

    def standing_facts(self, limit: int = MAX_STANDING) -> list[Fact]:
        return self._run_sync(self.astanding_facts(limit))

    def list_entities(self, limit: int = 500) -> list[Entity]:
        return self._run_sync(self.alist_entities(limit))

    def retire_fact(self, fact_id: str) -> bool:
        return self._run_sync(self.aretire_fact(fact_id))

    def count(self) -> int:
        return self._run_sync(self.acount())

    def reset(self) -> None:
        self._run_sync(self.areset())

    def close(self) -> None:
        return None

    # ------------------------------------------------------------------
    # write
    # ------------------------------------------------------------------

    async def aadd_fact(
        self,
        fact: ExtractedFact,
        embedding: list[float] | None,
        source: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> Fact | None:
        """Store ``fact``. ``None`` when the store already holds it.

        One transaction: resolve the entities, decide with ``plan_write``
        (repeat, correction, or a value a single-valued relation replaces),
        close what the new fact replaces — pointing at it — and insert. The
        close comes first: a correction keeps the old fact's key, and only one
        current copy of a key may exist. Losing an insert race rolls the close
        back with it.
        """
        object_key = entity_key(fact.object)
        fact_id = str(uuid.uuid4())
        try:
            return await self._add_fact(
                fact, embedding, source, metadata, object_key, fact_id
            )
        except _LostRace:
            return None

    async def _add_fact(
        self,
        fact: ExtractedFact,
        embedding: list[float] | None,
        source: str | None,
        metadata: dict[str, object] | None,
        object_key: str,
        fact_id: str,
    ) -> Fact | None:
        async with self._session() as session:
            subject_id = await self._entity(session, fact.subject, fact.subject_type)
            object_id = (
                await self._entity(session, fact.object, fact.object_type)
                if fact.object_type
                else None
            )
            current = (
                await session.execute(
                    sql(
                        f"SELECT id, object_key, metadata FROM {self.facts} "
                        "WHERE group_id = :group_id AND subject_id = :subject_id "
                        "AND relation = :relation AND valid_to IS NULL"
                    ),
                    {
                        "group_id": self.group_id,
                        "subject_id": subject_id,
                        "relation": fact.relation,
                    },
                )
            ).fetchall()
            write, retire = plan_write(
                [(str(r[0]), str(r[1]), r[2]) for r in current],
                object_key,
                qualifiers_of(metadata),
                fact.single,
            )
            if not write:
                return None
            if retire:
                await session.execute(
                    sql(
                        f"UPDATE {self.facts} SET valid_to = NOW(), "
                        "superseded_by = :winner "
                        "WHERE group_id = :group_id AND id = ANY(:ids)"
                    ),
                    {"winner": fact_id, "group_id": self.group_id, "ids": retire},
                )
            inserted = await session.execute(
                sql(
                    f"INSERT INTO {self.facts} (id, group_id, session_id, "
                    "subject_id, relation, object_id, object, object_key, "
                    "statement, embedding, source, metadata, kind) VALUES (:id, "
                    ":group_id, :session_id, :subject_id, :relation, :object_id, "
                    ":object, :object_key, :statement, "
                    "CAST(:embedding AS vector), :source, CAST(:metadata AS jsonb), "
                    ":kind) "
                    "ON CONFLICT DO NOTHING"
                ),
                {
                    "id": fact_id,
                    "group_id": self.group_id,
                    "session_id": self.session_id or "",
                    "subject_id": subject_id,
                    "relation": fact.relation,
                    "object_id": object_id,
                    "object": fact.object,
                    "object_key": object_key,
                    "statement": fact.statement,
                    "embedding": vector_to_pg(embedding) if embedding else None,
                    "source": source,
                    "metadata": json.dumps(dict(metadata or {})),
                    "kind": fact_kind(fact.kind),
                },
            )
            if not int(getattr(inserted, "rowcount", 0) or 0):
                # Another writer stored it first. Raising rolls the session
                # back, so the close above is undone with the insert.
                raise _LostRace
        return Fact(
            id=fact_id,
            subject_id=subject_id,
            subject=fact.subject,
            relation=fact.relation,
            object_id=object_id,
            object=fact.object,
            statement=fact.statement,
            source=source,
            metadata=dict(metadata or {}),
            kind=fact_kind(fact.kind),
        )

    async def _entity(self, session: AsyncSession, name: str, kind: str | None) -> str:
        """Id of the entity called ``name``, created on first mention."""
        params = {
            "id": str(uuid.uuid4()),
            "group_id": self.group_id,
            "name": name,
            "name_key": entity_key(name),
            "type": kind or "concept",
        }
        await session.execute(
            sql(
                f"INSERT INTO {self.entities} (id, group_id, name, name_key, type) "
                "VALUES (:id, :group_id, :name, :name_key, :type) "
                "ON CONFLICT (group_id, name_key) DO NOTHING"
            ),
            params,
        )
        row = (
            await session.execute(
                sql(
                    f"SELECT id FROM {self.entities} "
                    "WHERE group_id = :group_id AND name_key = :name_key"
                ),
                params,
            )
        ).fetchone()
        if row is None:
            raise RuntimeError(f"entity {name!r} could not be resolved")
        return str(row[0])

    # ------------------------------------------------------------------
    # read
    # ------------------------------------------------------------------

    async def asearch(
        self,
        query_text: str,
        query_embedding: list[float] | None,
        limit: int = 10,
        min_similarity: float = 0.0,
    ) -> list[tuple[Fact, float | None]]:
        """Current facts that match a query, ranked by fusing the vector and
        keyword rankings. What did not match is not returned: following a
        matched fact's entity to its other facts was tried, and every such
        "related" fact in real runs was noise. ``(fact, similarity)``."""
        if limit <= 0 or not query_embedding:
            return []
        where, params = self._read_where()
        pool = max(limit * 4, 20)
        async with self._session() as session:
            vector_rows = (
                await session.execute(
                    sql(
                        "SELECT f.id, 1.0 - (f.embedding <=> "
                        "CAST(:query_embedding AS vector)) AS similarity, f.kind "
                        f"FROM {self.facts} f WHERE {where} "
                        "AND f.embedding IS NOT NULL "
                        "ORDER BY f.embedding <=> CAST(:query_embedding AS vector) "
                        "LIMIT :pool"
                    ),
                    {
                        **params,
                        "query_embedding": vector_to_pg(query_embedding),
                        "pool": pool,
                    },
                )
            ).fetchall()
            keyword_ids = await self._keyword_ids(session, query_text, pool)
            seeds = rank_hits(
                drop_weak(
                    [
                        (str(row[0]), float(row[1] or 0.0), fact_kind(row[2]))
                        for row in vector_rows
                    ],
                    self.strict_min_similarity,
                ),
                keyword_ids,
                limit,
                min_similarity,
            )
            if not seeds:
                return []
            seed_ids = [fid for fid, _ in seeds]
            fetched = await self._select_facts(
                session, "f.id = ANY(:ids)", {"ids": seed_ids}, ""
            )
            by_id = {fact.id: fact for fact in fetched}
            out: list[tuple[Fact, float | None]] = [
                (by_id[fid], sim) for fid, sim in seeds if fid in by_id
            ]
            return out

    async def _keyword_ids(
        self, session: AsyncSession, query_text: str, pool: int
    ) -> list[str]:
        tokens = keyword_tokens(query_text)
        if not tokens:
            return []
        where, params = self._read_where()
        rows = (
            await session.execute(
                sql(
                    f"SELECT f.id FROM {self.facts} f WHERE {where} "
                    "AND f.tsv @@ to_tsquery('simple', :tsquery) "
                    "ORDER BY ts_rank_cd(f.tsv, to_tsquery('simple', :tsquery)) DESC "
                    "LIMIT :pool"
                ),
                # OR-ed: a long query AND-ed term by term matches nothing.
                {**params, "tsquery": " | ".join(tokens), "pool": pool},
            )
        ).fetchall()
        return [str(row[0]) for row in rows]

    def _read_where(self) -> tuple[str, dict[str, object]]:
        """Tenant scope for reads: the workspace, or one chat session when the
        "Workspace memory" switch is off. Current facts, and run notes still
        inside their retention period."""
        where = (
            "f.group_id = :group_id AND f.valid_to IS NULL "
            "AND NOT (f.kind = :episodic AND f.created_at < :note_cutoff)"
        )
        params: dict[str, object] = {
            "group_id": self.group_id,
            "episodic": FACT_EPISODIC,
            "note_cutoff": datetime.now(timezone.utc)
            - timedelta(days=self.note_retention_days),
        }
        if not self.workspace_wide:
            where += " AND f.session_id = :session_id"
            params["session_id"] = self.session_id or ""
        return where, params

    async def _select_facts(
        self, session: AsyncSession, where: str, params: dict[str, object], tail: str
    ) -> list[Fact]:
        rows = (
            await session.execute(
                sql(
                    f"SELECT {_FACT_COLUMNS} FROM {self.facts} f "
                    f"JOIN {self.entities} e ON e.id = f.subject_id "
                    f"WHERE f.group_id = :tenant AND {where} {tail}"
                ),
                {**params, "tenant": self.group_id},
            )
        ).fetchall()
        return [self._row_to_fact(row) for row in rows]

    @staticmethod
    def _row_to_fact(row: Sequence[object]) -> Fact:
        created_at, valid_to = row[8], row[9]
        return Fact(
            id=str(row[0]),
            subject_id=str(row[1]),
            relation=str(row[2]),
            object_id=str(row[3]) if row[3] else None,
            object=str(row[4]),
            statement=str(row[5]),
            source=str(row[6]) if row[6] else None,
            metadata=loads_or_empty(row[7]),
            created_at=(
                to_naive_utc(created_at) if isinstance(created_at, datetime) else None
            ),
            valid_to=to_naive_utc(valid_to) if isinstance(valid_to, datetime) else None,
            superseded_by=str(row[10]) if row[10] else None,
            subject=str(row[11] or ""),
            kind=fact_kind(row[12]),
        )

    # ------------------------------------------------------------------
    # housekeeping
    # ------------------------------------------------------------------

    async def alist_facts(
        self, limit: int = 100, include_retired: bool = False
    ) -> list[Fact]:
        """Newest first. Retired facts on request — they are history."""
        where = "TRUE" if include_retired else "f.valid_to IS NULL"
        async with self._session() as session:
            return await self._select_facts(
                session, where, {"n": int(limit)}, "ORDER BY f.created_at DESC LIMIT :n"
            )

    async def astanding_facts(self, limit: int = MAX_STANDING) -> list[Fact]:
        """The user's standing preferences and instructions, newest first."""
        where, params = self._read_where()
        async with self._session() as session:
            return await self._select_facts(
                session,
                f"{where} AND f.kind = :procedural",
                {**params, "procedural": FACT_PROCEDURAL, "n": int(limit)},
                "ORDER BY f.created_at DESC LIMIT :n",
            )

    async def alist_entities(self, limit: int = 500) -> list[Entity]:
        async with self._session() as session:
            rows = (
                await session.execute(
                    sql(
                        f"SELECT id, name, type FROM {self.entities} "
                        "WHERE group_id = :group_id ORDER BY name LIMIT :n"
                    ),
                    {"group_id": self.group_id, "n": int(limit)},
                )
            ).fetchall()
        return [Entity(id=str(r[0]), name=str(r[1]), type=str(r[2])) for r in rows]

    async def aretire_fact(self, fact_id: str) -> bool:
        """Stop recalling one fact, on request. It is closed, not deleted."""
        async with self._session() as session:
            result = await session.execute(
                sql(
                    f"UPDATE {self.facts} SET valid_to = NOW() "
                    "WHERE id = :id AND group_id = :group_id AND valid_to IS NULL"
                ),
                {"id": fact_id, "group_id": self.group_id},
            )
            return int(getattr(result, "rowcount", 0) or 0) > 0

    async def acount(self) -> int:
        async with self._session() as session:
            result = await session.execute(
                sql(
                    f"SELECT COUNT(*) FROM {self.facts} "
                    "WHERE group_id = :group_id AND valid_to IS NULL"
                ),
                {"group_id": self.group_id},
            )
            return int(result.scalar() or 0)

    async def areset(self) -> None:
        async with self._session() as session:
            for table in (self.facts, self.entities):
                await session.execute(
                    sql(f"DELETE FROM {table} WHERE group_id = :group_id"),
                    {"group_id": self.group_id},
                )

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def _session(self) -> AsyncIterator[AsyncSession]:
        """A Lakebase session with the graph tables guaranteed to exist.

        The DDL runs once per process, in a SEPARATE session: sessions here
        roll back on exception and Postgres DDL is transactional, so sharing
        one would let a failure in the caller's SQL undo the creation while the
        cache recorded it as done.
        """
        if self.facts not in _ready:
            await self._ensure_tables()
        async with self._open_session() as session:
            yield session

    async def _ensure_tables(self) -> None:
        try:
            async with self._open_session() as session:
                for statement in graph_ddl(self.table_name, self.embedding_dimension):
                    await session.execute(sql(statement))
            _ready.add(self.facts)
        except Exception as exc:  # noqa: BLE001 — the caller's SQL reports the truth
            # Not cached: a transient failure must be retried, and a role that
            # cannot CREATE needs the tables made by an admin, after which the
            # next call succeeds.
            logger.warning(
                "Could not create graph memory tables for %s (%s)",
                self.table_name,
                exc,
            )

    def _run_sync(self, coro: Coroutine[object, object, _T]) -> _T:
        """Block on ``coro`` on the shared memory bridge loop (see bridge_loop)."""
        return asyncio.run_coroutine_threadsafe(coro, get_bridge_loop()).result()


__all__ = ["LakebaseGraphStore", "graph_ddl", "reset_graph_schema_cache"]
