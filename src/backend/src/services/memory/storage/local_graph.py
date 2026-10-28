"""Local graph memory — entities and facts in the teamspace's SQLite file.

The development counterpart of ``lakebase_graph``: the same two tables, in the
same ``memory.db`` the record store uses. Embeddings are float32 blobs searched
with a numpy cosine; keyword search is FTS5 with its built-in BM25. No new
dependency and no graph database.

Scoped by the writing memory's root scope (``/<group_id>``), the tenant
boundary, exactly as the record store is.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

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

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS graph_entities (
    id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    name TEXT NOT NULL,
    name_key TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'concept',
    created_at TEXT NOT NULL,
    UNIQUE (scope, name_key)
);
CREATE TABLE IF NOT EXISTS graph_facts (
    id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    session_id TEXT,
    subject_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    object_id TEXT,
    object TEXT NOT NULL,
    object_key TEXT NOT NULL,
    statement TEXT NOT NULL,
    embedding BLOB,
    source TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    valid_to TEXT,
    superseded_by TEXT,
    kind TEXT NOT NULL DEFAULT 'semantic'
);
CREATE INDEX IF NOT EXISTS idx_graph_facts_subject
    ON graph_facts (scope, subject_id, relation);
CREATE INDEX IF NOT EXISTS idx_graph_facts_object ON graph_facts (scope, object_id);
-- One CURRENT copy of a fact, whichever process writes it: the server (chat)
-- and a crew subprocess share this file.
CREATE UNIQUE INDEX IF NOT EXISTS idx_graph_facts_current
    ON graph_facts (scope, subject_id, relation, object_key)
    WHERE valid_to IS NULL;
CREATE VIRTUAL TABLE IF NOT EXISTS graph_facts_fts
    USING fts5(statement, fact_id UNINDEXED);
"""

_FACT_COLUMNS = (
    "f.id, f.subject_id, f.relation, f.object_id, f.object, f.statement, "
    "f.source, f.metadata, f.created_at, f.valid_to, f.superseded_by, e.name, "
    "f.kind"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(value: object) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


class LocalGraphStore:
    """SQLite-backed entity/fact store for one teamspace."""

    def __init__(
        self,
        db_path: str | Path,
        embedder: object = None,
        scope: str = "/",
        session_id: str | None = None,
        workspace_wide: bool = True,
        note_retention_days: float = DEFAULT_NOTE_RETENTION_DAYS,
    ):
        self.db_path = Path(db_path)
        self.embedder = embedder
        self.scope = scope or "/"
        self.session_id = session_id
        self.workspace_wide = workspace_wide
        self.note_retention_days = float(note_retention_days)
        self.strict_min_similarity = DEFAULT_STRICT_MIN_SIMILARITY
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(self.db_path), check_same_thread=False, timeout=10.0
        )
        self._conn.executescript(_SCHEMA)
        self._migrate_columns()
        self._conn.commit()

    def _migrate_columns(self) -> None:
        """Add ``kind`` to a store created before it existed (see
        ``LocalStorageBackend._migrate_columns`` for why this cannot be DDL)."""
        existing = {
            row[1] for row in self._conn.execute("PRAGMA table_info(graph_facts)")
        }
        if "kind" not in existing:
            self._conn.execute(
                "ALTER TABLE graph_facts ADD COLUMN kind TEXT NOT NULL DEFAULT 'semantic'"
            )

    def embed(self, text: str) -> list[float]:
        return embed_text(self.embedder, text)

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        return embed_texts(self.embedder, texts)

    # ------------------------------------------------------------------
    # write
    # ------------------------------------------------------------------

    def add_fact(
        self,
        fact: ExtractedFact,
        embedding: list[float] | None,
        *,
        source: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> Fact | None:
        """Store ``fact``. ``None`` when the store already holds it.

        One transaction: resolve the entities, decide with ``plan_write``
        (repeat, correction, or a value a single-valued relation replaces),
        close what the new fact replaces — pointing at it — and insert. The
        close comes first: a correction keeps the old fact's key, and only one
        current copy of a key may exist.
        """
        object_key = entity_key(fact.object)
        qualifiers = qualifiers_of(metadata)
        with self._lock:
            subject_id = self._entity(fact.subject, fact.subject_type)
            object_id = (
                self._entity(fact.object, fact.object_type)
                if fact.object_type
                else None
            )
            current = self._conn.execute(
                "SELECT id, object_key, metadata FROM graph_facts WHERE scope = ? "
                "AND subject_id = ? AND relation = ? AND valid_to IS NULL",
                (self.scope, subject_id, fact.relation),
            ).fetchall()
            write, retire = plan_write(
                [(str(r[0]), str(r[1]), r[2]) for r in current],
                object_key,
                qualifiers,
                fact.single,
            )
            if not write:
                self._conn.commit()
                return None
            fact_id = str(uuid.uuid4())
            now = _now()
            self._conn.executemany(
                "UPDATE graph_facts SET valid_to = ?, superseded_by = ? WHERE id = ?",
                [(now, fact_id, old) for old in retire],
            )
            try:
                self._conn.execute(
                    "INSERT INTO graph_facts (id, scope, session_id, subject_id, "
                    "relation, object_id, object, object_key, statement, embedding, "
                    "source, metadata, created_at, kind) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        fact_id,
                        self.scope,
                        self.session_id,
                        subject_id,
                        fact.relation,
                        object_id,
                        fact.object,
                        object_key,
                        fact.statement,
                        (
                            np.asarray(embedding, dtype=np.float32).tobytes()
                            if embedding
                            else None
                        ),
                        source,
                        json.dumps(dict(metadata or {})),
                        now,
                        fact_kind(fact.kind),
                    ),
                )
            except sqlite3.IntegrityError:  # another process wrote it first
                self._conn.rollback()  # and the close above with it
                return None
            self._conn.execute(
                "INSERT INTO graph_facts_fts (statement, fact_id) VALUES (?, ?)",
                (fact.statement, fact_id),
            )
            self._conn.commit()
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
            created_at=_parse_dt(now),
            kind=fact_kind(fact.kind),
        )

    def _entity(self, name: str, kind: str | None) -> str:
        """Id of the entity called ``name``, created on first mention."""
        key = entity_key(name)
        self._conn.execute(
            "INSERT OR IGNORE INTO graph_entities "
            "(id, scope, name, name_key, type, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (str(uuid.uuid4()), self.scope, name, key, kind or "concept", _now()),
        )
        row = self._conn.execute(
            "SELECT id FROM graph_entities WHERE scope = ? AND name_key = ?",
            (self.scope, key),
        ).fetchone()
        return str(row[0])

    # ------------------------------------------------------------------
    # read
    # ------------------------------------------------------------------

    def search(
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
        with self._lock:
            rows = self._conn.execute(
                f"SELECT id, embedding, kind FROM graph_facts f WHERE {where}",  # literal table; WHERE from _read_where, values bound — nosemgrep: tmp.databricks.sql-injection.fstring-select,tmp.databricks.sql-injection.fstring-where
                params,
            ).fetchall()
            keyword_ids = self._keyword_ids(query_text, pool)
        seeds = rank_hits(
            drop_weak(
                self._cosine_hits(rows, query_embedding)[:pool],
                self.strict_min_similarity,
            ),
            keyword_ids,
            limit,
            min_similarity,
        )
        if not seeds:
            return []
        seed_ids = [fid for fid, _ in seeds]
        by_id = {fact.id: fact for fact in self._facts_by_id(seed_ids)}
        out: list[tuple[Fact, float | None]] = [
            (by_id[fid], sim) for fid, sim in seeds if fid in by_id
        ]
        return out

    @staticmethod
    def _cosine_hits(
        rows: list, query_embedding: list[float]
    ) -> list[tuple[str, float, str]]:
        query = np.asarray(query_embedding, dtype=np.float32)
        query_norm = float(np.linalg.norm(query))
        hits: list[tuple[str, float, str]] = []
        if query_norm <= 0:
            return hits
        for fact_id, blob, kind in rows:
            if not blob:
                continue
            vector = np.frombuffer(blob, dtype=np.float32)
            # A store written with another embedder has another dimension.
            if vector.shape != query.shape:
                continue
            denom = query_norm * float(np.linalg.norm(vector))
            if denom > 0:
                hits.append(
                    (
                        str(fact_id),
                        float(np.dot(query, vector) / denom),
                        fact_kind(kind),
                    )
                )
        hits.sort(key=lambda hit: hit[1], reverse=True)
        return hits

    def _keyword_ids(self, query_text: str, pool: int) -> list[str]:
        tokens = keyword_tokens(query_text)
        if not tokens:
            return []
        match = " OR ".join(f'"{token}"' for token in tokens)
        try:
            rows = self._conn.execute(
                "SELECT fact_id FROM graph_facts_fts WHERE graph_facts_fts MATCH ? "
                "ORDER BY bm25(graph_facts_fts) LIMIT ?",
                (match, pool),
            ).fetchall()
        except sqlite3.Error as exc:  # keyword search only re-ranks
            logger.debug("graph keyword search skipped: %s", exc)
            return []
        return [str(row[0]) for row in rows]

    def _read_where(self) -> tuple[str, list[object]]:
        # Current facts, and run notes still inside their retention period.
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.note_retention_days)
        where = (
            "f.scope = ? AND f.valid_to IS NULL "
            "AND NOT (f.kind = ? AND f.created_at < ?)"
        )
        params: list[object] = [self.scope, FACT_EPISODIC, cutoff.isoformat()]
        if not self.workspace_wide:
            where += " AND f.session_id = ?"
            params.append(self.session_id or "")
        return where, params

    def _facts_by_id(self, fact_ids: list[str]) -> list[Fact]:
        if not fact_ids:
            return []
        marks = ",".join("?" * len(fact_ids))
        return self._select_facts(f"f.id IN ({marks})", list(fact_ids), "")

    def _select_facts(self, where: str, params: list[object], tail: str) -> list[Fact]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_FACT_COLUMNS} FROM graph_facts f "  # constant columns, literal table — nosemgrep: tmp.databricks.sql-injection.fstring-select
                f"JOIN graph_entities e ON e.id = f.subject_id WHERE {where} {tail}",  # callers pass fixed WHERE text and int LIMIT; values bound — nosemgrep: tmp.databricks.sql-injection.fstring-where
                params,
            ).fetchall()
        return [self._row_to_fact(row) for row in rows]

    @staticmethod
    def _row_to_fact(row: tuple[object, ...]) -> Fact:
        try:
            metadata = json.loads(str(row[7] or "{}"))
        except ValueError:
            metadata = {}
        return Fact(
            id=str(row[0]),
            subject_id=str(row[1]),
            relation=str(row[2]),
            object_id=str(row[3]) if row[3] else None,
            object=str(row[4]),
            statement=str(row[5]),
            source=str(row[6]) if row[6] else None,
            metadata=metadata if isinstance(metadata, dict) else {},
            created_at=_parse_dt(row[8]),
            valid_to=_parse_dt(row[9]),
            superseded_by=str(row[10]) if row[10] else None,
            subject=str(row[11] or ""),
            kind=fact_kind(row[12]),
        )

    # ------------------------------------------------------------------
    # housekeeping
    # ------------------------------------------------------------------

    def list_facts(self, limit: int = 100, include_retired: bool = False) -> list[Fact]:
        """Newest first. Retired facts on request — they are history."""
        where = "f.scope = ?" + ("" if include_retired else " AND f.valid_to IS NULL")
        return self._select_facts(
            where, [self.scope], f"ORDER BY f.created_at DESC LIMIT {int(limit)}"
        )

    def list_entities(self, limit: int = 500) -> list[Entity]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, name, type FROM graph_entities WHERE scope = ? "
                "ORDER BY name LIMIT ?",
                (self.scope, int(limit)),
            ).fetchall()
        return [Entity(id=str(r[0]), name=str(r[1]), type=str(r[2])) for r in rows]

    def standing_facts(self, limit: int = MAX_STANDING) -> list[Fact]:
        """The user's standing preferences and instructions, newest first."""
        where, params = self._read_where()
        return self._select_facts(
            f"{where} AND f.kind = ?",  # _read_where text plus a literal filter; values bound — nosemgrep: tmp.databricks.sql-injection.fstring-where
            [*params, FACT_PROCEDURAL],
            f"ORDER BY f.created_at DESC LIMIT {int(limit)}",
        )

    def retire_fact(self, fact_id: str) -> bool:
        """Stop recalling one fact, on request. It is closed, not deleted, so
        what memory used to hold stays answerable."""
        with self._lock:
            cursor = self._conn.execute(
                "UPDATE graph_facts SET valid_to = ? "
                "WHERE id = ? AND scope = ? AND valid_to IS NULL",
                (_now(), fact_id, self.scope),
            )
            self._conn.commit()
        return cursor.rowcount > 0

    def count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM graph_facts WHERE scope = ? AND valid_to IS NULL",
                (self.scope,),
            ).fetchone()
        return int(row[0] or 0)

    def reset(self) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM graph_facts_fts WHERE fact_id IN "
                "(SELECT id FROM graph_facts WHERE scope = ?)",
                (self.scope,),
            )
            self._conn.execute("DELETE FROM graph_facts WHERE scope = ?", (self.scope,))
            self._conn.execute(
                "DELETE FROM graph_entities WHERE scope = ?", (self.scope,)
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
