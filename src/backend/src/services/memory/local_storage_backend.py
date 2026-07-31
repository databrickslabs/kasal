"""Local persistent memory storage — SQLite + brute-force cosine.

Default backend for dev / DEFAULT memory configuration. The crewAI LanceDB
default disappeared with the crewai library, which silently degraded local
memory to the engine's in-process dict. This backend restores persistent,
semantic local memory with zero new dependencies: records live in a SQLite
file under the crew storage directory, embeddings are stored as float32
blobs, and search is a numpy cosine over the candidate set — microseconds at
dev scale (thousands of records), no index required.

Implements the crewAI-1.x storage protocol (``search(query_embedding, ...)``,
same as the Databricks/Lakebase backends) so :class:`EngineStorageAdapter`
wraps all three uniformly.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from src.services.memory.engine import KIND_EPISODIC, MemoryRecord, ScopeInfo
from src.services.memory.engine_storage_adapter import embed_text

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS memories (
    id TEXT PRIMARY KEY,
    content TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT '/',
    categories TEXT NOT NULL DEFAULT '[]',
    metadata TEXT NOT NULL DEFAULT '{}',
    importance REAL NOT NULL DEFAULT 0.5,
    source TEXT,
    private INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    last_accessed TEXT NOT NULL,
    embedding BLOB,
    kind TEXT NOT NULL DEFAULT 'episodic',
    valid_from TEXT,
    valid_to TEXT,
    superseded_by TEXT
);
CREATE INDEX IF NOT EXISTS idx_memories_scope ON memories (scope);
"""
# NOTE: no index on ``kind`` here. This script also runs against a table created
# before that column existed, where CREATE TABLE IF NOT EXISTS is a no-op — so
# indexing kind at this point fails with "no such column". It is created in
# _migrate_columns, after the ALTER.

# Columns added after the first release. SQLite has no ADD COLUMN IF NOT EXISTS,
# and CREATE TABLE IF NOT EXISTS is a no-op on an existing table — so a dev store
# created before these landed keeps the old shape and every insert fails on
# "table memories has no column named kind". Applied one at a time, tolerating
# the duplicate-column error, which is the idiomatic SQLite equivalent.
_ADDED_COLUMNS = (
    ("kind", "TEXT NOT NULL DEFAULT 'episodic'"),
    ("valid_from", "TEXT"),
    ("valid_to", "TEXT"),
    ("superseded_by", "TEXT"),
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_blob(vector: list[float] | None) -> bytes | None:
    if vector is None:
        return None
    return np.asarray(vector, dtype=np.float32).tobytes()


def _from_blob(blob: bytes | None) -> np.ndarray | None:
    if not blob:
        return None
    return np.frombuffer(blob, dtype=np.float32)


class LocalMemoryStorage:
    """SQLite-backed local memory store (crewAI-1.x storage protocol)."""

    def __init__(
        self,
        db_path: str | Path,
        embedder: Any = None,
        semantic_weight: float | None = None,
        recency_weight: float | None = None,
        importance_weight: float | None = None,
        recency_half_life_days: float | None = None,
        relevance_threshold: float | None = None,
    ):
        self.db_path = Path(db_path)
        self.embedder = embedder
        # Tuning overrides; class constants are the defaults.
        if semantic_weight is not None:
            self.SEMANTIC_WEIGHT = float(semantic_weight)
        if recency_weight is not None:
            self.RECENCY_WEIGHT = float(recency_weight)
        if importance_weight is not None:
            self.IMPORTANCE_WEIGHT = float(importance_weight)
        if recency_half_life_days is not None:
            self.RECENCY_HALF_LIFE_DAYS = float(recency_half_life_days)
        if relevance_threshold is not None:
            self.RELEVANCE_THRESHOLD = float(relevance_threshold)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.executescript(_SCHEMA)
        self._migrate_columns()
        self._conn.commit()

    def _migrate_columns(self) -> None:
        """Add any post-release column missing from an existing store."""
        existing = {row[1] for row in self._conn.execute("PRAGMA table_info(memories)")}
        for column, ddl in _ADDED_COLUMNS:
            if column in existing:
                continue
            self._conn.execute(f"ALTER TABLE memories ADD COLUMN {column} {ddl}")
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_memories_kind ON memories (kind)"
        )

    # ------------------------------------------------------------------
    # embedding (same helper chain as the remote backends)
    # ------------------------------------------------------------------

    def _embed_sync(self, text_content: str) -> list[float]:
        return embed_text(self.embedder, text_content)

    # ------------------------------------------------------------------
    # protocol methods
    # ------------------------------------------------------------------

    def save(self, records: list[MemoryRecord]) -> None:
        rows = []
        for record in records:
            vector: list[float] | None = record.embedding
            if vector is None and self.embedder is not None:
                try:
                    vector = self._embed_sync(record.content)
                except Exception as exc:  # noqa: BLE001 — keyword fallback still works
                    logger.warning(
                        "Local memory embed failed (%s); saving without vector", exc
                    )
            rows.append(
                (
                    record.id,
                    record.content,
                    record.scope or "/",
                    json.dumps(list(record.categories or [])),
                    json.dumps(dict(record.metadata or {})),
                    float(record.importance),
                    record.source,
                    1 if record.private else 0,
                    (
                        record.created_at.isoformat()
                        if record.created_at
                        else _now_iso()
                    ),
                    _now_iso(),
                    _to_blob(vector),
                    record.kind,
                    record.valid_from.isoformat() if record.valid_from else None,
                    record.valid_to.isoformat() if record.valid_to else None,
                    record.superseded_by,
                )
            )
        with self._lock:
            self._conn.executemany(
                "INSERT OR REPLACE INTO memories "
                "(id, content, scope, categories, metadata, importance, source, "
                " private, created_at, last_accessed, embedding, "
                " kind, valid_from, valid_to, superseded_by) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            self._conn.commit()

    # Hybrid-scoring weights — parity with LakebaseStorageBackend.asearch.
    SEMANTIC_WEIGHT = 0.6
    KEYWORD_WEIGHT = 0.15
    RECENCY_WEIGHT = 0.15
    IMPORTANCE_WEIGHT = 0.10
    RECENCY_HALF_LIFE_DAYS = 30.0
    # Semantic gate applied BEFORE blending — unrelated memories never enter
    # the context, no matter how fresh or important.
    RELEVANCE_THRESHOLD = 0.35

    def search(
        self,
        query_embedding: list[float] | None,
        scope_prefix: str | None = None,
        categories: list[str] | None = None,
        metadata_filter: dict[str, Any] | None = None,
        limit: int = 10,
        min_score: float = 0.0,
        query_text: str | None = None,
    ) -> list[tuple[MemoryRecord, float]]:
        """Blended score = semantic + keyword-overlap + recency + importance
        (same fused-scoring shape as the Lakebase backend, numpy-local).

        Mirrors the two ``kind``-driven policies exactly: superseded records are
        excluded, and the recency decay applies to EPISODIC records only — a
        current fact does not get less true with age.
        """
        rows = self._fetch_rows(scope_prefix)
        if not rows:
            return []
        scored: list[tuple[MemoryRecord, float]] = []
        query_vec: np.ndarray | None = (
            np.asarray(query_embedding, dtype=np.float32) if query_embedding else None
        )
        query_norm = float(np.linalg.norm(query_vec)) if query_vec is not None else 0.0
        query_tokens = {
            token for token in (query_text or "").lower().split() if len(token) > 2
        }
        now = datetime.now(timezone.utc)
        half_life_seconds = self.RECENCY_HALF_LIFE_DAYS * 86400.0
        for row in rows:
            record = self._row_to_record(row)
            if not record.is_current:
                continue  # superseded — history, not context
            if categories and not set(categories) & set(record.categories):
                continue
            if metadata_filter and any(
                record.metadata.get(k) != v for k, v in metadata_filter.items()
            ):
                continue
            vector = _from_blob(row["embedding"])
            if query_vec is not None and vector is not None and query_norm > 0:
                denom = query_norm * float(np.linalg.norm(vector))
                semantic = (
                    float(np.dot(query_vec, vector) / denom) if denom > 0 else 0.0
                )
                if semantic < self.RELEVANCE_THRESHOLD:
                    continue  # unrelated — never rescued by recency/importance
            else:
                semantic = 0.0
            keyword = 0.0
            if query_tokens:
                content_tokens = set(record.content.lower().split())
                keyword = len(query_tokens & content_tokens) / len(query_tokens)
            if record.kind == KIND_EPISODIC:
                created = record.created_at
                if created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                age_seconds = max((now - created).total_seconds(), 0.0)
                recency = float(
                    np.exp(-0.6931471805599453 * age_seconds / half_life_seconds)
                )
            else:
                # Semantic/procedural: no decay. A preference learned two months
                # ago is exactly as true as one learned yesterday.
                recency = 1.0
            score = (
                self.SEMANTIC_WEIGHT * semantic
                + self.KEYWORD_WEIGHT * keyword
                + self.RECENCY_WEIGHT * recency
                + self.IMPORTANCE_WEIGHT * record.importance
            )
            if score >= min_score:
                scored.append((record, score))
        scored.sort(key=lambda pair: (pair[1], pair[0].importance), reverse=True)
        return scored[:limit]

    def list_records(
        self,
        scope_prefix: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[MemoryRecord]:
        with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM memories WHERE scope LIKE ? "
                "ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (f"{scope_prefix or ''}%", limit, offset),
            )
            cursor.row_factory = sqlite3.Row
            rows = [dict(row) for row in self._as_dict_rows(cursor)]
        return [self._row_to_record(row) for row in rows]

    def get_record(self, record_id: str) -> MemoryRecord | None:
        rows = self._select("SELECT * FROM memories WHERE id = ?", (record_id,))
        return self._row_to_record(rows[0]) if rows else None

    def update(self, record: MemoryRecord) -> None:
        self.save([record])

    def delete(
        self,
        scope_prefix: str | None = None,
        categories: list[str] | None = None,
        record_ids: list[str] | None = None,
        older_than: datetime | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> int:
        if record_ids:
            with self._lock:
                placeholders = ",".join("?" * len(record_ids))
                cursor = self._conn.execute(
                    f"DELETE FROM memories WHERE id IN ({placeholders})",
                    record_ids,
                )
                self._conn.commit()
            return cursor.rowcount
        clauses, params = ["scope LIKE ?"], [f"{scope_prefix or ''}%"]
        if older_than is not None:
            clauses.append("created_at < ?")
            params.append(older_than.isoformat())
        with self._lock:
            cursor = self._conn.execute(
                f"DELETE FROM memories WHERE {' AND '.join(clauses)}", params
            )
            self._conn.commit()
        return cursor.rowcount

    def get_scope_info(self, scope: str) -> ScopeInfo:
        rows = self._select(
            "SELECT COUNT(*) AS n, MIN(created_at) AS oldest, "
            "MAX(created_at) AS newest FROM memories WHERE scope LIKE ?",
            (f"{scope or '/'}%",),
        )
        count = int(rows[0]["n"]) if rows else 0
        return ScopeInfo(path=scope or "/", record_count=count)

    def list_scopes(self, parent: str = "/") -> list[str]:
        rows = self._select(
            "SELECT DISTINCT scope FROM memories WHERE scope LIKE ? ORDER BY scope",
            (f"{parent or '/'}%",),
        )
        return [row["scope"] for row in rows]

    def list_categories(self, scope_prefix: str | None = None) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self._select(
            "SELECT categories FROM memories WHERE scope LIKE ?",
            (f"{scope_prefix or ''}%",),
        ):
            for category in json.loads(row["categories"] or "[]"):
                counts[category] = counts.get(category, 0) + 1
        return counts

    def count(self, scope_prefix: str | None = None) -> int:
        rows = self._select(
            "SELECT COUNT(*) AS n FROM memories WHERE scope LIKE ?",
            (f"{scope_prefix or ''}%",),
        )
        return int(rows[0]["n"]) if rows else 0

    def reset(self, scope_prefix: str | None = None) -> None:
        with self._lock:
            if scope_prefix:
                self._conn.execute(
                    "DELETE FROM memories WHERE scope LIKE ?", (f"{scope_prefix}%",)
                )
            else:
                self._conn.execute("DELETE FROM memories")
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _fetch_rows(self, scope_prefix: str | None) -> list[dict]:
        return self._select(
            "SELECT * FROM memories WHERE scope LIKE ?",
            (f"{scope_prefix or ''}%",),
        )

    def _select(self, sql: str, params: tuple | list) -> list[dict]:
        with self._lock:
            cursor = self._conn.execute(sql, params)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @staticmethod
    def _as_dict_rows(cursor: sqlite3.Cursor) -> list:
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @staticmethod
    def _row_to_record(row: dict) -> MemoryRecord:
        def _parse_dt(value: Any) -> datetime:
            if isinstance(value, datetime):
                return value
            try:
                return datetime.fromisoformat(str(value))
            except (TypeError, ValueError):
                return datetime.now(timezone.utc)

        def _parse_optional_dt(value: Any) -> datetime | None:
            return None if value in (None, "") else _parse_dt(value)

        return MemoryRecord(
            id=row["id"],
            content=row["content"],
            scope=row.get("scope") or "/",
            categories=json.loads(row.get("categories") or "[]"),
            metadata=json.loads(row.get("metadata") or "{}"),
            importance=float(row.get("importance") or 0.5),
            source=row.get("source"),
            private=bool(row.get("private")),
            created_at=_parse_dt(row.get("created_at")),
            last_accessed=_parse_dt(row.get("last_accessed")),
            # ``kind`` validates unknown/NULL to episodic, so a row written
            # before the column existed reads correctly.
            kind=row.get("kind"),
            valid_from=_parse_optional_dt(row.get("valid_from")),
            valid_to=_parse_optional_dt(row.get("valid_to")),
            superseded_by=row.get("superseded_by"),
        )
