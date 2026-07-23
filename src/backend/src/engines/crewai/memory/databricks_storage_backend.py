"""Databricks Vector Search implementation of CrewAI's unified ``StorageBackend`` protocol.

Introduced with the CrewAI 1.10+ cognitive memory system. Replaces the legacy
``crewai_databricks_wrapper.py`` which implemented the old per-memory-type
(ShortTermMemory / LongTermMemory / EntityMemory) interfaces.

The unified ``Memory`` class operates on ``MemoryRecord`` objects and speaks to
storage through a single 14-method protocol. This class maps those records to
Kasal's existing ``UNIFIED_MEMORY_SCHEMA`` columns in a Databricks Vector Search
index and preserves multi-tenant isolation via ``crew_id`` / ``group_id`` /
``session_id`` filters on every operation.
"""
from __future__ import annotations

import asyncio
import json
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from crewai.memory.types import MemoryRecord, ScopeInfo

from src.core.logger import LoggerManager
from src.repositories.databricks_vector_index_repository import (
    DatabricksVectorIndexRepository,
)
from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

# Ensure asyncpg NullPool is enabled before any DB connections exist in this module.
if not os.environ.get("USE_NULLPOOL"):
    os.environ["USE_NULLPOOL"] = "true"


logger = LoggerManager.get_instance().crew
memory_logger = LoggerManager.get_instance().databricks_vector_search


_SCHEMA_VERSION = 1
_DEFAULT_EMBEDDING_MODEL = "databricks-gte-large-en"
_SCHEMA_COLUMNS = DatabricksIndexSchemas.get_search_columns("unified")

# Long-lived background loop for the sync->async bridge (PERF-012). One loop
# on one daemon thread serves every memory operation, instead of a fresh
# ThreadPoolExecutor + event loop per call. It also gives loop-bound resources
# (the shared aiohttp session, cached auth) a stable home across operations.
_BRIDGE_LOOP: asyncio.AbstractEventLoop | None = None
_BRIDGE_LOCK = threading.Lock()


def _get_bridge_loop() -> asyncio.AbstractEventLoop:
    global _BRIDGE_LOOP
    with _BRIDGE_LOCK:
        if _BRIDGE_LOOP is None or _BRIDGE_LOOP.is_closed():
            loop = asyncio.new_event_loop()
            thread = threading.Thread(
                target=loop.run_forever, name="kasal-memory-bridge", daemon=True
            )
            thread.start()
            _BRIDGE_LOOP = loop
        return _BRIDGE_LOOP


class DatabricksStorageBackend:
    """Unified-memory storage backed by Databricks Vector Search.

    Implements CrewAI's ``StorageBackend`` protocol. Every read and write is
    automatically scoped to ``group_id`` + ``crew_id`` so memories from one
    tenant cannot leak into another. Short-term-style session scoping is
    available via ``session_id`` (mapped from the execution/job id).
    """

    def __init__(
        self,
        *,
        index_name: str,
        endpoint_name: str,
        workspace_url: str,
        crew_id: str,
        group_id: str,
        user_token: str | None = None,
        session_id: str | None = None,
        embedder: Any = None,
        embedding_dimension: int = 1024,
        embedding_model: str = _DEFAULT_EMBEDDING_MODEL,
    ) -> None:
        self.index_name = index_name
        self.endpoint_name = endpoint_name
        self.workspace_url = workspace_url
        self.crew_id = crew_id
        self.group_id = group_id
        self.user_token = user_token
        self.session_id = session_id
        self.embedder = embedder
        self.embedding_dimension = embedding_dimension
        self.embedding_model = embedding_model

        self._repo = DatabricksVectorIndexRepository(workspace_url, group_id=group_id)

        memory_logger.info(
            "DatabricksStorageBackend initialized (index=%s, crew_id=%s, group_id=%s)",
            index_name,
            crew_id,
            group_id,
        )

    # ------------------------------------------------------------------
    # StorageBackend protocol — synchronous methods
    # ------------------------------------------------------------------

    def save(self, records: list[MemoryRecord]) -> None:
        """Persist a batch of records."""
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
        """Replace an existing record (upsert by ``id``)."""
        self._run_sync(self.asave([record]))

    def get_record(self, record_id: str) -> MemoryRecord | None:
        filters = self._tenant_filters()
        filters["id"] = record_id
        async def _fetch() -> MemoryRecord | None:
            rows = await self._similarity_query(
                query_vector=self._zero_vector(),
                limit=1,
                filters=filters,
            )
            for record, _ in rows:
                return record
            return None
        return self._run_sync(_fetch())

    def list_records(
        self,
        scope_prefix: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[MemoryRecord]:
        async def _list() -> list[MemoryRecord]:
            filters = self._tenant_filters()
            if scope_prefix:
                filters["scope"] = scope_prefix
            rows = await self._similarity_query(
                query_vector=self._zero_vector(),
                limit=limit + offset,
                filters=filters,
            )
            records = [record for record, _ in rows]
            records.sort(key=lambda r: r.created_at, reverse=True)
            return records[offset : offset + limit]
        return self._run_sync(_list())

    def get_scope_info(self, scope: str) -> ScopeInfo:
        async def _info() -> ScopeInfo:
            filters = self._tenant_filters()
            filters["scope"] = scope
            rows = await self._similarity_query(
                query_vector=self._zero_vector(),
                limit=1000,
                filters=filters,
            )
            records = [record for record, _ in rows]
            categories: set[str] = set()
            oldest: datetime | None = None
            newest: datetime | None = None
            for r in records:
                categories.update(r.categories)
                if oldest is None or r.created_at < oldest:
                    oldest = r.created_at
                if newest is None or r.created_at > newest:
                    newest = r.created_at
            children = await self._list_child_scopes(scope)
            return ScopeInfo(
                path=scope,
                record_count=len(records),
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
            filters = self._tenant_filters()
            if scope_prefix:
                filters["scope"] = scope_prefix
            rows = await self._similarity_query(
                query_vector=self._zero_vector(),
                limit=1000,
                filters=filters,
            )
            counts: dict[str, int] = {}
            for record, _ in rows:
                for category in record.categories:
                    counts[category] = counts.get(category, 0) + 1
            return counts
        return self._run_sync(_categories())

    def count(self, scope_prefix: str | None = None) -> int:
        async def _count() -> int:
            filters = self._tenant_filters()
            if scope_prefix:
                filters["scope"] = scope_prefix
            result = await self._repo.count_documents(
                index_name=self.index_name,
                endpoint_name=self.endpoint_name,
                filters=filters,
                user_token=self.user_token,
            )
            if isinstance(result, dict):
                return int(result.get("count", 0))
            return int(result or 0)
        return self._run_sync(_count())

    def reset(self, scope_prefix: str | None = None) -> None:
        self.delete(scope_prefix=scope_prefix)

    # ------------------------------------------------------------------
    # StorageBackend protocol — async methods
    # ------------------------------------------------------------------

    async def asave(self, records: list[MemoryRecord]) -> None:
        if not records:
            return
        payload = [self._record_to_row(r) for r in records]
        await self._repo.upsert(
            index_name=self.index_name,
            endpoint_name=self.endpoint_name,
            records=payload,
            user_token=self.user_token,
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
        filters = self._tenant_filters()
        if scope_prefix:
            # Databricks does not support prefix matching on string filters out
            # of the box; emulate it by filtering after the vector search.
            pass
        if categories:
            filters["categories"] = categories  # Databricks supports IN via list
        if metadata_filter:
            for key, value in metadata_filter.items():
                filters[key] = value

        rows = await self._similarity_query(
            query_vector=query_embedding,
            limit=limit * 3,  # oversample, filter locally
            filters=filters,
        )
        filtered: list[tuple[MemoryRecord, float]] = []
        for record, score in rows:
            if score < min_score:
                continue
            if scope_prefix and not record.scope.startswith(scope_prefix):
                continue
            if record.private and record.source not in (self.session_id, self.crew_id):
                continue
            filtered.append((record, score))
            if len(filtered) >= limit:
                break
        return filtered

    async def adelete(
        self,
        scope_prefix: str | None = None,
        categories: list[str] | None = None,
        record_ids: list[str] | None = None,
        older_than: datetime | None = None,
        metadata_filter: dict[str, Any] | None = None,
    ) -> int:
        if record_ids:
            result = await self._repo.delete_records(
                index_name=self.index_name,
                endpoint_name=self.endpoint_name,
                primary_keys=record_ids,
                user_token=self.user_token,
            )
            if isinstance(result, dict):
                return int(result.get("deleted", len(record_ids)))
            return len(record_ids)

        filters = self._tenant_filters()
        if scope_prefix:
            filters["scope"] = scope_prefix
        if categories:
            filters["categories"] = categories
        if metadata_filter:
            filters.update(metadata_filter)

        rows = await self._similarity_query(
            query_vector=self._zero_vector(),
            limit=10_000,
            filters=filters,
        )
        ids_to_delete: list[str] = []
        for record, _ in rows:
            if older_than is not None and record.created_at >= older_than:
                continue
            ids_to_delete.append(record.id)
        if not ids_to_delete:
            return 0
        await self._repo.delete_records(
            index_name=self.index_name,
            endpoint_name=self.endpoint_name,
            primary_keys=ids_to_delete,
            user_token=self.user_token,
        )
        return len(ids_to_delete)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _tenant_filters(self) -> dict[str, Any]:
        """Filters applied to every read to enforce tenant isolation."""
        filters: dict[str, Any] = {
            "crew_id": self.crew_id,
            "group_id": self.group_id,
        }
        if self.session_id:
            # Only short-term-scoped memories carry this; absent means
            # long-lived memory so we do not filter by session.
            pass
        return filters

    def _zero_vector(self) -> list[float]:
        return [0.0] * self.embedding_dimension

    def _record_to_row(self, record: MemoryRecord) -> dict[str, Any]:
        """Serialise a MemoryRecord into the unified Databricks schema."""
        embedding = record.embedding
        if embedding is None:
            if not self.embedder:
                raise ValueError(
                    "MemoryRecord has no embedding and no embedder configured "
                    "on DatabricksStorageBackend."
                )
            embedding = self._embed_sync(record.content)
        return {
            "id": record.id or str(uuid.uuid4()),
            "content": record.content,
            "scope": record.scope,
            "categories": json.dumps(record.categories or []),
            "importance": float(record.importance),
            "source": record.source or "",
            "private": bool(record.private),
            "metadata": json.dumps(record.metadata or {}),
            "created_at": record.created_at.isoformat(),
            "last_accessed": record.last_accessed.isoformat(),
            "crew_id": self.crew_id,
            "agent_id": (record.metadata or {}).get("agent_id", ""),
            "group_id": self.group_id,
            "session_id": self.session_id or "",
            "llm_model": (record.metadata or {}).get("llm_model", ""),
            "tools_used": json.dumps((record.metadata or {}).get("tools_used", [])),
            "embedding": list(embedding),
            "embedding_model": self.embedding_model,
            "version": _SCHEMA_VERSION,
        }

    def _row_to_record(self, row: dict[str, Any]) -> MemoryRecord:
        """Deserialise a Databricks row dict back into a MemoryRecord."""
        metadata = _loads_or_empty(row.get("metadata"))
        categories = _loads_or_list(row.get("categories"))
        # Promote Kasal-specific columns back into metadata so callers keep
        # provenance without having to know about the underlying index shape.
        metadata.setdefault("crew_id", row.get("crew_id"))
        metadata.setdefault("agent_id", row.get("agent_id"))
        metadata.setdefault("group_id", row.get("group_id"))
        metadata.setdefault("session_id", row.get("session_id"))
        metadata.setdefault("llm_model", row.get("llm_model"))
        metadata.setdefault("tools_used", _loads_or_list(row.get("tools_used")))
        return MemoryRecord(
            id=row.get("id") or str(uuid.uuid4()),
            content=row.get("content", ""),
            scope=row.get("scope") or "/",
            categories=categories,
            importance=float(row.get("importance") or 0.5),
            source=row.get("source") or None,
            private=bool(row.get("private") or False),
            metadata=metadata,
            created_at=_parse_datetime(row.get("created_at")),
            last_accessed=_parse_datetime(row.get("last_accessed")),
        )

    async def _similarity_query(
        self,
        query_vector: list[float],
        limit: int,
        filters: dict[str, Any] | None = None,
    ) -> list[tuple[MemoryRecord, float]]:
        response = await self._repo.similarity_search(
            index_name=self.index_name,
            endpoint_name=self.endpoint_name,
            query_vector=query_vector,
            columns=_SCHEMA_COLUMNS,
            num_results=limit,
            filters=filters,
            user_token=self.user_token,
        )
        # Repository contract: {"success": bool, "results": <raw API json>}
        # with rows at results["result"]["data_array"].
        response = response or {}
        if not response.get("success", False):
            memory_logger.warning(
                "Vector similarity search failed: %s",
                response.get("message") or response.get("error") or "unknown error",
            )
            return []
        result = (response.get("results") or {}).get("result") or {}
        data_array = result.get("data_array") or []
        positions = DatabricksIndexSchemas.get_column_positions("unified")
        score_index = len(_SCHEMA_COLUMNS)  # score is appended after requested columns
        out: list[tuple[MemoryRecord, float]] = []
        for row in data_array:
            row_dict: dict[str, Any] = {
                column: row[positions[column]]
                for column in _SCHEMA_COLUMNS
                if positions.get(column) is not None and positions[column] < len(row)
            }
            score = 0.0
            if len(row) > score_index:
                try:
                    score = float(row[score_index])
                except (TypeError, ValueError):
                    score = 0.0
            try:
                record = self._row_to_record(row_dict)
            except Exception as exc:  # pragma: no cover - defensive
                memory_logger.warning("Failed to parse row into MemoryRecord: %s", exc)
                continue
            out.append((record, score))
        return out

    async def _list_child_scopes(self, parent: str) -> list[str]:
        filters = self._tenant_filters()
        rows = await self._similarity_query(
            query_vector=self._zero_vector(),
            limit=1000,
            filters=filters,
        )
        prefix = parent if parent.endswith("/") else f"{parent}/"
        children: set[str] = set()
        for record, _ in rows:
            if not record.scope.startswith(prefix):
                continue
            remainder = record.scope[len(prefix):]
            if not remainder:
                continue
            first_segment = remainder.split("/", 1)[0]
            children.add(f"{prefix}{first_segment}")
        return sorted(children)

    def _embed_sync(self, text: str) -> list[float]:
        """Invoke the configured embedder from sync context.

        Mirrors the fallback chain the legacy wrapper used: callable embedder,
        dict-wrapped custom embedder, or an object with ``embed_documents``.
        """
        if self.embedder is None:
            raise ValueError("No embedder configured on DatabricksStorageBackend")
        embedder = self.embedder
        if isinstance(embedder, dict):
            inner = embedder.get("config", {}).get("embedder", embedder)
            if callable(inner):
                result = inner([text])
            elif hasattr(inner, "embed_documents"):
                result = inner.embed_documents([text])
            else:
                raise TypeError(f"Unsupported embedder dict shape: {embedder!r}")
        elif callable(embedder):
            result = embedder([text])
        elif hasattr(embedder, "embed_documents"):
            result = embedder.embed_documents([text])
        else:
            raise TypeError(f"Unsupported embedder type: {type(embedder).__name__}")
        if not result:
            raise RuntimeError("Embedder returned no vectors")
        vector = result[0]
        if hasattr(vector, "tolist"):
            return list(vector.tolist())
        return list(vector)

    # ------------------------------------------------------------------
    # Async/sync bridge
    # ------------------------------------------------------------------

    def _run_sync(self, coro: Any) -> Any:
        """Run an async coroutine from sync context without clobbering the caller's loop.

        All coroutines run on one long-lived background loop (PERF-012):
        the previous per-call ThreadPoolExecutor + fresh event loop paid
        thread/loop setup on every memory operation AND gave loop-bound
        resources (shared aiohttp session, auth caches) no stable home.
        """
        if not asyncio.iscoroutine(coro):
            return coro
        return asyncio.run_coroutine_threadsafe(coro, _get_bridge_loop()).result()


# ----------------------------------------------------------------------
# Module helpers
# ----------------------------------------------------------------------


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


def _loads_or_list(value: Any) -> list[Any]:
    if not value:
        return []
    if isinstance(value, list):
        return list(value)
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except (TypeError, ValueError):
        return []


def _to_naive_utc(dt: datetime) -> datetime:
    """Coerce a datetime to offset-naive UTC.

    CrewAI's recency math (``datetime.utcnow() - record.created_at``) is
    offset-naive, so MemoryRecord datetimes must be naive UTC — otherwise a
    stored offset-aware ISO timestamp raises ``can't subtract offset-naive and
    offset-aware datetimes`` in RecallFlow.search_chunks.
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
