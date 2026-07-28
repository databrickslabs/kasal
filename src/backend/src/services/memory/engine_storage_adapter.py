"""Bridge between ``src.services.memory.engine.Memory`` and Kasal's storage backends.

``Memory.recall`` speaks the engine ``StorageBackend`` protocol —
``search(query: str, limit, scope, score_threshold)`` — while the real
backends (:class:`DatabricksStorageBackend`, :class:`LakebaseStorageBackend`,
:class:`LocalMemoryStorage`) keep the crewAI-1.x protocol where the CALLER
embeds the query first: ``search(query_embedding, scope_prefix, ...,
min_score)``. This adapter absorbs that mismatch in one place:

* embeds the query (with a small LRU cache — repeated asks cost nothing),
* maps kwarg names (``scope`` → ``scope_prefix``, ``score_threshold`` →
  ``min_score``),
* unwraps ``(record, score)`` tuples into records (score kept in metadata),
* normalizes ``save`` to return the saved records (backends return ``None``),
* maps the record-id oriented ``update``/``delete`` onto the filter-oriented
  backend methods.

Keeping this app-side means the vendored ``kasal_engine`` package needs no
hand edits (its datamodel is regenerated upstream).
"""

from __future__ import annotations

import inspect
import logging
import threading
import time
from collections import OrderedDict
from typing import Any

from src.services.memory.engine import MemoryRecord, ScopeInfo
from src.services.memory.engine.memory import StorageBackend

logger = logging.getLogger(__name__)

_QUERY_CACHE_MAX = 128


def embed_text(embedder: Any, text: str) -> list[float]:
    """Embed ``text`` with whatever embedder shape Kasal resolved.

    Mirrors the fallback chain the storage backends use internally: a bare
    callable, a dict-wrapped custom embedder (``{"config": {"embedder": fn}}``),
    or an object exposing ``embed_documents``.
    """
    if embedder is None:
        raise ValueError("No embedder available to embed query text")
    if isinstance(embedder, dict):
        inner = embedder.get("config", {}).get("embedder", embedder)
        if callable(inner):
            result = inner([text])
        elif hasattr(inner, "embed_documents"):
            result = inner.embed_documents([text])
        else:
            raise TypeError(f"Unsupported embedder dict shape: {type(inner).__name__}")
    elif callable(embedder):
        result = embedder([text])
    elif hasattr(embedder, "embed_documents"):
        result = embedder.embed_documents([text])
    else:
        raise TypeError(f"Unsupported embedder type: {type(embedder).__name__}")
    if not result:
        raise RuntimeError("Embedder returned no vectors")
    vector = result[0]
    return list(vector)


def build_litellm_embedder(provider_config: dict) -> Any:
    """Turn an EmbedderConfigBuilder provider dict into an embedding callable.

    The DEFAULT memory backend historically relied on crewAI to materialize an
    embedding function from ``{"provider": ..., "config": {"model": ...}}``.
    With crewAI gone, route the same dict through litellm (already the app's
    LLM transport) so local/dev memory works without extra dependencies.

    Returns a ``callable(list[str]) -> list[list[float]]`` or ``None`` when the
    dict does not describe a usable embedding endpoint.
    """
    if not isinstance(provider_config, dict):
        return None
    provider = str(provider_config.get("provider") or "").strip().lower()
    config = provider_config.get("config") or {}
    model = config.get("model") or config.get("model_name")
    if not model:
        return None
    if "/" not in str(model) and provider and provider not in ("openai", "custom"):
        model = f"{provider}/{model}"
    call_kwargs: dict[str, Any] = {}
    for src_key, dst_key in (
        ("api_key", "api_key"),
        ("api_base", "api_base"),
        ("base_url", "api_base"),
        ("api_url", "api_base"),
    ):
        value = config.get(src_key)
        if value and dst_key not in call_kwargs:
            call_kwargs[dst_key] = value

    def _embed(texts: list[str]) -> list[list[float]]:
        import litellm

        response = litellm.embedding(model=str(model), input=list(texts), **call_kwargs)
        data = response["data"] if isinstance(response, dict) else response.data
        return [
            item["embedding"] if isinstance(item, dict) else item.embedding
            for item in data
        ]

    return _embed


class EngineStorageAdapter(StorageBackend):
    """Engine-protocol facade over a crewAI-1.x-protocol storage backend."""

    def __init__(self, backend: Any, embedder: Any = None):
        self._backend = backend
        self._embedder = (
            embedder if embedder is not None else getattr(backend, "embedder", None)
        )
        self._query_cache: OrderedDict[str, list[float]] = OrderedDict()
        self._cache_lock = threading.Lock()
        # Hybrid backends (Lakebase/local) accept the raw query text for
        # keyword scoring; older ones (Databricks VS) don't — detect once.
        try:
            self._backend_takes_query_text = (
                "query_text" in inspect.signature(backend.search).parameters
            )
        except (TypeError, ValueError):
            self._backend_takes_query_text = False
        # scope → (expires_at_monotonic, has_records). Lets recall skip the
        # embedding round-trip entirely on scopes with no memories yet.
        self._scope_probe_cache: dict[str, tuple[float, bool]] = {}

    # ------------------------------------------------------------------
    # embedding
    # ------------------------------------------------------------------

    def _embed_query(self, query: str) -> list[float] | None:
        key = " ".join(query.split()).lower()[:512]
        with self._cache_lock:
            cached = self._query_cache.get(key)
            if cached is not None:
                self._query_cache.move_to_end(key)
                return cached
        try:
            embed_sync = getattr(self._backend, "_embed_sync", None)
            vector = (
                embed_sync(query)
                if callable(embed_sync)
                else embed_text(self._embedder, query)
            )
        except Exception as exc:  # noqa: BLE001 — recall is best-effort
            logger.warning("Memory query embedding failed (%s); recall skipped", exc)
            return None
        with self._cache_lock:
            self._query_cache[key] = vector
            while len(self._query_cache) > _QUERY_CACHE_MAX:
                self._query_cache.popitem(last=False)
        return vector

    # ------------------------------------------------------------------
    # StorageBackend protocol (engine side)
    # ------------------------------------------------------------------

    def save(self, records: list[MemoryRecord]) -> list[MemoryRecord]:
        result = self._backend.save(records)
        # New records may fill a scope the probe cached as empty.
        self._scope_probe_cache.clear()
        # crewAI-1.x backends return None; the engine iterates the return value.
        return result if result is not None else records

    def _scope_has_records(self, scope: str | None) -> bool:
        """Cached (60s TTL) emptiness probe — a COUNT is far cheaper than the
        embedding round-trip recall would otherwise spend on a fresh workspace.
        Unknowable (no count support / probe error) counts as non-empty."""
        count = getattr(self._backend, "count", None)
        if not callable(count):
            return True
        key = scope or "/"
        now = time.monotonic()
        cached = self._scope_probe_cache.get(key)
        if cached and cached[0] > now:
            return cached[1]
        try:
            has_records = int(count(scope_prefix=scope) or 0) > 0
        except Exception as exc:  # noqa: BLE001 — probe is an optimization only
            logger.debug("Memory scope probe failed (%s); assuming non-empty", exc)
            return True
        self._scope_probe_cache[key] = (now + 60.0, has_records)
        return has_records

    def search(
        self,
        query: str,
        limit: int = 10,
        scope: str | None = None,
        score_threshold: float | None = None,
    ) -> list[MemoryRecord]:
        if not self._scope_has_records(scope):
            return []
        vector = self._embed_query(query)
        if vector is None:
            return []
        search_kwargs: dict[str, Any] = dict(
            query_embedding=vector,
            scope_prefix=scope,
            limit=limit,
            min_score=score_threshold if score_threshold is not None else 0.0,
        )
        if self._backend_takes_query_text:
            search_kwargs["query_text"] = query
        raw = self._backend.search(**search_kwargs)
        records: list[MemoryRecord] = []
        for item in raw or []:
            if isinstance(item, tuple):
                record, score = item[0], item[1] if len(item) > 1 else None
                if record is None:
                    continue
                if score is not None:
                    try:
                        record.metadata.setdefault("similarity", round(float(score), 4))
                    except Exception:  # noqa: BLE001 — score is advisory only
                        pass
                records.append(record)
            elif item is not None:
                records.append(item)
        return records

    def list_records(
        self, scope: str | None = None, limit: int = 100, offset: int = 0
    ) -> list[MemoryRecord]:
        return self._backend.list_records(
            scope_prefix=scope, limit=limit, offset=offset
        )

    def list_scopes(self, path: str = "/") -> list[str]:
        return self._backend.list_scopes(parent=path)

    def list_categories(self, path: str = "/") -> list[str]:
        categories = self._backend.list_categories(scope_prefix=path)
        if isinstance(categories, dict):
            return sorted(categories)
        return list(categories or [])

    def get_scope_info(self, path: str) -> ScopeInfo:
        return self._backend.get_scope_info(path)

    def get_record(self, record_id: str) -> MemoryRecord | None:
        return self._backend.get_record(record_id)

    def update(self, record_id: str, **changes: Any) -> MemoryRecord | None:
        record = self._backend.get_record(record_id)
        if record is None:
            return None
        for key, value in changes.items():
            if hasattr(record, key):
                setattr(record, key, value)
        self._backend.update(record)
        return record

    def delete(self, record_id: str) -> bool:
        deleted = self._backend.delete(record_ids=[record_id])
        try:
            return int(deleted or 0) > 0
        except (TypeError, ValueError):
            return bool(deleted)

    def reset(self, scope: str | None = None) -> None:
        self._backend.reset(scope_prefix=scope)

    def close(self) -> None:
        close = getattr(self._backend, "close", None)
        if callable(close):
            close()
