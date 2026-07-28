"""Memory — unified memory with native interception and context propagation.

Authored module; surface validated against the kasal_engine datamodel.

Engine-native fixes for kasal's memory patches:
- **save_hooks** (native requirement #2): callbacks receive the saved
  MemoryRecords after every remember/remember_many — no method wrapping.
- **Context propagation**: saves run on a serialized pool via
  ``contextvars.copy_context()``, so the ambient ``event_context`` (and any
  kasal ContextVar) reaches the MemorySave/Query events emitted from the
  save thread — no event ``__init__`` patching.
- **Save-time analysis**: one small LLM pass per record fills ``categories``,
  ``importance`` and extracted entities (``analyze_on_save``, needs ``llm``).
  This is what the Cognitive Memory Browser's concept/graph views are built
  from — without it every record persists with an empty tag list and the graph
  is blank. It runs on the save thread, so it never touches a run's hot path,
  and any failure degrades to an unlabelled record rather than a failed save.

Storage is duck-typed through StorageBackend; kasal plugs its
Databricks/Lakebase backends in as ``Memory(storage=backend)``.
"""

import concurrent.futures
import contextvars
import json
import logging
import re
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from src.services.execution.events.bus import event_bus
from src.services.execution.events.types import (
    MemoryQueryCompletedEvent,
    MemoryQueryFailedEvent,
    MemoryQueryStartedEvent,
    MemorySaveCompletedEvent,
    MemorySaveFailedEvent,
    MemorySaveStartedEvent,
)
from .analyze import MemoryAnalysis
from .types import MemoryRecord, ScopeInfo

logger = logging.getLogger(__name__)

# ---------------------------- save-time analysis ----------------------------
# One small LLM pass per saved record produces the categories (and importance)
# that concept/graph views are built from. Short fragments are not worth a call.
_MIN_ANALYSIS_CHARS = 40
_ANALYSIS_CHAR_CAP = 4000
_MAX_CATEGORIES = 6
_MAX_CATEGORY_LEN = 40

_ANALYSIS_SYSTEM_PROMPT = (
    "You label memories for a concept graph. Reply with ONE JSON object and no "
    "other text:\n"
    '{"categories": ["kebab-case-topic", ...], "importance": 0.0-1.0, '
    '"extracted_metadata": {"entities": [], "dates": [], "topics": []}}\n'
    "Rules:\n"
    "- 2-5 categories. Each is a short lowercase kebab-case noun phrase naming "
    "a topic, domain, or entity the memory is ABOUT "
    '(e.g. "swiss-politics", "vector-search", "quarterly-revenue").\n'
    "- Prefer conventional, reusable wording so the same subject gets the same "
    "label across memories; do not invent per-record identifiers.\n"
    "- importance: how useful this is to recall later (routine chatter ~0.3, "
    "durable facts/decisions ~0.8).\n"
    "- entities: proper nouns actually named in the text. Invent nothing."
)


def _clean_categories(values: list[str]) -> list[str]:
    """Trim, normalise to lowercase kebab-case, dedupe, and cap."""
    cleaned: list[str] = []
    for value in values or []:
        slug = re.sub(r"[\s_]+", "-", str(value).strip().lower())
        slug = re.sub(r"[^a-z0-9-]", "", slug).strip("-")
        slug = re.sub(r"-{2,}", "-", slug)[:_MAX_CATEGORY_LEN].strip("-")
        if slug and slug not in cleaned:
            cleaned.append(slug)
    return cleaned[:_MAX_CATEGORIES]


def _extract_json_object(text: str) -> Any:
    """Parse the first JSON object in ``text`` (models wrap it in prose/fences)."""
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z]*\n?|```$", "", stripped).strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start == -1 or end <= start:
        return None
    try:
        return json.loads(stripped[start : end + 1])
    except json.JSONDecodeError:
        return None


class StorageBackend(ABC):
    """What Memory needs from a storage backend."""

    @abstractmethod
    def save(self, records: list[MemoryRecord]) -> list[MemoryRecord]: ...

    @abstractmethod
    def search(
        self,
        query: str,
        limit: int = 10,
        scope: str | None = None,
        score_threshold: float | None = None,
    ) -> list[MemoryRecord]: ...

    @abstractmethod
    def list_records(
        self, scope: str | None = None, limit: int = 100, offset: int = 0
    ) -> list[MemoryRecord]: ...

    @abstractmethod
    def list_scopes(self, path: str = "/") -> list[str]: ...

    @abstractmethod
    def list_categories(self, path: str = "/") -> list[str]: ...

    @abstractmethod
    def get_scope_info(self, path: str) -> ScopeInfo: ...

    @abstractmethod
    def get_record(self, record_id: str) -> MemoryRecord | None: ...

    @abstractmethod
    def update(self, record_id: str, **changes: Any) -> MemoryRecord | None: ...

    @abstractmethod
    def delete(self, record_id: str) -> bool: ...

    @abstractmethod
    def reset(self, scope: str | None = None) -> None: ...

    def close(self) -> None:
        return None


class InMemoryStorage(StorageBackend):
    """Functional in-process backend (substring search, scope prefixes)."""

    def __init__(self) -> None:
        self._records: dict[str, MemoryRecord] = {}

    def save(self, records: list[MemoryRecord]) -> list[MemoryRecord]:
        for record in records:
            self._records[record.id] = record
        return records

    def _in_scope(self, record: MemoryRecord, scope: str | None) -> bool:
        return scope is None or record.scope == scope or record.scope.startswith(
            scope.rstrip("/") + "/"
        )

    def search(
        self,
        query: str,
        limit: int = 10,
        scope: str | None = None,
        score_threshold: float | None = None,
    ) -> list[MemoryRecord]:
        needle = query.lower()
        hits = [
            record
            for record in self._records.values()
            if self._in_scope(record, scope) and needle in record.content.lower()
        ]
        hits.sort(key=lambda r: r.importance, reverse=True)
        return hits[:limit]

    def list_records(
        self, scope: str | None = None, limit: int = 100, offset: int = 0
    ) -> list[MemoryRecord]:
        records = [r for r in self._records.values() if self._in_scope(r, scope)]
        records.sort(key=lambda r: r.created_at)
        return records[offset : offset + limit]

    def list_scopes(self, path: str = "/") -> list[str]:
        return sorted({r.scope for r in self._records.values() if self._in_scope(r, path)})

    def list_categories(self, path: str = "/") -> list[str]:
        categories: set[str] = set()
        for record in self._records.values():
            if self._in_scope(record, path):
                categories.update(record.categories)
        return sorted(categories)

    def get_scope_info(self, path: str) -> ScopeInfo:
        records = [r for r in self._records.values() if self._in_scope(r, path)]
        children = sorted(
            {
                r.scope
                for r in records
                if r.scope != path and r.scope.startswith(path.rstrip("/") + "/")
            }
        )
        return ScopeInfo(
            path=path,
            record_count=len(records),
            categories=sorted({c for r in records for c in r.categories}),
            oldest_record=min((r.created_at for r in records), default=None),
            newest_record=max((r.created_at for r in records), default=None),
            child_scopes=children,
        )

    def get_record(self, record_id: str) -> MemoryRecord | None:
        return self._records.get(record_id)

    def update(self, record_id: str, **changes: Any) -> MemoryRecord | None:
        record = self._records.get(record_id)
        if record is None:
            return None
        updated = record.model_copy(
            update={k: v for k, v in changes.items() if v is not None}
        )
        self._records[record_id] = updated
        return updated

    def delete(self, record_id: str) -> bool:
        return self._records.pop(record_id, None) is not None

    def reset(self, scope: str | None = None) -> None:
        if scope is None:
            self._records.clear()
        else:
            self._records = {
                rid: r for rid, r in self._records.items() if not self._in_scope(r, scope)
            }


class MemoryConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    root_scope: str | None = None


class Memory(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    llm: Any = Field(
        default=None,
        description="LLM for analysis (model name or BaseLLM instance).",
    )
    storage: Any = Field(
        default=None,
        description="Storage backend instance; None gets InMemoryStorage.",
    )
    embedder: Any = Field(default=None)
    read_only: bool = False
    root_scope: str | None = None
    save_hooks: list[Any] = Field(default_factory=list, exclude=True)
    analyze_on_save: bool = Field(
        default=True,
        description=(
            "Label each saved record with an LLM pass (categories, importance, "
            "extracted entities). Requires ``llm``; no-op without one."
        ),
    )
    default_importance: float = Field(
        default=0.5,
        description="Importance used when neither the caller nor analysis supplies one.",
    )

    _config: MemoryConfig = PrivateAttr(default_factory=MemoryConfig)
    _pool: concurrent.futures.ThreadPoolExecutor | None = PrivateAttr(default=None)

    def model_post_init(self, __context: Any) -> None:
        if self.storage is None or isinstance(self.storage, str):
            self.storage = InMemoryStorage()
        super().model_post_init(__context)

    # ------------------------------ writing ------------------------------

    def _save_pool(self) -> concurrent.futures.ThreadPoolExecutor:
        if self._pool is None:
            self._pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="kasal-engine-memory"
            )
        return self._pool

    def _submit_save(self, func: Callable[..., Any], *args: Any) -> concurrent.futures.Future:
        # copy_context: ambient event_context (and kasal's ContextVars)
        # propagate into the save thread — kills the memory-event patch.
        context = contextvars.copy_context()
        return self._save_pool().submit(context.run, func, *args)

    def remember(
        self,
        content: str,
        scope: str | None = None,
        categories: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        importance: float | None = None,
        source: str | None = None,
        private: bool = False,
        agent_role: str | None = None,
        root_scope: str | None = None,
    ) -> MemoryRecord | None:
        records = self.remember_many(
            [content], scope, categories, metadata, importance, source, private,
            agent_role, root_scope,
        )
        return records[0] if records else None

    def remember_many(
        self,
        contents: list[str],
        scope: str | None = None,
        categories: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        importance: float | None = None,
        source: str | None = None,
        private: bool = False,
        agent_role: str | None = None,
        root_scope: str | None = None,
    ) -> list[MemoryRecord]:
        if self.read_only:
            return []
        effective_root = root_scope if root_scope is not None else self.root_scope
        future = self._submit_save(
            self._save_batch, contents, scope, categories, metadata,
            importance, source, private, agent_role, effective_root,
        )
        return future.result()

    def _analyze_for_save(self, content: str) -> "MemoryAnalysis | None":
        """Label one record with the analysis LLM. ``None`` when unavailable.

        This is what fills ``categories`` — without it every record saves with
        an empty tag list, which is invisible in any concept/graph view. It runs
        on the save thread (never the caller's hot path) and is best-effort:
        any failure degrades to an unlabelled record, never a failed save.
        """
        llm = self.llm
        if not self.analyze_on_save or llm is None or not hasattr(llm, "call"):
            return None
        text = " ".join((content or "").split())
        if len(text) < _MIN_ANALYSIS_CHARS:
            return None
        try:
            raw = llm.call(
                [
                    {"role": "system", "content": _ANALYSIS_SYSTEM_PROMPT},
                    {"role": "user", "content": text[:_ANALYSIS_CHAR_CAP]},
                ]
            )
            payload = _extract_json_object(str(raw or ""))
            if payload is None:
                logger.debug("memory analysis returned no JSON object: %.200r", raw)
                return None
            return MemoryAnalysis.model_validate(payload)
        except Exception:  # noqa: BLE001 — labelling must never break a save
            logger.warning("memory analysis failed; saving unlabelled", exc_info=True)
            return None

    def _save_batch(
        self,
        contents: list[str],
        scope: str | None,
        categories: list[str] | None,
        metadata: dict[str, Any] | None,
        importance: float | None,
        source: str | None,
        private: bool,
        agent_role: str | None,
        root_scope: str | None,
    ) -> list[MemoryRecord]:
        start = time.perf_counter()
        for content in contents:
            event_bus.emit(
                self,
                MemorySaveStartedEvent(
                    value=content, metadata=metadata, agent_role=agent_role,
                    source_type="unified_memory",
                ),
            )
        effective_scope = scope or root_scope or "/"
        base_metadata = dict(metadata or {})
        if agent_role:
            # Provenance the record itself carries. The scope stays the tenant
            # boundary (backends filter on it, one of them by EXACT match), so
            # the writing agent has to live in metadata rather than in the path.
            base_metadata.setdefault("agent_role", str(agent_role))
        records: list[MemoryRecord] = []
        for content in contents:
            record_categories = list(categories or [])
            record_importance = importance
            record_metadata = dict(base_metadata)
            # Only analyze what the caller left unspecified — an explicit
            # categories list (consolidation, maintenance) is authoritative.
            if not record_categories or record_importance is None:
                analysis = self._analyze_for_save(content)
                if analysis is not None:
                    if not record_categories:
                        record_categories = _clean_categories(analysis.categories)
                    if record_importance is None:
                        record_importance = analysis.importance
                    for key, values in (
                        ("entities", analysis.extracted_metadata.entities),
                        ("topics", analysis.extracted_metadata.topics),
                        ("dates", analysis.extracted_metadata.dates),
                    ):
                        if values and key not in record_metadata:
                            record_metadata[key] = list(values)
            records.append(
                MemoryRecord(
                    content=content,
                    scope=effective_scope,
                    categories=record_categories,
                    metadata=record_metadata,
                    importance=(
                        record_importance
                        if record_importance is not None
                        else self.default_importance
                    ),
                    source=source,
                    private=private,
                )
            )
        try:
            saved = self.storage.save(records)
        except Exception as e:
            for record in records:
                event_bus.emit(
                    self,
                    MemorySaveFailedEvent(
                        value=record.content, metadata=record.metadata,
                        agent_role=agent_role, error=str(e),
                        source_type="unified_memory",
                    ),
                )
            raise
        elapsed_ms = (time.perf_counter() - start) * 1000
        for record in saved:
            event_bus.emit(
                self,
                MemorySaveCompletedEvent(
                    value=record.content,
                    metadata=record.metadata,
                    agent_role=agent_role,
                    save_time_ms=elapsed_ms,
                    source_type="unified_memory",
                ),
            )
        for hook in self.save_hooks:
            try:
                hook(saved)
            except Exception:
                logger.exception("memory save hook %r failed", hook)
        return saved

    def add_save_hook(self, hook: Callable[[list[MemoryRecord]], None]) -> None:
        self.save_hooks.append(hook)

    # ------------------------------ reading ------------------------------

    def recall(
        self,
        query: str,
        limit: int = 10,
        scope: str | None = None,
        score_threshold: float | None = None,
    ) -> list[MemoryRecord]:
        start = time.perf_counter()
        event_bus.emit(
            self,
            MemoryQueryStartedEvent(
                query=query, limit=limit, score_threshold=score_threshold,
                source_type="unified_memory",
            ),
        )
        try:
            results = self.storage.search(
                query, limit=limit, scope=scope or self.root_scope,
                score_threshold=score_threshold,
            )
        except Exception as e:
            event_bus.emit(
                self,
                MemoryQueryFailedEvent(
                    query=query, limit=limit, score_threshold=score_threshold,
                    error=str(e), source_type="unified_memory",
                ),
            )
            raise
        event_bus.emit(
            self,
            MemoryQueryCompletedEvent(
                query=query,
                results=results,
                limit=limit,
                score_threshold=score_threshold,
                query_time_ms=(time.perf_counter() - start) * 1000,
                source_type="unified_memory",
            ),
        )
        return results

    def list_records(
        self, scope: str | None = None, limit: int = 100, offset: int = 0
    ) -> list[MemoryRecord]:
        return self.storage.list_records(
            scope or self.root_scope, limit=limit, offset=offset
        )

    def list_scopes(self, path: str = "/") -> list[str]:
        return self.storage.list_scopes(path)

    def list_categories(self, path: str = "/") -> list[str]:
        return self.storage.list_categories(path)

    def info(self, path: str = "/") -> ScopeInfo:
        return self.storage.get_scope_info(path)

    def scope(self, path: str) -> "Memory":
        """A view of this memory rooted at path (shares storage and hooks)."""
        scoped = self.model_copy(update={"root_scope": path})
        scoped._pool = None
        return scoped

    def update(
        self,
        record_id: str,
        content: str | None = None,
        scope: str | None = None,
        categories: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        importance: float | None = None,
    ) -> MemoryRecord | None:
        return self.storage.update(
            record_id, content=content, scope=scope, categories=categories,
            metadata=metadata, importance=importance,
        )

    def reset(self, scope: str | None = None) -> None:
        self.storage.reset(scope)

    def close(self) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=True)
            self._pool = None
        self.storage.close()

    def __deepcopy__(self, memo: dict[int, Any] | None = None) -> "Memory":
        # storage/llm are shared (backend connections must not duplicate);
        # the save pool starts fresh.
        cloned = self.model_copy()
        cloned._pool = None
        return cloned
