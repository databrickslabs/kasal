"""``Memory`` — one object per run: ``remember`` and ``recall``.

``remember`` runs on a serialised save thread so a caller never waits: it
labels the record with the memory LLM (categories, importance, kind,
entities — what the Memory Browser's concept views are built from), folds it
into a near-duplicate the store already holds (``consolidation``), saves it
through the ``StorageBackend``, and emits the ``MemorySave*`` events that
become the run's "Memory Write" trace rows. ``save_hooks`` receive the records
that landed.

``recall`` is one search with a score floor — plus, per the Memory Tuning
knobs declared as fields below, distillation of a long query and exploration
of alternatives (``recall_planner``). ``mode="raw"`` is the plain search.

Saves run under ``contextvars.copy_context()`` so the ambient event context
(which task, which agent) reaches the events emitted from the save thread.
Storage is duck-typed through ``StorageBackend``; ``InMemoryStorage`` is the
test double, the real stores live in ``services.memory.storage``.
"""

import concurrent.futures
import contextvars
import logging
import os
import re
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from src.core.events.bus import event_bus
from src.core.events.types import (
    MemoryQueryCompletedEvent,
    MemoryQueryFailedEvent,
    MemoryQueryStartedEvent,
    MemorySaveCompletedEvent,
    MemorySaveFailedEvent,
    MemorySaveStartedEvent,
)

from .analyze import MemoryAnalysis, extract_json_object
from .consolidation import consolidate_on_save
from .recall_planner import deep_recall
from .types import KIND_EPISODIC, MemoryRecord, ScopeInfo

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
    '"kind": "episodic|semantic|procedural", '
    '"extracted_metadata": {"entities": [], "dates": [], "topics": []}}\n'
    "Rules:\n"
    "- 2-5 categories. Each is a short lowercase kebab-case noun phrase naming "
    "a topic, domain, or entity the memory is ABOUT "
    '(e.g. "swiss-politics", "vector-search", "quarterly-revenue").\n'
    "- Prefer conventional, reusable wording so the same subject gets the same "
    "label across memories; do not invent per-record identifiers.\n"
    "- importance: how useful this is to recall later (routine chatter ~0.3, "
    "durable facts/decisions ~0.8).\n"
    "- kind:\n"
    '  * "semantic" — states something that is CURRENTLY TRUE and would still '
    "be true tomorrow: a preference, a decision, a configuration, a property of "
    'a person/system/dataset. ("The user wants output as a table.")\n'
    '  * "procedural" — states HOW to do something: a repeatable method, '
    "sequence of steps, or rule of thumb.\n"
    '  * "episodic" — a record of what HAPPENED: a request and its answer, a '
    "task result, a tool call, an observation tied to one moment.\n"
    '  Default to "episodic" when unsure. A transcript of an exchange is '
    "episodic even when the answer inside it contains a fact.\n"
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


_extract_json_object = extract_json_object


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
        return (
            scope is None
            or record.scope == scope
            or record.scope.startswith(scope.rstrip("/") + "/")
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
        return sorted(
            {r.scope for r in self._records.values() if self._in_scope(r, path)}
        )

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
                rid: r
                for rid, r in self._records.items()
                if not self._in_scope(r, scope)
            }


# Fused-score floor (semantic + keyword + recency + importance, see
# LocalStorageBackend.search) below which a recall returns nothing. 0.75 was
# calibrated against live data with the Databricks embedder (see recall()).
# Ollama's nomic-embed-text compresses the cosine scale: measured live, a run's
# own previous task output scored 0.72 raw / 0.68 fused against the task
# description that produced it, while unrelated records sat at 0.58-0.63 raw /
# <=0.60 fused — so its floor sits between those clusters. Providers not listed
# keep the calibrated default.
DEFAULT_RECALL_MIN_SCORE = 0.75
RECALL_MIN_SCORE_BY_EMBEDDER: dict[str, float] = {"ollama": 0.62}


def default_recall_min_score(embedder_provider: str | None = None) -> float:
    """Blended-score floor for a recall that passes no threshold.

    Precedence: KASAL_MEMORY_RECALL_MIN_SCORE (deployment override) → the floor
    for ``embedder_provider`` → the calibrated default. A teamspace's explicit
    Memory Tuning value is applied by the caller (``Memory.recall_min_score``)
    and never reaches here.
    """
    raw = os.getenv("KASAL_MEMORY_RECALL_MIN_SCORE")
    if raw is not None:
        try:
            return max(0.0, float(raw))
        except ValueError:
            pass
    if embedder_provider:
        return RECALL_MIN_SCORE_BY_EMBEDDER.get(
            embedder_provider.lower(), DEFAULT_RECALL_MIN_SCORE
        )
    return DEFAULT_RECALL_MIN_SCORE


def _default_recall_min_score() -> float:
    return default_recall_min_score()


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
    recall_min_score: float | None = Field(
        default=None,
        description=(
            "Fused-score floor for recalls that pass no threshold. Set by "
            "CrewMemoryService from the teamspace's Memory Tuning or the "
            "resolved embedder's default; None falls back to "
            "default_recall_min_score()."
        ),
    )

    # ── Memory Tuning knobs (Configuration > Memory) ──
    # Declared as fields so a value set in the panel can never be dropped by
    # pydantic again; each is read by the code that implements it —
    # recall_planner (query analysis / exploration) and consolidation
    # (save-time merge). Defaults match the panel's placeholders.
    consolidation_threshold: float = Field(
        default=0.85,
        description="Similarity at/above which a new record is merged into an "
        "existing one at save time (0 disables). See engine/consolidation.py.",
    )
    consolidation_limit: int = Field(
        default=5,
        description="How many nearest records save-time consolidation compares.",
    )
    confidence_threshold_high: float = Field(
        default=0.8,
        description="Best hit score at/above which recall stops exploring.",
    )
    confidence_threshold_low: float = Field(
        default=0.5,
        description="Best hit score below which recall explores alternatives.",
    )
    complex_query_threshold: float = Field(
        default=0.7,
        description="Query complexity (0-1, from analysis) at/above which recall "
        "explores even when the best hit is between the two confidence bounds.",
    )
    exploration_budget: int = Field(
        default=1,
        description="LLM-driven rounds of alternative queries when confidence is "
        "low (0 = shallow search only).",
    )
    query_analysis_threshold: int = Field(
        default=200,
        description="Queries at least this many chars long are distilled by the "
        "memory LLM into a short search query before the search (0 = always).",
    )

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

    def _submit_save(
        self, func: Callable[..., Any], *args: Any
    ) -> concurrent.futures.Future:
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
        kind: str | None = None,
    ) -> MemoryRecord | None:
        records = self.remember_many(
            [content],
            scope,
            categories,
            metadata,
            importance,
            source,
            private,
            agent_role,
            root_scope,
            kind,
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
        kind: str | None = None,
    ) -> list[MemoryRecord]:
        if self.read_only:
            return []
        effective_root = root_scope if root_scope is not None else self.root_scope
        future = self._submit_save(
            self._save_batch,
            contents,
            scope,
            categories,
            metadata,
            importance,
            source,
            private,
            agent_role,
            effective_root,
            kind,
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
            # A skipped labelling is a silent degradation — the record saves
            # with no categories and disappears from every concept/graph view —
            # so say WHY it was skipped instead of nothing.
            logger.warning(
                "memory labelling skipped: analyze_on_save=%s llm=%s has_call=%s",
                self.analyze_on_save,
                type(llm).__name__ if llm is not None else None,
                hasattr(llm, "call"),
            )
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
        kind: str | None = None,
    ) -> list[MemoryRecord]:
        start = time.perf_counter()
        for content in contents:
            event_bus.emit(
                self,
                MemorySaveStartedEvent(
                    value=content,
                    metadata=metadata,
                    agent_role=agent_role,
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
            record_kind = kind
            record_metadata = dict(base_metadata)
            # Only analyze what the caller left unspecified — an explicit
            # categories list (consolidation, maintenance) is authoritative.
            #
            # A missing ``kind`` deliberately does NOT trigger analysis on its
            # own. Consolidation passes categories and importance explicitly and
            # must stay LLM-free per merged cluster; it sets the kind itself from
            # the records it merged. Every ordinary writer (a chat turn, a task
            # output) leaves categories/importance unset, so it analyses anyway
            # and gets classified for free — and anything unclassified falls
            # through to episodic, which is the conservative reading.
            if not record_categories or record_importance is None:
                analysis = self._analyze_for_save(content)
                if analysis is not None:
                    if not record_categories:
                        record_categories = _clean_categories(analysis.categories)
                    if record_importance is None:
                        record_importance = analysis.importance
                    if record_kind is None:
                        record_kind = analysis.kind
                    for key, values in (
                        ("entities", analysis.extracted_metadata.entities),
                        ("topics", analysis.extracted_metadata.topics),
                        ("dates", analysis.extracted_metadata.dates),
                    ):
                        if values and key not in record_metadata:
                            record_metadata[key] = list(values)
            record_kind = record_kind or KIND_EPISODIC
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
                    kind=record_kind,
                    # A durable record's validity window opens now. Without
                    # real-world date extraction, "when we learned it" is the
                    # best available answer for "when it started being true";
                    # created_at keeps the recording time separately, so a later
                    # extractor can correct valid_from without losing either.
                    valid_from=(
                        datetime.now(timezone.utc)
                        if record_kind != KIND_EPISODIC
                        else None
                    ),
                )
            )
        # Save-time consolidation: a record that says what a stored one already
        # says is folded into that record instead of being inserted beside it
        # (Memory Tuning: consolidation threshold / limit).
        to_insert: list[MemoryRecord] = []
        consolidated: list[MemoryRecord] = []
        for record in records:
            folded = consolidate_on_save(self, record, effective_scope)
            if folded is not None:
                consolidated.append(folded)
            else:
                to_insert.append(record)
        try:
            saved = self.storage.save(to_insert) if to_insert else []
        except Exception as e:
            for record in to_insert:
                event_bus.emit(
                    self,
                    MemorySaveFailedEvent(
                        value=record.content,
                        metadata=record.metadata,
                        agent_role=agent_role,
                        error=str(e),
                        source_type="unified_memory",
                    ),
                )
            raise
        elapsed_ms = (time.perf_counter() - start) * 1000
        landed = [*saved, *consolidated]
        for record in landed:
            event_bus.emit(
                self,
                MemorySaveCompletedEvent(
                    value=record.content,
                    metadata=record.metadata,
                    agent_role=agent_role,
                    save_time_ms=elapsed_ms,
                    record_id=record.id,
                    source_type="unified_memory",
                ),
            )
        for hook in self.save_hooks:
            try:
                hook(landed)
            except Exception:
                logger.exception("memory save hook %r failed", hook)
        return landed

    def add_save_hook(self, hook: Callable[[list[MemoryRecord]], None]) -> None:
        self.save_hooks.append(hook)

    # ------------------------------ reading ------------------------------

    def recall(
        self,
        query: str,
        limit: int = 10,
        scope: str | None = None,
        score_threshold: float | None = None,
        mode: str = "auto",
    ) -> list[MemoryRecord]:
        """Records relevant to ``query``, best first.

        ``mode="auto"`` applies the Memory Tuning knobs: a long query is
        distilled into a search query, and alternatives are explored when the
        shallow result is weak (``recall_planner``). ``mode="raw"`` is one plain
        vector search with the literal text — what the write-time duplicate
        check needs, since a distilled version of the content it is about to
        write is the wrong thing to compare.
        """
        # Nearest-neighbour search always returns SOMETHING — nearest is not
        # near. Without a floor, a query about a topic the store has never seen
        # returns the k least-unrelated records, and callers inject them as
        # context. Knowledge search stops the same failure with the same rule
        # (services/knowledge/search_guard.py: KNOWLEDGE_MIN_SCORE) — this is
        # that stopping rule for memory (measured live: "genie ontology" over
        # a news-only store recalled 18 news records at blended scores
        # 0.56-0.74, and the model wove "Lebanon news" into the diagram).
        # Related recalls in the same store scored 0.77-0.91, so the default
        # floor sits between the two clusters. Callers may pass an explicit
        # threshold — 0.0 disables.
        if score_threshold is None:
            score_threshold = (
                self.recall_min_score
                if self.recall_min_score is not None
                else _default_recall_min_score()
            )
        start = time.perf_counter()
        event_bus.emit(
            self,
            MemoryQueryStartedEvent(
                query=query,
                limit=limit,
                score_threshold=score_threshold,
                source_type="unified_memory",
            ),
        )
        effective_scope = scope or self.root_scope

        def _search(text: str) -> list[MemoryRecord]:
            return self.storage.search(
                text,
                limit=limit,
                scope=effective_scope,
                score_threshold=score_threshold,
            )

        distilled: str | None = None
        rounds = 0
        try:
            if mode == "raw":
                results = _search(query)
            else:
                outcome = deep_recall(
                    self, query, limit=limit, scope=effective_scope, search=_search
                )
                results = outcome.records
                rounds = outcome.rounds
                if (
                    outcome.plan.analyzed
                    and outcome.plan.query.lower() != query.lower()
                ):
                    distilled = outcome.plan.query
        except Exception as e:
            event_bus.emit(
                self,
                MemoryQueryFailedEvent(
                    query=query,
                    limit=limit,
                    score_threshold=score_threshold,
                    error=str(e),
                    source_type="unified_memory",
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
                distilled_query=distilled,
                exploration_rounds=rounds,
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
        kind: str | None = None,
        valid_from: datetime | None = None,
        valid_to: datetime | None = None,
        superseded_by: str | None = None,
    ) -> MemoryRecord | None:
        """Change named fields on one record. Unset arguments are left alone.

        Filtering ``None`` here rather than at each backend is what makes a
        partial update safe: ``InMemoryStorage`` dropped Nones but
        ``EngineStorageAdapter`` assigned them straight onto the record, so
        retiring a fact would also have blanked its ``content``.
        """
        changes = {
            "content": content,
            "scope": scope,
            "categories": categories,
            "metadata": metadata,
            "importance": importance,
            "kind": kind,
            "valid_from": valid_from,
            "valid_to": valid_to,
            "superseded_by": superseded_by,
        }
        return self.storage.update(
            record_id, **{k: v for k, v in changes.items() if v is not None}
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
