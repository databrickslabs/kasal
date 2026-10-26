"""``GraphMemory`` — the run-facing memory object when a teamspace stores an
entity/fact graph instead of whole records.

It answers to the same calls ``Memory`` does — ``remember`` and ``recall``, with
``root_scope`` as the tenant boundary — so ``run/recall.py``, ``run/persist.py``
and the CrewAI harness use it unchanged, and it emits the same ``Memory*``
events, so a run's trace shows its reads and writes.

* ``remember`` — one model call extracts zero or more facts (``extract``); each
  is embedded and handed to the store, which resolves its entities, skips what
  it already holds, and closes the value a single-valued fact replaces.
* ``recall`` — one embedding, one search: vector and keyword seeds fused by
  rank; standing preferences are put in front. Nothing is added that did
  not match the request. No model
  call on the read path.

Recalled facts are returned as ``MemoryRecord`` (kind ``semantic``), because
that is what every consumer of a recall already reads.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timezone

from src.core.events.bus import event_bus
from src.core.events.types import (
    MemoryQueryCompletedEvent,
    MemoryQueryFailedEvent,
    MemoryQueryStartedEvent,
    MemorySaveCompletedEvent,
    MemorySaveFailedEvent,
    MemorySaveStartedEvent,
)
from src.core.logger import LoggerManager
from src.services.memory.text import says_the_same, strip_run_boilerplate

from .extract import extract_turn
from .graph_types import (
    FACT_EPISODIC,
    FACT_FINDING,
    FACT_PROCEDURAL,
    ExtractedFact,
    Fact,
    GraphStore,
)
from .types import KIND_EPISODIC, KIND_PROCEDURAL, KIND_SEMANTIC, MemoryRecord

logger = logging.getLogger(__name__)
# One line per write in crew.log: without it, "nothing durable in this turn"
# and "the write never ran" look the same from outside.
run_log = LoggerManager.get_instance().crew

SaveHook = Callable[[list[MemoryRecord]], None]

# Facts shown to the extractor as already known (newest first).
_KNOWN_FACTS = 40

# One exchange can arrive twice: the chat agent persists its own turn, and the
# conversation recorder persists the same exchange once the answer settles.
# Record memory absorbed that with its duplicate search; here the second
# arrival is recognised by its text, before it costs a model call.
_SAME_TEXT = 0.9
_recent_texts: deque[tuple[str, str]] = deque(maxlen=64)
_recent_lock = threading.Lock()


def _seen_recently(scope: str, text: str) -> bool:
    """Whether ``text`` was already handed to this scope's memory; records it."""
    with _recent_lock:
        if any(
            seen_scope == scope and says_the_same(text, seen, _SAME_TEXT)
            for seen_scope, seen in _recent_texts
        ):
            return True
        _recent_texts.append((scope, text))
        return False


def clear_recent_texts() -> None:
    """Forget what was seen. For tests and process reuse."""
    with _recent_lock:
        _recent_texts.clear()


# Cosine a fact must reach to be recalled at all; run notes need more, and every
# match must also be close to the best one (see ``graph_types``: STRICT_KINDS,
# DEFAULT_RECALL_CLIFF). Measured on nomic-embed-text only. Memory Tuning
# ``relevance_threshold`` overrides it per teamspace.
DEFAULT_MIN_SIMILARITY = 0.52

# Same value ``Memory`` emits, so traces treat both stores alike.
_SOURCE_TYPE = "unified_memory"


def _dated(note: ExtractedFact) -> ExtractedFact:
    """A run note with today's date in front — stamped here, not by the model,
    which would write the year it remembers."""
    today = datetime.now(timezone.utc)
    return replace(note, statement=f"{today:%d %b %Y}: {note.object}")


# A graph kind as the record kind every recall consumer already reads. A
# finding is semantic memory; its own kind stays in the record's metadata.
_RECORD_KIND = {
    FACT_PROCEDURAL: KIND_PROCEDURAL,
    FACT_EPISODIC: KIND_EPISODIC,
}


def fact_to_record(
    fact: Fact, scope: str, similarity: float | None = None
) -> MemoryRecord:
    metadata: dict[str, object] = {
        **(fact.metadata or {}),
        "subject": fact.subject,
        "relation": fact.relation,
        "object": fact.object,
        "kind": fact.kind,
    }
    if similarity is not None:
        metadata["similarity"] = round(float(similarity), 4)
    record = MemoryRecord(
        id=fact.id,
        content=fact.statement,
        scope=scope,
        metadata=metadata,
        source=fact.source,
        kind=_RECORD_KIND.get(fact.kind, KIND_SEMANTIC),
        valid_to=fact.valid_to,
        superseded_by=fact.superseded_by,
    )
    if fact.created_at is not None:
        record.created_at = fact.created_at
    return record


class GraphMemory:
    """Entity/fact memory for one run. See the module docstring."""

    #: Lets the run layer skip record-only steps (the write-time duplicate
    #: search, the maintenance passes) without importing this class.
    extracts_facts = True

    def __init__(
        self,
        store: GraphStore,
        llm: object = None,
        root_scope: str | None = None,
        min_similarity: float | None = None,
        write_screening: str = "quarantine",
        read_only: bool = False,
    ):
        self.storage = store
        self.llm = llm
        self.root_scope = root_scope
        self.min_similarity = (
            DEFAULT_MIN_SIMILARITY if min_similarity is None else float(min_similarity)
        )
        self.write_screening = write_screening
        self.read_only = read_only
        self.save_hooks: list[SaveHook] = []

    # ------------------------------ writing ------------------------------

    def remember(
        self,
        content: str,
        scope: str | None = None,
        categories: list[str] | None = None,
        metadata: dict[str, object] | None = None,
        importance: float | None = None,
        source: str | None = None,
        private: bool = False,
        agent_role: str | None = None,
        root_scope: str | None = None,
        kind: str | None = None,
    ) -> MemoryRecord | None:
        """Extract the durable facts in ``content`` and store the new ones.

        Returns the first stored fact (``None`` when the text held nothing new)
        — the signature and return shape of ``Memory.remember``.
        """
        records = self._remember(content, metadata, source, agent_role)
        return records[0] if records else None

    def remember_many(
        self,
        contents: list[str],
        scope: str | None = None,
        categories: list[str] | None = None,
        metadata: dict[str, object] | None = None,
        importance: float | None = None,
        source: str | None = None,
        private: bool = False,
        agent_role: str | None = None,
        root_scope: str | None = None,
        kind: str | None = None,
    ) -> list[MemoryRecord]:
        records: list[MemoryRecord] = []
        for content in contents or []:
            records.extend(self._remember(content, metadata, source, agent_role))
        return records

    def _remember(
        self,
        content: str,
        metadata: dict[str, object] | None,
        source: str | None,
        agent_role: str | None,
    ) -> list[MemoryRecord]:
        text = " ".join((content or "").split())
        if self.read_only or not text:
            return []
        if _seen_recently(self.root_scope or "/", text):
            run_log.info(
                "[graph-memory] exchange already handled; skipped (%s)", source
            )
            return []
        base_metadata = dict(metadata or {})
        if agent_role:
            base_metadata.setdefault("agent_role", str(agent_role))
        stored: list[MemoryRecord] = []
        known = [
            fact
            for fact in self.storage.list_facts(limit=_KNOWN_FACTS * 2)
            if fact.kind != FACT_EPISODIC
        ][:_KNOWN_FACTS]
        extracted_facts, note = extract_turn(self.llm, text, known)
        # Facts where the run established some; a note only where it did not.
        if note and not any(f.kind == FACT_FINDING for f in extracted_facts):
            extracted_facts = [*extracted_facts, _dated(note)]
        # One embedding request for the whole write: an answer comparing a
        # dozen things should not cost a dozen round trips. A note is embedded
        # WITHOUT its date — every note shares that prefix, and shared text is
        # what makes unrelated records look alike.
        vectors = self._embed_all(
            [
                f.object if f.kind == FACT_EPISODIC else f.statement
                for f in extracted_facts
            ]
        )
        for extracted, vector in zip(extracted_facts, vectors):
            start = time.perf_counter()
            event_bus.emit(
                self,
                MemorySaveStartedEvent(
                    value=extracted.statement,
                    metadata=base_metadata,
                    agent_role=agent_role,
                    source_type=_SOURCE_TYPE,
                ),
            )
            try:
                fact = self.storage.add_fact(
                    extracted,
                    vector,
                    source=source,
                    metadata=(
                        {**base_metadata, "qualifiers": extracted.qualifiers}
                        if extracted.qualifiers
                        else base_metadata
                    ),
                )
            except Exception as exc:
                event_bus.emit(
                    self,
                    MemorySaveFailedEvent(
                        value=extracted.statement,
                        metadata=base_metadata,
                        agent_role=agent_role,
                        error=str(exc),
                        source_type=_SOURCE_TYPE,
                    ),
                )
                raise
            if fact is None:
                continue  # already known
            record = fact_to_record(fact, self.root_scope or "/")
            stored.append(record)
            event_bus.emit(
                self,
                MemorySaveCompletedEvent(
                    value=record.content,
                    metadata=record.metadata,
                    agent_role=agent_role,
                    save_time_ms=(time.perf_counter() - start) * 1000,
                    record_id=record.id,
                    source_type=_SOURCE_TYPE,
                ),
            )
        run_log.info(
            "[graph-memory] write (%s, run=%s): %d fact(s) extracted, %d stored, "
            "%d already known",
            source,
            base_metadata.get("execution_id"),
            len(extracted_facts),
            len(stored),
            len(extracted_facts) - len(stored),
        )
        if stored:
            for hook in self.save_hooks:
                try:
                    hook(stored)
                except Exception:
                    logger.exception("memory save hook %r failed", hook)
        return stored

    def _embed_all(self, texts: list[str]) -> list[list[float]]:
        """All of a write's embeddings in one request; one at a time if the
        embedder will not take a batch."""
        if not texts:
            return []
        try:
            return self.storage.embed_many(texts)
        except Exception:  # noqa: BLE001 — a batch failure must not lose the write
            logger.debug(
                "batch embedding failed; embedding one at a time", exc_info=True
            )
            return [self.storage.embed(text) for text in texts]

    def add_save_hook(self, hook: SaveHook) -> None:
        """Called with the facts a write stored — how a chat run's trace gets
        its "Memory Write" rows (see ``Memory.add_save_hook``)."""
        self.save_hooks.append(hook)

    # ------------------------------ reading ------------------------------

    def recall(
        self,
        query: str,
        limit: int = 10,
        scope: str | None = None,
        score_threshold: float | None = None,
        mode: str = "auto",
        report: bool = True,
    ) -> list[MemoryRecord]:
        """Standing preferences, then the facts that match ``query``, best first.

        ``score_threshold`` is the cosine floor for this call when given;
        ``scope`` and ``mode`` are accepted for ``Memory.recall`` parity — the
        store is already bound to its tenant, and there is only one mode.

        ``report=False`` leaves the result unannounced, for a caller that
        reports what it finally put in the prompt. A record store's pool and
        its selection differ, so a run's trace shows both ("Memory Search",
        "Memory Read"); here they are the same list, and two identical rows
        only made the trace look like two lookups.
        """
        floor = self.min_similarity if score_threshold is None else score_threshold
        start = time.perf_counter()
        event_bus.emit(
            self,
            MemoryQueryStartedEvent(
                query=query,
                limit=limit,
                score_threshold=floor,
                source_type=_SOURCE_TYPE,
            ),
        )
        try:
            # Symmetry with the write side: records never carry the run
            # scaffold, so the query must shed it too.
            text = strip_run_boilerplate(query) or query
            # Standing preferences and instructions come first, whatever the
            # request is about: "answer in tables" applies to a news briefing
            # as much as to a question that happens to mention tables.
            standing = self.storage.standing_facts()[: max(limit, 0)]
            found = self.storage.search(
                text,
                self.storage.embed(text),
                limit=limit - len(standing),
                min_similarity=floor,
            )
            always = {fact.id for fact in standing}
            hits: list[tuple[Fact, float | None]] = [
                *((fact, None) for fact in standing),
                *((fact, sim) for fact, sim in found if fact.id not in always),
            ]
        except Exception as exc:
            event_bus.emit(
                self,
                MemoryQueryFailedEvent(
                    query=query,
                    limit=limit,
                    score_threshold=floor,
                    error=str(exc),
                    source_type=_SOURCE_TYPE,
                ),
            )
            raise
        results = [
            fact_to_record(fact, self.root_scope or "/", similarity)
            for fact, similarity in hits
        ]
        if report:
            event_bus.emit(
                self,
                MemoryQueryCompletedEvent(
                    query=query,
                    results=results,
                    limit=limit,
                    score_threshold=floor,
                    query_time_ms=(time.perf_counter() - start) * 1000,
                    # What this returns is what the caller puts in its prompt —
                    # there is no later selection — so it is reported as the
                    # READ ("Memory Read", what a run's Recalled view lists).
                    # Reported as a search, a CrewAI agent recalling for itself
                    # left Recalled empty.
                    stage="selected",
                    source_type=_SOURCE_TYPE,
                ),
            )
        return results

    def list_records(
        self, scope: str | None = None, limit: int = 100, offset: int = 0
    ) -> list[MemoryRecord]:
        facts = self.storage.list_facts(limit=limit + offset, include_retired=True)
        return [fact_to_record(f, self.root_scope or "/") for f in facts[offset:]]

    def reset(self, scope: str | None = None) -> None:
        self.storage.reset()

    def close(self) -> None:
        self.storage.close()

    def __deepcopy__(self, memo: dict[int, object] | None = None) -> "GraphMemory":
        # A crew copies its agents; the store's connection must stay shared.
        return self
