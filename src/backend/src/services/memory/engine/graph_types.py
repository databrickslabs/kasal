"""Graph memory data model — entities, the facts between them, and the pure
helpers both graph stores share.

A fact is ``subject --relation--> object`` plus the one sentence that says it.
The sentence is what is embedded and keyword-indexed; the triple is what makes
"a new value replaces the old one" a lookup instead of a model call. Facts are
never deleted when replaced: ``valid_to`` is closed and ``superseded_by`` names
the winner, so history stays answerable.

The vocabulary is PRESCRIBED and small. A model left to invent types and
relations sprawls (one 49-record store carried 246 distinct tags); the
extractor is steered to these and anything else collapses onto a default.

The data model follows Graphiti (github.com/getzep/graphiti, Apache-2.0):
typed entities, fact edges with a validity window, invalidate-not-delete.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

ENTITY_TYPES = (
    "person",
    "organization",
    "system",
    "dataset",
    "project",
    "place",
    "concept",
)
DEFAULT_ENTITY_TYPE = "concept"

#: The entity every fact about the person talking is attached to.
USER_ENTITY = "user"

# Seed relations -> whether a subject can hold only ONE current value. The flag
# here is authoritative for these names; for a relation the extractor coins, its
# own ``single`` answer is used.
RELATIONS: dict[str, bool] = {
    "name_is": True,
    "role_is": True,
    "works_at": True,
    "located_in": True,
    "deadline_is": True,
    "status_is": True,
    "owned_by": True,
    "prefers": False,
    "uses": False,
    "part_of": False,
    "responsible_for": False,
    "instruction": False,
}

#: Provenance carried on a fact (run id, task name, writing agent).
Metadata = dict[str, object]

# ── Kind: what sort of memory an item is ─────────────────────────────────────
# The ontology (entity types, relations) says what things ARE. Kind is the other
# axis: it decides where an item may come from, when it is recalled and how
# long it lives. One policy per kind, applied by both stores:
#
#   kind        holds                                  comes from      recalled
#   procedural  standing preferences / instructions    the user        always
#   semantic    facts about the user and their work    the user        on a match
#   finding     facts a run established about a        the run's       on a STRONG
#               named entity (semantic memory, but     answer          match
#               only as good as the source it read)
#   episodic    one note per run: what it produced     the run         on a STRONG
#                                                                       match, and
#                                                                       it expires
FACT_PROCEDURAL = "procedural"
FACT_SEMANTIC = "semantic"
FACT_FINDING = "finding"
FACT_EPISODIC = "episodic"
FACT_KINDS = (FACT_PROCEDURAL, FACT_SEMANTIC, FACT_FINDING, FACT_EPISODIC)

#: Kinds the user alone can be the source of.
USER_KINDS = (FACT_PROCEDURAL, FACT_SEMANTIC)
#: Kinds that need the stricter match. A wrong fact about the user is harmless
#: context; a finding or a note recalled for an unrelated request is a false
#: memory. Measured on nomic-embed-text: right matches 0.61-0.84, wrong ones at
#: most 0.57 (a dataset finding against a Swiss-news request).
STRICT_KINDS = (FACT_FINDING, FACT_EPISODIC)
DEFAULT_STRICT_MIN_SIMILARITY = 0.6
#: Kinds the one-hop expansion starts from and returns.
HOP_KINDS = (FACT_SEMANTIC, FACT_FINDING)
#: Procedural facts put in front of every recall.
MAX_STANDING = 5

#: The relation every run note carries. Reserved: the extractor cannot use it.
NOTE_RELATION = "run_note"
DEFAULT_NOTE_RETENTION_DAYS = 180.0


def fact_kind(value: object) -> str:
    """Unknown or missing kinds read as semantic — every row written before
    the column existed was a fact."""
    text = str(value or "").strip().lower()
    return text if text in FACT_KINDS else FACT_SEMANTIC


MAX_OBJECT_CHARS = 200
MAX_STATEMENT_CHARS = 400
MAX_RELATION_CHARS = 40


@dataclass
class ExtractedFact:
    """One fact as the extractor returned it, before entities are resolved."""

    subject: str
    relation: str
    object: str
    statement: str
    subject_type: str = DEFAULT_ENTITY_TYPE
    object_type: str | None = None  # set when the object is itself an entity
    single: bool = False
    kind: str = FACT_SEMANTIC


@dataclass
class Entity:
    """One node of the graph."""

    id: str
    name: str
    type: str = DEFAULT_ENTITY_TYPE


@dataclass
class Fact:
    """One stored fact."""

    id: str
    subject_id: str
    relation: str
    object: str
    statement: str
    subject: str = ""
    object_id: str | None = None
    source: str | None = None
    metadata: Metadata = field(default_factory=dict)
    created_at: datetime | None = None
    valid_to: datetime | None = None
    superseded_by: str | None = None
    kind: str = FACT_SEMANTIC


class GraphStore(Protocol):
    """What ``GraphMemory`` needs from a store. ``LocalGraphStore`` (SQLite)
    and ``LakebaseGraphStore`` (Postgres) implement it."""

    def embed(self, text: str) -> list[float]: ...

    def add_fact(
        self,
        fact: ExtractedFact,
        embedding: list[float] | None,
        *,
        source: str | None = None,
        metadata: Metadata | None = None,
    ) -> Fact | None: ...

    def search(
        self,
        query_text: str,
        query_embedding: list[float] | None,
        limit: int = 10,
        min_similarity: float = 0.0,
    ) -> list[tuple[Fact, float | None]]: ...

    def list_facts(
        self, limit: int = 100, include_retired: bool = False
    ) -> list[Fact]: ...

    def standing_facts(self, limit: int = MAX_STANDING) -> list[Fact]: ...

    def list_entities(self, limit: int = 500) -> list[Entity]: ...

    def count(self) -> int: ...

    def reset(self) -> None: ...

    def close(self) -> None: ...


def entity_key(name: object) -> str:
    """The key two spellings of one entity share ("Zürich" / "zurich ")."""
    folded = unicodedata.normalize("NFKD", str(name or ""))
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    return " ".join(re.sub(r"[^\w]+", " ", folded.lower()).split())


def relation_key(name: object) -> str:
    """Relations are lowercase snake_case, bounded."""
    slug = re.sub(r"[^a-z0-9]+", "_", str(name or "").strip().lower()).strip("_")
    return slug[:MAX_RELATION_CHARS].strip("_")


def entity_type(value: object) -> str:
    text = str(value or "").strip().lower()
    return text if text in ENTITY_TYPES else DEFAULT_ENTITY_TYPE


def is_single_valued(relation: str, claimed: bool) -> bool:
    return RELATIONS.get(relation, bool(claimed))


_STOPWORDS = frozenset(
    "the and for that this with from have has are was were what which when "
    "where who how does did can could should would about into your you our "
    "their there then than them they been will not but any all".split()
)
_MAX_KEYWORDS = 16


def keyword_tokens(text: str) -> list[str]:
    """The distinct words of ``text`` worth a keyword search, in order.

    Both stores build their full-text query from these, OR-ed: a long query
    AND-ed term by term matches nothing. Word characters only, so a token can
    be placed in an FTS5 or tsquery expression without escaping.
    """
    seen: list[str] = []
    for token in re.findall(r"[^\W_]{3,}", str(text or "").lower()):
        if token not in _STOPWORDS and token not in seen:
            seen.append(token)
        if len(seen) >= _MAX_KEYWORDS:
            break
    return seen


_RRF_K = 60


def fuse(*rankings: list[str]) -> list[str]:
    """Reciprocal rank fusion of ranked id lists, best first.

    Ranks, not scores: a cosine and a BM25 value are not comparable, and
    blending them needs weights calibrated per embedder. Positions need none.
    """
    scores: dict[str, float] = {}
    for ranking in rankings:
        for position, item in enumerate(ranking):
            scores[item] = scores.get(item, 0.0) + 1.0 / (_RRF_K + position + 1)
    first_seen = {item: index for index, item in enumerate(scores)}
    return sorted(scores, key=lambda item: (-scores[item], first_seen[item]))


def drop_weak(
    hits: list[tuple[str, float, str]], strict_min_similarity: float
) -> list[tuple[str, float]]:
    """``(id, cosine, kind)`` hits without the findings and run notes below
    their own, stricter floor. Order is kept. Shared by both stores."""
    return [
        (fact_id, similarity)
        for fact_id, similarity, kind in hits
        if kind not in STRICT_KINDS or similarity >= strict_min_similarity
    ]


def hop_entities(seeds: Sequence[Fact]) -> set[str]:
    """The entities worth expanding from ``seeds`` — never the user, and never
    from a run note.

    Nearly every fact touches the ``user`` entity, so following it is not
    "related facts", it is the whole store: a request for Nestlé's revenue
    once came back with facts about an unrelated project that way. A note says
    what a run produced; what it mentions is not what the request is about.
    """
    entities: set[str] = set()
    for fact in seeds:
        if fact.kind not in HOP_KINDS:
            continue
        if entity_key(fact.subject) != USER_ENTITY:
            entities.add(fact.subject_id)
        if fact.object_id and entity_key(fact.object) != USER_ENTITY:
            entities.add(fact.object_id)
    return entities


def rank_hits(
    vector_hits: list[tuple[str, float]],
    keyword_ids: list[str],
    limit: int,
    min_similarity: float,
) -> list[tuple[str, float]]:
    """The seed facts of a search, best first, as ``(fact_id, similarity)``.

    ``vector_hits`` is ``(id, cosine)`` best first; ``keyword_ids`` is the
    full-text ranking. One stopping rule: a fact must clear ``min_similarity``
    to be a seed at all — nearest is not near, and a keyword hit on a common
    word must not carry an unrelated fact into a prompt. Among the facts that
    clear it, the two rankings are fused, so an exact name the embedding blurs
    still rises.

    Shared by both stores so their recall cannot drift apart.
    """
    similarity = {fid: sim for fid, sim in vector_hits if sim >= min_similarity}
    if not similarity:
        return []
    vector_ranked = [fid for fid, _ in vector_hits if fid in similarity]
    keyword_ranked = [fid for fid in keyword_ids if fid in similarity]
    order = fuse(vector_ranked, keyword_ranked)
    return [(fid, similarity[fid]) for fid in order[: max(limit, 0)]]
