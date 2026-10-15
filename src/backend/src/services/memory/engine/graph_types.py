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

The data model follows the Graphiti project (Apache-2.0; see src/docs/MEMORY.md):
typed entities, fact edges with a validity window, invalidate-not-delete.
"""

from __future__ import annotations

import json
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
#: Kinds that need the stricter match: a note recalled for an unrelated request
#: is a false memory of what a run did. Facts — the user's and the ones runs
#: found — share the base floor (``GraphMemory`` default 0.52) and the cut
#: below. Measured on nomic-embed-text: notes from other runs reached 0.54
#: against an unrelated request, right notes 0.64 and up.
STRICT_KINDS = (FACT_EPISODIC,)
DEFAULT_STRICT_MIN_SIMILARITY = 0.6
#: Keep only matches within this of the best one. A fixed floor alone cannot
#: separate right from wrong on this embedder: right facts for "what was the
#: most effective marketing campaign" scored 0.53-0.60 while wrong facts for
#: other requests scored up to 0.62. Relative to the best match they separate:
#: on every case measured that day (21 right facts, 12 requests) floor + cut
#: kept 15 right facts where fixed floors per kind kept 12, with the same
#: number of wrong ones let through.
DEFAULT_RECALL_CLIFF = 0.09
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
    #: What qualifies the relation without being a thing itself: a role, a
    #: period, a field of study. Kept off the entity's name, where it would
    #: turn "AWS" into a different node per job.
    qualifiers: dict[str, str] = field(default_factory=dict)


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

    def embed_many(self, texts: list[str]) -> list[list[float]]: ...

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

    def retire_fact(self, fact_id: str) -> bool: ...

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


def qualifiers_of(metadata: object) -> dict[str, str]:
    """The qualifiers stored in a fact's metadata (JSON text or a dict)."""
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata or "{}")
        except ValueError:
            return {}
    value = metadata.get("qualifiers") if isinstance(metadata, dict) else None
    if not isinstance(value, dict):
        return {}
    return {str(key): str(item) for key, item in value.items()}


def plan_write(
    current: Sequence[tuple[str, str, object]],
    object_key: str,
    qualifiers: dict[str, str],
    single: bool,
) -> tuple[bool, list[str]]:
    """Whether to store a new fact, and which current facts it retires.

    ``current`` is ``(id, object_key, metadata)`` for the current facts with the
    new fact's subject and relation. Shared by both stores so they cannot drift.

    * Same value, same qualifiers — or the new one has none — is a repeat:
      nothing is written, and a terser restatement never wipes the detail.
    * Same value, different qualifiers is a correction ("Spring Promo, ROAS
      4.36x" re-measured): the new fact replaces the old one.
    * A single-valued relation replaces whatever value it held.
    """
    same = [(fid, meta) for fid, key, meta in current if key == object_key]
    if same and (
        not qualifiers or any(qualifiers_of(meta) == qualifiers for _, meta in same)
    ):
        return False, []
    return True, [fid for fid, key, _ in current if single or key == object_key]


def rank_hits(
    vector_hits: list[tuple[str, float]],
    keyword_ids: list[str],
    limit: int,
    min_similarity: float,
    cliff: float = DEFAULT_RECALL_CLIFF,
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
    # ...and must be close to the best match: nearest is not near, and neither
    # is "above the floor" when something else matches far better.
    best = max(similarity.values())
    similarity = {fid: sim for fid, sim in similarity.items() if sim >= best - cliff}
    vector_ranked = [fid for fid, _ in vector_hits if fid in similarity]
    keyword_ranked = [fid for fid in keyword_ids if fid in similarity]
    order = fuse(vector_ranked, keyword_ranked)
    return [(fid, similarity[fid]) for fid in order[: max(limit, 0)]]
