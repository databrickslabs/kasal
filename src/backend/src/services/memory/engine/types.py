"""Memory data models (records, scope info).

Originally generated from the kasal_engine datamodel. That package is now
first-party code in this repo and there is no generator here, so this module is
maintained by hand.

``MemoryRecord`` carries the TYPE of a memory and, for facts, its
validity window. Both exist because episodic and semantic memories want
opposite retrieval policies: "what happened in run 47" is time-anchored, high
volume and should fade; "the user prefers Databricks SQL" is atemporal, low
volume, and must stay current — and must be RETIRED when it stops being true
rather than merely out-ranked by something newer.
"""

import json
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal, Self
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Memory kinds. Deliberately plain strings rather than an Enum: these cross
# the JSONB/SQLite/pydantic boundary constantly, and a ``str, Enum`` renders as
# "MemoryKind.EPISODIC" under f-strings on 3.11+ while ``.value`` on an already-
# coerced value is the exact footgun the memory backends have been bitten by
# before. A validator below keeps the field to these three.
KIND_EPISODIC = "episodic"
KIND_SEMANTIC = "semantic"
KIND_PROCEDURAL = "procedural"
MEMORY_KINDS = (KIND_EPISODIC, KIND_SEMANTIC, KIND_PROCEDURAL)


class MemoryRecord(BaseModel):
    """One memory. Episodic by default; facts carry a validity window."""

    id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique identifier for the memory record.",
    )
    content: str = Field(description="The textual content of the memory.")
    scope: str = Field(
        default="/",
        description="Hierarchical path organizing the memory (e.g. /company/team/user).",
    )
    categories: list[str] = Field(
        default_factory=list,
        description="Categories or tags for the memory.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary metadata associated with the memory.",
    )
    importance: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Importance score from 0.0 to 1.0, affects retrieval ranking.",
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When the memory was created.",
    )
    last_accessed: datetime = Field(
        default_factory=datetime.utcnow,
        description="When the memory was last accessed.",
    )
    embedding: list[float] | None = Field(
        default=None,
        exclude=True,
        repr=False,
        description="Vector embedding for semantic search. Excluded from serialization to save tokens.",
    )
    source: str | None = Field(
        default=None,
        description=(
            "Origin of this memory (e.g. user ID, session ID). "
            "Used for provenance tracking and privacy filtering."
        ),
    )
    private: bool = Field(
        default=False,
        description=(
            "If True, this memory is only visible to recall requests from the same source, "
            "or when include_private=True is passed."
        ),
    )
    kind: str = Field(
        default=KIND_EPISODIC,
        description=(
            "Memory type: 'episodic' (what happened, and when), 'semantic' "
            "(what is currently true), or 'procedural' (how to do something). "
            "Drives recall policy — episodic decays with age, semantic does not."
        ),
    )
    valid_from: datetime | None = Field(
        default=None,
        description=(
            "When this fact STARTED being true in the world. Semantic records "
            "only; distinct from created_at, which is when the system recorded it."
        ),
    )
    valid_to: datetime | None = Field(
        default=None,
        description=(
            "When this fact STOPPED being true. None means currently valid. "
            "Recall returns only currently-valid records; history is retained so "
            "'what did we believe on date X' stays answerable."
        ),
    )
    superseded_by: str | None = Field(
        default=None,
        description="Id of the record that replaced this one, when retired.",
    )

    @field_validator("kind", mode="before")
    @classmethod
    def _known_kind(cls, value: Any) -> str:
        """Unknown or missing kinds read as episodic.

        Every record written before this field existed comes back without it,
        and episodic is the safe reading: it decays and never claims to be a
        current fact.
        """
        text = str(value or "").strip().lower()
        return text if text in MEMORY_KINDS else KIND_EPISODIC

    @property
    def is_current(self) -> bool:
        """True when this record has not been retired."""
        return self.valid_to is None

    # ── CrewAI MemoryMatch surface ─────────────────────────────────────────
    # On the CrewAI engine this Memory is attached straight to a LiteAgent,
    # whose recall consumers iterate crewai.memory.types.MemoryMatch objects:
    # ``m.record.content`` / ``m.record.id`` (lite_agent._inject_memory_context,
    # tools/memory_tools dedupe) and ``m.format()``. Kasal's recall() returns
    # the records themselves — without this surface every one of those sites
    # raised AttributeError inside crewai's broad try/except, so recalled
    # memory was retrieved and then silently NEVER injected into the prompt
    # (observed live: three recalls carrying the right fact, zero of them in
    # any LLM request). The record acts as its own match.

    @property
    def record(self) -> "MemoryRecord":
        return self

    @property
    def score(self) -> float:
        value = (self.metadata or {}).get("similarity")
        return float(value) if isinstance(value, (int, float)) else 0.0

    def format(self) -> str:
        lines = [f"- (score={self.score:.2f}) {self.content}"]
        if self.categories:
            lines.append(f"  categories: {', '.join(self.categories)}")
        return "\n".join(lines)


class ScopeInfo(BaseModel):
    """Engine replacement for crewai.memory.types.ScopeInfo"""

    path: str = Field(description="The scope path (e.g. /company/engineering).")
    record_count: int = Field(
        default=0,
        description="Number of records in this scope (including subscopes if applicable).",
    )
    categories: list[str] = Field(
        default_factory=list,
        description="Categories used in this scope.",
    )
    oldest_record: datetime | None = Field(
        default=None,
        description="Timestamp of the oldest record in this scope.",
    )
    newest_record: datetime | None = Field(
        default=None,
        description="Timestamp of the newest record in this scope.",
    )
    child_scopes: list[str] = Field(
        default_factory=list,
        description="Immediate child scope paths.",
    )
