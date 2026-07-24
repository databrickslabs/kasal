"""Memory data models (records, scope info).

Generated from the kasal_engine datamodel — do not edit by hand.
Edit the component/component_member rows and re-run generator/generate.py.
"""

import json
import uuid
from uuid import uuid4
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal, Self
from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class MemoryRecord(BaseModel):
    """Engine replacement for crewai.memory.types.MemoryRecord"""

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
