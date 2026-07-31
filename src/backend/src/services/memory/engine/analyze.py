"""Memory analysis models — tolerant of malformed LLM JSON by design.

Authored module; surface validated against the kasal_engine datamodel.
Native requirement #5: kasal wrapped crewAI's analyze models with tolerant
variants (stringified-JSON coercion, malformed-metadata fallback); here the
tolerance is built in, so the wrapper dies. Every model coerces:

- a JSON *string* where an object/list is expected (LLMs love to nest
  stringified JSON) — parsed and used;
- malformed sub-objects — replaced by defaults instead of raising;
- out-of-range importance — clamped into [0, 1].
"""

import json
import logging
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .types import KIND_EPISODIC, MEMORY_KINDS

logger = logging.getLogger(__name__)


def _coerce_json(value: Any) -> Any:
    """If value is a string that parses as JSON, return the parsed value."""
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                return value
    return value


def _coerce_str_list(value: Any) -> list[str]:
    value = _coerce_json(value)
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if item is not None]
    return [str(value)]


class ExtractedMetadata(BaseModel):
    """Fixed schema for LLM-extracted metadata."""

    model_config = ConfigDict(extra="ignore")

    entities: list[str] = Field(default_factory=list)
    dates: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)

    @field_validator("entities", "dates", "topics", mode="before")
    @classmethod
    def _tolerant_lists(cls, value: Any) -> list[str]:
        return _coerce_str_list(value)


class MemoryAnalysis(BaseModel):
    """LLM output for analyzing content before saving to memory."""

    model_config = ConfigDict(extra="ignore")

    suggested_scope: str = "/"
    categories: list[str] = Field(default_factory=list)
    importance: float = 0.5
    kind: str = KIND_EPISODIC
    extracted_metadata: ExtractedMetadata = Field(default_factory=ExtractedMetadata)

    @field_validator("categories", mode="before")
    @classmethod
    def _tolerant_categories(cls, value: Any) -> list[str]:
        return _coerce_str_list(value)

    @field_validator("kind", mode="before")
    @classmethod
    def _tolerant_kind(cls, value: Any) -> str:
        """Anything the model did not say clearly reads as episodic.

        Episodic is the safe default in both directions: it decays, and it never
        claims to be a currently-true fact that could supersede a real one.
        """
        text = str(value or "").strip().lower()
        return text if text in MEMORY_KINDS else KIND_EPISODIC

    @field_validator("importance", mode="before")
    @classmethod
    def _clamp_importance(cls, value: Any) -> float:
        try:
            return min(1.0, max(0.0, float(value)))
        except (TypeError, ValueError):
            return 0.5

    @field_validator("extracted_metadata", mode="before")
    @classmethod
    def _tolerant_metadata(cls, value: Any) -> Any:
        value = _coerce_json(value)
        if isinstance(value, (dict, ExtractedMetadata)):
            return value
        if value is not None:
            logger.warning("dropping malformed extracted_metadata: %.200r", value)
        return ExtractedMetadata()


class QueryAnalysis(BaseModel):
    """LLM output for analyzing a recall query."""

    model_config = ConfigDict(extra="ignore")

    keywords: list[str] = Field(default_factory=list)
    suggested_scopes: list[str] = Field(default_factory=list)
    complexity: str = "simple"
    recall_queries: list[str] = Field(default_factory=list)
    time_filter: str | None = None

    @field_validator("keywords", "suggested_scopes", "recall_queries", mode="before")
    @classmethod
    def _tolerant_lists(cls, value: Any) -> list[str]:
        return _coerce_str_list(value)

    @field_validator("complexity", mode="before")
    @classmethod
    def _tolerant_complexity(cls, value: Any) -> str:
        return value if value in ("simple", "complex") else "simple"


class ExtractedMemories(BaseModel):
    """LLM output for extracting discrete memories from raw content."""

    model_config = ConfigDict(extra="ignore")

    memories: list[str] = Field(default_factory=list)

    @field_validator("memories", mode="before")
    @classmethod
    def _tolerant_memories(cls, value: Any) -> list[str]:
        return _coerce_str_list(value)


class ConsolidationAction(BaseModel):
    """A single action in a consolidation plan."""

    model_config = ConfigDict(extra="ignore")

    action: str = "keep"
    record_ids: list[str] = Field(default_factory=list)
    merged_content: str | None = None

    @field_validator("record_ids", mode="before")
    @classmethod
    def _tolerant_ids(cls, value: Any) -> list[str]:
        return _coerce_str_list(value)


class ConsolidationPlan(BaseModel):
    """A consolidation plan over memory records."""

    model_config = ConfigDict(extra="ignore")

    actions: list[ConsolidationAction] = Field(default_factory=list)

    @field_validator("actions", mode="before")
    @classmethod
    def _tolerant_actions(cls, value: Any) -> Any:
        value = _coerce_json(value)
        if isinstance(value, list):
            kept = []
            for item in value:
                item = _coerce_json(item)
                if isinstance(item, (dict, ConsolidationAction)):
                    kept.append(item)
                else:
                    logger.warning(
                        "dropping malformed consolidation action: %.100r", item
                    )
            return kept
        return []
