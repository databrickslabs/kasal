"""The ``Memory`` object — remember, recall, and the record they share.

* ``memory`` — ``Memory``: ``remember`` labels a record, folds it into a
  near-duplicate when one exists, and saves it; ``recall`` searches, and per
  the Memory Tuning knobs distils long queries and explores alternatives.
  Every tuning knob is a declared field here. ``StorageBackend`` is the
  protocol a store implements; ``InMemoryStorage`` is the test double.
* ``types`` — ``MemoryRecord`` (kind, validity window, provenance) and
  ``ScopeInfo``.
* ``analyze`` — the save-time analysis model the memory LLM fills
  (categories, importance, kind, entities), tolerant of malformed JSON.
* ``recall_planner`` — query distillation and exploration rounds.
* ``consolidation`` — the save-time merge into a near-duplicate.
"""

from .analyze import ExtractedMetadata, MemoryAnalysis
from .memory import InMemoryStorage, Memory, StorageBackend
from .types import (
    KIND_EPISODIC,
    KIND_PROCEDURAL,
    KIND_SEMANTIC,
    MEMORY_KINDS,
    MemoryRecord,
    ScopeInfo,
)

__all__ = [
    "KIND_EPISODIC",
    "KIND_PROCEDURAL",
    "KIND_SEMANTIC",
    "MEMORY_KINDS",
    "ExtractedMetadata",
    "InMemoryStorage",
    "Memory",
    "MemoryAnalysis",
    "MemoryRecord",
    "ScopeInfo",
    "StorageBackend",
]
