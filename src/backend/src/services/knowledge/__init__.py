"""Uploaded knowledge: embedding it, storing it, searching it.

Five files sharing a name prefix in ``src/services``, plus a search capability
that was trapped inside an agent tool. They are one subsystem: a file is
uploaded, chunked and embedded, stored per workspace and per uploader, then
searched — and the searching is worth reaching for outside a crew run (crew
generation researching before it plans, a chat turn answering from an attached
file, an exported app).

``KnowledgeSearch`` is that capability. ``DatabricksKnowledgeSearchTool`` is now
a thin agent-facing wrapper around it, not its owner.
"""

from src.services.knowledge.search import KnowledgeSearch
from src.services.knowledge.search_guard import (
    KnowledgeSearchBudget,
    filter_by_relevance,
    no_relevant_results_notice,
)

__all__ = [
    "KnowledgeSearch",
    "KnowledgeSearchBudget",
    "filter_by_relevance",
    "no_relevant_results_notice",
]
