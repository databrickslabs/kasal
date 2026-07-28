"""Stopping rules for knowledge search, so a fruitless search cannot loop.

A vector index always returns its top-k. When the knowledge base does not
contain the answer, it returns k *irrelevant* chunks — which read to an agent
exactly like k relevant ones. Observed: an agent asked for an expense policy
that had never been uploaded, got 20 unrelated chunks from a presales deck on
every attempt, rephrased the query 25 times and died on
"Tool-calling did not converge within 25 rounds".

Nothing in that loop was wrong except what the tool told the model. Three rules
fix it, and all three are about giving the model a reason to stop:

1. **Relevance floor** — results too far from the query are not returned at all,
   and the answer says so plainly.
2. **No repeats** — the same search twice returns what it returned the first
   time, not a fresh wall of text.
3. **A search budget** — after N searches the tool refuses, so a rephrasing
   carousel ends in an answer rather than a round-limit failure.
"""

import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, "") or default)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "") or default)
    except (TypeError, ValueError):
        return default


#: Cosine similarity a chunk must reach to be shown.
#:
#: Deliberately low. The cost of dropping a marginal chunk is an agent that says
#: "not in the knowledge base"; the cost of keeping noise is the loop above. It
#: is applied ONLY when the search actually produced scores, so a scoring
#: regression degrades to today's behaviour instead of silently returning nothing.
MIN_SCORE = _env_float("KNOWLEDGE_MIN_SCORE", 0.35)

#: Distinct searches one tool instance will serve for one agent's turn.
MAX_SEARCHES = _env_int("KNOWLEDGE_MAX_SEARCHES", 8)


def normalize_query(query: str) -> str:
    """Two searches that differ only in case or spacing are the same search."""
    return re.sub(r"\s+", " ", (query or "").strip().lower())


def _score_of(result: Dict[str, Any]) -> float:
    metadata = result.get("metadata") or {}
    try:
        return float(metadata.get("score") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def filter_by_relevance(
    results: List[Dict[str, Any]], min_score: float = MIN_SCORE
) -> Tuple[List[Dict[str, Any]], float, bool]:
    """Drop results below the floor.

    Returns (kept, best_score, scores_were_available). When no result carries a
    score — an unscored backend, or a scoring bug like the one that reported
    every chunk as 0.000 — everything is kept: a floor applied to absent data
    would return nothing for every query.
    """
    if not results:
        return [], 0.0, False

    scores = [_score_of(r) for r in results]
    best = max(scores) if scores else 0.0
    if best <= 0.0:
        return results, 0.0, False

    kept = [r for r, s in zip(results, scores) if s >= min_score]
    return kept, best, True


class KnowledgeSearchBudget:
    """Per-tool-instance memory of what has already been searched.

    A tool instance belongs to one agent for one run, which is the right scope:
    the budget resets between agents and between runs, and the agent that is
    looping is the one that gets stopped.
    """

    def __init__(self, max_searches: int = MAX_SEARCHES):
        self.max_searches = max_searches
        self._answers: Dict[str, str] = {}
        self._order: List[str] = []

    @property
    def searches_used(self) -> int:
        return len(self._order)

    def previous_answer(self, query: str) -> Optional[str]:
        """What this exact search returned last time, if it was already run."""
        return self._answers.get(normalize_query(query))

    def exhausted(self) -> bool:
        return self.max_searches > 0 and self.searches_used >= self.max_searches

    def record(self, query: str, answer: str) -> None:
        key = normalize_query(query)
        if key not in self._answers:
            self._order.append(key)
        self._answers[key] = answer

    def repeat_notice(self, query: str, answer: str) -> str:
        return (
            f'You already searched for "{query}". It returned the same thing it '
            f"returns now:\n\n{answer}\n\n"
            "Searching again will not produce anything new. Answer from what you "
            "have, or state that the knowledge base does not contain it."
        )

    def exhausted_notice(self) -> str:
        return (
            f"Search budget reached ({self.max_searches} searches for this task). "
            f"Previous queries: {', '.join(self._order)}.\n\n"
            "No further searches will run. Answer using the results you already "
            "received, or state plainly that the knowledge base does not contain "
            "the information."
        )


def no_relevant_results_notice(query: str, best_score: float, min_score: float = MIN_SCORE) -> str:
    """The answer when the index returned only distant matches.

    Says what was searched, how close the best match came, and — the part that
    ends the loop — that rephrasing is not expected to help.
    """
    closeness = (
        f"The closest match scored {best_score:.2f}, below the {min_score:.2f} "
        "relevance threshold."
        if best_score > 0
        else "Nothing in the knowledge base came close."
    )
    return (
        f'No relevant information found in the knowledge base for "{query}". '
        f"{closeness}\n\n"
        "The knowledge base does not appear to contain this. Rephrasing the query "
        "is unlikely to help — answer from what you already know, or state that "
        "the information is not available."
    )
