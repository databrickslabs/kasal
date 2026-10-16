"""Keyword fallback: pick a measure by name when the LLM could not answer.

Used by the DAX generators (Tools 72 and 80) as their last resort. It scores
each measure by how many of its name's words (three letters or more) appear
in the question, a word matching when one is a prefix of the other
("customer" / "customers"). The best score wins; with no overlap at all it
returns ``None`` rather than an arbitrary measure, so the run reports that it
could not answer instead of answering a different question.
"""

from __future__ import annotations

import re

# Read by the output formatters to label an answer that came from here.
KEYWORD_FALLBACK = "keyword_fallback"
KEYWORD_FALLBACK_NOTE = (
    "**Note**: this query came from the keyword fallback, not the LLM. It picks "
    "one measure by name and adds no grouping; check it answers the question "
    "before relying on the numbers.\n"
)


def _words(text: str) -> set[str]:
    return {w for w in re.findall(r"[a-z0-9]+", text.lower()) if len(w) >= 3}


def _same_word(a: str, b: str) -> bool:
    """Equal, or one a prefix of the other when both have 4+ letters."""
    if a == b:
        return True
    return min(len(a), len(b)) >= 4 and (a.startswith(b) or b.startswith(a))


def pick_measure(question: str, measures: list[dict]) -> dict | None:
    """The measure whose name shares the most words with the question."""
    asked = _words(question)
    best: dict | None = None
    best_score = 0
    for measure in measures:
        name_words = _words(str(measure.get("name", "")))
        score = sum(1 for w in name_words if any(_same_word(w, q) for q in asked))
        if score > best_score:
            best, best_score = measure, score
    return best
