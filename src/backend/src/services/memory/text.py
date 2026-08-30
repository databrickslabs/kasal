"""Text normalisation shared by every memory layer.

Two things both the read side and the write side need, and the storage
adapter too, so they live below all three:

* ``strip_run_boilerplate`` — the run-grounding scaffold every prompt shares,
  removed on BOTH sides so records embed what the user asked and queries
  embed what the task is for.
* ``says_the_same`` — token-set Jaccard, the model-free "one recollection
  said twice" test that recall uses to drop echoing snippets and persist uses
  to skip duplicate writes."""

from __future__ import annotations

import re
from typing import Any

_PATTERNS = [
    r"Respond directly and helpfully to the user's request\.?\s*",
    r"USER REQUEST — this run exists to answer it:\s*",
    r"MCP data sources attached — query them for data questions\.?\s*",
    r"Expected output: A helpful, complete answer to the user's request\.?\s*",
    # The attached-tools hint (" : browser Expected output…") — only the chunk
    # sandwiched before "Expected output:", so a user's own colon survives.
    r"\s*:\s*[a-z0-9_,\- ]{1,80}(?=\s*Expected output:)",
]

RUN_BOILERPLATE = re.compile("|".join(_PATTERNS))


def strip_run_boilerplate(text: str) -> str:
    """Remove the run scaffold; collapse the whitespace it leaves behind."""
    cleaned = RUN_BOILERPLATE.sub("", text or "")
    return " ".join(cleaned.split())


def normalized_text(value: Any) -> str:
    return " ".join(str(getattr(value, "content", "") or "").split())


#: Below this many distinct tokens, only exact equality counts. Short strings
#: overlap by accident — "deadline is Friday" and "deadline is Monday" share two
#: tokens of three and are opposite facts. Graphiti gates its shingle comparison
#: the same way, for the same reason.
_MIN_TOKENS = 8


def tokens(text: str) -> frozenset:
    return frozenset(text.lower().split())


def says_the_same(left: str, right: str, threshold: float) -> bool:
    """Whether two normalized texts are one recollection said twice."""
    if not left or not right:
        return False
    if left == right:
        return True
    a, b = tokens(left), tokens(right)
    if min(len(a), len(b)) < _MIN_TOKENS:
        return False
    union = len(a | b)
    return bool(union) and len(a & b) / union >= threshold
