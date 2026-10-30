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

import json
import re
from typing import Any

_PATTERNS = [
    r"Respond directly and helpfully to the user's request\.?\s*",
    r"USER REQUEST — this run exists to answer it:\s*",
    r"ANSWER FROM THE CONVERSATION — the transcript above already holds what this turn needs:\s*",
    r"Restate or reshape that answer as asked\. Use a tool only for something the transcript does not contain; do not research the question again\.?\s*",
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


def readable_output(text: str) -> str:
    """A run's output made readable for extraction, when it is JSON.

    Tools — an MCP server, a Genie space, an API — often return JSON, and an
    agent may hand it back as its answer. Read as text, most of it is ids, nulls
    and escape sequences (``\\n``, ``\\u00e9``), and the substance — a
    ``final_answer`` holding a results table — starts late. Rendered as
    ``key: value`` lines with empty values dropped and nested JSON strings
    opened, the same content is shorter and says what it says. Nothing here
    knows any tool's field names. Anything that is not JSON is returned as is.
    """
    stripped = (text or "").strip()
    if not stripped or stripped[0] not in "{[":
        return text
    try:
        data = json.loads(stripped)
    except ValueError:
        return text
    lines: list[str] = []

    def walk(value: object, path: str) -> None:
        if value is None or value == "" or value == [] or value == {}:
            return
        if isinstance(value, dict):
            for key, item in value.items():
                walk(item, f"{path}.{key}" if path else str(key))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                walk(item, f"{path}[{index}]")
        elif isinstance(value, str) and value.strip()[:1] in ("{", "["):
            try:
                walk(json.loads(value), path)
            except ValueError:
                lines.append(f"{path}: {value}")
        else:
            lines.append(f"{path}: {value}" if path else str(value))

    walk(data, "")
    return "\n".join(lines) or text
