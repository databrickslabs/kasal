"""Run-scaffold boilerplate, stripped from memory text on BOTH sides.

Every run wraps the user's request in the same grounding phrases ("Respond
directly and helpfully…", "USER REQUEST — this run exists to answer it:",
"Expected output: A helpful, complete answer…", the attached-tools hint).
Stored records used to carry them AND recall queries still do — and the two
must be treated symmetrically: stripping only the stored side (as first
shipped) made a scaffolded query score LOW against its own clean record, and
four consecutive live runs recalled zero over a store full of matches.

One module owns the pattern so the write path (hooks.format_turn_for_memory)
and the read path (EngineStorageAdapter.search) can never disagree.
"""

import re

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
