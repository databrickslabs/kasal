"""A run's output in a shape both protocols can render.

MCP returns a tool result; A2A returns an ``Artifact`` composed of ``Part``s.
The shaping decision — what counts as prose, what counts as structured data,
what is a file reference — is the same in both, so it is made once here and each
adapter only maps the field names.

Deliberately small. If an adapter starts reshaping what this returns, the shape
was wrong, not the adapter.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ArtifactPart:
    """One piece of a result.

    ``kind`` follows A2A's ``Part`` vocabulary — text / data / url — because
    that is the published standard, and MCP has no equivalent to borrow from.
    """

    kind: str  # "text" | "data" | "url"
    content: Any

    def as_dict(self) -> Dict[str, Any]:
        return {"kind": self.kind, self.kind: self.content}


@dataclass(frozen=True)
class Artifact:
    """A run's output, ready for either adapter to render."""

    parts: List[ArtifactPart] = field(default_factory=list)

    @property
    def text(self) -> Optional[str]:
        """The prose part, which is what a human-facing client shows."""
        for part in self.parts:
            if part.kind == "text":
                return part.content
        return None

    def as_dict(self) -> Dict[str, Any]:
        return {"parts": [p.as_dict() for p in self.parts]}


def build(raw_result: Any) -> Artifact:
    """Shape whatever a run produced into parts.

    A crew result is not one thing: it can be prose, a JSON envelope, a dict the
    engine wrapped, or a string that happens to contain JSON. Callers get both
    the readable text AND the structured form when both exist, rather than this
    layer guessing which one they wanted.
    """
    if raw_result is None:
        return Artifact(parts=[])

    if isinstance(raw_result, dict):
        # The engine's own wrapper: prose under `raw`/`content`, everything else
        # is structure worth keeping.
        prose = raw_result.get("raw") or raw_result.get("content")
        parts: List[ArtifactPart] = []
        if isinstance(prose, str) and prose.strip():
            parts.append(ArtifactPart("text", prose))
        parts.append(ArtifactPart("data", raw_result))
        return Artifact(parts=parts)

    if isinstance(raw_result, (list, tuple)):
        return Artifact(parts=[ArtifactPart("data", list(raw_result))])

    text = str(raw_result)
    parts = [ArtifactPart("text", text)]

    # A string that parses as JSON is offered as data TOO, not instead: the
    # caller may want either, and discarding the prose form to "helpfully"
    # return structure is how a readable answer disappears.
    stripped = text.strip()
    if stripped.startswith(("{", "[")):
        try:
            parts.append(ArtifactPart("data", json.loads(stripped)))
        except (ValueError, TypeError):
            pass

    return Artifact(parts=parts)
