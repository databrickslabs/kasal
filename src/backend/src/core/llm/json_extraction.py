"""
Pulling a JSON object out of an LLM response.

Models fence their JSON, prefix it with prose, or emit it bare. This is the
tolerant reader for all three, and it is PURE — no engine, no run, no I/O.

It lived in the agent runtime's executor, which meant ``core/llm/transport``
had to import the runtime to parse its own structured output: the LLM layer
depending on the agent layer for string handling.
"""

import json
import re
from typing import Any

#: ```json { ... } ``` — the common fenced form.
_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)


def extract_json_dict(text: str) -> dict[str, Any] | None:
    """Pull the first JSON object out of an LLM response, tolerantly."""
    candidates = _JSON_FENCE_RE.findall(text)
    if not candidates:
        start = text.find("{")
        if start != -1:
            depth = 0
            for i in range(start, len(text)):
                if text[i] == "{":
                    depth += 1
                elif text[i] == "}":
                    depth -= 1
                    if depth == 0:
                        candidates = [text[start : i + 1]]
                        break
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None
