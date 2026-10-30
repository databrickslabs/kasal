"""Find the JSON payload a crew handed off, inside the agent's narrative.

A crew's output often wraps its tool payload in prose ("Here is the full output:
{...}"). The flow handoff needs that payload whole: a Pipeline Config Generator
result is ``{"proposed_config": ..., "measures_json": ..., "mquery_json": ...}``,
nested three and more levels deep.

The earlier regex (``\\{[^{}]*(?:\\{[^{}]*\\}[^{}]*)*\\}``) tolerated ONE level of
nesting, so on such a payload it matched an INNER object (``{"Fact_OTC": [...]}``)
and the next crew silently received empty defaults instead of the real data.
"""

import json


def extract_first_json_object(raw: str) -> dict[str, object] | None:
    """The first complete top-level JSON object in ``raw``, or None.

    A balanced-brace scan from the first ``{`` that skips braces inside string
    literals (escape-aware). Returns None when there is no object, when the
    braces never balance, or when the balanced span is not valid JSON.
    """
    if not raw or not isinstance(raw, str):
        return None
    start = raw.find("{")
    if start == -1:
        return None
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(raw)):
        ch = raw[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    parsed = json.loads(raw[start : i + 1])
                except json.JSONDecodeError:
                    return None
                return parsed if isinstance(parsed, dict) else None
    return None
