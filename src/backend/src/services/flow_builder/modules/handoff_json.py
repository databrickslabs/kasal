"""Find the JSON payload a crew handed off, inside the agent's narrative.

A crew's output often wraps its tool payload in prose ("Here is the full output:
{...}"). The flow handoff needs that payload whole: a Pipeline Config Generator
result is ``{"proposed_config": ..., "measures_json": ..., "mquery_json": ...}``,
nested three and more levels deep.

The earlier regex (``\\{[^{}]*(?:\\{[^{}]*\\}[^{}]*)*\\}``) tolerated ONE level of
nesting, so on such a payload it matched an INNER object (``{"Fact_OTC": [...]}``)
and the next crew silently received empty defaults instead of the real data.

``handoff_config`` decides what the next crew's ``config_json`` becomes: the
handed-off pipeline config, with any switches the crew's own ``config_json``
already sets (``read_from_source_layer``, ``slicer_parameters`` ...) kept on top.
"""

import json

# Pipeline Config Generator result keys the flow injects into the next crew's UC
# Metric View Generator config, by the same name (only into an empty value).
UCMV_HANDOFF_KEYS = (
    "measures_json",
    "mquery_json",
    "relationships_json",
    "visual_usage_index",
    "implicit_column_measures",
    "report_scope",
)

# Keys that make a dict a pipeline config of its own (join maps, filters,
# resolutions), as opposed to a handful of switches.
PIPELINE_CONFIG_KEYS = (
    "join_key_map",
    "enrichment_joins",
    "filter_sets",
    "measure_resolutions",
)


def handoff_config(existing: object, payload: dict[str, object]) -> str | None:
    """The next crew's ``config_json`` after a pipeline config was handed off.

    - Empty ``existing`` (blank, ``{}``, ``null``): the handed-off config.
    - ``existing`` holds switches only (a JSON object with none of
      ``PIPELINE_CONFIG_KEYS``): the handed-off config with those switches on
      top, so a crew can turn on read mode and still receive the join maps.
    - ``existing`` is a pipeline config of its own, or not a JSON object: None,
      the caller leaves it as it is (a manually provided config is never
      replaced).
    """
    text = existing if isinstance(existing, str) else ""
    if not text or text in ("{}", "null") or len(text) <= 10:
        return json.dumps(payload)
    try:
        own = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(own, dict) or any(k in own for k in PIPELINE_CONFIG_KEYS):
        return None
    return json.dumps({**payload, **own})


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
