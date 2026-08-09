"""Building the names a router condition is evaluated against.

Split from ``flow_conditions`` — that module RESOLVES a field once state holds
it (paths, projections, any-element comparison); this one ASSEMBLES the state
and context a condition is evaluated over: parsing the crew output that just
arrived, coercing scalars, and pulling structured data out of prose.

Lifted out of a closure inside ``flow_builder.route_method``, which is over the
file-size ceiling and could not keep growing.
"""

import json
import logging
import re
from collections.abc import Mapping
from typing import Any, Dict

from src.services.flow_builder.modules.flow_conditions import (
    ConditionState,
    make_where,
    state_snapshot,
)

logger = logging.getLogger(__name__)


def coerce_scalar_value(val):
    """Coerce a string scalar to its natural type for reliable router comparisons.

    Booleans first: a crew may return ``has_results: true`` (JSON bool → Python
    True) on one run and ``"true"``/``"True"`` (string) on another, so a router
    condition like ``has_results == True`` would silently be False for the string
    form. Map "true"/"false" (case-insensitive) to Python bool; numeric strings to
    int/float; everything else is returned unchanged.
    """
    if isinstance(val, str):
        low = val.strip().lower()
        if low == "true":
            return True
        if low == "false":
            return False
        try:
            return int(val)
        except ValueError:
            pass
        try:
            return float(val)
        except ValueError:
            pass
    return val


def extract_embedded_json(text):
    """Extract a JSON object/array embedded in prose.

    Models on the soft output_json path commonly wrap their structured answer in
    a ```json ... ``` block surrounded by prose ("Based on my research… ```json
    {…} ``` ## Summary …"). The router's direct-JSON check requires the whole
    string to be JSON, so it misses these and routing silently stops. This pulls
    the JSON out: first a ```json/``` fenced block, then the first balanced
    ``{...}`` object. Returns the parsed value, or None.
    """
    if not isinstance(text, str):
        return None
    import json as _json
    import re as _re

    # 1) Fenced code block (```json ... ``` or ``` ... ```).
    for m in _re.finditer(r"```(?:json)?\s*(.*?)```", text, _re.DOTALL):
        try:
            return _json.loads(m.group(1).strip())
        except Exception:
            continue

    # 2) First balanced {...} object (brace counting; json.loads validates).
    start = text.find("{")
    while start != -1:
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return _json.loads(text[start : i + 1])
                    except Exception:
                        break
        start = text.find("{", start + 1)
    return None


def build_eval_context(flow: Any, args: tuple, kwargs: dict) -> Dict[str, Any]:
    """The names a router condition is evaluated against.

    Lifted verbatim out of a closure inside ``flow_builder.route_method``.
    It is condition-evaluation logic, so it belongs beside ConditionState —
    and flow_builder.py is over the file-size ceiling, which this is the
    largest coherent seam in.

    ``flow`` rather than a bare state so the ``hasattr(flow, "state")``
    check stays exactly as it was: a flow without state must yield an empty
    ConditionState, not raise.
    """

    eval_context = {}

    # Convert string scalars (bool/int/float) so router conditions
    # compare reliably — see module-level coerce_scalar_value.
    def auto_convert_value(val):
        return coerce_scalar_value(val)

    # Helper function to convert all string numerics in a dict
    def auto_convert_dict(d):
        """Recursively convert string numerics in a dict."""
        if not isinstance(d, dict):
            return auto_convert_value(d)
        return {k: auto_convert_dict(v) for k, v in d.items()}

    # Safe helper functions for condition evaluation
    def safe_int(val, default=0):
        """Safely convert value to int."""
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    def safe_float(val, default=0.0):
        """Safely convert value to float."""
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    # Add helper functions to context for use in conditions
    eval_context["int"] = safe_int
    eval_context["float"] = safe_float
    eval_context["str"] = str
    eval_context["len"] = len
    eval_context["bool"] = bool
    eval_context["abs"] = abs
    eval_context["min"] = min
    eval_context["max"] = max

    # Add state to context if available.
    #
    # Wrapped in ConditionState, which is what makes a router
    # condition able to reach a value that is NOT a top-level
    # key: nested inside an object, or repeated across a list.
    # The wrap happens HERE, before the merges below, because
    # it also makes those merges tolerant — a typed state
    # raises on a field it does not declare, and that raise
    # used to escape and stop the flow.
    if hasattr(flow, "state"):
        eval_context["state"] = ConditionState(flow.state)
    else:
        eval_context["state"] = ConditionState({})
    # Bound to THIS state, so a condition can ask about ONE item rather than
    # about each field independently. See make_where.
    eval_context["where"] = make_where(eval_context["state"])

    # Add result from args
    # Defined BEFORE `if args:`, deliberately.
    #
    # All three are also used by the state scan below, which runs whether
    # or not this router was called with args. A router listening to a
    # STARTING POINT receives none — so `if args:` was skipped, the helpers
    # were never defined, and the scan raised UnboundLocalError. The
    # handler swallowed it and abandoned the whole route evaluation, so a
    # route whose condition was true simply never ran. Observed on a real
    # flow as "cannot access local variable 'strip_code_fences'".
    def merge_parsed_json(parsed_data, source_label):
        if isinstance(parsed_data, dict):
            parsed_data = auto_convert_dict(parsed_data)
            eval_context["state"].update(parsed_data)
            eval_context.update(parsed_data)
            logger.info(
                f"Parsed {source_label} JSON object and merged into state: {list(parsed_data.keys())}"
            )
        elif isinstance(parsed_data, list) and parsed_data:
            # For JSON arrays, extract keys from the first dict item
            first_item = parsed_data[0]
            if isinstance(first_item, dict):
                first_item = auto_convert_dict(first_item)
                eval_context["state"].update(first_item)
                eval_context.update(first_item)
                logger.info(
                    f"Parsed {source_label} JSON array (first item) and merged into state: {list(first_item.keys())}"
                )
            # Also store the full array for advanced conditions
            eval_context["items"] = parsed_data
            eval_context["state"]["items"] = parsed_data
            logger.info(
                f"Stored {source_label} JSON array with {len(parsed_data)} items in context['items']"
            )

    def strip_code_fences(s):
        """Strip markdown code fences (```json ... ```) from a string."""
        s = s.strip()
        if s.startswith("```"):
            first_newline = s.find("\n")
            if first_newline != -1:
                s = s[first_newline + 1 :]
            if s.rstrip().endswith("```"):
                s = s.rstrip()[:-3].rstrip()
        return s

    def looks_like_json(s):
        s = s.strip()
        return (s.startswith("{") and s.endswith("}")) or (
            s.startswith("[") and s.endswith("]")
        )

    if args:
        eval_context["result"] = args[0]

        # Try to extract values from CrewOutput
        result_obj = args[0]

        # Helper to merge parsed JSON into eval context and state

        # Prefer the declared structured output (output_pydantic /
        # output_json) when present: a task with a declared schema yields a
        # CrewOutput whose .pydantic / .json_dict holds typed fields. Routing
        # on these is deterministic, so router conditions resolve reliably
        # instead of depending on ad-hoc raw-text JSON parsing below.
        if getattr(result_obj, "pydantic", None) is not None:
            try:
                merge_parsed_json(
                    result_obj.pydantic.model_dump(),
                    "crew output (pydantic)",
                )
            except (AttributeError, Exception) as parse_err:
                logger.debug(f"Could not read pydantic crew output: {parse_err}")

        elif getattr(result_obj, "json_dict", None):
            merge_parsed_json(result_obj.json_dict, "crew output (json_dict)")

        # If result has a 'raw' attribute (CrewOutput), try to parse it as JSON
        elif hasattr(result_obj, "raw"):
            try:
                raw_str = strip_code_fences(str(result_obj.raw))
                if looks_like_json(raw_str):
                    parsed_data = json.loads(raw_str)
                    merge_parsed_json(parsed_data, "crew output")
            except (json.JSONDecodeError, Exception) as parse_err:
                logger.debug(f"Could not parse crew output as JSON: {parse_err}")

        # If result is a string that looks like JSON, parse it
        elif isinstance(result_obj, str):
            try:
                raw_str = strip_code_fences(result_obj)
                if looks_like_json(raw_str):
                    parsed_data = json.loads(raw_str)
                    merge_parsed_json(parsed_data, "string result")
            except (json.JSONDecodeError, Exception) as parse_err:
                logger.debug(f"Could not parse string result as JSON: {parse_err}")

        # Also add common fields from result
        if isinstance(args[0], dict):
            eval_context.update(args[0])
        elif hasattr(args[0], "__dict__"):
            eval_context.update(vars(args[0]))

    # Parse JSON strings in state values and add them to top-level context
    # This makes values like state["Random Number"] = '{"number": 43}' accessible as eval_context["number"] = 43
    if eval_context.get("state"):
        for key, value in list(eval_context["state"].items()):
            if isinstance(value, str):
                # Strip markdown code fences if present (e.g., ```json\n...\n```)
                json_value = strip_code_fences(value)

                # Now check if it looks like JSON (object or array)
                if looks_like_json(json_value):
                    try:
                        parsed_value = json.loads(json_value)
                        merge_parsed_json(parsed_value, f"state['{key}']")
                    except (json.JSONDecodeError, Exception) as e:
                        logger.debug(f"Could not parse state['{key}'] as JSON: {e}")
                        pass  # Not JSON, leave as-is
                else:
                    # Prose-wrapped JSON (```json ... ``` inside a summary) —
                    # extract the embedded object so router fields resolve.
                    embedded = extract_embedded_json(value)
                    if isinstance(embedded, (dict, list)):
                        merge_parsed_json(
                            embedded,
                            f"state['{key}'] (embedded json)",
                        )

    # Add kwargs (coerce string scalars so e.g. has_results
    # passed as a bare "true"/"True" kwarg compares correctly).
    eval_context.update({k: coerce_scalar_value(v) for k, v in kwargs.items()})
    return eval_context
