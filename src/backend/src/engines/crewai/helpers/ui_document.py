"""
Server-side normalization of agent-produced A2UI "UI documents".

The companion module ``ui_emission`` appends the "return a single JSON UI
document" instruction to a Predefined-UI run's final task. The agent then emits
that document AS its final output — but weaker models routinely wrap it in a
prose preamble ("Here is your dashboard: { … }"), fence it in ```json, double-
encode it as a JSON string, or emit MISMATCHED / truncated brackets that
``json.loads`` rejects outright. Until now that salvage happened only on the
frontend (uiDocument.ts), once per render, on every client.

This module lifts the SAME tolerant parse + bracket-repair to the backend so a
malformed document is repaired ONCE, before it is persisted as the execution
result. The stored result then becomes a clean, canonical A2UI JSON document
that every channel (chat preview, Jobs result viewer, PDF export) reads without
client-side guesswork.

It is a faithful port of uiDocument.ts's coerceJson / repairJsonBrackets /
balancedBlock / parseUiDocument / findUiDocument. The recognized-component set
mirrors that file's VALID_TYPES exactly, so backend and frontend agree on what
counts as a renderable document. ``normalize_ui_document`` is the only public
entry point; it returns the canonical document string or None (not an A2UI
document → caller keeps the original result untouched).
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

# Components Kasal's renderer recognizes — a VERBATIM mirror of VALID_TYPES in
# the frontend uiDocument.ts. A document is only "renderable" (and thus worth
# canonicalizing) when it carries at least one of these, which is what keeps
# non-UI run output from ever being touched.
_VALID_TYPES = frozenset(
    {
        "Text",
        "Row",
        "Column",
        "Card",
        "List",
        "Divider",
        "Image",
        "Icon",
        "Badge",
        "Button",
        "TextField",
        "CheckBox",
        "Slider",
        "ChoicePicker",
        "Dashboard",
        "Stat",
        "Chart",
        "Table",
        "Quiz",
        "Slides",
        "Slide",
        "Album",
        "Mindmap",
        "Flashcards",
    }
)

# A ```json / ```ui fence wrapping the whole document (multiline).
_FENCE_RE = re.compile(r"^```(?:json|ui)?\s*\n([\s\S]*?)\n\s*```$")

# Depth caps mirror the frontend: bound double-encode unwrapping and the
# recursive walk against pathological nesting.
_COERCE_MAX_DEPTH = 4
_WALK_MAX_DEPTH = 6


def _balanced_block(text: str, open_ch: str, close_ch: str) -> Optional[str]:
    """Scan from the first ``open_ch`` to its balanced ``close_ch`` (string-aware)
    and return the enclosed substring, or None. Used to pull a JSON object/array
    out of surrounding prose."""
    start = text.find(open_ch)
    if start < 0:
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == open_ch:
            depth += 1
        elif ch == close_ch:
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def _repair_json_brackets(src: str) -> str:
    """Rebalance the bracket structure of a JSON-ish string so a slightly-
    malformed document still parses. Weak models routinely emit A2UI with
    MISMATCHED or EXTRA brackets (e.g. a tail of ``}]}]}}]}`` where ``}]}}]}``
    was meant), which ``json.loads`` rejects outright. String-aware (brackets
    inside string values are never touched): drop any closing bracket that does
    not match the top of the open-bracket stack, and auto-close anything still
    open at end-of-string.

    Conservative by construction — only rebalances brackets, never invents keys
    or values — and is invoked ONLY after strict parsing has already failed."""
    out: List[str] = []
    stack: List[str] = []
    opener = {"}": "{", "]": "["}
    in_str = False
    esc = False
    for ch in src:
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            out.append(ch)
            continue
        if ch in ("{", "["):
            stack.append(ch)
            out.append(ch)
            continue
        if ch in ("}", "]"):
            if stack and stack[-1] == opener[ch]:
                stack.pop()
                out.append(ch)
            # else: spurious / mismatched closer — drop it.
            continue
        out.append(ch)
    # Close anything left open (a model that truncated mid-document).
    while stack:
        out.append("}" if stack.pop() == "{" else "]")
    return "".join(out)


def _coerce_json(
    raw: Union[str, Dict[str, Any], List[Any]], depth: int = 0
) -> Optional[Union[Dict[str, Any], List[Any]]]:
    """Read a JSON value that may be a raw object/array, a JSON-encoded string, a
    ```json fenced block, or JSON EMBEDDED in surrounding prose. Mirrors
    coerceJson in uiDocument.ts, including the post-failure bracket-repair retry."""
    if isinstance(raw, (dict, list)):
        return raw
    if not isinstance(raw, str):
        return None
    trimmed = raw.strip()

    # The value may be a JSON-ENCODED string rather than raw JSON (an execution
    # result is frequently stored stringified), so decode the outer string
    # layer(s) and re-attempt. Depth-capped to avoid any loop.
    if depth < _COERCE_MAX_DEPTH and trimmed.startswith('"') and trimmed.endswith('"'):
        try:
            return _coerce_json(json.loads(trimmed), depth + 1)
        except (ValueError, TypeError):
            pass  # not a JSON-encoded string; fall through

    # Tolerate a ```json fence around the document.
    fence = _FENCE_RE.match(trimmed)
    fenced = fence.group(1).strip() if fence else None

    # Pull the first balanced {…} AND […] out of the text so a prose preamble or
    # a bracketed log prefix can't hide the document; whichever parses wins.
    obj_block = _balanced_block(trimmed, "{", "}") if "{" in trimmed else None
    arr_block = _balanced_block(trimmed, "[", "]") if "[" in trimmed else None

    candidates = [fenced, trimmed, obj_block, arr_block]

    # First pass: strict parse, in order (fenced body, whole string, {…}, […]).
    for cand in candidates:
        if not cand:
            continue
        c = cand.strip()
        if not (c.startswith("{") or c.startswith("[")):
            continue
        try:
            return json.loads(c)
        except ValueError:
            continue

    # Every candidate failed STRICT parsing — rebalance (string-aware) and retry.
    for cand in candidates:
        if not cand:
            continue
        c = cand.strip()
        if not (c.startswith("{") or c.startswith("[")):
            continue
        try:
            return json.loads(_repair_json_brackets(c))
        except ValueError:
            continue
    return None


def _extract_messages(obj: Any) -> Optional[List[Any]]:
    """Pull the message list out of any shape a document arrives in. A bare list
    of messages is wrapped into ``{messages}`` by the caller before this runs."""
    if isinstance(obj, dict):
        if isinstance(obj.get("messages"), list):
            return obj["messages"]
        if "createSurface" in obj or "updateComponents" in obj:
            return [obj]
    return None


def _is_ui_document(raw: Union[str, Dict[str, Any], List[Any]]) -> bool:
    """True when ``raw`` is a recognizable A2UI document — i.e. it coerces to
    messages carrying at least one recognized catalog component. This is the same
    strict predicate parseUiDocument uses on the frontend, so non-A2UI content can
    never false-positive (and therefore is never rewritten)."""
    obj = _coerce_json(raw)
    if obj is None:
        return False
    messages = _extract_messages({"messages": obj} if isinstance(obj, list) else obj)
    if not messages:
        return False
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        update = msg.get("updateComponents")
        if not isinstance(update, dict):
            continue
        components = update.get("components")
        if not isinstance(components, list):
            continue
        for c in components:
            if not isinstance(c, dict) or not isinstance(c.get("id"), str):
                continue
            # LLMs vary on the discriminator key — accept "component" or "type".
            comp_name = c.get("component", c.get("type"))
            if comp_name in _VALID_TYPES:
                return True
    return False


def _find_ui_document(
    raw: Any, depth: int = 0
) -> Optional[Union[str, Dict[str, Any], List[Any]]]:
    """Recursively locate the A2UI document node anywhere inside an arbitrary
    value and return it (the string/dict/list that ``_is_ui_document`` accepts),
    or None. Outermost-first (pre-order) so a clean top-level document wins; depth
    capped against pathological nesting. Mirrors findUiDocument in uiDocument.ts."""
    if raw is None or depth > _WALK_MAX_DEPTH:
        return None
    if isinstance(raw, str):
        return raw if _is_ui_document(raw) else None
    if not isinstance(raw, (dict, list)):
        return None
    if _is_ui_document(raw):
        return raw
    children = raw if isinstance(raw, list) else list(raw.values())
    for child in children:
        found = _find_ui_document(child, depth + 1)
        if found is not None:
            return found
    return None


def normalize_ui_document(raw: Any) -> Optional[str]:
    """Return a clean, canonical A2UI document JSON string for ``raw``, or None
    when ``raw`` does not contain a renderable A2UI document.

    Finds the document node (unwrapping a prose preamble, a ```json fence, a
    double-encoded string, or a multi-key envelope), repairs mismatched/truncated
    brackets, and re-serializes it so the persisted result is canonical JSON. The
    full document is preserved — ``summary``, ``theme`` and ``dataModelUpdate``
    messages survive; only the surrounding wrapper and malformations are dropped.

    Returns None (caller keeps the original result verbatim) for any non-A2UI
    output and on any error — normalization must never alter, let alone break, a
    run's result. Idempotent: a clean document re-serializes to itself."""
    try:
        node = _find_ui_document(raw)
        if node is None:
            return None
        doc = _coerce_json(node) if isinstance(node, str) else node
        if doc is None:
            return None
        return json.dumps(doc, ensure_ascii=False)
    except Exception as e:  # noqa: BLE001 — normalization must never break a run
        logger.warning("[UIDocument] Skipped UI-document normalization: %s", e)
        return None
