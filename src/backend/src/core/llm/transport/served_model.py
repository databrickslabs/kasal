"""Which model actually answered, when it is not the one that was asked for.

A router model (OpenRouter's Jev Router, ``openrouter/auto``) hands each request
to a model of its choosing, and an OpenAI-compatible response names that model
in its top-level ``model`` field. Every other provider echoes the requested id
there too, often decorated: Anthropic adds a date (``claude-sonnet-4-5`` →
``claude-sonnet-4-5-20250929``), gateways add or drop a vendor prefix. Showing
those would be noise, so the served id is reported only when it names a
different model.

The rule: reduce both ids to a core (lowercase, last ``/`` segment, no
``:variant`` suffix, ``.``/``_`` read as ``-``, date stamps removed) and call
them the same model when either core contains the other. So
``databricks-claude-sonnet-4-5`` and ``claude-sonnet-4-5-20250929`` match, and
``typesafe/jev-router`` and ``anthropic/claude-opus-5-5`` do not.
"""

import re

_DATE_STAMP = re.compile(r"-(?:\d{8}|\d{4}-\d{2}-\d{2})(?=-|$)")


def _core(model_id: str) -> str:
    core = model_id.strip().lower().rsplit("/", 1)[-1].split(":", 1)[0]
    return _DATE_STAMP.sub("", re.sub(r"[._]", "-", core))


def served_model_if_different(requested: str | None, served: str | None) -> str | None:
    """``served`` when it names a different model than ``requested``, else None."""
    if not served or not served.strip():
        return None
    if not requested:
        return served
    asked, got = _core(requested), _core(served)
    if not asked or not got or asked in got or got in asked:
        return None
    return served
