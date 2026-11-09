"""Dangerous-SQL check of a metric view YAML before deploy.

The generator documents untranslatable measures in YAML comments, including a review
draft such as ``CREATE OR REPLACE VIEW <catalog>.<schema>.x__unioned AS ...``. Scanning
the raw text refused every view that carried such a note, although comments are not part
of the deployed definition. So the check runs on the parsed YAML: every key and string
value (measure and dimension SQL, source, joins, filters) is checked exactly as strictly as
before, comments are not. ``$$`` stays a full-text check: the YAML is embedded in a
``$$ ... $$`` body, so it would end that body even from inside a comment. YAML that does
not parse is checked as text, as before.
"""

from __future__ import annotations

from collections.abc import Iterator

import yaml

from src.services.tools.metric_view_utils import yaml_emitter


def _strings(node: object) -> Iterator[str]:
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for key, value in node.items():
            yield from _strings(key)
            yield from _strings(value)
    elif isinstance(node, list):
        for item in node:
            yield from _strings(item)


def yaml_is_safe(text: str) -> bool:
    """True when no key or value of the YAML holds a dangerous SQL pattern."""
    if "$$" in text:
        return False
    try:
        parsed = yaml.safe_load(text)
    except yaml.YAMLError:
        return yaml_emitter._check_dangerous_sql(text)
    return all(yaml_emitter._check_dangerous_sql(s) for s in _strings(parsed))
