"""Tolerant readers for the untyped config and measure dicts config-gen passes around.

The PBI library hands back plain dicts whose values are not statically known.
These helpers narrow an ``object`` to the shape a caller needs, with the same
fallback the inline ``x.get(k) or default`` expressions used.
"""

from collections.abc import Mapping


def text_value(value: object) -> str:
    """``value`` when it is a string, otherwise an empty string."""
    return value if isinstance(value, str) else ""


def first_text(item: Mapping[str, object], *keys: str) -> str:
    """First truthy value among ``keys``, as text.

    Mirrors ``item.get(k1) or item.get(k2) or ""``; a truthy non-string yields "".
    """
    for key in keys:
        value = item.get(key)
        if value:
            return text_value(value)
    return ""


def read_section(config: Mapping[str, object], key: str) -> dict[str, object]:
    """``config[key]`` when it is a dict, otherwise a fresh empty dict (not stored)."""
    value = config.get(key)
    return value if isinstance(value, dict) else {}


def ensure_section(config: dict[str, object], key: str) -> dict[str, object]:
    """``config[key]`` when it is a dict; otherwise store and return a new dict."""
    value = config.get(key)
    if isinstance(value, dict):
        return value
    created: dict[str, object] = {}
    config[key] = created
    return created


def str_list(value: object) -> list[str]:
    """Elements of a list as text; anything that is not a list gives ``[]``."""
    return [str(item) for item in value] if isinstance(value, list) else []
