"""Verify that a deployed metric view's definition equals its YAML spec.

``DESCRIBE TABLE EXTENDED <view> AS JSON`` carries the live YAML as ``view_text``, starting
at the ``version:`` key. A failed deploy leaves the previous view live, so a name-only
schema check cannot prove the new definition landed; this compares the live definition
field by field: source, joins, filter, and per measure / dimension expr, comment,
display_name, format, synonyms and window.
"""

from __future__ import annotations

import json
from collections.abc import Mapping

import yaml

from .sql_helpers import SqlRunner

_BODY_START = "version:"
_MAX_LISTED = 50
_COLLAPSE_WHITESPACE = frozenset({"expr", "comment"})
_ITEM_FIELDS = ("expr", "comment", "display_name", "format", "synonyms", "window")
_TOP_LEVEL_FIELDS = ("source", "joins", "filter")


def parse_yaml_spec(text: str) -> dict | None:
    """Parse a YAML document into a mapping, or ``None`` when it is not one."""
    try:
        spec = yaml.safe_load(text)
    except yaml.YAMLError:
        return None
    return spec if isinstance(spec, dict) else None


def view_text_from_describe(data: object) -> str | None:
    """The ``view_text`` field of a ``DESCRIBE ... AS JSON`` payload, or ``None``."""
    if not isinstance(data, Mapping):
        return None
    result = data.get("result")
    if not isinstance(result, Mapping):
        return None
    rows = result.get("data_array")
    if (
        not isinstance(rows, list)
        or not rows
        or not isinstance(rows[0], list)
        or not rows[0]
    ):
        return None
    try:
        document = json.loads(str(rows[0][0]))
    except ValueError:
        return None
    text = document.get("view_text") if isinstance(document, dict) else None
    return text if isinstance(text, str) else None


def deployed_spec(view_text: str) -> dict | None:
    """Parsed YAML of a deployed view, or ``None`` when the text is not a metric view."""
    if _BODY_START not in view_text:
        return None
    return parse_yaml_spec(view_text[view_text.index(_BODY_START) :])


def _items(spec: Mapping[str, object], key: str) -> dict[str, Mapping[str, object]]:
    """``{name: item}`` for a list of named measures / dimensions."""
    raw = spec.get(key)
    if not isinstance(raw, list):
        return {}
    return {
        item["name"]: item
        for item in raw
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }


def item_names(spec: Mapping[str, object], key: str) -> list[str]:
    """Names of the measures (``key="measures"``) or dimensions of a YAML spec, in order."""
    return list(_items(spec, key))


def _collapsed(value: object) -> object:
    return " ".join(str(value).split())


def _changed_fields(
    ours: Mapping[str, object], theirs: Mapping[str, object]
) -> list[str]:
    changed = []
    for field in _ITEM_FIELDS:
        mine, deployed = ours.get(field), theirs.get(field)
        if field in _COLLAPSE_WHITESPACE and mine is not None and deployed is not None:
            mine, deployed = _collapsed(mine), _collapsed(deployed)
        if mine != deployed:
            changed.append(field)
    return changed


def definition_differences(
    repo: Mapping[str, object], deployed: Mapping[str, object]
) -> list[str]:
    """Human-readable differences between the YAML spec and the deployed spec (empty = equal)."""
    differences: list[str] = []
    for key in ("measures", "dimensions"):
        kind = key[:-1]
        ours, theirs = _items(repo, key), _items(deployed, key)
        for name, item in ours.items():
            if name not in theirs:
                differences.append(f"{kind} {name} (missing)")
                continue
            changed = _changed_fields(item, theirs[name])
            if changed:
                differences.append(f"{kind} {name} ({', '.join(changed)})")
        differences += [
            f"{kind} {name} (only deployed)" for name in theirs if name not in ours
        ]
    differences += [
        field for field in _TOP_LEVEL_FIELDS if repo.get(field) != deployed.get(field)
    ]
    return differences


def count_measures_without_format(spec: Mapping[str, object]) -> int:
    """Measures that declare no ``format`` (they display unformatted in BI tools)."""
    return sum(
        1 for item in _items(spec, "measures").values() if item.get("format") is None
    )


def definition_report(
    repo: Mapping[str, object], describe_data: object
) -> dict[str, object]:
    """Compare a YAML spec with the ``view_text`` of a DESCRIBE payload.

    ``verified`` is ``None`` when the live definition could not be read (the check did not
    run), otherwise ``True`` / ``False``."""
    text = view_text_from_describe(describe_data)
    deployed = deployed_spec(text) if text is not None else None
    if deployed is None:
        return {
            "verified": None,
            "note": "view_text not readable; definition not verified",
        }
    differences = definition_differences(repo, deployed)
    return {
        "verified": not differences,
        "difference_count": len(differences),
        "differences": differences[:_MAX_LISTED],
        "measures_without_format": count_measures_without_format(deployed),
    }


def verify_view_definition(
    run_sql: SqlRunner, view_fqn: str, repo: Mapping[str, object]
) -> dict[str, object]:
    """Standalone verify: DESCRIBE the view and compare it with the YAML spec.

    Status is ``verified``, ``differs``, ``not_metric_view`` or ``missing`` (DESCRIBE failed:
    the view does not exist or the principal cannot read it)."""
    outcome = run_sql(f"DESCRIBE TABLE EXTENDED {view_fqn} AS JSON")
    if not outcome.get("success"):
        return {"status": "missing", "view_fqn": view_fqn}
    report = definition_report(repo, outcome.get("data"))
    if report["verified"] is None:
        return {"status": "not_metric_view", "view_fqn": view_fqn, **report}
    status = "verified" if report["verified"] else "differs"
    return {"status": status, "view_fqn": view_fqn, **report}
