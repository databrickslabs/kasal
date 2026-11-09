"""Edits of one metric view YAML and of its reconciliation mapping.

The agent changes ONE measure at a time instead of rewriting a whole YAML: an edit can only
touch the named measure, every expression goes through the deployer's dangerous-SQL check, and
names must be plain identifiers. YAML comments (the generator's review notes) are not kept in the
edited YAML; the workbench keeps the original gaps and DAX for the agent instead.
"""

from __future__ import annotations

import json
import re
from typing import Optional

import yaml

from src.services.tools.metric_view_deployer_utils.yaml_safety import yaml_is_safe

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FQN = re.compile(r"^[A-Za-z_][\w]*\.[A-Za-z_][\w]*\.[A-Za-z_][\w]*$")


class EditError(ValueError):
    """An edit the workbench refuses, with a message for the agent."""


def _load(text: str) -> dict[str, object]:
    try:
        data = yaml.safe_load(text) if text else None
    except yaml.YAMLError as e:
        raise EditError(f"the view YAML does not parse: {e}") from e
    if not isinstance(data, dict):
        raise EditError("the view YAML is not a mapping")
    return data


def _dump(data: dict[str, object]) -> str:
    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=1000)


def _measures(data: dict[str, object]) -> list[dict[str, object]]:
    measures = data.get("measures")
    if measures is None:
        measures = []
        data["measures"] = measures
    if not isinstance(measures, list):
        raise EditError("'measures' is not a list")
    return [m for m in measures if isinstance(m, dict)]


def measure_names(text: str) -> list[str]:
    return [str(m.get("name")) for m in _measures(_load(text)) if m.get("name")]


def measure_entry(text: str, name: str) -> Optional[dict[str, object]]:
    return next((m for m in _measures(_load(text)) if m.get("name") == name), None)


def parse_format(raw: Optional[str]) -> Optional[dict[str, object]]:
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except ValueError as e:
        raise EditError(f"format must be a JSON object, e.g. {{\"type\": \"number\"}}: {e}") from e
    if not isinstance(value, dict):
        raise EditError("format must be a JSON object")
    return value


def set_measure(
    text: str,
    name: str,
    expr: str,
    *,
    display_name: Optional[str] = None,
    fmt: Optional[dict[str, object]] = None,
    comment: Optional[str] = None,
) -> tuple[str, bool]:
    """Add or replace one measure. Returns ``(new_yaml, replaced)``."""
    if not _IDENTIFIER.match(name or ""):
        raise EditError(f"measure name {name!r} must be a plain identifier (letters, digits, _)")
    if not (expr or "").strip():
        raise EditError("expr is empty")
    data = _load(text)
    raw = data.get("measures")
    measures: list[object] = list(raw) if isinstance(raw, list) else []
    entry: dict[str, object] = {"name": name, "expr": expr.strip()}
    if display_name:
        entry["display_name"] = display_name
    if fmt:
        entry["format"] = fmt
    if comment:
        entry["comment"] = comment
    replaced = False
    for i, m in enumerate(measures):
        if isinstance(m, dict) and m.get("name") == name:
            # keep what the edit does not set (format, display_name, synonyms, ...)
            measures[i] = {**m, **entry}
            replaced = True
            break
    if not replaced:
        measures.append(entry)
    data["measures"] = measures
    new_text = _dump(data)
    if not yaml_is_safe(new_text):
        raise EditError("the expression contains a dangerous SQL pattern")
    return new_text, replaced


def remove_measure(text: str, name: str) -> str:
    data = _load(text)
    before = _measures(data)
    kept = [m for m in before if m.get("name") != name]
    if len(kept) == len(before):
        raise EditError(f"no measure {name!r} in this view")
    data["measures"] = kept
    return _dump(data)


def mapping_dict(raw: object) -> dict[str, object]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            data = yaml.safe_load(raw)
        except yaml.YAMLError:
            return {}
        return dict(data) if isinstance(data, dict) else {}
    return {}


def rebind_mapping(mapping: dict[str, object], ucmv_fqn: str) -> dict[str, object]:
    """Point a reconciliation mapping at the sandbox view."""
    if not _FQN.match(ucmv_fqn):
        raise EditError(f"{ucmv_fqn!r} is not catalog.schema.view")
    binding = mapping.get("binding")
    binding = dict(binding) if isinstance(binding, dict) else {}
    binding["ucmv_table"] = ucmv_fqn
    binding.pop("ucmv", None)
    return {**mapping, "binding": binding}


def upsert_mapping_measure(mapping: dict[str, object], ucmv_measure: str, pbi_measure: str) -> dict[str, object]:
    """Compare ``ucmv_measure`` with the Power BI measure ``pbi_measure`` (``pbi_kind: direct``).
    An existing entry keeps its options (aggregation, filters, tolerance) and becomes resolved."""
    measures = mapping.get("measures")
    entries = [dict(m) for m in measures if isinstance(m, dict)] if isinstance(measures, list) else []
    for m in entries:
        if m.get("ucmv_measure") == ucmv_measure:
            if m.get("pbi_kind") in (None, "unresolved"):
                m["pbi_kind"] = "direct"
            m.setdefault("pbi_measure", pbi_measure)
            break
    else:
        entries.append({"ucmv_measure": ucmv_measure, "pbi_measure": pbi_measure, "pbi_kind": "direct"})
    return {**mapping, "measures": entries}


def drop_mapping_measure(mapping: dict[str, object], ucmv_measure: str) -> dict[str, object]:
    measures = mapping.get("measures")
    entries = [m for m in measures if isinstance(m, dict)] if isinstance(measures, list) else []
    return {**mapping, "measures": [m for m in entries if m.get("ucmv_measure") != ucmv_measure]}
