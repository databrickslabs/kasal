"""Optional flag: a measure that did not reconcile says so in its own comment.

Off unless ``flag_unreconciled_measures`` is set in the deployer's configuration. With it,
the comment of a measure that is below the bar, or was not compared with Power BI, gets a
note in square brackets, so the warning is visible in Unity Catalog and to Genie:

    comment: "Net sales revenue [Not reconciled with Power BI: 66.7 % of cells match]"

A generated view is edited as text (loading and dumping it would drop its comment lines);
there a measure whose comment spans several lines is left as it is, and stays in the review
list. A view without comment lines (healed or edited, so already re-serialised) is loaded,
changed and written back. The result must parse to the same measures, otherwise the original
YAML is kept.

Pure functions: no I/O.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

import yaml

from src.services.tools.metric_view_utils.raw_fields import as_list, as_map
from src.services.tools.metric_view_utils.reconciliation.review_list import (
    DEFAULT_BAR_PCT,
    build_review_list,
)

_ENTRY = re.compile(r"^  - name:\s*['\"]?([^'\"\s]+)['\"]?\s*$")
_COMMENT = re.compile(r"^(    comment:\s*)(.*)$")
_MARK = "[Not "


def measure_flags(
    output: Mapping[str, object], bar: float = DEFAULT_BAR_PCT
) -> dict[str, dict[str, str]]:
    """``{view: {measure: note}}`` for the measures below the bar or not compared."""
    flags: dict[str, dict[str, str]] = {}
    for row in build_review_list(output, bar):
        if row["status"] == "below_bar":
            note = f"Not reconciled with Power BI: {row.get('pct_aligned')} % of cells match"
        elif row["status"] == "not_compared":
            note = "Not compared with Power BI"
        else:
            continue
        flags.setdefault(str(row["view"]), {})[str(row["measure"])] = note
    return flags


def _flagged(value: str, note: str) -> str | None:
    """The comment value with the note added; None when it is not a one-line scalar."""
    suffix = f" [{note}]"
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "\"'":
        return text[:-1] + suffix + text[-1]
    if not text or text[0] in "|>&*!{[":
        return None
    escaped = (text + suffix).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _measure_names(text: str) -> list[str]:
    spec = as_map(yaml.safe_load(text))
    return [str(as_map(m).get("name")) for m in as_list(spec.get("measures"))]


def _flag_by_reload(yaml_text: str, notes: Mapping[str, str]) -> str:
    """For YAML without comment lines (a healed or edited view, already re-serialised):
    load it, add the notes, write it back the way those steps do."""
    try:
        spec = dict(as_map(yaml.safe_load(yaml_text)))
    except yaml.YAMLError:
        return yaml_text
    measures = [dict(as_map(m)) for m in as_list(spec.get("measures"))]
    for measure in measures:
        note = notes.get(str(measure.get("name")))
        comment = str(measure.get("comment") or "")
        if note and _MARK not in comment:
            measure["comment"] = f"{comment} [{note}]".strip()
    spec["measures"] = measures
    return yaml.safe_dump(spec, sort_keys=False, allow_unicode=True)


def flag_yaml(yaml_text: str, notes: Mapping[str, str]) -> str:
    """``yaml_text`` with each note added to its measure's comment."""
    if not re.search(r"(?m)^\s*#", yaml_text):
        return _flag_by_reload(yaml_text, notes)
    lines = yaml_text.split("\n")
    try:
        start = next(i for i, line in enumerate(lines) if line.rstrip() == "measures:")
    except StopIteration:
        return yaml_text
    out = lines[: start + 1]
    i = start + 1
    while i < len(lines):
        entry = _ENTRY.match(lines[i])
        if not entry:
            out.append(lines[i])
            i += 1
            continue
        end = i + 1
        while end < len(lines) and not _ENTRY.match(lines[end]):
            end += 1
        block = lines[i:end]
        note = notes.get(entry.group(1))
        if note and not any(_MARK in line for line in block):
            for j, line in enumerate(block):
                comment = _COMMENT.match(line)
                if comment:
                    flagged = _flagged(comment.group(2), note)
                    if flagged is not None:
                        block[j] = comment.group(1) + flagged
                    break
            else:
                block.insert(1, f'    comment: "[{note}]"')
        out.extend(block)
        i = end
    flagged_text = "\n".join(out)
    try:
        if _measure_names(flagged_text) == _measure_names(yaml_text):
            return flagged_text
    except yaml.YAMLError:
        pass
    return yaml_text


def flag_unreconciled(
    yaml_specs: Mapping[str, object], ucmv_output: object, enabled: object
) -> dict[str, object]:
    """The specs to deploy: unchanged unless ``enabled``, then with the notes added."""
    from .reconciliation_gate import _as_output

    if not enabled or str(enabled).lower() in ("false", "0", "no"):
        return dict(yaml_specs)
    output = _as_output(ucmv_output)
    bar = output.get("review_bar_pct")
    flags = measure_flags(
        {**output, "yaml": dict(yaml_specs)},
        float(bar) if isinstance(bar, (int, float)) and bar else DEFAULT_BAR_PCT,
    )
    return {
        view: (
            flag_yaml(text, flags[view])
            if isinstance(text, str) and view in flags
            else text
        )
        for view, text in yaml_specs.items()
    }
