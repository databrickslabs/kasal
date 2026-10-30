"""Patch a VERIFIED metric-view YAML with drifted measures — and prove nothing else moved.

The baseline is what a human reviewed and deployed. The patch is therefore textual
and minimal, never a parse→modify→dump round-trip (which would re-format every line):

  * a NEW measure is appended as a new list item at the end of ``measures:``
  * a CHANGED measure keeps its item and every field it had (display name, synonyms,
    format, comment text) — only ``expr`` (and ``window``, which is part of what the
    measure computes) is replaced, and the comment's ``dax#`` fingerprint updated
  * a join a new/changed measure needs is appended to ``joins:`` (the block is
    created after ``source:`` when the baseline has none)

Then ``verify_invariant`` re-parses both texts and checks, structurally, that every
top-level key, every baseline join and every untouched baseline measure is
identical. A patch that fails the check is discarded — the caller gets the problems,
not a YAML.
"""

import copy
import re
from dataclasses import dataclass, field
from typing import Optional

import yaml

from src.services.tools.metric_view_utils.drift.fingerprint import FINGERPRINT_PREFIX

_TOP_KEY_RE = re.compile(r"""^(?:"[^"]+"|'[^']+'|[A-Za-z_][\w\-]*)\s*:""")
_FP_TOKEN_RE = re.compile(r"\bdax#[0-9a-f]{8}\b")
# Fields of a changed measure taken from the new translation; all others stay.
_COMPUTE_FIELDS = ("expr", "window")


@dataclass
class Replacement:
    name: str  # baseline measure name
    entry: dict  # the new translation (its compute fields are used)
    fingerprint: Optional[str] = None


@dataclass
class PatchResult:
    yaml_text: Optional[str]
    changes: list[dict] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.yaml_text is not None and not self.problems


# ── text helpers ─────────────────────────────────────────────────────────────


def _block_span(lines: list[str], key: str) -> Optional[tuple[int, int]]:
    """[start, end) of a top-level ``key:`` block (start = the key line)."""
    start = None
    for i, line in enumerate(lines):
        if not _TOP_KEY_RE.match(line):
            continue
        name = line.split(":", 1)[0].strip().strip("\"'")
        if start is None and name == key:
            start = i
        elif start is not None:
            return start, i
    return (start, len(lines)) if start is not None else None


def _item_indent(lines: list[str], span: tuple[int, int]) -> str:
    for line in lines[span[0] + 1 : span[1]]:
        m = re.match(r"^(\s*)- ", line)
        if m:
            return m.group(1)
    return "  "


def _item_starts(lines: list[str], span: tuple[int, int], indent: str) -> list[int]:
    prefix = f"{indent}- "
    return [i for i in range(span[0] + 1, span[1]) if lines[i].startswith(prefix)]


def _render_items(entries: list[dict], indent: str) -> list[str]:
    text = yaml.safe_dump(
        entries,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
        width=4096,
    )
    return [f"{indent}{ln}" if ln else ln for ln in text.rstrip("\n").split("\n")]


def _content_end(lines: list[str], span: tuple[int, int]) -> int:
    """Index after the last non-blank line of a block (trailing blanks stay put)."""
    end = span[1]
    while end > span[0] + 1 and not lines[end - 1].strip():
        end -= 1
    return end


def with_fingerprint(comment: str, fingerprint: Optional[str]) -> str:
    if not fingerprint:
        return comment
    if _FP_TOKEN_RE.search(comment):
        return _FP_TOKEN_RE.sub(fingerprint, comment)
    return f"{comment} · {fingerprint}" if comment else fingerprint


# ── patch ────────────────────────────────────────────────────────────────────


def apply_patch(
    baseline_text: str,
    add_measures: list[dict],
    replace_measures: list[Replacement],
    add_joins: list[dict],
) -> PatchResult:
    lines = baseline_text.split("\n")
    changes: list[dict] = []
    problems: list[str] = []
    baseline = yaml.safe_load(baseline_text) or {}

    # 1. Replace changed measures in place (each item re-rendered on its own).
    if replace_measures:
        span = _block_span(lines, "measures")
        if span is None:
            return PatchResult(None, problems=["baseline has no measures block"])
        indent = _item_indent(lines, span)
        starts = _item_starts(lines, span, indent)
        bounds = list(zip(starts, starts[1:] + [_content_end(lines, span)]))
        wanted = {r.name: r for r in replace_measures}
        # Bottom-up so earlier line indices stay valid.
        for start, end in reversed(bounds):
            chunk = "\n".join(ln[len(indent) :] for ln in lines[start:end])
            try:
                item = (yaml.safe_load(chunk) or [None])[0]
            except yaml.YAMLError:
                continue
            if not isinstance(item, dict) or item.get("name") not in wanted:
                continue
            r = wanted.pop(item["name"])
            merged = copy.deepcopy(item)
            for f in _COMPUTE_FIELDS:
                if r.entry.get(f) is not None:
                    merged[f] = r.entry[f]
                else:
                    merged.pop(f, None)
            if "comment" in merged or r.fingerprint:
                merged["comment"] = with_fingerprint(
                    str(merged.get("comment") or ""), r.fingerprint
                )
            trailing = 0
            while end - trailing > start and not lines[end - trailing - 1].strip():
                trailing += 1
            lines[start:end] = _render_items([merged], indent) + [""] * trailing
            changes.append({"kind": "replaced_measure", "name": item["name"]})
        problems.extend(
            f"changed measure '{n}' not found in baseline YAML" for n in wanted
        )

    # 2. Append new measures.
    if add_measures:
        span = _block_span(lines, "measures")
        if span is None:
            lines += ["", "measures:"]
            span = (len(lines) - 1, len(lines))
        indent = _item_indent(lines, span)
        at = _content_end(lines, span)
        lines[at:at] = _render_items(add_measures, indent)
        changes.extend(
            {"kind": "added_measure", "name": m.get("name")} for m in add_measures
        )

    # 3. Append joins.
    if add_joins:
        span = _block_span(lines, "joins")
        if span is None:
            src = _block_span(lines, "source")
            at = _content_end(lines, src) if src else 0
            lines[at:at] = ["", "joins:"]
            span = (at + 1, at + 2)
        indent = _item_indent(lines, span)
        at = _content_end(lines, span)
        lines[at:at] = _render_items(add_joins, indent)
        changes.extend({"kind": "added_join", "name": j.get("name")} for j in add_joins)

    text = "\n".join(lines)
    problems.extend(
        verify_invariant(
            baseline,
            text,
            added_measures={m.get("name") for m in add_measures},
            replaced_measures={r.name for r in replace_measures},
            added_joins={j.get("name") for j in add_joins},
        )
    )
    return PatchResult(None if problems else text, changes, problems)


# ── invariant ────────────────────────────────────────────────────────────────


def verify_invariant(
    baseline: dict,
    proposed_text: str,
    added_measures: set,
    replaced_measures: set,
    added_joins: set,
) -> list[str]:
    """Problems found; empty list means the baseline is fully preserved."""
    try:
        proposed = yaml.safe_load(proposed_text)
    except yaml.YAMLError as exc:
        return [f"proposed YAML does not parse: {exc}"]
    if not isinstance(proposed, dict):
        return ["proposed YAML is not a mapping"]

    problems: list[str] = []
    for key in set(baseline) | set(proposed):
        if key in ("measures", "joins"):
            continue
        if baseline.get(key) != proposed.get(key):
            problems.append(f"top-level '{key}' changed")

    b_joins = baseline.get("joins") or []
    p_joins = proposed.get("joins") or []
    if p_joins[: len(b_joins)] != b_joins:
        problems.append("an existing join changed")
    extra_joins = {
        j.get("name") for j in p_joins[len(b_joins) :] if isinstance(j, dict)
    }
    if extra_joins != added_joins:
        problems.append(
            f"unexpected joins added: {sorted(map(str, extra_joins ^ added_joins))}"
        )

    b_meas = baseline.get("measures") or []
    p_meas = proposed.get("measures") or []
    if len(p_meas) != len(b_meas) + len(added_measures):
        problems.append(
            f"measure count {len(p_meas)} != baseline {len(b_meas)} + {len(added_measures)} added"
        )
    for b, p in zip(b_meas, p_meas):
        if (
            not isinstance(b, dict)
            or not isinstance(p, dict)
            or b.get("name") != p.get("name")
        ):
            problems.append(
                f"measure order changed at '{b.get('name') if isinstance(b, dict) else b}'"
            )
            continue
        if b["name"] in replaced_measures:
            keep = set(b) | set(p)
            keep -= set(_COMPUTE_FIELDS) | {"comment"}
            if any(b.get(k) != p.get(k) for k in keep):
                problems.append(f"measure '{b['name']}' changed beyond expr/window")
            b_comment = _FP_TOKEN_RE.sub("", str(b.get("comment") or "")).rstrip(" ·")
            p_comment = _FP_TOKEN_RE.sub("", str(p.get("comment") or "")).rstrip(" ·")
            if b_comment != p_comment:
                problems.append(
                    f"measure '{b['name']}' comment changed beyond its {FINGERPRINT_PREFIX} token"
                )
        elif b != p:
            problems.append(f"untouched measure '{b['name']}' changed")
    tail = {m.get("name") for m in p_meas[len(b_meas) :] if isinstance(m, dict)}
    if tail != added_measures:
        problems.append(
            f"unexpected measures added: {sorted(map(str, tail ^ added_measures))}"
        )
    return problems
