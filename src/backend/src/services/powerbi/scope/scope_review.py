"""Bucketed review of a scope table: the open rows grouped by what the metric views already show about them,
one proposed decision per bucket, so a person decides buckets instead of rows.

Open rows: decision ``review`` or outside the vocabulary, ``keep`` / ``snapshot`` without ``mirrored_as``, ``drop``
with a mirror, and a ``drop`` that a mirrored measure still needs (its change would go unnoticed). Evidence (names
and definitions only, never values): metric view measures by name, display name, synonym, or a ``PBI measure: X``
note in their comment; metric view dimensions by column name; the views of the schema; for a building block, the
decisions of the measures it is load-bearing for. Bucket ids are stable for the same scope table: review, decide,
apply, review again (the remaining rows get new buckets).

Buckets (fixed ids): L1 / L2 link a mirror that exists; M1 a dropped SWITCH wrapper whose branches are mirrored
(fold); M2 dropped but built (keep); M3 dropped but a step of a mirrored measure (fold); B1-B5 building blocks by
what their users are; C1-C3 unused columns; G* measures without a mirror per table; T* / R* other open rows.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from src.services.powerbi.scope.decisions import DECISIONS
from src.services.powerbi.scope.parts import arr, obj, text_at

_SIGNED_OFF = tuple(d for d in DECISIONS if d != "review")
_MIRRORED = ("keep", "snapshot", "fold")
_EXAMPLES = 8
_MIN_TABLE_BUCKET = 5  # gap buckets per PBI table from this size on; smaller tables share one bucket
_NON_WORD = re.compile(r"[^a-z0-9]+")
_IDENTIFIER = re.compile(r"[A-Za-z_]\w*")
_COND_FORM = " CondForm"
_WRAPPER = re.compile(r"\b(?:SWITCH|wrapper)\b", re.I)
_PBI_MEASURE_NOTE = "PBI measure"  # "PBI measure: X" / "PBI measures: X, Y" in a metric view comment
_NAME_END = ".,;)'"

Row = dict[str, str]
Key = tuple[str, str, str]


def norm(name: str) -> str:
    return _NON_WORD.sub("_", name.lower()).strip("_")


@dataclass
class Bucket:
    id: str
    title: str
    proposal: str  # a decision, "link" (keep + the mirror found), or "" (a person decides)
    why: str
    rows: list[Row] = field(default_factory=list)


@dataclass
class Evidence:
    measures: dict[str, set[str]]  # normalized name -> {"<view>.<measure>"}
    columns: set[str]  # normalized dimension names and identifiers of dimension expressions
    tables: dict[str, set[str]]  # PBI table -> {view}
    views: set[str]  # every view / snapshot name of the schema
    decisions: dict[str, Row]  # PBI measure name -> its row
    notes: list[tuple[str, str]] = field(default_factory=list)  # (text after "PBI measure(s):", target)
    draft_users: dict[Key, str] = field(default_factory=dict)  # row key -> load_bearing_for of the draft


def _key(row: Mapping[str, str]) -> Key:
    return row.get("object_type", ""), row.get("pbi_table", ""), row.get("name", "")


def _note_tails(comment: str) -> list[str]:
    tails, start = [], comment.find(_PBI_MEASURE_NOTE)
    while start >= 0:
        colon = comment.find(":", start)
        if colon < 0:
            break
        tails.append(comment[colon + 1 :].strip())
        start = comment.find(_PBI_MEASURE_NOTE, colon)
    return tails


def evidence(
    rows: list[Row],
    specs: Mapping[str, Mapping[str, object]],
    *,
    table_mirrors: Mapping[str, Iterable[str]] | None = None,
    views: Iterable[str] = (),
    draft_rows: Iterable[Row] = (),
) -> Evidence:
    """Evidence from metric view specs (view name -> parsed YAML: measures, dimensions)."""
    measures: dict[str, set[str]] = defaultdict(set)
    columns: set[str] = set()
    notes: list[tuple[str, str]] = []
    for view, spec in specs.items():
        for measure in (obj(m) for m in arr(spec, "measures")):
            name = text_at(measure, "name")
            if not name:
                continue
            target = f"{view}.{name}"
            for alias in [name, text_at(measure, "display_name") or "", *map(str, arr(measure, "synonyms"))]:
                if alias:
                    measures[norm(alias)].add(target)
            notes += [(tail, target) for tail in _note_tails(text_at(measure, "comment") or "")]
        for dimension in (obj(d) for d in arr(spec, "dimensions")):
            dim_name = text_at(dimension, "name")
            if dim_name:
                columns.add(norm(dim_name))
                columns |= {norm(t) for t in _IDENTIFIER.findall(str(dimension.get("expr", "")))}
    decisions = {r["name"]: r for r in rows if r.get("object_type") == "measure"}
    tables = {t: set(v) for t, v in (table_mirrors or {}).items()}
    all_views = set(views) | set(specs)
    draft_users = {_key(r): r.get("load_bearing_for") or "" for r in draft_rows}
    return Evidence(dict(measures), columns, tables, all_views, decisions, notes, draft_users)


def _table_mirror(pbi_table: str, ev: Evidence) -> set[str]:
    found = ev.tables.get(pbi_table)
    if found:
        return found
    short = {v.split(".")[-1]: v for v in ev.views}
    names = (norm(pbi_table), f"pbi_{norm(pbi_table)}", f"src_{norm(pbi_table)}")
    return {short[n] for n in names if n in short}


def _named_in(tail: str, name: str) -> bool:
    """``name`` is the first measure of the note, or one after a comma / semicolon, and ends there."""
    for lead in ("", ", ", "; "):
        at = 0 if not lead else tail.find(lead + name)
        if at < 0 or (not lead and not tail.startswith(name)):
            continue
        end = at + len(lead) + len(name)
        if end == len(tail) or tail[end] in _NAME_END:
            return True
        if tail[end].isspace() and tail[end + 1 : end + 2] in ("", "(", "="):
            return True
    return False


def _mirrors(name: str, ev: Evidence) -> set[str]:
    found = {target for tail, target in ev.notes if _named_in(tail, name)}
    return found or ev.measures.get(norm(name), set())


def is_open(row: Mapping[str, str]) -> bool:
    decision, mirrored = row.get("decision", ""), (row.get("mirrored_as") or "").strip()
    if decision not in _SIGNED_OFF:
        return True
    kind = row.get("object_type")
    if kind in ("measure", "table") and decision in ("keep", "snapshot") and not mirrored:
        return True
    return kind == "measure" and decision == "drop" and bool(mirrored)


def _users(row: Mapping[str, str], ev: Evidence | None = None) -> list[str]:
    text = row.get("load_bearing_for")
    if text is None and ev is not None:
        text = ev.draft_users.get(_key(row), "")
    return [u.strip() for u in (text or "").split(";") if u.strip()]


def _is_wrapper(user: str, ev: Evidence) -> bool:
    row = ev.decisions.get(user)
    return "wrapper" in user.lower() or (row is not None and bool(_WRAPPER.search(row.get("reason", ""))))


def _feeds_mirror(row: Mapping[str, str], ev: Evidence, seen: frozenset[str] = frozenset()) -> bool:
    """A chain of plain steps from ``row`` reaches a measure with its own mirror. A wrapper or a Genie-pattern
    measure ends the chain: their branches and date plumbing are decided apart."""
    for user in _users(row, ev):
        target = ev.decisions.get(user)
        if user in seen or target is None or user.endswith(_COND_FORM) or _is_wrapper(user, ev):
            continue
        if target.get("decision") in ("keep", "snapshot") or (target.get("mirrored_as") or "").strip():
            return True
        if target.get("decision") in ("fold", "drop") and _feeds_mirror(target, ev, seen | {user}):
            return True
    return False


def _user_state(user: str, ev: Evidence, seen: frozenset[str] = frozenset()) -> str:
    row = ev.decisions.get(user)
    if row is None or row.get("decision") not in _SIGNED_OFF or user in seen:
        return "open"
    if (row.get("mirrored_as") or "").strip() or row.get("decision") in ("fold", "genie"):
        return "mirrored"
    if row.get("decision") in ("drop", "n/a"):
        return "mirrored" if _feeds_mirror(row, ev, seen | {user}) else "dropped"
    return "open"


def needed_drop(row: Mapping[str, str], ev: Evidence) -> bool:
    """A dropped measure without a mirror that a mirrored measure still needs."""
    return (
        row.get("object_type") == "measure"
        and row.get("decision") == "drop"
        and not _WRAPPER.search(row.get("reason", ""))
        and not (row.get("mirrored_as") or "").strip()
        and _feeds_mirror(row, ev, frozenset({row.get("name", "")}))
    )


def _measure_bucket(row: Mapping[str, str], ev: Evidence) -> tuple[str, str]:
    if row.get("decision") == "drop" and not (row.get("mirrored_as") or "").strip():
        return "M3", ""
    if row.get("decision") == "drop":
        return ("M1", "") if _WRAPPER.search(row.get("reason", "")) else ("M2", "")
    found = _mirrors(row.get("name", ""), ev)
    if found:
        return "L1", "; ".join(sorted(found))
    users = _users(row, ev)
    if users and all(u.endswith(_COND_FORM) for u in users):
        return "B1", ""
    if any(_is_wrapper(u, ev) for u in users):
        return "B5", ""
    if users:
        states = [_user_state(u, ev) for u in users]
        if all(s == "dropped" for s in states):
            return "B2", ""
        if all(s == "mirrored" for s in states):
            return "B3", ""
        return "B4", ""
    return f"G:{row.get('pbi_table', '')}", ""


def _row_bucket(row: Mapping[str, str], ev: Evidence) -> tuple[str, str]:
    kind = row.get("object_type")
    if kind == "measure" and row.get("decision") in _SIGNED_OFF:
        return _measure_bucket(row, ev)
    if kind == "table":
        found = _table_mirror(row.get("pbi_table", ""), ev)
        return ("L2", "; ".join(sorted(found))) if found else (f"T:{row.get('reason', '').split(':')[0][:60]}", "")
    if kind == "column" and row.get("decision") == "review":
        if row.get("reason", "").startswith("calculated column"):
            return "C3", ""
        return ("C1", "") if norm(row.get("name", "")) in ev.columns else ("C2", "")
    return f"R:{kind}:{row.get('reason', '')[:60]}", ""


_FIXED: dict[str, tuple[str, str, str]] = {
    "L1": (
        "measure has a metric view measure that names it in its comment, or of the same display name / name",
        "link",
        "the mirror exists, only mirrored_as was empty",
    ),
    "L2": ("table has a view in the schema", "link", "the mirror exists, only mirrored_as was empty"),
    "M1": (
        "dropped SWITCH / wrapper whose branch measures are mirrored",
        "fold",
        "mirrored through its branches: a change to the wrapper must reach them, a drop would hide it",
    ),
    "M2": (
        "dropped (unreached), but a metric view measure was built for it",
        "keep",
        "it is built; or drop it and take the measure out of the YAML",
    ),
    "M3": (
        "dropped, but a step of a mirrored measure (its users lead to a mirror)",
        "fold",
        "written into the mirror's SQL: as drop, a change to it would go unnoticed",
    ),
    "B1": (
        "building block used only by conditional formatting (CondForm)",
        "drop",
        "colours / thresholds of a visual; Genie shows no formatting",
    ),
    "B2": ("building block whose measures are all dropped", "drop", "nothing mirrored needs it"),
    "B3": (
        "building block whose measures are all mirrored (or genie / fold)",
        "fold",
        "written into the SQL of the measures that use it",
    ),
    "B4": (
        "building block with at least one open measure among its users",
        "",
        "follows its users: decide those first, then review again",
    ),
    "B5": (
        "branch measure of a SWITCH / wrapper, no mirror found by name",
        "",
        "the KPI itself: find its metric view measure (keep + link) or decide it like a gap",
    ),
    "C1": ("unused column that is already a metric view dimension", "keep", "Genie can already group / filter by it"),
    "C2": ("unused column, not a metric view dimension", "drop", "not drawn, not filtering, not built"),
    "C3": ("unused calculated column", "drop", "not drawn, not filtering; keep only if Genie needs it"),
}
_GAP_WHY = "drawn / filtering, nothing built: translate (keep), genie, or drop"
_OPEN_WHY = {"T": "table promised as keep / snapshot, no view found", "R": "open decision"}


def buckets(rows: list[Row], ev: Evidence) -> list[Bucket]:
    grouped: dict[str, list[Row]] = defaultdict(list)
    for row in rows:
        if is_open(row) or needed_drop(row, ev):
            key, mirror = _row_bucket(row, ev)
            entry = {**row, "_mirror": mirror}
            if row.get("load_bearing_for") is None:
                entry["load_bearing_for"] = "; ".join(_users(row, ev))
            grouped[key].append(entry)
    return _named(grouped)


def _named(grouped: dict[str, list[Row]]) -> list[Bucket]:
    result = [Bucket(k, t, p, w, grouped[k]) for k, (t, p, w) in _FIXED.items() if grouped.get(k)]
    gaps = sorted((k for k in grouped if k.startswith("G:")), key=lambda k: (-len(grouped[k]), k))
    small = [r for k in gaps if len(grouped[k]) < _MIN_TABLE_BUCKET for r in grouped[k]]
    for n, key in enumerate(k for k in gaps if len(grouped[k]) >= _MIN_TABLE_BUCKET):
        result.append(Bucket(f"G{n + 1}", f"measure without a mirror in {key[2:]}", "", _GAP_WHY, grouped[key]))
    if small:
        result.append(Bucket("G0", "measure without a mirror in smaller tables", "", _GAP_WHY, small))
    for prefix, letter in (("T:", "T"), ("R:", "R")):
        keys = sorted((k for k in grouped if k.startswith(prefix)), key=lambda k: (-len(grouped[k]), k))
        for n, key in enumerate(keys):
            result.append(Bucket(f"{letter}{n + 1}", key[len(prefix) :], "", _OPEN_WHY[letter], grouped[key]))
    return result


def apply(rows: list[Row], result: list[Bucket], choices: Mapping[str, str]) -> int:
    """Set the decision of every row in the chosen buckets (``accept`` = the bucket's proposal; ``link`` keeps the
    row and fills the mirror found). Returns how many rows changed."""
    by_id = {b.id: b for b in result}
    unknown = sorted(set(choices) - set(by_id))
    if unknown:
        raise ValueError(f"unknown bucket(s): {', '.join(unknown)}")
    index = {_key(r): r for r in rows}
    changed = 0
    for bucket_id, chosen in choices.items():
        bucket = by_id[bucket_id]
        decision = bucket.proposal if chosen == "accept" else chosen
        if not decision:
            raise ValueError(f"{bucket_id} has no proposal: choose a decision")
        if decision != "link" and decision not in _SIGNED_OFF:
            raise ValueError(f"{bucket_id}: {decision!r} is not a decision ({', '.join(_SIGNED_OFF)}) or link")
        for found in bucket.rows:
            row = index[_key(found)]
            if decision == "link":
                if not found.get("_mirror"):
                    raise ValueError(f"{bucket_id}: no mirror found to link, choose a decision")
                row["decision"] = row["decision"] if row.get("decision") in _MIRRORED else "keep"
                row["mirrored_as"] = found["_mirror"]
            else:
                row["decision"] = decision
            changed += 1
    return changed


def _label(row: Mapping[str, str]) -> str:
    kind, table, name = row.get("object_type", ""), row.get("pbi_table", ""), row.get("name", "")
    if kind == "table":
        return f"table {table}"
    return f"{kind} {table}[{name}]" if table else f"{kind} {name}"


def _hint(row: Mapping[str, str]) -> str:
    if row.get("_mirror"):
        return f" -> {row['_mirror']}"
    users = _users(row)
    return f" (for {', '.join(users[:3])}{' ...' if len(users) > 3 else ''})" if users else ""


def summary(result: list[Bucket]) -> list[dict[str, object]]:
    """One entry per bucket: id, rows, proposal, what, why, examples (names only)."""
    return [
        {
            "bucket": b.id,
            "rows": len(b.rows),
            "proposal": b.proposal or "(you decide)",
            "what": b.title,
            "why": b.why,
            "examples": [_label(r) + _hint(r) for r in b.rows[:_EXAMPLES]],
        }
        for b in result
    ]


def markdown(title: str, result: list[Bucket]) -> str:
    total = sum(len(b.rows) for b in result)
    lines = [
        f"# Scope review: {title}",
        "",
        f"{total} open row(s) in {len(result)} bucket(s).",
        "",
        "| Bucket | Rows | Proposal | What | Why |",
        "|---|---|---|---|---|",
    ]
    lines += [f"| {b.id} | {len(b.rows)} | {b.proposal or '(you decide)'} | {b.title} | {b.why} |" for b in result]
    for b in result:
        lines += ["", f"## {b.id}: {b.title} ({len(b.rows)})", ""]
        lines += [f"- {_label(r)}{_hint(r)}" for r in b.rows[:_EXAMPLES]]
        if len(b.rows) > _EXAMPLES:
            lines.append(f"- ... {len(b.rows) - _EXAMPLES} more")
    return "\n".join(lines) + "\n"
