"""Semantic sync: snapshot a Power BI model + report, compare two snapshots, and say what each change means.

A snapshot holds every semantic object — tables (M / DAX source), columns (DAX of
calculated columns), measures, calculation items, functions, shared Power Query
expressions, relationships, RLS filters, pages (visibility, reachable or not), the
fields each reachable page draws or filters on, and optional rows of small
"tracked" tables whose DATA carries meaning (a KPI status list). Each object
carries a hash of its normalized definition (DAX comments removed, whitespace
collapsed), so reformatting is no change. Only metadata: no measure is evaluated.

Every change is routed through the report's scope table (``scope_table``): a change
to a mirrored object says which Databricks object to update, a change to a dropped
one needs nothing, an object the scope table doesn't know needs triage (with the
closure's draft decision of today's model as a starting point). Objects listed as
ignored (with a reason) keep their row but need no action.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from src.services.powerbi.scope import report_reachability as rr
from src.services.powerbi.scope.closure import collect_seeds, navigation_texts
from src.services.powerbi.scope.decisions import MIRRORED, strip_comments
from src.services.powerbi.scope.parts import PartTree, arr
from src.services.powerbi.scope.tmdl_model import (
    REPORT_TABLE,
    Model,
    Relationship,
    add_report_measures,
    load_model,
)

_AROUND_PUNCTUATION = re.compile(r" ?([(),\[\]{}=<>+*/&|-]) ?")
_DAX_TYPES = ("column", "measure", "calculation_item", "function", "rls_filter")
_NO_SCOPE_ACTION = {
    "page": "check page reachability: pages a viewer reaches decide what is mirrored",
    "page_field": "triage: a field drawn on / filtering a reachable page changed (rebuild the scope draft)",
    "relationship": "check the joins of the metric views that use these tables",
    "expression": "check the tables loaded through this shared query",
    "tracked_row": "data of a tracked table changed: check what it gates (reload its snapshot if it has one)",
}
PROPOSAL_ACTIONS = (
    "update the mirror",
    "removed in Power BI",
    "check the Genie pattern",
    "triage",
)
_HIDDEN_VALUE = "<number>"  # a fractional number in a tracked table is never stored

Key = tuple[str, str, str]


@dataclass(frozen=True)
class SemanticObject:
    object_type: str
    pbi_table: str
    name: str
    definition: str
    definition_hash: str

    @property
    def key(self) -> Key:
        return self.object_type, self.pbi_table, self.name


def normalized(object_type: str, text: str | None) -> str:
    """Definition as compared: DAX without comments, whitespace collapsed and dropped next to punctuation."""
    body: str = text or ""
    if object_type in _DAX_TYPES:
        body = strip_comments(body)
    return _AROUND_PUNCTUATION.sub(lambda m: m.group(1), " ".join(body.split()))


def make(
    object_type: str, table: str, name: str, definition: str | None
) -> SemanticObject:
    text = normalized(object_type, definition)
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
    return SemanticObject(object_type, table, name, text, digest)


def model_objects(model: Model) -> list[SemanticObject]:
    objects = []
    for table in model.tables.values():
        if table.kind == REPORT_TABLE:
            continue
        objects.append(make("table", table.name, "", f"{table.kind}: {table.source}"))
        objects += [
            make("column", table.name, c, dax or "data column")
            for c, dax in table.columns.items()
        ]
        objects += [
            make("calculation_item", table.name, i, dax)
            for i, dax in table.calc_items.items()
        ]
    objects += [
        make("measure", model.home[name], name, model.measure(name))
        for name in model.home
    ]
    objects += [
        make("function", "", name, body) for name, body in model.functions.items()
    ]
    objects += [
        make("expression", "", name, body) for name, body in model.expressions.items()
    ]
    objects += [
        make(
            "relationship",
            ft,
            f"{ft}[{fc}] -> {tt}[{tc}]",
            "active" if active else "inactive",
        )
        for ft, fc, tt, tc, active in model.relationships
    ]
    objects += [
        make("rls_filter", table, role, dax) for role, table, dax in model.roles
    ]
    return objects


def report_objects(report: PartTree, model: Model) -> list[SemanticObject]:
    """Pages (visibility, reachable or not) and the fields each reachable page draws or filters on."""
    texts = navigation_texts(model, rr.navigation_measures(rr.load_visuals(report)))
    reach = rr.report_reachability(report, texts)
    objects = [
        make(
            "page",
            "",
            page.name,
            f"{'hidden' if page.hidden else 'visible'}; {rr.page_status(reach.reached.get(pid, []))}",
        )
        for pid, page in reach.pages.items()
    ]
    seeds = collect_seeds(report, model)
    for use, fields in (
        ("filter", seeds.filters),
        ("drawn", seeds.drawn),
    ):  # drawn last: it wins
        for key, page_names in fields.items():
            label = f"[{key[1]}]" if key[0] == "m" else f"{key[1]}[{key[2]}]"
            objects += [
                make("page_field", page_name, label, use) for page_name in page_names
            ]
    return objects


def _shown(value: object) -> str:
    """Text, integers, booleans and dates are stored; a fractional number never is."""
    if isinstance(value, float) and not value.is_integer():
        return _HIDDEN_VALUE
    return str(value)


def tracked_objects(
    table: str, rows: Iterable[Mapping[str, object]]
) -> list[SemanticObject]:
    """One object per row of a small table whose DATA is semantics, keyed by its first column."""
    objects = []
    for row in rows:
        values = [_shown(v) for v in row.values()]
        columns = [str(c).split("[")[-1].rstrip("]") for c in row]
        if values:
            body = "; ".join(f"{c}={v}" for c, v in zip(columns[1:], values[1:]))
            objects.append(make("tracked_row", table, values[0], body))
    return objects


def build_snapshot(
    tmdl_parts: list[dict] | PartTree,
    report_def: Mapping[str, object] | PartTree | None = None,
    *,
    relationships: list[Relationship] | None = None,
    tracked: Mapping[str, list[Mapping[str, object]]] | None = None,
) -> dict[str, object]:
    """``{taken_at_utc, objects: [...]}`` of a model (and its report, when given)."""
    model_tree = (
        tmdl_parts
        if isinstance(tmdl_parts, PartTree)
        else PartTree.from_parts(tmdl_parts)
    )
    report = (
        report_def
        if isinstance(report_def, PartTree)
        else PartTree.from_report_definition(report_def)
    )
    model = load_model(model_tree, relationships)
    if report:
        add_report_measures(model, report)
    objects = model_objects(model) + (report_objects(report, model) if report else [])
    objects += [
        o
        for table, rows in (tracked or {}).items()
        for o in tracked_objects(table, rows)
    ]
    unique = {
        o.key: o for o in objects
    }  # a field drawn and filtered on one page counts once
    return {
        "taken_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "objects": [asdict(o) for o in sorted(unique.values(), key=lambda o: o.key)],
    }


def _index(snapshot: Mapping[str, object], field: str) -> dict[Key, str]:
    return {
        (str(o.get("object_type")), str(o.get("pbi_table")), str(o.get("name"))): str(
            o.get(field, "")
        )
        for o in arr(snapshot, "objects")
        if isinstance(o, dict)
    }


def compare(
    old: Mapping[str, object], new: Mapping[str, object]
) -> list[dict[str, str]]:
    """Added / removed / changed objects, sorted by key, with both definitions."""
    before, after = _index(old, "definition_hash"), _index(new, "definition_hash")
    old_text, new_text = _index(old, "definition"), _index(new, "definition")
    changes = []
    for key in sorted(before.keys() | after.keys()):
        old_hash, new_hash = before.get(key), after.get(key)
        if old_hash == new_hash:
            continue
        change_type = (
            "added"
            if old_hash is None
            else "removed"
            if new_hash is None
            else "changed"
        )
        changes.append(
            {
                "change_type": change_type,
                "object_type": key[0],
                "pbi_table": key[1],
                "name": key[2],
                "old_definition": old_text.get(key, ""),
                "new_definition": new_text.get(key, ""),
            }
        )
    return changes


def _scope_index(
    scope_rows: Iterable[Mapping[str, str]],
) -> dict[Key, Mapping[str, str]]:
    index: dict[Key, Mapping[str, str]] = {}
    for row in scope_rows:
        if row.get("object_type") == "rls_filter":  # one row per table, roles in `name`
            for role in str(row.get("name", "")).split("; "):
                index[("rls_filter", row.get("pbi_table", ""), role)] = row
        else:
            index[
                (
                    row.get("object_type", ""),
                    row.get("pbi_table", ""),
                    row.get("name", ""),
                )
            ] = row
    return index


def _scope_row(
    change: Mapping[str, str], index: Mapping[Key, Mapping[str, str]]
) -> Mapping[str, str] | None:
    key = (change["object_type"], change["pbi_table"], change["name"])
    if change["object_type"] == "measure":  # unique by name; the home table can move
        return index.get(key) or next(
            (
                r
                for k, r in index.items()
                if k[0] == "measure" and k[2] == change["name"]
            ),
            None,
        )
    if change["object_type"] == "column":
        return index.get(key) or index.get(("table", change["pbi_table"], ""))
    if change["object_type"] == "tracked_row":
        return index.get(("table", change["pbi_table"], ""))
    return index.get(key)


def action(change: Mapping[str, str], row: Mapping[str, str] | None) -> str:
    if change["object_type"] in _NO_SCOPE_ACTION:
        return _NO_SCOPE_ACTION[change["object_type"]]
    if row is None:
        return "triage: not in the scope table (rebuild the scope draft for a decision)"
    decision, mirror = row.get("decision", ""), row.get("mirrored_as") or ""
    if decision in MIRRORED and change["change_type"] == "removed":
        return f"removed in Power BI: retire or rework {mirror or 'its mirror'}"
    if decision in MIRRORED:
        return f"update the mirror: {mirror or '(mirrored_as missing)'}"
    if decision == "genie":
        return "check the Genie pattern / examples that stand in for it"
    if decision == "review":  # still undecided: the change must reach a person, not wait silently
        return "triage: open decision in the scope table (decide it, then handle the change)"
    if decision in ("drop", "n/a"):
        return "none (not mirrored)"
    # a decision outside the vocabulary must not hide the change
    return f"triage: decision {decision!r} is not in the scope vocabulary (fix the scope table)"


def with_impact(
    changes: list[dict[str, str]],
    scope_rows: Iterable[Mapping[str, str]],
    draft_rows: Iterable[Mapping[str, str]] = (),
) -> list[dict[str, str]]:
    """Each change with its scope decision, mirror and action; ``draft_rows`` (today's closure draft)
    adds a draft decision where the scope table has none, or where a dropped object became needed."""
    index, draft = _scope_index(scope_rows), _scope_index(draft_rows)
    result = []
    for change in changes:
        row = _scope_row(change, index)
        text = _with_draft(action(change, row), row, _scope_row(change, draft))
        result.append(
            {
                **change,
                "scope_decision": (row or {}).get("decision", ""),
                "mirrored_as": (row or {}).get("mirrored_as", ""),
                "action": text,
            }
        )
    return result


def _with_draft(
    text: str, row: Mapping[str, str] | None, draft: Mapping[str, str] | None
) -> str:
    if draft is None or not text.startswith(
        ("triage: not in the scope table", "none (not mirrored)")
    ):
        return text
    reason = str(draft.get("reason", ""))[:120]
    if row is None:
        return f"triage: not in the scope table; scope draft: {draft.get('decision')} ({reason})"
    if row.get("decision") == "drop" and draft.get("decision") != "drop":
        return f"triage: dropped in the scope table, now {draft.get('decision')} in the scope draft ({reason})"
    return text


def ignore_rules(rules: Iterable[Mapping[str, str]] | None) -> dict[Key, str]:
    """``{(object_type, pbi_table, name): reason}``; an entry without a reason is rejected."""
    found: dict[Key, str] = {}
    for rule in rules or []:
        reason = str(rule.get("reason") or "").strip()
        if not reason:
            raise ValueError(f"every ignore entry needs a reason: {dict(rule)}")
        key = (
            str(rule.get("object_type", "")),
            str(rule.get("pbi_table") or ""),
            str(rule.get("name") or ""),
        )
        found[key] = reason
    return found


def with_ignores(
    changes: list[dict[str, str]], rules: Mapping[Key, str]
) -> list[dict[str, str]]:
    """Changes of ignored objects keep their row, with action ``none (ignored: ...)``."""
    for change in changes:
        reason = rules.get((change["object_type"], change["pbi_table"], change["name"]))
        if reason:
            change["action"] = f"none (ignored: {reason})"
    return changes


def needs_action(change: Mapping[str, str]) -> bool:
    return str(change.get("action", "")).startswith(PROPOSAL_ACTIONS)
