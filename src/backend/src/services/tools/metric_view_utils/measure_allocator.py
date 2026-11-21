"""Column-based measure allocation (KASAL_FIXES M6 / M7 / M10).

A PBI measure belongs to the fact table whose **columns its DAX references**,
NOT to the table it happens to "live on" in the PBI model, and NOT to a
catch-all when a fact clearly owns it. This module decides where each measure
belongs by looking only at the physical columns (and, transitively, the columns
of the measures) its DAX touches, and matching them against the known columns of
each fact.

Three failure modes it fixes (see the OTC post-mortem):

- **M6** — a measure whose referenced columns all live on one fact was left in
  the ``none_allocated`` catch-all. Home it on that fact.
- **M7** — a measure was homed on the table it *lives on* in PBI (e.g. a SKU
  table) while its DAX references another fact's columns. Re-home it on the fact
  the columns come from.
- **M10** — a measure references a column that exists in *no* table (a measure
  broken in PBI itself). Flag it and keep it out of every fact so it is
  documented as skipped, never emitted as a valid measure.

Cross-fact ratios (referenced columns genuinely span two facts with no single
fact containing all of them, e.g. an NPS-fact numerator over a CustomerExp-fact
denominator) are detected explicitly and reported with the shared-grain columns
common to both facts, rather than silently dropped.

Everything here is PURE and tenant-agnostic — no OTC names, no pipeline imports.
The orchestrator ``reallocate_by_columns`` mutates a ``mapping`` list's
allocation keys in place so the existing pipeline routing
(``group_by_table`` → ``process_table``) picks up the corrected homes with no
further wiring.
"""

from __future__ import annotations

import copy
import re
from collections.abc import Mapping
from dataclasses import dataclass, field

from .data_classes import TableInfo

# ── Regexes ───────────────────────────────────────────────────────────────
# A COLUMN reference is `Table[Col]` or `'Table Name'[Col]` — a table token
# immediately precedes the bracket.
_COL_REF = re.compile(r"(?:'[^']+'|[A-Za-z_]\w*)\[([^\]]+)\]")
# Any bracketed token, used to find MEASURE references (`[Measure]` with no
# table token in front of the `[`).
_ANY_REF = re.compile(r"\[([^\]]+)\]")

# Allocation-role markers stamped on a mapping entry so downstream routing +
# the none_allocated catch-all can react without re-deriving anything.
BROKEN_KEY = "_allocation_broken"
CROSS_FACT_KEY = "_allocation_cross_fact"
REHOMED_KEY = "_allocation_rehomed"
UNASSIGNED = "__unassigned__"


def _norm(col: str) -> str:
    """Case/space-insensitive column key so `NPSResponses`, `NPS Responses` and
    `npsresponses` all compare equal (source columns are frequently mixed-case
    while DAX refs are not)."""
    return re.sub(r"\s+", "", col.strip().strip("\"'`")).lower()


# ── Reference extraction ────────────────────────────────────────────────────
def extract_column_refs(dax: str) -> set[str]:
    """Physical column references (`Table[col]`), normalized. Excludes bare
    `[Measure]` references (no table token)."""
    if not dax:
        return set()
    return {_norm(m.group(1)) for m in _COL_REF.finditer(dax)}


def extract_measure_refs(dax: str) -> set[str]:
    """Bare `[Measure]` references (no table token before the bracket), returned
    normalized. These are measures, not columns."""
    if not dax:
        return set()
    refs: set[str] = set()
    for m in _ANY_REF.finditer(dax):
        before = dax[: m.start()].rstrip()
        # A column ref has a word char or a closing quote right before `[`.
        if before and re.search(r"[\w']$", before):
            continue
        refs.add(_norm(m.group(1)))
    return refs


# ── Index builders ──────────────────────────────────────────────────────────
def _table_columns(tinfo: TableInfo) -> set[str]:
    """All known column keys exposed by a TableInfo (aggregate outputs + their
    inner source columns + group-by + calculated columns)."""
    cols: set[str] = set()
    for c in getattr(tinfo, "aggregate_columns", None) or []:
        if isinstance(c, dict):
            if c.get("name"):
                cols.add(_norm(c["name"]))
            if c.get("source_col"):
                cols.add(_norm(c["source_col"]))
    for c in getattr(tinfo, "group_by_columns", None) or []:
        cols.add(_norm(c))
    for c in getattr(tinfo, "calculated_columns", None) or []:
        if isinstance(c, dict) and c.get("name"):
            cols.add(_norm(c["name"]))
    return cols


def candidate_fact_keys(mquery_tables: dict, mapping: list[dict]) -> set[str]:
    """Tables that a measure may be homed on: real facts, plus any table named
    as an allocation target in the mapping — restricted to ones the parser gave
    a real ``source_table`` (nothing invented)."""
    keys: set[str] = set()
    for k, t in mquery_tables.items():
        if getattr(t, "source_table", None) and getattr(t, "is_fact", False):
            keys.add(k)
    intended: set[str] = set()
    for m in mapping:
        for alloc in m.get("all_allocations", []) or []:
            if alloc.get("table"):
                intended.add(alloc["table"])
        if m.get("proposed_allocation"):
            intended.add(m["proposed_allocation"])
    for k in intended:
        t = mquery_tables.get(k)
        if t is not None and getattr(t, "source_table", None):
            keys.add(k)
    keys.discard(UNASSIGNED)
    return keys


def _real_or_parsed(
    key: str, tinfo: TableInfo, source_columns: Mapping[str, list[str]] | None
) -> set[str]:
    """A table's column keys: its source view's real columns when they are known, else what the
    parser read from its Power Query (a view's real columns are what a metric view references).
    """
    real = (source_columns or {}).get(key)
    if real:
        return {_norm(c) for c in real}
    return _table_columns(tinfo)


def build_fact_column_index(
    mquery_tables: dict,
    fact_keys: set[str],
    source_columns: Mapping[str, list[str]] | None = None,
) -> dict[str, set[str]]:
    """fact_key → set of normalized column keys it exposes (its real source-view columns when
    ``source_columns`` has them, else the parsed ones)."""
    return {
        k: _real_or_parsed(k, mquery_tables[k], source_columns)
        for k in fact_keys
        if k in mquery_tables
    }


def build_fact_grain_index(
    mquery_tables: dict, fact_keys: set[str]
) -> dict[str, list[str]]:
    """fact_key → its group-by (grain) columns, used to report the shared grain
    of a cross-fact ratio."""
    out: dict[str, list[str]] = {}
    for k in fact_keys:
        t = mquery_tables.get(k)
        if t is not None:
            out[k] = list(getattr(t, "group_by_columns", None) or [])
    return out


def build_known_columns(
    mquery_tables: dict, source_columns: Mapping[str, list[str]] | None = None
) -> set[str]:
    """Every column key across ALL tables (facts + dimensions). A referenced
    column absent from this set exists nowhere → the measure is broken (M10)."""
    known: set[str] = set()
    for key, t in mquery_tables.items():
        known |= _real_or_parsed(key, t, source_columns)
    return known


def build_measure_column_index(
    mapping: list[dict],
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """Two indexes keyed by normalized measure name:
    - ``cols``:  the physical columns the measure's own DAX references, and
    - ``mrefs``: the other measures it references.
    Together they let a ratio-of-measures resolve to the columns behind the
    measures it composes."""
    cols_idx: dict[str, set[str]] = {}
    mref_idx: dict[str, set[str]] = {}
    for m in mapping:
        dax = m.get("dax_expression") or m.get("expression") or ""
        cols = extract_column_refs(dax)
        mrefs = extract_measure_refs(dax)
        for name in (
            m.get("measure_name"),
            m.get("original_name"),
            m.get("name"),
        ):
            if name:
                cols_idx[_norm(name)] = cols
                mref_idx[_norm(name)] = mrefs
    return cols_idx, mref_idx


def resolve_effective_columns(
    dax: str,
    cols_index: dict[str, set[str]],
    mref_index: dict[str, set[str]] | None = None,
    depth: int = 3,
) -> set[str]:
    """Columns a measure effectively depends on: its own column refs plus,
    transitively (up to ``depth`` hops, cycle-guarded), the columns of every
    measure it references."""
    mref_index = mref_index or {}
    cols = set(extract_column_refs(dax))
    seen: set[str] = set()
    frontier = set(extract_measure_refs(dax))
    for _ in range(max(0, depth)):
        if not frontier:
            break
        nxt: set[str] = set()
        for ref in frontier:
            if ref in seen:
                continue
            seen.add(ref)
            cols |= cols_index.get(ref, set())
            nxt |= mref_index.get(ref, set())
        frontier = nxt - seen
    return cols


# ── Decision ────────────────────────────────────────────────────────────────
@dataclass
class AllocationDecision:
    """Where a measure belongs, by referenced columns.

    kind:
      - ``single``     — exactly one fact owns all fact-resident columns → ``fact``
      - ``cross_fact`` — columns span ≥2 facts, no single fact owns all → ``facts``
      - ``broken``     — every referenced column exists in no table (M10)
      - ``ambiguous``  — ≥2 facts each own all columns (cannot choose safely)
      - ``no_columns`` — no fact-resident columns referenced (artifact / dim-only)
    """

    kind: str
    fact: str | None = None
    facts: list[str] = field(default_factory=list)
    referenced_columns: set[str] = field(default_factory=set)
    fact_columns: set[str] = field(default_factory=set)
    unknown_columns: set[str] = field(default_factory=set)
    shared_grain: list[str] = field(default_factory=list)
    reason: str = ""


def allocate(
    effective_cols: set[str],
    fact_index: dict[str, set[str]],
    grain_index: dict[str, list[str]],
    known_cols: set[str],
) -> AllocationDecision:
    """Decide the home of a measure from the columns it references."""
    referenced = set(effective_cols)
    if not referenced:
        return AllocationDecision(kind="no_columns", reason="no column references")

    fact_resident = {
        c for c in referenced if any(c in cols for cols in fact_index.values())
    }
    dim_only = {c for c in referenced if c in known_cols} - fact_resident
    unknown = referenced - known_cols

    if not fact_resident:
        if unknown and not dim_only:
            return AllocationDecision(
                kind="broken",
                referenced_columns=referenced,
                unknown_columns=unknown,
                reason=(
                    "references column(s) that exist in no table: "
                    f"{sorted(unknown)} — broken in the PBI model"
                ),
            )
        return AllocationDecision(
            kind="no_columns",
            referenced_columns=referenced,
            reason="no fact-resident columns (dimension-only or artifact)",
        )

    facts_full = [k for k, cols in fact_index.items() if fact_resident <= cols]
    if len(facts_full) == 1:
        f = facts_full[0]
        return AllocationDecision(
            kind="single",
            fact=f,
            facts=[f],
            referenced_columns=referenced,
            fact_columns=fact_resident,
            unknown_columns=unknown,
            reason=f"all referenced fact columns {sorted(fact_resident)} live on {f}",
        )
    if len(facts_full) > 1:
        return AllocationDecision(
            kind="ambiguous",
            facts=sorted(facts_full),
            referenced_columns=referenced,
            fact_columns=fact_resident,
            reason=f"columns {sorted(fact_resident)} present on multiple facts {sorted(facts_full)}",
        )

    # No single fact owns all fact-resident columns → cross-fact.
    contributing = sorted(k for k, cols in fact_index.items() if fact_resident & cols)
    shared: set[str] | None = None
    for k in contributing:
        g = {_norm(c) for c in grain_index.get(k, [])}
        shared = g if shared is None else (shared & g)
    shared_cols = sorted(shared or set())
    return AllocationDecision(
        kind="cross_fact",
        facts=contributing,
        referenced_columns=referenced,
        fact_columns=fact_resident,
        shared_grain=shared_cols,
        reason=(
            f"cross-fact: columns span {contributing}; "
            + (
                f"shared grain {shared_cols} — home on a shared-grain view/column"
                if shared_cols
                else "no shared grain column detected"
            )
        ),
    )


# ── Orchestrator ─────────────────────────────────────────────────────────────
def _current_home(m: dict) -> str | None:
    """The fact a measure is currently allocated to (primary), if any."""
    for alloc in m.get("all_allocations", []) or []:
        if alloc.get("role") == "primary" and alloc.get("table"):
            return str(alloc["table"])
    # Fall back to first allocation, then proposed_allocation.
    allocs = m.get("all_allocations") or []
    if allocs and allocs[0].get("table"):
        return str(allocs[0]["table"])
    prop = m.get("proposed_allocation")
    if prop and prop != UNASSIGNED:
        return str(prop)
    return None


def _set_home(m: dict, fact: str | None) -> None:
    m["all_allocations"] = [{"table": fact, "role": "primary"}]
    m["proposed_allocation"] = fact


def measure_home(m: dict) -> str | None:
    """The table a measure is homed on (its primary allocation), None when it has none."""
    return _current_home(m)


def move_measure_home(m: dict, fact: str) -> None:
    """Home a measure on ``fact`` (and nowhere else), recording where it came from."""
    home = _current_home(m)
    _set_home(m, fact)
    m[REHOMED_KEY] = {"from": home, "to": fact}


def _clear_home(m: dict) -> None:
    m["all_allocations"] = []
    m["proposed_allocation"] = UNASSIGNED


def _parsed_placements(
    mapping: list[dict], mquery_tables: dict, config: dict | None
) -> tuple[list[str | None], dict[str, set[str]]]:
    """Where each measure lands from the parsed columns alone, i.e. the allocation without the real
    columns, and the parsed column index of the candidate facts. The mapping is not changed.
    """
    trial = copy.deepcopy(mapping)
    reallocate_by_columns(trial, mquery_tables, config=config)
    homes = [_current_home(m) for m in trial]
    fact_keys = candidate_fact_keys(mquery_tables, mapping)
    return homes, build_fact_column_index(mquery_tables, fact_keys)


def _placement_confirmed(
    eff: set[str],
    kept: str | None,
    parsed_index: Mapping[str, set[str]],
    source_columns: Mapping[str, list[str]],
) -> tuple[bool, set[str]]:
    """Whether the real columns of the view ``kept`` confirm the columns the no-columns placement
    relies on: the measure's columns that ``kept`` owns by the parsed columns. Returns the verdict
    and those columns. A view with no real columns, or no column the placement relies on, confirms
    nothing (the measure is then judged by the real columns as before)."""
    if kept is None or not source_columns.get(kept):
        return False, set()
    owned = parsed_index.get(kept)
    if not owned:
        return False, set()
    needed = {c for c in eff if c in owned}
    if not needed:
        return False, needed
    real = {_norm(c) for c in source_columns[kept]}
    return needed <= real, needed


def _keep_placement(
    m: dict, home: str | None, kept: str, needed: set[str], report: dict, name: str
) -> None:
    """Home the measure on ``kept`` (its no-columns placement) when it is not there already."""
    if kept == home:
        return
    entry = {
        "measure": name,
        "to": kept,
        "columns": sorted(needed),
        "basis": "no-columns placement, confirmed by the view's real columns",
    }
    _set_home(m, kept)
    if home is None:
        report["rescued"].append(entry)
    else:
        m[REHOMED_KEY] = {"from": home, "to": kept}
        report["rehomed"].append({**entry, "from": home})


def reallocate_by_columns(
    mapping: list[dict],
    mquery_tables: dict,
    config: dict | None = None,
    source_columns: Mapping[str, list[str]] | None = None,
) -> dict:
    """Correct each measure's fact allocation by its referenced columns.

    Mutates ``mapping`` entries in place (allocation keys + ``_allocation_*``
    markers) so the existing pipeline routing homes measures correctly, then
    returns a summary for logging / limitations. Conservative by design:

    - A measure whose current home already owns its referenced columns is left
      untouched (no churn for correctly-allocated tenants).
    - **M6** — an un-homed measure whose columns all live on one fact is homed
      there.
    - **M7** — a measure homed on a fact that does NOT own its columns, while
      exactly one other fact does, is re-homed to that fact.
    - **M10** — a measure whose every referenced column exists nowhere is
      un-homed and marked broken (documented, never emitted).
    - Cross-fact ratios are un-homed and marked with the contributing facts and
      shared grain.
    """
    report: dict[str, list] = {
        "rescued": [],
        "rehomed": [],
        "broken": [],
        "cross_fact": [],
        "ambiguous": [],
    }
    if not mapping or not mquery_tables:
        return report

    # With real columns, the placement the parsed columns give is the reference: a measure keeps it
    # where the real columns of that view confirm the columns the placement relies on (see
    # _placement_confirmed). Computed before the mapping is changed below.
    placed = (
        _parsed_placements(mapping, mquery_tables, config) if source_columns else None
    )

    fact_keys = candidate_fact_keys(mquery_tables, mapping)
    if not fact_keys:
        return report
    full_index = build_fact_column_index(mquery_tables, fact_keys, source_columns)
    grain_index = build_fact_grain_index(mquery_tables, fact_keys)
    known_cols = build_known_columns(mquery_tables, source_columns)
    cols_index, mref_index = build_measure_column_index(mapping)

    # A fact whose columns we could not parse (e.g. a plain `SELECT *` source)
    # exposes an UNKNOWN column set. We must not judge such a fact's measures:
    # a referenced column could live there, so it is neither "broken" nor safely
    # re-homed. Match only against facts with KNOWN columns, and disable
    # broken/cross-fact verdicts while any wildcard fact is present.
    wildcard_facts = {k for k, cols in full_index.items() if not cols}
    fact_index = {k: cols for k, cols in full_index.items() if cols}
    has_wildcard = bool(wildcard_facts)

    for i, m in enumerate(mapping):
        dax = m.get("dax_expression") or m.get("expression") or ""
        eff = resolve_effective_columns(dax, cols_index, mref_index)
        if not eff:
            continue  # artifact / measure-only with no resolvable columns
        name = m.get("original_name") or m.get("measure_name") or m.get("name") or ""
        home = _current_home(m)
        if home in wildcard_facts:
            continue  # can't validate against an unknown-column source
        if placed is not None:
            kept, parsed_index = placed
            kept_home = kept[i]
            confirmed, needed = _placement_confirmed(
                eff, kept_home, parsed_index, source_columns or {}
            )
            if confirmed and kept_home is not None:
                _keep_placement(m, home, kept_home, needed, report, name)
                continue
        decision = allocate(eff, fact_index, grain_index, known_cols)
        home_cols = fact_index.get(home, set()) if home else set()
        home_owns = bool(decision.fact_columns) and decision.fact_columns <= home_cols

        if decision.kind == "single":
            if home == decision.fact or home_owns:
                continue  # already correct — leave untouched
            if home is None:
                _set_home(m, decision.fact)
                report["rescued"].append(
                    {
                        "measure": name,
                        "to": decision.fact,
                        "columns": sorted(decision.fact_columns),
                    }
                )
            else:
                _set_home(m, decision.fact)
                m[REHOMED_KEY] = {"from": home, "to": decision.fact}
                report["rehomed"].append(
                    {
                        "measure": name,
                        "from": home,
                        "to": decision.fact,
                        "columns": sorted(decision.fact_columns),
                    }
                )
        elif decision.kind == "broken":
            if has_wildcard:
                continue  # column may live on an unparsed (wildcard) source
            _clear_home(m)
            m[BROKEN_KEY] = {
                "unknown_columns": sorted(decision.unknown_columns),
                "reason": decision.reason,
            }
            report["broken"].append(
                {"measure": name, "unknown_columns": sorted(decision.unknown_columns)}
            )
        elif decision.kind == "cross_fact":
            # If the current home already owns all columns, some tenants model a
            # shared-grain fact holding both — respect that and don't disturb it.
            if home_owns or has_wildcard:
                continue
            _clear_home(m)
            m[CROSS_FACT_KEY] = {
                "facts": decision.facts,
                "shared_grain": decision.shared_grain,
                "reason": decision.reason,
            }
            report["cross_fact"].append(
                {
                    "measure": name,
                    "facts": decision.facts,
                    "shared_grain": decision.shared_grain,
                }
            )
        elif decision.kind == "ambiguous":
            # Known source views: a home that has no view of its own emits nothing, so when
            # exactly one of the facts that own the columns has a view, the measure goes there.
            viewed = [f for f in decision.facts if f in (source_columns or {})]
            if source_columns and home not in source_columns and len(viewed) == 1:
                _set_home(m, viewed[0])
                entry = {
                    "measure": name,
                    "to": viewed[0],
                    "columns": sorted(decision.fact_columns),
                }
                if home is None:
                    report["rescued"].append(entry)
                else:
                    m[REHOMED_KEY] = {"from": home, "to": viewed[0]}
                    report["rehomed"].append({**entry, "from": home})
                continue
            report["ambiguous"].append({"measure": name, "facts": decision.facts})
        # "no_columns" → leave as-is (existing routing / catch-all handles it)

    return report
