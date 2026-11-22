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
# The same column reference, with the table token captured: (table, column).
_QUAL_COL_REF = re.compile(r"(?:'([^']+)'|([A-Za-z_]\w*))\[([^\]]+)\]")

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


def extract_qualified_refs(dax: str) -> set[tuple[str, str]]:
    """Table-qualified column references `Table[col]` as normalized (table, column) pairs."""
    if not dax:
        return set()
    return {
        (_norm(m.group(1) or m.group(2)), _norm(m.group(3)))
        for m in _QUAL_COL_REF.finditer(dax)
    }


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


def build_measure_qualified_index(
    mapping: list[dict],
) -> dict[str, set[tuple[str, str]]]:
    """normalized measure name -> the table-qualified column references of its own DAX."""
    idx: dict[str, set[tuple[str, str]]] = {}
    for m in mapping:
        refs = extract_qualified_refs(
            m.get("dax_expression") or m.get("expression") or ""
        )
        for name in (m.get("measure_name"), m.get("original_name"), m.get("name")):
            if name:
                idx[_norm(name)] = refs
    return idx


def resolve_qualified_refs(
    dax: str,
    qual_index: Mapping[str, set[tuple[str, str]]],
    mref_index: Mapping[str, set[str]] | None = None,
    depth: int = 3,
) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    """(own, helper): the qualified column references of the measure's DAX, and those of the
    measures it references (transitively, up to ``depth`` hops, cycle-guarded)."""
    mref_index = mref_index or {}
    own = extract_qualified_refs(dax)
    helper: set[tuple[str, str]] = set()
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
            helper |= qual_index.get(ref, set())
            nxt |= mref_index.get(ref, set())
        frontier = nxt - seen
    return own, helper


# Who owns a table-qualified reference. FACT: a candidate fact with known columns. DIM: a model
# table with known columns that is not a fact. UNKNOWN: anything else, judged by its column name as
# before. IGNORED: a referenced measure's qualifier on a non-fact table (see _Owners.helper).
_FACT, _DIM, _UNKNOWN, _IGNORED = "fact", "dim", "unknown", "ignored"


@dataclass(frozen=True)
class _Owners:
    # normalized table -> fact key, for candidate facts with known columns
    facts: Mapping[str, str]
    # normalized tables the model flags as facts
    fact_tables: frozenset[str]
    # normalized tables whose columns are known (a source view or the parsed columns)
    structured: frozenset[str]

    def own(self, table: str) -> tuple[str, str | None]:
        """A measure's own `Table[col]`: a reference to a candidate fact is a fact reference, a
        reference to a known non-fact table is a dimension one, anything else is judged by name.
        """
        if table in self.facts:
            return _FACT, self.facts[table]
        if table in self.structured and table not in self.fact_tables:
            return _DIM, None
        return _UNKNOWN, None

    def helper(self, table: str) -> tuple[str, str | None]:
        """A referenced measure's `Table[col]`: only a fact contributes. A date helper on a
        non-fact table is not a second fact, so it is ignored."""
        if table in self.facts and table in self.fact_tables:
            return _FACT, self.facts[table]
        if table in self.structured and table not in self.fact_tables:
            return _IGNORED, None
        return _UNKNOWN, None


def _referenced(
    own: set[tuple[str, str]],
    helper: set[tuple[str, str]],
    owners: _Owners,
) -> tuple[set[str], set[str], set[str], set[str], set[str]]:
    """Column names a measure reads, split by who owns them.

    Returns (eff, fact_refs, dim_only, own_fact_keys, helper_fact_keys): ``eff`` is every name the
    measure is judged by; ``fact_refs`` the names read through a fact's qualifier (they must be
    owned by one fact); ``dim_only`` the names read only through a dimension's qualifier, or only
    by a referenced measure on a non-fact table (they are not fact columns); the two key sets are
    the facts the own and the helper references name.
    """
    fact_refs: set[str] = set()
    other: set[str] = set()
    dim: set[str] = set()
    own_fact_keys: set[str] = set()
    helper_fact_keys: set[str] = set()
    for table, col in own:
        kind, key = owners.own(table)
        if kind == _FACT and key is not None:
            fact_refs.add(col)
            own_fact_keys.add(key)
        elif kind == _DIM:
            dim.add(col)
        else:
            other.add(col)
    for table, col in helper:
        kind, key = owners.helper(table)
        if kind == _FACT and key is not None:
            fact_refs.add(col)
            helper_fact_keys.add(key)
        elif kind == _UNKNOWN:
            other.add(col)
        elif kind == _IGNORED:
            # Not a fact column, but still a known column: the measure is not "broken" on it.
            dim.add(col)
    eff = fact_refs | other | dim
    dim_only = dim - fact_refs - other
    return eff, fact_refs, dim_only, own_fact_keys, helper_fact_keys


def _home_keeps_measure(
    home: str,
    own: set[tuple[str, str]],
    own_fact_keys: set[str],
    helper_fact_keys: set[str],
    known_cols: set[str],
    fact_index: Mapping[str, set[str]],
) -> bool:
    """Whether the references say the measure reads its home's columns, so it stays there.

    (a) every own column reference is qualified by the home, and the columns are known to the
        model; or
    (b) the home owns every column the measure reads directly, and no own reference names another
        fact.
    Either way a referenced measure that reads a different fact keeps the verdict open.
    """
    if helper_fact_keys - {home}:
        return False
    own_names = {col for _, col in own}
    if not own_names or own_fact_keys - {home}:
        return False
    if all(table == _norm(home) for table, _ in own) and own_names <= known_cols:
        return True
    return own_names <= fact_index.get(home, set())


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
    *,
    fact_refs: set[str] | None = None,
    dim_only_refs: set[str] | None = None,
) -> AllocationDecision:
    """Decide the home of a measure from the columns it references.

    ``fact_refs`` are names read through a fact's qualifier: they are fact columns even when no
    single fact owns them (that is the cross-fact evidence). ``dim_only_refs`` are names read only
    through a dimension's qualifier: they are not fact columns, whatever fact view has the name.
    """
    referenced = set(effective_cols)
    if not referenced:
        return AllocationDecision(kind="no_columns", reason="no column references")

    forced = set(fact_refs or ()) & known_cols
    skipped = set(dim_only_refs or ())
    fact_resident = {
        c
        for c in referenced
        if c in forced
        or (c not in skipped and any(c in cols for cols in fact_index.values()))
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
    _, mref_index = build_measure_column_index(mapping)
    qual_index = build_measure_qualified_index(mapping)

    # A fact whose columns we could not parse (e.g. a plain `SELECT *` source)
    # exposes an UNKNOWN column set. We must not judge such a fact's measures:
    # a referenced column could live there, so it is neither "broken" nor safely
    # re-homed. Match only against facts with KNOWN columns, and disable
    # broken/cross-fact verdicts while any wildcard fact is present.
    wildcard_facts = {k for k, cols in full_index.items() if not cols}
    fact_index = {k: cols for k, cols in full_index.items() if cols}
    has_wildcard = bool(wildcard_facts)
    owners = _Owners(
        facts={_norm(k): k for k in fact_index},
        fact_tables=frozenset(
            _norm(k) for k, t in mquery_tables.items() if getattr(t, "is_fact", False)
        ),
        structured=frozenset(
            _norm(k)
            for k, t in mquery_tables.items()
            if _real_or_parsed(k, t, source_columns)
        ),
    )

    for i, m in enumerate(mapping):
        dax = m.get("dax_expression") or m.get("expression") or ""
        own, helper = resolve_qualified_refs(dax, qual_index, mref_index)
        eff, fact_refs, dim_only, own_fact_keys, helper_fact_keys = _referenced(
            own, helper, owners
        )
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
        # A home with no source view emits nothing, so it is not kept on the references alone.
        viewed_home = not source_columns or home in source_columns
        if (
            viewed_home
            and home in fact_index
            and _home_keeps_measure(
                home, own, own_fact_keys, helper_fact_keys, known_cols, fact_index
            )
        ):
            continue  # the references read the home's columns: nothing to re-home
        decision = allocate(
            eff,
            fact_index,
            grain_index,
            known_cols,
            fact_refs=fact_refs,
            dim_only_refs=dim_only,
        )
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
