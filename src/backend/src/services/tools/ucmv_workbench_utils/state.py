"""The workbench: the generated metric views as a working copy, the open gaps, the last
reconciliation per view and the agent's classifications, plus the finishing report.

Gaps of a view are (a) measures the generator could not translate, with their DAX, reason and
proposal, and (b) measures below 100 % in the last reconciliation. A gap is closed when its
measure reconciles at 100 % or is classified (``data_drift`` | ``pbi_defect`` | ``structural`` |
``out_of_scope``) with a note, which is the same bar the reference migration process uses.
Outputs carry shares, counts, names and definitions, never data values.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

from . import edits

CLASSIFICATIONS = ("data_drift", "pbi_defect", "structural", "out_of_scope")
_MAX_GAPS = 15
_MAX_DAX = 1200
_NON_WORD = re.compile(r"[^a-z0-9]+")


def _norm(name: str) -> str:
    return _NON_WORD.sub("_", str(name).lower()).strip("_")


def _text(value: object) -> str:
    return value if isinstance(value, str) else ""


def _pct(entry: dict[str, object]) -> float:
    value = entry.get("pct_aligned")
    try:
        return (
            float(value)
            if isinstance(value, (int, float, str)) and not isinstance(value, bool)
            else 0.0
        )
    except ValueError:
        return 0.0


def _count(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _mapped_pbi(mapping: dict[str, object], name: str) -> str:
    """The Power BI measure a mapping entry compares the measure with, '' when there is none."""
    entries = mapping.get("measures")
    for m in entries if isinstance(entries, list) else []:
        if isinstance(m, dict) and m.get("ucmv_measure") == name:
            return _text(m.get("pbi_measure"))
    return ""


def _visual_count(item: dict[str, object]) -> int:
    used = item.get("used_in_visuals")
    return len(used) if isinstance(used, list) else 0


@dataclass
class ViewState:
    key: str
    yaml: str
    mapping: dict[str, object]
    untranslatable: list[dict[str, object]] = field(default_factory=list)
    reconciliation: dict[str, dict[str, object]] = field(
        default_factory=dict
    )  # measure -> entry
    reconciled: bool = False
    deployed: str = ""  # "" | "ok" | error text
    edited: set[str] = field(
        default_factory=set
    )  # edited since the last reconciliation
    # The comparison's view-level facts (summary, not_compared, periods_not_in_power_bi, ...)
    # and the measures it could not compare, with the reason: an open gap, never a closed one.
    report: dict[str, object] = field(default_factory=dict)
    not_compared: dict[str, str] = field(default_factory=dict)
    changed: bool = False  # changed by the workbench since the incoming review


@dataclass
class Workbench:
    views: dict[str, ViewState]
    classified: dict[tuple[str, str], dict[str, str]] = field(default_factory=dict)
    log: list[str] = field(default_factory=list)
    # Incoming generator output, kept for the review the export carries.
    resolved: dict[str, object] = field(default_factory=dict)
    review_bar: object = None
    cycles: list[object] = field(default_factory=list)
    # Untranslated items of tables that have no metric view in the output (they are listed in
    # the review, and no measure of the workbench can close them).
    orphans: list[dict[str, object]] = field(default_factory=list)

    @classmethod
    def from_generator_output(cls, output: dict[str, object]) -> "Workbench":
        yamls = output.get("yaml")
        mappings = output.get("pbi_ucmv_mapping")
        items = output.get("untranslatable_items")
        views: dict[str, ViewState] = {}
        for key, text in (yamls.items() if isinstance(yamls, dict) else []):
            if isinstance(text, str) and text.strip():
                raw = mappings.get(key) if isinstance(mappings, dict) else None
                views[str(key)] = ViewState(str(key), text, edits.mapping_dict(raw))
        orphans: list[dict[str, object]] = []
        for item in items if isinstance(items, list) else []:
            if not isinstance(item, dict):
                continue
            if str(item.get("table_key")) in views:
                views[str(item.get("table_key"))].untranslatable.append(item)
            else:
                orphans.append(item)
        resolved = output.get("resolved_measures_by_table")
        cycles = output.get("cycles")
        bench = cls(
            views,
            resolved=resolved if isinstance(resolved, dict) else {},
            review_bar=output.get("review_bar_pct"),
            cycles=list(cycles) if isinstance(cycles, list) else [],
            orphans=orphans,
        )
        # Crew 2b already compared the views with Power BI: start from its result, so the
        # gaps below 100 % are known without reconciling every view again.
        recon = output.get("reconciliation")
        recon_views = recon.get("views") if isinstance(recon, dict) else None
        for key, data in (recon_views.items() if isinstance(recon_views, dict) else []):
            measures = data.get("measures") if isinstance(data, dict) else None
            if str(key) in views and isinstance(measures, dict) and measures:
                bench.record_reconciliation(str(key), measures, data)
        return bench

    # ── reads ──────────────────────────────────────────────────────────────

    def view(self, key: str) -> ViewState:
        if key in self.views:
            return self.views[key]
        match = [k for k in self.views if _norm(k) == _norm(key)]
        if len(match) == 1:
            return self.views[match[0]]
        raise edits.EditError(
            f"no view {key!r}; views: {', '.join(sorted(self.views))}"
        )

    def _is_closed(self, key: str, measure: str) -> bool:
        if (key, measure) in self.classified:
            return True
        entry = self.views[key].reconciliation.get(measure)
        return entry is not None and _pct(entry) >= 100

    def _covered_pbi_names(self, v: ViewState) -> set[str]:
        """Normalised Power BI measure names a measure of the view now reproduces: the measure's own
        name, or the ``pbi_measure`` of its resolved mapping entry."""
        names = set(edits.measure_names(v.yaml))
        covered = {_norm(n) for n in names}
        entries = v.mapping.get("measures")
        for m in entries if isinstance(entries, list) else []:
            if (
                isinstance(m, dict)
                and m.get("ucmv_measure") in names
                and m.get("pbi_kind") != "unresolved"
            ):
                covered.add(_norm(_text(m.get("pbi_measure"))))
        return covered

    def _open_untranslated(self, v: ViewState) -> list[dict[str, object]]:
        covered = self._covered_pbi_names(v)
        out = []
        for item in v.untranslatable:
            original = _text(item.get("original_name"))
            if _norm(original) in covered or (v.key, original) in self.classified:
                continue
            out.append(item)
        return out

    def _below_100(self, v: ViewState) -> list[tuple[str, dict[str, object]]]:
        """Open measures of a reconciled view: below 100 %, not compared (the comparison left
        them out: a prior-year measure whose second pass failed, a timed-out query), or edited
        since the last comparison. None of them is closed by silence."""
        open_rows: list[tuple[str, dict[str, object]]] = [
            (m, e) for m, e in v.reconciliation.items() if not self._is_closed(v.key, m)
        ]
        for m, reason in v.not_compared.items():
            if m not in v.reconciliation and (v.key, m) not in self.classified:
                open_rows.append(
                    (m, {"classification": "not_compared", "reason": reason})
                )
        for m in sorted(v.edited):
            if m not in v.reconciliation and (v.key, m) not in self.classified:
                open_rows.append((m, {"classification": "edited_not_compared"}))
        return open_rows

    def overview(self) -> list[dict[str, object]]:
        rows = []
        for key, v in sorted(self.views.items()):
            rec = v.reconciliation
            at_100 = sum(1 for e in rec.values() if _pct(e) >= 100)
            rows.append(
                {
                    "view": key,
                    "measures": len(edits.measure_names(v.yaml)),
                    "untranslated_open": len(self._open_untranslated(v)),
                    "reconciled": v.reconciled,
                    "measures_at_100": at_100 if v.reconciled else None,
                    "below_100_open": len(self._below_100(v)) if v.reconciled else None,
                    "classified": sum(1 for (k, _) in self.classified if k == key),
                    "deployed": v.deployed or "not yet",
                }
            )
        return rows

    def gaps(self, key: str) -> dict[str, object]:
        v = self.view(key)
        untranslated = [
            {
                "pbi_measure": _text(i.get("original_name")),
                "dax": _text(i.get("dax_expression"))[:_MAX_DAX],
                "reason": _text(i.get("skip_reason")),
                "category": _text(i.get("category")),
                "proposal": _text(i.get("proposal")),
                "used_in_visuals": _visual_count(i),
            }
            for i in self._open_untranslated(v)
        ]
        below = [
            {
                "measure": m,
                "pct_aligned": e.get("pct_aligned"),
                "classification": e.get("classification"),
            }
            for m, e in self._below_100(v)
        ]
        return {
            "view": v.key,
            "untranslated": untranslated[:_MAX_GAPS],
            "untranslated_total": len(untranslated),
            "below_100": below[:_MAX_GAPS],
            "below_100_total": len(below),
            "reconciled": v.reconciled,
        }

    def show(self, key: str, measure: Optional[str] = None) -> dict[str, object]:
        v = self.view(key)
        if not measure:
            return {
                "view": v.key,
                "yaml": v.yaml,
                "measures": edits.measure_names(v.yaml),
            }
        entry = edits.measure_entry(v.yaml, measure)
        dax = next(
            (
                _text(i.get("dax_expression"))
                for i in v.untranslatable
                if _norm(_text(i.get("original_name"))) == _norm(measure)
            ),
            "",
        )
        return {
            "view": v.key,
            "measure": measure,
            "entry": entry,
            "dax": dax[:_MAX_DAX],
            "reconciliation": v.reconciliation.get(measure),
        }

    # ── writes ─────────────────────────────────────────────────────────────

    def set_measure(
        self,
        key: str,
        name: str,
        expr: str,
        pbi_measure: str = "",
        *,
        display_name: str = "",
        fmt: Optional[dict[str, object]] = None,
        comment: str = "",
        clear_window: bool = False,
    ) -> str:
        v = self.view(key)
        v.yaml, replaced = edits.set_measure(
            v.yaml,
            name,
            expr,
            display_name=display_name or None,
            fmt=fmt,
            comment=comment or None,
            clear_window=clear_window,
        )
        # The window (kept unless cleared) decides whether the reconciliation compares the
        # measure by its calendar in the second pass; the mapping entry must say so.
        shifted = self.has_window(v.key, name)
        pbi = pbi_measure or _mapped_pbi(v.mapping, name)
        if pbi:
            v.mapping = edits.upsert_mapping_measure(
                v.mapping, name, pbi, time_shifted=shifted
            )
        v.edited.add(name)
        v.changed = True
        v.reconciliation.pop(name, None)
        self.classified.pop((v.key, name), None)
        window_note = "window kept" if shifted else "no window"
        self.log.append(
            f"{'replaced' if replaced else 'added'} {v.key}.{name} ({window_note})"
        )
        return "replaced" if replaced else "added"

    def has_window(self, key: str, name: str) -> bool:
        """Whether the measure is a prior-year measure (it carries a window) in the working copy."""
        entry = edits.measure_entry(self.view(key).yaml, name)
        return bool(entry and entry.get("window"))

    def remove_measure(self, key: str, name: str) -> None:
        v = self.view(key)
        v.yaml = edits.remove_measure(v.yaml, name)
        v.mapping = edits.drop_mapping_measure(v.mapping, name)
        v.reconciliation.pop(name, None)
        v.changed = True
        self.log.append(f"removed {v.key}.{name}")

    def review_untranslated(self, key: str) -> list[dict[str, object]]:
        """The untranslated items of a view as the review lists them. A view the workbench has
        not changed keeps the generator's list; a changed view drops the items its edits now
        translate (they are listed as the measure instead)."""
        v = self.view(key)
        return self._open_untranslated(v) if v.changed else list(v.untranslatable)

    def changed_views(self) -> list[str]:
        """Views the workbench changed since the incoming review (edited, removed, deployed,
        or reconciled here)."""
        return sorted(k for k, v in self.views.items() if v.changed)

    def classify(self, key: str, measure: str, classification: str, note: str) -> None:
        v = self.view(key)
        if classification not in CLASSIFICATIONS:
            raise edits.EditError(
                f"classification must be one of {', '.join(CLASSIFICATIONS)}"
            )
        if not note.strip():
            raise edits.EditError(
                "a classification needs a note (what was checked, why it is not a translation error)"
            )
        self.classified[(v.key, measure)] = {
            "classification": classification,
            "note": note.strip(),
        }
        self.log.append(f"classified {v.key}.{measure} as {classification}")

    def record_reconciliation(
        self,
        key: str,
        measures: dict[str, dict[str, object]],
        report: Optional[dict[str, object]] = None,
    ) -> None:
        """Store a view's comparison. ``report`` is the view-level part of the comparison
        (summary, not_compared, periods_not_in_power_bi, timed_out_measures); the measures
        the comparison left out are open gaps (see ``_below_100``)."""
        v = self.view(key)
        v.reconciliation = {
            str(m): dict(e) for m, e in measures.items() if isinstance(e, dict)
        }
        extras = {k: val for k, val in (report or {}).items() if k != "measures"}
        v.report = extras
        not_compared = extras.get("not_compared")
        v.not_compared = {
            str(m): str(reason)
            for m, reason in (
                not_compared.items() if isinstance(not_compared, dict) else []
            )
        }
        v.edited = set()
        v.reconciled = True

    # ── report ─────────────────────────────────────────────────────────────

    def report(self) -> dict[str, object]:
        views = self.overview()
        open_total = sum(
            _count(r.get("untranslated_open")) + _count(r.get("below_100_open"))
            for r in views
        )
        unreconciled = [r["view"] for r in views if not r["reconciled"]]
        return {
            "done": open_total == 0 and not unreconciled,
            "open_gaps": open_total,
            "views_not_reconciled": unreconciled,
            "views": views,
            "classified": [
                {"view": k, "measure": m, **c}
                for (k, m), c in sorted(self.classified.items())
            ],
            "changes": list(self.log),
        }
