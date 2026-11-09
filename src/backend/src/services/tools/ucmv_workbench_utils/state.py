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
        return float(value) if isinstance(value, (int, float, str)) and not isinstance(value, bool) else 0.0
    except ValueError:
        return 0.0


def _count(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _visual_count(item: dict[str, object]) -> int:
    used = item.get("used_in_visuals")
    return len(used) if isinstance(used, list) else 0


@dataclass
class ViewState:
    key: str
    yaml: str
    mapping: dict[str, object]
    untranslatable: list[dict[str, object]] = field(default_factory=list)
    reconciliation: dict[str, dict[str, object]] = field(default_factory=dict)  # measure -> entry
    reconciled: bool = False
    deployed: str = ""  # "" | "ok" | error text
    edited: set[str] = field(default_factory=set)


@dataclass
class Workbench:
    views: dict[str, ViewState]
    classified: dict[tuple[str, str], dict[str, str]] = field(default_factory=dict)
    log: list[str] = field(default_factory=list)

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
        for item in items if isinstance(items, list) else []:
            if isinstance(item, dict) and str(item.get("table_key")) in views:
                views[str(item.get("table_key"))].untranslatable.append(item)
        return cls(views)

    # ── reads ──────────────────────────────────────────────────────────────

    def view(self, key: str) -> ViewState:
        if key in self.views:
            return self.views[key]
        match = [k for k in self.views if _norm(k) == _norm(key)]
        if len(match) == 1:
            return self.views[match[0]]
        raise edits.EditError(f"no view {key!r}; views: {', '.join(sorted(self.views))}")

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
            if isinstance(m, dict) and m.get("ucmv_measure") in names and m.get("pbi_kind") != "unresolved":
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
        return [(m, e) for m, e in v.reconciliation.items() if not self._is_closed(v.key, m)]

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
            {"measure": m, "pct_aligned": e.get("pct_aligned"), "classification": e.get("classification")}
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
            return {"view": v.key, "yaml": v.yaml, "measures": edits.measure_names(v.yaml)}
        entry = edits.measure_entry(v.yaml, measure)
        dax = next(
            (_text(i.get("dax_expression")) for i in v.untranslatable if _norm(_text(i.get("original_name"))) == _norm(measure)),
            "",
        )
        return {"view": v.key, "measure": measure, "entry": entry, "dax": dax[:_MAX_DAX],
                "reconciliation": v.reconciliation.get(measure)}

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
    ) -> str:
        v = self.view(key)
        v.yaml, replaced = edits.set_measure(
            v.yaml, name, expr, display_name=display_name or None, fmt=fmt, comment=comment or None
        )
        if pbi_measure:
            v.mapping = edits.upsert_mapping_measure(v.mapping, name, pbi_measure)
        v.edited.add(name)
        v.reconciliation.pop(name, None)
        self.classified.pop((v.key, name), None)
        self.log.append(f"{'replaced' if replaced else 'added'} {v.key}.{name}")
        return "replaced" if replaced else "added"

    def remove_measure(self, key: str, name: str) -> None:
        v = self.view(key)
        v.yaml = edits.remove_measure(v.yaml, name)
        v.mapping = edits.drop_mapping_measure(v.mapping, name)
        v.reconciliation.pop(name, None)
        self.log.append(f"removed {v.key}.{name}")

    def classify(self, key: str, measure: str, classification: str, note: str) -> None:
        v = self.view(key)
        if classification not in CLASSIFICATIONS:
            raise edits.EditError(f"classification must be one of {', '.join(CLASSIFICATIONS)}")
        if not note.strip():
            raise edits.EditError("a classification needs a note (what was checked, why it is not a translation error)")
        self.classified[(v.key, measure)] = {"classification": classification, "note": note.strip()}
        self.log.append(f"classified {v.key}.{measure} as {classification}")

    def record_reconciliation(self, key: str, measures: dict[str, dict[str, object]]) -> None:
        v = self.view(key)
        v.reconciliation = {str(m): dict(e) for m, e in measures.items() if isinstance(e, dict)}
        v.reconciled = True

    # ── report ─────────────────────────────────────────────────────────────

    def report(self) -> dict[str, object]:
        views = self.overview()
        open_total = sum(_count(r.get("untranslated_open")) + _count(r.get("below_100_open")) for r in views)
        unreconciled = [r["view"] for r in views if not r["reconciled"]]
        return {
            "done": open_total == 0 and not unreconciled,
            "open_gaps": open_total,
            "views_not_reconciled": unreconciled,
            "views": views,
            "classified": [
                {"view": k, "measure": m, **c} for (k, m), c in sorted(self.classified.items())
            ],
            "changes": list(self.log),
        }
