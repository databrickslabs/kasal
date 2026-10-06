"""UCMV drift check: deployed metric views vs today's Power BI model → patch proposal.

    baseline (UC)  ─┐
                    ├─ match ─ classify ─ [semantic check] ─ translate drifted ─ patch ─ verify
    extraction (PBI)┘

Every external effect is injected so the whole check runs offline in tests:

  sql_fn(statement)            -> SQL Statement API result (reads the deployed YAML)
  generate_fn(measures_subset) -> UC Metric View Generator output for that subset
  complete_fn(system, user)    -> raw LLM text (legacy semantic check), or None

READ-ONLY by design: nothing here deploys. The report's ``yaml`` key is shaped for the
Metric View Deployer (tool 88), so applying a reviewed proposal goes through the
normal deploy path and its post-deploy DESCRIBE guard rail.
"""

import copy
import logging
import re
from collections import Counter
from typing import Optional

from src.services.tools.metric_view_utils.drift import diff as D
from src.services.tools.metric_view_utils.drift.baseline import (
    BaselineView,
    SqlFn,
    fetch_baseline,
)
from src.services.tools.metric_view_utils.drift.candidates import (
    GenerateFn,
    build_candidates,
    pick_candidate,
    referenced_aliases,
)
from src.services.tools.metric_view_utils.drift.matcher import (
    match_measures,
    to_pbi_measures,
)
from src.services.tools.metric_view_utils.drift.patcher import (
    Replacement,
    apply_patch,
    with_fingerprint,
)
from src.services.tools.metric_view_utils.drift.semantic_check import (
    CompleteFn,
    judge_legacy_measures,
)

logger = logging.getLogger(__name__)

_TRANSLATE = (D.NEW, D.CHANGED, D.POSSIBLY_CHANGED)
_MEASURE_CALL_RE = re.compile(r"\bMEASURE\(\s*`?([A-Za-z_]\w*)`?\s*\)", re.IGNORECASE)


def _join_names(joins: list) -> set[str]:
    """Every join alias, nested snowflake joins included."""
    out: set[str] = set()
    for j in joins or []:
        if isinstance(j, dict) and j.get("name"):
            out.add(str(j["name"]))
            out |= _join_names(j.get("joins") or [])
    return out


def _ddl(full_name: str, yaml_text: str) -> str:
    return f"CREATE OR REPLACE VIEW {full_name}\nWITH METRICS\nLANGUAGE YAML\nAS $$\n{yaml_text.strip()}\n$$"


def _translation_origin(entry: dict) -> str:
    """ "llm" when the generator's provenance tag says the LLM produced the SQL."""
    return "llm" if "LLM[" in str(entry.get("comment") or "") else "deterministic"


def _drop_dangling_measure_refs(
    planned: list[tuple], baseline_measures: set
) -> list[tuple]:
    """Hold back a patch whose SQL uses MEASURE(x) when x will not exist in the view.

    x may be a baseline measure or another measure added in the same proposal; a
    reference to anything else would deploy a broken view. Iterates because
    dropping one addition can strand another that referenced it.
    """
    kept = list(planned)
    while True:
        available = set(baseline_measures) | {
            str(p.get("name")) for _, p, _ in kept if isinstance(p, dict)
        }
        dropped = False
        for item in list(kept):
            d, payload, _ = item
            expr = str(
                (payload if isinstance(payload, dict) else payload.entry).get("expr")
                or ""
            )
            dangling = sorted(set(_MEASURE_CALL_RE.findall(expr)) - available)
            if dangling:
                d.note = f"references measure(s) {dangling} that this view does not have — apply manually"
                kept.remove(item)
                dropped = True
        if not dropped:
            return kept


def run_drift_check(
    view_names: list[str],
    extraction: dict,
    sql_fn: SqlFn,
    generate_fn: GenerateFn,
    complete_fn: Optional[CompleteFn] = None,
    apply_suspected_changes: bool = False,
    semantic_batch_size: int = 20,
    max_semantic_measures: int = 300,
) -> dict:
    baselines = [fetch_baseline(n, sql_fn) for n in view_names]
    pbi = to_pbi_measures(extraction.get("measures_json") or [])
    match = match_measures(baselines, pbi)
    drifts = D.classify(match, pbi)
    warnings: list[str] = []

    semantic: dict = {"enabled": complete_fn is not None}
    if complete_fn is not None:
        semantic.update(
            judge_legacy_measures(
                drifts, complete_fn, semantic_batch_size, max_semantic_measures
            )
        )

    # Translate every measure whose SQL must be (re)produced, once each.
    by_name = {p.original_name: p for p in pbi}
    target_names = {
        d.original_name for d in drifts if d.status in _TRANSLATE and d.view
    }
    targets = [by_name[n] for n in sorted(target_names) if by_name[n].dax.strip()]
    cands = build_candidates(targets, pbi, generate_fn) if targets else None
    if cands and cands.generator_warning:
        warnings.append(f"UC Metric View Generator: {cands.generator_warning}")

    views_out = [
        _patch_view(v, drifts, cands, apply_suspected_changes) for v in baselines
    ]

    unassigned = [d.to_dict() for d in drifts if d.status == D.UNASSIGNED]
    counts = Counter(d.status for d in drifts)
    yaml_out = {v["name"]: v["proposed_yaml"] for v in views_out if v.get("changes")}
    deploy_ddl = {
        v["full_name"]: _ddl(v["full_name"], v["proposed_yaml"])
        for v in views_out
        if v.get("changes")
    }
    targets_cs = {(v["catalog"], v["schema"]) for v in views_out if v.get("changes")}
    if len(targets_cs) > 1:
        warnings.append(
            "Proposed views span several catalog.schema targets — the Metric View "
            "Deployer deploys to ONE catalog.schema per run; deploy each group "
            "separately or use the per-view deploy_ddl."
        )

    report = {
        "drift_monitor": True,
        "views": views_out,
        "unassigned": unassigned,
        "semantic_check": semantic,
        "summary": {
            "views_checked": len(baselines),
            "views_unreadable": sum(1 for v in baselines if not v.ok),
            "views_with_proposals": len(yaml_out),
            "unchanged": counts[D.UNCHANGED] + counts[D.UNCHANGED_LLM],
            "changed_in_pbi": counts[D.CHANGED],
            "possibly_changed": counts[D.POSSIBLY_CHANGED],
            "legacy_unverified": counts[D.LEGACY],
            "new_in_pbi": counts[D.NEW],
            "unassigned": counts[D.UNASSIGNED],
            "removed_from_pbi": counts[D.REMOVED],
            "baseline_only": counts[D.BASELINE_ONLY],
            "measures_added": sum(v["counts"]["measures_added"] for v in views_out),
            "measures_updated": sum(v["counts"]["measures_updated"] for v in views_out),
            "joins_added": sum(v["counts"]["joins_added"] for v in views_out),
            "untranslated": sum(v["counts"]["untranslated"] for v in views_out),
            "pbi_measures": len(pbi),
        },
        "settings": {"apply_suspected_changes": apply_suspected_changes},
        # Metric View Deployer handoff (tool 88 reads `yaml` from its ucmv_output).
        "yaml": yaml_out,
        "deploy_ddl": deploy_ddl,
        "warnings": warnings,
    }
    if len(targets_cs) == 1:
        report["catalog"], report["schema_name"] = next(iter(targets_cs))
    logger.info(
        "[UCMVDrift] %s view(s): %s new, %s changed, %s possibly changed, %s removed, %s unassigned",
        len(baselines),
        counts[D.NEW],
        counts[D.CHANGED],
        counts[D.POSSIBLY_CHANGED],
        counts[D.REMOVED],
        counts[D.UNASSIGNED],
    )
    return report


def _patch_view(view: BaselineView, drifts: list, cands, apply_suspected: bool) -> dict:
    mine = [d for d in drifts if d.view == view.full_name]
    out = {
        "full_name": view.full_name,
        "catalog": view.catalog,
        "schema": view.schema,
        "name": view.name,
        "error": view.error,
        "source": view.spec.get("source") if view.ok else None,
        "baseline_yaml": view.yaml_text,
        "proposed_yaml": view.yaml_text,
        "changes": [],
        "invariant_problems": [],
        "measures": [],
        "counts": {
            "measures_added": 0,
            "measures_updated": 0,
            "joins_added": 0,
            "untranslated": 0,
        },
    }
    if not view.ok:
        return out

    taken = {
        str(m.get("name"))
        for m in view.spec.get("measures") or []
        if isinstance(m, dict)
    }
    taken |= {
        str(d.get("name"))
        for d in view.spec.get("dimensions") or []
        if isinstance(d, dict)
    }
    measure_names = {
        str(m.get("name"))
        for m in view.spec.get("measures") or []
        if isinstance(m, dict)
    }
    aliases = _join_names(view.spec.get("joins") or [])
    # (drift, new entry | Replacement, joins it needs) — pruned below, then applied.
    planned: list[tuple] = []

    for d in mine:
        if d.status not in _TRANSLATE:
            continue
        if cands is None:
            continue
        cand = pick_candidate(
            cands.by_name.get(d.original_name, []), d.allocations, aliases
        )
        if cand is None:
            d.note = cands.untranslated.get(d.original_name, "not translated")
            out["counts"]["untranslated"] += 1
            continue
        entry = copy.deepcopy(cand.entry)
        d.proposed_expr = str(entry.get("expr") or "")
        d.translation = _translation_origin(entry)

        missing = referenced_aliases(d.proposed_expr) - aliases
        needed = [j for j in cand.joins if j.get("name") in missing]
        unresolvable = missing - {j["name"] for j in needed}
        # An alias the fresh view has as a NESTED join can't be added as a flat join.
        nested = unresolvable & _join_names(cand.joins)
        if nested:
            d.note = f"needs nested join(s) {sorted(nested)} — apply manually"
            continue

        if d.status == D.POSSIBLY_CHANGED and not apply_suspected:
            d.note = "suggested replacement shown; not applied (apply_suspected_changes=false)"
            continue
        if d.status == D.NEW:
            name = str(entry.get("name"))
            if name in taken:
                d.note = (
                    f"name '{name}' already used in this view — rename before applying"
                )
                continue
            entry["comment"] = with_fingerprint(
                str(entry.get("comment") or ""), d.current_fingerprint
            )
            d.measure_name = name
            taken.add(name)
            planned.append((d, entry, needed))
        else:
            planned.append(
                (
                    d,
                    Replacement(d.measure_name or "", entry, d.current_fingerprint),
                    needed,
                )
            )

    planned = _drop_dangling_measure_refs(planned, measure_names)
    adds = [p for _, p, _ in planned if isinstance(p, dict)]
    replaces = [p for _, p, _ in planned if isinstance(p, Replacement)]
    joins: list[dict] = []
    for _, _, needed in planned:
        for j in needed:
            if j["name"] not in {x["name"] for x in joins}:
                joins.append(j)
    applied = [d for d, _, _ in planned]

    if adds or replaces or joins:
        patch = apply_patch(view.yaml_text, adds, replaces, joins)
        if patch.ok:
            out["proposed_yaml"] = patch.yaml_text
            out["changes"] = patch.changes
            for d in applied:
                d.applied = True
            out["counts"]["measures_added"] = len(adds)
            out["counts"]["measures_updated"] = len(replaces)
            out["counts"]["joins_added"] = len(joins)
        else:
            out["invariant_problems"] = patch.problems
            for d in applied:
                d.note = "patch rejected by the baseline-preservation check — see view problems"
    out["measures"] = [d.to_dict() for d in mine]
    return out
