"""Iterative UCMV generation with reconciliation feedback.

Composes the three existing tools — the UC Metric View generator, the deployer,
and the reconciliation tool — into the ``run_reconciliation_loop`` control flow:
generate → deploy → reconcile → feed the failing measures back into the next
generation cycle, up to ``max_cycles`` or until a cell-alignment target is met.

The three tools are dependency-INJECTED (any object exposing ``_run(**kwargs) ->
json_str``), so the orchestration is unit-tested with fakes and never touches
Databricks or Power BI here. The caller — a crew/flow wiring, or the generator
when ``enable_reconciliation`` is set — constructs the real tools (the
reconciliation tool needs PBI/Databricks credentials at construction) and calls
this. Without it, generation stays single-pass, exactly as before.
"""

from __future__ import annotations

import dataclasses
import json
import logging
from collections.abc import Mapping
from typing import Optional, Protocol

from src.services.tools.metric_view_utils.raw_fields import (
    RawMap,
    as_int,
    as_list,
    as_map,
)
from src.services.tools.metric_view_utils.reconciliation import (
    run_reconciliation_loop,
)

logger = logging.getLogger(__name__)


class ToolRunner(Protocol):
    """A tool the loop drives: ``_run(**kwargs)`` returns a JSON string."""

    def _run(self, **kwargs: object) -> object: ...


def _loads(raw: object) -> RawMap:
    """Parse a tool's JSON string output into a dict; {} on anything unexpected."""
    if isinstance(raw, Mapping):
        return raw
    if not isinstance(raw, str):
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def _carry_forward_overrides(
    resolved_by_table: RawMap, specs_summary: RawMap, feedback: list[RawMap]
) -> dict[str, list[dict[str, object]]]:
    """Build ``manual_overrides`` reusing the prior cycle's SQL for every measure
    NOT flagged as a failure in ``feedback``.

    Returns ``{table_key: [{name, original_name, expr, comment}, ...]}``. The ONLY
    measures left out (so they get re-translated) are the ones the reconciler said
    missed — everything already reconciled (or classified as drift / not scored)
    is carried forward verbatim. Correctness-first: a measure name that failed is
    excluded even if the view→table map is incomplete, so a miss is never carried
    forward; the cost of being wrong here is a redundant re-translation, never a
    stale-but-wrong measure.
    """
    if not resolved_by_table or not feedback:
        return {}
    view_to_table: dict[object, object] = {
        as_map(info).get("view_name"): tk
        for tk, info in specs_summary.items()
        if as_map(info).get("view_name")
    }
    failing: set[tuple[object, object]] = set()
    for item in feedback:
        if not item.get("measure"):
            continue
        # tk may be None when the view→table map misses — kept as a name-only
        # guard so the measure is still excluded from carry-forward.
        failing.add((view_to_table.get(item.get("view")), item["measure"]))

    overrides: dict[str, list[dict[str, object]]] = {}
    for tk, rows in resolved_by_table.items():
        for row in as_list(rows):
            r = as_map(row)
            name = r.get("measure_name")
            expr = r.get("sql_expr")
            if not name or not expr:
                continue
            if (tk, name) in failing or (None, name) in failing:
                continue  # a missed measure → re-translate; do not carry forward
            overrides.setdefault(tk, []).append(
                {
                    "name": name,
                    "original_name": r.get("original_name") or name,
                    "expr": expr,
                    "comment": "carried forward (reconciled in prior cycle)",
                }
            )
    return overrides


def _positive_or(value: object, default: float) -> float:
    """``float(value)``, or ``default`` when the value is unset, falsy or not numeric."""
    if not value:
        return default
    try:
        return float(str(value))
    except ValueError:
        return default


def run_iterative_ucmv_generation(
    *,
    generator: ToolRunner,
    deployer: ToolRunner,
    reconciler: ToolRunner,
    gen_kwargs: Optional[Mapping[str, object]] = None,
    deploy_kwargs: Optional[Mapping[str, object]] = None,
    recon_kwargs: Optional[Mapping[str, object]] = None,
    max_cycles: int = 5,
    target_pct: float = 100.0,
    settings: Optional[Mapping[str, object]] = None,
) -> dict[str, object]:
    """Run the generate→deploy→reconcile→refine loop and return a JSON-able report.

    ``settings``: the generator's effective ``config_json``, ``dataset_id`` and
    ``workspace_id``. In a crew they sit in the tool's configuration, not in the
    call's kwargs; read mode and the mapping assembly need them (default: the
    values in ``gen_kwargs``).

    - ``generator._run(**gen_kwargs[, refinement_feedback])`` → output with
      ``yaml`` (dict of view→YAML) and ``pbi_ucmv_mapping`` (view→mapping).
    - ``deployer._run(yaml_specs_json=..., **deploy_kwargs)`` → deployment summary.
    - ``reconciler._run(mappings_json=..., **recon_kwargs)`` → reconciliation report.

    Returns ``{stop_reason, best_cycle, best_pct, cycles[], yaml, reconciliation}``.
    """
    gen_base: dict[str, object] = dict(gen_kwargs or {})
    deploy_base: dict[str, object] = dict(deploy_kwargs or {})
    recon_base: dict[str, object] = dict(recon_kwargs or {})

    # Carry-forward state: the prior cycle's translations + view→table map, so a
    # refine cycle reuses already-reconciled measures instead of re-translating
    # the whole model.
    resolved_state: RawMap = {}
    specs_state: RawMap = {}
    # Source layer (read mode): this cycle's DDL, and the fingerprint already deployed.
    source_layer: dict[str, object] = {"ddl": {}, "deployed_fp": "", "report": {}}
    context: dict[str, object] = {**gen_base, **dict(settings or {})}
    read_mode = _read_mode(context)
    mapping_state: dict[str, dict[str, str]] = {"gaps": {}}
    # The last generator output (for mappings of healed views) and the measures a
    # heal removed, which go back to the DAX LLM with their deploy error.
    heal_state: dict[str, object] = {"generated": {}, "feedback": []}

    def generate(feedback: Optional[list[RawMap]]) -> RawMap:
        nonlocal resolved_state, specs_state
        kw = dict(gen_base)
        if feedback:
            # The generator normalises this into the DAX LLM's per-measure hints.
            feedback = [
                *feedback,
                *(as_map(x) for x in as_list(heal_state["feedback"])),
            ]
            kw["refinement_feedback"] = json.dumps(feedback)
            # Incremental refinement: reuse SQL for every measure NOT flagged as a
            # failure, so only the missed measures hit the LLM again this cycle.
            overrides = _carry_forward_overrides(resolved_state, specs_state, feedback)
            if overrides:
                merged: dict[str, list[object]] = {
                    k: list(as_list(v))
                    for k, v in as_map(kw.get("manual_overrides")).items()
                }
                for tk, entries in overrides.items():
                    merged.setdefault(tk, []).extend(entries)
                kw["manual_overrides"] = merged
                carried_measures = sum(len(v) for v in overrides.values())
                carried_tables = len(overrides)
                logger.info(
                    "[UCMV] incremental refine: carrying forward %d measure(s) "
                    "across %d table(s); only failures re-translated",
                    carried_measures,
                    carried_tables,
                )
        out = _loads(generator._run(**kw))
        resolved_state = as_map(out.get("resolved_measures_by_table"))
        specs_state = as_map(out.get("specs_summary"))
        source_layer["ddl"] = as_map(out.get("source_layer_ddl"))
        heal_state["generated"] = out
        yaml_specs = as_map(out.get("yaml"))
        assembled, gaps = _assembled_mappings(yaml_specs, out, context, deploy_base)
        # A mapping the generator was given under the view's own key (supplied or
        # corrected by hand) still applies where none could be assembled.
        given = as_map(out.get("pbi_ucmv_mapping"))
        mappings = {**{k: v for k, v in given.items() if k in yaml_specs}, **assembled}
        mapping_state["gaps"] = {k: r for k, r in gaps.items() if k not in mappings}
        return {"yaml": yaml_specs, "mappings": mappings}

    def deploy(yaml_specs: RawMap) -> RawMap:
        if not yaml_specs:
            return {"ok": False, "error": "generator produced no YAML"}
        layer_note = ""
        if read_mode:
            layer_note = _deploy_layer_once(deployer, source_layer, deploy_base)
        out = _loads(
            deployer._run(yaml_specs_json=json.dumps(yaml_specs), **deploy_base)
        )
        verdict = _healed(
            deploy_outcome(out),
            yaml_specs,
            deployer,
            deploy_base,
            context,
            mapping_state,
            heal_state,
        )
        verdict = _with_mapping_gaps(verdict, mapping_state["gaps"])
        if layer_note:
            failures = dict(as_map(verdict.get("failures")))
            for table, reason in as_map(
                as_map(source_layer["report"]).get("failures")
            ).items():
                failures[f"source_layer:{table}"] = str(reason)
            verdict = {**verdict, "failures": failures}
            key = "note" if verdict.get("ok") else "error"
            verdict[key] = f"{layer_note}; {verdict.get(key) or ''}".strip("; ")[:1000]
        return verdict

    def reconcile(yaml_specs: RawMap, mappings: RawMap) -> RawMap:
        if not mappings:
            return {"views": {}, "overall": {"cells_total": 0}}
        return _loads(reconciler._run(mappings_json=json.dumps(mappings), **recon_base))

    result = run_reconciliation_loop(
        generate=generate,
        deploy=deploy,
        reconcile=reconcile,
        max_cycles=max_cycles,
        target_pct=target_pct,
    )
    return {
        "stop_reason": result.stop_reason,
        "best_cycle": result.best_cycle,
        "best_pct": result.best_pct,
        "cycles": [dataclasses.asdict(c) for c in result.cycles],
        "yaml": result.final_yaml,
        "reconciliation": result.final_reconciliation,
    }


# Power BI credential/id fields on the generator → the reconciliation tool's
# constructor kwargs (the tool keeps creds out of its LLM-facing schema).
_GEN_TO_RECON_PBI = {
    "workspace_id": "pbi_workspace_id",
    "dataset_id": "pbi_dataset_id",
    "tenant_id": "pbi_tenant_id",
    "client_id": "pbi_client_id",
    "client_secret": "pbi_client_secret",
    "username": "pbi_username",
    "password": "pbi_password",
    "auth_method": "pbi_auth_method",
    "access_token": "pbi_access_token",
}


# A view that is live and complete. deployed_incomplete (truncated YAML) is not
# reconciled: it would score a measureless view.
_LIVE_STATUSES = frozenset({"deployed", "deployed_grants_incomplete"})
_MAX_FAILURES_IN_NOTE = 3


def _config(gen_kwargs: Mapping[str, object]) -> RawMap:
    config = gen_kwargs.get("config_json")
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except json.JSONDecodeError:
            return {}
    return as_map(config)


def _assembled_mappings(
    yaml_specs: RawMap,
    generated: RawMap,
    gen_kwargs: Mapping[str, object],
    deploy_kwargs: Mapping[str, object],
) -> tuple[dict[str, RawMap], dict[str, str]]:
    """Reconciliation mappings for this cycle's views, bound to the views the deployer
    creates, and why the other views have none (see ``mapping_assembly``)."""
    from src.services.tools.metric_view_deployer_tool import _view_parts
    from src.services.tools.metric_view_utils.reconciliation.mapping_assembly import (
        assemble_mappings,
    )

    catalog = str(deploy_kwargs.get("catalog") or "main")
    schema = str(deploy_kwargs.get("schema_name") or "default")
    fqns = {key: ".".join(_view_parts(key, catalog, schema)) for key in yaml_specs}
    return assemble_mappings(
        yaml_specs,
        as_map(generated.get("resolved_measures_by_table")),
        fqns,
        as_map(_config(gen_kwargs).get("join_key_map")),
        {
            "dataset_id": gen_kwargs.get("dataset_id"),
            "workspace_id": gen_kwargs.get("workspace_id"),
        },
    )


def _healed(
    verdict: RawMap,
    yaml_specs: RawMap,
    deployer: ToolRunner,
    deploy_base: Mapping[str, object],
    context: Mapping[str, object],
    mapping_state: dict[str, dict[str, str]],
    heal_state: dict[str, object],
) -> RawMap:
    """Heal the views that failed to deploy (``ucmv_recon_heal``) and redeploy them. The
    verdict then carries the healed YAML and fresh mappings for them (the loop compares
    those), what was removed per view, and the removed measures as LLM feedback."""
    from src.services.tools.ucmv_recon_heal import heal_failed_views, redeployer

    merged, healed_yaml, healed = heal_failed_views(
        redeployer(deployer, deploy_base, deploy_outcome), yaml_specs, verdict
    )
    if not healed_yaml:
        return merged
    assembled, gaps = _assembled_mappings(
        healed_yaml, as_map(heal_state["generated"]), context, deploy_base
    )
    mapping_state["gaps"] = {
        **{k: r for k, r in mapping_state["gaps"].items() if k not in healed_yaml},
        **gaps,
    }
    heal_state["feedback"] = [
        {
            "view": view,
            "measure": item.split(" ", 1)[1].split(" (", 1)[0],
            "deploy_error": item,
        }
        for view, items in healed.items()
        for item in items
        if item.startswith("measure ")
    ]
    return {
        **merged,
        "yaml_overrides": healed_yaml,
        "mapping_overrides": assembled,
        "healed": healed,
    }


def _with_mapping_gaps(verdict: RawMap, gaps: Mapping[str, str]) -> RawMap:
    """Views deployed but not compared, with the reason, next to the deploy failures."""
    if not gaps:
        return verdict
    failures = dict(as_map(verdict.get("failures")))
    for key, reason in gaps.items():
        failures.setdefault(f"mapping:{key}", reason)
    return {**verdict, "failures": failures}


def _read_mode(gen_kwargs: Mapping[str, object]) -> bool:
    """Whether the metric views read the source layer (``read_from_source_layer``
    in the generator's config), so the loop must deploy it first."""
    from src.services.tools.metric_view_utils.source_layer_binding import (
        read_from_source_layer,
    )

    return read_from_source_layer(dict(_config(gen_kwargs)))


def _deploy_layer_once(
    deployer: ToolRunner, state: dict[str, object], deploy_base: Mapping[str, object]
) -> str:
    """Deploy the source layer when its DDL changed since the last cycle; a note."""
    from src.services.tools.ucmv_recon_source_layer import (
        deploy_source_layer,
        fingerprint,
    )

    ddl = as_map(state.get("ddl"))
    if not ddl:
        return "no source layer emitted"
    fp = fingerprint(ddl)
    if fp != state.get("deployed_fp"):
        state["report"] = deploy_source_layer(deployer, ddl, deploy_base)
        state["deployed_fp"] = fp
    report = as_map(state.get("report"))
    ok = len(as_list(report.get("deployed")))
    failed = len(as_map(report.get("failures")))
    return f"source layer: {ok} of {ok + failed} views created"


def deploy_outcome(out: RawMap) -> RawMap:
    """The deployer's output -> the loop's deploy verdict.

    ``ok`` when at least one view is live: the cycle reconciles those
    (``deployed_views``) and keeps every other view's error in ``failures``. One
    failing view in a large model must not stop the comparison of the rest, and
    the reason must survive into the run output.
    """
    if out.get("error"):
        return {
            "ok": False,
            "error": f"deploy failed: {out['error']}"[:500],
            "failures": {},
        }
    results = as_map(out.get("deployment_results"))
    if not results:  # summary only: all or nothing, as the counts are all there is
        summary = as_map(out.get("summary"))
        if as_int(summary.get("deployed")) > 0 and as_int(summary.get("errors")) == 0:
            return {"ok": True, "failures": {}}
        return {"ok": False, "error": "deploy failed", "failures": {}}
    deployed: list[str] = []
    failures: dict[str, str] = {}
    for key, raw in results.items():
        entry = as_map(raw)
        status = str(entry.get("status") or "")
        if status in _LIVE_STATUSES:
            deployed.append(str(key))
        else:
            reason = entry.get("message") or entry.get("error") or status or "unknown"
            failures[str(key)] = f"{status}: {reason}"[:300]
    first = "; ".join(
        f"{k} ({v})" for k, v in list(failures.items())[:_MAX_FAILURES_IN_NOTE]
    )
    if not deployed:
        error = (
            f"no view deployed; {len(failures)} failed: {first}"
            if failures
            else "no view deployed"
        )
        return {"ok": False, "error": error[:1000], "failures": failures}
    note = f"{len(deployed)} of {len(results)} views deployed" + (
        f"; not reconciled: {first}" if failures else ""
    )
    return {
        "ok": True,
        "deployed_views": deployed,
        "failures": failures,
        "note": note[:1000],
    }


def run_from_generator(
    generator_tool: ToolRunner, kwargs: Mapping[str, object]
) -> Optional[dict[str, object]]:
    """Bridge from the UC Metric View generator's ``enable_reconciliation`` toggle
    to the iterative loop. Returns the loop report, or ``None`` when prerequisites
    are missing (caller then falls back to single-pass generation).

    The generate step re-invokes the SAME generator with ``enable_reconciliation``
    off and an ``_in_recon_loop`` guard, so there is no infinite recursion.
    """
    default_config = as_map(getattr(generator_tool, "_default_config", {}))

    def cfg(key: str) -> object:
        val = kwargs.get(key)
        if val is None:
            val = default_config.get(key)
        return val

    warehouse_id = cfg("warehouse_id")
    reference_years = cfg("reference_years")
    if isinstance(reference_years, str):
        try:
            reference_years = json.loads(reference_years)
        except json.JSONDecodeError:
            reference_years = [
                y.strip() for y in reference_years.split(",") if y.strip()
            ]
    # Reconciliation deploys + queries a live view, so a warehouse is required.
    # reference_years is OPTIONAL — without it the comparison spans all years
    # (empty list = no year filter) rather than being skipped.
    if not warehouse_id:
        logger.info(
            "[UCMV] reconciliation requested but warehouse_id missing — "
            "running single-pass generation."
        )
        return None
    reference_years = reference_years or []

    from src.services.tools.metric_view_deployer_tool import MetricViewDeployerTool
    from src.services.tools.ucmv_reconciliation_tool import UCMVReconciliationTool

    gen_kwargs = dict(kwargs)
    gen_kwargs["enable_reconciliation"] = False  # recursion guard
    gen_kwargs["_in_recon_loop"] = True

    recon_ctor = {
        recon_key: cfg(gen_key)
        for gen_key, recon_key in _GEN_TO_RECON_PBI.items()
        if cfg(gen_key) is not None
    }
    reconciler = UCMVReconciliationTool(**recon_ctor)
    _inherit_confidential_policy(generator_tool, reconciler)

    return run_iterative_ucmv_generation(
        generator=generator_tool,
        deployer=MetricViewDeployerTool(),
        reconciler=reconciler,
        gen_kwargs=gen_kwargs,
        deploy_kwargs={
            "warehouse_id": warehouse_id,
            "catalog": cfg("catalog"),
            "schema_name": cfg("schema_name"),
            "databricks_host": cfg("llm_workspace_url"),
        },
        recon_kwargs={
            "warehouse_id": warehouse_id,
            "reference_years": reference_years,
        },
        max_cycles=int(_positive_or(cfg("max_reconciliation_cycles"), 5)),
        target_pct=_positive_or(cfg("reconciliation_target_pct"), 100.0),
        settings={
            "config_json": cfg("config_json"),
            "dataset_id": cfg("dataset_id"),
            "workspace_id": cfg("workspace_id"),
        },
    )


def _inherit_confidential_policy(parent: object, child: object) -> None:
    """A tool built here, outside the ToolFactory, takes the parent's confidential
    policy: the reconciler then runs value-free like a factory-built one."""
    from src.services.tools.confidential import install_confidential_guard
    from src.services.tools.tool_policies import CONFIDENTIAL_ATTR

    policy = getattr(parent, CONFIDENTIAL_ATTR, None)
    if not policy:
        return
    object.__setattr__(child, CONFIDENTIAL_ATTR, dict(policy))
    if not install_confidential_guard(child):
        logger.error(
            "confidential_values is set but the reconciler could not be guarded"
        )
