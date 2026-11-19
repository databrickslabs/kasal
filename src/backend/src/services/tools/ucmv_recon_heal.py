"""Heal metric views that failed to deploy, so a verify-numbers cycle can still compare the rest.

Databricks names what it rejected: a measure (``[measures.`x`.expr]``), a column it cannot
resolve (``source.fte``, ``dim_calendar.month``), a struct field, or a missing table. One bad
measure or join made the whole view fail, and with it every other measure of the view. This
module removes exactly what the error names, with whatever depends on it (the dimensions and
measures that read a removed join), and redeploys. Databricks reports one error per statement,
so a view is healed in rounds.

What was removed is returned per view with the error, so it is visible in the run and the
removed measures go back to the DAX LLM in the next cycle. Nothing is guessed: an error that
names nothing removable, or that hits the view's own source, leaves the view failed.

Pure YAML edits plus the deployer's ``_run``; no other I/O.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Mapping

import yaml

from src.services.tools.metric_view_utils.raw_fields import RawMap, as_list, as_map

logger = logging.getLogger(__name__)

_MEASURE_AT = re.compile(r"\[measures\.`?([^`\]]+?)`?\.expr\]")
_DIMENSION_AT = re.compile(r"\[dimensions\.`?([^`\]]+?)`?\.expr\]")
_UNRESOLVED = re.compile(r"with name `([^`]+)`(?:\.`([^`]+)`)? cannot be resolved")
_FIELD = re.compile(r"No such struct field `([^`]+)` in ((?:`[^`]+`(?:,\s*)?)+)")
_MISSING_TABLE = re.compile(r"The table or view ((?:`[^`]+`\.?)+) cannot be found")
_SUBQUERY_SOURCE = "__auto_generated_subquery_name_source"
MAX_ROUNDS = 8
_PERIOD_COLUMNS = frozenset(
    {"fiscper", "calmonth", "fiscal_period", "period", "yearmonth", "month_id"}
)


def _norm(name: str) -> str:
    return ".".join(p.strip("`").lower() for p in str(name).split(".") if p.strip("`"))


def _refs(expr: object, alias: str | None, column: str) -> bool:
    # Without an alias Databricks names a bare identifier (a measure referenced by name),
    # so match it bare as well as after any alias.
    prefix = rf"\b{re.escape(alias)}\." if alias else r"(?:\.|(?<![\w`.]))"
    return bool(
        re.search(prefix + rf"`?{re.escape(column)}`?\b", str(expr or ""), re.I)
    )


class _View:
    def __init__(self, spec: dict[str, object]) -> None:
        self.spec = spec
        self.removed: list[str] = []

    def _items(self, key: str) -> list[dict[str, object]]:
        return [dict(as_map(x)) for x in as_list(self.spec.get(key))]

    def drop_items(
        self, key: str, keep: Callable[[dict[str, object]], bool], what: str
    ) -> int:
        items = self._items(key)
        kept = [i for i in items if keep(i)]
        for item in items:
            if item not in kept:
                self.removed.append(f"{what} {item.get('name')}")
        self.spec[key] = kept
        return len(items) - len(kept)

    def drop_join(self, alias: str) -> int:
        """Remove a join and everything that reads it (nested joins, dimensions, measures)."""
        joins = self._items("joins")
        if not any(j.get("name") == alias for j in joins):
            return 0
        self.spec["joins"] = [j for j in joins if j.get("name") != alias]
        self.removed.append(f"join {alias}")
        reads = re.compile(rf"\b{re.escape(alias)}\.")
        dependents = [
            j.get("name")
            for j in self._items("joins")
            if reads.search(str(j.get("on") or ""))
        ]
        for name in dependents:
            self.drop_join(str(name))
        self._period_to_source(alias)
        for key, what in (("dimensions", "dimension"), ("measures", "measure")):
            self.drop_items(
                key,
                lambda i: not re.search(
                    rf"\b{re.escape(alias)}\.", str(i.get("expr") or "")
                ),
                what,
            )
        return 1

    def drop_fields(self, missing: str, available: set[str]) -> int:
        """Every reference of the join that lacks ``missing`` to a field it does not have."""
        pattern = re.compile(rf"\b(\w+)\.`?{re.escape(missing)}`?\b", re.I)
        aliases = {
            m.group(1)
            for key in ("dimensions", "measures")
            for item in self._items(key)
            for m in pattern.finditer(str(item.get("expr") or ""))
        } - {"source"}
        removed = 0
        for alias in aliases:
            ref = re.compile(rf"\b{re.escape(alias)}\.`?(\w+)`?")
            for key, what in (("dimensions", "dimension"), ("measures", "measure")):
                removed += self.drop_items(
                    key,
                    lambda i: all(
                        f.lower() in available
                        for f in ref.findall(str(i.get("expr") or ""))
                    ),
                    what,
                )
        return removed or self.drop_column(None, missing)

    def _period_to_source(self, alias: str) -> None:
        """A period dimension read through the removed join (``dim_calendar.fiscper``) reads the
        fact's own column instead: the comparison needs a period. If the fact lacks it, the next
        round removes it like any other unresolved column."""
        dims = self._items("dimensions")
        for dim in dims:
            m = re.fullmatch(
                rf"\s*{re.escape(alias)}\.`?(\w+)`?\s*", str(dim.get("expr") or "")
            )
            if m and m.group(1).lower() in _PERIOD_COLUMNS:
                dim["expr"] = f"source.{m.group(1)}"
                self.removed.append(
                    f"dimension {dim.get('name')} now reads source.{m.group(1)}"
                )
        self.spec["dimensions"] = dims

    def drop_column(self, alias: str | None, column: str) -> int:
        removed = 0
        for key, what in (("measures", "measure"), ("dimensions", "dimension")):
            removed += self.drop_items(
                key, lambda i: not _refs(i.get("expr"), alias, column), what
            )
        if removed:
            return removed
        for join in self._items("joins"):
            if _refs(join.get("on"), alias, column):
                removed += self.drop_join(str(join.get("name")))
        return removed


def heal_view(yaml_text: str, error: str) -> tuple[str | None, list[str]]:
    """The YAML without what ``error`` names, and what was removed; ``(None, [])`` if nothing can be."""
    try:
        spec = dict(as_map(yaml.safe_load(yaml_text)))
    except yaml.YAMLError:
        return None, []
    view = _View(spec)
    measure = _MEASURE_AT.search(error)
    dimension = _DIMENSION_AT.search(error)
    if measure:
        name = measure.group(1)
        view.drop_items("measures", lambda i: i.get("name") != name, "measure")
    elif dimension:
        name = dimension.group(1)
        view.drop_items("dimensions", lambda i: i.get("name") != name, "dimension")
    elif m := _UNRESOLVED.search(error):
        alias, column = (m.group(1), m.group(2)) if m.group(2) else (None, m.group(1))
        view.drop_column("source" if alias == _SUBQUERY_SOURCE else alias, column)
    elif m := _FIELD.search(error):
        # The error lists every field the struct has: drop all references of that
        # join to fields outside the list at once, not one per round.
        available = {f.lower() for f in re.findall(r"`([^`]+)`", m.group(2))}
        view.drop_fields(m.group(1), available)
    elif m := _MISSING_TABLE.search(error):
        missing = _norm(m.group(1))
        if _norm(str(spec.get("source") or "")) == missing:
            return None, []  # the view's own source: nothing left to deploy
        for join in view._items("joins"):
            if _norm(str(join.get("source") or "")) == missing:
                view.drop_join(str(join.get("name")))
    if not view.removed or not as_list(view.spec.get("measures")):
        return None, []
    return yaml.safe_dump(view.spec, sort_keys=False, allow_unicode=True), view.removed


def heal_failed_views(
    redeploy: Callable[[Mapping[str, str]], RawMap],
    yaml_specs: Mapping[str, object],
    verdict: RawMap,
    rounds: int = MAX_ROUNDS,
) -> tuple[RawMap, dict[str, str], dict[str, list[str]]]:
    """Heal and redeploy failed views. Returns the merged verdict, the healed YAML per view and
    what was removed per view (``"<item> (<error>)"``)."""
    healed_yaml: dict[str, str] = {}
    healed: dict[str, list[str]] = {}
    deployed = [str(v) for v in as_list(verdict.get("deployed_views"))]
    failures = {str(k): str(v) for k, v in as_map(verdict.get("failures")).items()}
    for round_no in range(1, rounds + 1):
        batch: dict[str, str] = {}
        for key, reason in list(failures.items()):
            if key.startswith(("source_layer:", "mapping:")):
                continue
            current = healed_yaml.get(key) or str(yaml_specs.get(key) or "")
            text, removed = heal_view(current, reason)
            if text is None:
                continue
            short = reason.split("SQLSTATE")[0].strip()[:160]
            healed.setdefault(key, []).extend(f"{item} ({short})" for item in removed)
            healed_yaml[key] = batch[key] = text
        if not batch:
            break
        logger.info(
            "[UCMV] deploy heal round %d: redeploying %d view(s)", round_no, len(batch)
        )
        outcome = redeploy(batch)
        for key in batch:
            failures.pop(key, None)
        deployed += [str(v) for v in as_list(outcome.get("deployed_views"))]
        failures.update(
            {str(k): str(v) for k, v in as_map(outcome.get("failures")).items()}
        )
    if not healed:
        return verdict, healed_yaml, healed
    merged = dict(verdict)
    merged["deployed_views"] = sorted(set(deployed))
    merged["failures"] = failures
    if deployed:
        merged["ok"] = True
        merged.pop("error", None)
    merged["note"] = (
        f"{len(set(deployed))} view(s) deployed, {len(healed)} of them after healing; "
        f"{len(failures)} not deployed"
    )
    return merged, healed_yaml, healed


def redeployer(
    deployer: object,
    deploy_base: Mapping[str, object],
    outcome: Callable[[RawMap], RawMap],
) -> Callable[[Mapping[str, str]], RawMap]:
    """A ``redeploy`` for :func:`heal_failed_views` over the deployer tool."""

    def _redeploy(batch: Mapping[str, str]) -> RawMap:
        run = getattr(deployer, "_run")
        raw = run(yaml_specs_json=json.dumps(dict(batch)), **dict(deploy_base))
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except json.JSONDecodeError:
            parsed = {}
        return outcome(as_map(parsed))

    return _redeploy
