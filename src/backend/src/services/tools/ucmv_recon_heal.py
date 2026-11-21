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


def _split_dependents(
    measures: list[dict[str, object]], gone: set[str]
) -> tuple[list[dict[str, object]], list[str]]:
    """``measures`` without those that read a removed measure (``MEASURE(x)``), directly or
    through another one, and the names taken out. Each would fail the next deploy with its
    own error and cost a healing round."""
    gone = {name.lower() for name in gone}
    dropped: list[str] = []
    kept = list(measures)
    while True:
        reading = [
            m
            for m in kept
            if gone
            & {
                n.lower()
                for n in re.findall(
                    r"\bMEASURE\(\s*`?(\w+)`?\s*\)", str(m.get("expr") or ""), re.I
                )
            }
        ]
        if not reading:
            return kept, dropped
        for measure in reading:
            kept.remove(measure)
            dropped.append(str(measure.get("name")))
            gone.add(str(measure.get("name")).lower())


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
        # The period itself, or its number (the key prior-year windows order by).
        read = rf"{re.escape(alias)}\.`?(\w+)`?"
        for dim in dims:
            expr = str(dim.get("expr") or "")
            m = re.fullmatch(rf"\s*{read}\s*", expr) or re.fullmatch(
                rf"\s*TRY_CAST\(\s*{read}\s+AS\s+INT\s*\)\s*", expr, re.I
            )
            if m and m.group(1).lower() in _PERIOD_COLUMNS:
                dim["expr"] = re.sub(read, f"source.{m.group(1)}", expr)
                self.removed.append(
                    f"dimension {dim.get('name')} now reads source.{m.group(1)}"
                )
        self.spec["dimensions"] = dims

    def window_orders(self, name: str) -> set[str]:
        """The dimensions the windows of measure ``name`` order by."""
        return {
            str(as_map(w).get("order"))
            for item in self._items("measures")
            if item.get("name") == name
            for w in as_list(item.get("window"))
        }

    def drop_windows(self, orders: set[str] | None = None) -> int:
        """Remove the measures with a window (on ``orders``, or on a dimension the view
        does not have): a window the warehouse rejects, or one whose dimension is gone,
        fails for every measure that has it."""
        present = {str(d.get("name")) for d in self._items("dimensions")}

        def keep(item: dict[str, object]) -> bool:
            on = {str(as_map(w).get("order")) for w in as_list(item.get("window"))}
            return not (on & orders if orders is not None else on - present)

        return self.drop_items("measures", keep, "measure")

    def drop_dependents(self) -> None:
        """Remove the measures that read a measure removed in this round."""
        gone = {r.split(" ", 2)[1] for r in self.removed if r.startswith("measure ")}
        kept, dropped = _split_dependents(self._items("measures"), gone)
        if dropped:
            self.spec["measures"] = kept
            self.removed += [f"measure {n} (reads a removed measure)" for n in dropped]

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
        # A rejected window is rejected for every measure with a window on the same
        # dimension: they go in one round, not one per round.
        orders = view.window_orders(name) if "WINDOW" in error.upper() else set()
        view.drop_items("measures", lambda i: i.get("name") != name, "measure")
        if orders:
            view.drop_windows(orders)
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
    if view.removed or "window.order" in error:
        # Windows on a dimension that was just removed, or that the view never had
        # ("window.order field must reference existing dimension columns").
        view.drop_windows()
        view.drop_dependents()
    if not view.removed or not as_list(view.spec.get("measures")):
        return None, []
    return yaml.safe_dump(view.spec, sort_keys=False, allow_unicode=True), view.removed


_SOURCE_ERRORS = (
    "TABLE_OR_VIEW_NOT_FOUND",
    "NO_SUCH_CATALOG",
    "SCHEMA_NOT_FOUND",
    "PERMISSION_DENIED",
)
MAX_BISECT_DEPLOYS = 8
# Databricks read ``source.<column>`` as a field of a joined table's column named ``source``.
_SOURCE_AS_FIELD = "INVALID_EXTRACT_BASE_FIELD_TYPE"


def bisect_measure(
    redeploy: Callable[[Mapping[str, str]], RawMap],
    key: str,
    yaml_text: str,
    error: str,
) -> tuple[str | None, list[str]]:
    """The YAML without the one measure that makes the view fail, found by deploying halves.

    For errors that name no measure (``NESTED_AGGREGATE_FUNCTION``, ``MISSING_AGGREGATION``):
    the view is deployed with half its measures; the half that fails holds the culprit, down
    to one measure, which is removed. Costs about log2(measures) deploys of this view into the
    sandbox, capped at MAX_BISECT_DEPLOYS. ``(None, [])`` when the error is about the source,
    the view has a single measure, or no single measure explains the failure.
    """
    return _bisect(redeploy, key, yaml_text, error, "measures")


def bisect_dimension(
    redeploy: Callable[[Mapping[str, str]], RawMap],
    key: str,
    yaml_text: str,
    error: str,
) -> tuple[str | None, list[str]]:
    """The YAML without the one dimension that makes the view fail, for
    ``INVALID_EXTRACT_BASE_FIELD_TYPE`` on ``source``.

    A dimension on ``source.<column>`` the fact does not have normally fails as an unresolved
    column, which names it. When a joined table has a column called ``source``, Databricks
    reads ``source.<column>`` as a field of that column and reports only that it is not a
    struct. Found the same way as a measure: by deploying halves. ``(None, [])`` for any
    other error."""
    if _SOURCE_AS_FIELD not in error:
        return None, []
    return _bisect(redeploy, key, yaml_text, error, "dimensions")


def _bisect(
    redeploy: Callable[[Mapping[str, str]], RawMap],
    key: str,
    yaml_text: str,
    error: str,
    section: str,
) -> tuple[str | None, list[str]]:
    """``bisect_measure`` / ``bisect_dimension`` over one section of the view."""
    what = section[:-1]
    if any(code in error for code in _SOURCE_ERRORS):
        return None, []
    try:
        spec = dict(as_map(yaml.safe_load(yaml_text)))
    except yaml.YAMLError:
        return None, []
    items = [dict(as_map(m)) for m in as_list(spec.get(section))]
    if len(items) < 2:
        return None, []
    order, reads = _dependency_order(items, section)

    def closed(indexes: set[int]) -> set[int]:
        """``indexes`` plus every measure they read through MEASURE() references."""
        out, todo = set(indexes), list(indexes)
        while todo:
            for j in reads[todo.pop()]:
                if j not in out:
                    out.add(j)
                    todo.append(j)
        return out

    def deploys(chosen: set[int]) -> bool:
        subset = [items[i] for i in sorted(chosen)]
        trial = yaml.safe_dump(
            {**spec, section: subset}, sort_keys=False, allow_unicode=True
        )
        outcome = redeploy({key: trial})
        return key in {str(v) for v in as_list(outcome.get("deployed_views"))}

    # Trials are prefixes of the dependency order, closed under MEASURE(): a trial never fails
    # only because a measure it reads is outside it. "The first k fail" is then monotone in k,
    # and the least failing k holds the culprit. Trial deploys stay capped; one is kept for
    # the rest.
    lo, hi, budget = 1, len(order), MAX_BISECT_DEPLOYS
    while lo < hi and budget > 1:
        mid = (lo + hi) // 2
        budget -= 1
        if deploys(closed(set(order[:mid]))):
            lo = mid + 1
        else:
            hi = mid
    if lo != hi:
        return None, []
    new = closed(set(order[:lo])) - closed(set(order[: lo - 1]))
    if len(new) != 1:
        return None, []
    culprit_i = next(iter(new))
    name = str(items[culprit_i].get("name"))
    rest_i = set(range(len(items))) - {culprit_i}
    removed = [f"{what} {name} (found by bisection)"]
    if section == "measures":
        # The readers of the culprit go with it, before the rest is deployed.
        kept, dependents = _split_dependents([items[i] for i in sorted(rest_i)], {name})
        removed += [f"measure {n} (reads a removed measure)" for n in dependents]
        kept_ids = {id(m) for m in kept}
        rest_i = {i for i in rest_i if id(items[i]) in kept_ids}
    if not deploys(closed(rest_i)):
        # Another culprit, or the failure is not this section's: remove this one only if it
        # is broken on its own while another item alone deploys; the next round goes on.
        others = sorted(rest_i)
        if (
            not others
            or deploys(closed({culprit_i}))
            or not deploys(closed({others[0]}))
        ):
            return None, []
    logger.info("[UCMV] bisect: %s fails on %s %s", key, what, name)
    healed = yaml.safe_dump(
        {**spec, section: [items[i] for i in sorted(rest_i)]},
        sort_keys=False,
        allow_unicode=True,
    )
    return healed, removed


def _dependency_order(
    items: list[dict[str, object]], section: str
) -> tuple[list[int], dict[int, set[int]]]:
    """Item indexes with each measure after the measures it reads (MEASURE() references), and
    every item's reads. A cycle is broken where it closes; the closure in ``_bisect`` covers it.
    """
    by_name = {str(m.get("name")).lower(): i for i, m in enumerate(items)}
    reads: dict[int, set[int]] = {}
    for i, item in enumerate(items):
        refs: set[int] = set()
        if section == "measures":
            for ref in re.findall(
                r"\bMEASURE\(\s*`?(\w+)`?\s*\)", str(item.get("expr") or ""), re.I
            ):
                j = by_name.get(ref.lower())
                if j is not None and j != i:
                    refs.add(j)
        reads[i] = refs
    order: list[int] = []
    done: set[int] = set()

    def visit(i: int, stack: set[int]) -> None:
        if i in done or i in stack:
            return
        stack.add(i)
        for j in sorted(reads[i]):
            visit(j, stack)
        stack.discard(i)
        if i not in done:
            done.add(i)
            order.append(i)

    for i in range(len(items)):
        visit(i, set())
    return order, reads


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
            if text is None and _SOURCE_AS_FIELD in reason:
                # A dimension on a fact column that does not exist, reported without
                # its name (bisect_dimension).
                text, removed = bisect_dimension(redeploy, key, current, reason)
            if text is None:
                # The error names nothing removable: find the measure by bisection.
                text, removed = bisect_measure(redeploy, key, current, reason)
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
