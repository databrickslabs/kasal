"""Reconciliation mappings for the metric views a verify-numbers cycle actually deployed.

The generator's own mapping files are review drafts (``pbi_fact_table: TODO``, no time dimension,
the view named ``mv_<table>`` while the deployer creates ``<table>``), so the reconciliation could
not load them. This module builds a mapping per deployed view from what the run already knows:

- ``ucmv_table``: the view the deployer created.
- ``measures``: every DAX-translated measure of the view (``resolved_measures_by_table``), compared
  with the Power BI measure it came from (``original_name``, ``pbi_kind: direct``). Base measures
  (a SUM over a column) have no Power BI measure and are not compared.
- ``time_dimension``: the first period dimension of the view (``fiscper``, ``calmonth``, ...). On a
  join (``dim_calendar.fiscper``) the Power BI table is the joined table (``join_key_map`` alias),
  which the reconciliation reaches through the model relationship.
- ``default_dimension``: a second dimension, preferring company / country / plant / region.

The Power BI column is taken to have the metric view's column name. Where the model renames it, the
comparison of that view fails and names the column, and crew 4 fixes the mapping. A view without a
period dimension, a second dimension or a translated measure gets no mapping; the reason is
returned so the run names it.

Pure functions: no I/O.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

import yaml

from ..raw_fields import RawMap, as_list, as_map

_COLUMN_REF = re.compile(r"^\s*`?([A-Za-z_]\w*)`?\.`?([A-Za-z_]\w*)`?\s*$")
# Period columns whose values are period keys (YYYYNNN / YYYYMM), month grain.
_PERIOD_NAMES = (
    "fiscper",
    "calmonth",
    "fiscal_period",
    "period",
    "yearmonth",
    "month_id",
)
# Dimensions to compare by, best first: few members, present on most facts.
_PREFERRED_DIMENSIONS = ("comp_code", "company", "country", "plant", "region", "market")


def alias_tables(join_key_map: Mapping[str, object]) -> dict[str, str]:
    """``{join alias: Power BI table}`` from the pipeline config's ``join_key_map``."""
    out: dict[str, str] = {}
    for table, raw in join_key_map.items():
        alias = str(as_map(raw).get("alias") or "")
        if alias:
            out[alias] = str(table)
    return out


def _dimensions(
    spec: RawMap, fact: str, aliases: Mapping[str, str]
) -> list[tuple[str, str, str]]:
    """``(dimension, Power BI table, column)`` for each plain column dimension of the view."""
    out: list[tuple[str, str, str]] = []
    for raw in as_list(spec.get("dimensions")):
        dim = as_map(raw)
        match = _COLUMN_REF.match(str(dim.get("expr") or ""))
        if not match or not dim.get("name"):
            continue
        alias, column = match.groups()
        table = fact if alias == "source" else aliases.get(alias)
        if table:
            out.append((str(dim["name"]), table, column))
    return out


def _period(dims: list[tuple[str, str, str]]) -> tuple[str, str, str] | None:
    for candidate in _PERIOD_NAMES:
        for dim in dims:
            if dim[2].lower() == candidate or dim[0].lower() == candidate:
                return dim
    return None


def _measures(spec: RawMap, resolved: object) -> list[RawMap]:
    in_view = {str(as_map(m).get("name")) for m in as_list(spec.get("measures"))}
    out: list[RawMap] = []
    for raw in as_list(resolved):
        m = as_map(raw)
        name, original = str(m.get("measure_name") or ""), str(
            m.get("original_name") or ""
        )
        if m.get("dax_expression") and name in in_view and original:
            out.append(
                {
                    "ucmv_measure": name,
                    "pbi_kind": "direct",
                    "pbi_measure": original,
                    "value_format": "raw_number",
                    "tolerance": {"kind": "absolute", "value": 0.001},
                }
            )
    return out


def assemble_mapping(
    table_key: str,
    yaml_text: str,
    resolved: object,
    view_fqn: str,
    aliases: Mapping[str, str],
    model: Mapping[str, object],
) -> tuple[RawMap | None, str]:
    """One mapping for a deployed view, or ``(None, reason)``."""
    try:
        spec = as_map(yaml.safe_load(yaml_text))
    except yaml.YAMLError:
        return None, "the YAML does not parse"
    measures = _measures(spec, resolved)
    if not measures:
        return (
            None,
            "no DAX-translated measure to compare (base measures have no Power BI measure)",
        )
    dims = _dimensions(spec, table_key, aliases)
    period = _period(dims)
    if period is None:
        return (
            None,
            "no period dimension (fiscper, calmonth, ...); set time_dimension by hand",
        )
    others = [d for d in dims if d[0] != period[0]]
    if not others:
        return None, "no second dimension to compare by"
    default = next(
        (d for want in _PREFERRED_DIMENSIONS for d in others if want in d[0].lower()),
        others[0],
    )
    time_dimension: dict[str, object] = {
        "grain": "month",
        "ucmv": {"mode": "column", "name": period[0]},
        "pbi_column": period[2],
    }
    if period[1] != table_key:
        time_dimension["pbi_table"] = period[1]
    return {
        "binding": {
            "ucmv_table": view_fqn,
            "pbi_semantic_model_id": model.get("dataset_id"),
            "pbi_workspace_id": model.get("workspace_id"),
            "pbi_fact_table": table_key,
            "default_dimension": default[0],
        },
        "time_dimension": time_dimension,
        "dimensions": {
            default[0]: {
                "ucmv_column": default[0],
                "pbi_dim_table": default[1],
                "pbi_dim_column": default[2],
            }
        },
        "measures": measures,
    }, ""


def assemble_mappings(
    yaml_specs: Mapping[str, object],
    resolved_by_table: Mapping[str, object],
    view_fqns: Mapping[str, str],
    join_key_map: Mapping[str, object],
    model: Mapping[str, object],
) -> tuple[dict[str, RawMap], dict[str, str]]:
    """Mappings for every view in ``view_fqns`` (``{table key: deployed view}``), and why the
    others have none."""
    aliases = alias_tables(join_key_map)
    mappings: dict[str, RawMap] = {}
    gaps: dict[str, str] = {}
    for key, fqn in view_fqns.items():
        mapping, reason = assemble_mapping(
            key,
            str(yaml_specs.get(key) or ""),
            resolved_by_table.get(key),
            fqn,
            aliases,
            model,
        )
        if mapping is None:
            gaps[key] = reason
        else:
            mappings[key] = mapping
    return mappings, gaps
