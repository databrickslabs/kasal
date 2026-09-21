"""Parse Power BI Calculation Group definitions out of an Admin Scanner result.

The Admin Scanner API (``trigger_admin_scan`` in ``pipeline_config.py``) returns
a `calculationGroup` object on any table that IS a calculation group — the
Admin Scanner's own schema, not a Kasal invention. `parse_admin_tables` carries
that raw shape through unchanged on each table entry (`"calculation_group"`);
`derive_calculation_groups` here turns it into the ``{name, items}`` list shape
``expand_calculation_groups`` (``metric_view_utils/table_processor.py``) already
consumes via ``pipeline.MetricViewPipeline``'s ``config["calculation_groups"]`` —
that consumer already existed and was never fed, because nothing produced this
key. Calculation groups are a genuinely different Power BI feature from a
SELECTEDVALUE+SWITCH measure dispatcher (see ``switch_decomposition.py``): a
calc group is a first-class model object (an explicit list of named
``SELECTEDMEASURE()``-based calculation items, e.g. "Actual"/"Budget"/"Variance"
applied over any base measure), so it needs its own small parser rather than
being folded into the SWITCH-branch resolver.
"""

from __future__ import annotations

from typing import Any


def derive_calculation_groups(admin_tables: dict[str, dict]) -> list[dict]:
    """Collect every calculation group found across ``parse_admin_tables``' output.

    Returns ``[{"name": <table name>, "items": [{"name", "expression"}, ...]}, ...]``
    — the exact shape ``expand_calculation_groups`` expects, so it slots into
    ``config["calculation_groups"]`` with no further transformation. Returns an
    empty list when the model has no calculation groups (the common case) —
    ``expand_calculation_groups`` already treats that as a no-op.
    """
    groups: list[dict] = []
    for table_name, info in admin_tables.items():
        cg = info.get("calculation_group")
        if not cg:
            continue
        items: list[dict[str, Any]] = []
        for item in cg.get("calculationItems", []):
            name = item.get("name", "")
            if not name:
                continue
            items.append(
                {
                    "name": name,
                    "expression": item.get("expression", "SELECTEDMEASURE()"),
                }
            )
        if items:
            groups.append({"name": table_name, "items": items})
    return groups
