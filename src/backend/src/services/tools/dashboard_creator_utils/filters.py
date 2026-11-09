"""Lakeview filter widgets from Power BI slicers.

A slicer column becomes one filter widget per page. It filters every widget on the page whose
dataset has the matching metric view dimension: the widget gets one query per dataset, and the
encoding binds the field of each query. Date-like dimensions get a date range picker, the rest a
multi-select. Filters need metric-view datasets; on a SQL dataset grouped per visual a filter
would re-aggregate measures, so the SQL mode reports them instead of building them.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from .widgets import label, quote

_DATE = re.compile(r"(?:^|_)(?:date|datum|day|timestamp|dt)(?:$|_)", re.I)


def is_date_field(name: str) -> bool:
    return bool(_DATE.search(name))


def filter_widget(widget_name: str, dimension: str, datasets: list[str]) -> dict[str, object]:
    queries = [
        {
            "name": f"{widget_name}_{ds}",
            "query": {
                "datasetName": ds,
                "fields": [{"name": dimension, "expression": quote(dimension)}],
                "disaggregated": False,
            },
        }
        for ds in datasets
    ]
    widget_type = "filter-date-range-picker" if is_date_field(dimension) else "filter-multi-select"
    return {
        "name": widget_name,
        "queries": queries,
        "spec": {
            "version": 2,
            "widgetType": widget_type,
            "encodings": {
                "fields": [
                    {"fieldName": dimension, "displayName": label(dimension), "queryName": q["name"]}
                    for q in queries
                ]
            },
            "frame": {"showTitle": True, "title": label(dimension)},
        },
    }


def slicer_fields(slicer: Mapping[str, object]) -> list[dict[str, object]]:
    """The resolved ``{column, dimension, views}`` entries of a mapper slicer."""
    entries = slicer.get("fields")
    if not isinstance(entries, list):
        return []
    return [entry for entry in entries if isinstance(entry, dict)]
