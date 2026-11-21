"""Measure references: a Databricks ``MEASURE(name)`` call and a DAX ``[Measure name]`` reference.

A metric view calls another measure with ``MEASURE(name)``; DAX calls a measure with a bare
bracketed name ``[Measure name]`` (a column is always ``Table[column]``). Both name a measure,
not a column, so neither may be read as a column reference. The generator names a measure in
snake_case (``sales_py``) while DAX keeps the Power BI name (``Sales PY``), so the two sides
are compared by ``loose_name``: case, spaces, underscores and punctuation removed.
"""

from __future__ import annotations

import re
from typing import Set

from .prior_year_check import loose_name

# MEASURE(name) or MEASURE(`name`) in Databricks SQL.
UCMV_MEASURE_CALL = re.compile(
    r"\bMEASURE\s*\(\s*`?([A-Za-z_]\w*)`?\s*\)", re.IGNORECASE
)
# A bracketed name not preceded by a table name, quote or bracket: [Name] in CALCULATE([Name], ...).
DAX_BARE_REF = re.compile(r"(?<![\w'\]\)])\[([^\[\]]+)\]")


def strip_measure_calls(sql: str) -> str:
    """The SQL with each ``MEASURE(name)`` replaced by a neutral token, so the name is
    not read as a column."""
    return UCMV_MEASURE_CALL.sub(" 0 ", sql)


def ucmv_measure_refs(sql: str) -> Set[str]:
    """Loose names of the measures a metric-view expression calls with ``MEASURE(...)``."""
    return {loose_name(m.group(1)) for m in UCMV_MEASURE_CALL.finditer(sql or "")}


def dax_measure_refs(dax: str) -> Set[str]:
    """Loose names of the measures a DAX expression references as ``[name]``."""
    return {loose_name(m.group(1)) for m in DAX_BARE_REF.finditer(dax or "")}
