"""Column and source-table extraction from PBI scan-data native SQL.

Reads the first UNION arm of a scan table's ``Value.NativeQuery`` SQL and
returns its aggregate/group-by columns and FROM table. Pure string parsing.
"""

from __future__ import annotations

import re

from .data_classes import ScanTableInfo
from .m_transform_folder import MTransformFolder


def extract_columns_from_scan(
    scan_info: ScanTableInfo,
) -> tuple[list[dict], list[str]]:
    """Extract aggregate and group-by columns from scan data SQL.

    Returns: (aggregate_columns, group_by_columns)
    """
    sql = scan_info.native_sql
    first_arm = re.split(r"\bunion\b", sql, maxsplit=1, flags=re.IGNORECASE)[0]
    m = re.match(r"\s*SELECT\s+(.*?)\s+FROM\s+", first_arm, re.IGNORECASE | re.DOTALL)
    if not m:
        return [], []
    select_body = m.group(1)
    agg_cols = []
    grp_cols = []
    for part in MTransformFolder._split_select_columns(select_body):
        part = part.strip()
        if not part:
            continue
        as_match = re.search(r"\bAS\s+(\w+)\s*$", part, re.IGNORECASE)
        alias = as_match.group(1) if as_match else part.split(".")[-1].strip()
        alias = re.sub(r"\W+$", "", alias)
        if re.match(r"^\s*(SUM|AVG|COUNT|MIN|MAX)\s*\(", part, re.IGNORECASE):
            source_col = alias
            inner_match = re.match(r"^\s*\w+\s*\(([^()]+)\)\s*", part)
            if inner_match:
                inner = inner_match.group(1).strip()
                source_col = inner.split(".")[-1].strip()
            agg_cols.append({"name": alias, "source_col": source_col})
        elif re.match(r"^[a-zA-Z_]\w*$", alias):
            grp_cols.append(alias)
    return agg_cols, grp_cols


def extract_source_table_from_scan(scan_info: ScanTableInfo) -> str:
    """Extract source table name from scan data SQL's FROM clause."""
    sql = scan_info.native_sql
    first_arm = re.split(r"\bunion\b", sql, maxsplit=1, flags=re.IGNORECASE)[0]
    m = re.search(r"\bFROM\s+([\w.]+)", first_arm, re.IGNORECASE)
    return m.group(1) if m else ""
