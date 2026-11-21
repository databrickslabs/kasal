"""Audit which Power Query "M" transformation steps a source expression contains.

The UCMV source-recovery path resolves the underlying physical table (and an
embedded native-SQL query, if present) but does NOT transpile step-based M
transformations — ``Table.SelectRows`` filters, ``Table.Group`` aggregations,
``Table.NestedJoin`` merges, ``Table.AddColumn`` computed columns, pivots and
unions. Those are not reflected in the generated Source SQL, which can silently
drift from what Power BI actually loaded.

This module does NOT translate anything — it scans the raw M and names the
transformation steps that are present but may not be reflected downstream, so the
UI can surface them for review instead of leaving the loss invisible. Column
projection/rename and type casts are deliberately NOT flagged: the parser already
handles projection/rename, and type changes are cosmetic for a metric view.
"""

from __future__ import annotations

import re
from typing import List, Tuple

# (regex, human-readable label). Ordered most-impactful first. Each pattern is a
# Power Query function whose semantics are NOT carried into the UCMV source SQL
# by the deterministic parser or the source-recovery LLM step.
_AUDIT_PATTERNS: Tuple[Tuple[re.Pattern, str], ...] = (
    (re.compile(r"\bTable\.SelectRows\b"), "Row filter (Table.SelectRows → WHERE)"),
    (
        re.compile(r"\bTable\.Group\b"),
        "Aggregation / grouping (Table.Group → GROUP BY)",
    ),
    (
        re.compile(r"\bTable\.(NestedJoin|Join|FuzzyNestedJoin|Merge)\b"),
        "Join / merge of another source (Table.Join/NestedJoin → JOIN)",
    ),
    (
        re.compile(r"\bTable\.(Combine|Append)\b"),
        "Union of multiple sources (Table.Combine/Append → UNION ALL)",
    ),
    (
        re.compile(r"\bTable\.AddColumn\b"),
        "Computed/derived column (Table.AddColumn)",
    ),
    (
        re.compile(r"\bTable\.(Pivot|Unpivot|UnpivotOtherColumns)\b"),
        "Pivot / unpivot reshape (Table.Pivot/Unpivot)",
    ),
    (
        re.compile(r"\bTable\.ReplaceValue\b"),
        "Value replacement (Table.ReplaceValue)",
    ),
    (
        re.compile(r"\bTable\.RemoveColumns\b"),
        "Column removal (Table.RemoveColumns)",
    ),
    (
        re.compile(r"\bTable\.(FirstN|LastN|Range)\b"),
        "Row limit / slice (Table.FirstN/LastN)",
    ),
    (re.compile(r"\bTable\.Distinct\b"), "De-duplication (Table.Distinct)"),
)

# A native query embedded in the M carries its OWN WHERE/GROUP BY verbatim into
# the Source SQL, so transformation steps in that case are preserved — do not
# flag them.
_NATIVE_QUERY_RE = re.compile(r"(Value\.NativeQuery|Query\s*=|\[Query=)", re.IGNORECASE)


def audit_mquery_transformations(raw_m: str) -> List[str]:
    """Return human-readable labels for transformation steps present in ``raw_m``
    that may NOT be reflected in the generated Source SQL.

    Empty when the expression carries no such steps, is not raw Power Query M
    (e.g. it is already plain SQL), or embeds a native query (whose own SQL is
    preserved verbatim).
    """
    if not raw_m or not isinstance(raw_m, str):
        return []
    # Plain SQL (not M) — nothing to audit; the parser used it directly.
    if "\nlet" not in f"\n{raw_m}" and not raw_m.lstrip().lower().startswith("let"):
        return []
    if _NATIVE_QUERY_RE.search(raw_m):
        return []
    found: List[str] = []
    for pattern, label in _AUDIT_PATTERNS:
        if pattern.search(raw_m) and label not in found:
            found.append(label)
    return found
