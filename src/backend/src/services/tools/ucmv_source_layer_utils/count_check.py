"""PBI table vs Unity Catalog object: counts, per-period counts, sums, members.

Output is counts and verdicts. Measure values and sums are compared inside
this process and only the NAMES of differing columns are reported. Categorical
member values are shown (at most a short list) only for text columns.

Verdicts: ``match``, ``drift`` (matches once UC rows loaded after Power BI's
last refresh are excluded: the warehouse is simply newer), ``differs`` (an
error), and ``cut`` (Power BI hit a query limit, so nothing was compared).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from src.services.tools.ucmv_source_layer_utils.names import (
    parse_utc,
    quote_part,
    snake_case,
    utc_stamp,
)
from src.services.tools.ucmv_source_layer_utils.pbi_client import (
    DAX_ROW_LIMIT,
    DAX_VALUE_LIMIT,
    clean_rows,
    dax_column,
    dax_table,
)
from src.services.tools.ucmv_source_layer_utils.sql_types import SqlFn

DaxFn = Callable[[str], list[dict[str, object]]]
LastRefreshFn = Callable[[], str | None]
MEMBER_LIST_LIMIT = 30
NUMERIC_UC_TYPES = frozenset(
    {"TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL"}
)
_SEVERITY = {"match": 0, "drift": 1, "cut": 2, "differs": 3}


@dataclass(frozen=True)
class CountRequest:
    pbi_table: str
    target: str  # quoted fully qualified UC object
    by: str = ""  # PBI period / grouping column
    uc_by: str = ""  # UC name of that column when it differs
    members: tuple[str, ...] = ()  # "PBI column" or "PBI column=uc column"
    sums: bool = False
    column_map: dict[str, str] = field(default_factory=dict)
    cutoff_column: str = ""  # UC load-stamp column for the refresh cutoff


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def worst_verdict(verdicts: list[str]) -> str:
    """The most severe of the verdicts (differs > cut > drift > match)."""
    return max(verdicts, key=lambda v: _SEVERITY.get(v, 3), default="match")


def two_pass_verdict(uncut_equal: bool, cut_equal: bool | None) -> str:
    """Equal as stored: match. Equal only after the refresh cutoff: drift."""
    if uncut_equal:
        return "match"
    if cut_equal:
        return "drift"
    return "differs"


def normalize_key(value: object) -> str:
    """A period / member value as comparable text (dates cut to the day)."""
    if value is None:
        return "(blank)"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip().lower()
    if (
        len(text) >= 10
        and text[4:5] == "-"
        and text[7:8] == "-"
        and text[10:11] in ("t", " ", "")
    ):
        return text[:10]
    return text


def resolve_uc_column(
    uc_names: list[str], pbi_name: str, column_map: dict[str, str]
) -> str | None:
    """The UC column for a PBI column: explicit map, own name, then snake_case."""
    lowered = {name.lower(): name for name in uc_names}
    mapped = next(
        (v for k, v in column_map.items() if k.lower() == pbi_name.lower()), None
    )
    candidates = [mapped] if mapped else [pbi_name, snake_case(pbi_name)]
    for candidate in candidates:
        if candidate and candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def parse_describe(rows: list[list[str | None]]) -> list[tuple[str, str]]:
    """``DESCRIBE TABLE`` rows -> [(column, type)], stopping at the detail block."""
    columns: list[tuple[str, str]] = []
    for row in rows:
        name = (row[0] or "").strip() if row else ""
        if not name or name.startswith("#"):
            break
        columns.append((name, (row[1] or "").strip() if len(row) > 1 else ""))
    return columns


def group_differences(
    pbi: dict[str, list[int | float]],
    uc: dict[str, list[int | float]],
    sum_names: list[str],
) -> list[dict[str, object]]:
    """Per period that differs: row counts, and the names of differing sums."""
    differing: list[dict[str, object]] = []
    for key in sorted(set(pbi) | set(uc)):
        left, right = pbi.get(key), uc.get(key)
        if left is None or right is None:
            side = "UC" if left is None else "PBI"
            rows = (right or left or [0])[0]
            differing.append({"period": key, "only_in": side, "rows": rows})
            continue
        entry: dict[str, object] = {}
        if int(left[0]) != int(right[0]):
            entry["pbi_rows"] = int(left[0])
            entry["uc_rows"] = int(right[0])
        sums = [
            name
            for name, a, b in zip(sum_names, left[1:], right[1:])
            if not _close(a, b)
        ]
        if sums:
            entry["sums_differ"] = sums
        if entry:
            entry["period"] = key
            differing.append(entry)
    return differing


def _close(a: int | float | None, b: int | float | None) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= 1e-9 * max(abs(a), abs(b), 1.0)


def member_comparison(
    pbi_values: Sequence[object], uc_values: Sequence[object], *, text: bool
) -> dict[str, object]:
    """Distinct-member comparison: counts always, values only for text columns."""
    pbi = {normalize_key(v) for v in pbi_values}
    uc = {normalize_key(v) for v in uc_values}
    only_pbi, only_uc = sorted(pbi - uc), sorted(uc - pbi)
    result: dict[str, object] = {
        "verdict": "match" if not only_pbi and not only_uc else "differs",
        "pbi_members": len(pbi),
        "uc_members": len(uc),
        "in_both": len(pbi & uc),
        "only_pbi": len(only_pbi),
        "only_uc": len(only_uc),
    }
    if text and (only_pbi or only_uc):
        result["only_pbi_values"] = only_pbi[:MEMBER_LIST_LIMIT]
        result["only_uc_values"] = only_uc[:MEMBER_LIST_LIMIT]
    return result


def parse_member_spec(spec: str) -> tuple[str, str]:
    """``'Col'`` -> ('Col', ''); ``'Col=uc_col'`` -> ('Col', 'uc_col')."""
    pbi_name, sep, uc_name = spec.rpartition("=")
    if not sep:
        return spec.strip(), ""
    return pbi_name.strip(), uc_name.strip()


# ---------------------------------------------------------------------------
# Queries (PBI and UC)
# ---------------------------------------------------------------------------


def _pbi_count(dax: DaxFn, expression: str) -> int:
    rows = clean_rows(dax(f'EVALUATE ROW("n", {expression})'))
    value = rows[0].get("n") if rows else 0
    return int(value) if isinstance(value, (int, float)) else 0


def _at_limit(rows: int, width: int) -> bool:
    return rows >= DAX_ROW_LIMIT or rows * width >= DAX_VALUE_LIMIT


def _uc_where(cutoff: str, column: str) -> str:
    if not cutoff:
        return ""
    return f" WHERE CAST({quote_part(column)} AS TIMESTAMP) <= TIMESTAMP '{cutoff}'"


def _uc_total(sql: SqlFn, target: str, where: str) -> int:
    rows = sql(f"SELECT COUNT(*) FROM {target}{where}").rows
    return int(rows[0][0] or 0) if rows else 0


def _pbi_groups(
    dax: DaxFn, table: str, by: str, sum_columns: list[str]
) -> dict[str, list[int | float]] | None:
    """{period: [rows, sum...]}; None when the listing is at a Power BI limit."""
    quoted = dax_table(table)
    aggregates = [f'"n", COUNTROWS({quoted})']
    aggregates += [
        f'"s{i}", SUM({dax_column(table, c)})' for i, c in enumerate(sum_columns)
    ]
    if by:
        by_ref = dax_column(table, by)
        rows = clean_rows(
            dax(f"EVALUATE SUMMARIZECOLUMNS({by_ref}, {', '.join(aggregates)})")
        )
        if _at_limit(len(rows), len(aggregates) + 1):
            return None
        expected = _pbi_count(dax, f"COUNTROWS(SUMMARIZE({quoted}, {by_ref}))")
        if len(rows) != expected:
            return None
    else:
        rows = clean_rows(dax(f"EVALUATE ROW({', '.join(aggregates)})"))
    groups: dict[str, list[int | float]] = {}
    for row in rows:
        key = normalize_key(row.get(by)) if by else "(all)"
        groups[key] = [_num(row.get("n"))] + [
            _num(row.get(f"s{i}")) for i in range(len(sum_columns))
        ]
    return groups


def _num(value: object) -> int | float:
    if isinstance(value, bool) or value is None:
        return 0
    if isinstance(value, (int, float)):
        return value
    try:
        return float(str(value))
    except ValueError:
        return 0


def _uc_groups(
    sql: SqlFn,
    target: str,
    by: str,
    sum_columns: list[str],
    where: str,
) -> dict[str, list[int | float]]:
    aggregates = ["COUNT(*)"] + [f"SUM({quote_part(c)})" for c in sum_columns]
    if by:
        key = f"CAST({quote_part(by)} AS STRING)"
        statement = (
            f"SELECT {key}, {', '.join(aggregates)} FROM {target}{where} GROUP BY 1"
        )
    else:
        key = "'(all)'"
        statement = f"SELECT {key}, {', '.join(aggregates)} FROM {target}{where}"
    groups: dict[str, list[int | float]] = {}
    for row in sql(statement).rows:
        groups[normalize_key(row[0])] = [_num(v) for v in row[1:]]
    return groups


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def _total_check(
    req: CountRequest, dax: DaxFn, sql: SqlFn, cutoff: str
) -> dict[str, object]:
    pbi_rows = _pbi_count(dax, f"COUNTROWS({dax_table(req.pbi_table)})")
    uc_rows = _uc_total(sql, req.target, "")
    result: dict[str, object] = {"pbi_rows": pbi_rows, "uc_rows": uc_rows}
    cut_equal: bool | None = None
    if cutoff:
        after = _uc_total(sql, req.target, _uc_where(cutoff, req.cutoff_column))
        result["uc_rows_after_cutoff"] = after
        cut_equal = after == pbi_rows
    result["verdict"] = two_pass_verdict(uc_rows == pbi_rows, cut_equal)
    return result


def _by_check(
    req: CountRequest,
    dax: DaxFn,
    sql: SqlFn,
    uc_names: list[str],
    sum_pbi: list[str],
    sum_uc: list[str],
    cutoff: str,
) -> dict[str, object]:
    pbi = _pbi_groups(dax, req.pbi_table, req.by, sum_pbi)
    if pbi is None:
        return {
            "column": req.by,
            "verdict": "cut",
            "note": "Power BI result is at a query limit; not compared",
        }
    uc_by = req.uc_by or resolve_uc_column(uc_names, req.by, req.column_map) or req.by
    uc = _uc_groups(sql, req.target, uc_by, sum_uc, "")
    uncut = group_differences(pbi, uc, sum_pbi)
    cut_diffs: list[dict[str, object]] | None = None
    if cutoff:
        cut_uc = _uc_groups(
            sql, req.target, uc_by, sum_uc, _uc_where(cutoff, req.cutoff_column)
        )
        cut_diffs = group_differences(pbi, cut_uc, sum_pbi)
    verdict = two_pass_verdict(not uncut, None if cut_diffs is None else not cut_diffs)
    return {
        "column": req.by,
        "groups": len(set(pbi) | set(uc)),
        "verdict": verdict,
        "differing_groups": uncut[:MEMBER_LIST_LIMIT],
        "differing_groups_total": len(uncut),
        "sum_columns_compared": sum_pbi,
    }


def _member_check(
    req: CountRequest,
    dax: DaxFn,
    sql: SqlFn,
    uc_names: list[str],
    uc_types: dict[str, str],
    spec: str,
) -> dict[str, object]:
    pbi_name, uc_override = parse_member_spec(spec)
    mapping = dict(req.column_map)
    if uc_override:
        mapping[pbi_name] = uc_override
    uc_name = resolve_uc_column(uc_names, pbi_name, mapping)
    if uc_name is None:
        return {
            "column": pbi_name,
            "verdict": "differs",
            "note": "no such column in the UC object",
        }
    pbi_ref = dax_column(req.pbi_table, pbi_name)
    pbi_rows = clean_rows(dax(f"EVALUATE DISTINCT({pbi_ref})"))
    values = [next(iter(row.values()), None) for row in pbi_rows]
    expected = _pbi_count(dax, f"COUNTROWS(DISTINCT({pbi_ref}))")
    if len(values) != expected or _at_limit(len(values), 1):
        return {
            "column": pbi_name,
            "verdict": "cut",
            "note": "Power BI member list is at a query limit; not compared",
        }
    uc_values = [
        r[0]
        for r in sql(
            f"SELECT DISTINCT CAST({quote_part(uc_name)} AS STRING) FROM {req.target}"
        ).rows
    ]
    text = uc_types.get(uc_name, "").upper().split("(")[0] in (
        "STRING",
        "VARCHAR",
        "CHAR",
    )
    return {"column": pbi_name, **member_comparison(values, uc_values, text=text)}


def _sum_columns(
    req: CountRequest, dax: DaxFn, uc_names: list[str], uc_types: dict[str, str]
) -> tuple[list[str], list[str]]:
    """(PBI names, UC names) of the numeric columns both sides have."""
    sample = clean_rows(dax(f"EVALUATE TOPN(1, {dax_table(req.pbi_table)})"))
    if not sample:
        return [], []
    pbi_sum: list[str] = []
    uc_sum: list[str] = []
    for pbi_name in sample[0]:
        uc_name = resolve_uc_column(uc_names, pbi_name, req.column_map)
        if (
            uc_name
            and uc_types.get(uc_name, "").upper().split("(")[0] in NUMERIC_UC_TYPES
        ):
            pbi_sum.append(pbi_name)
            uc_sum.append(uc_name)
    return pbi_sum, uc_sum


def run_count_check(
    req: CountRequest,
    *,
    dax: DaxFn,
    sql: SqlFn,
    last_refresh: LastRefreshFn,
) -> dict[str, object]:
    """Run every requested check; returns the report (counts and verdicts)."""
    described = parse_describe(sql(f"DESCRIBE TABLE {req.target}").rows)
    uc_names = [name for name, _ in described]
    uc_types = {name: sql_type for name, sql_type in described}
    cutoff = ""
    cutoff_info: dict[str, str] | None = None
    if req.cutoff_column:
        refreshed = last_refresh()
        if not refreshed:
            raise ValueError(
                "no completed Power BI refresh found; cannot apply a cutoff"
            )
        cutoff = utc_stamp(parse_utc(refreshed))
        cutoff_info = {"column": req.cutoff_column, "pbi_refresh_utc": cutoff}
    report: dict[str, object] = {
        "pbi_table": req.pbi_table,
        "target": req.target,
        "cutoff": cutoff_info,
        "total": _total_check(req, dax, sql, cutoff),
    }
    verdicts = [str(as_dict(report["total"]).get("verdict"))]
    sum_pbi: list[str] = []
    sum_uc: list[str] = []
    if req.sums:
        sum_pbi, sum_uc = _sum_columns(req, dax, uc_names, uc_types)
    if req.by:
        by_result = _by_check(req, dax, sql, uc_names, sum_pbi, sum_uc, cutoff)
        report["by"] = by_result
        verdicts.append(str(by_result.get("verdict")))
    if req.members:
        members = [
            _member_check(req, dax, sql, uc_names, uc_types, m) for m in req.members
        ]
        report["members"] = members
        verdicts += [str(m.get("verdict")) for m in members]
    report["verdict"] = worst_verdict(verdicts)
    return report


def as_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}
