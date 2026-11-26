"""In-memory stand-ins for the Power BI and Databricks executors.

``FakePbi`` answers the three DAX shapes the runner sends (probe, full table,
keyset page) and enforces the executeQueries row cap by cutting results short
without an error, as the real service does. ``FakeSql`` keeps the catalog state
the writer reads and records every statement.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

_KEY_IN_ORDER = re.compile(r"'[^']*'\[([^\]]+)\], ASC\)$")
_AFTER = re.compile(r"> (-?\d+)\)")
_TOP_N = re.compile(r"TOPN\((\d+)")
_STAGE = re.compile(r"`(snap_[^`]*__stage_[0-9a-f]+)`")
_BACKTICKED = re.compile(r"`((?:[^`]|``)*)`")


class FakePbi:
    def __init__(
        self,
        table: str,
        columns: Sequence[tuple[str, str]],
        rows: Sequence[dict[str, Any]],
        *,
        cut_at: int | None = None,
        fail: bool = False,
    ) -> None:
        self.table = table
        self.columns = list(columns)
        self.rows = [dict(r) for r in rows]
        self.cut_at = cut_at
        self.fail = fail
        self.queries: list[str] = []

    async def __call__(self, dax: str) -> list[dict[str, Any]]:
        self.queries.append(dax)
        if self.fail:
            raise RuntimeError("upstream failure")
        if dax.startswith("EVALUATE ROW("):
            return [self._probe()]
        if dax.startswith("EVALUATE TOPN("):
            return self._page(dax)
        return self._cut(list(self.rows))

    def _cut(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if self.cut_at is not None:
            return rows[: self.cut_at]
        return rows

    def _probe(self) -> dict[str, Any]:
        out: dict[str, Any] = {"rows": len(self.rows)}
        for index, (name, pbi_type) in enumerate(self.columns):
            if pbi_type != "int64":
                continue
            values = [r[name] for r in self.rows]
            out[f"u{index}"] = len({v for v in values if v is not None})
            out[f"b{index}"] = sum(1 for v in values if v is None)
        return out

    def _page(self, dax: str) -> list[dict[str, Any]]:
        key = _KEY_IN_ORDER.search(dax)
        assert key, f"unexpected keyset shape: {dax}"
        key_name = key.group(1)
        size = int(_TOP_N.search(dax).group(1))  # type: ignore[union-attr]
        rows = sorted(self.rows, key=lambda r: r[key_name])
        after = _AFTER.search(dax)
        if after:
            rows = [r for r in rows if r[key_name] > int(after.group(1))]
        return self._cut(rows)[:size]


class FakeSql:
    def __init__(
        self,
        *,
        existing: dict[str, str] | None = None,
        props: dict[str, dict[str, str]] | None = None,
        count_delta: int = 0,
        fail_on: str | None = None,
    ) -> None:
        self.existing = dict(
            existing or {}
        )  # leaf name -> information_schema table_type
        self.props = {
            k: dict(v) for k, v in (props or {}).items()
        }  # leaf -> properties
        self.count_delta = count_delta
        self.fail_on = fail_on
        self.statements: list[str] = []
        self.inserted: dict[str, int] = {}

    async def __call__(self, sql: str) -> list[list[object]]:
        self.statements.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("warehouse rejected the statement")
        if "information_schema.tables" in sql:
            leaf = re.search(r"table_name = '((?:[^']|'')*)'", sql)
            name = leaf.group(1).replace("''", "'") if leaf else ""
            kind = self.existing.get(name)
            return [[kind]] if kind else []
        if sql.startswith("SHOW TBLPROPERTIES"):
            leaf = _BACKTICKED.findall(sql)[-1].replace("``", "`")
            return [[k, v] for k, v in self.props.get(leaf, {}).items()]
        if sql.startswith("CREATE TABLE") and "__stage_" in sql:
            stage = _STAGE.search(sql)
            assert stage, sql
            self.inserted[stage.group(1)] = 0
            return []
        if sql.startswith("INSERT INTO"):
            stage = _STAGE.search(sql)
            assert stage, sql
            self.inserted[stage.group(1)] += sql.count("\n(")
            return []
        if sql.startswith("SELECT COUNT(*)"):
            stage = _STAGE.search(sql)
            assert stage, sql
            return [[str(self.inserted[stage.group(1)] + self.count_delta)]]
        return []

    def writes(self) -> list[str]:
        """Statements that change data (everything except catalog reads)."""
        read_only = ("SELECT", "SHOW")
        return [s for s in self.statements if not s.startswith(read_only)]
