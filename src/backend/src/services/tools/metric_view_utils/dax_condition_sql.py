"""DAX filter condition -> SQL predicate: the translator's FILTER/CALCULATE seam.

All or nothing. A condition translates only when EVERY ``&&`` conjunct and
every ``||`` arm does, and each one is read in full. A conjunct this module
cannot read makes the whole condition untranslatable (``""``), never a smaller
filter: dropping ``Sales[OrderDate] >= DATE(2024,1,1)`` from
``Sales[Region] = "EU" && ...`` would ship an all-years total as a 2024 KPI.
Callers treat ``""`` for a non-empty condition as a gap.
"""

from __future__ import annotations

import re
from typing import Callable, Iterable

_AliasOf = Callable[[str, str], tuple[str, str]]


def _sql_str_literal(value: object) -> str:
    """Return a safe, single-quoted SQL string literal for a DAX-sourced value.

    SECURITY: DAX filter values (measure predicates, IN-lists) are attacker-
    influenceable (a semantic model author controls them) and get interpolated
    into generated SQL that is deployed as a UC Metric View. Without escaping, a
    value like ``x' OR '1'='1`` forges the predicate — and the deployer's
    dangerous-SQL deny-list does NOT catch quote-breakout. Escape backslashes
    first — Databricks SQL processes ``\\`` escapes in string literals, so a
    value ending in ``\\`` would otherwise turn the closing quote into ``\\'``
    and let the NEXT value out of the literal — then double single quotes and
    strip NULs. Always returns the value wrapped in single quotes.
    """
    s = str(value).replace("\x00", "").replace("\\", "\\\\")
    return "'" + s.replace("'", "''") + "'"


def _sql_in_list(values: Iterable[object]) -> str:
    """Build a safe ``'a', 'b', 'c'`` IN-list from DAX-sourced values (escaped)."""
    return ", ".join(_sql_str_literal(v) for v in values)


_OP = r"(=|<>|<|>|<=|>=)"
_QUALIFIED_STR = re.compile(rf'(\w+)\[(\w+)\]\s*{_OP}\s*"([^"]*)"')
_QUALIFIED_IN = re.compile(r"(\w+)\[(\w+)\]\s+in\s+\{([^}]+)\}", re.IGNORECASE)
_QUALIFIED_NUM = re.compile(rf"(\w+)\[(\w+)\]\s*{_OP}\s*(\d+(?:\.\d+)?)")
_IN_ITEM = re.compile(r"""\s*(?:"([^"]*)"|'([^']*)'|(-?\d+(?:\.\d+)?))\s*(?:,|$)""")


def _in_list_sql(text: str) -> str | None:
    """SQL for every value of a DAX ``{"a", 'b', 3}`` list.

    Strings are escaped, numbers kept bare. ``None`` when any item is
    something else (a function, a column), so no value is silently dropped.
    """
    items: list[str] = []
    pos = 0
    while pos < len(text):
        m = _IN_ITEM.match(text, pos)
        if not m or m.end() == pos:
            return None
        if m.group(3) is not None:
            items.append(m.group(3))
        else:
            items.append(
                _sql_str_literal(m.group(1) if m.group(1) is not None else m.group(2))
            )
        pos = m.end()
    return ", ".join(items) or None


def split_top_level_args(text: str) -> list[str]:
    """Split DAX call arguments on commas outside parens, braces and strings."""
    parts: list[str] = []
    depth = 0
    start = 0
    in_str = False
    for i, ch in enumerate(text):
        if ch == '"':
            in_str = not in_str
        elif in_str:
            continue
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i])
            start = i + 1
    parts.append(text[start:])
    return parts


def mixes_and_or(text: str) -> bool:
    """True when ``&&`` and ``||`` both appear outside parens and strings.

    DAX binds ``&&`` tighter than ``||``, so ``A || B && C`` is
    ``A || (B && C)``. Rather than guess at a precedence the source author may
    not have meant either, an unparenthesized mix is refused.
    """
    depth = 0
    in_str = False
    seen = set()
    for i, ch in enumerate(text):
        if ch == '"':
            in_str = not in_str
        elif in_str:
            continue
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif depth == 0 and text[i : i + 2] in ("&&", "||"):
            seen.add(text[i : i + 2])
    return len(seen) == 2


def _strip_wrapping_parens(cond: str) -> str:
    """Strip parens that wrap the WHOLE condition (balanced at the ends).

    ``CALCULATE([M], (table[col] in {...}))`` delivers the predicate
    parenthesized, and every pattern below is anchored, so a leading ``(``
    would make them all miss.
    """
    while len(cond) >= 2 and cond[0] == "(" and cond[-1] == ")":
        depth = 0
        for i, ch in enumerate(cond):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0 and i != len(cond) - 1:
                    return cond
        cond = cond[1:-1].strip()
    return cond


class DaxConditionMixin:
    """Condition translation for ``DaxTranslator`` (state lives there)."""

    column_overrides: dict[str, str]
    _dim_alias_map: dict[str, str]
    _fact_join_map_cfg: dict[str, dict]
    _fact_joins: list[dict]

    def _dim_table_to_alias(self, dim_table: str) -> str:
        """Map a PBI dimension table name to its SQL join alias."""
        if dim_table in self._dim_alias_map:
            return self._dim_alias_map[dim_table]
        return dim_table.lower()

    def _resolve_filter_alias(self, dax_table: str, dax_col: str) -> tuple[str, str]:
        """Resolve a filter column reference that may target a cross-table fact.

        Returns (alias, physical_col).
        """
        if dax_table in self._fact_join_map_cfg:
            for fj in self._fact_joins:
                if fj.get("_pbi_name") == dax_table:
                    cfg = fj["_fact_join_config"]
                    fc = cfg.get("filter_columns", {})
                    if dax_col in fc:
                        return fj["name"], fc[dax_col]
        return self._dim_table_to_alias(dax_table), dax_col

    def _resolve_column(self, pbi_table: str, pbi_col: str, table_key: str) -> str:
        """Resolve a PBI column name to physical column (with overrides)."""
        override_key = f"{pbi_table}.{pbi_col}"
        if override_key in self.column_overrides:
            return self.column_overrides[override_key]
        return pbi_col

    @staticmethod
    def _split_conditions(condition: str) -> list[str]:
        """Split DAX condition on && (AND)."""
        return re.split(r"\s*&&\s*", condition)

    def _all_or_nothing(
        self, condition: str, translate_part: Callable[[str], str | None]
    ) -> str:
        """Join every translated ``&&`` part, or ``""`` if any part fails (or
        ``&&`` and ``||`` are mixed without parentheses)."""
        if not condition or not condition.strip() or mixes_and_or(condition):
            return ""
        sql_parts = []
        for part in self._split_conditions(condition.strip()):
            part = part.strip()
            if not part:
                continue
            sql_part = translate_part(part)
            if not sql_part:
                return ""
            sql_parts.append(sql_part)
        return " AND ".join(sql_parts)

    def _filtered_agg(
        self, agg: str, match: dict, table_key: str
    ) -> tuple[str | None, str]:
        """``AGG(source.<col>) FILTER (WHERE <condition>)``, or a gap when the
        condition cannot be read in full (never the unfiltered aggregate)."""
        condition = (match.get("condition") or "").strip()
        cond_sql = self._dax_condition_to_sql(condition, table_key)
        if condition and not cond_sql:
            return None, f"Untranslatable FILTER condition: {condition}"
        col = match["column"]
        if cond_sql:
            return f"{agg}(source.{col}) FILTER (WHERE {cond_sql})", ""
        return f"{agg}(source.{col})", ""

    def _dax_condition_to_sql(self, condition: str, table_key: str) -> str:
        """Translate a DAX filter condition to SQL; ``""`` unless ALL of it is."""
        return self._all_or_nothing(
            condition, lambda p: self._translate_single_condition(p, table_key)
        )

    def _dax_condition_to_sql_cross_table(
        self, condition: str, filter_table: str
    ) -> str:
        """Translate DAX conditions that reference a cross-table fact's columns."""
        return self._all_or_nothing(
            condition, lambda p: self._qualified_part(p, self._resolve_filter_alias)
        )

    def _dax_condition_to_sql_qualified(
        self, condition: str, filter_table: str, table_key: str
    ) -> str:
        """Translate DAX conditions with qualified dim table references."""
        return self._all_or_nothing(
            condition,
            lambda p: self._qualified_part(
                p, lambda t, c: (self._dim_table_to_alias(t), c)
            ),
        )

    @staticmethod
    def _qualified_part(part: str, alias_of: _AliasOf) -> str | None:
        """One ``Table[col] <op> value`` / ``IN {...}`` part with a join alias."""
        m = _QUALIFIED_STR.fullmatch(part)
        if m:
            alias, col = alias_of(m.group(1), m.group(2))
            return f"{alias}.{col} {m.group(3)} {_sql_str_literal(m.group(4))}"
        m = _QUALIFIED_IN.fullmatch(part)
        if m:
            in_sql = _in_list_sql(m.group(3))
            if in_sql is None:
                return None
            alias, col = alias_of(m.group(1), m.group(2))
            return f"{alias}.{col} IN ({in_sql})"
        m = _QUALIFIED_NUM.fullmatch(part)
        if m:
            alias, col = alias_of(m.group(1), m.group(2))
            return f"{alias}.{col} {m.group(3)} {m.group(4)}"
        return None

    def _translate_single_condition(self, cond: str, table_key: str) -> str | None:
        """Translate a single DAX condition to SQL, or ``None`` if any part of
        it can't be read (patterns match the WHOLE condition, never a prefix)."""
        cond = _strip_wrapping_parens(cond.strip())
        if mixes_and_or(cond):
            return None  # && and || without parentheses: never guess the grouping

        # OR (||): every arm must translate, else the whole condition fails.
        if "||" in cond:
            sql_or_parts = []
            for op in re.split(r"\s*\|\|\s*", cond):
                sql_op = self._translate_single_condition(op.strip(), table_key)
                if sql_op is None:
                    return None
                sql_or_parts.append(sql_op)
            return "(" + " OR ".join(sql_or_parts) + ")"

        neg = re.fullmatch(r"NOT\s+(.+)", cond, re.IGNORECASE | re.DOTALL)
        target = neg.group(1).strip() if neg else cond
        m = re.fullmatch(r"(?:(\w+))?\[(\w+)\]\s+in\s+\{([^}]+)\}", target, re.I)
        if m:
            in_sql = _in_list_sql(m.group(3))
            if in_sql is None:
                return None
            col = self._column(m.group(1), m.group(2), table_key)
            keyword = "NOT IN" if neg else "IN"
            return f"source.{col} {keyword} ({in_sql})"
        if neg:
            return None  # NOT of anything but an IN list is not handled

        m = re.fullmatch(
            rf"(?:(\w+))?\[(\w+)\]\s*{_OP}\s*(?:\"([^\"]*)\"|'([^']*)'|(\d+(?:\.\d+)?))",
            cond,
        )
        if not m:
            return None
        col = self._column(m.group(1), m.group(2), table_key)
        if m.group(6) is not None:
            return f"source.{col} {m.group(3)} {m.group(6)}"
        text = m.group(4) if m.group(4) is not None else m.group(5)
        return f"source.{col} {m.group(3)} {_sql_str_literal(text)}"

    def _column(self, table: str | None, col: str, table_key: str) -> str:
        """``Table[col]`` resolves through overrides; a bare ``[col]`` does not."""
        return self._resolve_column(table, col, table_key) if table else col
