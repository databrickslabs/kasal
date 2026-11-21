"""Deterministic DAX scalar and date converters (no LLM).

Mixed into ``DaxTranslator``. Each converter matches one top-level DAX function wrapping a
``Table[Column]`` reference (the common shape) and emits Spark SQL with columns as
``source.<col>``. Anything more complex returns None and falls through to the LLM.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING


class DaxScalarConvertersMixin:
    # Methods of the host (DaxTranslator) that these converters call; declared for type checks.
    if TYPE_CHECKING:

        def translate_expression(self, dax_expr: str, table_key: str) -> str | None: ...

        def _match_divide(self, dax: str, name: str) -> dict | None: ...

        def _translate_divide(
            self, match: dict, dax: str, table_key: str
        ) -> tuple[str | None, str]: ...

        @staticmethod
        def _strip_var_block(dax: str) -> str: ...

        @staticmethod
        def _strip_return(text: str) -> str: ...

    # ── Deterministic scalar/date converters (field-eng parity) ───────────
    #
    # These translate cheap, high-frequency DAX scalar/date functions to Spark
    # SQL WITHOUT an LLM call. They match a single top-level function wrapping a
    # ``Table[Column]`` reference (the common shape); anything more complex
    # returns None so it falls through to the LLM. Column refs emit as
    # ``source.<col>`` to match the rest of the translator.

    # DAX date-part function → Spark SQL function
    _DATE_PART_FUNCS = {
        "YEAR": "year",
        "MONTH": "month",
        "DAY": "day",
        "QUARTER": "quarter",
        "HOUR": "hour",
        "MINUTE": "minute",
        "SECOND": "second",
        "WEEKNUM": "weekofyear",
        "WEEKDAY": "dayofweek",
    }

    def _match_date_part(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax))
        m = re.fullmatch(
            r"\s*(YEAR|MONTH|DAY|QUARTER|HOUR|MINUTE|SECOND|WEEKNUM|WEEKDAY)\s*\(\s*"
            r"(\w+)\[(\w+)\]\s*(?:,\s*\d+\s*)?\)\s*",
            cleaned,
            re.IGNORECASE,
        )
        if not m:
            return None
        return {"func": m.group(1).upper(), "column": m.group(3)}

    def _translate_date_part(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        spark = self._DATE_PART_FUNCS.get(match["func"])
        if not spark:
            return None, f"Unsupported date part {match['func']}"
        return f"{spark}(source.{match['column']})", ""

    # DATEDIFF(<start>, <end>, <interval>) → Spark SQL per interval.
    def _match_datediff(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax))
        if "DATEDIFF" not in cleaned.upper():
            return None
        m = re.fullmatch(
            r"\s*DATEDIFF\s*\(\s*(\w+)\[(\w+)\]\s*,\s*(\w+)\[(\w+)\]\s*,\s*"
            r"(DAY|WEEK|MONTH|QUARTER|YEAR|HOUR|MINUTE|SECOND)\s*\)\s*",
            cleaned,
            re.IGNORECASE,
        )
        if not m:
            return None
        return {"a": m.group(2), "b": m.group(4), "interval": m.group(5).upper()}

    def _translate_datediff(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        a, b, iv = f"source.{match['a']}", f"source.{match['b']}", match["interval"]
        if iv == "DAY":
            return f"datediff({b}, {a})", ""
        if iv == "WEEK":
            return f"floor(datediff({b}, {a}) / 7)", ""
        if iv == "MONTH":
            return f"floor(months_between({b}, {a}))", ""
        if iv == "QUARTER":
            return f"floor(months_between({b}, {a}) / 3)", ""
        if iv == "YEAR":
            return f"floor(months_between({b}, {a}) / 12)", ""
        # Sub-day intervals via unix-second difference.
        divisor = {"HOUR": 3600, "MINUTE": 60, "SECOND": 1}[iv]
        inner = f"(unix_timestamp({b}) - unix_timestamp({a}))"
        return (inner if divisor == 1 else f"floor({inner} / {divisor})"), ""

    def _resolve_scalar_inner(self, expr: str) -> str | None:
        """Resolve an inner expression for SCALAR context (FORMAT/RANKX arg).

        A bare ``Table[col]`` maps to ``source.<col>`` WITHOUT a SUM wrapper —
        unlike ``translate_expression`` (which SUM-wraps bare columns for its
        DIVIDE-numerator use). An aggregate/other expr is delegated as-is.
        """
        expr = expr.strip()
        cm = re.fullmatch(r"\s*(\w+)\[(\w+)\]\s*", expr)
        if cm:
            return f"source.{cm.group(2)}"
        # DIVIDE is a valid scalar inner but translate_expression skips it — use
        # the divide converter directly.
        if re.match(r"\s*DIVIDE\s*\(", expr, re.IGNORECASE):
            dm = self._match_divide(expr, "")
            if dm is not None:
                sql, _ = self._translate_divide(dm, expr, "")
                if sql:
                    return sql
        return self.translate_expression(expr, "")

    # RANKX(<table>, <expr>, , <order>, <ties>) → rank()/dense_rank() OVER.
    def _match_rankx(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax))
        if "RANKX" not in cleaned.upper():
            return None
        # RANKX(All/Table, <expr>, [<value>], [ASC|DESC], [Dense|Skip])
        m = re.fullmatch(
            r"\s*RANKX\s*\(\s*[^,]+,\s*(.+?)\s*"
            r"(?:,\s*[^,]*)??"  # optional value arg
            r"(?:,\s*(ASC|DESC))?"  # optional order
            r"(?:,\s*(DENSE|SKIP))?\s*\)\s*",
            cleaned,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            return None
        expr = m.group(1).strip()
        # Only handle a simple aggregate/column expr deterministically.
        inner = self._resolve_scalar_inner(expr)
        if not inner:
            return None
        order = (m.group(2) or "DESC").upper()
        ties = (m.group(3) or "SKIP").upper()
        return {"inner": inner, "order": order, "dense": ties == "DENSE"}

    def _translate_rankx(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        fn = "dense_rank" if match["dense"] else "rank"
        return f"{fn}() OVER (ORDER BY {match['inner']} {match['order']})", ""

    # EDATE(<date>, <n>) → add_months ; EOMONTH(<date>, <n>) → last_day(add_months)
    def _match_edate_eomonth(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax))
        m = re.fullmatch(
            r"\s*(EDATE|EOMONTH)\s*\(\s*(\w+)\[(\w+)\]\s*,\s*(-?\d+)\s*\)\s*",
            cleaned,
            re.IGNORECASE,
        )
        if not m:
            return None
        return {
            "func": m.group(1).upper(),
            "column": m.group(3),
            "months": int(m.group(4)),
        }

    def _translate_edate_eomonth(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        base = f"add_months(source.{match['column']}, {match['months']})"
        if match["func"] == "EDATE":
            return base, ""
        return f"last_day({base})", ""

    # FIRSTNONBLANK / LASTNONBLANK / LASTNONBLANKVALUE → MIN/MAX simplification.
    def _match_firstlastnonblank(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax))
        m = re.fullmatch(
            r"\s*(FIRSTNONBLANK|LASTNONBLANK|LASTNONBLANKVALUE)\s*\(\s*"
            r"(\w+)\[(\w+)\]\s*(?:,\s*[^)]+)?\)\s*",
            cleaned,
            re.IGNORECASE,
        )
        if not m:
            return None
        return {"func": m.group(1).upper(), "column": m.group(3)}

    def _translate_firstlastnonblank(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        agg = "MIN" if match["func"] == "FIRSTNONBLANK" else "MAX"
        return f"{agg}(source.{match['column']})", ""

    # FORMAT(<expr>, "<format string>") → date_format / format_number.
    # Format-string → (kind, spark_arg) lookup. kind: 'date' | 'number' | 'raw'.
    _FORMAT_MAP = {
        # Date formats (DAX format string → Spark date_format pattern)
        "MMM YYYY": ("date", "MMM yyyy"),
        "MMM-YYYY": ("date", "MMM-yyyy"),
        "MMMM YYYY": ("date", "MMMM yyyy"),
        "MMM": ("date", "MMM"),
        "MMMM": ("date", "MMMM"),
        "YYYY-MM-DD": ("date", "yyyy-MM-dd"),
        "DD/MM/YYYY": ("date", "dd/MM/yyyy"),
        "MM/DD/YYYY": ("date", "MM/dd/yyyy"),
        "YYYY": ("date", "yyyy"),
        "YYYY-MM": ("date", "yyyy-MM"),
        "DDDD": ("date", "EEEE"),
        "DDD": ("date", "EEE"),
        # Number formats (DAX format string → Spark format_number decimal places)
        "#,##0": ("number", 0),
        "#,##0.0": ("number", 1),
        "#,##0.00": ("number", 2),
        "0": ("number", 0),
        "0.0": ("number", 1),
        "0.00": ("number", 2),
    }

    def _match_format_func(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax))
        if not re.match(r"\s*FORMAT\s*\(", cleaned, re.IGNORECASE):
            return None
        # FORMAT(<expr>, "<fmt>") — expr is a Table[col] or a simple aggregate.
        m = re.fullmatch(
            r'\s*FORMAT\s*\(\s*(.+?)\s*,\s*"([^"]*)"\s*\)\s*',
            cleaned,
            re.IGNORECASE | re.DOTALL,
        )
        if not m:
            return None
        expr, fmt = m.group(1).strip(), m.group(2).strip()
        # Resolve the inner expr for scalar context (bare Table[col] → source.col,
        # NOT SUM-wrapped; aggregates delegated as-is).
        inner = self._resolve_scalar_inner(expr)
        if not inner:
            return None
        return {"inner": inner, "fmt": fmt}

    def _translate_format_func(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        inner, fmt = match["inner"], match["fmt"]
        spec = self._FORMAT_MAP.get(fmt) or self._FORMAT_MAP.get(fmt.upper())
        if not spec:
            # Percent formats (contain %) → multiply *100 and format_number.
            if "%" in fmt:
                decimals = fmt.count("0", fmt.find(".")) if "." in fmt else 0
                return f"CONCAT(format_number({inner} * 100, {decimals}), '%')", ""
            # Currency (leading $/€/£) → format_number with 2 decimals + symbol.
            cur = re.match(r"\s*([$€£])", fmt)
            if cur:
                return f"CONCAT('{cur.group(1)}', format_number({inner}, 2))", ""
            # Unknown format code: keep the value unformatted rather than lose the
            # measure (display formatting isn't essential in a UC metric view).
            return inner, ""
        kind, arg = spec
        if kind == "date":
            return f"date_format({inner}, '{arg}')", ""
        if kind == "number":
            return f"format_number({inner}, {arg})", ""
        return inner, ""
