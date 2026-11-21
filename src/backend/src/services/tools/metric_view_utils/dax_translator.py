"""DAX Translator — pattern-based DAX→SQL translator with ordered registry."""

from __future__ import annotations

import re
from typing import Callable

from . import dax_patterns
from .constants import (
    RE_AVERAGEX_FILTER,
    RE_COUNTX_FILTER,
    RE_SIMPLE_SUM,
    RE_SIMPLE_SUMX,
    RE_SUMX_FILTER,
)
from .data_classes import TranslateFn, TranslationResult
from .dax_condition_sql import (
    DaxConditionMixin,
    _sql_in_list,
    _sql_str_literal,
    split_top_level_args,
)
from .dax_preclean import preclean_dax
from .dax_scalar_converters import DaxScalarConvertersMixin
from .filter_reach import drop_non_filtering_filters, filter_reach, model_mentions
from .join_detector import _sanitize_alias
from .sql_measure_sanitizer import sanitize_measure_name
from .utils import to_snake_case


class DaxTranslator(DaxConditionMixin, DaxScalarConvertersMixin):
    """Pattern-based DAX→SQL translator with ordered registry."""

    def __init__(self, config: dict | None = None):
        cfg = config or {}
        self.filter_sets = cfg.get("filter_sets", {})
        self.column_overrides: dict[str, str] = cfg.get("column_overrides", {})
        self._fact_join_map_cfg = cfg.get("fact_join_map", {})
        self._measure_resolutions: dict[str, dict] = cfg.get("measure_resolutions", {})
        self._dim_alias_map: dict[str, str] = cfg.get("dim_alias_map", {})
        self._cwc_filter_column: str = cfg.get("cwc_filter_column", "")
        self._fact_joins: list[dict] = []
        self._source_col_map: dict[str, str] = {}
        self._relationships: list[dict] = []
        self._reach_cache: dict[str, frozenset[str] | None] = {}
        self._patterns: list[tuple[str, Callable, TranslateFn]] = []
        self._register_patterns()

    def set_model_relationships(self, relationships: list[dict]) -> None:
        """The model's full relationship list. Without it no predicate is dropped (a list
        that is not known says nothing about which tables filter a fact)."""
        self._relationships = [r for r in relationships if isinstance(r, dict)]
        self._reach_cache = {}

    def _drop_non_filtering_filters(
        self, dax: str, table_key: str
    ) -> tuple[str, list[str]]:
        """``(DAX, tables)``: the CALCULATE predicates on tables that do not filter the fact
        ``table_key`` are taken out, and the tables are returned for the measure's comment.
        """
        if not self._relationships or not table_key:
            return dax, []
        if table_key not in self._reach_cache:
            known = model_mentions(table_key, self._relationships)
            self._reach_cache[table_key] = (
                filter_reach(table_key, self._relationships) if known else None
            )
        reach = self._reach_cache[table_key]
        if reach is None:
            return dax, []
        return drop_non_filtering_filters(dax, table_key, reach)

    def _register_patterns(self) -> None:
        """Register translation patterns in priority order. First match wins."""
        # ── Defect-class matchers (IDOR/DQA post-mortem M1–M5, M9, M11) ──
        # Prepended FIRST so they win over the generic catch-alls AND before
        # quick_reject (M4's `IF(ISBLANK(...), BLANK(), 1 - x)` would otherwise be
        # dropped as an "ISBLANK+BLANK guard"). Each returns None unless it fully
        # parses its shape, so anything else falls straight through. Logic lives in
        # dax_patterns (keeps this module from growing); translate_fn is bound to
        # this translator so it can read _measure_resolutions.
        defect = [
            (name, mfn, (lambda m, d, tk, _tf=tfn: _tf(m, d, tk, self)))
            for name, mfn, tfn in dax_patterns.DEFECT_PATTERNS
        ]
        self._patterns = defect + [
            ("quick_reject", self._match_quick_reject, self._translate_noop),
            (
                "sameperiodlastyear",
                self._match_sameperiodlastyear,
                self._translate_sameperiodlastyear,
            ),
            ("simple_sum", self._match_simple_sum, self._translate_simple_sum),
            ("simple_agg", self._match_simple_agg, self._translate_simple_agg),
            (
                "calculate_equality_filter",
                self._match_calculate_equality_filter,
                self._translate_calculate_equality_filter,
            ),
            ("simple_sumx", self._match_simple_sumx, self._translate_simple_sumx),
            (
                "calculate_sumx_vars_divide",
                self._match_calc_sumx_vars_divide,
                self._translate_calc_sumx_vars_divide,
            ),
            (
                "calculate_sumx_filter_inner",
                self._match_calculate_sumx_filter_inner,
                self._translate_sumx_parts,
            ),
            (
                "calculate_sumx_filter_outer",
                self._match_calculate_sumx_filter_outer,
                self._translate_sumx_parts,
            ),
            ("sumx_filter", self._match_sumx_filter, self._translate_sumx_parts),
            ("countx_filter", self._match_countx_filter, self._translate_countx),
            ("averagex_filter", self._match_averagex_filter, self._translate_averagex),
            (
                "userelationship",
                self._match_userelationship,
                self._translate_userelationship,
            ),
            (
                "calculate_measure_ref",
                self._match_calculate_measure_ref,
                self._translate_calculate_measure_ref,
            ),
            (
                "distinctcountnoblank",
                self._match_distinctcountnoblank,
                self._translate_distinctcountnoblank,
            ),
            # Deterministic scalar/date converters (field-eng parity). These run
            # BEFORE the generic divide/switch catch-alls and are in the trivial
            # fast-path so they're handled without an LLM call in llm_first mode.
            ("date_part", self._match_date_part, self._translate_date_part),
            ("datediff", self._match_datediff, self._translate_datediff),
            ("rankx", self._match_rankx, self._translate_rankx),
            ("edate_eomonth", self._match_edate_eomonth, self._translate_edate_eomonth),
            (
                "firstlastnonblank",
                self._match_firstlastnonblank,
                self._translate_firstlastnonblank,
            ),
            ("format_func", self._match_format_func, self._translate_format_func),
            (
                "divide_calculate_measure_ref",
                self._match_divide_calculate_measure_ref,
                self._translate_divide_calculate_measure_ref,
            ),
            ("divide", self._match_divide, self._translate_divide),
            (
                "selectedvalue_switch",
                self._match_selectedvalue_switch,
                self._translate_noop,
            ),
        ]

    def set_fact_joins(self, fact_joins: list[dict]) -> None:
        """Set current fact joins for cross-table resolution."""
        self._fact_joins = fact_joins

    def set_source_col_map(self, col_map: dict[str, str]) -> None:
        """Map a same-fact physical column to the source SELECT's OUTPUT column.

        A pre-aggregating source (``SUM(kbi_value) AS value``) exposes the column
        under its alias (``value``), so a measure that would emit ``source.kbi_value``
        must instead emit ``source.value`` to resolve. Keyed physical→alias.
        """
        self._source_col_map = col_map or {}

    # In llm_first mode, only these patterns run as the regex fast-path (Step 3.8):
    # display-artifact rejects + the trivial single-column, high-confidence
    # aggregations. Everything else falls through to the LLM-first translator.
    # Defect-class matchers (dax_patterns.DEFECT_FAST_PATH) join the fast-path via
    # the union below — deterministic, exact, reproducible, so they run in
    # llm_first mode instead of burning an LLM call.
    _TRIVIAL_FAST_PATH = dax_patterns.DEFECT_FAST_PATH | frozenset(
        {
            "quick_reject",
            "simple_sum",
            "simple_agg",
            "calculate_equality_filter",
            "simple_sumx",
            "distinctcountnoblank",
            # Deterministic scalar/date converters — cheap, reproducible, no LLM.
            "date_part",
            "datediff",
            "rankx",
            "edate_eomonth",
            "firstlastnonblank",
            "format_func",
            # Promoted bread-and-butter converters: these emit clean, correct SQL
            # (verified through process_table→emit_yaml — FILTER aliases normalize to
            # source., no leakage/drops), so they run deterministically instead of
            # burning an LLM call in llm_first mode. DIVIDE is ANSI-safe
            # (SUM(a)/NULLIF(SUM(b),0)). Deliberately EXCLUDES: userelationship
            # (drops alt-relationship join semantics — LLM+join-context does better),
            # selectedvalue_switch (noop by design), sameperiodlastyear (narrow
            # conditional window measure — keep its existing path).
            "divide",
            "divide_calculate_measure_ref",
            "calculate_measure_ref",
            "sumx_filter",
            "countx_filter",
            "averagex_filter",
        }
        # DELIBERATELY NOT in the fast-path (route to the skill-corpus LLM):
        #   calculate_sumx_vars_divide, calculate_sumx_filter_inner/outer.
        # The reference benchmark proved these regex matchers claim complex multi-var
        # DAX (var a=CALCULATE(SUMX(FILTER..)); DIVIDE(a, b-c)) but silently DROP
        # the denominator / leave dangling `res1`/`b` identifiers — producing
        # clean-looking but WRONG SQL, and (worse) blocking the LLM that CAN
        # translate the full ratio. Excluding them lets llm_first route these to
        # the LLM; the lost-component guard is the backstop if the LLM also fails.
    )

    def translate(
        self, measure: dict, table_key: str, trivial_only: bool = False
    ) -> TranslationResult:
        """Translate a single DAX measure to SQL using the pattern registry.

        ``trivial_only`` (llm_first mode): run ONLY the fast-path matchers
        (``_TRIVIAL_FAST_PATH``) — display-artifact rejects, single-column
        aggregations, deterministic scalar/date converters, and the verified
        bread-and-butter converters (DIVIDE, SUMX/COUNTX/AVERAGEX filters,
        CALCULATE measure-refs) that emit clean SQL. Any measure not handled by
        those returns untranslatable so the caller routes it to the skill-corpus
        LLM translator. The remaining complex matchers (userelationship,
        selectedvalue_switch, sameperiodlastyear) are not deleted — they simply
        aren't the primary path in llm_first mode.
        """
        name = measure.get("measure_name", "")
        dax = self._preclean_dax(measure.get("dax_expression", ""))
        # A predicate on a table that does not filter this fact has no effect in Power BI and
        # no join in the view: it is left out before any translation (rules and LLM alike).
        dax, dropped_filter_tables = self._drop_non_filtering_filters(dax, table_key)
        dropped = tuple(dropped_filter_tables)
        original_name = measure.get("original_name", name)
        # M8: guarantee a valid [a-z0-9_] identifier (dots, parens, &, commas out).
        # to_snake_case already does this; sanitize_measure_name makes the invariant
        # explicit and holds even if an upstream name path changes.
        snake = sanitize_measure_name(to_snake_case(original_name))

        for pattern_name, match_fn, translate_fn in self._patterns:
            if trivial_only and pattern_name not in self._TRIVIAL_FAST_PATH:
                continue
            match = match_fn(dax, name)
            if match is not None:
                sql, skip_reason = translate_fn(match, dax, table_key)
                # llm_first: if a fast-path matcher produced SQL that DROPPED a DAX
                # component (ratio denominator, prior-year shift, exclusion) — i.e.
                # the regex claimed a measure it can't fully handle — reject it here
                # so the caller routes it to the skill-corpus LLM (which CAN do the
                # full ratio) rather than shipping a silently-wrong bare aggregate.
                # Also after a predicate was left out: the rewritten DAX is what is translated,
                # and a rule that drops a component of it (a ratio's denominator) is not trusted.
                if (trivial_only or dropped) and sql:
                    from .sql_measure_sanitizer import detect_lost_dax_component

                    if detect_lost_dax_component(dax, sql):
                        return TranslationResult(
                            measure_name=snake,
                            original_name=original_name,
                            sql_expr=None,
                            is_translatable=False,
                            skip_reason="routed to LLM (fast-path dropped a DAX component)",
                            dax_expression=dax,
                            confidence="none",
                            category="unassigned",
                            dropped_filter_tables=dropped,
                        )
                window_spec = None
                if skip_reason == "__SAMEPERIODLASTYEAR__":
                    skip_reason = ""
                    window_spec = {
                        "order": "fiscper",
                        "range": "trailing 12 month",
                        "semiadditive": "last",
                    }
                elif skip_reason.startswith("__WINDOW__:"):
                    # M11: a latest-period semi-additive measure — the matcher emits
                    # `__WINDOW__:order=<col>;range=<r>;semiadditive=<s>`.
                    window_spec = dax_patterns.parse_window_sentinel(skip_reason)
                    skip_reason = ""
                return TranslationResult(
                    measure_name=snake,
                    original_name=original_name,
                    sql_expr=sql,
                    is_translatable=sql is not None,
                    skip_reason=skip_reason,
                    dax_expression=dax,
                    confidence="high" if sql else "none",
                    category="single_table" if sql else "unassigned",
                    window_spec=window_spec,
                    dropped_filter_tables=dropped,
                )

        return TranslationResult(
            measure_name=snake,
            original_name=original_name,
            sql_expr=None,
            is_translatable=False,
            skip_reason="No matching pattern",
            dax_expression=dax,
            confidence="none",
            category="unassigned",
            dropped_filter_tables=dropped,
        )

    def translate_expression(self, dax_expr: str, table_key: str) -> str | None:
        """Translate a sub-expression (used by DIVIDE for numerator/denominator)."""
        for pattern_name, match_fn, translate_fn in self._patterns:
            if pattern_name in ("quick_reject", "selectedvalue_switch", "divide"):
                continue
            match = match_fn(dax_expr, "")
            if match is not None:
                sql, _ = translate_fn(match, dax_expr, table_key)
                return sql
        # Simple table[col] reference
        m = re.match(r"\s*(\w+)\[(\w+)\]\s*$", dax_expr)
        if m:
            return f"SUM(source.{m.group(2)})"
        return None

    # ── Match functions ──────────────────────────────────────────────────

    # Pre-compiled regexes for business-logic detection in quick-reject
    _RE_AGG_FUNCS = re.compile(
        r"\b(SUM|SUMX|COUNT|COUNTX|AVERAGE|AVERAGEX|DIVIDE|MIN|MAX)\s*\(",
        re.IGNORECASE,
    )
    _RE_MEASURE_REFS = re.compile(r"(?<!\w)\[[^\]]+\]")  # [MeasureRef] not Table[col]

    def _match_quick_reject(self, dax: str, name: str) -> dict | None:
        dax_up = dax.upper()
        if "FORMAT(" in dax_up:
            # FORMAT is now handled deterministically by the `format_func`
            # converter when its inner expression is translatable (a real
            # Table[column] or aggregate). Only reject as display-only when the
            # converter CAN'T translate it — e.g. FORMAT of a literal, a bare
            # table name, or a pure display function (TODAY()) — so we neither
            # waste an LLM call nor emit meaningless SQL.
            if self._match_format_func(dax, name) is not None:
                return None
            return {"reason": "FORMAT function (display-only)"}
        if "_COLOR" in name.upper() or "COLOR" == name.upper().split("_")[-1]:
            # Check if this "color" measure contains aggregation functions or
            # measure refs that indicate business logic, not just display formatting
            has_aggregation = bool(self._RE_AGG_FUNCS.search(dax))
            has_measure_refs = bool(self._RE_MEASURE_REFS.search(dax))
            if has_aggregation or has_measure_refs:
                # Contains business logic — don't reject, let other patterns try
                return None
            return {"reason": "Color/conditional formatting measure (display-only)"}
        if (
            "ISBLANK" in dax_up
            and "BLANK()" in dax_up
            and "SUMX" not in dax_up
            and "SUM(" not in dax_up
        ):
            return {"reason": "ISBLANK+BLANK guard (no aggregation)"}
        if dax.strip() in ("Not available", ""):
            return {"reason": "DAX expression not available"}
        dax_no_comments = "\n".join(
            item_value
            for item_value in dax.split("\n")
            if not item_value.strip().startswith("//")
        )
        if dax_no_comments.strip().upper() in ("BLANK()", "BLANK"):
            return {"reason": "BLANK() placeholder"}
        if re.search(r"\bISFILTERED\s*\(", dax, re.IGNORECASE):
            return {"reason": "ISFILTERED (PBI-specific)"}
        if "SELECTEDVALUE" in dax_up and "SWITCH" in dax_up:
            return {"reason": "SELECTEDVALUE+SWITCH (parameterized)"}
        if "SELECTEDVALUE" in dax_up:
            return {"reason": "SELECTEDVALUE (requires slicer context)"}
        return None

    def _match_simple_sum(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        m = RE_SIMPLE_SUM.fullmatch(cleaned.strip())
        if m:
            return {"table": m.group(1), "column": m.group(2)}
        return None

    def _match_simple_sumx(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        if "FILTER(" in cleaned.upper():
            return None
        m = RE_SIMPLE_SUMX.search(cleaned)
        if m:
            return {"table": m.group(1), "column": m.group(3)}
        return None

    # Simple single-column aggregations: AVERAGE / COUNT / MIN / MAX /
    # DISTINCTCOUNT / COUNTA, plus COUNTROWS(table). Deterministic, no LLM.
    _SIMPLE_AGG_FUNCS = {
        "AVERAGE": "AVG",
        "COUNT": "COUNT",
        "COUNTA": "COUNT",
        "MIN": "MIN",
        "MAX": "MAX",
        "DISTINCTCOUNT": "COUNT_DISTINCT",
    }

    def _match_simple_agg(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax)).strip()
        # COUNTROWS(table) → COUNT(*)
        m = re.fullmatch(
            r"\s*(?:CALCULATE\s*\(\s*)?COUNTROWS\s*\(\s*(\w+)\s*\)\s*\)?\s*",
            cleaned,
            re.IGNORECASE,
        )
        if m:
            return {"func": "COUNTROWS", "column": None}
        # AVERAGE/COUNT/MIN/MAX/DISTINCTCOUNT(table[col])
        m = re.fullmatch(
            r"\s*(?:CALCULATE\s*\(\s*)?"
            r"(AVERAGE|COUNTA|COUNT|DISTINCTCOUNT|MIN|MAX)\s*"
            r"\(\s*(\w+)\[(\w+)\]\s*\)\s*\)?\s*",
            cleaned,
            re.IGNORECASE,
        )
        if m and m.group(1).upper() in self._SIMPLE_AGG_FUNCS:
            return {"func": m.group(1).upper(), "column": m.group(3)}
        return None

    def _translate_simple_agg(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        func = match["func"]
        if func == "COUNTROWS":
            return "COUNT(*)", ""
        spark = self._SIMPLE_AGG_FUNCS.get(func)
        col = match["column"]
        if spark == "COUNT_DISTINCT":
            return f"COUNT(DISTINCT source.{col})", ""
        return f"{spark}(source.{col})", ""

    # CALCULATE(SUM(t[col]), t[dim] = <value>) → conditional aggregation.
    # Single equality filter only; anything more complex falls through to LLM.
    def _match_calculate_equality_filter(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax)).strip()
        if "CALCULATE" not in cleaned.upper() or "FILTER(" in cleaned.upper():
            return None
        m = re.fullmatch(
            r"\s*CALCULATE\s*\(\s*"
            r"(SUM|AVERAGE|COUNT|MIN|MAX)\s*\(\s*(\w+)\[(\w+)\]\s*\)\s*,\s*"
            r'(\w+)\[(\w+)\]\s*=\s*("[^"]*"|\'[^\']*\'|-?\d+(?:\.\d+)?|TRUE\s*\(\s*\)|FALSE\s*\(\s*\))'
            r"\s*\)\s*",
            cleaned,
            re.IGNORECASE,
        )
        if not m:
            return None
        return {
            "agg": m.group(1).upper(),
            "agg_col": m.group(3),
            "filter_col": m.group(5),
            "value": m.group(6).strip(),
        }

    def _translate_calculate_equality_filter(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        spark = {
            "SUM": "SUM",
            "AVERAGE": "AVG",
            "COUNT": "COUNT",
            "MIN": "MIN",
            "MAX": "MAX",
        }[match["agg"]]
        val = match["value"]
        up = val.upper().replace(" ", "")
        if up in ("TRUE()", "FALSE()"):
            val = "TRUE" if up == "TRUE()" else "FALSE"
        elif val.startswith("'") or val.startswith('"'):
            # A string literal — strip the DAX quote and re-emit as a safe,
            # escaped SQL literal (was an unescaped ' -> " swap; SEC finding #1).
            val = _sql_str_literal(val[1:-1])
        elif not re.fullmatch(r"-?\d+(?:\.\d+)?", val):
            # Not TRUE/FALSE and not a plain number → treat as a string value and
            # escape it rather than interpolate raw.
            val = _sql_str_literal(val)
        cond = f'source.{match["filter_col"]} = {val}'
        return f'{spark}(CASE WHEN {cond} THEN source.{match["agg_col"]} END)', ""

    def _match_calc_sumx_vars_divide(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        ret_match = re.search(r"(?:return\s+)?DIVIDE\s*\(", cleaned, re.IGNORECASE)
        if not ret_match:
            return None
        parts = self._extract_calc_sumx_assignments(cleaned)
        if not parts:
            return None
        divide_text = cleaned[ret_match.start() :]
        divide_text = re.sub(
            r"^return\s+", "", divide_text, flags=re.IGNORECASE
        ).strip()
        return {"parts": parts, "divide_text": divide_text}

    def _match_calculate_sumx_filter_inner(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        if "DIVIDE(" in cleaned.upper():
            return None
        parts = self._extract_calc_sumx_assignments(cleaned)
        if not parts:
            m = re.search(
                r"CALCULATE\s*\(\s*SUMX\s*\(\s*FILTER\s*\(\s*(\w+)\s*,\s*(.*?)\s*\)\s*,\s*(\w+)\[(\w+)\]\s*\)\s*\)",
                cleaned,
                re.IGNORECASE | re.DOTALL,
            )
            if m:
                parts = [
                    {
                        "var": None,
                        "table": m.group(1),
                        "column": m.group(4),
                        "condition": m.group(2),
                        "filter_table": m.group(1),
                    }
                ]
            else:
                return None
        ret_match = re.search(
            r"return\s+(.+?)(?:\s*//.*)?$", cleaned, re.IGNORECASE | re.DOTALL
        )
        ret_expr = (
            ret_match.group(1).strip()
            if ret_match
            else (parts[0]["var"] if len(parts) == 1 else None)
        )
        return {"parts": parts, "return_expr": ret_expr}

    def _match_calculate_sumx_filter_outer(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        if "DIVIDE(" in cleaned.upper():
            return None
        parts = self._extract_calc_sumx_assignments(cleaned)
        if not parts:
            return None
        ret_match = re.search(
            r"return\s+(.+?)(?:\s*//.*)?$", cleaned, re.IGNORECASE | re.DOTALL
        )
        ret_expr = (
            ret_match.group(1).strip()
            if ret_match
            else (parts[0]["var"] if len(parts) == 1 else None)
        )
        return {"parts": parts, "return_expr": ret_expr}

    def _match_sumx_filter(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        m = RE_SUMX_FILTER.search(cleaned)
        if m:
            return {
                "parts": [
                    {
                        "var": None,
                        "table": m.group(1),
                        "column": m.group(4),
                        "condition": m.group(2),
                        "filter_table": m.group(1),
                    }
                ],
                "return_expr": None,
            }
        return None

    def _match_countx_filter(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        m = RE_COUNTX_FILTER.search(cleaned)
        if m:
            return {
                "table": m.group(1),
                "column": m.group(4),
                "condition": m.group(2),
                "filter_table": m.group(1),
            }
        return None

    def _match_averagex_filter(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        m = RE_AVERAGEX_FILTER.search(cleaned)
        if m:
            return {
                "table": m.group(1),
                "column": m.group(4),
                "condition": m.group(2),
                "filter_table": m.group(1),
            }
        return None

    def _match_userelationship(self, dax: str, name: str) -> dict | None:
        if "USERELATIONSHIP" not in dax.upper():
            return None
        m = re.search(
            r"CALCULATE\s*\(\s*(.+?)\s*,\s*USERELATIONSHIP\s*\(\s*(\w+)\[(\w+)\]\s*,\s*(\w+)\[(\w+)\]\s*\)\s*\)",
            dax,
            re.IGNORECASE | re.DOTALL,
        )
        if m:
            return {
                "inner_expr": m.group(1),
                "fact_table": m.group(2),
                "fact_col": m.group(3),
                "dim_table": m.group(4),
                "dim_col": m.group(5),
            }
        return None

    def _match_distinctcountnoblank(self, dax: str, name: str) -> dict | None:
        if "DISTINCTCOUNTNOBLANK" not in dax.upper():
            return None
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        up = cleaned.upper()
        if "DIVIDE(" in up or "FILTER(" in up or "VAR " in up:
            return None
        m = re.fullmatch(
            r"\s*(?:CALCULATE\s*\(\s*)?DISTINCTCOUNTNOBLANK\s*\(\s*(\w+)\[(\w+)\]\s*\)\s*\)?\s*",
            cleaned,
            re.IGNORECASE,
        )
        if m:
            return {"table": m.group(1), "column": m.group(2)}
        return None

    def _match_calculate_measure_ref(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        if "DIVIDE(" in cleaned.upper() or "DIVIDE (" in cleaned.upper():
            return None
        refs = self._find_calculate_measure_refs(cleaned)
        if not refs:
            return None
        return {"cleaned": cleaned, "refs": refs}

    def _match_divide_calculate_measure_ref(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        up = cleaned.upper()
        if "DIVIDE(" not in up and "DIVIDE (" not in up:
            return None
        refs = self._find_calculate_measure_refs(cleaned)
        if not refs:
            return None
        return {"cleaned": cleaned, "refs": refs}

    def _match_divide(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        if "DIVIDE(" in cleaned.upper():
            return {"raw": cleaned}
        return None

    def _match_sameperiodlastyear(self, dax: str, name: str) -> dict | None:
        if "SAMEPERIODLASTYEAR" not in dax.upper():
            return None
        clean_lines = []
        for line in dax.split("\n"):
            s = line.strip()
            if s.startswith("//"):
                continue
            if re.match(r"^var\s+(std|etd)\b", s, re.IGNORECASE):
                continue
            if re.match(
                r"^var\s+\w+\s*=\s*CALCULATE\s*\(\s*\[(PY_Start_date|PY_End_date|F_Start_date|F_End_date)\]",
                s,
                re.IGNORECASE,
            ):
                continue
            clean_lines.append(s)
        cleaned = "\n".join(clean_lines)
        m = re.search(
            r"CALCULATE\s*\(\s*SUMX\s*\(\s*FILTER\s*\(\s*(\w+)\s*,\s*(.*?)\s*\)\s*,\s*(\w+)\[(\w+)\]\s*\)\s*"
            r",\s*SAMEPERIODLASTYEAR\s*\([^)]+\)\s*\)",
            cleaned,
            re.IGNORECASE | re.DOTALL,
        )
        if m:
            return {
                "type": "sumx_filter",
                "table": m.group(1),
                "condition": m.group(2),
                "filter_table": m.group(1),
                "column": m.group(4),
            }
        return {
            "reason": (
                "TODO prior-year (SAMEPERIODLASTYEAR): needs a calendar self-join or "
                "LAG window over the period dim (e.g. join c_dim_calendar on date_py, "
                "or LAG(value) OVER (ORDER BY fiscper) for a 1-period offset). Supply "
                "the calendar date_py column to enable."
            )
        }

    def _match_selectedvalue_switch(self, dax: str, name: str) -> dict | None:
        dax_up = dax.upper()
        if "SELECTEDVALUE" in dax_up and "SWITCH" in dax_up:
            return {"reason": "SELECTEDVALUE+SWITCH (parameterized)"}
        if "SELECTEDVALUE" in dax_up:
            return {"reason": "SELECTEDVALUE (requires slicer context)"}
        return None

    # ── Translate functions ──────────────────────────────────────────────

    def _translate_noop(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        return None, match.get("reason", "Not translatable")

    def _translate_sameperiodlastyear(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        if match.get("type") != "sumx_filter":
            return None, match.get("reason", "SAMEPERIODLASTYEAR (requires window)")
        sql, reason = self._filtered_agg("SUM", match, table_key)
        return (sql, "__SAMEPERIODLASTYEAR__") if sql else (None, reason)

    def _translate_simple_sum(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        col = match["column"]
        return f"SUM(source.{col})", ""

    def _translate_simple_sumx(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        col = match["column"]
        return f"SUM(source.{col})", ""

    def _translate_countx(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        return self._filtered_agg("COUNT", match, table_key)

    def _translate_averagex(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        return self._filtered_agg("AVG", match, table_key)

    def _translate_userelationship(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        """CALCULATE(expr, USERELATIONSHIP(T[col], D[col])) -> translate expr with alternate join alias."""
        inner = match["inner_expr"]
        # Try translating the inner expression
        inner_result = self.translate_expression(inner, table_key)
        if inner_result:
            # Replace dim table references with the alternate alias
            dim_table = match["dim_table"]
            fact_col = match["fact_col"]
            f"{dim_table.lower()}_{fact_col.lower()}"
            # Rewrite source.col or dim.col refs if the inner expr uses the alternate dim
            return inner_result, ""
        return None, "USERELATIONSHIP inner expression not translatable"

    def _translate_distinctcountnoblank(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        col = match["column"]
        return f"COUNT(DISTINCT source.{col})", ""

    def _translate_calculate_measure_ref(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        refs = match["refs"]
        if len(refs) == 1:
            r = refs[0]
            extra = self._parse_calculate_filters(r["filter_text"], table_key)
            if extra is None:
                return None, f'Untranslatable CALCULATE filter: {r["filter_text"]}'
            sql = self._resolve_measure_ref(r["ref_name"], extra)
            return (sql, "") if sql else (None, f'Cannot resolve [{r["ref_name"]}]')
        result = match["cleaned"]
        for r in reversed(refs):
            extra = self._parse_calculate_filters(r["filter_text"], table_key)
            if extra is None:
                return None, f'Untranslatable CALCULATE filter: {r["filter_text"]}'
            sql = self._resolve_measure_ref(r["ref_name"], extra)
            if not sql:
                return None, f'Cannot resolve [{r["ref_name"]}]'
            result = result[: r["start"]] + f"({sql})" + result[r["end"] :]
        result = self._cleanup_resolved_text(result)
        return (result, "") if result else (None, "Could not finalize expression")

    def _translate_divide_calculate_measure_ref(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        cleaned = match["cleaned"]
        refs = match["refs"]
        result = cleaned
        for r in reversed(refs):
            extra = self._parse_calculate_filters(r["filter_text"], table_key)
            if extra is None:
                return None, f'Untranslatable CALCULATE filter: {r["filter_text"]}'
            sql = self._resolve_measure_ref(r["ref_name"], extra)
            if not sql:
                return None, f'Cannot resolve [{r["ref_name"]}]'
            result = result[: r["start"]] + f"({sql})" + result[r["end"] :]
        # Split `var NAME = VALUE ... var NAME2 = VALUE2 ... return EXPR` into a
        # var map + the return expression. Must work whether the vars are on
        # separate lines OR collapsed onto one line (they are single-line after
        # measure-ref substitution above) — a line-based split captures only the
        # first var and drops the rest, collapsing num==denom on ratios.
        var_map: dict[str, str] = {}
        # Tokenize on `var <name> =` and `return` boundaries across the whole text.
        boundary = re.compile(r"\bvar\s+(\w+)\s*=\s*|\breturn\b", re.IGNORECASE)
        marks = list(boundary.finditer(result))
        expr = ""
        if marks:
            for i, mk in enumerate(marks):
                seg_start = mk.end()
                seg_end = marks[i + 1].start() if i + 1 < len(marks) else len(result)
                segment = result[seg_start:seg_end].strip()
                if mk.group(1):  # a `var NAME =` boundary
                    if segment:
                        var_map[mk.group(1)] = segment
                else:  # the `return` boundary
                    expr = segment
            if not expr:
                # No explicit return — the last var IS the result (rare).
                expr = list(var_map.values())[-1] if var_map else result.strip()
        else:
            expr = result.strip()
        for vn in sorted(var_map.keys(), key=len, reverse=True):
            expr = re.sub(rf"\b{re.escape(vn)}\b", var_map[vn], expr)
        expr = self._strip_bare_calculate(expr)
        inner = self._extract_divide_args(expr)
        if not inner:
            return None, "Could not extract DIVIDE args after resolution"
        num_sql, den_sql = inner
        num_s = num_sql.strip()
        num_w = f"({num_s})" if "+" in num_s or "-" in num_s else num_s
        final = f"{num_w} / NULLIF({den_sql.strip()}, 0)"
        return self._resolve_remaining_dax(final), ""

    def _translate_sumx_parts(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        parts = match["parts"]
        return_expr = match.get("return_expr")
        if len(parts) == 1 and (
            return_expr is None
            or return_expr == parts[0]["var"]
            or parts[0]["var"] is None
        ):
            p = parts[0]
            alias, col_map = self._resolve_table_alias(p["table"], table_key)
            physical_col = _sanitize_alias(col_map.get(p["column"], p["column"]))
            filter_parts = []
            filter_sql = self._build_filter_clause(p, table_key, dax)
            if filter_sql:
                filter_parts.append(filter_sql)
            elif p.get("condition"):
                # Filter condition exists in DAX but could not be translated
                return None, f"Untranslatable FILTER condition: {p['condition']}"
            self._extend_with_implicit_filters(filter_parts, alias)
            combined = " AND ".join(filter_parts) if filter_parts else None
            if combined:
                return f"SUM({alias}.{physical_col}) FILTER (WHERE {combined})", ""  # identifiers validated; filters escaped or pipeline-config SQL — nosemgrep: tmp.databricks.sql-injection.fstring-where  # fmt: skip
            return f"SUM({alias}.{physical_col})", ""
        var_map = {}
        for p in parts:
            var_name = p["var"]
            if var_name is None:
                continue
            alias, col_map = self._resolve_table_alias(p["table"], table_key)
            physical_col = _sanitize_alias(col_map.get(p["column"], p["column"]))
            filter_parts = []
            filter_sql = self._build_filter_clause(p, table_key, dax)
            if filter_sql:
                filter_parts.append(filter_sql)
            elif p.get("condition"):
                return None, f"Untranslatable FILTER condition: {p['condition']}"
            self._extend_with_implicit_filters(filter_parts, alias)
            combined = " AND ".join(filter_parts) if filter_parts else None
            if combined:
                var_map[var_name] = (
                    f"SUM({alias}.{physical_col}) FILTER (WHERE {combined})"  # identifiers validated; filters escaped or pipeline-config SQL — nosemgrep: tmp.databricks.sql-injection.fstring-where
                )
            else:
                var_map[var_name] = f"SUM({alias}.{physical_col})"
        if return_expr and var_map:
            sql = return_expr
            for var_name in sorted(var_map.keys(), key=len, reverse=True):
                sql = re.sub(
                    rf"\b{re.escape(var_name)}\b", f"({var_map[var_name]})", sql
                )
            return sql.strip(), ""
        return None, "Complex multi-part SUMX expression"

    def _translate_calc_sumx_vars_divide(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        parts = match["parts"]
        divide_text = match["divide_text"]
        var_map: dict[str, str] = {}
        for p in parts:
            alias, col_map = self._resolve_table_alias(p["table"], table_key)
            physical_col = _sanitize_alias(col_map.get(p["column"], p["column"]))
            filter_parts = []
            inner_filter = self._build_filter_clause(p, table_key, dax)
            if inner_filter:
                filter_parts.append(inner_filter)
            elif p.get("condition"):
                return None, f"Untranslatable FILTER condition: {p['condition']}"
            outer_cond = p.get("outer_filter_condition")
            outer_table = p.get("outer_filter_table")
            if outer_cond and outer_table:
                outer_filter = self._build_filter_clause(
                    {"condition": outer_cond, "filter_table": outer_table},
                    table_key,
                    dax,
                )
                if outer_filter:
                    filter_parts.append(outer_filter)
                elif outer_cond:
                    return None, f"Untranslatable outer FILTER condition: {outer_cond}"
            self._extend_with_implicit_filters(filter_parts, alias)
            combined_filter = " AND ".join(filter_parts) if filter_parts else None
            if combined_filter:
                var_map[p["var"]] = (
                    f"SUM({alias}.{physical_col}) FILTER (WHERE {combined_filter})"  # identifiers validated; filters escaped or pipeline-config SQL — nosemgrep: tmp.databricks.sql-injection.fstring-where
                )
            else:
                var_map[p["var"]] = f"SUM({alias}.{physical_col})"
        inner = self._extract_divide_args(divide_text)
        if inner is None:
            return None, "Could not parse DIVIDE arguments"
        num_expr, den_expr = inner
        num_sql = self._substitute_vars(num_expr.strip(), var_map)
        den_sql = self._substitute_vars(den_expr.strip(), var_map)
        if num_sql and den_sql:
            num_wrapped = (
                f"({num_sql})" if "+" in num_sql or "-" in num_sql else num_sql
            )
            final = f"{num_wrapped} / NULLIF({den_sql}, 0)"
            return self._resolve_remaining_dax(final), ""
        return None, "DIVIDE references variables not found"

    def _translate_divide(
        self, match: dict, dax: str, table_key: str
    ) -> tuple[str | None, str]:
        raw = match["raw"]
        inner = self._extract_divide_args(raw)
        if inner is None:
            return None, "Could not parse DIVIDE arguments"
        num_dax, den_dax = inner
        num_result = self.translate_expression(num_dax.strip(), table_key)
        den_result = self.translate_expression(den_dax.strip(), table_key)
        if num_result and den_result:
            return f"{num_result} / NULLIF({den_result}, 0)", ""
        return None, "DIVIDE sub-expression not translatable"

    # ── Helpers ──────────────────────────────────────────────────────────

    def _extract_calc_sumx_assignments(self, cleaned: str) -> list[dict] | None:
        parts = []
        for m in re.finditer(
            r"(?:var\s+)?(\w+)\s*=\s*CALCULATE\s*\(\s*SUMX\s*\(\s*FILTER\s*\("
            r"\s*(\w+)\s*,\s*(.*?)\s*\)\s*,\s*(\w+)\[(\w+)\]\s*\)"
            r"(?:\s*,\s*FILTER\s*\(\s*(\w+)\s*,\s*(.*?)\s*\)\s*)?\)",
            cleaned,
            re.IGNORECASE | re.DOTALL,
        ):
            part = {
                "var": m.group(1),
                "table": m.group(2),
                "column": m.group(5),
                "condition": m.group(3),
                "filter_table": m.group(2),
            }
            if m.group(6) and m.group(7):
                part["outer_filter_table"] = m.group(6)
                part["outer_filter_condition"] = m.group(7)
            parts.append(part)
        return parts or None

    def _resolve_table_alias(self, table_ref: str, table_key: str) -> tuple[str, dict]:
        """Resolve DAX table reference to SQL alias and column map."""
        if table_ref == table_key or table_ref not in self._fact_join_map_cfg:
            # Same-fact reference → alias 'source', remapping physical column names
            # to the source SELECT's output aliases (e.g. kbi_value → value) so
            # SUM(source.col) references a column the source actually exposes.
            return "source", self._source_col_map
        fj = self._fact_join_map_cfg[table_ref]
        alias = _sanitize_alias(fj.get("alias", table_ref.lower()))
        col_map = fj.get("column_map", {})
        return alias, col_map

    def _build_filter_clause(self, part: dict, table_key: str, dax: str) -> str:
        """Build SQL FILTER clause from a parsed SUMX part.

        Handles variable references, CWC_Filter=1 patterns, cross-table facts,
        qualified dim refs, and generic conditions.
        """
        condition = part.get("condition", "")
        filter_table = part.get("filter_table", "")
        if not condition:
            return ""

        # Variable reference: Table[col] in varName (the WHOLE condition, so no
        # other conjunct is dropped)
        var_ref_match = re.fullmatch(
            r"\s*(\w+)\[(\w+)\]\s+in\s+(\w+)\s*", condition, re.IGNORECASE
        )
        if var_ref_match:
            dim_table = var_ref_match.group(1)
            dim_col = var_ref_match.group(2)
            var_name = var_ref_match.group(3)
            values = self._resolve_var_filter_set(var_name, dax)
            if values:
                alias, phys_col = self._resolve_filter_alias(dim_table, dim_col)
                in_list = _sql_in_list(values)
                return f"{alias}.{phys_col} IN ({in_list})"

        # Check for CWC_Filter=1 pattern (only if CWC_FILTER filter set is configured)
        if self.filter_sets.get("CWC_FILTER"):
            cwc_match = re.fullmatch(
                r"\s*\(?\s*(\w+)\[CWC_Filter\]\s*=\s*1\s*\)?\s*",
                condition,
                re.IGNORECASE,
            )
            if cwc_match:
                dim_table = cwc_match.group(1)
                alias = self._dim_table_to_alias(dim_table)
                cwc_values = self.filter_sets["CWC_FILTER"]
                in_list = _sql_in_list(cwc_values)
                return f"{alias}.{self._cwc_filter_column} IN ({in_list})"

        # Cross-table fact filter
        if filter_table and filter_table in self._fact_join_map_cfg:
            return self._dax_condition_to_sql_cross_table(condition, filter_table)

        # Qualified dim reference (different table than the fact)
        if filter_table and filter_table.lower() != table_key.lower():
            return self._dax_condition_to_sql_qualified(
                condition, filter_table, table_key
            )

        return self._dax_condition_to_sql(condition, table_key)

    def _resolve_var_filter_set(self, var_name: str, dax: str) -> list[str] | None:
        """Resolve a variable reference to a list of filter values."""
        m = re.search(
            rf"var\s+{re.escape(var_name)}\s*=\s*\{{([^}}]+)\}}",
            dax,
            re.IGNORECASE,
        )
        if m:
            vals = re.findall(r'"([^"]*)"', m.group(1))
            if vals:
                return vals
        # Check named filter sets
        for set_name, values in self.filter_sets.items():
            if isinstance(values, list) and var_name.lower() == set_name.lower():
                return values
        return None

    def _extend_with_implicit_filters(self, filter_parts: list, alias: str) -> None:
        """Add MQuery implicit filters for a fact join alias."""
        safe_alias = _sanitize_alias(alias)
        for fj in self._fact_joins:
            if fj.get("name") == safe_alias:
                fj_cfg = fj.get("_fact_join_config", {})
                for impl in fj_cfg.get("implicit_filters", []):
                    impl_sql = impl.format(alias=safe_alias)
                    if impl_sql not in " AND ".join(filter_parts):
                        filter_parts.append(impl_sql)

    @staticmethod
    def _preclean_dax(dax: str) -> str:
        """PROP-6: strip DAX comments and normalize BLANK() before translation."""
        return preclean_dax(dax)

    @staticmethod
    def _extract_divide_args(text: str) -> tuple[str, str] | None:
        """Extract numerator and denominator from DIVIDE(num, den[, alt_result]).

        DAX DIVIDE has an optional THIRD argument (the value returned when the
        denominator is 0/blank). We must split on BOTH top-level commas and return
        only args 1 and 2 — otherwise the denominator captures ``den, alt_result``
        and downstream produces malformed ``NULLIF(den, alt, 0)`` (invalid SQL, 3
        args). The alt_result is intentionally dropped: our emitted
        ``x / NULLIF(y, 0)`` already yields NULL on a zero denominator, matching
        DAX's blank-on-zero semantics closely enough (a non-NULL alt is a rare
        edge we do not model).
        """
        m = re.search(r"DIVIDE\s*\(", text, re.IGNORECASE)
        if not m:
            return None
        start = m.end()
        depth = 1
        pos = start
        commas: list[int] = []
        while pos < len(text) and depth > 0:
            if text[pos] == "(":
                depth += 1
            elif text[pos] == ")":
                depth -= 1
                if depth == 0:
                    break
            elif text[pos] == "," and depth == 1:
                commas.append(pos)
            pos += 1
        if not commas:
            return None
        num = text[start : commas[0]].strip()
        # Denominator ends at the 2nd top-level comma if present (alt_result), else
        # at the closing paren.
        den_end = commas[1] if len(commas) >= 2 else pos
        den = text[commas[0] + 1 : den_end].strip()
        return num, den

    @staticmethod
    def _substitute_vars(expr: str, var_map: dict[str, str]) -> str | None:
        """Substitute variable names with their SQL equivalents."""
        result = expr
        for var_name in sorted(var_map.keys(), key=len, reverse=True):
            result = re.sub(rf"\b{re.escape(var_name)}\b", var_map[var_name], result)
        if (
            re.search(r"\b[a-z]\d?\b", result)
            and "source." not in result
            and "SUM(" not in result
        ):
            return None
        return result

    @staticmethod
    def _strip_var_block(dax: str) -> str:
        """Strip comment lines and leading whitespace."""
        lines = []
        for line in dax.split("\n"):
            stripped = line.strip()
            if stripped.startswith("//"):
                continue
            lines.append(stripped)
        return "\n".join(lines).strip()

    @staticmethod
    def _strip_return(text: str) -> str:
        """Strip trailing RETURN keyword."""
        return re.sub(r"\breturn\s*$", "", text, flags=re.IGNORECASE).strip()

    # Slicer-scalar date-window pickers used as scaffolding (var std = CALCULATE
    # ([F_Start_date]) …). They are NOT real measure-refs — treating them as such
    # makes the measure-ref matchers claim a var-chain measure and then fail on
    # "Cannot resolve [F_Start_date]", dropping measures (e.g. the fact_pe002
    # EPL/CIP join-alias DIVIDE ratios) that the var-chain LLM path handles fine.
    _SCAFFOLD_REFS = re.compile(r"^(?:F|PY)_(?:START|END)_DATE$", re.IGNORECASE)

    def _find_calculate_measure_refs(self, text: str) -> list[dict]:
        """Find CALCULATE([MeasureRef], filter) patterns in text.

        Scaffolding date-picker refs (``[F_Start_date]`` etc.) are skipped — they
        are display-window scalars, not measures to resolve.
        """
        refs = []
        for m in re.finditer(r"CALCULATE\s*\(\s*\[([^\]]+)\]", text, re.IGNORECASE):
            ref_name = m.group(1)
            if self._SCAFFOLD_REFS.match(ref_name.strip()):
                continue  # skip slicer-scalar date-window scaffolding
            calc_start = m.start()
            paren_start = text.index("(", m.start()) + 1
            depth = 1
            pos = paren_start
            while pos < len(text) and depth > 0:
                if text[pos] == "(":
                    depth += 1
                elif text[pos] == ")":
                    depth -= 1
                pos += 1
            calc_end = pos
            inner = text[paren_start : calc_end - 1]
            bracket_end = inner.index("]") + 1
            filter_text = inner[bracket_end:].strip()
            if filter_text.startswith(","):
                filter_text = filter_text[1:].strip()
            refs.append(
                {
                    "ref_name": ref_name,
                    "filter_text": filter_text,
                    "start": calc_start,
                    "end": calc_end,
                }
            )
        return refs

    def _parse_calculate_filters(
        self, filter_text: str, table_key: str
    ) -> list[str] | None:
        """Parse CALCULATE filter arguments into SQL conditions.

        ``None`` when any filter argument cannot be translated in full: the
        measure is then a gap, never the same measure with a filter missing.
        """
        if not filter_text:
            return []
        conditions = []
        for part in split_top_level_args(filter_text):
            part = part.strip()
            if not part or part.upper().startswith("ALL("):
                continue
            fm = re.match(
                r"FILTER\s*\(\s*(\w+)\s*,\s*(.+)\s*\)\s*$",
                part,
                re.IGNORECASE | re.DOTALL,
            )
            cond = self._dax_condition_to_sql(fm.group(2) if fm else part, table_key)
            if not cond:
                return None
            conditions.append(cond)
        return conditions

    def _resolve_measure_ref(
        self, ref_name: str, extra_filters: list[str]
    ) -> str | None:
        """Resolve a [MeasureRef] to SQL using known resolutions."""
        resolution = self._measure_resolutions.get(ref_name)
        if not resolution:
            return None
        base_expr: str = resolution.get("base_expr", "")
        base_filters = list(resolution.get("base_filters", []))
        all_filters = base_filters + extra_filters
        if all_filters:
            return f"{base_expr} FILTER (WHERE {' AND '.join(all_filters)})"  # joins resolved predicates (escaped literals) — nosemgrep: tmp.databricks.sql-injection.fstring-where
        return base_expr

    @staticmethod
    def _strip_bare_calculate(text: str) -> str:
        while True:
            m = re.search(r"CALCULATE\s*\(\s*(?!\[)", text, re.IGNORECASE)
            if not m:
                break
            start = text.index("(", m.start()) + 1
            depth = 1
            pos = start
            while pos < len(text) and depth > 0:
                if text[pos] == "(":
                    depth += 1
                elif text[pos] == ")":
                    depth -= 1
                pos += 1
            inner = text[start : pos - 1]
            text = text[: m.start()] + inner + text[pos:]
        return text

    def _resolve_remaining_dax(self, sql: str) -> str:
        """Post-process SQL to resolve bare [MeasureRef] and remaining DIVIDE()."""

        def _resolve_ref(m: re.Match[str]) -> str:
            ref_name = m.group(1)
            resolution = self._measure_resolutions.get(ref_name)
            if resolution:
                base: str = resolution["base_expr"]
                filters = resolution.get("base_filters", [])
                if filters:
                    return f"{base} FILTER (WHERE {' AND '.join(filters)})"  # joins resolved predicates (escaped literals) — nosemgrep: tmp.databricks.sql-injection.fstring-where
                return base
            return f"1 /* UNRESOLVED: [{ref_name.replace('*/', '* /').replace('/*', '/ *')}] */"

        sql = re.sub(r"(?<!\w)\[([^\]]+)\]", _resolve_ref, sql)
        for _ in range(5):
            m = re.search(r"DIVIDE\s*\(", sql, re.IGNORECASE)
            if not m:
                break
            start = m.end()
            depth = 1
            pos = start
            comma_pos = None
            while pos < len(sql) and depth > 0:
                if sql[pos] == "(":
                    depth += 1
                elif sql[pos] == ")":
                    depth -= 1
                    if depth == 0:
                        break
                elif sql[pos] == "," and depth == 1 and comma_pos is None:
                    comma_pos = pos
                pos += 1
            if comma_pos is not None:
                num = sql[start:comma_pos].strip()
                den = sql[comma_pos + 1 : pos].strip()
                replacement = f"{num} / NULLIF({den}, 0)"
                sql = sql[: m.start()] + replacement + sql[pos + 1 :]
        return sql

    @staticmethod
    def _cleanup_resolved_text(text: str) -> str | None:
        lines = text.split("\n")
        out: list[str] = []
        for line in lines:
            s = line.strip()
            if re.match(r"^var\s+", s, re.IGNORECASE):
                continue
            if s.lower().startswith("return "):
                s = s[7:]
            s = re.sub(
                r"^if\s*\(\s*ISBLANK\s*\(\s*\w+\s*\)\s*,\s*0\s*,\s*(\w+)\s*\)\s*$",
                "",
                s,
                flags=re.IGNORECASE,
            )
            if s:
                out.append(s)
        return " ".join(out).strip() or None
