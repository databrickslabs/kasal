"""DAX Translator — pattern-based DAX→SQL translator with ordered registry."""
from __future__ import annotations

import re
from typing import Callable

from .constants import (
    RE_AVERAGEX_FILTER,
    RE_COUNTX_FILTER,
    RE_SIMPLE_SUM,
    RE_SIMPLE_SUMX,
    RE_SUMX_FILTER,
)
from .data_classes import TranslationResult
from .join_detector import _sanitize_alias
from .utils import to_snake_case


class DaxTranslator:
    """Pattern-based DAX→SQL translator with ordered registry."""

    def __init__(self, config: dict | None = None):
        cfg = config or {}
        self.filter_sets = cfg.get('filter_sets', {})
        self.column_overrides = cfg.get('column_overrides', {})
        self._fact_join_map_cfg = cfg.get('fact_join_map', {})
        self._measure_resolutions = cfg.get('measure_resolutions', {})
        self._dim_alias_map: dict[str, str] = cfg.get('dim_alias_map', {})
        self._cwc_filter_column: str = cfg.get('cwc_filter_column', '')
        self._fact_joins: list[dict] = []
        self._patterns: list[tuple[str, Callable, Callable]] = []
        self._register_patterns()

    def _register_patterns(self):
        """Register translation patterns in priority order. First match wins."""
        self._patterns = [
            ('quick_reject', self._match_quick_reject, self._translate_noop),
            ('sameperiodlastyear', self._match_sameperiodlastyear, self._translate_sameperiodlastyear),
            ('simple_sum', self._match_simple_sum, self._translate_simple_sum),
            ('simple_agg', self._match_simple_agg, self._translate_simple_agg),
            ('calculate_equality_filter', self._match_calculate_equality_filter, self._translate_calculate_equality_filter),
            ('simple_sumx', self._match_simple_sumx, self._translate_simple_sumx),
            ('calculate_sumx_vars_divide', self._match_calc_sumx_vars_divide, self._translate_calc_sumx_vars_divide),
            ('calculate_sumx_filter_inner', self._match_calculate_sumx_filter_inner, self._translate_sumx_parts),
            ('calculate_sumx_filter_outer', self._match_calculate_sumx_filter_outer, self._translate_sumx_parts),
            ('sumx_filter', self._match_sumx_filter, self._translate_sumx_parts),
            ('countx_filter', self._match_countx_filter, self._translate_countx),
            ('averagex_filter', self._match_averagex_filter, self._translate_averagex),
            ('userelationship', self._match_userelationship, self._translate_userelationship),
            ('calculate_measure_ref', self._match_calculate_measure_ref, self._translate_calculate_measure_ref),
            ('distinctcountnoblank', self._match_distinctcountnoblank, self._translate_distinctcountnoblank),
            # Deterministic scalar/date converters (field-eng parity). These run
            # BEFORE the generic divide/switch catch-alls and are in the trivial
            # fast-path so they're handled without an LLM call in llm_first mode.
            ('date_part', self._match_date_part, self._translate_date_part),
            ('datediff', self._match_datediff, self._translate_datediff),
            ('rankx', self._match_rankx, self._translate_rankx),
            ('edate_eomonth', self._match_edate_eomonth, self._translate_edate_eomonth),
            ('firstlastnonblank', self._match_firstlastnonblank, self._translate_firstlastnonblank),
            ('format_func', self._match_format_func, self._translate_format_func),
            ('divide_calculate_measure_ref', self._match_divide_calculate_measure_ref, self._translate_divide_calculate_measure_ref),
            ('divide', self._match_divide, self._translate_divide),
            ('selectedvalue_switch', self._match_selectedvalue_switch, self._translate_noop),
        ]

    def set_fact_joins(self, fact_joins: list[dict]):
        """Set current fact joins for cross-table resolution."""
        self._fact_joins = fact_joins

    # In llm_first mode, only these patterns run as the regex fast-path (Step 3.8):
    # display-artifact rejects + the trivial single-column, high-confidence
    # aggregations. Everything else falls through to the LLM-first translator.
    _TRIVIAL_FAST_PATH = frozenset(
        {'quick_reject', 'simple_sum', 'simple_agg', 'calculate_equality_filter',
         'simple_sumx', 'distinctcountnoblank',
         # Deterministic scalar/date converters — cheap, reproducible, no LLM.
         'date_part', 'datediff', 'rankx', 'edate_eomonth',
         'firstlastnonblank', 'format_func'}
    )

    def translate(self, measure: dict, table_key: str, trivial_only: bool = False) -> TranslationResult:
        """Translate a single DAX measure to SQL using the pattern registry.

        ``trivial_only`` (llm_first mode): run ONLY the trivial fast-path
        matchers (single-column aggregations + display-artifact rejects). Any
        non-trivial measure returns untranslatable so the caller routes it to the
        skill-corpus LLM translator. The remaining ~11 complex matchers are not
        deleted — they simply stop being the primary path.
        """
        name = measure.get('measure_name', '')
        dax = measure.get('dax_expression', '')
        original_name = measure.get('original_name', name)
        snake = to_snake_case(original_name)

        for pattern_name, match_fn, translate_fn in self._patterns:
            if trivial_only and pattern_name not in self._TRIVIAL_FAST_PATH:
                continue
            match = match_fn(dax, name)
            if match is not None:
                sql, skip_reason = translate_fn(match, dax, table_key)
                window_spec = None
                if skip_reason == '__SAMEPERIODLASTYEAR__':
                    skip_reason = ''
                    window_spec = {
                        'order': 'fiscper',
                        'range': 'trailing 12 month',
                        'semiadditive': 'last',
                    }
                return TranslationResult(
                    measure_name=snake,
                    original_name=original_name,
                    sql_expr=sql,
                    is_translatable=sql is not None,
                    skip_reason=skip_reason,
                    dax_expression=dax,
                    confidence='high' if sql else 'none',
                    category='single_table' if sql else 'unassigned',
                    window_spec=window_spec,
                )

        return TranslationResult(
            measure_name=snake,
            original_name=original_name,
            sql_expr=None,
            is_translatable=False,
            skip_reason='No matching pattern',
            dax_expression=dax,
            confidence='none',
            category='unassigned',
        )

    def translate_expression(self, dax_expr: str, table_key: str) -> str | None:
        """Translate a sub-expression (used by DIVIDE for numerator/denominator)."""
        for pattern_name, match_fn, translate_fn in self._patterns:
            if pattern_name in ('quick_reject', 'selectedvalue_switch', 'divide'):
                continue
            match = match_fn(dax_expr, '')
            if match is not None:
                sql, _ = translate_fn(match, dax_expr, table_key)
                return sql
        # Simple table[col] reference
        m = re.match(r'\s*(\w+)\[(\w+)\]\s*$', dax_expr)
        if m:
            return f'SUM(source.{m.group(2)})'
        return None

    # ── Match functions ──────────────────────────────────────────────────

    # Pre-compiled regexes for business-logic detection in quick-reject
    _RE_AGG_FUNCS = re.compile(
        r'\b(SUM|SUMX|COUNT|COUNTX|AVERAGE|AVERAGEX|DIVIDE|MIN|MAX)\s*\(',
        re.IGNORECASE,
    )
    _RE_MEASURE_REFS = re.compile(r'(?<!\w)\[[^\]]+\]')  # [MeasureRef] not Table[col]

    def _match_quick_reject(self, dax: str, name: str) -> dict | None:
        dax_up = dax.upper()
        if 'FORMAT(' in dax_up:
            # FORMAT is now handled deterministically by the `format_func`
            # converter when its inner expression is translatable (a real
            # Table[column] or aggregate). Only reject as display-only when the
            # converter CAN'T translate it — e.g. FORMAT of a literal, a bare
            # table name, or a pure display function (TODAY()) — so we neither
            # waste an LLM call nor emit meaningless SQL.
            if self._match_format_func(dax, name) is not None:
                return None
            return {'reason': 'FORMAT function (display-only)'}
        if '_COLOR' in name.upper() or 'COLOR' == name.upper().split('_')[-1]:
            # Check if this "color" measure contains aggregation functions or
            # measure refs that indicate business logic, not just display formatting
            has_aggregation = bool(self._RE_AGG_FUNCS.search(dax))
            has_measure_refs = bool(self._RE_MEASURE_REFS.search(dax))
            if has_aggregation or has_measure_refs:
                # Contains business logic — don't reject, let other patterns try
                return None
            return {'reason': 'Color/conditional formatting measure (display-only)'}
        if 'ISBLANK' in dax_up and 'BLANK()' in dax_up and 'SUMX' not in dax_up and 'SUM(' not in dax_up:
            return {'reason': 'ISBLANK+BLANK guard (no aggregation)'}
        if dax.strip() in ('Not available', ''):
            return {'reason': 'DAX expression not available'}
        dax_no_comments = '\n'.join(l for l in dax.split('\n') if not l.strip().startswith('//'))
        if dax_no_comments.strip().upper() in ('BLANK()', 'BLANK'):
            return {'reason': 'BLANK() placeholder'}
        if re.search(r'\bISFILTERED\s*\(', dax, re.IGNORECASE):
            return {'reason': 'ISFILTERED (PBI-specific)'}
        if 'SELECTEDVALUE' in dax_up and 'SWITCH' in dax_up:
            return {'reason': 'SELECTEDVALUE+SWITCH (parameterized)'}
        if 'SELECTEDVALUE' in dax_up:
            return {'reason': 'SELECTEDVALUE (requires slicer context)'}
        return None

    def _match_simple_sum(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        m = RE_SIMPLE_SUM.fullmatch(cleaned.strip())
        if m:
            return {'table': m.group(1), 'column': m.group(2)}
        return None

    def _match_simple_sumx(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        if 'FILTER(' in cleaned.upper():
            return None
        m = RE_SIMPLE_SUMX.search(cleaned)
        if m:
            return {'table': m.group(1), 'column': m.group(3)}
        return None

    # Simple single-column aggregations: AVERAGE / COUNT / MIN / MAX /
    # DISTINCTCOUNT / COUNTA, plus COUNTROWS(table). Deterministic, no LLM.
    _SIMPLE_AGG_FUNCS = {
        'AVERAGE': 'AVG', 'COUNT': 'COUNT', 'COUNTA': 'COUNT',
        'MIN': 'MIN', 'MAX': 'MAX', 'DISTINCTCOUNT': 'COUNT_DISTINCT',
    }

    def _match_simple_agg(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax)).strip()
        # COUNTROWS(table) → COUNT(*)
        m = re.fullmatch(
            r'\s*(?:CALCULATE\s*\(\s*)?COUNTROWS\s*\(\s*(\w+)\s*\)\s*\)?\s*',
            cleaned, re.IGNORECASE,
        )
        if m:
            return {'func': 'COUNTROWS', 'column': None}
        # AVERAGE/COUNT/MIN/MAX/DISTINCTCOUNT(table[col])
        m = re.fullmatch(
            r'\s*(?:CALCULATE\s*\(\s*)?'
            r'(AVERAGE|COUNTA|COUNT|DISTINCTCOUNT|MIN|MAX)\s*'
            r'\(\s*(\w+)\[(\w+)\]\s*\)\s*\)?\s*',
            cleaned, re.IGNORECASE,
        )
        if m and m.group(1).upper() in self._SIMPLE_AGG_FUNCS:
            return {'func': m.group(1).upper(), 'column': m.group(3)}
        return None

    def _translate_simple_agg(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        func = match['func']
        if func == 'COUNTROWS':
            return 'COUNT(*)', ''
        spark = self._SIMPLE_AGG_FUNCS.get(func)
        col = match['column']
        if spark == 'COUNT_DISTINCT':
            return f'COUNT(DISTINCT source.{col})', ''
        return f'{spark}(source.{col})', ''

    # CALCULATE(SUM(t[col]), t[dim] = <value>) → conditional aggregation.
    # Single equality filter only; anything more complex falls through to LLM.
    def _match_calculate_equality_filter(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax)).strip()
        if 'CALCULATE' not in cleaned.upper() or 'FILTER(' in cleaned.upper():
            return None
        m = re.fullmatch(
            r'\s*CALCULATE\s*\(\s*'
            r'(SUM|AVERAGE|COUNT|MIN|MAX)\s*\(\s*(\w+)\[(\w+)\]\s*\)\s*,\s*'
            r'(\w+)\[(\w+)\]\s*=\s*("[^"]*"|\'[^\']*\'|-?\d+(?:\.\d+)?|TRUE\s*\(\s*\)|FALSE\s*\(\s*\))'
            r'\s*\)\s*',
            cleaned, re.IGNORECASE,
        )
        if not m:
            return None
        return {
            'agg': m.group(1).upper(), 'agg_col': m.group(3),
            'filter_col': m.group(5), 'value': m.group(6).strip(),
        }

    def _translate_calculate_equality_filter(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        spark = {'SUM': 'SUM', 'AVERAGE': 'AVG', 'COUNT': 'COUNT', 'MIN': 'MIN', 'MAX': 'MAX'}[match['agg']]
        val = match['value']
        up = val.upper().replace(' ', '')
        if up in ('TRUE()', 'FALSE()'):
            val = 'TRUE' if up == 'TRUE()' else 'FALSE'
        elif val.startswith("'"):
            val = '"' + val[1:-1] + '"'
        cond = f'source.{match["filter_col"]} = {val}'
        return f'{spark}(CASE WHEN {cond} THEN source.{match["agg_col"]} END)', ''

    def _match_calc_sumx_vars_divide(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        ret_match = re.search(r'(?:return\s+)?DIVIDE\s*\(', cleaned, re.IGNORECASE)
        if not ret_match:
            return None
        parts = self._extract_calc_sumx_assignments(cleaned)
        if not parts:
            return None
        divide_text = cleaned[ret_match.start():]
        divide_text = re.sub(r'^return\s+', '', divide_text, flags=re.IGNORECASE).strip()
        return {'parts': parts, 'divide_text': divide_text}

    def _match_calculate_sumx_filter_inner(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        if 'DIVIDE(' in cleaned.upper():
            return None
        parts = self._extract_calc_sumx_assignments(cleaned)
        if not parts:
            m = re.search(
                r'CALCULATE\s*\(\s*SUMX\s*\(\s*FILTER\s*\(\s*(\w+)\s*,\s*(.*?)\s*\)\s*,\s*(\w+)\[(\w+)\]\s*\)\s*\)',
                cleaned, re.IGNORECASE | re.DOTALL,
            )
            if m:
                parts = [{'var': None, 'table': m.group(1), 'column': m.group(4),
                           'condition': m.group(2), 'filter_table': m.group(1)}]
            else:
                return None
        ret_match = re.search(r'return\s+(.+?)(?:\s*//.*)?$', cleaned, re.IGNORECASE | re.DOTALL)
        ret_expr = ret_match.group(1).strip() if ret_match else (parts[0]['var'] if len(parts) == 1 else None)
        return {'parts': parts, 'return_expr': ret_expr}

    def _match_calculate_sumx_filter_outer(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        if 'DIVIDE(' in cleaned.upper():
            return None
        parts = self._extract_calc_sumx_assignments(cleaned)
        if not parts:
            return None
        ret_match = re.search(r'return\s+(.+?)(?:\s*//.*)?$', cleaned, re.IGNORECASE | re.DOTALL)
        ret_expr = ret_match.group(1).strip() if ret_match else (parts[0]['var'] if len(parts) == 1 else None)
        return {'parts': parts, 'return_expr': ret_expr}

    def _match_sumx_filter(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        m = RE_SUMX_FILTER.search(cleaned)
        if m:
            return {'parts': [{'var': None, 'table': m.group(1), 'column': m.group(4),
                               'condition': m.group(2), 'filter_table': m.group(1)}],
                    'return_expr': None}
        return None

    def _match_countx_filter(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        m = RE_COUNTX_FILTER.search(cleaned)
        if m:
            return {'table': m.group(1), 'column': m.group(4),
                    'condition': m.group(2), 'filter_table': m.group(1)}
        return None

    def _match_averagex_filter(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        m = RE_AVERAGEX_FILTER.search(cleaned)
        if m:
            return {'table': m.group(1), 'column': m.group(4),
                    'condition': m.group(2), 'filter_table': m.group(1)}
        return None

    def _match_userelationship(self, dax: str, name: str) -> dict | None:
        if 'USERELATIONSHIP' not in dax.upper():
            return None
        m = re.search(
            r'CALCULATE\s*\(\s*(.+?)\s*,\s*USERELATIONSHIP\s*\(\s*(\w+)\[(\w+)\]\s*,\s*(\w+)\[(\w+)\]\s*\)\s*\)',
            dax, re.IGNORECASE | re.DOTALL,
        )
        if m:
            return {
                'inner_expr': m.group(1),
                'fact_table': m.group(2),
                'fact_col': m.group(3),
                'dim_table': m.group(4),
                'dim_col': m.group(5),
            }
        return None

    def _match_distinctcountnoblank(self, dax: str, name: str) -> dict | None:
        if 'DISTINCTCOUNTNOBLANK' not in dax.upper():
            return None
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        up = cleaned.upper()
        if 'DIVIDE(' in up or 'FILTER(' in up or 'VAR ' in up:
            return None
        m = re.fullmatch(
            r'\s*(?:CALCULATE\s*\(\s*)?DISTINCTCOUNTNOBLANK\s*\(\s*(\w+)\[(\w+)\]\s*\)\s*\)?\s*',
            cleaned, re.IGNORECASE,
        )
        if m:
            return {'table': m.group(1), 'column': m.group(2)}
        return None

    def _match_calculate_measure_ref(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        if 'DIVIDE(' in cleaned.upper() or 'DIVIDE (' in cleaned.upper():
            return None
        refs = self._find_calculate_measure_refs(cleaned)
        if not refs:
            return None
        return {'cleaned': cleaned, 'refs': refs}

    def _match_divide_calculate_measure_ref(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        up = cleaned.upper()
        if 'DIVIDE(' not in up and 'DIVIDE (' not in up:
            return None
        refs = self._find_calculate_measure_refs(cleaned)
        if not refs:
            return None
        return {'cleaned': cleaned, 'refs': refs}

    def _match_divide(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_var_block(dax)
        cleaned = self._strip_return(cleaned)
        if 'DIVIDE(' in cleaned.upper():
            return {'raw': cleaned}
        return None

    def _match_sameperiodlastyear(self, dax: str, name: str) -> dict | None:
        if 'SAMEPERIODLASTYEAR' not in dax.upper():
            return None
        clean_lines = []
        for line in dax.split('\n'):
            s = line.strip()
            if s.startswith('//'):
                continue
            if re.match(r'^var\s+(std|etd)\b', s, re.IGNORECASE):
                continue
            if re.match(r'^var\s+\w+\s*=\s*CALCULATE\s*\(\s*\[(PY_Start_date|PY_End_date|F_Start_date|F_End_date)\]',
                        s, re.IGNORECASE):
                continue
            clean_lines.append(s)
        cleaned = '\n'.join(clean_lines)
        m = re.search(
            r'CALCULATE\s*\(\s*SUMX\s*\(\s*FILTER\s*\(\s*(\w+)\s*,\s*(.*?)\s*\)\s*,\s*(\w+)\[(\w+)\]\s*\)\s*'
            r',\s*SAMEPERIODLASTYEAR\s*\([^)]+\)\s*\)',
            cleaned, re.IGNORECASE | re.DOTALL,
        )
        if m:
            return {
                'type': 'sumx_filter',
                'table': m.group(1), 'condition': m.group(2),
                'filter_table': m.group(1), 'column': m.group(4),
            }
        return {'reason': 'SAMEPERIODLASTYEAR (prior-year, requires window function)'}

    def _match_selectedvalue_switch(self, dax: str, name: str) -> dict | None:
        dax_up = dax.upper()
        if 'SELECTEDVALUE' in dax_up and 'SWITCH' in dax_up:
            return {'reason': 'SELECTEDVALUE+SWITCH (parameterized)'}
        if 'SELECTEDVALUE' in dax_up:
            return {'reason': 'SELECTEDVALUE (requires slicer context)'}
        return None

    # ── Translate functions ──────────────────────────────────────────────

    def _translate_noop(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        return None, match.get('reason', 'Not translatable')

    def _translate_sameperiodlastyear(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        if match.get('type') != 'sumx_filter':
            return None, match.get('reason', 'SAMEPERIODLASTYEAR (requires window)')
        col = match['column']
        cond_sql = self._dax_condition_to_sql(match.get('condition', ''), table_key)
        if cond_sql:
            sql = f"SUM(source.{col}) FILTER (WHERE {cond_sql})"
        else:
            sql = f"SUM(source.{col})"
        return sql, '__SAMEPERIODLASTYEAR__'

    def _translate_simple_sum(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        col = match['column']
        return f'SUM(source.{col})', ''

    def _translate_simple_sumx(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        col = match['column']
        return f'SUM(source.{col})', ''

    def _translate_countx(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        col = match['column']
        cond_sql = self._dax_condition_to_sql(match.get('condition', ''), table_key)
        if cond_sql:
            return f"COUNT(source.{col}) FILTER (WHERE {cond_sql})", ''
        return f"COUNT(source.{col})", ''

    def _translate_averagex(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        col = match['column']
        cond_sql = self._dax_condition_to_sql(match.get('condition', ''), table_key)
        if cond_sql:
            return f"AVG(source.{col}) FILTER (WHERE {cond_sql})", ''
        return f"AVG(source.{col})", ''

    def _translate_userelationship(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        """CALCULATE(expr, USERELATIONSHIP(T[col], D[col])) -> translate expr with alternate join alias."""
        inner = match['inner_expr']
        # Try translating the inner expression
        inner_result = self.translate_expression(inner, table_key)
        if inner_result:
            # Replace dim table references with the alternate alias
            dim_table = match['dim_table']
            fact_col = match['fact_col']
            alt_alias = f"{dim_table.lower()}_{fact_col.lower()}"
            # Rewrite source.col or dim.col refs if the inner expr uses the alternate dim
            return inner_result, ''
        return None, 'USERELATIONSHIP inner expression not translatable'

    def _translate_distinctcountnoblank(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        col = match['column']
        return f'COUNT(DISTINCT source.{col})', ''

    # ── Deterministic scalar/date converters (field-eng parity) ───────────
    #
    # These translate cheap, high-frequency DAX scalar/date functions to Spark
    # SQL WITHOUT an LLM call. They match a single top-level function wrapping a
    # ``Table[Column]`` reference (the common shape); anything more complex
    # returns None so it falls through to the LLM. Column refs emit as
    # ``source.<col>`` to match the rest of the translator.

    # DAX date-part function → Spark SQL function
    _DATE_PART_FUNCS = {
        'YEAR': 'year', 'MONTH': 'month', 'DAY': 'day', 'QUARTER': 'quarter',
        'HOUR': 'hour', 'MINUTE': 'minute', 'SECOND': 'second',
        'WEEKNUM': 'weekofyear', 'WEEKDAY': 'dayofweek',
    }

    def _match_date_part(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax))
        m = re.fullmatch(
            r'\s*(YEAR|MONTH|DAY|QUARTER|HOUR|MINUTE|SECOND|WEEKNUM|WEEKDAY)\s*\(\s*'
            r'(\w+)\[(\w+)\]\s*(?:,\s*\d+\s*)?\)\s*',
            cleaned, re.IGNORECASE,
        )
        if not m:
            return None
        return {'func': m.group(1).upper(), 'column': m.group(3)}

    def _translate_date_part(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        spark = self._DATE_PART_FUNCS.get(match['func'])
        if not spark:
            return None, f"Unsupported date part {match['func']}"
        return f"{spark}(source.{match['column']})", ''

    # DATEDIFF(<start>, <end>, <interval>) → Spark SQL per interval.
    def _match_datediff(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax))
        if 'DATEDIFF' not in cleaned.upper():
            return None
        m = re.fullmatch(
            r'\s*DATEDIFF\s*\(\s*(\w+)\[(\w+)\]\s*,\s*(\w+)\[(\w+)\]\s*,\s*'
            r'(DAY|WEEK|MONTH|QUARTER|YEAR|HOUR|MINUTE|SECOND)\s*\)\s*',
            cleaned, re.IGNORECASE,
        )
        if not m:
            return None
        return {'a': m.group(2), 'b': m.group(4), 'interval': m.group(5).upper()}

    def _translate_datediff(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        a, b, iv = f"source.{match['a']}", f"source.{match['b']}", match['interval']
        if iv == 'DAY':
            return f"datediff({b}, {a})", ''
        if iv == 'WEEK':
            return f"floor(datediff({b}, {a}) / 7)", ''
        if iv == 'MONTH':
            return f"floor(months_between({b}, {a}))", ''
        if iv == 'QUARTER':
            return f"floor(months_between({b}, {a}) / 3)", ''
        if iv == 'YEAR':
            return f"floor(months_between({b}, {a}) / 12)", ''
        # Sub-day intervals via unix-second difference.
        divisor = {'HOUR': 3600, 'MINUTE': 60, 'SECOND': 1}[iv]
        inner = f"(unix_timestamp({b}) - unix_timestamp({a}))"
        return (inner if divisor == 1 else f"floor({inner} / {divisor})"), ''

    def _resolve_scalar_inner(self, expr: str) -> str | None:
        """Resolve an inner expression for SCALAR context (FORMAT/RANKX arg).

        A bare ``Table[col]`` maps to ``source.<col>`` WITHOUT a SUM wrapper —
        unlike ``translate_expression`` (which SUM-wraps bare columns for its
        DIVIDE-numerator use). An aggregate/other expr is delegated as-is.
        """
        expr = expr.strip()
        cm = re.fullmatch(r'\s*(\w+)\[(\w+)\]\s*', expr)
        if cm:
            return f"source.{cm.group(2)}"
        # DIVIDE is a valid scalar inner but translate_expression skips it — use
        # the divide converter directly.
        if re.match(r'\s*DIVIDE\s*\(', expr, re.IGNORECASE):
            dm = self._match_divide(expr, '')
            if dm is not None:
                sql, _ = self._translate_divide(dm, expr, '')
                if sql:
                    return sql
        return self.translate_expression(expr, '')

    # RANKX(<table>, <expr>, , <order>, <ties>) → rank()/dense_rank() OVER.
    def _match_rankx(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax))
        if 'RANKX' not in cleaned.upper():
            return None
        # RANKX(All/Table, <expr>, [<value>], [ASC|DESC], [Dense|Skip])
        m = re.fullmatch(
            r'\s*RANKX\s*\(\s*[^,]+,\s*(.+?)\s*'
            r'(?:,\s*[^,]*)??'                       # optional value arg
            r'(?:,\s*(ASC|DESC))?'                   # optional order
            r'(?:,\s*(DENSE|SKIP))?\s*\)\s*',
            cleaned, re.IGNORECASE | re.DOTALL,
        )
        if not m:
            return None
        expr = m.group(1).strip()
        # Only handle a simple aggregate/column expr deterministically.
        inner = self._resolve_scalar_inner(expr)
        if not inner:
            return None
        order = (m.group(2) or 'DESC').upper()
        ties = (m.group(3) or 'SKIP').upper()
        return {'inner': inner, 'order': order, 'dense': ties == 'DENSE'}

    def _translate_rankx(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        fn = 'dense_rank' if match['dense'] else 'rank'
        return f"{fn}() OVER (ORDER BY {match['inner']} {match['order']})", ''

    # EDATE(<date>, <n>) → add_months ; EOMONTH(<date>, <n>) → last_day(add_months)
    def _match_edate_eomonth(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax))
        m = re.fullmatch(
            r'\s*(EDATE|EOMONTH)\s*\(\s*(\w+)\[(\w+)\]\s*,\s*(-?\d+)\s*\)\s*',
            cleaned, re.IGNORECASE,
        )
        if not m:
            return None
        return {'func': m.group(1).upper(), 'column': m.group(3), 'months': int(m.group(4))}

    def _translate_edate_eomonth(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        base = f"add_months(source.{match['column']}, {match['months']})"
        if match['func'] == 'EDATE':
            return base, ''
        return f"last_day({base})", ''

    # FIRSTNONBLANK / LASTNONBLANK / LASTNONBLANKVALUE → MIN/MAX simplification.
    def _match_firstlastnonblank(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax))
        m = re.fullmatch(
            r'\s*(FIRSTNONBLANK|LASTNONBLANK|LASTNONBLANKVALUE)\s*\(\s*'
            r'(\w+)\[(\w+)\]\s*(?:,\s*[^)]+)?\)\s*',
            cleaned, re.IGNORECASE,
        )
        if not m:
            return None
        return {'func': m.group(1).upper(), 'column': m.group(3)}

    def _translate_firstlastnonblank(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        agg = 'MIN' if match['func'] == 'FIRSTNONBLANK' else 'MAX'
        return f"{agg}(source.{match['column']})", ''

    # FORMAT(<expr>, "<format string>") → date_format / format_number.
    # Format-string → (kind, spark_arg) lookup. kind: 'date' | 'number' | 'raw'.
    _FORMAT_MAP = {
        # Date formats (DAX format string → Spark date_format pattern)
        'MMM YYYY': ('date', 'MMM yyyy'), 'MMM-YYYY': ('date', 'MMM-yyyy'),
        'MMMM YYYY': ('date', 'MMMM yyyy'), 'MMM': ('date', 'MMM'),
        'MMMM': ('date', 'MMMM'), 'YYYY-MM-DD': ('date', 'yyyy-MM-dd'),
        'DD/MM/YYYY': ('date', 'dd/MM/yyyy'), 'MM/DD/YYYY': ('date', 'MM/dd/yyyy'),
        'YYYY': ('date', 'yyyy'), 'YYYY-MM': ('date', 'yyyy-MM'),
        'DDDD': ('date', 'EEEE'), 'DDD': ('date', 'EEE'),
        # Number formats (DAX format string → Spark format_number decimal places)
        '#,##0': ('number', 0), '#,##0.0': ('number', 1), '#,##0.00': ('number', 2),
        '0': ('number', 0), '0.0': ('number', 1), '0.00': ('number', 2),
    }

    def _match_format_func(self, dax: str, name: str) -> dict | None:
        cleaned = self._strip_return(self._strip_var_block(dax))
        if not re.match(r'\s*FORMAT\s*\(', cleaned, re.IGNORECASE):
            return None
        # FORMAT(<expr>, "<fmt>") — expr is a Table[col] or a simple aggregate.
        m = re.fullmatch(
            r'\s*FORMAT\s*\(\s*(.+?)\s*,\s*"([^"]*)"\s*\)\s*',
            cleaned, re.IGNORECASE | re.DOTALL,
        )
        if not m:
            return None
        expr, fmt = m.group(1).strip(), m.group(2).strip()
        # Resolve the inner expr for scalar context (bare Table[col] → source.col,
        # NOT SUM-wrapped; aggregates delegated as-is).
        inner = self._resolve_scalar_inner(expr)
        if not inner:
            return None
        return {'inner': inner, 'fmt': fmt}

    def _translate_format_func(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        inner, fmt = match['inner'], match['fmt']
        spec = self._FORMAT_MAP.get(fmt) or self._FORMAT_MAP.get(fmt.upper())
        if not spec:
            # Percent formats (contain %) → multiply *100 and format_number.
            if '%' in fmt:
                decimals = fmt.count('0', fmt.find('.')) if '.' in fmt else 0
                return f"CONCAT(format_number({inner} * 100, {decimals}), '%')", ''
            # Currency (leading $/€/£) → format_number with 2 decimals + symbol.
            cur = re.match(r'\s*([$€£])', fmt)
            if cur:
                return f"CONCAT('{cur.group(1)}', format_number({inner}, 2))", ''
            # Unknown format code: keep the value unformatted rather than lose the
            # measure (display formatting isn't essential in a UC metric view).
            return inner, ''
        kind, arg = spec
        if kind == 'date':
            return f"date_format({inner}, '{arg}')", ''
        if kind == 'number':
            return f"format_number({inner}, {arg})", ''
        return inner, ''

    def _translate_calculate_measure_ref(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        refs = match['refs']
        if len(refs) == 1:
            r = refs[0]
            extra = self._parse_calculate_filters(r['filter_text'], table_key)
            sql = self._resolve_measure_ref(r['ref_name'], extra)
            return (sql, '') if sql else (None, f'Cannot resolve [{r["ref_name"]}]')
        result = match['cleaned']
        for r in reversed(refs):
            extra = self._parse_calculate_filters(r['filter_text'], table_key)
            sql = self._resolve_measure_ref(r['ref_name'], extra)
            if not sql:
                return None, f'Cannot resolve [{r["ref_name"]}]'
            result = result[:r['start']] + f'({sql})' + result[r['end']:]
        result = self._cleanup_resolved_text(result)
        return (result, '') if result else (None, 'Could not finalize expression')

    def _translate_divide_calculate_measure_ref(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        cleaned = match['cleaned']
        refs = match['refs']
        result = cleaned
        for r in reversed(refs):
            extra = self._parse_calculate_filters(r['filter_text'], table_key)
            sql = self._resolve_measure_ref(r['ref_name'], extra)
            if not sql:
                return None, f'Cannot resolve [{r["ref_name"]}]'
            result = result[:r['start']] + f'({sql})' + result[r['end']:]
        var_map: dict[str, str] = {}
        expr_lines: list[str] = []
        for line in result.split('\n'):
            s = line.strip()
            vm = re.match(r'var\s+(\w+)\s*=\s*(.+)', s, re.IGNORECASE)
            if vm and vm.group(2).strip():
                var_map[vm.group(1)] = vm.group(2).strip()
                continue
            if s.lower().startswith('return '):
                s = s[7:]
            if s:
                expr_lines.append(s)
        expr = ' '.join(expr_lines).strip()
        for vn in sorted(var_map.keys(), key=len, reverse=True):
            expr = re.sub(rf'\b{re.escape(vn)}\b', var_map[vn], expr)
        expr = self._strip_bare_calculate(expr)
        inner = self._extract_divide_args(expr)
        if not inner:
            return None, 'Could not extract DIVIDE args after resolution'
        num_sql, den_sql = inner
        num_s = num_sql.strip()
        num_w = f'({num_s})' if '+' in num_s or '-' in num_s else num_s
        final = f'{num_w} / NULLIF({den_sql.strip()}, 0)'
        return self._resolve_remaining_dax(final), ''

    def _translate_sumx_parts(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        parts = match['parts']
        return_expr = match.get('return_expr')
        if len(parts) == 1 and (return_expr is None or return_expr == parts[0]['var'] or parts[0]['var'] is None):
            p = parts[0]
            alias, col_map = self._resolve_table_alias(p['table'], table_key)
            physical_col = col_map.get(p['column'], p['column'])
            filter_parts = []
            filter_sql = self._build_filter_clause(p, table_key, dax)
            if filter_sql:
                filter_parts.append(filter_sql)
            elif p.get('condition'):
                # Filter condition exists in DAX but could not be translated
                return None, f"Untranslatable FILTER condition: {p['condition']}"
            self._extend_with_implicit_filters(filter_parts, alias)
            combined = ' AND '.join(filter_parts) if filter_parts else None
            if combined:
                return f"SUM({alias}.{physical_col}) FILTER (WHERE {combined})", ''
            return f"SUM({alias}.{physical_col})", ''
        var_map = {}
        for p in parts:
            var_name = p['var']
            if var_name is None:
                continue
            alias, col_map = self._resolve_table_alias(p['table'], table_key)
            physical_col = col_map.get(p['column'], p['column'])
            filter_parts = []
            filter_sql = self._build_filter_clause(p, table_key, dax)
            if filter_sql:
                filter_parts.append(filter_sql)
            elif p.get('condition'):
                return None, f"Untranslatable FILTER condition: {p['condition']}"
            self._extend_with_implicit_filters(filter_parts, alias)
            combined = ' AND '.join(filter_parts) if filter_parts else None
            if combined:
                var_map[var_name] = f"SUM({alias}.{physical_col}) FILTER (WHERE {combined})"
            else:
                var_map[var_name] = f"SUM({alias}.{physical_col})"
        if return_expr and var_map:
            sql = return_expr
            for var_name in sorted(var_map.keys(), key=len, reverse=True):
                sql = re.sub(rf'\b{re.escape(var_name)}\b', f'({var_map[var_name]})', sql)
            return sql.strip(), ''
        return None, 'Complex multi-part SUMX expression'

    def _translate_calc_sumx_vars_divide(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        parts = match['parts']
        divide_text = match['divide_text']
        var_map: dict[str, str] = {}
        for p in parts:
            alias, col_map = self._resolve_table_alias(p['table'], table_key)
            physical_col = col_map.get(p['column'], p['column'])
            filter_parts = []
            inner_filter = self._build_filter_clause(p, table_key, dax)
            if inner_filter:
                filter_parts.append(inner_filter)
            elif p.get('condition'):
                return None, f"Untranslatable FILTER condition: {p['condition']}"
            outer_cond = p.get('outer_filter_condition')
            outer_table = p.get('outer_filter_table')
            if outer_cond and outer_table:
                outer_filter = self._build_filter_clause(
                    {'condition': outer_cond, 'filter_table': outer_table}, table_key, dax)
                if outer_filter:
                    filter_parts.append(outer_filter)
                elif outer_cond:
                    return None, f"Untranslatable outer FILTER condition: {outer_cond}"
            self._extend_with_implicit_filters(filter_parts, alias)
            combined_filter = ' AND '.join(filter_parts) if filter_parts else None
            if combined_filter:
                var_map[p['var']] = f"SUM({alias}.{physical_col}) FILTER (WHERE {combined_filter})"
            else:
                var_map[p['var']] = f"SUM({alias}.{physical_col})"
        inner = self._extract_divide_args(divide_text)
        if inner is None:
            return None, 'Could not parse DIVIDE arguments'
        num_expr, den_expr = inner
        num_sql = self._substitute_vars(num_expr.strip(), var_map)
        den_sql = self._substitute_vars(den_expr.strip(), var_map)
        if num_sql and den_sql:
            num_wrapped = f'({num_sql})' if '+' in num_sql or '-' in num_sql else num_sql
            final = f'{num_wrapped} / NULLIF({den_sql}, 0)'
            return self._resolve_remaining_dax(final), ''
        return None, 'DIVIDE references variables not found'

    def _translate_divide(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        raw = match['raw']
        inner = self._extract_divide_args(raw)
        if inner is None:
            return None, 'Could not parse DIVIDE arguments'
        num_dax, den_dax = inner
        num_result = self.translate_expression(num_dax.strip(), table_key)
        den_result = self.translate_expression(den_dax.strip(), table_key)
        if num_result and den_result:
            return f'{num_result} / NULLIF({den_result}, 0)', ''
        return None, f'DIVIDE sub-expression not translatable'

    # ── Helpers ──────────────────────────────────────────────────────────

    def _extract_calc_sumx_assignments(self, cleaned: str) -> list[dict] | None:
        parts = []
        for m in re.finditer(
            r'(?:var\s+)?(\w+)\s*=\s*CALCULATE\s*\(\s*SUMX\s*\(\s*FILTER\s*\('
            r'\s*(\w+)\s*,\s*(.*?)\s*\)\s*,\s*(\w+)\[(\w+)\]\s*\)'
            r'(?:\s*,\s*FILTER\s*\(\s*(\w+)\s*,\s*(.*?)\s*\)\s*)?\)',
            cleaned, re.IGNORECASE | re.DOTALL,
        ):
            part = {
                'var': m.group(1), 'table': m.group(2),
                'column': m.group(5), 'condition': m.group(3),
                'filter_table': m.group(2),
            }
            if m.group(6) and m.group(7):
                part['outer_filter_table'] = m.group(6)
                part['outer_filter_condition'] = m.group(7)
            parts.append(part)
        return parts or None

    def _resolve_table_alias(self, table_ref: str, table_key: str) -> tuple[str, dict]:
        """Resolve DAX table reference to SQL alias and column map."""
        if table_ref == table_key or table_ref not in self._fact_join_map_cfg:
            return 'source', {}
        fj = self._fact_join_map_cfg[table_ref]
        alias = _sanitize_alias(fj.get('alias', table_ref.lower()))
        col_map = fj.get('column_map', {})
        return alias, col_map

    def _build_filter_clause(self, part: dict, table_key: str, dax: str) -> str:
        """Build SQL FILTER clause from a parsed SUMX part.

        Handles variable references, CWC_Filter=1 patterns, cross-table facts,
        qualified dim refs, and generic conditions.
        """
        condition = part.get('condition', '')
        filter_table = part.get('filter_table', '')
        if not condition:
            return ''

        # Check for variable reference: Table[col] in varName
        var_ref_match = re.search(r'(\w+)\[(\w+)\]\s+in\s+(\w+)\s*$', condition, re.IGNORECASE)
        if var_ref_match:
            dim_table = var_ref_match.group(1)
            dim_col = var_ref_match.group(2)
            var_name = var_ref_match.group(3)
            values = self._resolve_var_filter_set(var_name, dax)
            if values:
                alias, phys_col = self._resolve_filter_alias(dim_table, dim_col)
                in_list = ', '.join(f"'{v}'" for v in values)
                return f"{alias}.{phys_col} IN ({in_list})"

        # Check for CWC_Filter=1 pattern (only if CWC_FILTER filter set is configured)
        if self.filter_sets.get('CWC_FILTER'):
            cwc_match = re.search(r'(\w+)\[CWC_Filter\]\s*=\s*1', condition, re.IGNORECASE)
            if cwc_match:
                dim_table = cwc_match.group(1)
                alias = self._dim_table_to_alias(dim_table)
                cwc_values = self.filter_sets['CWC_FILTER']
                in_list = ', '.join(f"'{v}'" for v in cwc_values)
                return f"{alias}.{self._cwc_filter_column} IN ({in_list})"

        # Cross-table fact filter
        if filter_table and filter_table in self._fact_join_map_cfg:
            return self._dax_condition_to_sql_cross_table(condition, filter_table)

        # Qualified dim reference (different table than the fact)
        if filter_table and filter_table.lower() != table_key.lower():
            return self._dax_condition_to_sql_qualified(condition, filter_table, table_key)

        return self._dax_condition_to_sql(condition, table_key)

    def _resolve_var_filter_set(self, var_name: str, dax: str) -> list[str] | None:
        """Resolve a variable reference to a list of filter values."""
        m = re.search(
            rf'var\s+{re.escape(var_name)}\s*=\s*\{{([^}}]+)\}}',
            dax, re.IGNORECASE,
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
                if fj.get('_pbi_name') == dax_table:
                    cfg = fj['_fact_join_config']
                    fc = cfg.get('filter_columns', {})
                    if dax_col in fc:
                        return fj['name'], fc[dax_col]
        return self._dim_table_to_alias(dax_table), dax_col

    def _dax_condition_to_sql(self, condition: str, table_key: str) -> str:
        """Translate a DAX filter condition to SQL with proper formatting."""
        if not condition or not condition.strip():
            return ''
        condition = condition.strip()
        parts = self._split_conditions(condition)
        sql_parts = []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            sql_part = self._translate_single_condition(part, table_key)
            if sql_part:
                sql_parts.append(sql_part)
        return ' AND '.join(sql_parts) if sql_parts else ''

    def _dax_condition_to_sql_cross_table(self, condition: str, filter_table: str) -> str:
        """Translate DAX conditions that reference a cross-table fact's columns."""
        parts = self._split_conditions(condition)
        sql_parts = []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            m = re.match(r'(\w+)\[(\w+)\]\s*(=|<>|<|>|<=|>=)\s*"([^"]*)"', part)
            if m:
                alias, phys_col = self._resolve_filter_alias(m.group(1), m.group(2))
                sql_parts.append(f"{alias}.{phys_col} {m.group(3)} '{m.group(4)}'")
                continue
            m = re.match(r'(\w+)\[(\w+)\]\s+in\s+\{([^}]+)\}', part, re.IGNORECASE)
            if m:
                alias, phys_col = self._resolve_filter_alias(m.group(1), m.group(2))
                vals = re.findall(r'"([^"]*)"', m.group(3))
                in_list = ', '.join(f"'{v}'" for v in vals)
                sql_parts.append(f"{alias}.{phys_col} IN ({in_list})")
                continue
            m = re.match(r'(\w+)\[(\w+)\]\s*(=|<>|<|>|<=|>=)\s*(\d+(?:\.\d+)?)', part)
            if m:
                alias, phys_col = self._resolve_filter_alias(m.group(1), m.group(2))
                sql_parts.append(f"{alias}.{phys_col} {m.group(3)} {m.group(4)}")
                continue
        return ' AND '.join(sql_parts) if sql_parts else ''

    def _dax_condition_to_sql_qualified(self, condition: str, filter_table: str, table_key: str) -> str:
        """Translate DAX conditions with qualified dim table references."""
        parts = self._split_conditions(condition)
        sql_parts = []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            m = re.match(r'(\w+)\[(\w+)\]\s*(=|<>|<|>|<=|>=)\s*"([^"]*)"', part)
            if m:
                alias = self._dim_table_to_alias(m.group(1))
                sql_parts.append(f"{alias}.{m.group(2)} {m.group(3)} '{m.group(4)}'")
                continue
            m = re.match(r'(\w+)\[(\w+)\]\s+in\s+\{([^}]+)\}', part, re.IGNORECASE)
            if m:
                alias = self._dim_table_to_alias(m.group(1))
                vals = re.findall(r'"([^"]*)"', m.group(3))
                in_list = ', '.join(f"'{v}'" for v in vals)
                sql_parts.append(f"{alias}.{m.group(2)} IN ({in_list})")
                continue
            m = re.match(r'(\w+)\[(\w+)\]\s*(=|<>|<|>|<=|>=)\s*(\d+(?:\.\d+)?)', part)
            if m:
                alias = self._dim_table_to_alias(m.group(1))
                sql_parts.append(f"{alias}.{m.group(2)} {m.group(3)} {m.group(4)}")
                continue
        return ' AND '.join(sql_parts) if sql_parts else ''

    @staticmethod
    def _split_conditions(condition: str) -> list[str]:
        """Split DAX condition on && (AND)."""
        return re.split(r'\s*&&\s*', condition)

    def _translate_single_condition(self, cond: str, table_key: str) -> str | None:
        """Translate a single DAX condition to SQL."""
        cond = cond.strip()

        # Handle OR conditions (|| → OR): split, translate each side, join with OR
        if '||' in cond:
            or_parts = re.split(r'\s*\|\|\s*', cond)
            sql_or_parts = []
            for op in or_parts:
                sql_op = self._translate_single_condition(op.strip(), table_key)
                if sql_op is None:
                    return None
                sql_or_parts.append(sql_op)
            return '(' + ' OR '.join(sql_or_parts) + ')'

        # NOT Table[col] IN {…}
        m = re.match(r'NOT\s+(\w+)\[(\w+)\]\s+in\s+\{([^}]+)\}', cond, re.IGNORECASE)
        if m:
            col = self._resolve_column(m.group(1), m.group(2), table_key)
            vals = re.findall(r'"([^"]*)"', m.group(3))
            in_list = ', '.join(f"'{v}'" for v in vals)
            return f"source.{col} NOT IN ({in_list})"

        # Table[col] = "val"
        m = re.match(r'(\w+)\[(\w+)\]\s*(=|<>|<|>|<=|>=)\s*"([^"]*)"', cond)
        if m:
            col = self._resolve_column(m.group(1), m.group(2), table_key)
            return f"source.{col} {m.group(3)} '{m.group(4)}'"
        # Table[col] = 'val' (single-quoted)
        m = re.match(r"(\w+)\[(\w+)\]\s*(=|<>|<|>|<=|>=)\s*'([^']*)'", cond)
        if m:
            col = self._resolve_column(m.group(1), m.group(2), table_key)
            return f"source.{col} {m.group(3)} '{m.group(4)}'"
        # Table[col] in {"a","b","c"}
        m = re.match(r'(\w+)\[(\w+)\]\s+in\s+\{([^}]+)\}', cond, re.IGNORECASE)
        if m:
            col = self._resolve_column(m.group(1), m.group(2), table_key)
            vals = re.findall(r'"([^"]*)"', m.group(3))
            if not vals:
                vals = re.findall(r"'([^']*)'", m.group(3))
            in_list = ', '.join(f"'{v}'" for v in vals)
            return f"source.{col} IN ({in_list})"
        # Table[col] = 123
        m = re.match(r'(\w+)\[(\w+)\]\s*(=|<>|<|>|<=|>=)\s*(\d+(?:\.\d+)?)', cond)
        if m:
            col = self._resolve_column(m.group(1), m.group(2), table_key)
            return f"source.{col} {m.group(3)} {m.group(4)}"

        # ── Bare column patterns (no Table prefix): [col] = "val" ──
        m = re.match(r'\[(\w+)\]\s*(=|<>|<|>|<=|>=)\s*"([^"]*)"', cond)
        if m:
            return f"source.{m.group(1)} {m.group(2)} '{m.group(3)}'"
        # [col] = 'val' (single-quoted)
        m = re.match(r"\[(\w+)\]\s*(=|<>|<|>|<=|>=)\s*'([^']*)'", cond)
        if m:
            return f"source.{m.group(1)} {m.group(2)} '{m.group(3)}'"
        # [col] IN {"a","b"}
        m = re.match(r'\[(\w+)\]\s+in\s+\{([^}]+)\}', cond, re.IGNORECASE)
        if m:
            vals = re.findall(r'"([^"]*)"', m.group(2))
            if not vals:
                vals = re.findall(r"'([^']*)'", m.group(2))
            in_list = ', '.join(f"'{v}'" for v in vals)
            return f"source.{m.group(1)} IN ({in_list})"
        # NOT [col] IN {…}
        m = re.match(r'NOT\s+\[(\w+)\]\s+in\s+\{([^}]+)\}', cond, re.IGNORECASE)
        if m:
            vals = re.findall(r'"([^"]*)"', m.group(2))
            if not vals:
                vals = re.findall(r"'([^']*)'", m.group(2))
            in_list = ', '.join(f"'{v}'" for v in vals)
            return f"source.{m.group(1)} NOT IN ({in_list})"
        # [col] = 123
        m = re.match(r'\[(\w+)\]\s*(=|<>|<|>|<=|>=)\s*(\d+(?:\.\d+)?)', cond)
        if m:
            return f"source.{m.group(1)} {m.group(2)} {m.group(3)}"

        return None

    def _resolve_column(self, pbi_table: str, pbi_col: str, table_key: str) -> str:
        """Resolve a PBI column name to physical column (with overrides)."""
        override_key = f'{pbi_table}.{pbi_col}'
        if override_key in self.column_overrides:
            return self.column_overrides[override_key]
        return pbi_col

    def _extend_with_implicit_filters(self, filter_parts: list[str], alias: str):
        """Add MQuery implicit filters for a fact join alias."""
        safe_alias = _sanitize_alias(alias)
        for fj in self._fact_joins:
            if fj.get('name') == safe_alias:
                fj_cfg = fj.get('_fact_join_config', {})
                for impl in fj_cfg.get('implicit_filters', []):
                    impl_sql = impl.format(alias=safe_alias)
                    if impl_sql not in ' AND '.join(filter_parts):
                        filter_parts.append(impl_sql)

    @staticmethod
    def _extract_divide_args(text: str) -> tuple[str, str] | None:
        """Extract numerator and denominator from DIVIDE(a, b)."""
        m = re.search(r'DIVIDE\s*\(', text, re.IGNORECASE)
        if not m:
            return None
        start = m.end()
        depth = 1
        pos = start
        comma_pos = None
        while pos < len(text) and depth > 0:
            if text[pos] == '(':
                depth += 1
            elif text[pos] == ')':
                depth -= 1
                if depth == 0:
                    break
            elif text[pos] == ',' and depth == 1 and comma_pos is None:
                comma_pos = pos
            pos += 1
        if comma_pos is None:
            return None
        return text[start:comma_pos].strip(), text[comma_pos + 1:pos].strip()

    @staticmethod
    def _substitute_vars(expr: str, var_map: dict[str, str]) -> str | None:
        """Substitute variable names with their SQL equivalents."""
        result = expr
        for var_name in sorted(var_map.keys(), key=len, reverse=True):
            result = re.sub(rf'\b{re.escape(var_name)}\b', var_map[var_name], result)
        if re.search(r'\b[a-z]\d?\b', result) and 'source.' not in result and 'SUM(' not in result:
            return None
        return result

    @staticmethod
    def _strip_var_block(dax: str) -> str:
        """Strip comment lines and leading whitespace."""
        lines = []
        for line in dax.split('\n'):
            stripped = line.strip()
            if stripped.startswith('//'):
                continue
            lines.append(stripped)
        return '\n'.join(lines).strip()

    @staticmethod
    def _strip_return(text: str) -> str:
        """Strip trailing RETURN keyword."""
        return re.sub(r'\breturn\s*$', '', text, flags=re.IGNORECASE).strip()

    def _find_calculate_measure_refs(self, text: str) -> list[dict]:
        """Find CALCULATE([MeasureRef], filter) patterns in text."""
        refs = []
        for m in re.finditer(r'CALCULATE\s*\(\s*\[([^\]]+)\]', text, re.IGNORECASE):
            ref_name = m.group(1)
            calc_start = m.start()
            paren_start = text.index('(', m.start()) + 1
            depth = 1
            pos = paren_start
            while pos < len(text) and depth > 0:
                if text[pos] == '(':
                    depth += 1
                elif text[pos] == ')':
                    depth -= 1
                pos += 1
            calc_end = pos
            inner = text[paren_start:calc_end - 1]
            bracket_end = inner.index(']') + 1
            filter_text = inner[bracket_end:].strip()
            if filter_text.startswith(','):
                filter_text = filter_text[1:].strip()
            refs.append({
                'ref_name': ref_name,
                'filter_text': filter_text,
                'start': calc_start,
                'end': calc_end,
            })
        return refs

    def _parse_calculate_filters(self, filter_text: str, table_key: str) -> list[str]:
        """Parse CALCULATE filter arguments into SQL conditions."""
        if not filter_text:
            return []
        conditions = []
        for part in re.split(r',\s*(?=FILTER\b|ALL\b|\w+\[)', filter_text, flags=re.IGNORECASE):
            part = part.strip()
            if not part:
                continue
            if part.upper().startswith('ALL('):
                continue
            fm = re.match(r'FILTER\s*\(\s*(\w+)\s*,\s*(.+)\s*\)\s*$', part, re.IGNORECASE | re.DOTALL)
            if fm:
                cond = self._dax_condition_to_sql(fm.group(2), table_key)
                if cond:
                    conditions.append(cond)
            else:
                cond = self._dax_condition_to_sql(part, table_key)
                if cond:
                    conditions.append(cond)
        return conditions

    def _resolve_measure_ref(self, ref_name: str, extra_filters: list[str]) -> str | None:
        """Resolve a [MeasureRef] to SQL using known resolutions."""
        resolution = self._measure_resolutions.get(ref_name)
        if not resolution:
            return None
        base_expr = resolution.get('base_expr', '')
        base_filters = list(resolution.get('base_filters', []))
        all_filters = base_filters + extra_filters
        if all_filters:
            return f"{base_expr} FILTER (WHERE {' AND '.join(all_filters)})"
        return base_expr

    @staticmethod
    def _strip_bare_calculate(text: str) -> str:
        while True:
            m = re.search(r'CALCULATE\s*\(\s*(?!\[)', text, re.IGNORECASE)
            if not m:
                break
            start = text.index('(', m.start()) + 1
            depth = 1
            pos = start
            while pos < len(text) and depth > 0:
                if text[pos] == '(':
                    depth += 1
                elif text[pos] == ')':
                    depth -= 1
                pos += 1
            inner = text[start:pos - 1]
            text = text[:m.start()] + inner + text[pos:]
        return text

    def _resolve_remaining_dax(self, sql: str) -> str:
        """Post-process SQL to resolve bare [MeasureRef] and remaining DIVIDE()."""
        def _resolve_ref(m: re.Match) -> str:
            ref_name = m.group(1)
            resolution = self._measure_resolutions.get(ref_name)
            if resolution:
                base = resolution['base_expr']
                filters = resolution.get('base_filters', [])
                if filters:
                    return f"{base} FILTER (WHERE {' AND '.join(filters)})"
                return base
            return f'1 /* UNRESOLVED: [{ref_name}] */'
        sql = re.sub(r'(?<!\w)\[([^\]]+)\]', _resolve_ref, sql)
        for _ in range(5):
            m = re.search(r'DIVIDE\s*\(', sql, re.IGNORECASE)
            if not m:
                break
            start = m.end()
            depth = 1
            pos = start
            comma_pos = None
            while pos < len(sql) and depth > 0:
                if sql[pos] == '(':
                    depth += 1
                elif sql[pos] == ')':
                    depth -= 1
                    if depth == 0:
                        break
                elif sql[pos] == ',' and depth == 1 and comma_pos is None:
                    comma_pos = pos
                pos += 1
            if comma_pos is not None:
                num = sql[start:comma_pos].strip()
                den = sql[comma_pos + 1:pos].strip()
                replacement = f'{num} / NULLIF({den}, 0)'
                sql = sql[:m.start()] + replacement + sql[pos + 1:]
        return sql

    @staticmethod
    def _cleanup_resolved_text(text: str) -> str | None:
        lines = text.split('\n')
        out: list[str] = []
        for line in lines:
            s = line.strip()
            if re.match(r'^var\s+', s, re.IGNORECASE):
                continue
            if s.lower().startswith('return '):
                s = s[7:]
            s = re.sub(
                r'^if\s*\(\s*ISBLANK\s*\(\s*\w+\s*\)\s*,\s*0\s*,\s*(\w+)\s*\)\s*$',
                '', s, flags=re.IGNORECASE)
            if s:
                out.append(s)
        return ' '.join(out).strip() or None
