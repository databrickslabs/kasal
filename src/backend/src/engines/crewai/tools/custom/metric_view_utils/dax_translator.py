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
from .utils import to_snake_case


# Resolvable measure references (populated from config at runtime)
_MEASURE_RESOLUTIONS: dict[str, dict] = {}

# Filter set mappings (populated from config at runtime)
_FILTER_SETS: dict[str, dict] = {}

# Column override mappings (populated from config at runtime)
_COLUMN_OVERRIDES: dict[str, dict] = {}


class DaxTranslator:
    """Pattern-based DAX→SQL translator with ordered registry."""

    def __init__(self, config: dict | None = None):
        cfg = config or {}
        self.filter_sets = cfg.get('filter_sets', {})
        self.column_overrides = cfg.get('column_overrides', {})
        self._fact_join_map_cfg = cfg.get('fact_join_map', {})
        self._measure_resolutions = cfg.get('measure_resolutions', {})
        self._fact_joins: list[dict] = []
        self._patterns: list[tuple[str, Callable, Callable]] = []
        self._register_patterns()

    def _register_patterns(self):
        """Register translation patterns in priority order. First match wins."""
        self._patterns = [
            ('quick_reject', self._match_quick_reject, self._translate_noop),
            ('sameperiodlastyear', self._match_sameperiodlastyear, self._translate_sameperiodlastyear),
            ('simple_sum', self._match_simple_sum, self._translate_simple_sum),
            ('simple_sumx', self._match_simple_sumx, self._translate_simple_sumx),
            ('calculate_sumx_vars_divide', self._match_calc_sumx_vars_divide, self._translate_calc_sumx_vars_divide),
            ('calculate_sumx_filter_inner', self._match_calculate_sumx_filter_inner, self._translate_sumx_parts),
            ('calculate_sumx_filter_outer', self._match_calculate_sumx_filter_outer, self._translate_sumx_parts),
            ('sumx_filter', self._match_sumx_filter, self._translate_sumx_parts),
            ('countx_filter', self._match_countx_filter, self._translate_countx),
            ('averagex_filter', self._match_averagex_filter, self._translate_averagex),
            ('calculate_measure_ref', self._match_calculate_measure_ref, self._translate_calculate_measure_ref),
            ('distinctcountnoblank', self._match_distinctcountnoblank, self._translate_distinctcountnoblank),
            ('divide_calculate_measure_ref', self._match_divide_calculate_measure_ref, self._translate_divide_calculate_measure_ref),
            ('divide', self._match_divide, self._translate_divide),
            ('selectedvalue_switch', self._match_selectedvalue_switch, self._translate_noop),
        ]

    def set_fact_joins(self, fact_joins: list[dict]):
        """Set current fact joins for cross-table resolution."""
        self._fact_joins = fact_joins

    def translate(self, measure: dict, table_key: str) -> TranslationResult:
        """Translate a single DAX measure to SQL using the pattern registry."""
        name = measure.get('measure_name', '')
        dax = measure.get('dax_expression', '')
        original_name = measure.get('original_name', name)
        snake = to_snake_case(original_name)

        for pattern_name, match_fn, translate_fn in self._patterns:
            match = match_fn(dax, name)
            if match is not None:
                sql, skip_reason = translate_fn(match, dax, table_key)
                window_spec = None
                if skip_reason == '__SAMEPERIODLASTYEAR__':
                    skip_reason = ''
                    window_spec = {
                        'order': 'fiscper',
                        'range': 'trailing 12 month',
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

    def _match_quick_reject(self, dax: str, name: str) -> dict | None:
        dax_up = dax.upper()
        if 'FORMAT(' in dax_up:
            return {'reason': 'FORMAT function (display-only)'}
        if '_COLOR' in name.upper() or 'COLOR' == name.upper().split('_')[-1]:
            return {'reason': 'Color/conditional formatting measure'}
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

    def _translate_distinctcountnoblank(self, match: dict, dax: str, table_key: str) -> tuple[str | None, str]:
        col = match['column']
        return f'COUNT(DISTINCT source.{col})', ''

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
            outer_cond = p.get('outer_filter_condition')
            outer_table = p.get('outer_filter_table')
            if outer_cond and outer_table:
                outer_filter = self._build_filter_clause(
                    {'condition': outer_cond, 'filter_table': outer_table}, table_key, dax)
                if outer_filter:
                    filter_parts.append(outer_filter)
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
        alias = fj.get('alias', table_ref.lower())
        col_map = fj.get('column_map', {})
        return alias, col_map

    def _build_filter_clause(self, part: dict, table_key: str, dax: str) -> str:
        """Convert DAX FILTER condition to SQL WHERE clause."""
        condition = part.get('condition', '')
        if not condition:
            return ''
        filter_table = part.get('filter_table', '')
        alias, col_map = self._resolve_table_alias(filter_table, table_key)
        # Check filter_sets for known patterns
        for set_name, set_config in self.filter_sets.items():
            pattern = set_config.get('pattern', '')
            if pattern and re.search(pattern, condition, re.IGNORECASE):
                sql_parts = set_config.get('sql_parts', [])
                return ' AND '.join(f'{alias}.{p}' for p in sql_parts) if sql_parts else ''
        # Generic condition translation
        return self._dax_condition_to_sql(condition, table_key)

    def _dax_condition_to_sql(self, condition: str, table_key: str) -> str:
        """Translate a DAX filter condition to SQL."""
        if not condition:
            return ''
        # table[col] = "val" → source.col = 'val'
        condition = re.sub(r'(\w+)\[(\w+)\]', r'source.\2', condition)
        # Double quotes → single quotes for values
        condition = re.sub(r'"([^"]*)"', r"'\1'", condition)
        # && → AND, || → OR
        condition = condition.replace('&&', 'AND').replace('||', 'OR')
        return condition

    def _extend_with_implicit_filters(self, filter_parts: list[str], alias: str):
        """Add MQuery implicit filters for a fact join alias."""
        for fj in self._fact_joins:
            if fj.get('name') == alias:
                fj_cfg = fj.get('_fact_join_config', {})
                for impl in fj_cfg.get('implicit_filters', []):
                    impl_sql = impl.format(alias=alias)
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
