"""YAML Emitter — generate UC Metric View YAML from MetricViewSpec.

Exact port of emit_yaml() from the monolith generate_metric_views.py
(lines 4570-5038) plus its helper functions (_yaml_scalar, _yaml_val,
_yaml_needs_quoting, _clean_filter_prefixes).
"""
from __future__ import annotations

import re
from typing import Any

from .data_classes import MetricViewSpec, TranslationResult
from .metadata_generator import MetadataGenerator
from .utils import col_to_readable, spark_sql_compat


# ─── YAML formatting helpers ─────────────────────────────────────────────────

def _yaml_scalar(value: str, indent: int = 0) -> str:
    """Format a string value for safe YAML emission.

    - Plain text with no special chars -> bare scalar
    - Single-line with YAML-special chars -> double-quoted
    - Multi-line -> block scalar (|-)
    """
    if not value:
        return "''"
    if '\n' in value:
        prefix = ' ' * indent
        block_lines = value.split('\n')
        return '|-\n' + '\n'.join(f'{prefix}  {l}' for l in block_lines)
    if any(c in value for c in ('{', '}', ':', '#', "'", '[', ']', '*', '&', '!', '%', '@', '`')):
        # Prefer single quotes for values with backticks (no escaping needed)
        if '`' in value and "'" not in value:
            return f"'{value}'"
        escaped = value.replace('\\', '\\\\').replace('"', '\\"')
        return f'"{escaped}"'
    return value


def _yaml_needs_quoting(val: str) -> bool:
    """Check if a YAML scalar value needs quoting."""
    if not val:
        return True
    # Characters that require quoting in YAML
    if any(c in val for c in (':', '#', '{', '}', '[', ']', '&', '*', '!', '|', '>', '%', '@')):
        return True
    # Leading/trailing whitespace
    if val != val.strip():
        return True
    return False


def _yaml_val(val: str) -> str:
    """Format a YAML scalar value, quoting only when necessary."""
    if _yaml_needs_quoting(val):
        escaped = val.replace('\\', '\\\\').replace('"', '\\"')
        return f'"{escaped}"'
    return val


# ─── FILTER prefix cleaning ──────────────────────────────────────────────────

def _clean_filter_prefixes(expr: str, fact_table_key: str,
                           valid_join_aliases: set[str] | None = None,
                           fact_join_map: dict | None = None) -> str:
    """Strip/rewrite table-name prefixes in FILTER clauses for UC Metric View compatibility.

    UC MV FILTER uses bare column names for source-table columns and join-alias
    prefixes for cross-table columns.  Rewrites inside FILTER (WHERE ...) only:
      - 'source.col'             -> 'col'       (source prefix stripped)
      - 'fact_table_name.col'    -> 'col'       (current PBI table stripped)
      - 'other_pbi_table.col'    -> 'alias.col' (cross-table -> join alias)
      - 'unknown_prefix.col'     -> 'col'       (any unrecognised prefix stripped)
    """
    _valid = valid_join_aliases or set()
    _fjm = fact_join_map or {}
    # Prefixes to strip entirely (-> bare column name)
    strip_prefixes = {'source'}
    if fact_table_key:
        strip_prefixes.add(fact_table_key.lower())

    # Build rewrite map: lowered PBI table name -> join alias (for cross-table refs)
    pbi_to_alias: dict[str, str] = {}
    for pbi_name, cfg in _fjm.items():
        low = pbi_name.lower()
        if low not in strip_prefixes:
            pbi_to_alias[low] = cfg['alias']

    # Process each FILTER clause independently
    def _strip_in_filter(m: re.Match) -> str:
        filter_body = m.group(1)
        # 1. Strip source-table prefixes -> bare column names
        for pfx in strip_prefixes:
            filter_body = re.sub(rf'\b{re.escape(pfx)}\.(\w+)', r'\1', filter_body)
        # 2. Rewrite known cross-table PBI names -> join aliases
        for pbi_low, alias in pbi_to_alias.items():
            filter_body = re.sub(
                rf'\b{re.escape(pbi_low)}\.(\w+)', rf'{alias}.\1', filter_body)
        # 3. Keep prefixes that are valid join aliases or unknown (for later validation).
        # Only strip prefixes that were explicitly in strip_prefixes (already handled above).
        # Unknown prefixes like dim_wkctr.col are left as-is so join-alias validation can catch them.
        pass  # no further prefix stripping — unknown prefixes preserved
        # 4. Deduplicate identical AND conditions
        parts = [p.strip() for p in filter_body.split(' AND ')]
        seen: list[str] = []
        seen_norm: set[str] = set()
        for p in parts:
            norm = p.replace(' ', '')
            if norm not in seen_norm:
                seen.append(p)
                seen_norm.add(norm)
        filter_body = ' AND '.join(seen)
        return f'FILTER (WHERE {filter_body})'
    return re.sub(r'FILTER\s*\(WHERE\s+(.*?)\)', _strip_in_filter, expr)


# ─── Main YAML emitter ───────────────────────────────────────────────────────

def emit_yaml(spec: MetricViewSpec,
              measure_metadata: dict | None = None,
              dimension_metadata: dict | None = None,
              dimension_order: list[str] | None = None,
              column_alias_map: dict | None = None,
              known_missing_tables: set | None = None,
              fact_join_map: dict | None = None) -> str:
    """Emit UC Metric View YAML matching the hand-crafted pe002 format."""
    _COLUMN_ALIAS_MAP = column_alias_map or {}
    _KNOWN_MISSING_TABLES = known_missing_tables or set()
    _FACT_JOIN_MAP = fact_join_map or {}

    # Apply T-SQL -> Spark SQL compatibility to source_filter BEFORE emitting YAML
    _parts = spec.source_table.split('.')
    _cat = _parts[0] if len(_parts) >= 2 else ''
    _sch = _parts[1] if len(_parts) >= 2 else ''
    if spec.source_filter:
        spec.source_filter = spark_sql_compat(spec.source_filter, _cat, _sch)

    lines: list[str] = []
    lines.append("version: '1.1'")
    lines.append('')
    if spec.source_sql:
        lines.append('source: |-')
        for sql_line in spec.source_sql.split('\n'):
            lines.append(f'  {sql_line}')
    else:
        lines.append(f'source: {_yaml_scalar(spec.source_table)}')
        if spec.source_filter:
            lines.append(f'filter: {_yaml_scalar(spec.source_filter, indent=0)}')
    lines.append('')
    lines.append('comment: |-')
    for comment_line in spec.comment.split('\n'):
        lines.append(f'  {comment_line}')

    # Validate join tables: drop joins referencing known-missing tables.
    # Measures that reference dropped join aliases will be caught later.
    if spec.joins:
        _dropped_join_aliases: set[str] = set()
        valid_joins = []
        for j in spec.joins:
            tbl_short = j['source'].split('.')[-1] if '.' in j['source'] else j['source']
            if tbl_short in _KNOWN_MISSING_TABLES:
                _dropped_join_aliases.add(j['name'])
            else:
                valid_joins.append(j)
        spec.joins = valid_joins

    # Build calculated-column expansion map: source.<calc_name> -> actual expression
    # e.g. source.plant_workcenter_key -> CONCAT(source.plant, '/', source.workcenter)
    _calc_col_exprs: dict[str, str] = {}
    for d in spec.dimensions:
        comment = d.get('comment', '')
        if comment.startswith('Calculated:'):
            _calc_col_exprs[d['name']] = d['expr']

    # Joins
    if spec.joins:
        lines.append('')
        lines.append('# \u2500\u2500\u2500 Joins ' + '\u2500' * 68)
        lines.append('')
        lines.append('joins:')
        for j in spec.joins:
            lines.append(f'  - name: {j["name"]}')
            lines.append(f'    source: {_yaml_scalar(j["source"], indent=4)}')
            on_clause = j.get('join_on') or j.get('on') or ''
            # Expand calculated column refs in ON clause (e.g. source.plant_workcenter_key
            # -> CONCAT(source.plant, '/', source.workcenter))
            on_str = str(on_clause)
            for calc_name, calc_expr in _calc_col_exprs.items():
                on_str = re.sub(
                    rf'\bsource\.{re.escape(calc_name)}\b', calc_expr, on_str)
            # YAML 1.1 treats bare 'on' as boolean True — must quote the key
            lines.append(f'    "on": {_yaml_scalar(on_str, indent=4)}')
            if j.get('join_type'):
                lines.append(f'    type: {j["join_type"]}')

    # Measures — split into base and DAX-translated sections (needed for dimension validation below)
    base_measures = [m for m in spec.measures if m.category == 'base']
    dax_measures = [m for m in spec.measures if m.category not in ('base', 'switch_decomposition')]
    switch_measures = [m for m in spec.measures if m.category == 'switch_decomposition']

    # Rewrite known MQuery column aliases -> physical columns (e.g. nr_of_deliveries_final -> nr_of_deliveries)
    for m in base_measures + dax_measures + switch_measures:
        if m.sql_expr:
            for alias_name, phys_name in _COLUMN_ALIAS_MAP.items():
                m.sql_expr = re.sub(
                    rf'\bsource\.{re.escape(alias_name)}\b', f'source.{phys_name}', m.sql_expr)
    # Also rewrite in dimensions
    for d in spec.dimensions:
        for alias_name, phys_name in _COLUMN_ALIAS_MAP.items():
            d['expr'] = re.sub(
                rf'\bsource\.{re.escape(alias_name)}\b', f'source.{phys_name}', d['expr'])

    # Rewrite measure-name-as-column refs: source.{measure_name} -> source.{physical_col}
    # DAX uses SUM(source.nsr_bev) where nsr_bev is a measure alias, not a physical column.
    _measure_to_phys: dict[str, str] = {}
    for m in base_measures:
        if m.sql_expr:
            col_match = re.search(r'source\.(\w+)', m.sql_expr)
            if col_match and col_match.group(1) != m.measure_name:
                _measure_to_phys[m.measure_name] = col_match.group(1)
    if _measure_to_phys:
        for m in dax_measures + switch_measures:
            if m.sql_expr:
                for mname, phys in _measure_to_phys.items():
                    m.sql_expr = re.sub(
                        rf'\bsource\.{re.escape(mname)}\b', f'source.{phys}', m.sql_expr)

    # Fix cross-table column refs: source.col -> alias.col when col belongs
    # to a joined fact table, not the source table.
    fact_key = spec.fact_table_key
    _join_col_to_alias: dict[str, str] = {}  # column_name -> join alias
    if spec.joins:
        for j in spec.joins:
            alias = j['name']
            # Check if this join corresponds to a _FACT_JOIN_MAP entry
            for pbi_name, cfg in _FACT_JOIN_MAP.items():
                if cfg['alias'] == alias:
                    for col in cfg.get('column_map', {}).values():
                        _join_col_to_alias[col] = alias
    for m in base_measures + dax_measures + switch_measures:
        if m.sql_expr and _join_col_to_alias:
            for col, alias in _join_col_to_alias.items():
                m.sql_expr = re.sub(
                    rf'\bsource\.{re.escape(col)}\b', f'{alias}.{col}', m.sql_expr)

    # Clean FILTER clause prefixes: UC MV FILTER uses bare column names
    # Collect valid join aliases from the spec's declared joins
    _join_aliases = {j['name'].lower() for j in spec.joins} if spec.joins else set()
    for m in base_measures + dax_measures + switch_measures:
        if m.sql_expr and 'FILTER' in m.sql_expr:
            m.sql_expr = _clean_filter_prefixes(
                m.sql_expr, fact_key, _join_aliases, _FACT_JOIN_MAP)

    # Strip SQL single-line comments (-- ...) from expressions — breaks YAML parser
    for m in base_measures + dax_measures + switch_measures:
        if m.sql_expr and '--' in m.sql_expr:
            m.sql_expr = re.sub(r'--\s*\w[^\n)]*', '', m.sql_expr).strip()

    # Strip trailing " as <alias>" from base measure expressions (invalid in UC MV)
    for m in base_measures:
        if m.sql_expr:
            m.sql_expr = re.sub(r'\s+[Aa][Ss]\s+\w+\s*$', '', m.sql_expr).strip()

    # Apply T-SQL -> Spark SQL compatibility to all measure expressions
    for m in base_measures + dax_measures + switch_measures:
        if m.sql_expr:
            m.sql_expr = spark_sql_compat(m.sql_expr, _cat, _sch)

    # Validate join-alias references: drop measures referencing undeclared join aliases.
    # e.g. hr_a.col when no hr_a join exists, or dim_wkctr.col when dim_wkctr not joined.
    _declared_aliases = {j['name'] for j in spec.joins} if spec.joins else set()
    _declared_aliases.add('source')

    def _has_invalid_alias(expr: str) -> str | None:
        """Return the first undeclared alias found in expr, or None."""
        for alias_m in re.finditer(r'\b(\w+)\.(\w+)', expr):
            alias = alias_m.group(1)
            if alias not in _declared_aliases and alias.lower() not in _declared_aliases:
                return alias
        return None

    for measure_list in (dax_measures, switch_measures):
        drop_idx = []
        for i, m in enumerate(measure_list):
            if m.sql_expr:
                bad_alias = _has_invalid_alias(m.sql_expr)
                if bad_alias:
                    m.is_translatable = False
                    m.skip_reason = f'References undeclared join alias: {bad_alias}'
                    m.sql_expr = None
                    spec.untranslatable.append(m)
                    drop_idx.append(i)
        for i in reversed(drop_idx):
            measure_list.pop(i)

    # Validate source.column refs against known columns — drop measures with
    # unresolvable references (dimension-table columns, MQuery computed columns).
    _known_source_cols: set[str] = set()
    for m in base_measures:
        if m.sql_expr:
            _known_source_cols.update(re.findall(r'\bsource\.(\w+)', m.sql_expr))
    for d in spec.dimensions:
        _known_source_cols.update(re.findall(r'\bsource\.(\w+)', d['expr']))
    # Also pull columns from FILTER clauses in switch measures (bic_csubkbi, bic_chversion, etc.)
    for m in switch_measures:
        if m.sql_expr:
            for fc_m in re.finditer(r'FILTER\s*\(WHERE\s+([^)]+)\)', m.sql_expr):
                _known_source_cols.update(re.findall(r'\b(\w+)\s*(?:=|<>|!=|IN\b)', fc_m.group(1)))
    if _known_source_cols:
        for measure_list in (dax_measures, switch_measures):
            drop_idx = []
            for i, m in enumerate(measure_list):
                if m.sql_expr:
                    src_refs = set(re.findall(r'\bsource\.(\w+)', m.sql_expr))
                    unknown = src_refs - _known_source_cols
                    if unknown:
                        m.is_translatable = False
                        m.skip_reason = f'References unknown source columns: {unknown}'
                        m.sql_expr = None
                        spec.untranslatable.append(m)
                        drop_idx.append(i)
            for i in reversed(drop_idx):
                measure_list.pop(i)

    # MEASURE() reference validation: drop measures that reference non-existent measures.
    # Iterative cascade: dropping one measure might invalidate others.
    for _mref_pass in range(5):
        _final_names = {m.measure_name for m in base_measures + dax_measures + switch_measures}
        _dropped = 0
        for measure_list in (dax_measures, switch_measures):
            drop_idx = []
            for i, m in enumerate(measure_list):
                if m.sql_expr:
                    measure_refs = set(re.findall(r'\bMEASURE\((\w+)\)', m.sql_expr))
                    invalid_refs = measure_refs - _final_names
                    if invalid_refs:
                        m.is_translatable = False
                        m.skip_reason = f'References non-existent measures: {invalid_refs}'
                        m.sql_expr = None
                        spec.untranslatable.append(m)
                        drop_idx.append(i)
                        _dropped += 1
            for i in reversed(drop_idx):
                measure_list.pop(i)
        if _dropped == 0:
            break

    # FILTER bare-column validation: after prefix stripping, check that bare column
    # names in FILTER clauses exist on source or joined tables.
    if _known_source_cols:
        _join_cols_fv: set[str] = set()
        if spec.joins:
            for j in spec.joins:
                on_clause = j.get('join_on') or j.get('on') or ''
                _join_cols_fv.update(re.findall(r'\w+\.(\w+)', str(on_clause)))
        _all_known_filter_cols = _known_source_cols | _join_cols_fv
        for measure_list in (dax_measures, switch_measures):
            drop_idx = []
            for i, m in enumerate(measure_list):
                if m.sql_expr and 'FILTER' in m.sql_expr:
                    for fc_m in re.finditer(r'FILTER\s*\(WHERE\s+([^)]+)\)', m.sql_expr):
                        filter_body = fc_m.group(1)
                        bare_in_filter = set(re.findall(
                            r'(?<!\.)(?<!\w)\b([a-z_]\w*)\s*(?:=|<>|!=|IN\b|<|>|<=|>=|LIKE\b)',
                            filter_body))
                        _SQL_KW_FV = {'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'BETWEEN',
                                      'LIKE', 'TRUE', 'FALSE'}
                        bare_in_filter = {c for c in bare_in_filter
                                          if c.upper() not in _SQL_KW_FV}
                        unknown_filter_cols = bare_in_filter - _all_known_filter_cols
                        if unknown_filter_cols:
                            m.is_translatable = False
                            m.skip_reason = (f'FILTER references unknown columns: '
                                             f'{unknown_filter_cols}')
                            m.sql_expr = None
                            spec.untranslatable.append(m)
                            drop_idx.append(i)
                            break
            for i in reversed(drop_idx):
                measure_list.pop(i)

    # Handle empty measures: if ALL measures were dropped, skip the view entirely
    if not base_measures and not dax_measures and not switch_measures:
        return ''

    # Early dimension validation: drop phantom source.column dimensions.
    # Build column set from base measures and FILTER clauses only (not from dimensions
    # themselves — that would be circular self-validation).
    _base_only_cols_early: set[str] = set()
    for m in base_measures:
        if m.sql_expr:
            _base_only_cols_early.update(re.findall(r'\bsource\.(\w+)', m.sql_expr))
    for m in dax_measures + switch_measures:
        if m.sql_expr:
            for fc_m in re.finditer(r'FILTER\s*\(WHERE\s+([^)]+)\)', m.sql_expr):
                _base_only_cols_early.update(
                    re.findall(r'\b(\w+)\s*(?:=|<>|!=|IN\b)', fc_m.group(1)))
    if spec.source_filter:
        _base_only_cols_early.update(
            re.findall(r'\b(\w+)\s*(?:=|<>|!=|IN\b|NOT\s+IN\b)', spec.source_filter))
    # When source_sql is present (inline SQL), ALL columns in the SELECT list are valid
    if spec.source_sql:
        # Extract AS aliases
        for col_m in re.finditer(r'\bAS\s+(\w+)', spec.source_sql, re.IGNORECASE):
            _base_only_cols_early.add(col_m.group(1))
            _base_only_cols_early.add(col_m.group(1).lower())
        # Extract bare column refs from first SELECT (before FROM)
        first_arm = re.split(r'\bUNION\b', spec.source_sql, maxsplit=1, flags=re.IGNORECASE)[0]
        sel_m = re.match(r'\s*SELECT\s+(.*?)\s+FROM\s+', first_arm, re.IGNORECASE | re.DOTALL)
        if sel_m:
            for col_part in sel_m.group(1).split(','):
                bare = re.match(r'^\s*(\w+)\s*$', col_part.strip())
                if bare:
                    _base_only_cols_early.add(bare.group(1))
                    _base_only_cols_early.add(bare.group(1).lower())
    if _base_only_cols_early and spec.dimensions:
        valid_dims = []
        for d in spec.dimensions:
            src_refs = set(re.findall(r'\bsource\.(\w+)', d['expr']))
            if src_refs:
                unknown_dim_cols = src_refs - _base_only_cols_early
                if unknown_dim_cols:
                    continue  # phantom dimension — skip
            valid_dims.append(d)
        spec.dimensions = valid_dims

    # Enrich dimensions and measures with metadata (display_name, synonyms, format)
    _meta_gen = MetadataGenerator()
    _dm = dimension_metadata or {}
    for d in spec.dimensions:
        d_name = d['name']
        # Per-table dimension metadata overrides have priority
        if d_name in _dm:
            d_override = _dm[d_name]
            if 'display_name' not in d or d_override.get('display_name'):
                d['display_name'] = d_override.get('display_name', d.get('display_name', ''))
            if 'comment' not in d or d_override.get('comment'):
                d['comment'] = d_override.get('comment', d.get('comment', ''))
            if d_override.get('synonyms'):
                d['synonyms'] = d_override['synonyms']
        elif 'display_name' not in d:
            meta = _meta_gen.get_dimension_meta(d_name)
            d.update(meta)

    # Apply dimension ordering if specified
    if dimension_order:
        order_map = {name: i for i, name in enumerate(dimension_order)}
        spec.dimensions.sort(key=lambda d: order_map.get(d['name'], 999))

    # Dimensions
    if spec.dimensions:
        lines.append('')
        lines.append('# \u2500\u2500\u2500 Dimensions ' + '\u2500' * 63)
        lines.append('')
        lines.append('dimensions:')
        for d in spec.dimensions:
            lines.append(f'  - name: {d["name"]}')
            expr = d['expr']
            if any(c in expr for c in ('"', "'", '(', ',')):
                lines.append(f'    expr: "{expr}"')
            else:
                lines.append(f'    expr: {expr}')
            if d.get('comment'):
                lines.append(f'    comment: {_yaml_val(d["comment"])}')
            if d.get('display_name'):
                lines.append(f'    display_name: {_yaml_val(d["display_name"])}')
            if d.get('synonyms'):
                lines.append('    synonyms:')
                for syn in d['synonyms']:
                    lines.append(f'      - {_yaml_val(syn)}')

    lines.append('')

    _mm = measure_metadata or {}
    if base_measures:
        lines.append(f'# \u2500\u2500\u2500 Base Measures ({len(base_measures)}) ' + '\u2500' * (58 - len(str(len(base_measures)))))
        lines.append('')
        lines.append('measures:')
        for m in base_measures:
            lines.append(f'  - name: {m.measure_name}')
            expr = m.sql_expr
            if expr is None:
                continue
            lines.append(f'    expr: {expr}')
            # Use per-table metadata override, fall back to generated
            m_override = _mm.get(m.measure_name, {})
            comment = m_override.get('comment') or m.skip_reason or col_to_readable(m.measure_name)
            lines.append(f'    comment: {_yaml_val(comment)}')
            m_meta = _meta_gen.get_measure_meta(m.measure_name, expr)
            display_name = m_override.get('display_name') or m_meta.get('display_name', '')
            if display_name:
                lines.append(f'    display_name: {_yaml_val(display_name)}')
            # Format BEFORE synonyms (matching customer target style)
            if m_meta.get('format'):
                fmt = m_meta['format']
                lines.append('    format:')
                lines.append(f'      type: {fmt["type"]}')
                if 'decimal_places' in fmt:
                    lines.append('      decimal_places:')
                    lines.append('        type: exact')
                    lines.append(f'        places: {fmt["decimal_places"]["places"]}')
            synonyms = m_override.get('synonyms', [])
            if synonyms:
                lines.append('    synonyms:')
                for syn in synonyms:
                    lines.append(f'      - {_yaml_val(syn)}')
            lines.append('')
    else:
        lines.append('measures:')

    if dax_measures:
        lines.append(f'  # \u2500\u2500\u2500 DAX-Translated Measures ({len(dax_measures)}) ' + '\u2500' * (50 - len(str(len(dax_measures)))))
        lines.append('')
        for m in dax_measures:
            if not m.sql_expr:
                continue  # safety: skip measures with no SQL
            lines.append(f'  - name: {m.measure_name}')
            expr = m.sql_expr
            # Add 100* multiplier for turnover/rate ratio measures (actuals only, not BP)
            if ('turnover' in m.measure_name.lower()
                    and not m.measure_name.lower().endswith('_bp')
                    and '/' in expr):
                expr = f'100*{expr}'
            if any(c in expr for c in ("'", '"', ':', '#', '{', '}', '[', ']')) or 'FILTER' in expr:
                if len(expr) > 120 or '\n' in expr or ('"' in expr and "'" in expr):
                    lines.append('    expr: >-')
                    lines.append(f'      {expr}')
                elif '"' in expr:
                    lines.append(f"    expr: '{expr}'")
                else:
                    lines.append(f'    expr: "{expr}"')
            else:
                lines.append(f'    expr: {expr}')
            if m.window_spec:
                lines.append('    window:')
                lines.append(f"      - order: {m.window_spec['order']}")
                lines.append(f"        range: {m.window_spec['range']}")
                lines.append(f"        semiadditive: {m.window_spec['semiadditive']}")
            # Comment and metadata from per-table overrides or auto-generated
            m_override = _mm.get(m.measure_name, {})
            dax_comment = m_override.get('comment', '')
            if not dax_comment and m.original_name != m.measure_name:
                dax_comment = f'PBI: {m.original_name}'
            if dax_comment:
                lines.append(f'    comment: {_yaml_val(dax_comment)}')
            # Display name
            m_meta = _meta_gen.get_measure_meta(m.measure_name, expr)
            dax_display = m_override.get('display_name', '')
            if not dax_display:
                dax_display = m_meta.get('display_name', '')
                if dax_display:
                    if m.measure_name.endswith('_bp'):
                        dax_display += ' (Budget)'
                    elif any(
                        m.measure_name.replace('_bp', '') == m2.measure_name
                        for m2 in dax_measures if m2.measure_name.endswith('_bp')
                    ):
                        dax_display += ' (Actual)'
            if dax_display:
                lines.append(f'    display_name: {_yaml_val(dax_display)}')
            # Format BEFORE synonyms (matching customer target style)
            if m_meta.get('format'):
                fmt = m_meta['format']
                lines.append('    format:')
                lines.append(f'      type: {fmt["type"]}')
                if 'decimal_places' in fmt:
                    lines.append('      decimal_places:')
                    lines.append('        type: exact')
                    lines.append(f'        places: {fmt["decimal_places"]["places"]}')
            # Synonyms AFTER format
            synonyms = m_override.get('synonyms', [])
            if synonyms:
                lines.append('    synonyms:')
                for syn in synonyms:
                    lines.append(f'      - {_yaml_val(syn)}')
            lines.append('')

    if switch_measures:
        lines.append(f'  # \u2500\u2500\u2500 SWITCH-Decomposed Measures ({len(switch_measures)}) ' + '\u2500' * (46 - len(str(len(switch_measures)))))
        lines.append('')
        for m in switch_measures:
            lines.append(f'  - name: {m.measure_name}')
            expr = m.sql_expr
            if expr is None:
                continue
            if len(expr) > 120 or 'FILTER' in expr:
                lines.append('    expr: >-')
                lines.append(f'      {expr}')
            else:
                lines.append(f'    expr: {expr}')
            lines.append(f'    comment: "{m.skip_reason}"')
            lines.append('')

    # Untranslatable measures as comments
    if spec.untranslatable:
        lines.append(f'  # \u2500\u2500\u2500 Untranslatable PBI Measures ({len(spec.untranslatable)}) \u2500\u2500\u2500')
        for m in spec.untranslatable:
            lines.append(f'  # {m.original_name}: {m.skip_reason}')

    lines.append('')
    return '\n'.join(lines)
