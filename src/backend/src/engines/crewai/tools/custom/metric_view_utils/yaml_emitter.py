"""YAML Emitter — generate UC Metric View YAML from MetricViewSpec.

Exact port of emit_yaml() from the monolith generate_metric_views.py
(lines 4570-5038) plus its helper functions (_yaml_scalar, _yaml_val,
_yaml_needs_quoting, _clean_filter_prefixes).
"""
from __future__ import annotations

import copy
import logging
import re
from typing import Any

from .data_classes import MetricViewSpec, TranslationResult
from .metadata_generator import MetadataGenerator
from .utils import col_to_readable, spark_sql_compat

logger = logging.getLogger(__name__)


# ─── SQL injection prevention ────────────────────────────────────────────────

def _check_dangerous_sql(expr: str) -> bool:
    """Check if a SQL expression contains dangerous patterns.

    Returns True if the expression is SAFE, False if dangerous.
    """
    if not expr:
        return True
    # Check for SQL injection patterns
    _DANGEROUS_PATTERNS = re.compile(
        r'(?:'
        r'\b(?:DROP\s+TABLE|DROP\s+VIEW|DROP\s+SCHEMA|DROP\s+DATABASE|'
        r'DELETE\s+FROM|TRUNCATE\s+TABLE|ALTER\s+TABLE|'
        r'INSERT\s+INTO|UPDATE\s+\w+\s+SET|'
        r'GRANT\s+|REVOKE\s+|CREATE\s+USER|xp_cmdshell|'
        # A measure expression must never define objects — block CREATE DDL too.
        r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW|FUNCTION|SCHEMA|DATABASE))\b'
        r'|\bEXEC\s*\(|\bEXECUTE\s*\('
        r'|;\s*(?:DROP|DELETE|INSERT|UPDATE|CREATE|ALTER|GRANT|REVOKE)\b'
        # Dollar-quote delimiter — would break out of a $$...$$ quoted DDL body.
        r'|\$\$'
        r'|\bUNION\s+SELECT\s+.*\bFROM\s+information_schema\b'
        r')',
        re.IGNORECASE,
    )
    return not _DANGEROUS_PATTERNS.search(expr)


# ─── Metadata limits check ───────────────────────────────────────────────────

def _check_metadata_limits(spec: MetricViewSpec) -> list[str]:
    """Check UC Metric View metadata limits. Returns list of warnings."""
    warnings: list[str] = []
    MAX_MEASURES = 500
    MAX_DIMENSIONS = 200
    MAX_JOINS = 50
    MAX_COMMENT_LENGTH = 4000
    MAX_EXPR_LENGTH = 4000

    if len(spec.measures) > MAX_MEASURES:
        warnings.append(f"Too many measures: {len(spec.measures)} > {MAX_MEASURES}")
    if len(spec.dimensions) > MAX_DIMENSIONS:
        warnings.append(f"Too many dimensions: {len(spec.dimensions)} > {MAX_DIMENSIONS}")
    if len(spec.joins) > MAX_JOINS:
        warnings.append(f"Too many joins: {len(spec.joins)} > {MAX_JOINS}")
    if spec.comment and len(spec.comment) > MAX_COMMENT_LENGTH:
        warnings.append(f"Comment too long: {len(spec.comment)} > {MAX_COMMENT_LENGTH}")
    for m in spec.measures:
        if m.sql_expr and len(m.sql_expr) > MAX_EXPR_LENGTH:
            warnings.append(f"Expression too long for {m.measure_name}: {len(m.sql_expr)} > {MAX_EXPR_LENGTH}")
    return warnings


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


def _categorize_untranslatable(measure) -> tuple[str, str]:
    """Classify an untranslatable measure into a human-readable CATEGORY + why.

    Turns the raw internal skip_reason / DAX shape into one of a few clear buckets
    so the emitted comment explains to a reviewer WHY a measure was not emitted:
      - display artifact (color/label/format — not data)
      - slicer/scalar helper (SELECTEDVALUE date pickers etc. — not aggregatable)
      - dynamic KPI selector (SWITCH mega-wrapper — picks a KPI by slicer)
      - prior-year time-intelligence (not expressible in a static metric view)
      - complex DAX needing manual translation (the residual)
    Returns (category, explanation).
    """
    name = (getattr(measure, 'original_name', '') or '').lower()
    dax = (getattr(measure, 'dax_expression', '') or '')
    reason = (getattr(measure, 'skip_reason', '') or '')
    du = dax.upper()

    if name.endswith('_color') or 'color' in name or 'format' in reason.lower() \
            or 'display' in reason.lower():
        return ('display artifact',
                'formatting/label only — not a data measure')
    if re.search(r'SAMEPERIODLASTYEAR|DATEADD|PARALLELPERIOD|PREVIOUSYEAR', du) \
            or 'prior-year' in reason.lower():
        return ('prior-year time-intelligence',
                'period-shift not expressible in a static UC metric view '
                '(supply a calendar date_py column or compute in the source view)')
    if 'SWITCH' in du and 'SELECTEDVALUE' in du:
        return ('dynamic KPI selector',
                'SWITCH(SELECTEDVALUE(...)) picks a KPI by slicer — no static '
                'metric-view equivalent; expand per-branch or handle in the report')
    # Construct-specific guidance — state the ACTUAL unlock (or honest skip) so a
    # reviewer knows the next step, not just "needs manual translation". These run
    # BEFORE the generic SELECTEDVALUE/scalar catch below because these constructs
    # frequently co-occur with a SELECTEDVALUE arg (a slicer feeding the pattern),
    # and the specific construct is the actionable signal, not the SELECTEDVALUE.
    if 'TREATAS' in du:
        return ('disconnected-slicer dispatch (TREATAS)',
                'slicer picks which KPI to show — display-layer, not a metric; '
                'define each underlying KPI as its own measure, no source-view unlock')
    if 'LOOKUPVALUE' in du:
        return ('parameter/label lookup (LOOKUPVALUE)',
                'builds a display string or reads a slicer parameter table — not a '
                'metric; a real attribute lookup is a join (RELATED), not this')
    if 'TOPN' in du:
        return ('top-N row selection (TOPN)',
                'ranks-and-slices rows — needs a source-view ROW_NUMBER()/QUALIFY '
                'precompute; do not approximate with MAX')
    if 'ALLEXCEPT' in du:
        return ('fixed-LOD (ALLEXCEPT)',
                'aggregate at the kept-column grain — 1 kept col → window range:all; '
                '2+ kept cols → source-view SUM(...) OVER (PARTITION BY ...)')
    if 'SUMMARIZE' in du or 'CALCULATETABLE' in du or 'ADDCOLUMNS' in du:
        return ('group-then-aggregate (SUMMARIZE/CALCULATETABLE)',
                'builds a grouped virtual table — materialize the GROUP BY in the '
                'source SELECT as an identity dimension, then SUM it')
    if 'SELECTEDVALUE' in du or re.search(r'\bF_(START|END)_DATE\b|CUR_MONTH|CUR_YR', du):
        return ('slicer/scalar helper',
                'returns a single slicer-driven scalar (e.g. a date picker) — '
                'not an aggregatable measure')
    if 'DISTINCTCOUNT' in du:
        return ('distinct-count pattern', reason or 'DISTINCTCOUNT not translated')
    return ('complex DAX — needs manual translation', reason or 'no matching pattern')


def _usage_suffix(referenced_by: int) -> str:
    """Human-readable usage annotation for a measure comment.

    Counts measure→measure references only (not dashboard/visual usage), so the
    wording is deliberately "referenced by N measure(s)". Empty string when the
    measure is referenced by nothing (avoids noise on the many leaf measures).
    """
    if not referenced_by or referenced_by < 1:
        return ''
    noun = 'measure' if referenced_by == 1 else 'measures'
    return f' — referenced by {referenced_by} {noun}'


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
              fact_join_map: dict | None = None,
              percentage_multiplier_patterns: list[str] | None = None,
              budget_suffix: str | None = None) -> str:
    """Emit UC Metric View YAML from a MetricViewSpec."""
    spec = copy.deepcopy(spec)

    # Check metadata limits (warn, don't block)
    limit_warnings = _check_metadata_limits(spec)
    for w in limit_warnings:
        logger.warning(f"[UCMV] Metadata limit exceeded: {w}")

    budget_suffix = budget_suffix or '_bp'
    _COLUMN_ALIAS_MAP = column_alias_map or {}
    _KNOWN_MISSING_TABLES = known_missing_tables or set()
    _FACT_JOIN_MAP = fact_join_map or {}

    # Apply T-SQL -> Spark SQL compatibility to source_filter BEFORE emitting YAML
    _parts = spec.source_table.split('.')
    _cat = _parts[0] if len(_parts) >= 2 else ''
    _sch = _parts[1] if len(_parts) >= 2 else ''
    if spec.source_filter:
        spec.source_filter = spark_sql_compat(spec.source_filter, _cat, _sch)

    # SEC #6: dangerous-SQL check on the inline source_sql BEFORE it is emitted
    # into the view body (defense-in-depth for non-deployer consumers that render
    # the YAML without the deployer's whole-document scan). source_sql originates
    # from transpiled/native-query M — a crafted Value.NativeQuery could embed
    # DROP/GRANT/stacked statements. On hit, drop the inline SQL (falls back to the
    # plain source_table below) rather than emit a dangerous body.
    # NOTE: _check_dangerous_sql returns True when SAFE — drop when NOT safe.
    if spec.source_sql and not _check_dangerous_sql(spec.source_sql):
        logger.warning(
            f"[SECURITY] Dangerous SQL in source_sql for {getattr(spec, 'view_name', '?')} "
            f"— dropping inline source, falling back to source_table.")
        spec.source_sql = ''

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
        _seen_join_names: set[str] = set()
        valid_joins = []
        for j in spec.joins:
            tbl_short = j['source'].split('.')[-1] if '.' in j['source'] else j['source']
            if tbl_short in _KNOWN_MISSING_TABLES:
                _dropped_join_aliases.add(j['name'])
                continue
            # Drop duplicate join names (CORRECTNESS: a repeated join alias is
            # invalid YAML — e.g. dim_plant emitted twice from two detectors).
            # Keep the first; later same-name joins are the redundant repeats.
            if j['name'] in _seen_join_names:
                continue
            _seen_join_names.add(j['name'])
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
    # DAX may use SUM(source.<alias>) where alias is a measure name, not a physical column.
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

    # Security: reject measures with dangerous SQL patterns
    for measure_list in (base_measures, dax_measures, switch_measures):
        drop_idx = []
        for i, m in enumerate(measure_list):
            if m.sql_expr and not _check_dangerous_sql(m.sql_expr):
                logger.warning(f"[SECURITY] Dangerous SQL detected in {m.measure_name}: {m.sql_expr[:100]}")
                m.is_translatable = False
                m.skip_reason = 'Blocked: dangerous SQL pattern detected'
                m.sql_expr = None
                spec.untranslatable.append(m)
                drop_idx.append(i)
        for i in reversed(drop_idx):
            measure_list.pop(i)

    # Security: validate source_filter and source_sql
    if spec.source_filter and not _check_dangerous_sql(spec.source_filter):
        logger.warning(f"[SECURITY] Dangerous SQL in source_filter: {spec.source_filter[:100]}")
        spec.source_filter = ''

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
    # Also pull columns from FILTER clauses in switch measures
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

    # Filter out empty/phantom dimensions before emission
    spec.dimensions = [d for d in spec.dimensions if d.get('name') and d.get('expr') and d['expr'] != 'source.']

    # Deduplicate dimensions by name (CORRECTNESS: duplicate dimension names are
    # invalid UCMV YAML — a metric view rejects repeated dimension names). This
    # happens when the same calendar/plant table is reached via several join
    # aliases (e.g. dim_calendar + dim_calendar_dummy + c_dim_calendar all map to
    # a `date`/`fiscper` dimension). Keep the FIRST occurrence (richest metadata /
    # earliest join precedence); drop later same-name repeats.
    if spec.dimensions:
        _seen_dim_names: set[str] = set()
        _deduped_dims = []
        for d in spec.dimensions:
            nm = d['name']
            if nm in _seen_dim_names:
                continue
            _seen_dim_names.add(nm)
            _deduped_dims.append(d)
        spec.dimensions = _deduped_dims

    # Drop dimensions that collide by name with an emitted measure (CORRECTNESS:
    # a UCMV cannot declare a dimension and a measure with the same name — it
    # fails validation). Seen when a source column (e.g. `kbi_value`, `ebit`) is
    # both passed through as a dimension and aggregated as a base measure. The
    # measure is the KPI and wins; the raw column is dropped as a dimension.
    if spec.dimensions:
        _measure_names = {m.measure_name for m in
                          (base_measures + dax_measures + switch_measures)}
        if _measure_names:
            spec.dimensions = [d for d in spec.dimensions
                               if d['name'] not in _measure_names]

    # Dimensions
    if spec.dimensions:
        lines.append('')
        lines.append('# \u2500\u2500\u2500 Dimensions ' + '\u2500' * 63)
        lines.append('')
        lines.append('dimensions:')
        for d in spec.dimensions:
            lines.append(f'  - name: {d["name"]}')
            expr = d['expr']
            lines.append(f'    expr: {_yaml_scalar(expr, indent=4)}')
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
            comment += _usage_suffix(m.referenced_by)
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
                if 'currency_code' in fmt:
                    lines.append(f'      currency_code: {fmt["currency_code"]}')
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
            # Apply percentage multiplier only if explicitly configured via patterns
            if percentage_multiplier_patterns and '/' in expr:
                for _pmp in percentage_multiplier_patterns:
                    if re.search(_pmp, m.measure_name, re.IGNORECASE):
                        expr = f'100*{expr}'
                        break
            if any(c in expr for c in ("'", '"', ':', '#', '{', '}', '[', ']')) or 'FILTER' in expr:
                if len(expr) > 120 or '\n' in expr or ('"' in expr and "'" in expr):
                    # Use the shared scalar formatter: multi-line values become a
                    # properly-indented block scalar (|-). Writing a raw multi-line
                    # string after 'expr: >-' left continuation lines unindented and
                    # broke YAML parsing ("while scanning a simple key").
                    lines.append(f'    expr: {_yaml_scalar(expr, indent=4)}')
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
                lines.append(f"        semiadditive: {m.window_spec.get('semiadditive', 'last')}")
            # Comment and metadata from per-table overrides or auto-generated
            m_override = _mm.get(m.measure_name, {})
            dax_comment = m_override.get('comment', '')
            if not dax_comment and m.original_name != m.measure_name:
                dax_comment = f'PBI: {m.original_name}'
            dax_comment += _usage_suffix(m.referenced_by)
            if dax_comment:
                lines.append(f'    comment: {_yaml_val(dax_comment)}')
            # Display name
            m_meta = _meta_gen.get_measure_meta(m.measure_name, expr)
            dax_display = m_override.get('display_name', '')
            if not dax_display:
                dax_display = m_meta.get('display_name', '')
                if dax_display:
                    if m.measure_name.endswith(budget_suffix):
                        dax_display += ' (Budget)'
                    elif any(
                        m.measure_name.replace(budget_suffix, '') == m2.measure_name
                        for m2 in dax_measures if m2.measure_name.endswith(budget_suffix)
                    ):
                        dax_display += ' (Actual)'
            if dax_display:
                lines.append(f'    display_name: {_yaml_val(dax_display)}')
            # Format BEFORE synonyms (matching customer target style)
            if m_meta.get('format'):
                fmt = m_meta['format']
                lines.append('    format:')
                lines.append(f'      type: {fmt["type"]}')
                if 'currency_code' in fmt:
                    lines.append(f'      currency_code: {fmt["currency_code"]}')
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
            if len(expr) > 120 or 'FILTER' in expr or '\n' in expr:
                # Shared scalar formatter → multi-line becomes an indented block
                # scalar (|-); avoids the unindented-continuation YAML parse error.
                lines.append(f'    expr: {_yaml_scalar(expr, indent=4)}')
            else:
                lines.append(f'    expr: {expr}')
            if m.window_spec:
                lines.append('    window:')
                lines.append(f"      - order: {m.window_spec['order']}")
                lines.append(f"        range: {m.window_spec['range']}")
                lines.append(f"        semiadditive: {m.window_spec.get('semiadditive', 'last')}")
            lines.append(f'    comment: {_yaml_val(m.skip_reason + _usage_suffix(m.referenced_by))}')
            lines.append('')

    # Untranslatable measures as comments \u2014 sorted highest-usage-first so the
    # gaps that block the most downstream measures surface at the top for
    # reviewers with scarce time.
    if spec.untranslatable:
        lines.append(f'  # \u2500\u2500\u2500 Not emitted as measures ({len(spec.untranslatable)}) \u2014 grouped by reason \u2500\u2500\u2500')
        # Group by human category so a reviewer sees WHY each measure was skipped
        # (display artifact / slicer-scalar / dynamic selector / prior-year /
        # complex-DAX-needs-manual). Within a group, highest-usage first.
        _by_cat: dict[str, list] = {}
        _cat_why: dict[str, str] = {}
        for m in spec.untranslatable:
            cat, why = _categorize_untranslatable(m)
            _by_cat.setdefault(cat, []).append(m)
            _cat_why.setdefault(cat, why)
        # Stable, informative order: the "not-a-measure-by-nature" buckets first
        # (expected, no action), then the ones that need work.
        _order = ['display artifact', 'slicer/scalar helper', 'dynamic KPI selector',
                  'disconnected-slicer dispatch (TREATAS)',
                  'parameter/label lookup (LOOKUPVALUE)',
                  'prior-year time-intelligence', 'distinct-count pattern',
                  # translatable-with-source-view-work buckets last (actionable):
                  'fixed-LOD (ALLEXCEPT)',
                  'top-N row selection (TOPN)',
                  'group-then-aggregate (SUMMARIZE/CALCULATETABLE)',
                  'complex DAX \u2014 needs manual translation']
        for cat in _order + [c for c in _by_cat if c not in _order]:
            ms = _by_cat.get(cat)
            if not ms:
                continue
            lines.append(f'  #')
            lines.append(f'  # [{cat}] ({len(ms)}) \u2014 {_cat_why[cat]}')
            for m in sorted(ms, key=lambda x: x.referenced_by, reverse=True):
                lines.append(f'  #   - {m.original_name}{_usage_suffix(m.referenced_by)}')
                # Preserve the full original DAX so a reviewer can hand-translate
                # without re-opening the PBIX. Each DAX line is emitted as its own
                # comment line (multi-line DAX would otherwise break YAML). Indented
                # under the measure name for readability.
                dax = (getattr(m, 'dax_expression', '') or '').strip()
                if dax:
                    lines.append(f'  #       DAX:')
                    for dax_line in dax.split('\n'):
                        lines.append(f'  #         {dax_line}')

    lines.append('')
    return '\n'.join(lines)
