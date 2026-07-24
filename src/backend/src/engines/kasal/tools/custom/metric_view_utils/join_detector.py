"""Join Detector — auto-detect joins from DAX dimension references."""
from __future__ import annotations

import logging
import re

from .constants import RE_DAX_DIM_REF
from .data_classes import TableInfo
from .utils import col_to_readable

logger = logging.getLogger(__name__)

_RE_SAFE_ALIAS = re.compile(r'^[a-zA-Z_]\w*$')


def _sanitize_alias(alias: str) -> str:
    """Sanitize a SQL alias to prevent injection. Only allow alphanumeric + underscore."""
    if not _RE_SAFE_ALIAS.match(alias):
        raise ValueError(f"Invalid SQL alias: {alias}")
    return alias


class JoinDetector:
    """Auto-detect joins from DAX measure references to dimension tables."""

    def __init__(self, mquery_tables: dict[str, TableInfo], config: dict | None = None):
        self.mquery_tables = mquery_tables
        cfg = config or {}
        self._join_key_map = cfg.get('join_key_map', {})
        self._fact_join_map = cfg.get('fact_join_map', {})
        # P4a: global opt-in to dedup ALL dim joins with a QUALIFY subquery
        # (per-dim `dedup_dim` in join_key_map takes precedence / adds to this).
        self._dedup_all_dim_joins = bool(cfg.get('dedup_dim_joins', False))

    def detect(self, fact_table_key: str, measures: list[dict],
               fact_info: TableInfo,
               inner_dim_joins: bool = False) -> list[dict]:
        """Detect required dimension joins for a fact table based on DAX references."""
        referenced_dims: set[str] = set()
        for m in measures:
            dax = m.get('dax_expression', '')
            for match in RE_DAX_DIM_REF.finditer(dax):
                table_ref = match.group(1)
                if table_ref in self._join_key_map:
                    referenced_dims.add(table_ref)

        # Also add dims where the join_key matches a GROUP BY column
        # (these are natural joins even without DAX references)
        for dim_name, jk_check in self._join_key_map.items():
            if dim_name in referenced_dims:
                continue
            jk_key = jk_check.get('join_key', '')
            if jk_key and jk_key in fact_info.group_by_columns:
                referenced_dims.add(dim_name)
            else:
                for alt_k in jk_check.get('alt_join_keys', []):
                    if alt_k in fact_info.group_by_columns:
                        referenced_dims.add(dim_name)
                        break

        joins = []
        for dim_name in sorted(referenced_dims):
            jk = self._join_key_map[dim_name]
            dim_table_info = self.mquery_tables.get(dim_name)
            source = ''
            if dim_table_info and dim_table_info.source_table:
                source = dim_table_info.source_table
            else:
                # Fallback: check fact table's dim_source_tables (from MQuery LEFT JOINs)
                alias = jk.get('alias', dim_name.lower())
                for fact_alias, fact_src in fact_info.dim_source_tables.items():
                    if fact_alias.lower() == alias or dim_name.lower() in fact_alias.lower():
                        source = fact_src
                        break
            if not source:
                # Fallback: use source_table from join_key_map config
                source = jk.get('source_table', '')
            if not source:
                continue

            alias = jk['alias']
            join_key = jk['join_key']
            dim_key = jk.get('dim_key', join_key)

            has_key = (join_key in fact_info.group_by_columns
                       or (not fact_info.group_by_columns
                           and join_key in fact_info.full_sql))

            # P4b: zero-pad the fact-side key when the dim key is a fixed-width
            # code and the fact carries the unpadded form (classic SAP
            # co_code_bw(3) → comp_code(4)). Config-driven — the pad length is a
            # data fact the tool can't infer, but once given it generates exactly
            # the LPAD the customer otherwise hand-writes.
            fact_key_sql = self._padded_key_sql('source', join_key, jk)
            join_on = f'{fact_key_sql} = {alias}.{dim_key}'

            if not has_key:
                alt_keys = jk.get('alt_join_keys', [])
                alt_join_on = jk.get('alt_join_on')
                if alt_keys and alt_join_on and all(
                    k in fact_info.group_by_columns for k in alt_keys
                ):
                    has_key = True
                    join_on = alt_join_on

            if has_key:
                # P4a: dedup a non-unique dim on its key via a QUALIFY subquery
                # source, so the join can't fan out rows. Opt-in (per-dim
                # `dedup_dim` flag or global `dedup_dim_joins`) because whether a
                # dim is non-unique is a data fact; when flagged we generate the
                # SELECT DISTINCT … QUALIFY ROW_NUMBER() = 1 the GT hand-wrote.
                join_source = self._maybe_dedup_source(source, dim_key, jk)
                join_entry = {
                    'name': alias,
                    'source': join_source,
                    'join_on': join_on,
                }
                if inner_dim_joins:
                    join_entry['join_type'] = 'inner'
                joins.append(join_entry)

        return joins

    def _padded_key_sql(self, prefix: str, join_key: str, jk: dict) -> str:
        """Build the fact-side join-key expression, applying LPAD when the dim
        config declares a fixed-width padded key (P4b). ``jk['pad_key']`` is
        ``{'len': int, 'char': str}`` (char defaults to '0'); absent → no padding.
        """
        pad = jk.get('pad_key')
        base = f'{prefix}.{join_key}'
        if isinstance(pad, dict) and pad.get('len'):
            char = str(pad.get('char', '0'))[:1] or '0'
            return f"LPAD({base}, {int(pad['len'])}, '{char}')"
        return base

    def _maybe_dedup_source(self, source: str, dim_key: str, jk: dict) -> str:
        """Wrap a dim source in a QUALIFY-dedup subquery when the join is flagged
        as pointing at a non-unique dim (P4a). Returns ``source`` unchanged when
        not flagged, or when ``source`` is already a subquery. ``order_by``
        defaults to the dim key (deterministic pick of the first row per key)."""
        if not (jk.get('dedup_dim') or self._dedup_all_dim_joins):
            return source
        s = source.strip()
        if s.startswith('('):
            return source  # already a subquery — don't double-wrap
        order_by = jk.get('dedup_order_by') or dim_key
        return (
            f"(SELECT * FROM {source} "
            f"QUALIFY ROW_NUMBER() OVER "
            f"(PARTITION BY {dim_key} ORDER BY {order_by}) = 1)"
        )

    def detect_fact_joins(self, fact_table_key: str, measures: list[dict],
                          fact_info: TableInfo) -> list[dict]:
        """Detect required fact-to-fact joins for cross-table measures."""
        referenced_facts: set[str] = set()
        for m in measures:
            for ref in m.get('direct_fact_refs', []):
                if ref != fact_table_key and ref in self._fact_join_map:
                    referenced_facts.add(ref)

        # Also add fact joins that target this fact table (union_mode, source_embed)
        # even without explicit DAX references — config declares the relationship
        logger.info(
            f"[{fact_table_key}] detect_fact_joins: {len(self._fact_join_map)} entries in fact_join_map, "
            f"{len(referenced_facts)} from DAX refs"
        )
        for fj_name, fj_cfg in self._fact_join_map.items():
            if fj_name in referenced_facts or fj_name == fact_table_key:
                continue
            target = fj_cfg.get('target_fact', '')
            if target == fact_table_key:
                referenced_facts.add(fj_name)
                logger.info(f"[{fact_table_key}] Added fact join '{fj_name}' via target_fact config")

        logger.info(f"[{fact_table_key}] Total referenced_facts after config scan: {referenced_facts}")

        joins = []
        for fact_name in sorted(referenced_facts):
            fj = self._fact_join_map[fact_name]
            fact_table_info = self.mquery_tables.get(fact_name)
            source_table = ''
            if fact_table_info and fact_table_info.source_table:
                source_table = fact_table_info.source_table
                logger.info(f"[{fact_table_key}] Fact join '{fact_name}': source from mquery_tables = {source_table}")
            else:
                # Fallback: use source_table from fact_join_map config
                source_table = fj.get('source_table', '')
                logger.info(f"[{fact_table_key}] Fact join '{fact_name}': source from config = {source_table}")
            if not source_table:
                logger.warning(
                    f"[{fact_table_key}] Cannot add fact join '{fact_name}' — "
                    f"no source table. Add 'source_table' to fact_join_map['{fact_name}']."
                )
                continue
            alias = _sanitize_alias(fj['alias'])

            # Build join ON clause
            if 'join_on_expr' in fj:
                join_on = fj['join_on_expr'].format(alias=alias)
                for src_ref in re.findall(r'\bsource\.(\w+)', join_on):
                    if src_ref not in fact_info.group_by_columns:
                        for calc in fact_info.calculated_columns:
                            if calc['name'] == src_ref:
                                calc_expr = calc['expr']
                                for gb in fact_info.group_by_columns:
                                    calc_expr = re.sub(
                                        rf'(?<![.\w])\b{re.escape(gb)}\b(?!\s*\()',
                                        f'source.{gb}', calc_expr)
                                join_on = join_on.replace(f'source.{src_ref}', calc_expr)
                                break
                        else:
                            logger.warning(
                                "[JOIN] source.%s referenced in join_on_expr but "
                                "not found in group_by_columns or calculated_columns",
                                src_ref,
                            )
            elif 'join_key' in fj:
                join_keys = fj['join_key'] if isinstance(fj['join_key'], list) else [fj['join_key']]
                if not all(k in fact_info.group_by_columns for k in join_keys):
                    continue
                join_on = ' AND '.join(f'source.{k} = {alias}.{k}' for k in join_keys)
            else:
                continue

            # Pivot narrow/vertical KBI tables
            pivot_col = fj.get('pivot_col')
            pivot_kbi_map: dict[str, str] = {}
            if pivot_col:
                value_col = fj.get('value_col', 'val')
                _col_map = fj.get('column_map', {})
                phys_value_col = _col_map.get(value_col, value_col)
                grain_cols = fj.get('grain', [])
                kbi_codes: set[str] = set()
                _kbi_pattern = re.compile(
                    rf'\b{re.escape(fact_name)}\[{re.escape(pivot_col)}\]\s*'
                    rf'(?:=\s*"([A-Z0-9]+)"|in\s*\{{([^}}]+)\}})',
                    re.IGNORECASE,
                )
                for m_entry in measures:
                    for hit in _kbi_pattern.finditer(m_entry.get('dax_expression', '')):
                        if hit.group(1):
                            kbi_codes.add(hit.group(1))
                        elif hit.group(2):
                            kbi_codes.update(re.findall(r'[A-Z0-9]+', hit.group(2)))

                # Fallback: use kbi_codes from config if DAX extraction found none
                if not kbi_codes:
                    config_codes = fj.get('kbi_codes', [])
                    if config_codes:
                        kbi_codes.update(config_codes)
                        logger.info(
                            f"[{fact_table_key}] Using {len(config_codes)} kbi_codes from "
                            f"fact_join_map config for {fact_name}"
                        )

                if kbi_codes:
                    pivot_kbi_map = {code: f'sc_{code.lower()}' for code in kbi_codes}
                    _empty_alias = ''  # no alias needed for pivot subquery
                    implicit = [
                        f.format(alias=_empty_alias).lstrip('.')
                        for f in fj.get('implicit_filters', [])
                    ]
                    where_clause = ('WHERE ' + ' AND '.join(implicit)) if implicit else ''
                    pivot_select = ',\n    '.join(
                        f"SUM(CASE WHEN {pivot_col} = '{code}' THEN {phys_value_col} END)"
                        f" AS sc_{code.lower()}"
                        for code in sorted(kbi_codes)
                    )
                    grain_str = ', '.join(grain_cols)

                    if fj.get('union_mode'):
                        key_expr = fj.get('union_key_expr', '')
                        if not key_expr:
                            logger.warning(
                                "[JOIN] union_key_expr not configured for %s — skipping union join",
                                fact_name,
                            )
                            continue
                        null_pivot_cols = ', '.join(
                            f"CAST(NULL AS DOUBLE) AS sc_{code.lower()}"
                            for code in sorted(kbi_codes)
                        )
                        union_arm_sql = (
                            f"SELECT {key_expr}, {grain_str},\n    {pivot_select}\n"
                            f"FROM {source_table}\n"
                            f"{where_clause}\n"
                            f"GROUP BY {grain_str}"
                        )
                        joins.append({
                            'name': alias,
                            '_union_mode': True,
                            '_union_arm_sql': union_arm_sql,
                            '_null_pivot_cols': null_pivot_cols,
                            '_primary_exclude_filter': fj.get('primary_exclude_filter', ''),
                            '_pivot_kbi_map': pivot_kbi_map,
                            '_fact_join_config': fj,
                            '_pbi_name': fact_name,
                        })
                        continue

                    join_source = (
                        f"SELECT {grain_str},\n    {pivot_select}\n"
                        f"FROM {source_table}\n"
                        f"{where_clause}\n"
                        f"GROUP BY {grain_str}"
                    )
                    joins.append({
                        'name': alias,
                        'source': join_source,
                        'join_on': join_on,
                        '_fact_join_config': fj,
                        '_pbi_name': fact_name,
                        '_pivot_kbi_map': pivot_kbi_map,
                    })
                    continue

            # Source-embed mode
            if fj.get('source_embed'):
                embed_inline = fj.get('inline_source', '')
                embed_key = fj.get('embed_join_key', [])
                embed_cols = fj.get('embed_columns', {})
                joins.append({
                    'name': alias,
                    '_source_embed': True,
                    '_embed_inline_sql': embed_inline,
                    '_embed_join_key': embed_key,
                    '_embed_columns': embed_cols,
                    '_fact_join_config': fj,
                    '_pbi_name': fact_name,
                })
                continue

            join_source = fj.get('inline_source') or source_table
            if not fj.get('inline_source') and fact_table_info and fact_table_info.static_filters:
                qualified = []
                for f in fact_table_info.static_filters:
                    f = f.replace('source.', f'{alias}.')
                    f = re.sub(r'(?<![.\w])(\w+)\s*(=|<>|!=|IN\b|NOT\s+IN\b|LIKE\b|<|>|<=|>=)',
                               rf'{alias}.\1 \2', f)
                    qualified.append(f)
                join_on = join_on + ' AND ' + ' AND '.join(qualified)

            joins.append({
                'name': alias,
                'source': join_source,
                'join_on': join_on,
                '_fact_join_config': fj,
                '_pbi_name': fact_name,
            })
        return joins

    def get_dim_dimensions(self, joins: list[dict],
                           fact_info: TableInfo) -> list[dict]:
        """Get extra dimensions from joined dimension tables."""
        dims = []
        for j in joins:
            alias = j['name']
            for dim_name, jk in self._join_key_map.items():
                if jk['alias'] == alias:
                    for col_spec in jk['dim_columns']:
                        if isinstance(col_spec, dict):
                            dim_name_str = col_spec['name']
                            dim_expr = f"{alias}.{col_spec['expr']}"
                        else:
                            dim_name_str = col_spec
                            dim_expr = f'{alias}.{col_spec}'
                        dims.append({
                            'name': dim_name_str,
                            'expr': dim_expr,
                            'comment': f'{col_to_readable(dim_name_str)} from {dim_name}',
                        })
                    break
        return dims
