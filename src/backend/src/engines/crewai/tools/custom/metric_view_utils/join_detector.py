"""Join Detector — auto-detect joins from DAX dimension references."""
from __future__ import annotations

import re

from .constants import RE_DAX_DIM_REF
from .data_classes import TableInfo
from .utils import col_to_readable


class JoinDetector:
    """Auto-detect joins from DAX measure references to dimension tables."""

    def __init__(self, mquery_tables: dict[str, TableInfo], config: dict | None = None):
        self.mquery_tables = mquery_tables
        cfg = config or {}
        self._join_key_map = cfg.get('join_key_map', {})
        self._fact_join_map = cfg.get('fact_join_map', {})

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

        joins = []
        for dim_name in sorted(referenced_dims):
            jk = self._join_key_map[dim_name]
            dim_table_info = self.mquery_tables.get(dim_name)
            if dim_table_info and dim_table_info.source_table:
                source = dim_table_info.source_table
            else:
                continue

            alias = jk['alias']
            join_key = jk['join_key']
            dim_key = jk.get('dim_key', join_key)

            has_key = (join_key in fact_info.group_by_columns
                       or (not fact_info.group_by_columns
                           and join_key in fact_info.full_sql))

            join_on = f'source.{join_key} = {alias}.{dim_key}'

            if not has_key:
                alt_keys = jk.get('alt_join_keys', [])
                alt_join_on = jk.get('alt_join_on')
                if alt_keys and alt_join_on and all(
                    k in fact_info.group_by_columns for k in alt_keys
                ):
                    has_key = True
                    join_on = alt_join_on

            if has_key:
                join_entry = {
                    'name': alias,
                    'source': source,
                    'join_on': join_on,
                }
                if inner_dim_joins:
                    join_entry['join_type'] = 'inner'
                joins.append(join_entry)

        return joins

    def detect_fact_joins(self, fact_table_key: str, measures: list[dict],
                          fact_info: TableInfo) -> list[dict]:
        """Detect required fact-to-fact joins for cross-table measures."""
        referenced_facts: set[str] = set()
        for m in measures:
            for ref in m.get('direct_fact_refs', []):
                if ref != fact_table_key and ref in self._fact_join_map:
                    referenced_facts.add(ref)

        joins = []
        for fact_name in sorted(referenced_facts):
            fj = self._fact_join_map[fact_name]
            fact_table_info = self.mquery_tables.get(fact_name)
            if not fact_table_info or not fact_table_info.source_table:
                continue
            alias = fj['alias']

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
                            if src_ref == 'plant_workcenter_key' and \
                               'plant' in fact_info.group_by_columns and \
                               'workcenter' in fact_info.group_by_columns:
                                join_on = join_on.replace(
                                    f'source.{src_ref}',
                                    "CONCAT(source.plant, '/', source.workcenter)")
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

                if kbi_codes:
                    pivot_kbi_map = {code: f'sc_{code.lower()}' for code in kbi_codes}
                    implicit = [
                        f.format(alias='').lstrip('.')
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
                        key_expr = fj.get('union_key_expr', "CONCAT(plant, '/', workcenter) AS plant_workcenter_key")
                        null_pivot_cols = ', '.join(
                            f"CAST(NULL AS DOUBLE) AS sc_{code.lower()}"
                            for code in sorted(kbi_codes)
                        )
                        union_arm_sql = (
                            f"SELECT {key_expr}, {grain_str},\n    {pivot_select}\n"
                            f"FROM {fact_table_info.source_table}\n"
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
                        f"FROM {fact_table_info.source_table}\n"
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

            join_source = fj.get('inline_source') or fact_table_info.source_table
            if not fj.get('inline_source') and fact_table_info.static_filters:
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
