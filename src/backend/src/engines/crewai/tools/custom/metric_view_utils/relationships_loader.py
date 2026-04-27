"""Relationships Loader — auto-build enrichment joins from PBI relationship JSON."""
from __future__ import annotations

import json

from .data_classes import TableInfo


class RelationshipsLoader:
    """Build enrichment joins from a PBI relationships JSON file.

    Accepts two input formats:
    - Raw PBI Execute Queries API response
    - Simplified flat list of relationship dicts
    """

    _SYSTEM_TABLE_PREFIXES = ('LocalDateTable', 'DateTableTemplate')

    def load(
        self,
        source: str | list | dict,
        mquery_tables: dict[str, TableInfo],
        fact_tables: set[str],
    ) -> dict[str, list[dict]]:
        """Parse relationships and return enrichment joins per fact table.

        Args:
            source: Path to JSON file, raw list, or raw dict
            mquery_tables: Parsed MQuery table info (TableInfo objects)
            fact_tables: Set of known fact table keys

        Returns:
            Dict mapping fact_table_key → list of enrichment join dicts
        """
        if isinstance(source, str):
            with open(source) as f:
                raw = json.load(f)
        else:
            raw = source

        relationships = self._parse_format(raw)
        enrichment: dict[str, list[dict]] = {}

        for rel in relationships:
            if not rel.get('is_active', True):
                continue
            from_tbl = rel.get('from_table', '')
            to_tbl = rel.get('to_table', '')
            if any(from_tbl.startswith(p) or to_tbl.startswith(p)
                   for p in self._SYSTEM_TABLE_PREFIXES):
                continue

            from_card = rel.get('from_cardinality', 'Many')
            to_card = rel.get('to_cardinality', 'One')

            if from_card == 'Many' and to_card == 'Many':
                continue
            elif from_card == 'One' and to_card == 'Many':
                from_tbl, to_tbl = to_tbl, from_tbl
                rel['from_column'], rel['to_column'] = (
                    rel.get('to_column', ''), rel.get('from_column', ''))
            elif from_card == 'One' and to_card == 'One':
                pass

            fact_key = from_tbl
            dim_key = to_tbl

            if fact_key not in fact_tables:
                continue

            dim_info = mquery_tables.get(dim_key)
            if not dim_info or not dim_info.source_table:
                continue

            fact_info = mquery_tables.get(fact_key)
            fact_source = fact_info.source_table if fact_info else ''
            if dim_info.source_table and fact_source and dim_info.source_table == fact_source:
                continue

            alias = dim_key.lower()
            if alias.startswith('c_dim_'):
                alias = alias[2:]

            from_col = rel.get('from_column', '')
            to_col = rel.get('to_column', '')
            if not from_col or not to_col:
                continue

            join_entry = {
                'name': alias,
                'source': dim_info.source_table,
                'join_on': f'source.{from_col} = {alias}.{to_col}',
                'dim_columns': list(dim_info.group_by_columns),
                '_auto': True,
            }

            enrichment.setdefault(fact_key, []).append(join_entry)

        return enrichment

    @staticmethod
    def _parse_format(raw: object) -> list[dict]:
        """Normalise raw JSON to a flat list of relationship dicts."""
        if isinstance(raw, list):
            return raw

        if isinstance(raw, dict) and 'results' in raw:
            rows = (raw.get('results', [{}])[0]
                       .get('tables', [{}])[0]
                       .get('rows', []))
            result = []
            seen: set[int] = set()
            for row in rows:
                rid = row.get('[ID]')
                if rid in seen:
                    continue
                seen.add(rid)
                result.append({
                    'from_table': row.get('[FromTable]', ''),
                    'from_column': row.get('[FromColumn]', ''),
                    'from_cardinality': row.get('[FromCardinality]', 'Many'),
                    'to_table': row.get('[ToTable]', ''),
                    'to_column': row.get('[ToColumn]', ''),
                    'to_cardinality': row.get('[ToCardinality]', 'One'),
                    'is_active': row.get('[IsActive]', True),
                })
            return result

        return []
