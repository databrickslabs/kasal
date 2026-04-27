"""Scan Data Parser — extract PBI native SQL + M transforms from scan JSON."""
from __future__ import annotations

import re

from .data_classes import MStep, ScanTableInfo


class ScanDataParser:
    """Parse scan_result_debug.json to extract native SQL and M transforms."""

    _RE_NATIVE_SQL = re.compile(
        r'Value\.NativeQuery\s*\(\s*\w+\s*,\s*"(.*?)"'
        r'\s*,\s*\n?\s*null\s*,\s*\[EnableFolding\s*=\s*true\]',
        re.DOTALL,
    )

    _RE_M_STEP = re.compile(
        r'#"([^"]+)"\s*=\s*Table\.(\w+)\s*\((.+)',
        re.DOTALL,
    )

    def parse(self, json_source: str | dict, dataset_name: str | None = None) -> dict[str, ScanTableInfo]:
        """Parse scan JSON → dict[pbi_table_name, ScanTableInfo].

        Args:
            json_source: Path to JSON file or raw dict/list
            dataset_name: Optional PBI dataset name. If None, auto-selects.
        """
        import json as _json
        if isinstance(json_source, str):
            with open(json_source) as f:
                data = _json.load(f)
        else:
            data = json_source

        workspaces = data.get('workspaces', [])
        if not workspaces:
            return {}
        ws = workspaces[0]
        datasets = ws.get('datasets', [])
        if not datasets:
            return {}

        if dataset_name:
            ds = next((d for d in datasets if d.get('name') == dataset_name), None)
            if not ds:
                return {}
        else:
            ds = max(datasets, key=lambda d: sum(
                1 for t in d.get('tables', [])
                for s in t.get('source', [])
                if 'NativeQuery' in str(s.get('expression', ''))
            ))

        result: dict[str, ScanTableInfo] = {}
        for table in ds.get('tables', []):
            sources = table.get('source', [])
            if not sources:
                continue
            expr = sources[0].get('expression', '')
            if 'Value.NativeQuery' not in expr:
                continue

            pbi_name = table['name']
            native_sql = self._extract_native_sql(expr)
            if not native_sql:
                continue

            m_steps = self._parse_m_steps(expr)
            has_union = 'union' in native_sql.lower()
            pbi_columns = table.get('columns', [])

            result[pbi_name] = ScanTableInfo(
                pbi_table_name=pbi_name,
                raw_m_expression=expr,
                native_sql=native_sql,
                m_steps=m_steps,
                has_union=has_union,
                pbi_columns=pbi_columns,
            )

        return result

    def _extract_native_sql(self, m_expr: str) -> str:
        """Extract the SQL string from Value.NativeQuery(conn, "SQL", ...)."""
        m = self._RE_NATIVE_SQL.search(m_expr)
        if not m:
            idx = m_expr.find('Value.NativeQuery')
            if idx < 0:
                return ''
            rest = m_expr[idx:]
            comma_idx = rest.find(',')
            if comma_idx < 0:
                return ''
            after_comma = rest[comma_idx + 1:]
            q1 = after_comma.find('"')
            if q1 < 0:
                return ''
            remaining = after_comma[q1 + 1:]
            end_pattern = re.search(r'"\s*,\s*\n?\s*null\s*,\s*\[EnableFolding', remaining)
            if end_pattern:
                return remaining[:end_pattern.start()].strip()
            return ''
        return m.group(1).strip()

    def _parse_m_steps(self, m_expr: str) -> list[MStep]:
        """Parse M transform steps after the NativeQuery/EnableFolding block."""
        idx = m_expr.find('[EnableFolding=true])')
        if idx < 0:
            return []
        rest = m_expr[idx + len('[EnableFolding=true])'):]

        steps = []
        for line in rest.split('\n'):
            line = line.strip().rstrip(',')
            if not line or line.startswith('Result') or line.startswith('in') or line.startswith('let'):
                continue
            if 'Table.FirstN' in line or 'RowLimit' in line or line.startswith('if RowLimit'):
                continue
            m = self._RE_M_STEP.match(line)
            if m:
                step_type = m.group(2)
                steps.append(MStep(step_type=step_type, raw_expression=line))

        return steps
