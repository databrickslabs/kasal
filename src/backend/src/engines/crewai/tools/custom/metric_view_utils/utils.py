"""Shared utility functions for metric-view generation."""
from __future__ import annotations

import asyncio
import concurrent.futures
import json
import re


def run_async(coro):
    """Safely run async code from sync context, handling existing event loops.

    Used by pipeline.py and uc_metric_view_generator_tool.py to bridge
    async PBI API calls from sync CrewAI tool execution.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, coro)
            return future.result(timeout=300)
    else:
        return asyncio.run(coro)


def to_snake_case(name: str) -> str:
    """Convert PBI measure name to snake_case identifier."""
    s = name.strip()
    s = re.sub(r'[%]', '_pct', s)
    s = re.sub(r'[^a-zA-Z0-9_]', '_', s)
    s = re.sub(r'_+', '_', s)
    s = s.strip('_').lower()
    return s


def col_to_readable(col_name: str) -> str:
    """Convert snake_case column to readable comment."""
    return col_name.replace('_', ' ').strip().capitalize()


def spark_sql_compat(expr: str, catalog: str = '', schema: str = '',
                     rewrite_2part_tables: bool = False) -> str:
    """Rewrite T-SQL patterns to Spark SQL equivalents."""
    # Strip SQL block comments
    expr = re.sub(r'/\*.*?\*/', '', expr, flags=re.DOTALL).strip()
    # GETDATE() → CURRENT_DATE()
    expr = re.sub(r'\bGETDATE\s*\(\s*\)', 'CURRENT_DATE()', expr, flags=re.IGNORECASE)
    # INT(expr) → CAST(expr AS INT)
    while True:
        m = re.search(r'\bINT\s*\(', expr, re.IGNORECASE)
        if not m:
            break
        start = m.end()
        depth = 1
        pos = start
        while pos < len(expr) and depth > 0:
            if expr[pos] == '(':
                depth += 1
            elif expr[pos] == ')':
                depth -= 1
            pos += 1
        inner = expr[start:pos - 1]
        expr = expr[:m.start()] + f'CAST({inner} AS INT)' + expr[pos:]
    # CONVERT(type, expr) → CAST(expr AS type)
    expr = re.sub(
        r'\bCONVERT\s*\(\s*(\w+)\s*,\s*([^)]+)\)',
        r'CAST(\2 AS \1)', expr, flags=re.IGNORECASE,
    )
    # ISNULL(a, b) → COALESCE(a, b)
    expr = re.sub(r'\bISNULL\s*\(', 'COALESCE(', expr, flags=re.IGNORECASE)
    # Rewrite 2-part table names to 3-part (only when explicitly enabled)
    if rewrite_2part_tables and catalog and schema:
        def _rewrite_2part(m: re.Match) -> str:
            prefix = m.group(1)
            tbl_schema = m.group(2)
            tbl_name = m.group(3)
            return f'{prefix}{catalog}.{schema}.{tbl_schema}__{tbl_name}'
        expr = re.sub(
            r'(FROM\s+|JOIN\s+)(\w+)\.(\w+)(?!\.\w)',
            _rewrite_2part, expr, flags=re.IGNORECASE,
        )
    return expr


def load_mapping(mapping_source) -> list | dict:
    """Load measure mapping from JSON path, raw list, or raw dict."""
    if isinstance(mapping_source, (list, dict)):
        return mapping_source
    if isinstance(mapping_source, str):
        # Could be a JSON string or a file path
        stripped = mapping_source.strip()
        if stripped.startswith(('{', '[')):
            return json.loads(stripped)
        # Treat as a file path
        with open(mapping_source) as f:
            return json.load(f)
    raise TypeError(f"load_mapping: unsupported type {type(mapping_source)}")


def yaml_scalar(value: str, width: int = 100) -> str:
    """Format a string as a YAML literal scalar if it contains newlines."""
    if '\n' not in value:
        return value
    lines = value.split('\n')
    indented = '\n'.join(f'  {line}' if line.strip() else '' for line in lines)
    return f'|-\n{indented}'


def unflatten_table_name(flat_name: str, catalog: str = '', schema: str = '') -> str:
    """Convert flattened table refs (catalog__schema__table) to 3-level names."""
    prefix = f'{catalog}.{schema}.'
    remainder = flat_name[len(prefix):] if flat_name.startswith(prefix) else flat_name
    parts = remainder.split('__')
    return '.'.join(parts) if len(parts) >= 3 else flat_name
