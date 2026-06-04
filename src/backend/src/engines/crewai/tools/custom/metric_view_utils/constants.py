"""Compiled regex patterns shared across metric-view modules."""
from __future__ import annotations

import re

# Regex: SUM(alias.col) AS name
RE_AGG_COL = re.compile(
    r'sum\s*\(\s*(?:\w+\.\s*)?([\w]+)\s*\)\s+as\s+([\w]+)',
    re.IGNORECASE,
)

# Regex: extract FROM clause
RE_FROM_CLAUSE = re.compile(
    r'FROM\s+([\w.]+)(?:\s+(?:as\s+)?(\w+))?',
    re.IGNORECASE,
)

# Regex: LEFT [OUTER] JOIN
RE_LEFT_JOIN = re.compile(
    r'LEFT\s+(?:OUTER\s+)?JOIN\s+([\w.]+)\s+(?:as\s+)?(\w+)\s+ON\s+(\S+\s*=\s*\S+)',
    re.IGNORECASE,
)

# Regex: GROUP BY columns
RE_GROUP_BY = re.compile(
    r'GROUP\s+BY\s+([\s\S]+?)(?:ORDER\s+BY|LIMIT|HAVING|UNION\b|$)',
    re.IGNORECASE,
)

# Regex: calculated column
RE_CALC_COL = re.compile(
    r'^\s+(.+?)\s+AS\s+`?([\w]+)`?\s*$',
    re.IGNORECASE,
)

# Regex: COALESCE expression within aggregate
RE_COALESCE_AGG = re.compile(
    r'\(coalesce\(sum\((\w+)\)\s*,\s*\d+\)\s*-\s*coalesce\(sum\((\w+)\)\s*,\s*\d+\)\)\s+as\s+(\w+)',
    re.IGNORECASE,
)

# Regex: SUM(CASE WHEN ... THEN col END) AS name
RE_CASE_AGG = re.compile(
    r'SUM\s*\(\s*CASE\s+WHEN\s+.+?\s+END\s*\)\s+AS\s+(\w+)',
    re.IGNORECASE | re.DOTALL,
)

# Regex: DAX dimension table references  DimTable[column]
RE_DAX_DIM_REF = re.compile(r"(\w+)\[(\w+)\]")

# DAX aggregate patterns
RE_SIMPLE_SUM = re.compile(
    r'(?:CALCULATE\s*\(\s*)?SUM\s*\(\s*(\w+)\[(\w+)\]\s*\)\s*\)?',
    re.IGNORECASE,
)

RE_SUMX_FILTER = re.compile(
    r'SUMX\s*\(\s*FILTER\s*\(\s*(\w+)\s*,\s*(.*?)\s*\)\s*,\s*(\w+)\[(\w+)\]\s*\)',
    re.IGNORECASE | re.DOTALL,
)

RE_SIMPLE_SUMX = re.compile(
    r'SUMX\s*\(\s*(\w+)\s*,\s*(\w+)\[(\w+)\]\s*\)',
    re.IGNORECASE,
)

RE_COUNTX_FILTER = re.compile(
    r'COUNTX\s*\(\s*FILTER\s*\(\s*(\w+)\s*,\s*(.*?)\s*\)\s*,\s*(\w+)\[(\w+)\]\s*\)',
    re.IGNORECASE | re.DOTALL,
)

RE_AVERAGEX_FILTER = re.compile(
    r'AVERAGEX\s*\(\s*FILTER\s*\(\s*(\w+)\s*,\s*(.*?)\s*\)\s*,\s*(\w+)\[(\w+)\]\s*\)',
    re.IGNORECASE | re.DOTALL,
)
