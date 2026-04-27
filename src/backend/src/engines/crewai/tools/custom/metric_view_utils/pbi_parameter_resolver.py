"""PBI Parameter Resolver — resolve Power BI parameters in native SQL."""
from __future__ import annotations

import re


class PbiParameterResolver:
    """Resolve Power BI parameters (FiscperFilter, RE_Version, CurrencyFilter)
    in native SQL extracted from scan data or MQuery transpiled SQL."""

    _RE_VERSION_CASE = (
        "CASE "
        "WHEN MONTH(CURRENT_DATE()) >= 10 THEN 'R100' "
        "WHEN MONTH(CURRENT_DATE()) >= 7 THEN 'R070' "
        "WHEN MONTH(CURRENT_DATE()) >= 4 THEN 'R040' "
        "ELSE 'R000' END"
    )

    _RE_VERSION_RANGES = {
        'R000': '1 = 0',
        'R040': 'MONTH(CURRENT_DATE()) >= 4 AND MONTH(CURRENT_DATE()) < 7',
        'R070': 'MONTH(CURRENT_DATE()) >= 7 AND MONTH(CURRENT_DATE()) < 10',
        'R100': 'MONTH(CURRENT_DATE()) >= 10',
    }

    def resolve(self, sql: str) -> str:
        """Apply all PBI parameter resolutions to SQL."""
        sql = self._resolve_fiscper_filter(sql)
        sql = self._resolve_re_version(sql)
        sql = self._resolve_currency_filter(sql)
        return sql

    def _resolve_fiscper_filter(self, sql: str) -> str:
        """Collapse FiscperFilter CASE expressions."""
        param_ref = r"""(?:['"]+\s*&\s*FiscperFilter\s*&\s*['"]+|'\$\{FiscperFilter\}')"""
        pattern = re.compile(
            r"""\(?\s*CASE\s+WHEN\s+""" + param_ref + r"""\s*=\s*'Sample'\s*THEN\s+"""
            r"""(.*?)\s*ELSE\s+(.*?)\s*END\s*\)?""",
            re.IGNORECASE | re.DOTALL,
        )
        def _replace(m: re.Match) -> str:
            else_branch = m.group(2).strip()
            if else_branch.startswith('(') and else_branch.endswith(')'):
                else_branch = else_branch[1:-1].strip()
            return else_branch
        return pattern.sub(_replace, sql)

    def _resolve_re_version(self, sql: str) -> str:
        """Resolve RE_Version parameter in two contexts."""
        for version_code, month_range in self._RE_VERSION_RANGES.items():
            sql = re.sub(
                r"""'\$\{RE_Version\}'\s*=\s*'""" + version_code + r"""'""",
                month_range, sql,
            )
            sql = re.sub(
                r"""['"]+\s*&\s*RE_Version\s*&\s*['"]+\s*=\s*'""" + version_code + r"""'""",
                month_range, sql,
            )
            sql = re.sub(
                r"""CASE\s+WHEN\s+MONTH\(CURRENT_DATE\(\)\)\s*>=\s*10\s+THEN\s+'R100'\s+"""
                r"""WHEN\s+MONTH\(CURRENT_DATE\(\)\)\s*>=\s*7\s+THEN\s+'R070'\s+"""
                r"""WHEN\s+MONTH\(CURRENT_DATE\(\)\)\s*>=\s*4\s+THEN\s+'R040'\s+"""
                r"""ELSE\s+'R000'\s*END\s*=\s*'""" + version_code + r"""'""",
                month_range, sql, flags=re.IGNORECASE,
            )
        sql = re.sub(r"""'\$\{RE_Version\}'""", self._RE_VERSION_CASE, sql)
        sql = re.sub(r"""['"]+\s*&\s*RE_Version\s*&\s*['"]+""", self._RE_VERSION_CASE, sql)
        return sql

    def _resolve_currency_filter(self, sql: str) -> str:
        """Resolve CurrencyFilter parameter → '30'."""
        sql = re.sub(r"""'\$\{CurrencyFilter\}'""", "'30'", sql)
        sql = re.sub(r"""['"]+\s*&\s*CurrencyFilter\s*&\s*['"]+""", "'30'", sql)
        return sql
