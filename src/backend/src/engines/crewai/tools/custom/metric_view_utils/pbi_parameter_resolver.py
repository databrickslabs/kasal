"""PBI Parameter Resolver — resolve Power BI parameters in native SQL."""
from __future__ import annotations

import re


class PbiParameterResolver:
    """Resolve Power BI parameters (FiscperFilter, RE_Version, CurrencyFilter)
    in native SQL extracted from scan data or MQuery transpiled SQL."""

    def __init__(self, parameter_defaults: dict | None = None):
        self._defaults = parameter_defaults or {}

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

    @property
    def _re_version_case(self):
        """RE_Version CASE expression — overridable via config."""
        return self._defaults.get('RE_Version_CASE', self._RE_VERSION_CASE)

    # Unresolved Power BI parameter interpolation, e.g.  " & FiscperFilter & "
    # or  '${FiscperFilter}' , left over after the known-param resolvers ran.
    # Emitting these into SQL produces invalid output (the report's pe002 case).
    _UNRESOLVED_PARAM = re.compile(
        r"""['"]*\s*&\s*(\w+)\s*&\s*['"]*|'\$\{(\w+)\}'""")

    def resolve(self, sql: str) -> str:
        """Apply all PBI parameter resolutions to SQL."""
        sql = self._resolve_fiscper_filter(sql)
        sql = self._resolve_re_version(sql)
        sql = self._resolve_currency_filter(sql)
        return sql

    def find_unresolved_params(self, sql: str) -> list[str]:
        """Return the names of any PBI parameters still interpolated in ``sql``
        after ``resolve()``. Empty list means the SQL is parameter-clean."""
        names: list[str] = []
        for m in self._UNRESOLVED_PARAM.finditer(sql or ""):
            name = m.group(1) or m.group(2)
            if name and name not in names:
                names.append(name)
        return names

    def _resolve_fiscper_filter(self, sql: str) -> str:
        """Collapse FiscperFilter CASE expressions.

        Only applied when ``resolve_fiscper_filter`` is True (default) in the
        parameter defaults.  Non-SAP deployments that lack FiscperFilter can
        set this to False and their SQL will pass through unchanged.
        """
        if not self._defaults.get('resolve_fiscper_filter', True):
            return sql
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
        """Resolve RE_Version parameter in two contexts.

        If ``RE_Version_ranges`` is not configured, RE_Version references are
        left as-is so non-SAP deployments are not affected.
        """
        re_version_ranges = self._defaults.get('RE_Version_ranges')
        if not re_version_ranges:
            return sql  # Not configured — leave RE_Version references as-is
        re_version_case = self._re_version_case
        for version_code, month_range in re_version_ranges.items():
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
        sql = re.sub(r"""'\$\{RE_Version\}'""", re_version_case, sql)
        sql = re.sub(r"""['"]+\s*&\s*RE_Version\s*&\s*['"]+""", re_version_case, sql)
        return sql

    def _resolve_currency_filter(self, sql: str) -> str:
        """Resolve CurrencyFilter parameter to configured default.

        If ``CurrencyFilter`` is not present in the defaults, references are
        left as-is.
        """
        currency_val = self._defaults.get('CurrencyFilter')
        if currency_val is None:
            return sql  # Not configured — leave CurrencyFilter references as-is
        sql = re.sub(r"""'\$\{CurrencyFilter\}'""", f"'{currency_val}'", sql)
        sql = re.sub(r"""['"]+\s*&\s*CurrencyFilter\s*&\s*['"]+""", f"'{currency_val}'", sql)
        return sql
