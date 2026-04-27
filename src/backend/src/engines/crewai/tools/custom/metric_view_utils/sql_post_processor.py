"""SQL Post-Processor — clean up generated inline SQL."""
from __future__ import annotations

import re

from .m_transform_folder import MTransformFolder


class SqlPostProcessor:
    """Post-process inline SQL: strip aliases, fix keywords, normalize formatting."""

    def __init__(self, unflatten_tables: bool = False):
        self.unflatten_tables = unflatten_tables

    def process(self, sql: str) -> str:
        """Apply all post-processing passes to SQL."""
        sql = self._strip_aliases(sql)
        sql = self._expand_nested_re_version_case(sql)
        sql = self._remove_dead_branches(sql)
        sql = self._fix_sql_keywords(sql)
        sql = self._remove_pbi_comments(sql)
        sql = self._clean_paren_whitespace(sql)
        sql = self._normalize_indentation(sql)
        if self.unflatten_tables:
            sql = self._unflatten_table_names(sql)
        if re.search(r'\bUNION\b', sql, re.IGNORECASE):
            sql = MTransformFolder.reformat_source_sql(sql)
        sql = self._clean_paren_whitespace(sql)
        return sql

    def _strip_aliases(self, sql: str) -> str:
        """Strip table alias prefixes from column references."""
        alias_pattern = re.compile(r'\bFROM\s+([\w.]+)\s+(?:AS\s+)?(\w+)\b', re.IGNORECASE)
        aliases_to_strip = set()
        sql_keywords = {'AS', 'ON', 'WHERE', 'GROUP', 'ORDER', 'HAVING',
                        'UNION', 'ALL', 'SELECT', 'FROM', 'JOIN', 'LEFT',
                        'RIGHT', 'INNER', 'OUTER', 'CROSS', 'FULL'}
        for m in alias_pattern.finditer(sql):
            alias = m.group(2)
            if alias.upper() not in sql_keywords:
                aliases_to_strip.add(alias)
        if not aliases_to_strip:
            return sql
        for alias in aliases_to_strip:
            sql = re.sub(rf'\b{re.escape(alias)}\.\s*(\w+)', r'\1', sql)
            sql = re.sub(
                rf'(\bFROM\s+[\w.]+)\s+(?:AS\s+)?{re.escape(alias)}\b',
                r'\1', sql, flags=re.IGNORECASE,
            )
        return sql

    @staticmethod
    def _expand_nested_re_version_case(sql: str) -> str:
        """Expand nested RE_Version CASE into multi-WHEN form."""
        pattern = re.compile(
            r"CASE\s+WHEN\s+(\w+)\s*=\s*"
            r"CASE\s+WHEN\s+MONTH\(CURRENT_DATE\(\)\)\s*>=\s*10\s+THEN\s+'R100'\s+"
            r"WHEN\s+MONTH\(CURRENT_DATE\(\)\)\s*>=\s*7\s+THEN\s+'R070'\s+"
            r"WHEN\s+MONTH\(CURRENT_DATE\(\)\)\s*>=\s*4\s+THEN\s+'R040'\s+"
            r"ELSE\s+'R000'\s*END\s+"
            r"THEN\s+'RE'\s+"
            r"ELSE\s+(\w+)\s*"
            r"END",
            re.IGNORECASE | re.DOTALL,
        )
        def _expand(m: re.Match) -> str:
            col = m.group(1)
            else_col = m.group(2)
            return (
                "CASE\n"
                f"    WHEN MONTH(CURRENT_DATE()) >= 10 AND {col} = 'R100' THEN 'RE'\n"
                f"    WHEN MONTH(CURRENT_DATE()) >= 7 AND MONTH(CURRENT_DATE()) < 10 AND {col} = 'R070' THEN 'RE'\n"
                f"    WHEN MONTH(CURRENT_DATE()) >= 4 AND MONTH(CURRENT_DATE()) < 7 AND {col} = 'R040' THEN 'RE'\n"
                f"    ELSE {else_col}\n"
                f"  END"
            )
        return pattern.sub(_expand, sql)

    @staticmethod
    def _remove_dead_branches(sql: str) -> str:
        """Remove dead SQL branches like (1 = 0 AND 1 = 0) from OR chains."""
        sql = re.sub(r'\s*OR\s+\(1\s*=\s*0(?:\s+AND\s+1\s*=\s*0)?\)', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\(1\s*=\s*0(?:\s+AND\s+1\s*=\s*0)?\)\s*OR\s+', '', sql, flags=re.IGNORECASE)
        return sql

    @staticmethod
    def _fix_sql_keywords(sql: str) -> str:
        """Normalize SQL keyword casing."""
        keywords = ['GROUP BY ALL', 'GROUP BY', 'ORDER BY', 'UNION ALL', 'UNION',
                    'LEFT JOIN', 'INNER JOIN',
                    'SELECT', 'FROM', 'WHERE', 'HAVING',
                    'AND', 'OR', 'ON', 'AS', 'IN', 'BETWEEN',
                    'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
                    'SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'CAST', 'COALESCE',
                    'CONCAT', 'LEFT', 'YEAR', 'MONTH', 'CURRENT_DATE', 'NOT', 'NULL',
                    'LIKE', 'IS', 'SUBSTRING', 'LENGTH', 'REPLACE', 'TRIM']
        for kw in keywords:
            pattern = re.compile(r'\b' + r'\s+'.join(re.escape(w) for w in kw.split()) + r'\b', re.IGNORECASE)
            sql = pattern.sub(kw, sql)
        return sql

    @staticmethod
    def _remove_pbi_comments(sql: str) -> str:
        """Remove PBI-specific comments."""
        sql = re.sub(r'--\s*Calculated Columns.*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'--\s*Static Table.*$', '', sql, flags=re.MULTILINE)
        return sql

    @staticmethod
    def _clean_paren_whitespace(sql: str) -> str:
        """Clean up whitespace around parentheses."""
        sql = re.sub(r'\(\s+', '(', sql)
        sql = re.sub(r'\s+\)', ')', sql)
        return sql

    @staticmethod
    def _normalize_indentation(sql: str) -> str:
        """Normalize indentation to 2 spaces."""
        lines = sql.split('\n')
        result = []
        for line in lines:
            stripped = line.lstrip()
            if not stripped:
                continue
            indent = len(line) - len(stripped)
            new_indent = (indent // 4) * 2 if indent > 0 else 0
            result.append(' ' * new_indent + stripped)
        return '\n'.join(result)

    @staticmethod
    def _unflatten_table_names(sql: str) -> str:
        """Unflatten table names: catalog.schema.cat__sch__tbl → cat.sch.tbl."""
        def _unflatten(m: re.Match) -> str:
            prefix = m.group(1)
            full = m.group(2)
            parts = full.split('.')
            if len(parts) == 3 and '__' in parts[2]:
                sub = parts[2].split('__')
                if len(sub) >= 3:
                    return prefix + '.'.join(sub)
            return m.group(0)
        sql = re.sub(r'(FROM\s+|JOIN\s+)([\w.]+)', _unflatten, sql, flags=re.IGNORECASE)
        return sql
