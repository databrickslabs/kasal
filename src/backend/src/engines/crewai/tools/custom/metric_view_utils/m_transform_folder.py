"""M Transform Folder — fold M steps into base SQL query."""
from __future__ import annotations

import re

from .data_classes import MStep


class MTransformFolder:
    """Fold Power BI M transform steps into the base SQL query."""

    def fold(self, base_sql: str, m_steps: list[MStep], pbi_columns: list) -> str:
        """Apply M transform steps to the base SQL."""
        if not m_steps:
            return base_sql

        select_rows = []
        column_transforms = []
        remove_columns = []
        rename_map = {}

        i = 0
        while i < len(m_steps):
            step = m_steps[i]
            if step.step_type == 'SelectRows':
                select_rows.append(step)
            elif step.step_type == 'ReplaceValue':
                column_transforms.append(step)
            elif step.step_type == 'DuplicateColumn':
                column_transforms.append(step)
            elif step.step_type == 'SplitColumn':
                column_transforms.append(step)
            elif step.step_type == 'RemoveColumns':
                remove_columns.append(step)
            elif step.step_type == 'RenameColumns':
                renames = re.findall(r'\{"([^"]+)",\s*"([^"]+)"\}', step.raw_expression)
                for old_name, new_name in renames:
                    rename_map[old_name] = new_name
            elif step.step_type == 'TransformColumnTypes':
                column_transforms.append(step)
            elif step.step_type == 'TransformColumns':
                column_transforms.append(step)
            elif step.step_type == 'AddColumn':
                column_transforms.append(step)
            i += 1

        if not select_rows and not column_transforms and not remove_columns:
            return base_sql

        where_conditions = []
        for step in select_rows:
            cond = self._parse_select_rows(step)
            if cond:
                where_conditions.append(cond)

        col_exprs = self._build_column_transforms(column_transforms, rename_map, remove_columns)

        if col_exprs:
            arms = self._split_union(base_sql)
            if len(arms) > 1:
                base_sql = self._apply_union_inline(arms, where_conditions, col_exprs, pbi_columns)
            else:
                base_sql = self._apply_with_wrapper(base_sql, where_conditions, col_exprs, pbi_columns)
        elif where_conditions:
            base_sql = self._apply_where_only(base_sql, where_conditions)

        return base_sql

    @staticmethod
    def _split_union(sql: str) -> list[str]:
        """Split SQL into UNION arms, preserving UNION ALL vs UNION."""
        arms = []
        current = []
        paren_depth = 0
        tokens = re.split(r'(\bunion\s+all\b|\bunion\b)', sql, flags=re.IGNORECASE)
        for token in tokens:
            token_stripped = token.strip().lower()
            if token_stripped in ('union', 'union all'):
                if paren_depth == 0:
                    arms.append(''.join(current).strip())
                    current = []
                    continue
            paren_depth += token.count('(') - token.count(')')
            current.append(token)
        if current:
            arms.append(''.join(current).strip())
        return [a for a in arms if a]

    def _apply_union_inline(self, arms: list[str], where_conditions: list,
                             col_exprs: dict, pbi_columns: list) -> str:
        """Inline M transforms into each UNION arm's SELECT list."""
        transformed_arms = []
        for arm in arms:
            arm = self._inline_transforms_into_arm(arm, col_exprs)
            if where_conditions:
                arm = self._inject_where_into_arm(arm, where_conditions)
            arm = self._clean_where_clause(arm)
            transformed_arms.append(arm)
        return '\nUNION\n'.join(transformed_arms)

    @staticmethod
    def _inject_where_into_arm(arm_sql: str, where_conditions: list[str]) -> str:
        """Inject WHERE conditions into a single UNION arm."""
        arm_lower = arm_sql.lower()
        effective = []
        for cond in where_conditions:
            m = re.match(r"(\w+)\s*(?:<>|!=)\s*'([^']+)'", cond.strip())
            if m:
                col = m.group(1).lower()
                if re.search(rf"\b{col}\s*=\s*'[^']+'", arm_lower):
                    continue
            effective.append(cond)
        if not effective:
            return arm_sql
        additional = ' AND '.join(effective)
        group_match = re.search(r'\bGROUP\s+BY\b', arm_sql, re.IGNORECASE)
        if group_match:
            before = arm_sql[:group_match.start()].rstrip()
            after = arm_sql[group_match.start():]
            if re.search(r'\bWHERE\b', before, re.IGNORECASE):
                return f"{before}\n  AND {additional}\n{after}"
            else:
                return f"{before}\nWHERE {additional}\n{after}"
        if re.search(r'\bWHERE\b', arm_sql, re.IGNORECASE):
            return f"{arm_sql}\n  AND {additional}"
        return f"{arm_sql}\nWHERE {additional}"

    @staticmethod
    def _clean_where_clause(arm_sql: str) -> str:
        """Post-process a UNION arm to clean up its WHERE clause."""
        eq_cols: set[str] = set()
        for m in re.finditer(r"\b(\w+)\s*=\s*'[^']+'", arm_sql):
            eq_cols.add(m.group(1).lower())

        if eq_cols:
            def _remove_redundant_neq(match: re.Match) -> str:
                col = match.group(1).lower()
                if col in eq_cols:
                    return ''
                return match.group(0)
            arm_sql = re.sub(
                r'\n\s*AND\s+\(?(\w+)\s*(?:<>|!=)\s*\'[^\']+\'\)?\s*(?=\n|$)',
                _remove_redundant_neq, arm_sql,
            )

        arm_sql = re.sub(
            r'(\bAND\s+)\((\w+\s*(?:<>|!=|=|>=|<=|>|<|LIKE|IN)\s*[^()]+)\)',
            r'\1\2', arm_sql,
        )
        return arm_sql

    @staticmethod
    def reformat_source_sql(sql: str) -> str:
        """Reformat the full source SQL for clean, compact output."""
        arms = re.split(r'\s*\bUNION\b\s*', sql, flags=re.IGNORECASE)
        formatted = [MTransformFolder._reformat_arm(a.strip()) for a in arms]
        if len(formatted) <= 1:
            return formatted[0] if formatted else sql
        parts = []
        for i, arm in enumerate(formatted):
            arm = re.sub(r'\n\s*GROUP\s+BY\s+ALL\s*$', '', arm)
            if i == 0:
                parts.append(arm)
            else:
                arm_body = re.sub(r'^SELECT\n', '', arm, flags=re.IGNORECASE)
                parts.append(arm_body)
        return '\nGROUP BY ALL UNION SELECT\n'.join(parts) + '\nGROUP BY ALL'

    @staticmethod
    def _reformat_arm(arm_sql: str) -> str:
        """Reformat a single SELECT…FROM…WHERE…GROUP BY arm."""
        arm_sql = arm_sql.strip()
        select_m = re.match(r'SELECT\s+(.*?)\s+FROM\s+', arm_sql, re.DOTALL | re.IGNORECASE)
        if not select_m:
            return arm_sql
        cols_text = select_m.group(1)
        rest = arm_sql[select_m.end():]

        table_m = re.match(r'(\S+)(.*)', rest, re.DOTALL)
        if not table_m:
            return arm_sql
        table_name = table_m.group(1)
        after_table = table_m.group(2).strip()
        has_group_by = bool(re.search(r'\bGROUP\s+BY\s+ALL\b', after_table, re.IGNORECASE))
        no_gb = re.sub(r'\s*GROUP\s+BY\s+ALL\s*$', '', after_table, flags=re.IGNORECASE).strip()
        where_text = ''
        if re.match(r'WHERE\b', no_gb, re.IGNORECASE):
            where_text = no_gb[5:].strip()

        cols = MTransformFolder._split_select_columns(cols_text)
        normalized: list[str] = []
        for c in cols:
            c = c.strip()
            if not c:
                continue
            if 'CASE' in c.upper() and 'END' in c.upper() and '\n' in c:
                case_lines = c.split('\n')
                fmt_lines: list[str] = []
                for cl in case_lines:
                    cl = cl.strip()
                    if not cl:
                        continue
                    if cl.upper().startswith(('WHEN ', 'ELSE ')):
                        fmt_lines.append(f'  {cl}')
                    else:
                        fmt_lines.append(cl)
                normalized.append('\n'.join(fmt_lines))
            else:
                normalized.append(' '.join(c.split()))
        cols = normalized
        col_lines: list[str] = []
        buf: list[str] = []
        for col in cols:
            is_simple = ('\n' not in col and '(' not in col
                         and 'CASE' not in col.upper()
                         and "'" not in col and len(col) < 30)
            if is_simple:
                buf.append(col)
            else:
                if buf:
                    col_lines.append(', '.join(buf))
                    buf = []
                col_lines.append(col)
        if buf:
            col_lines.append(', '.join(buf))

        where_fmt = ''
        if where_text:
            where_text = MTransformFolder._reorder_re_branches(where_text)
            where_fmt = MTransformFolder._compact_where(where_text)

        result = 'SELECT\n'
        formatted_cols: list[str] = []
        for line in col_lines:
            if '\n' in line:
                sub_lines = line.split('\n')
                formatted_cols.append('\n'.join(f'  {sl}' for sl in sub_lines))
            else:
                formatted_cols.append(f'  {line}')
        result += ',\n'.join(formatted_cols)
        if where_fmt:
            first_line, *rest_lines = where_fmt.split('\n')
            result += f'\nFROM {table_name} WHERE {first_line}'
            for rl in rest_lines:
                result += f'\n  {rl}'
        else:
            result += f'\nFROM {table_name}'
        if has_group_by:
            result += '\nGROUP BY ALL'
        return result

    @staticmethod
    def _compact_where(where_text: str) -> str:
        """Compact WHERE clause into clean lines split at top-level AND."""
        where_text = ' '.join(where_text.split())
        parts: list[str] = []
        depth = 0
        buf: list[str] = []
        i = 0
        while i < len(where_text):
            c = where_text[i]
            if c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
            if depth == 0 and where_text[i:i + 5].upper() == ' AND ' and buf:
                parts.append(''.join(buf).strip())
                buf = []
                i += 5
                continue
            buf.append(c)
            i += 1
        if buf:
            parts.append(''.join(buf).strip())

        if len(parts) <= 1:
            return where_text

        out_lines = [parts[0]]
        for p in parts[1:]:
            if p.startswith('(') and p.endswith(')') and ' OR ' in p:
                inner = p[1:-1].strip()
                or_parts = MTransformFolder._split_top_level_or(inner)
                if len(or_parts) > 1:
                    inline = f'AND {p}'
                    if len(inline) <= 120:
                        out_lines.append(inline)
                        continue
                    out_lines.append('AND (')
                    for j, op in enumerate(or_parts):
                        prefix = '  OR ' if j > 0 else '  '
                        out_lines.append(f'{prefix}{op.strip()}')
                    out_lines.append(')')
                    continue
            out_lines.append(f'AND {p}')
        return '\n'.join(out_lines)

    @staticmethod
    def _split_top_level_or(text: str) -> list[str]:
        """Split text at top-level OR (paren-aware)."""
        parts: list[str] = []
        depth = 0
        buf: list[str] = []
        i = 0
        while i < len(text):
            c = text[i]
            if c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
            if depth == 0 and text[i:i + 4].upper() == ' OR ' and buf:
                parts.append(''.join(buf).strip())
                buf = []
                i += 4
                continue
            buf.append(c)
            i += 1
        if buf:
            parts.append(''.join(buf).strip())
        return parts

    @staticmethod
    def _reorder_re_branches(where_text: str) -> str:
        """Reorder month-range OR branches descending (R100→R070→R040)."""
        i = 0
        while i < len(where_text):
            if where_text[i] == '(':
                depth = 1
                j = i + 1
                while j < len(where_text) and depth > 0:
                    if where_text[j] == '(':
                        depth += 1
                    elif where_text[j] == ')':
                        depth -= 1
                    j += 1
                inner = where_text[i + 1:j - 1]
                or_parts = MTransformFolder._split_top_level_or(inner.strip())
                if (len(or_parts) >= 2
                        and all(re.search(r'MONTH\(CURRENT_DATE\(\)\)', p, re.IGNORECASE) for p in or_parts)):
                    first_num = re.search(r'>=\s*(\d+)', or_parts[0])
                    last_num = re.search(r'>=\s*(\d+)', or_parts[-1])
                    if (first_num and last_num and int(first_num.group(1)) < int(last_num.group(1))):
                        or_parts = list(reversed(or_parts))
                        new_inner = ' OR '.join(p.strip() for p in or_parts)
                        where_text = where_text[:i + 1] + new_inner + where_text[j - 1:]
                        return where_text
                i = j
            else:
                i += 1
        return where_text

    def _inline_transforms_into_arm(self, arm_sql: str, col_exprs: dict) -> str:
        """Modify a single SELECT arm's column list to apply transforms inline."""
        remove_set = {k for k, v in col_exprs.items() if v is None}
        transform_exprs = {k: v for k, v in col_exprs.items() if v is not None}

        if not transform_exprs and not remove_set:
            return arm_sql

        select_match = re.match(r'(\s*SELECT\s+)(.*?)(\s+FROM\s+)', arm_sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return arm_sql

        prefix = select_match.group(1)
        select_body = select_match.group(2)
        from_onwards = arm_sql[select_match.end(2):]

        new_cols = []
        added_transforms = set()
        for col_str in self._split_select_columns(select_body):
            col_str = col_str.strip()
            if not col_str:
                continue
            col_name = self._get_column_name(col_str)
            if col_name and col_name.lower() in {r.lower() for r in remove_set}:
                continue
            if col_name and col_name in transform_exprs:
                new_cols.append(f'{transform_exprs[col_name]} AS {col_name}')
                added_transforms.add(col_name)
            else:
                new_cols.append(col_str)

        for col, expr in transform_exprs.items():
            if col not in added_transforms:
                new_entry = f'{expr} AS {col}'
                agg_idx = next(
                    (i for i, c in enumerate(new_cols)
                     if re.match(r'\s*(SUM|AVG|COUNT|MIN|MAX)\s*\(', c, re.IGNORECASE)),
                    None,
                )
                if agg_idx is not None:
                    new_cols.insert(agg_idx, new_entry)
                else:
                    new_cols.append(new_entry)

        new_select = ',\n'.join(new_cols)
        return f"{prefix}{new_select}{from_onwards}"

    @staticmethod
    def _split_select_columns(select_body: str) -> list[str]:
        """Split SELECT column list on commas, respecting parentheses."""
        cols = []
        current = []
        depth = 0
        for char in select_body:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            elif char == ',' and depth == 0:
                cols.append(''.join(current))
                current = []
                continue
            current.append(char)
        if current:
            cols.append(''.join(current))
        return cols

    @staticmethod
    def _get_column_name(col_str: str) -> str | None:
        """Extract column name from a SELECT expression."""
        col_str = col_str.strip().rstrip(',')
        as_match = re.search(r'\bAS\s+(\w+)\s*$', col_str, re.IGNORECASE)
        if as_match:
            return as_match.group(1)
        bare = col_str.split('.')[-1].strip()
        bare = re.sub(r'\W+$', '', bare)
        if re.match(r'^[a-zA-Z_]\w*$', bare):
            return bare
        return None

    def _parse_select_rows(self, step: MStep) -> str:
        """Parse Table.SelectRows filter condition to SQL."""
        expr = step.raw_expression
        each_match = re.search(r'each\s+(.+?)\s*\)\s*$', expr, re.DOTALL)
        if not each_match:
            return ''
        condition = each_match.group(1).strip()
        condition = re.sub(r'\[([^\]]+)\]', r'\1', condition)
        condition = condition.replace('"', "'")
        return condition

    def _build_column_transforms(self, transforms: list, rename_map: dict,
                                  remove_columns: list) -> dict:
        """Build a dict of column_name → SQL expression for transforms."""
        col_exprs: dict[str, str | None] = {}
        active_renames: dict[str, str] = dict(rename_map)
        dup_source = None

        remove_set: set[str] = set()
        for step in remove_columns:
            for m in re.finditer(r'"([^"]+)"', step.raw_expression.split('{', 1)[-1]):
                remove_set.add(m.group(1))

        for step in transforms:
            if step.step_type == 'ReplaceValue':
                m = re.search(
                    r'Table\.ReplaceValue\([^,]+,\s*(null|"[^"]*")\s*,\s*"([^"]*)"\s*,'
                    r'\s*Replacer\.(\w+)\s*,\s*\{"([^"]+)"\}',
                    step.raw_expression,
                )
                if m:
                    old_val = m.group(1)
                    new_val = m.group(2)
                    replacer_type = m.group(3)
                    col = m.group(4)
                    if old_val == 'null' and replacer_type == 'ReplaceValue':
                        if col in col_exprs and col_exprs[col] is not None:
                            col_exprs[col] = f"COALESCE({col_exprs[col]}, '{new_val}')"
                        else:
                            col_exprs[col] = f"COALESCE({col}, '{new_val}')"
                    elif replacer_type == 'ReplaceText':
                        old_str = old_val.strip('"')
                        col_exprs[col] = f"REPLACE({col}, '{old_str}', '{new_val}')"

            elif step.step_type == 'DuplicateColumn':
                m = re.search(r'Table\.DuplicateColumn\([^,]+,\s*"([^"]+)",\s*"([^"]+)"',
                              step.raw_expression)
                if m:
                    dup_source = m.group(1)

            elif step.step_type == 'SplitColumn' and dup_source:
                positions_match = re.search(
                    r'SplitTextByPositions\(\{([\d,\s]+)\}\)',
                    step.raw_expression,
                )
                names_match = re.search(r',\s*\{([^}]+)\}\s*\)\s*$', step.raw_expression)
                names = re.findall(r'"([^"]+)"', names_match.group(1)) if names_match else []
                if positions_match and names:
                    positions = [int(p.strip()) for p in positions_match.group(1).split(',')]
                    for i, name in enumerate(names):
                        final_name = active_renames.get(name, name)
                        start = positions[i] + 1
                        length = (positions[i + 1] - positions[i]) if (i + 1) < len(positions) else None
                        if name in active_renames:
                            if length:
                                col_exprs[final_name] = f"SUBSTRING({dup_source}, {start}, {length})"
                            else:
                                col_exprs[final_name] = f"SUBSTRING({dup_source}, {start})"
                        else:
                            remove_set.add(name)
                dup_source = None

            elif step.step_type == 'TransformColumnTypes':
                for m in re.finditer(r'\{"([^"]+)",\s*([\w.]+)\.Type\}', step.raw_expression):
                    col = m.group(1)
                    type_name = m.group(2)
                    sql_type = 'BIGINT' if type_name == 'Int64' else 'STRING'
                    if col in col_exprs and col_exprs[col] is not None:
                        col_exprs[col] = f"CAST({col_exprs[col]} AS {sql_type})"
                    else:
                        col_exprs[col] = f"CAST({col} AS {sql_type})"

            elif step.step_type == 'TransformColumns':
                m = re.search(
                    r'\{"([^"]+)",\s*each\s+if\s+Text\.StartsWith\(_,\s*"([^"]+)"\)\s+then\s+"([^"]+)"\s+else\s+_\}',
                    step.raw_expression,
                )
                if m:
                    col = m.group(1)
                    prefix = m.group(2)
                    replacement = m.group(3)
                    col_exprs[col] = f"CASE WHEN {col} LIKE '{prefix}%' THEN '{replacement}' ELSE {col} END"

        for col in remove_set:
            col_exprs[col] = None

        return col_exprs

    def _apply_with_wrapper(self, base_sql: str, where_conditions: list,
                             col_exprs: dict, pbi_columns: list) -> str:
        """Wrap base SQL in an outer SELECT with transforms applied."""
        base_cols = self._extract_select_columns(base_sql)
        if not base_cols:
            base_cols = [c['name'] for c in pbi_columns if c.get('columnType') != 'Calculated']

        remove_set = {k for k, v in col_exprs.items() if v is None}
        transform_exprs = {k: v for k, v in col_exprs.items() if v is not None}

        select_parts = []
        used_transforms = set()
        for col in base_cols:
            if col in remove_set or col.lower() in {r.lower() for r in remove_set}:
                continue
            if col in transform_exprs:
                select_parts.append(f"  {transform_exprs[col]} AS {col}")
                used_transforms.add(col)
            else:
                select_parts.append(f"  {col}")

        for col, expr in transform_exprs.items():
            if col not in used_transforms:
                select_parts.append(f"  {expr} AS {col}")

        select_clause = ',\n'.join(select_parts)

        where_clause = ''
        if where_conditions:
            where_clause = '\nWHERE ' + ' AND '.join(where_conditions)

        return f"SELECT\n{select_clause}\nFROM (\n{base_sql}\n) _src{where_clause}"

    def _apply_where_only(self, base_sql: str, where_conditions: list) -> str:
        """Append WHERE conditions to existing SQL."""
        additional = ' AND '.join(where_conditions)
        if re.search(r'\bGROUP\s+BY\s+ALL\b', base_sql, re.IGNORECASE):
            parts = re.split(r'(\bGROUP\s+BY\s+ALL\b)', base_sql, maxsplit=1, flags=re.IGNORECASE)
            if len(parts) >= 3:
                before = parts[0].rstrip()
                if re.search(r'\bWHERE\b', before, re.IGNORECASE):
                    return f"{before}\n AND {additional}\n{parts[1]}{parts[2]}"
                else:
                    return f"{before}\nWHERE {additional}\n{parts[1]}{parts[2]}"
        group_match = re.search(r'(\bGROUP\s+BY\b)', base_sql, re.IGNORECASE)
        if group_match:
            before = base_sql[:group_match.start()].rstrip()
            after = base_sql[group_match.start():]
            if re.search(r'\bWHERE\b', before, re.IGNORECASE):
                return f"{before}\n AND {additional}\n{after}"
            else:
                return f"{before}\nWHERE {additional}\n{after}"
        if re.search(r'\bWHERE\b', base_sql, re.IGNORECASE):
            return f"{base_sql}\n AND {additional}"
        return f"{base_sql}\nWHERE {additional}"

    @staticmethod
    def _extract_select_columns(sql: str) -> list[str]:
        """Extract column names from SELECT clause of SQL."""
        m = re.match(r'\s*SELECT\s+(.*?)\s+FROM\s+', sql, re.IGNORECASE | re.DOTALL)
        if not m:
            return []
        select_body = m.group(1)
        cols = []
        for part in select_body.split(','):
            part = part.strip()
            if not part:
                continue
            as_match = re.search(r'\bAS\s+(\w+)\s*$', part, re.IGNORECASE)
            if as_match:
                cols.append(as_match.group(1))
            else:
                col = part.split('.')[-1].strip()
                col = re.sub(r'\W+$', '', col)
                if re.match(r'^[a-zA-Z_]\w*$', col):
                    cols.append(col)
        return cols
