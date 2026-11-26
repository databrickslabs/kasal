"""M Transform Folder — fold M steps into base SQL query."""

from __future__ import annotations

import re

from .data_classes import MStep
from .dax_condition_sql import _sql_str_literal
from .m_column_expressions import (
    keep_one_row_per_key,
    quote_references,
    sql_ident,
    star_select_list,
)
from .m_sql_formatter import (
    _compact_where,
    _reformat_arm,
    _reorder_re_branches,
    _split_select_columns,
    _split_top_level_or,
    reformat_source_sql,
)
from .mquery_parser import strip_m_comments

# M transform steps that carry a deterministic column expression / filter.
_COLUMN_TRANSFORM_STEPS = frozenset(
    {
        "ReplaceValue",
        "ReplaceValueAccumulate",
        "DuplicateColumn",
        "SplitColumn",
        "TransformColumnTypes",
        "TransformColumns",
        "AddColumn",
    }
)
# Steps recognized structurally but NOT deterministically translatable to SQL.
# Table.Combine appends hard-coded records (needs VALUES the tool can't infer);
# Table.Group / Table.Distinct-on-a-key-subset change grain (a first-row pick is
# non-deterministic); joins need a partner table. Emitted as `-- TODO:` so the
# reviewer sees the step instead of it vanishing.
_UNTRANSLATABLE_STEPS = frozenset(
    {
        "Combine",
        "Group",
        "Pivot",
        "Unpivot",
        "Join",
        "NestedJoin",
        "AddIndexColumn",
        "FillDown",
        "FillUp",
        "Sort",
        "Buffer",
    }
)


def _todo_note(step: MStep) -> str:
    """Render an M step that could not be translated as a one-line SQL comment."""
    compact = " ".join(step.raw_expression.split())
    if len(compact) > 240:
        compact = compact[:237] + "..."
    return f"-- TODO: M step not translatable to SQL, apply manually: {compact}"


class MTransformFolder:
    """Fold Power BI M transform steps into the base SQL query."""

    def fold(self, base_sql: str, m_steps: list[MStep], pbi_columns: list) -> str:
        """Apply M transform steps to the base SQL.

        Deterministically translatable steps (``Table.SelectRows`` filters,
        ``Table.ReplaceValue`` / ``List.Accumulate`` code remaps, type casts) fold
        into the SQL. Any recognized-but-untranslatable step (appended rows,
        group-by, keyed dedup, cross-table filters) is NEVER dropped — it is
        recorded on ``self.transform_todos`` and appended to the returned SQL as a
        ``-- TODO:`` comment so it surfaces in the emitted source (S5)."""
        self.transform_todos: list[str] = []
        self._added_columns: set[str] = set()
        if not m_steps:
            return base_sql

        select_rows = []
        column_transforms = []
        remove_columns = []
        rename_map = {}
        distinct_all = False
        distinct_keys: list[str] = []

        i = 0
        while i < len(m_steps):
            step = m_steps[i]
            st = step.step_type
            if st == "SelectRows":
                select_rows.append(step)
            elif st in _COLUMN_TRANSFORM_STEPS:
                column_transforms.append(step)
            elif st == "RemoveColumns":
                remove_columns.append(step)
            elif st == "RenameColumns":
                renames = re.findall(r'\{"([^"]+)",\s*"([^"]+)"\}', step.raw_expression)
                for old_name, new_name in renames:
                    rename_map[old_name] = new_name
            elif st == "Distinct":
                # Full-row Table.Distinct(t) → SELECT DISTINCT; a keyed
                # Table.Distinct(t, {"col"}) keeps the first row per key (a
                # non-deterministic pick) → not translatable, emit a TODO.
                keyed = re.search(
                    r"Table\.Distinct\s*\([^,]+,\s*\{([^}]*)\}\s*\)",
                    step.raw_expression,
                )
                keys = re.findall(r'"([A-Za-z_]\w*)"', keyed.group(1)) if keyed else []
                if keys:
                    distinct_keys = keys
                elif re.search(r"Table\.Distinct\s*\([^,]+,", step.raw_expression):
                    self.transform_todos.append(_todo_note(step))
                else:
                    distinct_all = True
            elif st in _UNTRANSLATABLE_STEPS:
                self.transform_todos.append(_todo_note(step))
            elif st == "SelectColumns":
                # A projection — harmless to omit from the emitted source (the
                # metric view selects the columns it needs), but surface it so the
                # reviewer knows the PBI table was narrowed.
                self.transform_todos.append(_todo_note(step))
            elif st:
                # Unknown Table.* op — never silently drop it.
                self.transform_todos.append(_todo_note(step))
            i += 1

        where_conditions = []
        for step in select_rows:
            cond = self._parse_select_rows(step)
            if cond:
                where_conditions.append(cond)
            else:
                # A filter we could not lower to SQL (e.g. a List.Contains against
                # a buffer of another table's rows) — keep it as a TODO.
                self.transform_todos.append(_todo_note(step))

        col_exprs = quote_references(
            self._build_column_transforms(column_transforms, rename_map, remove_columns)
        )

        self._renames = dict(rename_map)
        self._duplicates = {
            new: src
            for step in column_transforms
            for src, new in re.findall(
                r'DuplicateColumn\([^,]+,\s*"([^"]+)"\s*,\s*"([^"]+)"',
                step.raw_expression,
            )
        }
        if col_exprs or rename_map:
            # Power Query steps act on the query's RESULT: over a UNION they wrap the whole
            # union. Writing them into the first arm only left the arms with different
            # columns (and a split/renamed column read where it does not exist).
            base_sql = self._apply_with_wrapper(
                base_sql, where_conditions, col_exprs, pbi_columns
            )
        elif where_conditions:
            base_sql = self._apply_where_only(base_sql, where_conditions)

        if distinct_all:
            base_sql = re.sub(
                r"^(\s*)SELECT\b",
                r"\1SELECT DISTINCT",
                base_sql,
                count=1,
                flags=re.IGNORECASE,
            )

        if distinct_keys:
            base_sql = keep_one_row_per_key(base_sql, distinct_keys)

        if self.transform_todos:
            base_sql = base_sql + "\n" + "\n".join(self.transform_todos)

        return base_sql

    def fold_from_mquery(
        self, base_sql: str, mquery: str, pbi_columns: list | None = None
    ) -> str:
        """Fold the transform steps of a raw Power Query M ``let`` block into SQL.

        The scan-data path already yields ``MStep`` objects; the config-generation
        path only has the raw M text (``table_mquery_expressions``). This parses the
        steps that follow the source out of the M and runs :meth:`fold`, so both
        paths carry the M transforms (S5) rather than only the source table.
        After it returns, ``self.transform_todos`` lists any untranslatable steps.
        """
        steps = self.parse_let_steps(mquery)
        return self.fold(base_sql, steps, pbi_columns or [])

    @staticmethod
    def parse_let_steps(mquery: str) -> list[MStep]:
        """Extract the transform steps from a Power Query M ``let ... in`` block.

        Comments are stripped first (a disabled ``// FilterExclude… = Table.SelectRows``
        line must not become a live filter). The source assignment and pure
        navigation/buffer/row-limit plumbing are skipped; each remaining
        ``Name = Table.<Op>(…)`` (or ``List.Accumulate`` remap) becomes an ``MStep``
        in author order. Returns ``[]`` when there is no ``let`` block."""
        if not mquery or not isinstance(mquery, str):
            return []
        m = strip_m_comments(mquery)
        lm = re.search(r"\blet\b", m)
        if not lm:
            return []
        body = MTransformFolder._strip_trailing_in(m[lm.end() :])
        steps: list[MStep] = []
        for part in MTransformFolder._split_top_level_commas(body):
            expr = part.strip()
            if not expr or "=" not in expr:
                continue
            rhs = expr.split("=", 1)[1].strip()
            step_type = MTransformFolder._classify_m_step(rhs)
            if step_type:
                steps.append(MStep(step_type=step_type, raw_expression=expr))
        return steps

    @staticmethod
    def _classify_m_step(rhs: str) -> str | None:
        """Map the right-hand side of an M assignment to an ``MStep`` type, or None
        to skip it (source / navigation / buffer / row-limit / result plumbing)."""
        # List.Accumulate that folds repeated Table.ReplaceValue over code pairs.
        if re.match(r"List\.Accumulate\b", rhs) and "Table.ReplaceValue" in rhs:
            return "ReplaceValueAccumulate"
        tm = re.match(r"Table\.(\w+)\s*\(", rhs)
        if tm:
            op = tm.group(1)
            if op in ("FirstN", "LastN", "Skip", "Range"):
                return None  # RowLimit plumbing, not a transform
            return op
        return None  # Source, navigation records, List.Buffer, `if RowLimit`, etc.

    @staticmethod
    def _strip_trailing_in(body: str) -> str:
        """Drop the final top-level ``in <result>`` clause from a ``let`` body."""
        depth = 0
        i = 0
        n = len(body)
        last_in = -1
        while i < n:
            c = body[i]
            if c == '"':
                i += 1
                while i < n and body[i] != '"':
                    i += 1
                i += 1
                continue
            if c in "([{":
                depth += 1
            elif c in ")]}":
                depth -= 1
            elif (
                depth == 0
                and body[i : i + 2] == "in"
                and (i == 0 or not body[i - 1].isalnum())
                and (i + 2 >= n or not body[i + 2].isalnum())
            ):
                last_in = i
            i += 1
        return body[:last_in] if last_in >= 0 else body

    @staticmethod
    def _split_top_level_commas(body: str) -> list[str]:
        """Split an M expression list on top-level commas (bracket + string aware)."""
        parts: list[str] = []
        buf: list[str] = []
        depth = 0
        i = 0
        n = len(body)
        while i < n:
            c = body[i]
            if c == '"':
                j = i + 1
                while j < n:
                    if body[j] == '"':
                        if j + 1 < n and body[j + 1] == '"':
                            j += 2
                            continue
                        j += 1
                        break
                    j += 1
                buf.append(body[i:j])
                i = j
                continue
            if c in "([{":
                depth += 1
            elif c in ")]}":
                depth -= 1
            if c == "," and depth == 0:
                parts.append("".join(buf))
                buf = []
                i += 1
                continue
            buf.append(c)
            i += 1
        if buf:
            parts.append("".join(buf))
        return parts

    @staticmethod
    def _split_union(sql: str) -> list[str]:
        """Split SQL into UNION arms, preserving UNION ALL vs UNION."""
        arms = []
        current: list[str] = []
        paren_depth = 0
        tokens = re.split(r"(\bunion\s+all\b|\bunion\b)", sql, flags=re.IGNORECASE)
        for token in tokens:
            token_stripped = token.strip().lower()
            if token_stripped in ("union", "union all"):
                if paren_depth == 0:
                    arms.append("".join(current).strip())
                    current = []
                    continue
            paren_depth += token.count("(") - token.count(")")
            current.append(token)
        if current:
            arms.append("".join(current).strip())
        return [a for a in arms if a]

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
        additional = " AND ".join(effective)
        group_match = re.search(r"\bGROUP\s+BY\b", arm_sql, re.IGNORECASE)
        if group_match:
            before = arm_sql[: group_match.start()].rstrip()
            after = arm_sql[group_match.start() :]
            if re.search(r"\bWHERE\b", before, re.IGNORECASE):
                return f"{before}\n  AND {additional}\n{after}"
            else:
                return f"{before}\nWHERE {additional}\n{after}"
        if re.search(r"\bWHERE\b", arm_sql, re.IGNORECASE):
            return f"{arm_sql}\n  AND {additional}"
        return f"{arm_sql}\nWHERE {additional}"

    @staticmethod
    def _clean_where_clause(arm_sql: str) -> str:
        """Post-process a UNION arm to clean up its WHERE clause."""
        eq_cols: set[str] = set()
        for m in re.finditer(r"\b(\w+)\s*=\s*'[^']+'", arm_sql):
            eq_cols.add(m.group(1).lower())

        if eq_cols:

            def _remove_redundant_neq(match: re.Match[str]) -> str:
                col = match.group(1).lower()
                if col in eq_cols:
                    return ""
                return match.group(0)

            arm_sql = re.sub(
                r"\n\s*AND\s+\(?(\w+)\s*(?:<>|!=)\s*\'[^\']+\'\)?\s*(?=\n|$)",
                _remove_redundant_neq,
                arm_sql,
            )

        arm_sql = re.sub(
            r"(\bAND\s+)\((\w+\s*(?:<>|!=|=|>=|<=|>|<|LIKE|IN)\s*[^()]+)\)",
            r"\1\2",
            arm_sql,
        )
        return arm_sql

    @staticmethod
    def reformat_source_sql(sql: str) -> str:
        """Reformat the full source SQL for clean, compact output."""
        return reformat_source_sql(sql)

    @staticmethod
    def _reformat_arm(arm_sql: str) -> str:
        """Reformat a single SELECT…FROM…WHERE…GROUP BY arm."""
        return _reformat_arm(arm_sql)

    @staticmethod
    def _compact_where(where_text: str) -> str:
        """Compact WHERE clause into clean lines split at top-level AND."""
        return _compact_where(where_text)

    @staticmethod
    def _split_top_level_or(text: str) -> list[str]:
        """Split text at top-level OR (paren-aware)."""
        return _split_top_level_or(text)

    @staticmethod
    def _reorder_re_branches(where_text: str) -> str:
        """Reorder month-range OR branches descending (R100→R070→R040)."""
        return _reorder_re_branches(where_text)

    def _inline_transforms_into_arm(self, arm_sql: str, col_exprs: dict) -> str:
        """Modify a single SELECT arm's column list to apply transforms inline."""
        remove_set = {k for k, v in col_exprs.items() if v is None}
        transform_exprs = {k: v for k, v in col_exprs.items() if v is not None}

        if not transform_exprs and not remove_set:
            return arm_sql

        select_match = re.match(
            r"(\s*SELECT\s+)(.*?)(\s+FROM\s+)", arm_sql, re.IGNORECASE | re.DOTALL
        )
        if not select_match:
            return arm_sql

        prefix = select_match.group(1)
        select_body = select_match.group(2)
        from_onwards = arm_sql[select_match.end(2) :]

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
                new_cols.append(f"{transform_exprs[col_name]} AS {col_name}")
                added_transforms.add(col_name)
            else:
                new_cols.append(col_str)

        for col, expr in transform_exprs.items():
            if col not in added_transforms:
                new_entry = f"{expr} AS {sql_ident(col)}"
                agg_idx = next(
                    (
                        i
                        for i, c in enumerate(new_cols)
                        if re.match(
                            r"\s*(SUM|AVG|COUNT|MIN|MAX)\s*\(", c, re.IGNORECASE
                        )
                    ),
                    None,
                )
                if agg_idx is not None:
                    new_cols.insert(agg_idx, new_entry)
                else:
                    new_cols.append(new_entry)

        new_select = ",\n".join(new_cols)
        return f"{prefix}{new_select}{from_onwards}"

    @staticmethod
    def _split_select_columns(select_body: str) -> list[str]:
        """Split SELECT column list on commas, respecting parentheses."""
        return _split_select_columns(select_body)

    @staticmethod
    def _get_column_name(col_str: str) -> str | None:
        """Extract column name from a SELECT expression."""
        col_str = col_str.strip().rstrip(",")
        as_match = re.search(r"\bAS\s+(\w+)\s*$", col_str, re.IGNORECASE)
        if as_match:
            return as_match.group(1)
        bare = col_str.split(".")[-1].strip()
        bare = re.sub(r"\W+$", "", bare)
        if re.match(r"^[a-zA-Z_]\w*$", bare):
            return bare
        return None

    def _parse_select_rows(self, step: MStep) -> str:
        """Parse a ``Table.SelectRows`` filter condition to a SQL predicate.

        Returns '' when the ``each`` predicate is not a plain column comparison —
        notably a ``List.Contains(buffer, [col])`` against a buffered list of
        another table's values, which has no deterministic SQL form (the caller
        then surfaces it as a TODO rather than emitting invalid SQL)."""
        expr = step.raw_expression
        each_match = re.search(r"each\s+(.+?)\s*\)\s*$", expr, re.DOTALL)
        if not each_match:
            return ""
        condition = each_match.group(1).strip()
        # M list/table/record calls in a filter (e.g. List.Contains against a
        # buffered column of another table) don't lower to a column predicate —
        # bail so the step becomes a TODO instead of malformed SQL.
        if re.search(r"\b(?:List|Table|Record)\.\w+", condition):
            return ""
        condition = re.sub(r"\[([^\]]+)\]", r"\1", condition)
        condition = condition.replace('"', "'")
        # M's [x] <> null is true for a non-null x; SQL's x <> NULL never is.
        condition = re.sub(
            r"(\w+)\s*<>\s*null\b", r"\1 IS NOT NULL", condition, flags=re.I
        )
        condition = re.sub(r"(\w+)\s*=\s*null\b", r"\1 IS NULL", condition, flags=re.I)
        return condition

    def _build_column_transforms(
        self, transforms: list, rename_map: dict, remove_columns: list
    ) -> dict:
        """Build a dict of column_name → SQL expression for transforms."""
        col_exprs: dict[str, str | None] = {}
        active_renames: dict[str, str] = dict(rename_map)
        dup_source = None

        remove_set: set[str] = set()
        for step in remove_columns:
            for rm in re.finditer(r'"([^"]+)"', step.raw_expression.split("{", 1)[-1]):
                remove_set.add(rm.group(1))

        added = self.__dict__.setdefault("_added_columns", set())
        for step in transforms:
            if step.step_type == "AddColumn":
                self._add_columns(step, col_exprs, added)
                continue
            if step.step_type == "ReplaceValue":
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
                    if old_val == "null" and replacer_type == "ReplaceValue":
                        if col in col_exprs and col_exprs[col] is not None:
                            col_exprs[col] = (
                                f"COALESCE({col_exprs[col]}, {_sql_str_literal(new_val)})"
                            )
                        else:
                            col_exprs[col] = (
                                f"COALESCE({col}, {_sql_str_literal(new_val)})"
                            )
                    elif replacer_type == "ReplaceText":
                        old_str = old_val.strip('"')
                        # Nest onto any prior expression for this column so chained
                        # replaces compose (Baltics→Estonia THEN Rep.of Ireland→
                        # Ireland) instead of the later one clobbering the earlier.
                        base = (
                            col_exprs[col]
                            if col in col_exprs and col_exprs[col] is not None
                            else col
                        )
                        col_exprs[col] = (
                            f"REPLACE({base}, {_sql_str_literal(old_str)}, "
                            f"{_sql_str_literal(new_val)})"
                        )

            elif step.step_type == "ReplaceValueAccumulate":
                # List.Accumulate({{"ROI","IE"},{"CR","HR"},…}, state,
                #   (s,c)=>Table.ReplaceValue(s, c{0}, c{1}, Replacer.ReplaceText, {"col"}))
                # = fold each code pair into a nested REPLACE() on the target column
                # (the classic "these codes differ between this table and the fact"
                # remap). Deterministic; PBI applies the pairs left-to-right.
                colm = re.search(
                    r'Replacer\.\w+\s*,\s*\{"([^"]+)"\}', step.raw_expression
                )
                pairs = re.findall(
                    r'\{\s*"([^"]*)"\s*,\s*"([^"]*)"\s*\}', step.raw_expression
                )
                if colm and pairs:
                    col = colm.group(1)
                    expr = (
                        col_exprs[col]
                        if col in col_exprs and col_exprs[col] is not None
                        else col
                    )
                    for old_code, new_code in pairs:
                        expr = (
                            f"REPLACE({expr}, {_sql_str_literal(old_code)}, "
                            f"{_sql_str_literal(new_code)})"
                        )
                    col_exprs[col] = expr

            elif step.step_type == "DuplicateColumn":
                m = re.search(
                    r'Table\.DuplicateColumn\([^,]+,\s*"([^"]+)",\s*"([^"]+)"',
                    step.raw_expression,
                )
                if m:
                    dup_source = m.group(1)

            elif step.step_type == "SplitColumn" and dup_source:
                positions_match = re.search(
                    r"SplitTextByPositions\(\{([\d,\s]+)\}",  # optional ", false" follows
                    step.raw_expression,
                )
                names_match = re.search(
                    r",\s*\{([^}]+)\}\s*\)\s*$", step.raw_expression
                )
                names = (
                    re.findall(r'"([^"]+)"', names_match.group(1))
                    if names_match
                    else []
                )
                if positions_match and names:
                    positions = [
                        int(p.strip()) for p in positions_match.group(1).split(",")
                    ]
                    for i, name in enumerate(names):
                        final_name = active_renames.get(name, name)
                        start = positions[i] + 1
                        length = (
                            (positions[i + 1] - positions[i])
                            if (i + 1) < len(positions)
                            else None
                        )
                        if name in active_renames:
                            if length:
                                col_exprs[final_name] = (
                                    f"SUBSTRING({dup_source}, {start}, {length})"
                                )
                            else:
                                col_exprs[final_name] = (
                                    f"SUBSTRING({dup_source}, {start})"
                                )
                        else:
                            remove_set.add(name)
                dup_source = None

            elif step.step_type == "TransformColumnTypes":
                for m in re.finditer(
                    r'\{"([^"]+)",\s*([\w.]+)\.Type\}', step.raw_expression
                ):
                    col = m.group(1)
                    # A split part renamed later (Year.1 -> Year) is cast under its final name.
                    if active_renames.get(col) in col_exprs:
                        col = active_renames[col]
                    type_name = m.group(2)
                    sql_type = "BIGINT" if type_name == "Int64" else "STRING"
                    if col in col_exprs and col_exprs[col] is not None:
                        col_exprs[col] = f"CAST({col_exprs[col]} AS {sql_type})"
                    else:
                        col_exprs[col] = f"CAST({col} AS {sql_type})"

            elif step.step_type == "TransformColumns":
                # The entry may carry a type after the function: {"c", each ..., type text}.
                m = re.search(
                    r'\{"([^"]+)",\s*each\s+if\s+Text\.StartsWith\(_,\s*"([^"]+)"\)\s+then\s+"([^"]+)"\s+else\s+_\s*(?:,\s*[\w. ]+)?\}',
                    step.raw_expression,
                )
                if m:
                    col = m.group(1)
                    prefix = m.group(2)
                    replacement = m.group(3)
                    base = col_exprs.get(col) or col
                    # M text inside SQL quotes: escaped literals, never raw '{v}'.
                    col_exprs[col] = (
                        f"CASE WHEN {base} LIKE {_sql_str_literal(prefix + '%')} "  # literals escaped — nosemgrep: tmp.databricks.sql-injection.fstring-like
                        f"THEN {_sql_str_literal(replacement)} ELSE {base} END"
                    )
                else:  # a value transform not reproduced: named, never dropped silently
                    self.__dict__.setdefault("transform_todos", []).append(
                        _todo_note(step)
                    )

        for col in remove_set:
            col_exprs[col] = None

        return col_exprs

    def _add_columns(
        self, step: MStep, col_exprs: dict[str, str | None], added: set[str]
    ) -> None:
        """``Table.AddColumn``: a new column from an ``each`` expression. One that does
        not translate is a TODO, never dropped silently."""
        from .generated_table_emitter import _find_addcols
        from .m_column_expressions import m_column_expression_to_sql

        found = _find_addcols(step.raw_expression)
        for name, body in found:
            sql = m_column_expression_to_sql(body)
            if sql is None or not re.match(r"^[A-Za-z_]\w*$", name):
                self.__dict__.setdefault("transform_todos", []).append(_todo_note(step))
                return
            col_exprs[name] = sql
            added.add(name)
        if not found:
            self.__dict__.setdefault("transform_todos", []).append(_todo_note(step))

    def _apply_with_wrapper(
        self, base_sql: str, where_conditions: list, col_exprs: dict, pbi_columns: list
    ) -> str:
        """Wrap base SQL in an outer SELECT with transforms applied."""
        base_cols = self._extract_select_columns(base_sql)
        if not base_cols:
            base_cols = [
                c["name"]
                for c in pbi_columns
                if isinstance(c, dict) and c.get("columnType") != "Calculated"
            ]

        remove_set = {k for k, v in col_exprs.items() if v is None}
        transform_exprs = {k: v for k, v in col_exprs.items() if v is not None}
        if not base_cols:
            # A SELECT * read whose columns are unknown. An explicit column list cannot
            # be built (empty is invalid SQL, transforms alone drop every other column),
            # so keep all columns and replace or remove only the ones the steps name.
            # A step that names a column the source lacks then fails on deploy, by name.
            select_list = star_select_list(
                remove_set,
                transform_exprs,
                self.__dict__.get("_renames") or {},
                self.__dict__.get("_added_columns") or set(),
                self.__dict__.get("_duplicates") or {},
            )
            where_clause = (
                "\nWHERE " + " AND ".join(where_conditions) if where_conditions else ""
            )
            return f"SELECT\n{select_list}\nFROM (\n{base_sql}\n) _src{where_clause}"  # composes built parts; deployed only via the metric-view deployer's SQL check — nosemgrep: tmp.databricks.sql-injection.fstring-select

        select_parts = []
        used_transforms = set()
        for col in base_cols:
            if col in remove_set or col.lower() in {r.lower() for r in remove_set}:
                continue
            renamed = (self.__dict__.get("_renames") or {}).get(col)
            if col in transform_exprs:
                select_parts.append(f"  {transform_exprs[col]} AS {sql_ident(col)}")
                used_transforms.add(col)
            elif renamed and renamed not in transform_exprs:
                select_parts.append(f"  {col} AS {renamed}")
            else:
                select_parts.append(f"  {sql_ident(col)}")

        for col, expr in transform_exprs.items():
            if col not in used_transforms:
                select_parts.append(f"  {expr} AS {sql_ident(col)}")

        select_clause = ",\n".join(select_parts)

        where_clause = ""
        if where_conditions:
            where_clause = "\nWHERE " + " AND ".join(where_conditions)

        return f"SELECT\n{select_clause}\nFROM (\n{base_sql}\n) _src{where_clause}"  # composes built parts; deployed only via the metric-view deployer's SQL check — nosemgrep: tmp.databricks.sql-injection.fstring-select

    def _apply_where_only(self, base_sql: str, where_conditions: list) -> str:
        """Append WHERE conditions to existing SQL."""
        additional = " AND ".join(where_conditions)
        if re.search(r"\bGROUP\s+BY\s+ALL\b", base_sql, re.IGNORECASE):
            parts = re.split(
                r"(\bGROUP\s+BY\s+ALL\b)", base_sql, maxsplit=1, flags=re.IGNORECASE
            )
            if len(parts) >= 3:
                before = parts[0].rstrip()
                if re.search(r"\bWHERE\b", before, re.IGNORECASE):
                    return f"{before}\n AND {additional}\n{parts[1]}{parts[2]}"
                else:
                    return f"{before}\nWHERE {additional}\n{parts[1]}{parts[2]}"
        group_match = re.search(r"(\bGROUP\s+BY\b)", base_sql, re.IGNORECASE)
        if group_match:
            before = base_sql[: group_match.start()].rstrip()
            after = base_sql[group_match.start() :]
            if re.search(r"\bWHERE\b", before, re.IGNORECASE):
                return f"{before}\n AND {additional}\n{after}"
            else:
                return f"{before}\nWHERE {additional}\n{after}"
        if re.search(r"\bWHERE\b", base_sql, re.IGNORECASE):
            return f"{base_sql}\n AND {additional}"
        return f"{base_sql}\nWHERE {additional}"

    @staticmethod
    def _extract_select_columns(sql: str) -> list[str]:
        """Extract column names from SELECT clause of SQL."""
        m = re.match(r"\s*SELECT\s+(.*?)\s+FROM\s+", sql, re.IGNORECASE | re.DOTALL)
        if not m:
            return []
        select_body = re.sub(r"^\s*(DISTINCT|ALL)\s+", "", m.group(1), flags=re.I)
        if any(
            p.strip() == "*" or p.strip().endswith(".*") for p in select_body.split(",")
        ):
            return []  # SELECT * : the columns are not known
        cols = []
        for part in select_body.split(","):
            part = part.strip()
            if not part:
                continue
            as_match = re.search(r"\bAS\s+(\w+)\s*$", part, re.IGNORECASE)
            if as_match:
                cols.append(as_match.group(1))
            else:
                col = part.split(".")[-1].strip()
                if re.match(r"^`[^`]+`$", col):
                    cols.append(col[1:-1])  # a quoted name (AC_UC_%age)
                    continue
                col = re.sub(r"\W+$", "", col)
                if re.match(r"^[a-zA-Z_]\w*$", col):
                    cols.append(col)
        return cols
