"""Measure arithmetic: a measure whose DAX is only arithmetic over other measures of the same
view becomes ``MEASURE()`` references (``[A] - [B]`` -> ``MEASURE(a) - MEASURE(b)``).

It runs in ``table_processor.process_table`` twice: before the LLM step (the measures the
rules can already resolve) and again after everything that translates measures has run
(the LLM, the SWITCH decompositions). A measure the LLM declines because a measure it reads
was not in the list yet, while that measure is in the view by the end, is resolved by the
second run. Each run repeats until no measure is resolved.

A measure is resolved only when every measure it reads is translated in this view and its
SQL passes the DAX-only check; the silent-wrong guards in ``sql_measure_sanitizer`` run later
on the emitted measures and are not touched here.
"""

from __future__ import annotations

import re
from collections.abc import Callable

from .data_classes import TranslationResult
from .utils import to_snake_case

# (start, end, numerator, denominator) of a DIVIDE(...) call, as dax_var_chain reports it.
DivideArgs = tuple[int, int, str, str]

# DAX-only constructs: a measure that still has one after the rewrite is not SQL.
_DAX_ONLY = re.compile(
    r"\b(SELECTEDVALUE|ISFILTERED|HASONEVALUE|FORMAT|CONTAINSSTRING|"
    r"SWITCH|CALCULATE\s*\(|SAMEPERIODLASTYEAR|DATEADD|"
    r"FIRSTDATE|LASTDATE|EARLIER|VALUES|SUMX|SUMMARIZE|"
    r"var\s+\w+\s*=|return\s)\b",
    re.IGNORECASE,
)
_PRE_CLEAN_LEAVE_OUT = re.compile(
    r"^var\s+\w+\s*=\s*CALCULATE\s*\(\s*\[(F_Start_date|F_End_date|PY_Start_date|PY_End_date)\]",
    re.IGNORECASE,
)
_PRE_CLEAN_DATE_VAR = re.compile(r"^var\s+(std|etd)\b", re.IGNORECASE)
_BODY_LEAVE_OUT = re.compile(r"^var\s+(std|etd|x|y)\b", re.IGNORECASE)
_BODY_CALC_LEAVE_OUT = re.compile(
    r"^var\s+\w+\s*=\s*CALCULATE\s*\(\s*\[(F_Start|F_End|PY_Start|PY_End)",
    re.IGNORECASE,
)
_VAR_LINE = re.compile(r"^var\s+(\w+)\s*=\s*(.+)$", re.IGNORECASE)
_SAME_PERIOD_WRAPPER = re.compile(r",\s*SAMEPERIODLASTYEAR\([^)]+\)", re.IGNORECASE)
_BARE_CALCULATE = re.compile(r"\bCALCULATE\(\s*(MEASURE\([^)]+\))\s*\)", re.IGNORECASE)
_ISBLANK_GUARD = re.compile(
    r"\bIF\s*\(\s*ISBLANK\s*\(\s*(MEASURE\([^)]+\))\s*\)\s*,\s*0\s*,\s*\1\s*\)",
    re.IGNORECASE,
)
_CALC_SUM = re.compile(
    r"\bCALCULATE\s*\(\s*SUM\s*\(\s*\w+\[(\w+)\]\s*\)\s*(?:\*\s*(\d+)\s*)?\)"
    r"(?:\s*\*\s*(\d+))?",
    re.IGNORECASE,
)
_BARE_SUM = re.compile(r"\bSUM\s*\(\s*\w+\[(\w+)\]\s*\)", re.IGNORECASE)
_TABLE_COLUMN = re.compile(r"\b\w+\[(\w+)\]")


def _pre_clean(dax_expr: str) -> str:
    """The DAX without comment lines and without the date-variable lines."""
    kept: list[str] = []
    for line in dax_expr.split("\n"):
        stripped = line.strip()
        if stripped.startswith("//"):
            continue
        if _PRE_CLEAN_LEAVE_OUT.match(stripped) or _PRE_CLEAN_DATE_VAR.match(stripped):
            continue
        kept.append(stripped)
    return "\n".join(kept)


def _standalone_refs(dax_clean: str) -> set[str]:
    """``[Ref]`` names that are measures (not ``Table[col]``)."""
    refs: set[str] = set()
    for ref_match in re.finditer(r"\[([^\]]+)\]", dax_clean):
        before = dax_clean[: ref_match.start()].rstrip()
        if before and re.search(r"\w$", before):
            continue  # Table[col] -- skip
        refs.add(ref_match.group(1))
    return refs


def _expression_body(dax_expr: str) -> str:
    """The result expression with its variable lines (kept, as in the chain resolver)
    stripped of comments, the RETURN keyword and the date-variable lines."""
    expr_lines: list[str] = []
    for line in dax_expr.split("\n"):
        s = line.strip()
        if not s or s.startswith("//"):
            continue
        if _BODY_LEAVE_OUT.match(s) or _BODY_CALC_LEAVE_OUT.match(s):
            continue
        if _VAR_LINE.match(s):
            expr_lines.append(s)
            continue
        expr_lines.append(s)  # RETURN stays: the chain resolver substitutes into it
    return " ".join(item.strip() for item in expr_lines if item.strip())


_RETURN_LINE = re.compile(r"^\s*return\s+(.*)$", re.IGNORECASE | re.DOTALL)


def _result_only(resolved: str) -> str | None:
    """The RETURN expression of a resolved ``VAR ... RETURN`` block, None when it still
    reads a variable (one the resolver does not inline: a CALCULATE or an iterator).

    Without this the variable lines were emitted as they stood
    (``var ret = MEASURE(x) ret``), which is not SQL."""
    lines = [line.strip() for line in resolved.split("\n") if line.strip()]
    names = [m.group(1) for line in lines if (m := _VAR_LINE.match(line))]
    if not names:
        only = _RETURN_LINE.match(resolved.strip())
        return only.group(1).strip() if only else resolved
    for line in lines:
        result = _RETURN_LINE.match(line)
        if result:
            text = result.group(1).strip()
            if any(re.search(rf"\b{re.escape(n)}\b", text) for n in names):
                return None
            return text
    return None


def _if_blank_to_case(expr: str) -> str:
    """``IF(ISBLANK(MEASURE(x)), BLANK(), e)`` -> ``CASE WHEN MEASURE(x) IS NULL THEN NULL
    ELSE (e) END``: ``ISBLANK`` is not SQL. Only the first such guard is rewritten per call;
    anything else is returned as it is."""
    match = re.search(
        r"\bIF\s*\(\s*ISBLANK\s*\(\s*(MEASURE\([^()]*\))\s*\)\s*,\s*(?:NULL|BLANK\(\))\s*,\s*",
        expr,
        re.IGNORECASE,
    )
    if match is None:
        return expr
    depth = 1
    index = match.end()
    while index < len(expr) and depth:
        if expr[index] == "(":
            depth += 1
        elif expr[index] == ")":
            depth -= 1
        index += 1
    if depth:
        return expr
    inner = expr[match.end() : index - 1]
    head = f"CASE WHEN {match.group(1)} IS NULL THEN NULL ELSE ({inner}) END"
    return _if_blank_to_case(expr[: match.start()] + head + expr[index:])


def _to_sql(
    expr: str,
    measure_refs: set[str],
    original_to_snake: dict[str, str],
    resolve_var_chain_fn: Callable[[str], str],
    extract_divide_args_fn: Callable[[str], DivideArgs | None],
) -> str | None:
    """``expr`` (DAX over measures of this view) as SQL over ``MEASURE()``; None when a
    DAX-only construct remains."""
    result = _result_only(resolve_var_chain_fn(expr))
    if result is None:
        return None
    expr = result
    for ref_name in sorted(measure_refs, key=len, reverse=True):
        snake = original_to_snake.get(ref_name, to_snake_case(ref_name))
        expr = expr.replace(f"[{ref_name}]", f"MEASURE({snake})")
    while "DIVIDE(" in expr.upper():
        div_args = extract_divide_args_fn(expr)
        if not div_args:
            break
        d_start, d_end, num, den = div_args
        expr = (
            expr[:d_start] + f"{num.strip()} / NULLIF({den.strip()}, 0)" + expr[d_end:]
        )
    expr = re.sub(r"\bBLANK\(\)", "NULL", expr, flags=re.IGNORECASE)
    expr = _ISBLANK_GUARD.sub(r"\1", expr)
    expr = _if_blank_to_case(expr)
    expr = _SAME_PERIOD_WRAPPER.sub("", expr)  # before the CALCULATE strip
    expr = _BARE_CALCULATE.sub(r"\1", expr)
    expr = _CALC_SUM.sub(_calc_sum_replacement, expr)
    expr = _BARE_SUM.sub(r"SUM(source.\1)", expr)
    expr = _TABLE_COLUMN.sub(r"source.\1", expr)
    if _DAX_ONLY.search(re.sub(r"MEASURE\s*\([^)]*\)", "", expr)):
        return None
    return expr


def _calc_sum_replacement(match: re.Match[str]) -> str:
    sql = f"SUM(source.{match.group(1)})"
    if match.group(2):
        sql += f" * {match.group(2)}"
    if match.group(3):
        sql += f" * {match.group(3)}"
    return sql


def _resolve_one(
    m: TranslationResult,
    original_to_snake: dict[str, str],
    base_names: set[str],
    resolve_var_chain_fn: Callable[[str], str],
    extract_divide_args_fn: Callable[[str], DivideArgs | None],
) -> bool:
    """Set ``m``'s SQL when every measure it reads is in this view; True when it did."""
    refs = _standalone_refs(_pre_clean(m.dax_expression))
    if not refs or not all(
        original_to_snake.get(r, to_snake_case(r)) in base_names for r in refs
    ):
        return False
    sql = _to_sql(
        _expression_body(m.dax_expression),
        refs,
        original_to_snake,
        resolve_var_chain_fn,
        extract_divide_args_fn,
    )
    if sql is None:
        return False
    m.sql_expr = sql
    m.is_translatable = True
    m.skip_reason = ""
    m.category = "measure_arithmetic"
    return True


def resolve_measure_arithmetic(
    translated: list[TranslationResult],
    untranslatable: list[TranslationResult],
    base_names: set[str],
    original_to_snake: dict[str, str],
    topo_priority: dict[str, int],
    *,
    resolve_var_chain_fn: Callable[[str], str],
    extract_divide_args_fn: Callable[[str], DivideArgs | None],
) -> tuple[list[TranslationResult], list[TranslationResult]]:
    """Resolve the untranslatable measures that are arithmetic over measures of this view.

    Returns ``(translated, untranslatable)``: the resolved measures are appended to
    ``translated`` and removed from ``untranslatable``. ``base_names`` and
    ``original_to_snake`` are updated in place with each resolved measure.
    """
    for m in translated:
        original_to_snake[m.original_name] = m.measure_name
    for _iteration in range(5):
        resolved: list[TranslationResult] = []
        still: list[TranslationResult] = []
        for m in sorted(
            untranslatable, key=lambda x: topo_priority.get(x.original_name, 999)
        ):
            if _resolve_one(
                m,
                original_to_snake,
                base_names,
                resolve_var_chain_fn,
                extract_divide_args_fn,
            ):
                resolved.append(m)
            else:
                still.append(m)
        if not resolved:
            break  # no progress
        for m in resolved:
            original_to_snake[m.original_name] = m.measure_name
            base_names.add(m.measure_name)
        translated.extend(resolved)
        untranslatable = still
    return translated, untranslatable
