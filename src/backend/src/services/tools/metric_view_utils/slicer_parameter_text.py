"""Placeholder token, substitution and LLM prompt lines for selector parameters.

A selector read in a measure (``SELECTEDVALUE(T[c])``) is rewritten, before translation, to
the DAX string literal ``"__sel_<name>"``, where ``<name>`` is the view's parameter. A string
literal is what the translator's existing patterns already handle (a filter on a value is
``Table[col] = "..."``), so the literal flows through them unchanged. The SQL they produce
holds ``'__sel_<name>'``; ``substitute`` turns it into the bare parameter name. The LLM
reads the same literal and is told, by ``parameter_context_lines``, to write the bare name.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Iterable

TOKEN_PREFIX = "__sel_"

# A token with its optional quotes: 'x' / "x" / x. Quotes are dropped by the substitution.
_TOKEN_TEXT = re.compile(r"(['\"]?)__sel_(\w+)\1")


def token_for(name: str) -> str:
    """The DAX literal (quotes included) that stands for parameter ``name``."""
    return f'"{TOKEN_PREFIX}{name}"'


def token_names(text: str | None) -> set[str]:
    """The parameter names whose token is written in ``text`` (DAX or SQL)."""
    return set(re.findall(rf"{TOKEN_PREFIX}(\w+)", text or ""))


def substitute(sql: str | None, names: Collection[str]) -> str | None:
    """``sql`` with each token of a registered parameter replaced by the bare name.

    A token of a name not in ``names`` is left as it is, so a stray token stays visible
    to the caller's checks instead of being turned into a guessed identifier."""
    if not sql:
        return sql

    def _bare(match: re.Match[str]) -> str:
        name = match.group(2)
        return name if name in names else match.group(0)

    return _TOKEN_TEXT.sub(_bare, sql)


def parameter_context_lines(params: Iterable[dict]) -> list[str]:
    """Lines for the LLM's table context: the view's parameters and how to write them."""
    items = list(params)
    if not items:
        return []
    lines = [
        "- slicer parameters of this view (string constants, NOT columns): in the DAX the "
        'literal "__sel_<name>" stands for the parameter <name>. Write the bare name <name> '
        "in the SQL, unquoted and without a source. prefix."
    ]
    for param in items:
        values = ", ".join(f"'{v}'" for v in param.get("allowed_values") or [])
        lines.append(
            f"  {param['name']}: values {values or '(none listed)'}; "
            f"default '{param.get('default')}'"
        )
    return lines


def with_parameter_lines(context: str, params: Iterable[dict]) -> str:
    """``context`` with the parameter lines appended (unchanged when there are none)."""
    lines = parameter_context_lines(params)
    return context + "\n" + "\n".join(lines) if lines else context
