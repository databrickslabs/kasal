"""YAML of a metric view's ``parameters:`` block (see ``slicer_parameters``).

Only the parameters that a written measure reads are emitted: a parameter whose measure
was dropped by the emitter's validations would be an unused declaration.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from .data_classes import MetricViewSpec, TranslationResult


def _quote(value: str) -> str:
    """A YAML single-quoted scalar (``'`` is written as ``''``)."""
    return "'" + value.replace("'", "''") + "'"


def _sql_default(value: str) -> str:
    """The default as Databricks wants it: a SQL string literal inside the YAML scalar
    (``default: "'MTD'"``). A bare YAML string is read as an expression and refused
    ("default value must be a constant expression")."""
    literal = "'" + value.replace("'", "''") + "'"
    return '"' + literal.replace("\\", "\\\\").replace('"', '\\"') + '"'


def insert_parameter_block(
    lines: list[str], at: int, spec: MetricViewSpec, written: Iterable[TranslationResult]
) -> None:
    """Insert, at line ``at``, the parameters block of the view: the parameters that the
    measures in ``written`` (those the emitter kept) read."""
    lines[at:at] = parameter_block_lines(
        spec.parameters, [m.sql_expr for m in written if m.sql_expr]
    )


def parameter_block_lines(
    parameters: list[dict], written_sql: Iterable[str]
) -> list[str]:
    """The ``parameters:`` block for the parameters read by ``written_sql``; empty when none."""
    sql_texts = [sql for sql in written_sql if sql]
    read = [
        p
        for p in parameters
        if any(re.search(rf"\b{re.escape(str(p['name']))}\b", sql) for sql in sql_texts)
    ]
    if not read:
        return []
    lines = ["parameters:"]
    for param in read:
        options = ", ".join(_quote(v) for v in param["allowed_values"])
        lines.append(
            f"  # {param['selector']}: allowed values {options}; "
            f"default {_quote(str(param['default']))}"
        )
        lines.append(f"  - name: {param['name']}")
        lines.append(f"    data_type: {param['data_type']}")
        lines.append(f"    default: {_sql_default(str(param['default']))}")
    lines.append("")
    return lines
