"""YAML of a metric view's ``parameters:`` block (see ``slicer_parameters``).

Only the parameters that a written measure reads are emitted: a parameter whose measure
was dropped by the emitter's validations would be an unused declaration.
"""

from __future__ import annotations

import re
from collections.abc import Iterable


def _quote(value: str) -> str:
    """A YAML single-quoted scalar (``'`` is written as ``''``)."""
    return "'" + value.replace("'", "''") + "'"


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
        lines.append(f"    default: {_quote(str(param['default']))}")
    lines.append("")
    return lines
