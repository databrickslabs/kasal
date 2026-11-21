"""The order of a metric view's measures: a measure after the measures it reads.

A metric view resolves ``MEASURE(x)`` only against measures defined BEFORE the one that reads
it. A measure listed ahead of one it reads fails to deploy with "x cannot be resolved",
although x is in the view (checked on a warehouse). The translation order is not a dependency
order: a ratio can be translated before its numerator.

``order_by_dependencies`` moves each measure behind the measures of the same list it reads
and keeps the given order otherwise. Measures in a cycle keep their place.

Pure function: no I/O.
"""

from __future__ import annotations

import re

_MEASURE_REF = re.compile(r"\bMEASURE\(\s*`?(\w+)`?\s*\)", re.IGNORECASE)


def order_by_dependencies(measures: list) -> None:
    """Reorder ``measures`` (objects with ``measure_name`` and ``sql_expr``) in place."""
    names = [str(getattr(m, "measure_name", "")).lower() for m in measures]
    position = {name: i for i, name in enumerate(names)}
    reads = [
        {
            position[ref.lower()]
            for ref in _MEASURE_REF.findall(str(getattr(m, "sql_expr", "") or ""))
            if ref.lower() in position and position[ref.lower()] != i
        }
        for i, m in enumerate(measures)
    ]
    if not any(any(dep > i for dep in deps) for i, deps in enumerate(reads)):
        return  # nothing reads a later measure
    placed: list[int] = []
    done: set[int] = set()
    pending = list(range(len(measures)))
    while pending:
        ready = [i for i in pending if reads[i] <= done]
        if not ready:
            placed += pending  # a cycle: left as it is
            break
        first = ready[0]
        placed.append(first)
        done.add(first)
        pending.remove(first)
    measures[:] = [measures[i] for i in placed]
