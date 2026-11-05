"""Dependency-safe deploy order for source-layer materialized views.

Source-layer views read base tables and snapshot tables. If one view's DDL
names another view of the same run, that one must be created first; the
check is by back-quoted object name so ``src_a`` never matches ``src_ab``.
"""

from __future__ import annotations


def _object_name(target: str) -> str:
    return target.rsplit(".", 1)[-1].strip("`")


def deploy_order(views: dict[str, tuple[str, str]]) -> list[str]:
    """Table names in creation order. ``views``: {table: (target, ddl)}.

    Raises ValueError when two views depend on each other.
    """
    names = {table: _object_name(target) for table, (target, _) in views.items()}
    deps: dict[str, set[str]] = {table: set() for table in views}
    for table, (_, ddl) in views.items():
        for other, name in names.items():
            if other != table and f"`{name}`" in ddl:
                deps[table].add(other)
    order: list[str] = []
    remaining = {table: set(d) for table, d in deps.items()}
    while remaining:
        ready = sorted(t for t, d in remaining.items() if not d)
        if not ready:
            raise ValueError(
                "circular dependency between source-layer views: "
                + ", ".join(sorted(remaining))
            )
        for table in ready:
            order.append(table)
            del remaining[table]
        for pending in remaining.values():
            pending.difference_update(ready)
    return order
