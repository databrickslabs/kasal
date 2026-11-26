"""The SQL executor contract shared by the source-layer actions.

A ``SqlFn`` runs one statement on the SQL warehouse and returns its result
(empty for DDL / DML). Tests pass fakes; the tool passes the real executor.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field


class SqlError(RuntimeError):
    """A statement failed on the warehouse; the message is the warehouse's."""


@dataclass(frozen=True)
class SqlResult:
    columns: list[str] = field(default_factory=list)
    rows: list[list[str | None]] = field(default_factory=list)


SqlFn = Callable[[str], SqlResult]
