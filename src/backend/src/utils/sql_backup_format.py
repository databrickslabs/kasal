"""The PostgreSQL ``.sql`` backup Kasal writes, and the only statements it restores.

``DatabaseBackupRepository`` exports PostgreSQL to a ``.sql`` file on a
Databricks volume and restores from one. The restore used to execute every
statement in the file verbatim, so anyone able to write to that volume could run
arbitrary SQL as the app's database role the next time an admin restored. It now
accepts exactly the shapes ``create_postgres_backup`` writes:

* ``SET <one of the dump's settings> = <value>``
* ``ALTER TABLE <t> DISABLE|ENABLE TRIGGER ALL``
* ``DELETE FROM <t>``
* ``INSERT INTO <t> (<cols>) VALUES (<v>, ...)`` — each value ``NULL``,
  ``TRUE``/``FALSE``, a number, or a quoted string, optionally ``::jsonb``,
  one per column

The file is split by the dump's own lexical rules: the dump sets
``standard_conforming_strings = on``, so ``''`` is the only escape and ``\\`` is
an ordinary character. The splitter this replaces treated ``\\`` as an escape —
a value ending in one swallowed every statement after it while the restore
reported success — and dropped any line of a multi-line value that began with
``--``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

__all__ = ["BackupFormatError", "RestoreStatement", "restore_statements"]

_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
_SETTINGS = (
    "statement_timeout",
    "lock_timeout",
    "client_encoding",
    "standard_conforming_strings",
    "check_function_bodies",
    "client_min_messages",
)
_SET = re.compile(
    rf"SET (?:{'|'.join(_SETTINGS)}) = (?:'[A-Za-z0-9_-]*'|[A-Za-z0-9_]+)", re.I
)
_TRIGGERS = re.compile(rf"ALTER TABLE ({_IDENT}) (?:DISABLE|ENABLE) TRIGGER ALL", re.I)
_DELETE = re.compile(rf"DELETE FROM ({_IDENT})", re.I)
_INSERT = re.compile(
    rf"INSERT INTO ({_IDENT}) \(({_IDENT}(?:, {_IDENT})*)\) VALUES \((.*)\)",
    re.I | re.S,
)
_VALUE = re.compile(
    r"NULL|TRUE|FALSE|-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?|'(?:[^']|'')*'(?:::jsonb)?",
    re.I | re.S,
)


class BackupFormatError(ValueError):
    """The file holds a statement the exporter never writes; nothing was run."""


@dataclass(frozen=True)
class RestoreStatement:
    sql: str
    #: The table an INSERT writes to, for the restore summary; None otherwise.
    insert_table: Optional[str] = None


def _split(sql: str) -> list[str]:
    """Statements outside string literals and ``--`` comments, without ``;``."""
    statements: list[str] = []
    buf: list[str] = []
    in_string = False
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if in_string:
            buf.append(ch)
            if ch == "'":
                if sql.startswith("''", i):
                    buf.append("'")
                    i += 2
                    continue
                in_string = False
        elif ch == "'":
            in_string = True
            buf.append(ch)
        elif sql.startswith("--", i):
            newline = sql.find("\n", i)
            i = n if newline == -1 else newline
            continue
        elif ch == ";":
            statements.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    # A trailing statement without ";" (or an unterminated string) is kept, so
    # validation refuses it rather than the restore silently dropping it.
    statements.append("".join(buf).strip())
    return [s for s in statements if s]


def _value_count(values: str) -> Optional[int]:
    """How many literals ``values`` lists, or None if any item is not one."""
    count, pos = 0, 0
    while True:
        match = _VALUE.match(values, pos)
        if not match:
            return None
        count, pos = count + 1, match.end()
        if pos == len(values):
            return count
        if not values.startswith(", ", pos):
            return None
        pos += 2


def _classify(stmt: str) -> Optional[RestoreStatement]:
    if _SET.fullmatch(stmt) or _TRIGGERS.fullmatch(stmt) or _DELETE.fullmatch(stmt):
        return RestoreStatement(stmt)
    insert = _INSERT.fullmatch(stmt)
    if insert and _value_count(insert[3]) == len(insert[2].split(", ")):
        return RestoreStatement(stmt, insert_table=insert[1].lower())
    return None


def restore_statements(sql: str) -> list[RestoreStatement]:
    """Every statement of a Kasal ``.sql`` backup, in order, validated.

    Raises ``BackupFormatError`` naming the first statement the exporter would
    never have written — before the caller runs any of them.
    """
    statements: list[RestoreStatement] = []
    for number, stmt in enumerate(_split(sql), start=1):
        parsed = _classify(stmt)
        if parsed is None:
            raise BackupFormatError(
                f"statement {number} is not one a Kasal backup contains: "
                f"{stmt[:80]!r}"
            )
        statements.append(parsed)
    return statements
