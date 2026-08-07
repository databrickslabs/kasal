"""Services must not build queries — repositories own that.

`import-linter` enforces the import layering (see `[tool.importlinter]` in
pyproject.toml), but it cannot express this rule: constructing a query is about
what you DO with an object, not what you import. Hence an AST check.

**This is a ratchet, not a gate.** The 26 files below already build queries and
are recorded as a baseline so the suite stays green; the test fails when a NEW
file starts, or when a file in the baseline is fixed but not removed from it.
The list is meant to shrink and can only shrink.

Why the rule: a service that writes a table directly skips whatever the owning
repository enforces on it — group scoping, encryption of sensitive fields,
cascade behaviour. Every one of those has been a real bug in this codebase.

A service may still hold a session for TRANSACTION control (`commit`,
`rollback`). That is not what this checks. There is no `UnitOfWork` to reach for —
`src.core.unit_of_work` was deleted; the session IS the unit of work.
"""

import ast
import pathlib

import pytest

#: `<session>.execute(...)` and friends — running a query.
_QUERY_METHODS = {"execute", "scalar", "scalars", "stream"}
#: `select(...)`, `text("SELECT …")` — building one.
_QUERY_BUILDERS = {"select", "text", "insert", "update", "delete"}

#: Paths where raw SQL is correct, not a violation.
_EXEMPT = (
    # DDL and connection management: there is no repository for CREATE SCHEMA,
    # GRANT, or a migration. These services ARE the database layer.
    "services/databricks/lakebase/",
    "services/memory/lakebase_",
    # Storage backends implement the engine's StorageBackend interface directly
    # against SQL; a repository in front of them would have one caller and no
    # invariants to protect.
    "_storage_backend.py",
    # Shipped verbatim into exported apps, which have no repository layer at all.
    # The other linters skip this tree too (see pyproject.toml).
    "services/export/templates/",
    # The engine's own flow-state checkpoint store: raw sqlite3 against a file
    # under db_storage_path(), not the application database. It matches only
    # because `connection.execute(...)` trips the "conn" heuristic.
    "services/flow_builder/runtime/persistence.py",
    # Schema bootstrap for the knowledge tables — information_schema probes plus
    # CREATE TABLE / CREATE INDEX. Same category as the lakebase DDL above: there
    # is no repository for CREATE TABLE.
    "services/knowledge/embedding_session.py",
)

#: Files that already build queries. Shrink this; never add to it.
#:
#: Down from 6 to 0. Four moved their SQL into a repositories file: `flow_service`
#: (the flow cascade-delete, now on FlowRepository), the two PowerBI/UCMV tools and
#: `flow_execution_runner` (latest-trace and checkpoint lookups, now on
#: ExecutionTraceRepository / ExecutionHistoryRepository). Three of those queries
#: also used Postgres-only casts (`output::text`, `result::text`) that silently
#: matched nothing on SQLite; the repository versions are dialect-neutral.
#:
#: The last two were the DEAD post-subprocess log writers — raw DBAPI against
#: `execution_logs`, neither reachable from src/ — and were DELETED rather than
#: refactored, which is why this is now empty. Keep it that way: an empty baseline
#: makes this a hard ban, not a ratchet.
_BASELINE = set()

_SERVICES = pathlib.Path(__file__).resolve().parents[3] / "src" / "services"


def _sqlalchemy_names(tree: ast.AST) -> set[str]:
    """Names this module imported FROM sqlalchemy.

    Without this, any local called `delete` or `select` reads as a query. That is
    not hypothetical: services/memory/maintenance.py did
    `delete = getattr(storage, "delete", None)` against a pluggable memory
    backend, and the check flagged a file with no database access in it at all.
    """
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
            "sqlalchemy"
        ):
            names.update(alias.asname or alias.name for alias in node.names)
    return names


def _builds_queries(path: pathlib.Path) -> bool:
    try:
        tree = ast.parse(path.read_text())
    except SyntaxError:
        return False  # export templates hold {{TOKEN}} placeholders
    builders = _QUERY_BUILDERS & _sqlalchemy_names(tree)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        if isinstance(fn, ast.Attribute) and fn.attr in _QUERY_METHODS:
            owner = (
                getattr(fn.value, "id", "") or getattr(fn.value, "attr", "")
            ).lower()
            if "session" in owner or "conn" in owner:
                return True
        if isinstance(fn, ast.Name) and fn.id in builders:
            return True
    return False


def _offenders() -> set[str]:
    found = set()
    for path in _SERVICES.rglob("*.py"):
        rel = path.relative_to(_SERVICES.parents[0]).as_posix()
        if any(part in rel for part in _EXEMPT):
            continue
        if _builds_queries(path):
            found.add(rel)
    return found


def test_no_new_service_builds_queries():
    new = sorted(_offenders() - _BASELINE)
    assert not new, (
        "These services construct queries; that belongs in a repository:\n  "
        + "\n  ".join(new)
        + "\n\nA service may hold a session for commit/rollback, but the query "
        "itself goes behind a repository — otherwise group scoping, field "
        "encryption and cascades silently do not run."
    )


def test_baseline_has_no_stale_entries():
    """A file fixed but left in the baseline hides the next regression in it."""
    stale = sorted(_BASELINE - _offenders())
    assert (
        not stale
    ), "These no longer build queries — delete them from _BASELINE:\n  " + "\n  ".join(
        stale
    )


# ---------------------------------------------------------------------------
# ORM writes — the half the query check above cannot see
# ---------------------------------------------------------------------------
#
# `_builds_queries` looks for `session.execute(...)` and `select(...)`. It says
# nothing about `session.add(row)` / `session.delete(row)`, which is how a service
# persists WITHOUT writing SQL at all — and that is the more common bypass: ten
# services were doing it while this file reported six offenders.
#
# The rule is the same one, applied to writes: the repositories file owns
# persistence for its model. A service that adds or deletes a row itself skips
# whatever that repository enforces — group scoping, field encryption, cascade
# order. All three have been real bugs here.
#
# A service may still own the TRANSACTION (`commit`, `rollback`), and may mutate an
# instance a repository handed it — an ORM attribute assignment is not a query.
# What it may not do is stage or remove rows.

#: `session.<op>(...)` — persistence operations that belong behind a repository.
_WRITE_OPS = {"add", "add_all", "delete", "merge"}

#: Identifiers that mean "a database session" in this codebase.
_SESSION_NAMES = {"session", "db", "iso_session", "log_session", "_session"}


def _looks_like_a_session(name: str) -> bool:
    return name in _SESSION_NAMES or name.endswith("_session")


def _orm_writers(path: pathlib.Path) -> set[str]:
    """ORM write ops this module calls on something that looks like a session."""
    source = path.read_text()
    # aiohttp's ClientSession has .delete()/.put() too, and `async with
    # aiohttp.ClientSession() as session` is a common shape here — an HTTP DELETE
    # is not a database write. mcp_handler tripped exactly this.
    if "aiohttp" in source and "ClientSession" in source:
        return set()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()
    found = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        owner = (
            getattr(node.func.value, "id", "") or getattr(node.func.value, "attr", "")
        ).lower()
        if _looks_like_a_session(owner) and node.func.attr in _WRITE_OPS:
            found.add(node.func.attr)
    return found


def test_no_service_persists_rows_itself():
    """Cleared to zero — keep it there. There is no baseline on purpose."""
    offenders = {}
    for path in _SERVICES.rglob("*.py"):
        rel = path.relative_to(_SERVICES.parents[0]).as_posix()
        if any(part in rel for part in _EXEMPT):
            continue
        writers = _orm_writers(path)
        if writers:
            offenders[rel] = sorted(writers)

    assert not offenders, (
        "These services stage or remove rows themselves:\n  "
        + "\n  ".join(f"{p}: {ops}" for p, ops in sorted(offenders.items()))
        + "\n\nPut the write on the model's repository in src/repositories/ "
        "(insert/remove/save) and call that instead. The service keeps the "
        "transaction (commit/rollback); the repository owns persistence, and with "
        "it group scoping, encryption and cascade order."
    )
