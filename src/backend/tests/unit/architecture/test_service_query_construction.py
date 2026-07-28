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
`rollback`) or use `UnitOfWork`. That is not what this checks.
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
_BASELINE = {
    "services/execution/logs/db_handler.py",
    "services/flow_builder/flow_execution_runner.py",
    "services/flow_builder/flow_service.py",
    "services/flow_builder/process_executor.py",
    "services/tools/databricks_dashboard_creator_tool.py",
    "services/tools/metric_view_validator_tool.py",
}

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
