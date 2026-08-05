"""Background work that reads subprocess-written state goes through the ROUTER.

There are two ways to get a session and only one of them is self-correcting:

* ``get_smart_db_session()`` re-reads ``is_lakebase_enabled()`` on EVERY call.
* the raw ``async_session_factory`` is a SNAPSHOT.
  ``_SwappableSessionFactory.__call__`` just returns ``self._factory()`` — no
  config check, no lazy refresh. It points at Lakebase only if
  ``activate_lakebase()`` ran in THIS process, which happens in the ``main.py``
  lifespan (so only when Lakebase was already enabled at BOOT) or inside a
  spawned subprocess. Enabling Lakebase at RUNTIME goes through
  ``/lakebase/enable`` → save config + ``dispose_engines()``, which never swaps
  the factory.

So after a runtime enable the process runs SPLIT: routed reads go to Lakebase
while every raw-factory holder keeps using the local database. That is not
theoretical — it was proven against the live Lakebase. A GEPA crew committed
COMPLETED at 19:12:16 while the optimizer's poll, on the raw factory, saw None
for ~14 minutes and scored the candidate 0.0. The same split silently emptied
the API-key lookup and would have stopped the execution safety-net from ever
finding a stuck run.

The failures share a shape: **the read succeeds and returns nothing**. No
exception, no log — the caller concludes "no rows" and does nothing. That is why
this is a test and not a convention.

Scope is deliberately narrow. Plenty of raw-factory use is CORRECT: seeders,
bootstrap config reads that must stay local (the Lakebase config row itself
lives in SQLite), ``main.py``'s own lifespan, and the router's internals. What
must NOT use it is background work reading or writing state a crew/flow
SUBPROCESS owns, because the subprocess re-activates Lakebase for itself and
therefore lands on the other side of the split.
"""

import ast
import pathlib

import pytest

BACKEND_SRC = pathlib.Path(__file__).resolve().parents[3] / "src"

#: Modules whose background loops read or write subprocess-owned state. Each was
#: converted from the raw factory to the router after the split was proven; the
#: comment records what silently broke while it was on the snapshot.
ROUTER_REQUIRED = {
    # SSE pollers: the UI's status/trace stream goes quiet while the run is fine.
    "services/execution/broadcast.py",
    "services/trace/broadcast.py",
    # Startup sweep + the net that recovers a crew whose status write failed.
    # On the wrong database it finds nothing to recover and leaves runs at
    # RUNNING forever — the exact opposite of its purpose.
    "services/execution/cleanup.py",
    # Expired-approval sweep: a timed-out approval is never actioned, so its run
    # waits forever.
    "services/hitl/timeout.py",
    # Reads + writes `schedules`; on the wrong database no schedule is ever due.
    "services/scheduling/scheduler.py",
    # Writes embeddings. Succeeds on the wrong database, so nothing looks broken
    # and the rows simply land where no search will read them.
    "services/knowledge/embedding_queue.py",
    # The GEPA optimizer poll and its run-status writes — where this was found.
    "services/prompt_optimization/gepa/reflection.py",
    "services/prompt_optimization/run_state.py",
    # PAT / workspace-host lookups from `apikey` and `databricksconfig`. Same bug
    # class as the tool-credential miss: the query succeeds against the wrong
    # database, returns nothing, and auth silently degrades to environment
    # variables — which on a deployed app means no PAT at all.
    "utils/databricks_auth.py",
}


def _imports_raw_factory(tree: ast.AST) -> bool:
    """Whether the module imports ``async_session_factory`` at all."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "async_session_factory":
                    return True
    return False


@pytest.mark.parametrize("relative_path", sorted(ROUTER_REQUIRED))
def test_module_does_not_use_the_snapshot_factory(relative_path):
    path = BACKEND_SRC / relative_path
    assert path.exists(), f"{relative_path} moved — update this list, do not delete it"
    tree = ast.parse(path.read_text())
    assert not _imports_raw_factory(tree), (
        f"{relative_path} imports async_session_factory. That factory is a "
        "per-process snapshot: after a runtime /lakebase/enable it still points "
        "at the local database while routed reads go to Lakebase, and the "
        "resulting miss is SILENT. Use get_smart_db_session() (or "
        "execute_db_operation_smart) instead."
    )


@pytest.mark.parametrize("relative_path", sorted(ROUTER_REQUIRED))
def test_no_return_inside_a_router_session_loop(relative_path):
    """``return`` inside ``async for ... in get_smart_db_session()`` is a bug.

    The router yields ONCE and does its ``commit()`` and ``close()`` AFTER the
    yield. Returning from inside the loop abandons the generator, so on the
    local-DB branch the write is never committed and the session is never closed.
    ``break`` exits the loop and lets the generator finish.

    Four sites had this when the conversion was first made, including one that
    committed on every insert — so it leaked a session per embedding.
    """
    path = BACKEND_SRC / relative_path
    source = path.read_text()
    tree = ast.parse(source)
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFor):
            continue
        iterated = ast.get_source_segment(source, node.iter) or ""
        if "get_smart_db_session" not in iterated:
            continue
        # A return inside a NESTED function is that function's own, not ours.
        nested = {
            id(inner)
            for stmt in node.body
            for fn in ast.walk(stmt)
            if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef))
            for inner in ast.walk(fn)
            if isinstance(inner, ast.Return)
        }
        offenders += [
            node.lineno
            for stmt in node.body
            for inner in ast.walk(stmt)
            if isinstance(inner, ast.Return) and id(inner) not in nested
        ]
    assert not offenders, (
        f"{relative_path}: `return` inside a get_smart_db_session loop at line(s) "
        f"{sorted(set(offenders))}. Use `break` — a return abandons the generator "
        "before its commit/close run."
    )
