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

#: Modules that must ROUTE but are also allowed to hold the raw factory, with the
#: reason. Auth is the one genuine case: the router needs a credential to reach
#: Lakebase, so ``get_smart_db_session`` calls ``get_auth_context``. Routing
#: auth's own read unconditionally closed the loop
#: (``get_auth_context → get_smart_db_session → get_auth_context``) and the
#: deployed app logged 1,287 "maximum recursion depth exceeded", killing every
#: crew and flow subprocess.
#:
#: ``_auth_scoped_session`` resolves it: route on the OUTERMOST entry (so the
#: Lakebase split is still fixed) and use the raw factory only for a read made
#: while already resolving auth — which wants the local database anyway, since the
#: Lakebase config row lives there. Hence the factory import is correct HERE and
#: nowhere else on this list.
RAW_FACTORY_ALLOWED = {
    "utils/databricks_auth.py": "_auth_scoped_session, for the reentrant case",
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
    if relative_path in RAW_FACTORY_ALLOWED:
        pytest.skip(f"allowed: {RAW_FACTORY_ALLOWED[relative_path]}")
    assert not _imports_raw_factory(tree), (
        f"{relative_path} imports async_session_factory. That factory is a "
        "per-process snapshot: after a runtime /lakebase/enable it still points "
        "at the local database while routed reads go to Lakebase, and the "
        "resulting miss is SILENT. Use get_smart_db_session() (or "
        "execute_db_operation_smart) instead."
    )


@pytest.mark.parametrize("relative_path", sorted(RAW_FACTORY_ALLOWED))
def test_an_allowed_module_still_routes_somewhere(relative_path):
    """An exemption is for a REENTRANT read, not for abandoning the router.

    Without this, "allowed to use the raw factory" would quietly become "no longer
    routed at all" — reintroducing the silent Lakebase split the list exists to
    prevent.
    """
    source = (BACKEND_SRC / relative_path).read_text()
    assert "get_smart_db_session" in source, (
        f"{relative_path} is exempt from the raw-factory ban but no longer routes "
        "at all. The exemption covers the nested call only."
    )


#: Modules allowed to leave a router loop early, with the reason. Both are the
#: crew/flow SUBPROCESS entry points, where the loop wraps the whole run and the
#: process ends right after it; converting them changes what is committed at the
#: end of a run, which needs a real subprocess run to verify. Shrink-only.
EARLY_EXIT_ALLOWED = {
    "services/agent_builder/process_executor.py": "subprocess entry point",
    "services/flow_builder/process_executor.py": "subprocess entry point",
}


def _early_exits(body):
    """Line numbers of a ``return`` or a ``break`` that leaves THIS loop.

    A return in a nested function is that function's own; a break in a nested
    loop exits that loop, not this one.
    """
    found, stack = [], list(body)
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        if isinstance(node, (ast.Return, ast.Break)):
            found.append(node.lineno)
        if isinstance(node, (ast.For, ast.AsyncFor, ast.While)):
            # Its returns still leave our loop; its breaks do not.
            stack.extend(n for n in ast.walk(node) if isinstance(n, ast.Return))
            continue
        stack.extend(ast.iter_child_nodes(node))
    return found


@pytest.mark.parametrize(
    "relative_path",
    sorted(
        str(p.relative_to(BACKEND_SRC))
        for p in BACKEND_SRC.rglob("*.py")
        if "get_smart_db_session()" in p.read_text()
    ),
)
def test_no_early_exit_from_a_router_session_loop(relative_path):
    """``return`` OR ``break`` inside ``async for ... in get_smart_db_session()``.

    The router yields ONCE and does its ``commit()``, ``close()`` and the reset
    of ``_request_session`` AFTER the yield. Leaving the loop early by either
    route abandons the generator: the commit never runs, the close happens later
    in a finaliser task, and the next routed block in this task reuses the
    abandoned session. (This test used to recommend ``break``; it is the same
    bug.) Use ``async with asynccontextmanager(get_smart_db_session)() as s:``.
    """
    if relative_path in EARLY_EXIT_ALLOWED:
        pytest.skip(f"allowed: {EARLY_EXIT_ALLOWED[relative_path]}")
    source = (BACKEND_SRC / relative_path).read_text()
    offenders = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.AsyncFor):
            continue
        iterated = ast.get_source_segment(source, node.iter) or ""
        if "get_smart_db_session" in iterated:
            offenders += _early_exits(node.body)
    assert not offenders, (
        f"{relative_path}: `return`/`break` inside a get_smart_db_session loop at "
        f"line(s) {sorted(set(offenders))}. It abandons the generator before its "
        "commit/close run; use the router as a context manager instead."
    )
