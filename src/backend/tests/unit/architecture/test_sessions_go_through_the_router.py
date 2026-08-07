"""Only the entry point picks a database. Everything else receives a session.

The layering this enforces::

    entry point (router / background task / tool)  ->  decides the database
      service                                     ->  transaction boundary
        repository                                ->  queries only

A service or repository that OPENS its own session has picked a database, and the
one it picks is usually wrong. ``async_session_factory`` is a per-process
SNAPSHOT: ``_SwappableSessionFactory.__call__`` returns ``self._factory()`` with
no config re-read. It points at Lakebase only if ``activate_lakebase()`` ran in
THIS process — the ``main.py`` lifespan (so only when Lakebase was already enabled
at BOOT) or inside a spawned subprocess. A runtime ``/lakebase/enable`` saves the
config and disposes engines; it never swaps the factory.

So after a runtime enable the process runs SPLIT: routed reads go to Lakebase
while every raw-factory holder keeps reading local SQLite. The failures share a
shape that makes them expensive to find — **the query succeeds and returns
nothing**. No exception, no log; the caller concludes "no rows" and carries on:

* a GEPA optimizer polled a crew that had committed COMPLETED at 19:12:16, saw
  ``None`` for ~14 minutes, and scored the candidate 0.0
* a configured Perplexity API key read as absent, so the tool ran without it
* an MCP server enabled for the workspace gave "Added 1 explicit MCP servers" in
  Agent Builder and "Added 0" in Chat
* ``workflow_recipes.embedding`` was written where no search would read it
* chat read a different ``databricksconfig`` row than the crew subprocess

``routed_scoped_session()`` is the fix for in-process work: it reuses the
request's session when there is one, and otherwise goes through the ROUTER, which
re-reads ``is_lakebase_enabled()`` per call.

This is a linter, not a style guide. Every allowed site is listed below WITH the
reason it cannot route. Adding an entry needs a reason in review — the whole value
of the check is that it fails on a new one.

Companion to ``test_background_readers_use_the_router.py``, which is narrower on
purpose: it names the specific background loops where the split was PROVEN and
also catches ``return`` inside a ``get_smart_db_session`` loop (which abandons the
generator before its commit). This file is the general rule; that one keeps the
incident history attached to the modules it happened in. A change to the
raw-factory policy needs both updated.
"""

import ast
import pathlib

import pytest

BACKEND_SRC = pathlib.Path(__file__).resolve().parents[3] / "src"

#: Names that ACQUIRE a database session or build an engine. Importing one to
#: inspect it is fine (healthcheck does exactly that); CALLING it picks a
#: database, which is what this test is about.
ACQUIRING_NAMES = {
    "async_session_factory",
    "create_async_engine",
    "async_sessionmaker",
    "sessionmaker",
    "create_engine",
}

#: Paths allowed to acquire, each with the reason. Prefix match on the
#: ``src/``-relative path, so a directory covers what is under it.
ALLOWED = {
    # ---- the session/routing machinery itself -----------------------------
    "db/": "this IS the session layer — it builds the engines everyone else receives",
    # `core/unit_of_work.py` was here — it opened the session it owned. It has
    # been DELETED rather than kept exempt: async UnitOfWork had 4 call sites and
    # none spanned more than ONE repository (the 3 in databricks_jobs_tool used no
    # repository at all), so it bought no cross-repository atomicity, only a second
    # way to acquire a session — the bug class this file exists to prevent. Its
    # callers now use the injected session or routed_scoped_session; the 3
    # SyncUnitOfWork callers (demo guardrails, sync callbacks) open a scoped
    # sync session instead.
    "utils/asyncio_utils.py": (
        "execute_db_operation_with_fresh_engine — binds a sessionmaker to the "
        "EXISTING global engine and disposes nothing. Disposing a StaticPool "
        "SQLite engine loses WAL data and has corrupted the file "
        '("file is not a database"), which is why it may not be simplified'
    ),
    # ---- entry points: deciding the database is their JOB -----------------
    "main.py": "the lifespan performs the boot-time Lakebase swap",
    "seeds/": (
        "seeders take an explicit factory argument (seed_runner patches it) so "
        "they seed whichever database is active"
    ),
    "scripts/": "one-shot operator scripts, run against a chosen database",
    # ---- genuinely cannot route -------------------------------------------
    "utils/databricks_auth.py": (
        "REENTRANT. The router needs a credential to reach Lakebase, so it calls "
        "get_auth_context; routing auth's own read unconditionally closed the loop "
        "and the deployed app logged 1,287 'maximum recursion depth exceeded', "
        "killing every crew and flow subprocess. _auth_scoped_session routes the "
        "OUTERMOST entry and uses the raw factory only while already resolving "
        "auth — which wants the local DB anyway, since the Lakebase config row "
        "lives there by design"
    ),
    "api/healthcheck_router.py": (
        "reports whether THIS process's factory got swapped. Routing it would make "
        "the check pass by construction and hide the split it exists to detect"
    ),
    # ---- Lakebase's own plumbing -----------------------------------------
    "services/databricks/lakebase/": (
        "connects TO Lakebase to test/migrate/seed it — it cannot ask the router "
        "for a session to the thing it is still setting up"
    ),
    # ---- (was: two DEAD post-subprocess log writers) ----------------------
    # `execution/logs/db_handler.py` and
    # `flow_builder/process_executor._write_logs_postgres_async` were exempted here
    # because both built an engine from settings.DATABASE_URI — the LOCAL database,
    # with no Lakebase awareness — so each was a latent split rather than a genuine
    # exception. Neither was reachable: the handler was never instantiated in src/
    # (subprocess_bootstrap imported the name and never used it) and the writer had
    # no caller outside tests. Both have now been DELETED, along with the tests that
    # were keeping them looking alive. The live logs path is queue -> writer, and
    # writer.py routes through get_smart_db_session.
}

#: Layers that must NEVER acquire, whatever the reason. A repository has no
#: legitimate case — it receives a session in its constructor, full stop.
NEVER_ACQUIRE_PREFIXES = ("repositories/",)


def _is_allowed(relative_path: str) -> bool:
    return any(relative_path.startswith(prefix) for prefix in ALLOWED)


def _acquiring_calls(tree: ast.AST) -> set[str]:
    """Names from ACQUIRING_NAMES that this module CALLS (not merely imports).

    Tracks ``as`` aliases, because ``async_session_factory as _plan_factory``
    hides the call site from a grep — one such alias was the last unrouted
    in-process write when this was written.
    """
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                if alias.name in ACQUIRING_NAMES:
                    aliases[alias.asname or alias.name] = alias.name

    called: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = (
            func.id
            if isinstance(func, ast.Name)
            else func.attr if isinstance(func, ast.Attribute) else None
        )
        if name in aliases:
            called.add(aliases[name])
        elif name in ACQUIRING_NAMES:
            # Imported as a module attribute (``sa.create_async_engine(...)``).
            called.add(name)
    return called


def _source_files() -> list[pathlib.Path]:
    return sorted(
        p
        for p in BACKEND_SRC.rglob("*.py")
        if "__pycache__" not in p.parts and "templates" not in p.parts
    )


def _relative(path: pathlib.Path) -> str:
    return str(path.relative_to(BACKEND_SRC))


class TestNoUnapprovedSessionAcquisition:
    def test_every_acquiring_module_is_on_the_list(self):
        """THE check. A new session/engine open must be justified in ALLOWED."""
        offenders: dict[str, set[str]] = {}
        for path in _source_files():
            relative = _relative(path)
            if _is_allowed(relative):
                continue
            try:
                tree = ast.parse(path.read_text())
            except SyntaxError:  # pragma: no cover
                continue
            called = _acquiring_calls(tree)
            if called:
                offenders[relative] = called

        assert not offenders, (
            "These modules open their own database session or engine:\n"
            + "\n".join(f"  {p}: {sorted(n)}" for p, n in sorted(offenders.items()))
            + "\n\nThe entry point decides the database; a service receives that "
            "session and hands it to its repositories. async_session_factory is a "
            "per-process SNAPSHOT that a runtime /lakebase/enable never swaps, so "
            "it reads the WRONG database and the miss is SILENT. Use "
            "routed_scoped_session() for in-process background work, or the "
            "injected SessionDep inside a request. If it genuinely cannot route, "
            "add it to ALLOWED in this file WITH the reason."
        )

    @pytest.mark.parametrize("prefix", NEVER_ACQUIRE_PREFIXES)
    def test_a_layer_that_may_never_acquire_does_not(self, prefix):
        """No allowlist applies here — a repository always receives its session.

        Three used to open their own (flow, task, execution_history); the removal
        comments are still in those files.
        """
        offenders = {}
        for path in _source_files():
            relative = _relative(path)
            if not relative.startswith(prefix):
                continue
            called = _acquiring_calls(ast.parse(path.read_text()))
            if called:
                offenders[relative] = sorted(called)

        assert not offenders, (
            f"{prefix} must never acquire a session: {offenders}. A repository "
            "takes `session` in its constructor and only builds queries — opening "
            "one there picks a database, which is the service/router's decision."
        )


class TestTheIndirectSnapshotPathIsGone:
    """``request_scoped_session`` no longer exists, and must not come back.

    It was a second way to get a session whose fallback branch was literally
    ``async with async_session_factory()`` — the same per-process snapshot, the
    same silent Lakebase split — behind a name that read as safe. That is why it
    survived several audits: 37 call sites looked request-scoped and 33 of them
    were plain snapshot reads.

    It is deleted rather than merely discouraged. ``routed_scoped_session`` absorbed
    the one case that genuinely needed the raw factory (a read made while already
    resolving auth, guarded by ``_RESOLVING_AUTH``), so there is nothing left for a
    second helper to do — and one helper cannot be picked wrongly.
    """

    def test_the_helper_is_not_reintroduced(self):
        from src.db import session as session_module

        assert not hasattr(session_module, "request_scoped_session"), (
            "request_scoped_session is back. Its fallback took the snapshot "
            "factory unconditionally outside a request, which is the silent "
            "Lakebase split. routed_scoped_session already handles the auth "
            "reentrancy that was its only justification."
        )

    def test_nothing_calls_or_imports_it(self):
        """A COMMENT may still name it (the history is worth keeping); code may not."""
        offenders = []
        for path in _source_files():
            for i, line in enumerate(path.read_text().splitlines(), 1):
                if "request_scoped_session" not in line:
                    continue
                if line.lstrip().startswith("#"):
                    continue  # history in a comment, fine
                if "async with" in line or "import" in line:
                    offenders.append(f"{_relative(path)}:{i}")
        assert not offenders, (
            f"{offenders} import or call request_scoped_session, which no longer "
            "exists — these fail at import, or in a test silently patch nothing."
        )

    def test_routed_scoped_session_still_breaks_the_auth_recursion(self):
        """The absorbed branch. Without it, deleting the old helper reopens the loop.

        The router needs a credential to reach Lakebase, so ``get_auth_context`` ->
        ``ApiKeysService`` -> a session. If that read routes, it re-enters the
        router: the deployed app logged 1,287 "maximum recursion depth exceeded"
        and every crew and flow subprocess died.
        """
        source = (BACKEND_SRC / "db" / "session.py").read_text()
        routed = source[source.index("async def routed_scoped_session") :]
        routed = routed[: routed.index("\ndef ") if "\ndef " in routed else len(routed)]
        assert "_RESOLVING_AUTH" in routed, (
            "routed_scoped_session no longer checks _RESOLVING_AUTH. It is now the "
            "only session helper, so this check is the ONLY thing keeping a "
            "credential lookup from recursing through the router."
        )


class TestTheListItselfStaysHonest:
    """An allowlist rots two ways: stale entries, and entries that stop routing."""

    @pytest.mark.parametrize("relative_path", sorted(ALLOWED))
    def test_the_entry_still_exists(self, relative_path):
        assert (BACKEND_SRC / relative_path).exists(), (
            f"{relative_path} moved or was deleted — update this entry rather than "
            "leaving a rule that silently covers nothing."
        )

    @pytest.mark.parametrize("relative_path", sorted(ALLOWED))
    def test_the_entry_has_a_reason(self, relative_path):
        assert len(ALLOWED[relative_path]) > 30, (
            f"{relative_path} is exempt without a real reason. The reason is what "
            "makes this list reviewable."
        )

    @pytest.mark.parametrize(
        "relative_path",
        ["utils/databricks_auth.py"],
    )
    def test_a_reentrant_exemption_still_routes_somewhere(self, relative_path):
        """Exempt for the NESTED call only — not licence to stop routing.

        Without this, "allowed to use the raw factory" quietly becomes "no longer
        routed at all", reintroducing the split the list exists to prevent.
        """
        source = (BACKEND_SRC / relative_path).read_text()
        assert (
            "get_smart_db_session" in source
        ), f"{relative_path} is exempt from the ban but no longer routes at all."
