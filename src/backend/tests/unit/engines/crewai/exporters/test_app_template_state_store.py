"""
Unit tests for the exported Databricks App's durable conversation state.

The app template's ``agent_server.state_store`` backs the previously in-memory
stores (conversation history, cancel flags, progress, A2UI surfaces) with
Lakebase/SQLite so they survive an app restart. These tests import the template
package straight from the templates directory and prove:
  - backend selection (SQLite when no Lakebase env; memory as last resort),
  - round-trip + TTL semantics of the KV layer,
  - cancel / a2ui / progress / history all survive a simulated process restart
    (module reload with the same SQLite file).
"""

import importlib
import sys
import time

import pytest

from src.engines.crewai.exporters.databricks_app_exporter import TEMPLATE_DIR


def _purge_agent_server_modules():
    for name in list(sys.modules):
        if name == "agent_server" or name.startswith("agent_server."):
            del sys.modules[name]


@pytest.fixture
def template_env(tmp_path, monkeypatch):
    """Import the template's agent_server package against a temp SQLite file."""
    monkeypatch.syspath_prepend(str(TEMPLATE_DIR))
    monkeypatch.setenv("AGENT_STATE_SQLITE", str(tmp_path / "state.sqlite"))
    for var in ("LAKEBASE_INSTANCE_NAME", "PGHOST"):
        monkeypatch.delenv(var, raising=False)
    _purge_agent_server_modules()
    yield tmp_path
    _purge_agent_server_modules()


def _import(name):
    return importlib.import_module(f"agent_server.{name}")


def _simulate_restart():
    """Reload the state modules so all in-process state is lost, but the
    SQLite file (same AGENT_STATE_SQLITE) persists — like an app restart."""
    state_store = _import("state_store")
    importlib.reload(state_store)
    for name in ("cancel", "progress", "a2ui_store"):
        if f"agent_server.{name}" in sys.modules:
            importlib.reload(sys.modules[f"agent_server.{name}"])


class TestStateStoreKV:
    def test_sqlite_backend_selected_without_lakebase(self, template_env):
        state_store = _import("state_store")
        assert state_store.backend_name() == "sqlite"

    def test_text_and_json_round_trip(self, template_env):
        state_store = _import("state_store")
        state_store.set_text("c1", "k", "v")
        assert state_store.get_text("c1", "k") == "v"
        state_store.set_json("c1", "j", {"a": [1, 2]})
        assert state_store.get_json("c1", "j") == {"a": [1, 2]}
        state_store.delete("c1", "k")
        assert state_store.get_text("c1", "k") is None

    def test_max_age_expires_values(self, template_env):
        state_store = _import("state_store")
        state_store.set_text("c1", "k", "v")
        time.sleep(0.05)
        assert state_store.get_text("c1", "k", max_age=0.01) is None
        assert state_store.get_text("c1", "k") == "v"  # no max_age -> still there

    def test_none_conversation_id_is_noop(self, template_env):
        state_store = _import("state_store")
        state_store.set_text(None, "k", "v")
        assert state_store.get_text(None, "k") is None
        state_store.delete(None, "k")

    def test_values_survive_restart(self, template_env):
        state_store = _import("state_store")
        state_store.set_text("c1", "k", "v")
        _simulate_restart()
        state_store = _import("state_store")
        assert state_store.get_text("c1", "k") == "v"

    def test_memory_fallback_when_sqlite_path_unusable(
        self, template_env, monkeypatch, tmp_path
    ):
        # Point the SQLite path INSIDE a regular file so opening it fails.
        blocker = tmp_path / "blocker"
        blocker.write_text("x")
        monkeypatch.setenv("AGENT_STATE_SQLITE", str(blocker / "state.sqlite"))
        state_store = _import("state_store")
        importlib.reload(state_store)
        assert state_store.backend_name() == "memory"
        state_store.set_text("c1", "k", "v")  # still works in-process
        assert state_store.get_text("c1", "k") == "v"


class TestCancelDurability:
    def test_cancel_within_process(self, template_env):
        cancel = _import("cancel")
        cancel.request("c1")
        assert cancel.is_cancelled("c1") is True
        cancel.clear("c1")
        assert cancel.is_cancelled("c1") is False

    def test_cancel_survives_restart(self, template_env):
        cancel = _import("cancel")
        cancel.request("c1")
        _simulate_restart()
        cancel = _import("cancel")
        assert not cancel._cancelled  # in-process flag really was lost
        assert cancel.is_cancelled("c1") is True  # found via the durable store
        cancel.clear("c1")
        assert cancel.is_cancelled("c1") is False

    def test_none_or_unknown_not_cancelled(self, template_env):
        cancel = _import("cancel")
        assert cancel.is_cancelled(None) is False
        assert cancel.is_cancelled("never-seen") is False


class TestA2uiDurability:
    def test_lifecycle_within_process(self, template_env):
        a2ui_store = _import("a2ui_store")
        assert a2ui_store.get("c1")["status"] == "idle"
        a2ui_store.begin("c1")
        assert a2ui_store.get("c1")["status"] == "pending"
        a2ui_store.put("c1", {"surfaceKind": "dashboard"})
        got = a2ui_store.get("c1")
        assert got["status"] == "ready"
        assert got["surface"] == {"surfaceKind": "dashboard"}
        a2ui_store.put("c1", None)
        assert a2ui_store.get("c1")["status"] == "none"

    def test_surface_survives_restart(self, template_env):
        a2ui_store = _import("a2ui_store")
        a2ui_store.put("c1", {"surfaceKind": "presentation", "root": {}})
        _simulate_restart()
        a2ui_store = _import("a2ui_store")
        assert not a2ui_store._store  # in-process stash really was lost
        got = a2ui_store.get("c1")
        assert got["status"] == "ready"
        assert got["surface"]["surfaceKind"] == "presentation"


class TestProgressDurability:
    def test_report_and_get_within_process(self, template_env):
        progress = _import("progress")
        progress.set_current("c1")
        progress.report("Researching the topic")
        got = progress.get("c1")
        assert got["status"] == "Researching the topic"
        assert got["seq"] == 1
        progress.clear("c1")
        assert progress.get("c1") is None
        progress.clear_current()

    def test_status_survives_restart(self, template_env):
        progress = _import("progress")
        progress.set_current("c1")
        progress.report("Calling the search tool")
        progress.clear_current()
        _simulate_restart()
        progress = _import("progress")
        got = progress.get("c1")
        assert got is not None
        assert got["status"] == "Calling the search tool"


class TestConversationHistoryDurability:
    def test_history_survives_restart(self, template_env):
        pytest.importorskip("crewai")
        pytest.importorskip("mlflow")
        conversation = _import("conversation")
        messages = [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "hello"},
        ]
        conversation._save_history("c1", list(messages))
        assert conversation.get_history("c1") == messages

        state_store = _import("state_store")
        importlib.reload(state_store)
        conversation._HISTORY.clear()  # in-process cache really is lost
        assert conversation.get_history("c1") == messages

    def test_history_trimmed_to_cap(self, template_env):
        pytest.importorskip("crewai")
        pytest.importorskip("mlflow")
        conversation = _import("conversation")
        many = [{"role": "user", "content": str(i)} for i in range(50)]
        conversation._save_history("c1", many)
        got = conversation.get_history("c1")
        assert len(got) == conversation._HISTORY_MAX_MESSAGES
        assert got[-1]["content"] == "49"

    def test_empty_history_for_unknown_or_none(self, template_env):
        pytest.importorskip("crewai")
        pytest.importorskip("mlflow")
        conversation = _import("conversation")
        assert conversation.get_history(None) == []
        assert conversation.get_history("never-seen") == []


def _install_fake_pg8000(monkeypatch, fail_on_sql=()):
    """Install a fake pg8000 module; returns the list of executed SQL.

    ``fail_on_sql``: substrings — any executed statement containing one raises
    a permission error, like Lakebase's 42501 'permission denied for schema
    public' for non-owner CREATEs in ``public``.
    """
    import types

    executed = []

    class FakeCursor:
        def execute(self, sql, params=()):
            executed.append(sql)
            for pattern in fail_on_sql:
                if pattern in sql:
                    raise RuntimeError(
                        "{'C': '42501', 'M': 'permission denied for schema public'}"
                    )

        def fetchone(self):
            return None

        def close(self):
            pass

    class FakeConnection:
        autocommit = False

        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    dbapi = types.ModuleType("pg8000.dbapi")
    dbapi.connect = lambda **kwargs: FakeConnection()
    pkg = types.ModuleType("pg8000")
    pkg.dbapi = dbapi
    monkeypatch.setitem(sys.modules, "pg8000", pkg)
    monkeypatch.setitem(sys.modules, "pg8000.dbapi", dbapi)
    return executed


class TestPostgresSchemaSelection:
    """Lakebase (Postgres 15+) denies CREATE in `public` to non-owners; the
    app's CAN_CONNECT_AND_CREATE resource grants database-level CREATE only.
    The store must therefore live in its own schema — these tests pin that."""

    @pytest.fixture
    def pg_env(self, template_env, monkeypatch):
        monkeypatch.setenv("PGHOST", "instance.example.com")
        monkeypatch.setenv("PGUSER", "app-sp-client-id")
        monkeypatch.setenv("PGPASSWORD", "token")
        return monkeypatch

    def test_uses_dedicated_schema(self, pg_env):
        executed = _install_fake_pg8000(pg_env)
        state_store = _import("state_store")
        importlib.reload(state_store)
        assert state_store.backend_name() == "lakebase"
        assert any("CREATE SCHEMA IF NOT EXISTS agent_server" in s for s in executed)
        assert any("agent_server.kasal_app_state" in s for s in executed)
        state_store.set_text("c1", "k", "v")
        assert any(
            s.startswith("INSERT INTO agent_server.kasal_app_state") for s in executed
        )

    def test_falls_back_to_public_when_schema_denied(self, pg_env):
        executed = _install_fake_pg8000(pg_env, fail_on_sql=("CREATE SCHEMA",))
        state_store = _import("state_store")
        importlib.reload(state_store)
        assert state_store.backend_name() == "lakebase"
        # Retried in `public` (unqualified) after the schema create was denied.
        assert any("CREATE TABLE IF NOT EXISTS kasal_app_state" in s for s in executed)
        state_store.set_text("c1", "k", "v")
        assert any(s.startswith("INSERT INTO kasal_app_state") for s in executed)

    def test_falls_back_to_sqlite_when_all_creates_denied(self, pg_env):
        # The incident observed in production: every CREATE is denied (42501)
        # -> the store must degrade to SQLite, not crash the app.
        _install_fake_pg8000(pg_env, fail_on_sql=("CREATE",))
        state_store = _import("state_store")
        importlib.reload(state_store)
        assert state_store.backend_name() == "sqlite"
        state_store.set_text("c1", "k", "v")
        assert state_store.get_text("c1", "k") == "v"


class TestCrossProcessSignals:
    def test_cancel_from_another_process_is_seen(self, template_env):
        """A Stop that lands on a different process writes only the durable
        flag; the process running the turn must still see it (the first check
        for an unknown conversation is not throttled)."""
        cancel = _import("cancel")
        state_store = _import("state_store")
        state_store.set_text("c1", "cancel", "1")  # what POST /cancel does elsewhere
        assert "c1" not in cancel._cancelled
        assert cancel.is_cancelled("c1") is True

    def test_cancel_durable_check_is_throttled(self, template_env, monkeypatch):
        """Step callbacks poll is_cancelled; between throttle windows only the
        in-process flag is consulted so crew steps stay cheap."""
        cancel = _import("cancel")
        state_store = _import("state_store")
        assert cancel.is_cancelled("c1") is False  # primes the throttle window
        state_store.set_text("c1", "cancel", "1")
        assert cancel.is_cancelled("c1") is False  # within window: no durable read
        monkeypatch.setattr(cancel, "_CHECK_INTERVAL_SECONDS", 0.0)
        assert cancel.is_cancelled("c1") is True  # window elapsed -> flag seen

    def test_progress_mirror_is_throttled(self, template_env, monkeypatch):
        """Event-bus reports mirror to the durable store at most once per
        window; local reads always see the freshest status."""
        progress = _import("progress")
        state_store = _import("state_store")
        progress.set_current("c1")
        progress.report("step one")  # first report always mirrors
        progress.report("step two")  # inside the window: in-process only
        assert state_store.get_json("c1", "progress")["status"] == "step one"
        assert progress.get("c1")["status"] == "step two"
        monkeypatch.setattr(progress, "_MIRROR_INTERVAL_SECONDS", 0.0)
        progress.report("step three")
        assert state_store.get_json("c1", "progress")["status"] == "step three"
        progress.clear_current()
