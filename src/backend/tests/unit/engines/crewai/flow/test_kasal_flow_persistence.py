"""
Unit tests for KasalFlowPersistence (CrewAI @persist backed by Kasal's DB).

Covers:
 - _run_async thread bridge (success + error propagation)
 - save_state / load_state round-trip against a real SQLite DB (dict, BaseModel,
   empty, non-serializable default=str, latest-wins, missing -> None)
 - init_db table creation
 - _execute_with_retry: success, locked-retry, missing-table self-heal
   (incl. init_db failure), other-error propagation, attempt exhaustion
 - load_state JSON decode failure -> None
"""
import json

import pytest
from pydantic import BaseModel

import src.engines.crewai.paths.flow.kasal_flow_persistence as mod
from src.engines.crewai.paths.flow.kasal_flow_persistence import KasalFlowPersistence


@pytest.fixture
def persistence(tmp_path, monkeypatch):
    """A persistence instance wired to a fresh on-disk SQLite DB with the table created."""
    import src.db.session as session_mod
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import NullPool

    db_file = tmp_path / "fp.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_file}", poolclass=NullPool)
    factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(session_mod, "async_session_factory", factory, raising=False)

    p = KasalFlowPersistence()
    p.init_db()  # exercises the real init_db / _init closure
    return p


# ---------------------------------------------------------------------------
# _run_async
# ---------------------------------------------------------------------------

class TestRunAsync:
    def test_returns_value(self):
        async def coro():
            return 42
        assert mod._run_async(lambda: coro()) == 42

    def test_propagates_error(self):
        async def coro():
            raise RuntimeError("boom")
        with pytest.raises(RuntimeError, match="boom"):
            mod._run_async(lambda: coro())


# ---------------------------------------------------------------------------
# Real DB round-trip
# ---------------------------------------------------------------------------

class TestSaveLoadRealDB:
    def test_dict_round_trip_latest_wins(self, persistence):
        persistence.save_state("u1", "m0", {"id": "u1", "v": 1})
        persistence.save_state("u1", "m1", {"id": "u1", "v": 2})
        assert persistence.load_state("u1") == {"id": "u1", "v": 2}

    def test_basemodel_state(self, persistence):
        class StateModel(BaseModel):
            id: str = "u2"
            n: int = 5
        persistence.save_state("u2", "m", StateModel())
        assert persistence.load_state("u2") == {"id": "u2", "n": 5}

    def test_empty_state(self, persistence):
        persistence.save_state("u3", "m", {})
        assert persistence.load_state("u3") == {}

    def test_non_serializable_uses_default_str(self, persistence):
        sentinel = object()
        persistence.save_state("u4", "m", {"id": "u4", "obj": sentinel})
        loaded = persistence.load_state("u4")
        assert loaded["id"] == "u4"
        assert isinstance(loaded["obj"], str)  # default=str stringified it

    def test_load_missing_returns_none(self, persistence):
        assert persistence.load_state("nope") is None


# ---------------------------------------------------------------------------
# _execute_with_retry branches
# ---------------------------------------------------------------------------

class TestExecuteWithRetry:
    def test_success_passthrough(self, monkeypatch):
        p = KasalFlowPersistence()
        monkeypatch.setattr(mod, "_run_async", lambda factory: "ok")
        assert p._execute_with_retry(lambda: None) == "ok"

    def test_retry_on_locked_then_success(self, monkeypatch):
        p = KasalFlowPersistence()
        calls = {"n": 0}

        def fake(factory):
            calls["n"] += 1
            if calls["n"] == 1:
                raise Exception("database is locked")
            return "ok"

        monkeypatch.setattr(mod, "_run_async", fake)
        monkeypatch.setattr(mod.time, "sleep", lambda *_: None)
        assert p._execute_with_retry(lambda: None) == "ok"
        assert calls["n"] == 2

    def test_self_heal_on_missing_table(self, monkeypatch):
        p = KasalFlowPersistence()
        monkeypatch.setattr(mod, "_table_ensured", False)
        calls = {"n": 0}

        def fake(factory):
            calls["n"] += 1
            if calls["n"] == 1:
                raise Exception("no such table: flow_states")
            return "ok"

        monkeypatch.setattr(mod, "_run_async", fake)
        init_called = {"v": False}

        def fake_init(self):
            init_called["v"] = True

        monkeypatch.setattr(mod.KasalFlowPersistence, "init_db", fake_init)
        assert p._execute_with_retry(lambda: None) == "ok"
        assert init_called["v"] is True

    def test_self_heal_init_db_failure_is_swallowed(self, monkeypatch):
        p = KasalFlowPersistence()
        monkeypatch.setattr(mod, "_table_ensured", False)
        calls = {"n": 0}

        def fake(factory):
            calls["n"] += 1
            if calls["n"] == 1:
                raise Exception("no such table: flow_states")
            return "ok"

        def boom(self):
            raise RuntimeError("cannot create")

        monkeypatch.setattr(mod, "_run_async", fake)
        monkeypatch.setattr(mod.KasalFlowPersistence, "init_db", boom)
        assert p._execute_with_retry(lambda: None) == "ok"

    def test_other_error_propagates_immediately(self, monkeypatch):
        p = KasalFlowPersistence()

        def fake(factory):
            raise ValueError("unexpected")

        monkeypatch.setattr(mod, "_run_async", fake)
        with pytest.raises(ValueError, match="unexpected"):
            p._execute_with_retry(lambda: None)

    def test_exhausts_attempts_and_raises_last_error(self, monkeypatch):
        p = KasalFlowPersistence()

        def fake(factory):
            raise Exception("database is locked")

        monkeypatch.setattr(mod, "_run_async", fake)
        monkeypatch.setattr(mod.time, "sleep", lambda *_: None)
        with pytest.raises(Exception, match="locked"):
            p._execute_with_retry(lambda: None, attempts=3)


# ---------------------------------------------------------------------------
# load_state parsing
# ---------------------------------------------------------------------------

class TestLoadStateParsing:
    def test_none_when_no_row(self, monkeypatch):
        p = KasalFlowPersistence()
        monkeypatch.setattr(p, "_execute_with_retry", lambda factory: None)
        assert p.load_state("u") is None

    def test_valid_json(self, monkeypatch):
        p = KasalFlowPersistence()
        monkeypatch.setattr(p, "_execute_with_retry", lambda factory: '{"a": 1}')
        assert p.load_state("u") == {"a": 1}

    def test_invalid_json_returns_none(self, monkeypatch):
        p = KasalFlowPersistence()
        monkeypatch.setattr(p, "_execute_with_retry", lambda factory: "not-json{")
        assert p.load_state("u") is None
