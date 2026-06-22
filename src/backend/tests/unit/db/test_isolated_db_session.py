"""Regression tests for get_isolated_db_session (private-connection isolation).

Background — the bug these guard against:

Progressive crew generation commits an agent, then makes a seconds-long LLM call,
then inserts a task referencing that agent. On SQLite the whole app shares ONE
connection (StaticPool), and SQLite transactions are per-connection. During that
long window a concurrent request's session — sharing the same connection — could
commit/rollback and silently discard the just-written agent, after which the
task INSERT failed with "FOREIGN KEY constraint failed" (tasks.agent_id ->
agents.id). The fix routes the generation flow onto its OWN connection
(get_isolated_db_session, a NullPool engine for SQLite) so no other session can
interfere.

These tests demonstrate the mechanism (shared connection loses the write) and
the property the fix relies on (a private connection does not).
"""
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool, StaticPool


def _make_engine(tmp_path, name, poolclass):
    uri = f"sqlite+aiosqlite:///{tmp_path / name}"
    return create_async_engine(
        uri,
        future=True,
        poolclass=poolclass,
        connect_args={"check_same_thread": False},
    )


@pytest.mark.asyncio
async def test_shared_connection_loses_write_on_other_sessions_rollback(tmp_path):
    """The bug mechanism: with ONE shared connection (StaticPool), a second
    session's rollback discards the first session's pending write, so its commit
    persists nothing — exactly how a generated agent vanished and broke the
    task's foreign key."""
    engine = _make_engine(tmp_path, "shared.db", StaticPool)
    try:
        async with engine.begin() as conn:
            await conn.exec_driver_sql("CREATE TABLE agents (id TEXT PRIMARY KEY)")

        Session = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
        writer = Session()
        other = Session()
        try:
            await writer.execute(text("INSERT INTO agents (id) VALUES ('a1')"))
            # The two sessions share ONE underlying connection, so `other` is in
            # the same transaction and sees the not-yet-committed row...
            seen = (await other.execute(text("SELECT COUNT(*) FROM agents"))).scalar()
            assert seen == 1
            # ...and its rollback throws away the writer's pending INSERT.
            await other.rollback()
            await writer.commit()  # commits an empty transaction
        finally:
            await writer.close()
            await other.close()

        async with Session() as check:
            remaining = (await check.execute(text("SELECT COUNT(*) FROM agents"))).scalar()
        # The agent is GONE despite writer.commit() — the shared-connection hazard.
        assert remaining == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_private_connection_isolates_writes_from_other_sessions(tmp_path):
    """The fix: with NullPool each session gets its OWN connection. Another
    session can neither see the writer's uncommitted row nor roll it back, so the
    committed agent survives and a task referencing it satisfies the FK."""
    engine = _make_engine(tmp_path, "private.db", NullPool)
    # Enforce foreign keys so a missing agent would actually fail the task insert.
    from sqlalchemy import event

    @event.listens_for(engine.sync_engine, "connect")
    def _fk_on(dbapi_conn, _record):
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    try:
        async with engine.begin() as conn:
            await conn.exec_driver_sql("CREATE TABLE agents (id TEXT PRIMARY KEY)")
            await conn.exec_driver_sql(
                "CREATE TABLE tasks (id TEXT PRIMARY KEY, "
                "agent_id TEXT REFERENCES agents(id))"
            )

        Session = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)

        # Writer commits an agent on its private connection.
        async with Session() as writer:
            await writer.execute(text("INSERT INTO agents (id) VALUES ('a1')"))
            await writer.commit()

        # A separate session does work and rolls back — must not touch the agent.
        async with Session() as other:
            await other.execute(text("SELECT 1"))
            await other.rollback()

        # Inserting a task referencing the agent succeeds (agent survived).
        async with Session() as writer:
            await writer.execute(text("INSERT INTO tasks (id, agent_id) VALUES ('t1', 'a1')"))
            await writer.commit()

        async with Session() as check:
            agents = (await check.execute(text("SELECT id FROM agents"))).scalars().all()
            tasks = (
                await check.execute(text("SELECT id, agent_id FROM tasks"))
            ).all()
        assert agents == ["a1"]
        assert [(r[0], r[1]) for r in tasks] == [("t1", "a1")]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_get_isolated_db_session_uses_private_nullpool_engine_for_sqlite(monkeypatch, tmp_path):
    """The helper must route SQLite onto a NullPool engine that is NOT the shared
    StaticPool `engine` — otherwise the generation flow shares the one connection
    again and the bug returns."""
    from src.db import session as sess_mod

    db_file = tmp_path / "iso_helper.db"
    monkeypatch.setattr(sess_mod.settings, "DATABASE_URI", f"sqlite+aiosqlite:///{db_file}")
    # Force a fresh isolated engine bound to the temp DB (module-level cache).
    monkeypatch.setattr(sess_mod, "_isolated_sqlite_engine", None)
    monkeypatch.setattr(sess_mod, "_isolated_sqlite_session_factory", None)
    # Ensure the factory does not believe Lakebase is active.
    monkeypatch.setattr(sess_mod.async_session_factory, "_is_lakebase", False, raising=False)

    try:
        async with sess_mod.get_isolated_db_session() as session:
            bound = session.get_bind()
            assert isinstance(bound.pool, NullPool)
            # Distinct from the shared engine → distinct connection.
            assert bound is not sess_mod.engine
            # And it is usable.
            assert (await session.execute(text("SELECT 1"))).scalar() == 1
    finally:
        iso = sess_mod._isolated_sqlite_engine
        if iso is not None:
            await iso.dispose()
