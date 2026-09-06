"""The startup heal that gives every user a personal-workspace id
(_assign_personal_workspace_ids), in creation order (audit F06 / R2-06)."""

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from src.db.self_heal import data as heal
from src.utils.user_context import GroupContext

A, B = "alice.smith@example.com", "alice-smith@example.com"


async def _rows(conn):
    return {
        r[0]: r[1]
        for r in (
            await conn.exec_driver_sql("SELECT email, personal_group_id FROM users")
        ).fetchall()
    }


@pytest.mark.asyncio
async def test_first_come_keeps_the_derived_id_and_a_collision_is_disambiguated():
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await conn.exec_driver_sql(
                "CREATE TABLE users (id TEXT, email TEXT, created_at TEXT, personal_group_id TEXT)"
            )
            await conn.exec_driver_sql(
                "INSERT INTO users VALUES "
                f"('u-a', '{A}', '2026-01-01', NULL), "
                f"('u-b', '{B}', '2026-02-01', NULL), "
                "('u-c', 'carol@example.com', '2026-03-01', NULL)"
            )
            await heal._assign_personal_workspace_ids(conn)
            rows = await _rows(conn)
            assert rows[A] == GroupContext.generate_individual_group_id(A)
            assert rows[B] == GroupContext.disambiguated_individual_group_id(B)
            assert rows["carol@example.com"] == "user_carol_example_com"
            assert len(set(rows.values())) == 3
            # Idempotent: a second run changes nothing.
            await heal._assign_personal_workspace_ids(conn)
            assert await _rows(conn) == rows
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_an_already_assigned_id_is_respected_by_later_users():
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await conn.exec_driver_sql(
                "CREATE TABLE users (id TEXT, email TEXT, created_at TEXT, personal_group_id TEXT)"
            )
            legacy = GroupContext.generate_individual_group_id(A)
            await conn.exec_driver_sql(
                "INSERT INTO users VALUES "
                f"('u-a', '{A}', '2026-01-01', '{legacy}'), "
                f"('u-b', '{B}', '2026-02-01', NULL)"
            )
            await heal._assign_personal_workspace_ids(conn)
            rows = await _rows(conn)
            assert rows[A] == legacy
            assert rows[B] == GroupContext.disambiguated_individual_group_id(B)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_a_missing_table_does_not_raise():
    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await heal._assign_personal_workspace_ids(conn)
    finally:
        await engine.dispose()
