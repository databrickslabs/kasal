"""Saving the decision-model switch against a real database, and what a failed save says.

The switch could not be enabled in a personal workspace: ``decision_config``
had a foreign key to ``groups.id`` and ``user_<email>`` workspaces have no
``groups`` row, so the insert failed — and the IntegrityError was reported as
"changed concurrently". These run on SQLite with foreign keys ON, which is what
made the key bite.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from src.core.exceptions import ConflictError, KasalError
from src.models.decision_config import DecisionConfig
from src.models.group import Group
from src.schemas.decision_config import DecisionConfigUpdate
from src.services.decisions.settings import DecisionSettingsService
from src.services.settings import engine_settings

PERSONAL = "user_dev_localhost"


@pytest.fixture
def jev_endpoint(monkeypatch):
    monkeypatch.setitem(
        engine_settings._snapshot, engine_settings.JEV_API_BASE, "https://example.com"
    )


@pytest.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite://")

    @event.listens_for(engine.sync_engine, "connect")
    def _foreign_keys_on(dbapi_conn, _record):
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync: (
                Group.__table__.create(sync),
                DecisionConfig.__table__.create(sync),
            )
        )
    async with AsyncSession(engine, expire_on_commit=False) as db:
        yield db
    await engine.dispose()


def _service(db: AsyncSession, group_id: str) -> DecisionSettingsService:
    service = DecisionSettingsService(db, group_id)
    # The key lookup is ApiKeysService's concern; here it simply has a key.
    service.api_keys = AsyncMock()
    service.api_keys.find_by_name.return_value = SimpleNamespace(
        encrypted_value="ciphertext"
    )
    return service


@pytest.mark.asyncio
async def test_personal_workspace_without_a_groups_row_can_enable_jev(
    session, jev_endpoint
):
    pragma = await session.execute(text("PRAGMA foreign_keys"))
    assert pragma.scalar() == 1

    result = await _service(session, PERSONAL).save(DecisionConfigUpdate(enabled=True))

    assert result.enabled is True
    config = await _service(session, PERSONAL).get()
    assert config.enabled is True and config.api_key_configured is True


@pytest.mark.asyncio
async def test_opt_in_stays_in_its_own_workspace(session, jev_endpoint):
    await _service(session, PERSONAL).save(DecisionConfigUpdate(enabled=True))

    other = await _service(session, "user_someone_else").get()
    assert other.enabled is False


@pytest.mark.asyncio
async def test_toggling_twice_updates_the_same_row(session, jev_endpoint):
    await _service(session, PERSONAL).save(DecisionConfigUpdate(enabled=True))
    await _service(session, PERSONAL).save(DecisionConfigUpdate(enabled=False))

    assert (await _service(session, PERSONAL).get()).enabled is False


def _failing_service(row_after_rollback):
    db = AsyncMock()
    service = DecisionSettingsService(db, PERSONAL)
    service.api_keys = AsyncMock()
    service.api_keys.find_by_name.return_value = SimpleNamespace(
        encrypted_value="ciphertext"
    )
    service.repository = AsyncMock()
    service.repository.save.side_effect = IntegrityError(
        "INSERT", {}, Exception("constraint failed")
    )
    service.repository.get.return_value = row_after_rollback
    return service, db


@pytest.mark.asyncio
async def test_a_concurrent_insert_is_a_conflict(jev_endpoint):
    service, db = _failing_service(SimpleNamespace(enabled=False))

    with pytest.raises(ConflictError, match="changed concurrently"):
        await service.save(DecisionConfigUpdate(enabled=True))
    db.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_any_other_integrity_error_is_not_called_a_conflict(jev_endpoint):
    service, db = _failing_service(None)

    with pytest.raises(KasalError) as raised:
        await service.save(DecisionConfigUpdate(enabled=True))

    assert not isinstance(raised.value, ConflictError)
    assert raised.value.status_code == 500
    assert "concurrently" not in raised.value.detail
    db.rollback.assert_awaited_once()
