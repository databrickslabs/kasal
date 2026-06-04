"""Unit tests for UIConfigRepository.get_for_group."""
import pytest
from unittest.mock import AsyncMock, MagicMock

from src.repositories.ui_config_repository import UIConfigRepository
from src.models.ui_config import UIConfig


def _result_with(value):
    result = MagicMock()
    result.scalars.return_value.first.return_value = value
    return result


@pytest.mark.asyncio
async def test_get_for_group_with_group_id():
    session = AsyncMock()
    session.execute = AsyncMock(return_value=_result_with(UIConfig(id=1, group_id="g1")))

    repo = UIConfigRepository(session)
    out = await repo.get_for_group("g1")

    assert out is not None
    assert out.group_id == "g1"
    session.execute.assert_awaited()


@pytest.mark.asyncio
async def test_get_for_group_with_none_group_returns_none():
    session = AsyncMock()
    session.execute = AsyncMock(return_value=_result_with(None))

    repo = UIConfigRepository(session)
    out = await repo.get_for_group(None)

    assert out is None
    session.execute.assert_awaited()
