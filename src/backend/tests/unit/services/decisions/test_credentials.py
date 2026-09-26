from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import pytest

from src.services.decisions import credentials

# tests/unit/services/conftest.py patches decision_credential itself for every
# service test; grab the real function before that fixture runs.
_REAL_DECISION_CREDENTIAL = credentials.decision_credential


@pytest.mark.asyncio
async def test_one_isolated_session_per_lookup():
    """The lookup opens exactly one session and hands it to the service."""
    session = object()
    opened = []

    @asynccontextmanager
    async def isolated():
        opened.append(session)
        yield session

    with (
        patch.object(credentials, "get_isolated_db_session", isolated),
        patch.object(credentials, "DecisionSettingsService") as service_cls,
    ):
        service_cls.return_value.credential = AsyncMock(return_value="secret")
        assert await _REAL_DECISION_CREDENTIAL("workspace-a") == "secret"

    assert opened == [session]
    service_cls.assert_called_once_with(session, "workspace-a")


def test_db_layer_no_longer_hosts_the_decisions_helper():
    """src.db must not import src.services; the old proxy module is gone."""
    import importlib.util

    assert importlib.util.find_spec("src.db.decision_context") is None
