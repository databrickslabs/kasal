"""Connection-disposal diagnostics must not hide failed transactions.

A closed connection can mean an uncommitted write was lost. Both Lakebase
session paths must propagate that primary failure, even if rollback also fails.
"""

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize("crew_thread", [False, True])
@pytest.mark.parametrize("rollback_fails", [False, True])
async def test_lakebase_commit_failure_is_not_suppressed(crew_thread, rollback_fails):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, patch

    from src.db import lakebase_session as module

    primary = RuntimeError("connection is closed")
    session = AsyncMock()
    session.commit.side_effect = primary
    if rollback_fails:
        session.rollback.side_effect = RuntimeError("rollback also failed")

    @asynccontextmanager
    async def session_context():
        yield session

    factory = SimpleNamespace(
        instance_name="audit", user_email=None, get_session=session_context
    )
    with (
        patch.object(module, "_is_crew_thread", return_value=crew_thread),
        patch.object(module, "_lakebase_factory", factory),
        patch.object(module, "_thread_local", SimpleNamespace(factory=factory)),
        pytest.raises(RuntimeError) as caught,
    ):
        async with module.get_lakebase_session(instance_name="audit") as actual:
            assert actual is session
    assert caught.value is primary
    session.rollback.assert_awaited_once()
    session.close.assert_awaited_once()
