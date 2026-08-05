"""The Lakebase router must not resolve the PAT chain on a deployed app.

Lakebase authenticates as the APP'S SERVICE PRINCIPAL, from environment
variables. ``LakebaseConnectionService.get_workspace_client`` requires
``DATABRICKS_CLIENT_ID``/``SECRET``/``HOST`` for the ``postgres`` scope and
deliberately STRIPS ``DATABRICKS_TOKEN``/``DATABRICKS_API_KEY`` first, because a
PAT alongside SPN makes the SDK raise "more than one authorization method".
``get_username()`` likewise prefers the SPN client_id. So the PAT contributes
nothing to a Lakebase connection when an SP is configured.

It also cannot be consulted here, because ``get_auth_context()`` reads the
``apikey`` table. Once that read goes through the router, the two call each other:

    get_auth_context → get_smart_db_session → get_auth_context → …

The deployed app logged 1,287 "maximum recursion depth exceeded" over 80 minutes.
Chat kept working — its process had a warm PAT cache — while every crew and flow
SUBPROCESS died, because a spawned interpreter starts with a cold cache AND is the
only place that calls ``activate_lakebase_in_subprocess()``. That asymmetry
(builders broken, chat fine) is what identified the cycle.

Local dev still needs an identity: with no SPN configured the connection falls
back to PAT/OBO. That path has no Lakebase config to fetch through the router, so
there is no cycle to close — hence the gate is "is an SP configured", not
"never".
"""

import os
from contextlib import ExitStack, asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import src.db.database_router as router


@asynccontextmanager
async def _fake_lakebase_session(*args, **kwargs):
    yield MagicMock(name="lakebase_session")


def _lakebase_enabled(stack: ExitStack) -> None:
    """Put the router on its Lakebase branch for the duration of ``stack``."""
    stack.enter_context(
        patch.object(router, "is_lakebase_enabled", AsyncMock(return_value=True))
    )
    stack.enter_context(
        patch.object(
            router,
            "get_lakebase_config_from_db",
            AsyncMock(return_value={"instance_name": "kasalnt", "endpoint": "h"}),
        )
    )
    stack.enter_context(
        patch.object(router, "get_lakebase_session", _fake_lakebase_session)
    )


@pytest.mark.asyncio
class TestDeployedApp:
    async def test_auth_is_never_consulted_when_an_sp_is_configured(self):
        """THE fix: zero calls, so the cycle cannot form at all."""
        auth = AsyncMock(return_value=MagicMock(token="t", user_identity="u"))
        with ExitStack() as stack:
            stack.enter_context(
                patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "spn-client-id"})
            )
            stack.enter_context(
                patch("src.utils.databricks_auth.get_auth_context", auth)
            )
            _lakebase_enabled(stack)
            async for session in router.get_smart_db_session():
                assert session is not None
                break

        auth.assert_not_awaited()

    async def test_the_session_is_still_a_lakebase_one(self):
        """Skipping the PAT must not quietly fall back to the local database."""
        with ExitStack() as stack:
            stack.enter_context(
                patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "spn-client-id"})
            )
            stack.enter_context(
                patch("src.utils.databricks_auth.get_auth_context", AsyncMock())
            )
            _lakebase_enabled(stack)
            async for session in router.get_smart_db_session():
                assert "lakebase_session" in repr(session)
                break


@pytest.mark.asyncio
class TestLocalDev:
    async def test_auth_is_consulted_exactly_once_without_an_sp(self):
        """No SPN means PAT/OBO is the only identity available — still needed."""
        auth = AsyncMock(
            return_value=MagicMock(
                token="pat", user_identity="dev@example.com", auth_method="PAT"
            )
        )
        env = {k: v for k, v in os.environ.items() if k != "DATABRICKS_CLIENT_ID"}
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, env, clear=True))
            stack.enter_context(
                patch("src.utils.databricks_auth.get_auth_context", auth)
            )
            _lakebase_enabled(stack)
            async for _ in router.get_smart_db_session():
                break

        auth.assert_awaited_once()

    async def test_an_auth_failure_does_not_block_the_session(self):
        """Best-effort: the SDK can still authenticate from ~/.databrickscfg."""
        env = {k: v for k, v in os.environ.items() if k != "DATABRICKS_CLIENT_ID"}
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, env, clear=True))
            stack.enter_context(
                patch(
                    "src.utils.databricks_auth.get_auth_context",
                    AsyncMock(side_effect=Exception("no PAT configured")),
                )
            )
            _lakebase_enabled(stack)
            async for session in router.get_smart_db_session():
                assert session is not None
                break
