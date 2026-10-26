"""The process-wide DatabricksAuth singleton must never keep a user's token.

`get_databricks_auth_headers(user_token=...)` used to store the token on the
module singleton, and nothing cleared it, so the next caller that passed no
token (the periodic memory sweep, the Genie MCP fallback) was served as the
last user who had passed one — across users and teamspaces.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.utils import databricks_auth
from src.utils.databricks_auth import DatabricksAuth, get_databricks_auth_headers


def _unconfigured_auth() -> DatabricksAuth:
    """An instance with no PAT and no SPN, so OBO is the only way to a header."""
    auth = DatabricksAuth()
    auth._config_loaded = True
    auth._api_token = None
    auth._client_id = None
    auth._client_secret = None
    return auth


@pytest.mark.asyncio
async def test_tokenless_call_after_a_user_call_is_not_served_as_that_user():
    auth = _unconfigured_auth()
    with patch.object(auth, "_load_config", new_callable=AsyncMock, return_value=True):
        first, _ = await auth.get_auth_headers(user_token="user-a-token")
        second, err = await auth.get_auth_headers()

    assert first["Authorization"] == "Bearer user-a-token"
    assert second is None
    assert "No Databricks authentication method available" in err
    assert auth._user_access_token is None


@pytest.mark.asyncio
async def test_two_users_each_get_their_own_token():
    auth = _unconfigured_auth()
    with patch.object(auth, "_load_config", new_callable=AsyncMock, return_value=True):
        a, _ = await auth.get_auth_headers(user_token="user-a-token")
        b, _ = await auth.get_auth_headers(user_token="user-b-token")

    assert a["Authorization"] == "Bearer user-a-token"
    assert b["Authorization"] == "Bearer user-b-token"


@pytest.mark.asyncio
async def test_module_helper_does_not_leak_through_the_singleton():
    """The real path: the sweep calls get_databricks_auth_headers() with no token."""
    singleton = _unconfigured_auth()
    with (
        patch.object(databricks_auth, "_databricks_auth", singleton),
        patch.object(
            singleton, "_load_config", new_callable=AsyncMock, return_value=True
        ),
    ):
        await get_databricks_auth_headers(user_token="user-a-token")
        headers, _ = await get_databricks_auth_headers()

    assert headers is None
    assert singleton._user_access_token is None


@pytest.mark.asyncio
async def test_private_instance_with_a_set_token_still_uses_it():
    """get_mcp_auth_headers builds a fresh instance and sets the token on it."""
    auth = _unconfigured_auth()
    auth.set_user_access_token("private-token")
    with patch.object(auth, "_load_config", new_callable=AsyncMock, return_value=True):
        headers, _ = await auth.get_auth_headers()

    assert headers["Authorization"] == "Bearer private-token"
