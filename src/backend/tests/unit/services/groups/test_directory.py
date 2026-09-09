import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from src.core.exceptions import KasalError
from src.services.groups.directory import search_directory


@pytest.mark.asyncio
async def test_directory_quotes_filters_bounds_results_and_skips_inactive():
    auth = SimpleNamespace(
        workspace_url="https://workspace.example.com",
        get_headers=lambda: {"Authorization": "Bearer fake"},
    )
    response = httpx.Response(
        200,
        request=httpx.Request("GET", auth.workspace_url),
        json={
            "Resources": [
                {
                    "userName": "new@example.com",
                    "displayName": "New Person",
                    "active": True,
                },
                {"userName": "disabled@example.com", "active": False},
                {"userName": "invalid"},
            ]
        },
    )
    client = AsyncMock()
    client.get.return_value = response
    query = 'a" or active eq true'
    with (
        patch(
            "src.services.groups.directory.get_auth_context",
            AsyncMock(return_value=auth),
        ) as get_auth,
        patch("src.services.groups.directory.httpx.AsyncClient") as factory,
    ):
        factory.return_value.__aenter__.return_value = client
        result = await search_directory(query, "caller-token")
    assert [person.email for person in result] == ["new@example.com"]
    get_auth.assert_awaited_once_with(user_token="caller-token")
    params = client.get.call_args.kwargs["params"]
    assert (
        params["filter"]
        == f"userName co {json.dumps(query)} or displayName co {json.dumps(query)}"
    )
    assert params["count"] == 20


@pytest.mark.asyncio
async def test_unavailable_directory_returns_actionable_error():
    with patch(
        "src.services.groups.directory.get_auth_context", AsyncMock(return_value=None)
    ):
        with pytest.raises(KasalError, match="exact sign-in email") as error:
            await search_directory("ada", None)
    assert error.value.status_code == 503
