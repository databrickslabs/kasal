import json
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.core.databricks_app import DatabricksAppInstallation
from src.core.exceptions import KasalError
from src.services.groups.directory import search_directory


@pytest.fixture(autouse=True)
def local_installation():
    with patch(
        "src.services.groups.directory.DatabricksAppInstallation.from_env",
        return_value=DatabricksAppInstallation(hosted=False),
    ):
        yield


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
    assert "HTTP=unavailable" in error.value.detail
    assert error.value.detail.count("exact sign-in email") == 1


@pytest.mark.asyncio
async def test_hosted_directory_uses_app_identity_not_forwarded_token_or_team_pat():
    installation = DatabricksAppInstallation(
        hosted=True, host="https://installed-workspace.example.com"
    )
    response = httpx.Response(
        200,
        request=httpx.Request("GET", installation.host),
        json={"Resources": [{"userName": "new@example.com"}]},
    )
    client = AsyncMock()
    client.get.return_value = response
    config = MagicMock()
    config.authenticate.return_value = {"Authorization": "Bearer app-token"}
    with (
        patch(
            "src.services.groups.directory.DatabricksAppInstallation.from_env",
            return_value=installation,
        ),
        patch(
            "src.services.groups.directory.Config", return_value=config
        ) as config_factory,
        patch(
            "src.services.groups.directory.get_auth_context", AsyncMock()
        ) as unified_auth,
        patch("src.services.groups.directory.httpx.AsyncClient") as factory,
    ):
        factory.return_value.__aenter__.return_value = client
        result = await search_directory("new", "restricted-forwarded-token")
    assert result[0].email == "new@example.com"
    config_factory.assert_called_once_with(
        host=installation.host,
        auth_type="oauth-m2m",
        http_timeout_seconds=10,
        retry_timeout_seconds=10,
    )
    unified_auth.assert_not_awaited()
    assert (
        client.get.call_args.args[0]
        == installation.host + "/api/2.0/preview/scim/v2/Users"
    )
    assert client.get.call_args.kwargs["headers"] == {
        "Authorization": "Bearer app-token"
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,expected",
    [
        (401, "rejected"),
        (403, "app's service principal"),
        (429, "rate limited"),
        (400, "HTTP 400"),
        (500, "HTTP 500"),
    ],
)
async def test_http_errors_identify_failure_without_exposing_remote_body(
    status, expected, caplog
):
    installation = DatabricksAppInstallation(
        hosted=True, host="https://installed-workspace.example.com"
    )
    response = httpx.Response(
        status,
        request=httpx.Request("GET", installation.host),
        json={"message": "sensitive remote details"},
    )
    client = AsyncMock()
    client.get.return_value = response
    with (
        patch(
            "src.services.groups.directory.DatabricksAppInstallation.from_env",
            return_value=installation,
        ),
        patch("src.services.groups.directory.Config") as config,
        patch("src.services.groups.directory.httpx.AsyncClient") as factory,
    ):
        config.return_value.authenticate.return_value = {
            "Authorization": "Bearer secret"
        }
        factory.return_value.__aenter__.return_value = client
        with pytest.raises(KasalError, match=expected) as error:
            await search_directory("private-search", "forwarded")
    assert error.value.status_code == 503
    assert "exact sign-in email" in str(error.value)
    assert "sensitive remote details" not in str(error.value) + caplog.text
    assert "private-search" not in caplog.text
    assert f"status={status} auth=app" in caplog.text
    diagnostic = re.search(r"DIR-v3/[a-f0-9]{12}", error.value.detail).group()
    assert diagnostic in caplog.text
    assert "stage=directory_request" in error.value.detail
    assert f"HTTP={status}" in error.value.detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,expected",
    [
        (httpx.ReadTimeout("timeout"), "timed out"),
        (httpx.ConnectError("unreachable"), "Cannot reach"),
    ],
)
async def test_transport_errors_do_not_claim_permission_denied(failure, expected):
    auth = SimpleNamespace(
        workspace_url="https://workspace.example.com", get_headers=lambda: {}
    )
    client = AsyncMock()
    client.get.side_effect = failure
    with (
        patch(
            "src.services.groups.directory.get_auth_context",
            AsyncMock(return_value=auth),
        ),
        patch("src.services.groups.directory.httpx.AsyncClient") as factory,
    ):
        factory.return_value.__aenter__.return_value = client
        with pytest.raises(KasalError, match=expected):
            await search_directory("new", None)


@pytest.mark.asyncio
@pytest.mark.parametrize("wrapped", [False, True])
async def test_authentication_http_failure_preserves_status_without_claiming_directory_denied(
    wrapped, caplog
):
    response = httpx.Response(
        401,
        request=httpx.Request("POST", "https://workspace.example/oidc/v1/token"),
    )
    failure = httpx.HTTPStatusError(
        "secret-token private-search", request=response.request, response=response
    )
    if wrapped:
        wrapper = ValueError("SDK config contained secret-token")
        wrapper.__cause__ = failure
        failure = wrapper
    with (
        patch(
            "src.services.groups.directory.DatabricksAppInstallation.from_env",
            return_value=DatabricksAppInstallation(
                hosted=True, host="https://workspace.example"
            ),
        ),
        patch("src.services.groups.directory.Config", side_effect=failure),
        patch("src.services.groups.directory.httpx.AsyncClient") as client,
    ):
        with pytest.raises(KasalError) as error:
            await search_directory("private-search", None)
    client.assert_not_called()
    assert "before the directory was queried" in error.value.detail
    assert "stage=authentication; HTTP=401" in error.value.detail
    assert "stage=authentication status=401 auth=app" in caplog.text
    assert "secret-token" not in error.value.detail + caplog.text
    assert "private-search" not in error.value.detail + caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("body", ["not-json secret-token", "[]"])
async def test_invalid_response_reports_response_stage_and_unique_search_reference(
    body, caplog
):
    auth = SimpleNamespace(
        workspace_url="https://workspace.example", get_headers=lambda: {}
    )
    response = httpx.Response(
        200, request=httpx.Request("GET", auth.workspace_url), text=body
    )
    client = AsyncMock()
    client.get.return_value = response
    references = []
    with (
        patch(
            "src.services.groups.directory.get_auth_context",
            AsyncMock(return_value=auth),
        ),
        patch("src.services.groups.directory.httpx.AsyncClient") as factory,
    ):
        factory.return_value.__aenter__.return_value = client
        for _ in range(2):
            with pytest.raises(KasalError) as error:
                await search_directory("private-search", None)
            assert "stage=directory_response; HTTP=200" in error.value.detail
            references.append(
                re.search(r"DIR-v3/[a-f0-9]{12}", error.value.detail).group()
            )
    assert len(set(references)) == 2
    assert all(reference in caplog.text for reference in references)
    assert "secret-token" not in caplog.text
    assert "private-search" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload,expected",
    [
        (
            {
                "Resources": [{"id": "123", "displayName": "Existing Person"}],
                "totalResults": 1,
            },
            "none provided an active, usable sign-in email",
        ),
        (
            {
                "Resources": [{"userName": "disabled@example.com", "active": False}],
                "totalResults": 1,
            },
            "were inactive",
        ),
        ({"message": "private upstream response"}, "could not initialize or read"),
        ({"totalResults": 1}, "could not initialize or read"),
    ],
)
async def test_unusable_or_unexpected_response_is_not_reported_as_no_matches(
    payload, expected, caplog
):
    auth = SimpleNamespace(
        workspace_url="https://workspace.example", get_headers=lambda: {}
    )
    client = AsyncMock()
    client.get.return_value = httpx.Response(
        200, request=httpx.Request("GET", auth.workspace_url), json=payload
    )
    with (
        patch(
            "src.services.groups.directory.get_auth_context",
            AsyncMock(return_value=auth),
        ),
        patch("src.services.groups.directory.httpx.AsyncClient") as factory,
    ):
        factory.return_value.__aenter__.return_value = client
        with pytest.raises(KasalError, match=expected) as error:
            await search_directory("Existing Person", None)
    assert "HTTP=200" in error.value.detail
    assert "Existing Person" not in caplog.text
    assert "private upstream response" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload", [{"Resources": [], "totalResults": 0}, {"totalResults": 0}]
)
async def test_true_empty_results_log_completion_counts(payload, caplog):
    caplog.set_level("INFO", logger="src.services.groups.directory")
    auth = SimpleNamespace(
        workspace_url="https://workspace.example", get_headers=lambda: {}
    )
    client = AsyncMock()
    client.get.return_value = httpx.Response(
        200, request=httpx.Request("GET", auth.workspace_url), json=payload
    )
    with (
        patch(
            "src.services.groups.directory.get_auth_context",
            AsyncMock(return_value=auth),
        ),
        patch("src.services.groups.directory.httpx.AsyncClient") as factory,
    ):
        factory.return_value.__aenter__.return_value = client
        assert await search_directory("not-present", None) == []
    assert "User directory search completed: diagnostic=DIR-v3/" in caplog.text
    assert "received=0 returned=0 inactive=0 unusable=0" in caplog.text
    assert "not-present" not in caplog.text


@pytest.mark.asyncio
async def test_search_continues_past_inactive_first_page():
    auth = SimpleNamespace(
        workspace_url="https://workspace.example", get_headers=lambda: {}
    )
    client = AsyncMock()
    client.get.side_effect = [
        httpx.Response(
            200,
            request=httpx.Request("GET", auth.workspace_url),
            json={"Resources": [{"active": False}] * 20, "totalResults": 21},
        ),
        httpx.Response(
            200,
            request=httpx.Request("GET", auth.workspace_url),
            json={"Resources": [{"userName": "found@example.com"}], "totalResults": 21},
        ),
    ]
    with (
        patch(
            "src.services.groups.directory.get_auth_context",
            AsyncMock(return_value=auth),
        ),
        patch("src.services.groups.directory.httpx.AsyncClient") as factory,
    ):
        factory.return_value.__aenter__.return_value = client
        result = await search_directory("found", None)
    assert [person.email for person in result] == ["found@example.com"]
    assert [
        call.kwargs["params"]["startIndex"] for call in client.get.call_args_list
    ] == [1, 21]


@pytest.mark.asyncio
async def test_directory_that_repeats_unusable_pages_is_bounded():
    auth = SimpleNamespace(
        workspace_url="https://workspace.example", get_headers=lambda: {}
    )
    client = AsyncMock()
    client.get.return_value = httpx.Response(
        200,
        request=httpx.Request("GET", auth.workspace_url),
        json={"Resources": [{"id": "123"}] * 20, "totalResults": 999},
    )
    with (
        patch(
            "src.services.groups.directory.get_auth_context",
            AsyncMock(return_value=auth),
        ),
        patch("src.services.groups.directory.httpx.AsyncClient") as factory,
    ):
        factory.return_value.__aenter__.return_value = client
        with pytest.raises(KasalError, match="usable sign-in email"):
            await search_directory("found", None)
    assert client.get.call_count == 5
