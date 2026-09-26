"""Audit C1 regression: inside Databricks Apps, X-Forwarded-Email wins everywhere.

Each surface that turns headers into a caller is exercised with BOTH header
families present. Inside Apps the X-Auth-Request-* family is client-supplied,
so the caller must resolve to the X-Forwarded-Email identity.
"""

import importlib
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.mcp_jsonrpc_router import router as mcp_jsonrpc_router
from src.core.exceptions import UnauthorizedError
from src.dependencies.providers import get_request_email, get_smart_db_session
from src.services.external.identity import ExternalAuthError

# ``src.api`` re-exports each module's ``router`` under the module's own name,
# so import the MODULES explicitly.
a2a_router = importlib.import_module("src.api.a2a_router")
mcp_server_router = importlib.import_module("src.api.mcp_server_router")

SPOOF = {
    "x_forwarded_email": "real@example.com",
    "x_forwarded_access_token": "real-token",
    "x_auth_request_email": "victim@example.com",
    "x_auth_request_access_token": "spoofed-token",
    "x_group_id": None,
}


@pytest.fixture
def in_apps(monkeypatch):
    monkeypatch.setenv("DATABRICKS_APP_NAME", "kasal")


@pytest.fixture
def outside_apps(monkeypatch):
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "module, dependency, protocol",
    [
        (a2a_router, "get_a2a_caller", "a2a"),
        (mcp_server_router, "get_external_caller", "mcp"),
    ],
)
async def test_external_caller_uses_forwarded_identity_in_apps(
    in_apps, module, dependency, protocol
):
    with patch.object(module, "resolve_caller", new=AsyncMock()) as resolve:
        await getattr(module, dependency)(**SPOOF)

    resolve.assert_awaited_once_with(
        protocol=protocol,
        email="real@example.com",
        access_token="real-token",
        group_id=None,
    )


@pytest.mark.asyncio
async def test_a2a_caller_keeps_oauth2_proxy_precedence_outside_apps(outside_apps):
    with patch.object(a2a_router, "resolve_caller", new=AsyncMock()) as resolve:
        await a2a_router.get_a2a_caller(**SPOOF)

    assert resolve.await_args.kwargs["email"] == "victim@example.com"


@pytest.mark.parametrize("method", ["post", "get"])
def test_mcp_jsonrpc_uses_forwarded_identity_in_apps(in_apps, method):
    app = FastAPI()
    app.include_router(mcp_jsonrpc_router)
    app.dependency_overrides[get_smart_db_session] = lambda: None
    client = TestClient(app, raise_server_exceptions=False)
    headers = {
        "X-Forwarded-Email": "real@example.com",
        "X-Forwarded-Access-Token": "real-token",
        "X-Auth-Request-Email": "victim@example.com",
        "X-Auth-Request-Access-Token": "spoofed-token",
        "Accept": "text/event-stream",
    }
    # Refuse after recording the arguments: only the identity choice matters.
    with patch(
        "src.api.mcp_jsonrpc_router._resolve",
        new=AsyncMock(side_effect=ExternalAuthError("stop")),
    ) as resolve:
        if method == "post":
            client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
                headers=headers,
            )
        else:
            client.get("/mcp", headers=headers)

    resolve.assert_awaited_once_with(
        email="real@example.com", token="real-token", group_id=None
    )


@pytest.mark.asyncio
async def test_request_email_dependency_uses_forwarded_email_in_apps(in_apps):
    email = await get_request_email(
        x_forwarded_email="real@example.com",
        x_auth_request_email="victim@example.com",
    )
    assert email == "real@example.com"


@pytest.mark.asyncio
async def test_request_email_dependency_refuses_without_identity(in_apps):
    with pytest.raises(UnauthorizedError):
        await get_request_email(
            x_forwarded_email=None, x_auth_request_email="victim@example.com"
        )
