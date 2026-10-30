"""Tests for the shared request-identity resolver (audit C1)."""

import logging

import pytest
from starlette.datastructures import Headers

from src.utils.request_identity import (
    SOURCE_DATABRICKS_APPS,
    SOURCE_FORWARDED,
    SOURCE_NONE,
    SOURCE_OAUTH2_PROXY,
    RequestIdentity,
    UntrustedIdentityHeadersMiddleware,
    identity_from_headers,
    resolve_request_identity,
)

BOTH_FAMILIES = {
    "X-Forwarded-Email": "real@example.com",
    "X-Forwarded-User": "123@456",
    "X-Forwarded-Access-Token": "real-token",
    "X-Auth-Request-Email": "victim@example.com",
    "X-Auth-Request-User": "victim",
    "X-Auth-Request-Access-Token": "spoofed-token",
}


class TestDatabricksAppsMode:
    def test_forwarded_email_wins_when_both_families_are_sent(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_APP_NAME", "kasal")
        identity = identity_from_headers(Headers(BOTH_FAMILIES))

        assert identity.email == "real@example.com"
        assert identity.access_token == "real-token"
        assert identity.user == "123@456"
        assert identity.source == SOURCE_DATABRICKS_APPS

    def test_auth_request_alone_is_no_identity(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_APP_NAME", "kasal")
        identity = resolve_request_identity(
            auth_request_email="victim@example.com",
            auth_request_access_token="spoofed-token",
        )

        assert identity.email is None
        assert identity.access_token is None
        assert identity.source == SOURCE_NONE
        assert not identity.is_authenticated

    def test_forwarded_user_is_never_used_as_email(self):
        identity = resolve_request_identity(
            forwarded_user="123@456", databricks_apps=True
        )
        assert identity.email is None

    def test_ignored_headers_are_logged_without_values(self, caplog):
        with caplog.at_level(logging.WARNING, logger="src.utils.request_identity"):
            resolve_request_identity(
                forwarded_email="real@example.com",
                auth_request_email="victim@example.com",
                auth_request_access_token="spoofed-token",
                databricks_apps=True,
            )
        assert "Ignoring X-Auth-Request-*" in caplog.text
        assert "victim@example.com" not in caplog.text
        assert "spoofed-token" not in caplog.text


class TestOutsideDatabricksApps:
    def test_oauth2_proxy_headers_are_preferred(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
        identity = identity_from_headers(BOTH_FAMILIES)

        assert identity.email == "victim@example.com"
        assert identity.access_token == "spoofed-token"
        assert identity.source == SOURCE_OAUTH2_PROXY

    def test_forwarded_headers_are_the_fallback(self):
        identity = identity_from_headers(
            {"x-forwarded-email": "a@example.com", "x-forwarded-access-token": "t"},
            databricks_apps=False,
        )
        assert identity.email == "a@example.com"
        assert identity.access_token == "t"
        assert identity.source == SOURCE_FORWARDED

    def test_blank_headers_are_absent(self):
        identity = resolve_request_identity(
            forwarded_email="  ", auth_request_email="", databricks_apps=False
        )
        assert identity.email is None
        assert identity.source == SOURCE_NONE


class TestUntrustedIdentityHeadersMiddleware:
    @staticmethod
    async def _run(enabled: bool):
        seen = {}

        async def app(scope, receive, send):
            seen["headers"] = scope["headers"]

        scope = {
            "type": "http",
            "headers": [
                (b"x-forwarded-email", b"real@example.com"),
                (b"x-auth-request-email", b"victim@example.com"),
                (b"X-Auth-Request-Access-Token", b"spoofed-token"),
                (b"group_id", b"g1"),
            ],
        }
        await UntrustedIdentityHeadersMiddleware(app, enabled=enabled)(
            scope, None, None
        )
        return [name.lower() for name, _ in seen["headers"]]

    @pytest.mark.asyncio
    async def test_strips_auth_request_family_in_apps(self):
        names = await self._run(enabled=True)
        assert names == [b"x-forwarded-email", b"group_id"]

    @pytest.mark.asyncio
    async def test_passes_everything_through_outside_apps(self):
        names = await self._run(enabled=False)
        assert b"x-auth-request-email" in names

    def test_enabled_follows_databricks_app_name(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_APP_NAME", "kasal")
        assert UntrustedIdentityHeadersMiddleware(None).enabled is True
        monkeypatch.delenv("DATABRICKS_APP_NAME")
        assert UntrustedIdentityHeadersMiddleware(None).enabled is False


class TestRequestIdentityRepr:
    def test_request_identity_repr_omits_access_token(self):
        identity = RequestIdentity(email="a@example.com", access_token="tok-123")
        assert "tok-123" not in repr(identity)
        assert "a@example.com" in repr(identity)
