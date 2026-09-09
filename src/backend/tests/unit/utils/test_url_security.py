"""
Unit tests for src.utils.url_security (P1 SSRF / token-exfiltration fixes).
"""

import pytest

from src.utils.url_security import (
    UnsafeUrlError,
    assert_safe_outbound_url,
    check_url_structure,
    is_trusted_databricks_host,
)


class TestIsTrustedDatabricksHost:
    WS = "https://myws.cloud.databricks.com"

    @pytest.mark.parametrize(
        "url",
        [
            "https://myws.cloud.databricks.com/api/2.0/mcp/x",
            "https://adb-123.4.azuredatabricks.net/api/2.0/mcp/",
            "https://foo.databricksapps.com",
            "https://bar.gcp.databricks.com/api/2.0/mcp/",
        ],
    )
    def test_trusted(self, url):
        assert is_trusted_databricks_host(url, self.WS) is True

    @pytest.mark.parametrize(
        "url",
        [
            "https://attacker.com/collect",
            "http://169.254.169.254/latest/meta-data/",
            # suffix-spoofing must not match
            "https://evil.databricks.com.attacker.com",
            "https://databricks.com.evil.net",
            "",
            None,
        ],
    )
    def test_untrusted(self, url):
        assert is_trusted_databricks_host(url, self.WS) is False

    def test_non_string_workspace_host_is_ignored(self):
        # Defensive: a non-str workspace_host must not raise.
        assert is_trusted_databricks_host("https://x.databricks.com", object()) is True
        assert is_trusted_databricks_host("https://attacker.com", object()) is False


class TestCheckUrlStructure:
    def test_accepts_public_https(self):
        assert check_url_structure("https://hooks.example.com/x") == "hooks.example.com"

    @pytest.mark.parametrize(
        "url",
        [
            "http://hooks.example.com/x",  # not https
            "ftp://example.com",  # bad scheme
            "https://169.254.169.254/",  # metadata
            "https://localhost/x",  # loopback name
            "https://10.0.0.5/x",  # RFC1918
            "https://127.0.0.1/x",  # loopback
            "https://[::1]/x",  # ipv6 loopback
            "https://foo.internal/x",  # internal tld
            "https:///nohost",  # no host
        ],
    )
    def test_rejects(self, url):
        with pytest.raises(UnsafeUrlError):
            check_url_structure(url, require_https=True)


class TestAssertSafeOutboundUrl:
    @pytest.mark.asyncio
    async def test_public_https_ok(self):
        # example.com resolves to public addresses
        assert (
            await assert_safe_outbound_url("https://example.com")
            == "https://example.com"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "url",
        ["https://localhost", "https://169.254.169.254", "http://example.com"],
    )
    async def test_blocks(self, url):
        with pytest.raises(UnsafeUrlError):
            await assert_safe_outbound_url(url)


class TestPublicConnector:
    """Exercise the actual HTTPX adapter, including HTTP host and TLS identity."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "private_ip", ["127.0.0.1", "169.254.169.254", "::1", "10.0.0.1"]
    )
    async def test_dns_rebinding_cannot_reach_private_address(
        self, monkeypatch, private_ip
    ):
        import asyncio
        import socket
        from unittest.mock import AsyncMock

        import httpx

        from src.utils.safe_http import PublicHTTPTransport

        dns = AsyncMock(
            side_effect=[
                [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))],
                [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (private_ip, 443))],
            ]
        )
        monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", dns)
        await assert_safe_outbound_url("https://hooks.example.com")
        transport = PublicHTTPTransport()
        connect = AsyncMock()
        monkeypatch.setattr(
            transport._pool._network_backend._backend, "connect_tcp", connect
        )
        async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
            with pytest.raises(UnsafeUrlError):
                await client.post("https://hooks.example.com", json={"secret": "test"})
        connect.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ip", ["8.8.8.8", "2606:4700:4700::1111"])
    async def test_validated_ip_preserves_host_and_tls_sni(self, monkeypatch, ip):
        import asyncio
        import socket
        from unittest.mock import AsyncMock

        import httpcore
        import httpx

        from src.utils.safe_http import PublicHTTPTransport

        class Stream(httpcore.AsyncNetworkStream):
            def __init__(self):
                self.writes = []
                self.server_hostname = None
                self.response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"

            async def read(self, max_bytes, timeout=None):
                response, self.response = self.response, b""
                return response

            async def write(self, buffer, timeout=None):
                self.writes.append(buffer)

            async def aclose(self):
                pass

            async def start_tls(self, ssl_context, server_hostname=None, timeout=None):
                self.server_hostname = server_hostname
                assert ssl_context.check_hostname
                return self

            def get_extra_info(self, info):
                return None

        monkeypatch.setattr(
            asyncio.get_running_loop(),
            "getaddrinfo",
            AsyncMock(
                return_value=[
                    (
                        socket.AF_INET6 if ":" in ip else socket.AF_INET,
                        socket.SOCK_STREAM,
                        6,
                        "",
                        (ip, 443),
                    ),
                ]
            ),
        )
        monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:3128")
        stream = Stream()
        transport = PublicHTTPTransport()
        connect = AsyncMock(return_value=stream)
        monkeypatch.setattr(
            transport._pool._network_backend._backend, "connect_tcp", connect
        )
        async with httpx.AsyncClient(transport=transport, trust_env=False) as client:
            response = await client.get("https://hooks.example.com/path")
        assert response.text == "OK"
        assert connect.await_args.args == (ip, 443)
        assert stream.server_hostname == "hooks.example.com"
        assert b"Host: hooks.example.com\r\n" in b"".join(stream.writes)

    @pytest.mark.asyncio
    async def test_dns_timeout_is_bounded(self, monkeypatch):
        import asyncio

        import httpcore

        from src.utils.safe_http import PublicNetworkBackend

        async def never_resolves(*args, **kwargs):
            await asyncio.Event().wait()

        monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", never_resolves)
        with pytest.raises(httpcore.ConnectTimeout):
            await PublicNetworkBackend().connect_tcp(
                "hooks.example.com", 443, timeout=0.01
            )

    @pytest.mark.asyncio
    async def test_aiohttp_rejects_mixed_public_private_answers(self, monkeypatch):
        from unittest.mock import AsyncMock

        from src.utils.safe_http import PublicResolver

        resolver = PublicResolver()
        monkeypatch.setattr(
            resolver._resolver,
            "resolve",
            AsyncMock(
                return_value=[
                    {"host": "8.8.8.8"},
                    {"host": "127.0.0.1"},
                ]
            ),
        )
        try:
            with pytest.raises(UnsafeUrlError):
                await resolver.resolve("hooks.example.com", 443)
        finally:
            await resolver.close()
