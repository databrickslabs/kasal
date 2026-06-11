"""
Unit tests for src.utils.url_security (P1 SSRF / token-exfiltration fixes).
"""
import pytest

from src.utils.url_security import (
    is_trusted_databricks_host,
    check_url_structure,
    assert_safe_outbound_url,
    UnsafeUrlError,
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
            "http://hooks.example.com/x",          # not https
            "ftp://example.com",                    # bad scheme
            "https://169.254.169.254/",             # metadata
            "https://localhost/x",                  # loopback name
            "https://10.0.0.5/x",                   # RFC1918
            "https://127.0.0.1/x",                  # loopback
            "https://[::1]/x",                      # ipv6 loopback
            "https://foo.internal/x",               # internal tld
            "https:///nohost",                      # no host
        ],
    )
    def test_rejects(self, url):
        with pytest.raises(UnsafeUrlError):
            check_url_structure(url, require_https=True)


class TestAssertSafeOutboundUrl:
    @pytest.mark.asyncio
    async def test_public_https_ok(self):
        # example.com resolves to public addresses
        assert await assert_safe_outbound_url("https://example.com") == "https://example.com"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "url",
        ["https://localhost", "https://169.254.169.254", "http://example.com"],
    )
    async def test_blocks(self, url):
        with pytest.raises(UnsafeUrlError):
            await assert_safe_outbound_url(url)
