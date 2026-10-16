"""The scrape tool's configured cookies and headers go only to its configured site.

``website_url`` in the tool config names the site the cookies and headers were
set up for. The model may still ask the tool to read another URL; that request
is made with the tool's default browser headers only. A redirect from the
configured site to another origin drops them as well.
"""

import http.server
import ipaddress
import logging
import socketserver
import threading
from contextlib import ExitStack, contextmanager
from unittest.mock import patch

import pytest

import src.services.tools.scrape_website as scrape_website
import src.services.tools.web_fetch as web_fetch
from src.services.tools.scrape_website import ScrapeWebsiteTool

SITE = "https://example.com/reports"
COOKIE = {"name": "session", "value": "cookie-value"}
CONFIGURED_HEADERS = {"X-Site-Token": "header-value", "Accept": "text/html"}


def _tool(website_url=SITE, **kwargs):
    return ScrapeWebsiteTool(
        website_url=website_url,
        cookies=COOKIE,
        headers=dict(CONFIGURED_HEADERS),
        **kwargs,
    )


def _sent(tool, **kwargs):
    """Run the tool and return the headers it handed to the fetch."""
    seen = {}

    def fake_fetch(url, headers, timeout=15, max_bytes=None):
        seen["url"] = url
        seen["headers"] = dict(headers)
        return "<html><body><p>ok</p></body></html>"

    with patch.object(scrape_website, "_safe_fetch", fake_fetch):
        tool._run(**kwargs)
    return seen["headers"]


def _carries_configured_credentials(headers):
    names = {name.lower() for name in headers}
    return "cookie" in names or "x-site-token" in names


class TestTheConfiguredSiteGetsThem:
    def test_the_configured_url(self):
        headers = _sent(_tool())

        assert headers["Cookie"] == "session=cookie-value"
        assert headers["X-Site-Token"] == "header-value"

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.com/other/path?q=1",
            "https://EXAMPLE.com/",
            "https://example.com:443/explicit-default-port",
        ],
    )
    def test_another_path_on_the_same_origin(self, url):
        headers = _sent(_tool(), website_url=url)

        assert headers["Cookie"] == "session=cookie-value"
        assert headers["X-Site-Token"] == "header-value"


class TestAnotherOriginDoesNot:
    @pytest.mark.parametrize(
        "url",
        [
            "https://example.net/reports",  # another host
            "https://sub.example.com/reports",  # a subdomain is another host
            "http://example.com/reports",  # same host, another scheme
            "https://example.com:8443/reports",  # same host, another port
            "https://example.com@example.net/reports",  # userinfo, host is example.net
            # Userinfo or a backslash before the configured host: parsers
            # (and proxies) disagree on which host this names.
            "https://example.net@example.com/reports",
            "https://example.net\\@example.com/reports",
            "https://example.net%2F@example.com/reports",
        ],
    )
    def test_the_model_chosen_url_gets_no_cookies_or_headers(self, url):
        headers = _sent(_tool(), website_url=url)

        assert not _carries_configured_credentials(headers), headers

    def test_it_still_reads_like_a_browser(self):
        """The defaults carry no credential, so they still go everywhere."""
        headers = _sent(_tool(), website_url="https://example.net/")

        assert headers["User-Agent"] == ScrapeWebsiteTool().headers["User-Agent"]

    def test_the_skipped_headers_are_logged_by_name_only(self, caplog):
        with caplog.at_level(logging.INFO, logger=scrape_website.__name__):
            _sent(_tool(), website_url="https://example.net/")

        assert "Cookie" in caplog.text
        assert "X-Site-Token" in caplog.text
        assert "cookie-value" not in caplog.text
        assert "header-value" not in caplog.text


class TestNoConfiguredSite:
    """Without ``website_url`` there is no site to bind them to."""

    def test_cookies_are_not_sent(self):
        tool = ScrapeWebsiteTool(cookies=COOKIE)

        headers = _sent(tool, website_url="https://example.com/")

        assert "Cookie" not in headers

    def test_configured_headers_are_not_sent(self):
        tool = ScrapeWebsiteTool(headers=dict(CONFIGURED_HEADERS))

        headers = _sent(tool, website_url="https://example.com/")

        assert "X-Site-Token" not in headers
        assert headers["User-Agent"] == ScrapeWebsiteTool().headers["User-Agent"]

    def test_the_default_headers_are_sent(self):
        assert _sent(ScrapeWebsiteTool(), website_url="https://example.com/") == dict(
            ScrapeWebsiteTool().headers
        )


# --------------------------- redirects, end to end ---------------------------

#: Captured before any patching — a lambda that calls the patched name recurses.
_PUBLIC_IP = ipaddress.ip_address("93.184.216.34")


@contextmanager
def _server(redirect_to=None):
    """A real local server that records each request's headers. With
    ``redirect_to`` set, ``/start`` 302s there."""
    received = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            received.append((self.path, dict(self.headers)))
            if redirect_to and self.path == "/start":
                self.send_response(302)
                self.send_header("Location", redirect_to)
                self.end_headers()
                return
            body = b"<html><body><p>landed</p></body></html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            yield f"http://127.0.0.1:{server.server_address[1]}", received
        finally:
            server.shutdown()


def _lower(headers):
    return {name.lower(): value for name, value in headers.items()}


class TestRedirects:
    """The address CLASSIFICATION is relaxed so 127.0.0.1 passes; the
    request, the redirect and the headers on each hop are real."""

    def test_a_redirect_to_another_origin_drops_them(self):
        with ExitStack() as stack:
            target, landed = stack.enter_context(_server())
            site, started = stack.enter_context(_server(f"{target}/landing"))
            stack.enter_context(
                patch.object(web_fetch.ipaddress, "ip_address", lambda _a: _PUBLIC_IP)
            )
            out = _tool(website_url=f"{site}/start")._run()

        assert "landed" in out
        first = _lower(started[0][1])
        assert first["cookie"] == "session=cookie-value"
        assert first["x-site-token"] == "header-value"
        hop = _lower(landed[0][1])
        assert "cookie" not in hop
        assert "x-site-token" not in hop
        assert hop["accept"] == "text/html"  # an allowlisted header survives

    def test_a_redirect_on_the_same_origin_keeps_them(self):
        with ExitStack() as stack:
            site, received = stack.enter_context(_server("/landing"))
            stack.enter_context(
                patch.object(web_fetch.ipaddress, "ip_address", lambda _a: _PUBLIC_IP)
            )
            _tool(website_url=f"{site}/start")._run()

        assert [path for path, _ in received] == ["/start", "/landing"]
        hop = _lower(received[1][1])
        assert hop["cookie"] == "session=cookie-value"
        assert hop["x-site-token"] == "header-value"
