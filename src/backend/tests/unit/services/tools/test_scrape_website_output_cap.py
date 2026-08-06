"""ScrapeWebsiteTool bounds what it returns, and what it downloads.

A scraped page went straight into the conversation with no ceiling anywhere: the
transport clamps the model's OUTPUT budget, not tool RESULTS, and nothing between
the tool and the message trims them. Measured against a real 435 KB page, the tool
returned **432,048 characters** — roughly 100k tokens from a single call. One long
article could crowd out the task, the earlier turns and every other tool's result,
and on a big enough page it exceeds the context window outright, failing the task
with a token error instead of a truncated read.

Two independent limits, because they solve different problems:

* ``max_chars`` — what the MODEL sees. This is the context-window protection.
* ``max_fetch_bytes`` — what is DOWNLOADED. HTML markup dwarfs the text it
  carries, so without this a multi-megabyte page is still pulled into memory and
  decoded in full just to extract a few thousand characters.

Both are overridable per instance and via the environment, and ``max_chars=0``
restores the old unbounded behaviour for anyone who depends on it.
"""

import http.server
import ipaddress
import socketserver
import threading
from contextlib import contextmanager
from unittest.mock import patch

import pytest

import src.services.tools.web_fetch as web_fetch
from src.services.tools.scrape_website import (
    _DEFAULT_MAX_CHARS,
    _DEFAULT_MAX_FETCH_BYTES,
    ScrapeWebsiteTool,
    _env_int,
)

#: Captured before any patching — a lambda that calls the patched name recurses.
_PUBLIC_IP = ipaddress.ip_address("93.184.216.34")

_PARAGRAPH = "<p>" + ("lorem ipsum dolor sit amet " * 40) + "</p>"
BIG_PAGE = "<html><body>" + _PARAGRAPH * 400 + "</body></html>"


@contextmanager
def serving(html: str):
    """A real local HTTP server, with the SSRF guard told the host is public.

    Only the address CLASSIFICATION is relaxed; resolution stays real, so the
    request genuinely goes over HTTP and the read limit is exercised for real
    rather than against a mocked response object.
    """

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = html.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass  # the client stopped reading early — that is the point

        def log_message(self, *args):
            pass

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
        threading.Thread(target=server.serve_forever, daemon=True).start()
        url = f"http://127.0.0.1:{server.server_address[1]}/page"
        with patch.object(web_fetch.ipaddress, "ip_address", lambda _a: _PUBLIC_IP):
            yield url
        server.shutdown()


class TestTheOutputIsCapped:
    def test_a_long_page_is_truncated_to_the_default(self):
        """THE bug: 435 KB of HTML previously returned 432k characters."""
        with serving(BIG_PAGE) as url:
            out = ScrapeWebsiteTool()._run(website_url=url)
        assert len(out) < _DEFAULT_MAX_CHARS + 500, len(out)

    def test_it_says_that_it_truncated(self):
        """Cutting a page silently reads as the page ENDING there.

        An agent then reports the remainder as absent instead of fetching a more
        specific URL — worse than a shorter answer, because it is confidently wrong.
        """
        with serving(BIG_PAGE) as url:
            out = ScrapeWebsiteTool(max_chars=800)._run(website_url=url)
        assert "[Truncated:" in out
        assert "characters omitted" in out
        # And it says what to do about it.
        assert "more specific URL" in out

    def test_a_short_page_is_untouched(self):
        with serving("<html><body><p>hello world</p></body></html>") as url:
            out = ScrapeWebsiteTool()._run(website_url=url)
        assert "[Truncated:" not in out
        assert "hello world" in out

    def test_the_content_prefix_survives_truncation(self):
        """Callers key off this line; truncation must not drop it."""
        with serving(BIG_PAGE) as url:
            out = ScrapeWebsiteTool(max_chars=200)._run(website_url=url)
        assert out.startswith("The following text is scraped website content:")

    def test_zero_disables_the_cap(self):
        """An escape hatch, so the change cannot break a workflow that needs it."""
        with serving(BIG_PAGE) as url:
            out = ScrapeWebsiteTool(max_chars=0)._run(website_url=url)
        assert len(out) > _DEFAULT_MAX_CHARS
        assert "[Truncated:" not in out


class TestTheDownloadIsCapped:
    def test_only_the_capped_bytes_are_read(self):
        """Independent of max_chars: markup dwarfs the text it carries.

        A 4 MB page must not be pulled into memory to extract a few thousand
        characters, so the ceiling is on the transfer as well as the output.
        """
        huge = "<html><body>" + "<p>x</p>" * 500_000 + "</body></html>"
        with serving(huge) as url:
            out = ScrapeWebsiteTool(max_fetch_bytes=50_000, max_chars=0)._run(
                website_url=url
            )
        # Text extracted from ≤50 KB of markup, not from 4 MB.
        assert len(out) < 60_000, len(out)


class TestConfiguration:
    def test_the_defaults_are_sane(self):
        tool = ScrapeWebsiteTool()
        assert tool.max_chars == _DEFAULT_MAX_CHARS
        assert tool.max_fetch_bytes == _DEFAULT_MAX_FETCH_BYTES

    def test_an_instance_can_override(self):
        assert ScrapeWebsiteTool(max_chars=1234).max_chars == 1234

    def test_the_environment_can_override(self, monkeypatch):
        monkeypatch.setenv("SCRAPE_WEBSITE_MAX_CHARS", "4321")
        assert ScrapeWebsiteTool().max_chars == 4321

    @pytest.mark.parametrize("value", ["", "abc", "-5", "0", "1.5"])
    def test_a_bad_env_value_falls_back_to_the_default(self, monkeypatch, value):
        """A typo in config must not silently disable the protection."""
        monkeypatch.setenv("SCRAPE_WEBSITE_MAX_CHARS", value)
        assert ScrapeWebsiteTool().max_chars == _DEFAULT_MAX_CHARS

    def test_env_int_is_positive_only(self):
        assert _env_int("KASAL_NO_SUCH_VAR_XYZ", 99) == 99


class TestSafeFetchStillGuards:
    """The read limit must not weaken the SSRF checks it sits inside."""

    def test_private_addresses_are_still_refused(self):
        with pytest.raises(ValueError, match="private/internal"):
            web_fetch._safe_fetch("http://127.0.0.1/x", headers={}, max_bytes=1000)

    def test_non_http_schemes_are_still_refused(self):
        with pytest.raises(ValueError, match="Unsupported URL scheme"):
            web_fetch._safe_fetch("file:///etc/passwd", headers={}, max_bytes=1000)

    def test_no_limit_keeps_the_previous_behaviour(self):
        """Other callers pass no max_bytes and must be unaffected."""
        with serving("<html><body><p>ok</p></body></html>") as url:
            body = web_fetch._safe_fetch(url, headers={})
        assert "ok" in body
