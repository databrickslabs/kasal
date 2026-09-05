"""Redirects are held to the same rule as the first URL (audit F09).

``_safe_fetch`` checked where a URL resolved, then let urllib follow a
redirect anywhere — a public page could 302 to ``http://127.0.0.1/…`` and the
internal response came back as the page's content.
"""

import email.message
import http.server
import socketserver
import threading
import urllib.error
import urllib.request
from contextlib import contextmanager
from unittest.mock import patch

import pytest

import src.services.tools.web_fetch as web_fetch


def _handler():
    return web_fetch._SafeRedirects()


class TestEachHopIsChecked:
    def test_a_hop_to_loopback_is_refused(self):
        req = urllib.request.Request("http://example.com/start")
        with pytest.raises(urllib.error.HTTPError, match="redirect refused"):
            _handler().redirect_request(
                req,
                None,
                302,
                "Found",
                email.message.Message(),
                "http://127.0.0.1/private",
            )

    def test_a_relative_hop_is_resolved_before_it_is_checked(self):
        req = urllib.request.Request("http://example.com/a/b")
        with patch.object(web_fetch, "_assert_public_target") as check:
            _handler().redirect_request(
                req, None, 302, "Found", email.message.Message(), "../c"
            )
        check.assert_called_once_with("http://example.com/c")

    def test_credentials_do_not_follow_to_another_origin(self):
        req = urllib.request.Request(
            "http://example.com/a",
            headers={
                "Authorization": "Bearer secret",
                "Cookie": "s=1",
                "Accept": "*/*",
            },
        )
        with patch.object(web_fetch, "_assert_public_target"):
            new = _handler().redirect_request(
                req,
                None,
                302,
                "Found",
                email.message.Message(),
                "http://other.example.net/b",
            )
        assert new.get_header("Authorization") is None
        assert new.get_header("Cookie") is None
        assert new.get_header("Accept") == "*/*"

    def test_credentials_stay_on_the_same_origin(self):
        req = urllib.request.Request(
            "http://example.com/a", headers={"Authorization": "Bearer secret"}
        )
        with patch.object(web_fetch, "_assert_public_target"):
            new = _handler().redirect_request(
                req, None, 302, "Found", email.message.Message(), "http://example.com/b"
            )
        assert new.get_header("Authorization") == "Bearer secret"

    def test_the_hop_count_is_capped(self):
        assert web_fetch._SafeRedirects.max_redirections == 5


@contextmanager
def _redirecting_server():
    """A real local server whose /start 302s to /private on the same host.
    The first address classification is relaxed so the start URL passes; the
    redirect target is classified for real, and 127.0.0.1 is loopback."""

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/start":
                self.send_response(302)
                self.send_header("Location", "/private")
                self.end_headers()
                return
            body = b"<html><body>internal</body></html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
        threading.Thread(target=server.serve_forever, daemon=True).start()
        yield f"http://127.0.0.1:{server.server_address[1]}"
        server.shutdown()


def test_end_to_end_a_public_start_that_redirects_inside_is_refused():
    """The start URL passes the guard (relaxed once, as a public page would);
    the hop it redirects to is checked for real, and 127.0.0.1 is loopback."""
    real = web_fetch._assert_public_target
    calls = {"n": 0}

    def first_pass_then_real(url):
        calls["n"] += 1
        return None if calls["n"] == 1 else real(url)

    with (
        _redirecting_server() as base,
        patch.object(web_fetch, "_assert_public_target", first_pass_then_real),
    ):
        with pytest.raises(RuntimeError, match="redirect refused"):
            web_fetch._safe_fetch(f"{base}/start", headers={}, max_bytes=1000)
    assert calls["n"] == 2
