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
        # The start URL passes (as a public page would) and is pinned to the
        # local server; the hop it redirects to is checked for real.
        calls["n"] += 1
        return "127.0.0.1" if calls["n"] == 1 else real(url)

    with (
        _redirecting_server() as base,
        patch.object(web_fetch, "_assert_public_target", first_pass_then_real),
    ):
        with pytest.raises(RuntimeError, match="redirect refused"):
            web_fetch._safe_fetch(f"{base}/start", headers={}, max_bytes=1000)
    assert calls["n"] == 2


class TestTheConnectionGoesWhereTheCheckLooked:
    """R2-08. The check resolved the name once and urllib resolved it again to
    connect: a name that answered public first and private second reached the
    private address. The transport now connects to the validated address."""

    def test_the_socket_is_opened_to_the_validated_address(self, monkeypatch):
        answers = iter(
            [[(2, 1, 6, "", ("93.184.216.34", 0))], [(2, 1, 6, "", ("127.0.0.1", 0))]]
        )
        monkeypatch.setattr(
            web_fetch.socket, "getaddrinfo", lambda *a, **k: next(answers)
        )
        connected = {}

        def fake_create_connection(address, timeout=None, source_address=None):
            connected["address"] = address
            raise OSError("no network in tests")

        monkeypatch.setattr(
            web_fetch.socket, "create_connection", fake_create_connection
        )
        with pytest.raises(RuntimeError):
            web_fetch._safe_fetch(
                "http://rebinding.example.com/x", headers={}, max_bytes=100
            )
        # The one validated answer; the name is never resolved a second time.
        assert connected["address"] == ("93.184.216.34", 80)
        assert next(answers, None) is not None  # the loopback answer was never consumed

    def test_a_private_first_answer_never_connects(self, monkeypatch):
        monkeypatch.setattr(
            web_fetch.socket,
            "getaddrinfo",
            lambda *a, **k: [(2, 1, 6, "", ("10.0.0.5", 0))],
        )
        touched = []
        monkeypatch.setattr(
            web_fetch.socket, "create_connection", lambda *a, **k: touched.append(a)
        )
        with pytest.raises(ValueError, match="private/internal"):
            web_fetch._safe_fetch("http://internal.example.com/x", headers={})
        assert touched == []


def test_a_redirect_to_a_file_url_has_nowhere_to_go():
    """The opener carries HTTP handlers only: a file: hop is refused as a
    redirect, and could not be served even if it were not."""
    req = urllib.request.Request("http://example.com/a")
    with pytest.raises(urllib.error.HTTPError, match="redirect refused"):
        _handler().redirect_request(
            req, None, 302, "Found", email.message.Message(), "file:///etc/passwd"
        )
    captured = {}

    def capture(self, request, timeout=None):
        captured["handlers"] = [type(h).__name__ for h in self.handlers]
        raise urllib.error.URLError("stop here")

    with patch.object(web_fetch.urllib.request.OpenerDirector, "open", capture):
        with pytest.raises(urllib.error.URLError):
            web_fetch._open(req, timeout=1)
    assert not any(
        name.startswith(("File", "FTP", "Data")) for name in captured["handlers"]
    )
    assert "_PinnedHandler" in captured["handlers"]
