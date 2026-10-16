"""``_http_json`` (the Serper and Image Generation POST) goes through the same
guarded opener as ``_safe_fetch``: http(s) only, no private, loopback,
link-local or metadata address, and the connection is made to the address that
was checked. Nothing here touches the network: DNS and the socket are stubbed.
"""

import email.message
import urllib.request

import pytest

import src.services.tools.web_fetch as web_fetch


@pytest.fixture
def connections(monkeypatch):
    """Record every connection attempt instead of making it."""
    attempts = []

    def fake_create_connection(address, timeout=None, source_address=None):
        attempts.append(address)
        raise OSError("no network in unit tests")

    monkeypatch.setattr(web_fetch.socket, "create_connection", fake_create_connection)
    return attempts


def _resolve_to(monkeypatch, address):
    monkeypatch.setattr(
        web_fetch.socket,
        "getaddrinfo",
        lambda *a, **k: [(2, 1, 6, "", (address, 0))],
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/search",
        "http://10.0.0.1:8080/search",
        "http://169.254.169.254/latest/meta-data",
    ],
)
def test_private_loopback_and_metadata_addresses_are_refused(url, connections):
    with pytest.raises(ValueError, match="private/internal"):
        web_fetch._http_json(url, {"q": "x"}, {"X-API-KEY": "k"})

    assert connections == []


def test_a_name_that_resolves_to_a_private_address_is_refused(monkeypatch, connections):
    _resolve_to(monkeypatch, "10.1.2.3")

    with pytest.raises(ValueError, match="private/internal"):
        web_fetch._http_json(
            "https://internal.example.com/search", {"q": "x"}, {"X-API-KEY": "k"}
        )

    assert connections == []


def test_a_public_name_connects_to_the_address_that_was_checked(
    monkeypatch, connections
):
    """DNS pinning: one lookup, and the socket goes to that answer."""
    answers = iter(
        [[(2, 1, 6, "", ("93.184.216.34", 0))], [(2, 1, 6, "", ("127.0.0.1", 0))]]
    )
    monkeypatch.setattr(web_fetch.socket, "getaddrinfo", lambda *a, **k: next(answers))

    with pytest.raises(OSError):
        web_fetch._http_json("http://search.example.com/search", {"q": "x"}, {})

    assert connections == [("93.184.216.34", 80)]
    assert next(answers, None) is not None  # never resolved a second time


def test_non_http_schemes_are_refused(connections):
    with pytest.raises(ValueError, match="scheme"):
        web_fetch._http_json("file:///etc/passwd", {}, {})

    assert connections == []


def test_the_search_key_does_not_follow_a_redirect_to_another_origin(monkeypatch):
    _resolve_to(monkeypatch, "93.184.216.34")
    req = urllib.request.Request(
        "https://search.example.com/search",
        headers={"X-API-KEY": "teamspace-key", "Accept": "*/*"},
    )

    new = web_fetch._SafeRedirects().redirect_request(
        req, None, 302, "Found", email.message.Message(), "https://other.example.net/"
    )

    assert not any(h.lower() == "x-api-key" for h in new.headers)
    assert new.get_header("Accept") == "*/*"
