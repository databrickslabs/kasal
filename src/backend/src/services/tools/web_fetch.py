"""
Shared HTTP + HTML helpers for the web tools.

Extracted from what used to be one 514-line ``bundled.py`` in the engine
library: a JSON POST, a guarded GET, and a stdlib HTML-to-text extractor, used
by both the search and scrape tools.
"""

import datetime
import http.client
import ipaddress
import json
import logging
import socket
import ssl
import urllib.error
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from typing import Any, TypedDict

logger = logging.getLogger(__name__)


# --------------------------- HTTP helpers ---------------------------


def _http_json(
    url: str, payload: dict[str, Any], headers: dict[str, str], timeout: int = 10
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json", **headers},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"request to {url} failed: {e}\n{detail}") from e
    results = json.loads(body)
    if not results:
        raise ValueError(f"Empty response from {url}")
    return dict(results)


#: Redirect hops followed before giving up. urllib's own default is 10.
_MAX_REDIRECTS = 5
#: Request headers dropped when a redirect changes the origin.
_SENSITIVE_REQUEST_HEADERS = ("Authorization", "Cookie", "Proxy-Authorization")


def _assert_http_scheme(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Unsupported URL scheme: {parsed.scheme!r}")


def _assert_public_target(url: str) -> str:
    """Refuse non-HTTP schemes and hosts that resolve to private, loopback,
    link-local or reserved addresses, and return the address that passed.

    Applied to the first URL AND to every redirect hop — a public URL that
    302s to ``http://127.0.0.1/…`` used to be followed without a second look
    (audit F09). The connection is then made to the returned address, never
    to a second resolution: a name that answered public here and private a
    moment later reached the private address (R2-08).
    """
    _assert_http_scheme(url)
    host = urllib.parse.urlparse(url).hostname or ""
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror as e:
        raise ValueError(f"Cannot resolve host {host!r}: {e}") from e
    if not infos:
        raise ValueError(f"Cannot resolve host {host!r}")
    for info in infos:
        address = ipaddress.ip_address(info[4][0])
        if (
            address.is_private
            or address.is_loopback
            or address.is_link_local
            or address.is_reserved
        ):
            raise ValueError(f"Refusing to fetch private/internal address for {host!r}")
    return infos[0][4][0]


class _PinnedHTTPConnection(http.client.HTTPConnection):
    """Connects to the address that was validated, not to a fresh lookup."""

    def __init__(self, *args, pinned: str, **kwargs):
        super().__init__(*args, **kwargs)
        self._pinned = pinned

    def connect(self):
        self.sock = socket.create_connection(
            (self._pinned, self.port), self.timeout, self.source_address
        )


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    """As above; TLS still verifies the certificate against the HOST NAME."""

    def __init__(self, *args, pinned: str, **kwargs):
        super().__init__(*args, **kwargs)
        self._pinned = pinned

    def connect(self):
        sock = socket.create_connection(
            (self._pinned, self.port), self.timeout, self.source_address
        )
        self.sock = self._context.wrap_socket(sock, server_hostname=self.host)


class _PinnedHandler(urllib.request.HTTPHandler, urllib.request.HTTPSHandler):
    """The opener's transport: every request — first URL and every redirect
    hop, which the redirect handler re-issues through here — is validated and
    then connected to exactly the address that passed."""

    def __init__(self):
        urllib.request.HTTPHandler.__init__(self)
        urllib.request.HTTPSHandler.__init__(self, context=ssl.create_default_context())

    def http_open(self, req):
        pinned = _assert_public_target(req.full_url)
        return self.do_open(_PinnedHTTPConnection, req, pinned=pinned)

    def https_open(self, req):
        pinned = _assert_public_target(req.full_url)
        return self.do_open(
            _PinnedHTTPSConnection, req, pinned=pinned, context=self._context
        )


def _origin(url: str) -> tuple:
    parts = urllib.parse.urlsplit(url)
    return (parts.scheme, parts.netloc)


class _SafeRedirects(urllib.request.HTTPRedirectHandler):
    """Every hop is held to the same rule as the first URL, the hop count is
    capped, and credentials do not travel to a different origin."""

    max_redirections = _MAX_REDIRECTS

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urllib.parse.urljoin(req.full_url, newurl)
        try:
            _assert_public_target(target)
        except ValueError as refused:
            raise urllib.error.HTTPError(
                target, code, f"redirect refused: {refused}", headers, fp
            ) from refused
        new = super().redirect_request(req, fp, code, msg, headers, target)
        if new is not None and _origin(target) != _origin(req.full_url):
            for header in _SENSITIVE_REQUEST_HEADERS:
                new.remove_header(header)
        return new


def _open(request: urllib.request.Request, timeout: int):
    """Open through the pinned, redirect-checking opener. The one seam tests stub.

    Built by hand rather than with ``build_opener``: the default set includes
    file:, ftp: and data: handlers, and a redirect to ``file:///etc/passwd``
    must have nowhere to go."""
    opener = urllib.request.OpenerDirector()
    for handler in (
        _PinnedHandler(),
        _SafeRedirects(),
        urllib.request.HTTPDefaultErrorHandler(),
        urllib.request.HTTPErrorProcessor(),
    ):
        opener.add_handler(handler)
    return opener.open(request, timeout=timeout)


def _safe_fetch(
    url: str,
    headers: dict[str, str],
    timeout: int = 15,
    max_bytes: int | None = None,
) -> str:
    """Fetch a URL, refusing non-HTTP schemes and private/loopback hosts.

    ``max_bytes`` bounds how much of the response body is READ. Without it a
    multi-megabyte page is pulled into memory in full and only trimmed later,
    which pays the download and decode cost regardless — and on a hostile or
    misconfigured URL there is no upper bound at all. None keeps the previous
    unbounded behaviour for callers that have their own limit.
    """
    _assert_http_scheme(url)  # the transport checks the address, per hop
    request = urllib.request.Request(url, headers=headers)
    try:
        with _open(request, timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            # read(n) caps the transfer itself. One extra byte is requested so the
            # caller can tell "exactly at the limit" from "truncated".
            raw = response.read() if max_bytes is None else response.read(max_bytes + 1)
            return raw.decode(charset, errors="replace")
    except urllib.error.HTTPError as e:
        # urllib's message is just "HTTP Error 404: Not Found" — no URL. The
        # model receives this as the tool result and needs to know WHICH source
        # is dead to pick another. Redirect loops arrive as HTTPError too.
        raise RuntimeError(f"HTTP {e.code} fetching {url}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Could not reach {url}: {e.reason}") from e


class _TextExtractor(HTMLParser):
    _SKIP = {"script", "style", "noscript", "template"}

    def __init__(self) -> None:
        super().__init__()
        self._chunks: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        if tag in self._SKIP:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._skip_depth and data.strip():
            self._chunks.append(data)

    def text(self) -> str:
        return " ".join(self._chunks)


# --------------------------- SerperDevTool ---------------------------


class KnowledgeGraph(TypedDict, total=False):
    title: str
    type: str
    website: str
    imageUrl: str
    description: str
    descriptionSource: str
    descriptionLink: str
    attributes: dict[str, Any]


class Sitelink(TypedDict):
    title: str
    link: str


class OrganicResult(TypedDict, total=False):
    title: str
    link: str
    snippet: str
    position: int | None
    sitelinks: list[Sitelink]


class PeopleAlsoAskResult(TypedDict):
    question: str
    snippet: str
    title: str
    link: str


class RelatedSearchResult(TypedDict):
    query: str


class NewsResult(TypedDict):
    title: str
    link: str
    snippet: str
    date: str
    source: str
    imageUrl: str


class SearchParameters(TypedDict, total=False):
    q: str
    type: str


class FormattedResults(TypedDict, total=False):
    searchParameters: SearchParameters
    knowledgeGraph: KnowledgeGraph
    organic: list[OrganicResult]
    peopleAlsoAsk: list[PeopleAlsoAskResult]
    relatedSearches: list[RelatedSearchResult]
    news: list[NewsResult]
    credits: int


def _save_results_to_file(content: str) -> None:
    filename = (
        f"search_results_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
    )
    with open(filename, "w") as file:
        file.write(content)
    logger.info("Results saved to %s", filename)
