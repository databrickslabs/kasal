"""
Shared HTTP + HTML helpers for the web tools.

Extracted from what used to be one 514-line ``bundled.py`` in the engine
library: a JSON POST, a guarded GET, and a stdlib HTML-to-text extractor, used
by both the search and scrape tools.
"""

import datetime
import ipaddress
import json
import logging
import os
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from typing import Any, Literal, TypedDict

from pydantic import BaseModel, Field

from .base import BaseTool, EnvVar

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


def _safe_fetch(url: str, headers: dict[str, str], timeout: int = 15) -> str:
    """Fetch a URL, refusing non-HTTP schemes and private/loopback hosts."""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Unsupported URL scheme: {parsed.scheme!r}")
    host = parsed.hostname or ""
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror as e:
        raise ValueError(f"Cannot resolve host {host!r}: {e}") from e
    for info in infos:
        address = ipaddress.ip_address(info[4][0])
        if (
            address.is_private
            or address.is_loopback
            or address.is_link_local
            or address.is_reserved
        ):
            raise ValueError(f"Refusing to fetch private/internal address for {host!r}")
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
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
    filename = f"search_results_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
    with open(filename, "w") as file:
        file.write(content)
    logger.info("Results saved to %s", filename)


