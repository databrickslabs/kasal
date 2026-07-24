"""Bundled tools: SerperDevTool, ScrapeWebsiteTool, DallETool.

Authored module; surface validated against the kasal_engine datamodel.
These are tool_factory fallbacks in kasal (1 file). Behavior follows
crewai_tools 1.15.5 (code2db evidence) reimplemented on the stdlib:
urllib instead of requests/openai-client, html.parser instead of
BeautifulSoup, and an address-checking fetch guard in place of
crewai_tools.security.safe_requests.safe_get.
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
    with urllib.request.urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


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


class SerperDevToolSchema(BaseModel):
    """Input for SerperDevTool."""

    search_query: str = Field(
        ..., description="Mandatory search query you want to use to search the internet"
    )


class SerperDevTool(BaseTool):
    name: str = "Search the internet with Serper"
    description: str = (
        "A tool that can be used to search the internet with a search_query. "
        "Supports different search types: 'search' (default), 'news'"
    )
    args_schema: type[BaseModel] = SerperDevToolSchema
    base_url: str = "https://google.serper.dev"
    n_results: int = 10
    save_file: bool = False
    search_type: str = "search"
    country: str | None = ""
    location: str | None = ""
    locale: str | None = ""
    env_vars: list[EnvVar] = Field(
        default_factory=lambda: [
            EnvVar(
                name="SERPER_API_KEY", description="API key for Serper", required=True
            ),
        ]
    )

    def _get_search_url(self, search_type: str) -> str:
        search_type = search_type.lower()
        allowed_search_types = ["search", "news"]
        if search_type not in allowed_search_types:
            raise ValueError(
                f"Invalid search type: {search_type}. Must be one of: {', '.join(allowed_search_types)}"
            )
        return f"{self.base_url}/{search_type}"

    @staticmethod
    def _process_knowledge_graph(kg: dict[str, Any]) -> KnowledgeGraph:
        return {
            "title": kg.get("title", ""),
            "type": kg.get("type", ""),
            "website": kg.get("website", ""),
            "imageUrl": kg.get("imageUrl", ""),
            "description": kg.get("description", ""),
            "descriptionSource": kg.get("descriptionSource", ""),
            "descriptionLink": kg.get("descriptionLink", ""),
            "attributes": kg.get("attributes", {}),
        }

    def _process_organic_results(
        self, organic_results: list[dict[str, Any]]
    ) -> list[OrganicResult]:
        processed_results: list[OrganicResult] = []
        for result in organic_results[: self.n_results]:
            try:
                result_data: OrganicResult = {
                    "title": result["title"],
                    "link": result["link"],
                    "snippet": result.get("snippet", ""),
                    "position": result.get("position"),
                }
                if "sitelinks" in result:
                    result_data["sitelinks"] = [
                        {
                            "title": sitelink.get("title", ""),
                            "link": sitelink.get("link", ""),
                        }
                        for sitelink in result["sitelinks"]
                    ]
                processed_results.append(result_data)
            except KeyError:
                logger.warning("Skipping malformed organic result: %s", result)
        return processed_results

    def _process_people_also_ask(
        self, paa_results: list[dict[str, Any]]
    ) -> list[PeopleAlsoAskResult]:
        processed_results: list[PeopleAlsoAskResult] = []
        for result in paa_results[: self.n_results]:
            try:
                processed_results.append(
                    {
                        "question": result["question"],
                        "snippet": result.get("snippet", ""),
                        "title": result.get("title", ""),
                        "link": result.get("link", ""),
                    }
                )
            except KeyError:
                logger.warning("Skipping malformed PAA result: %s", result)
        return processed_results

    def _process_related_searches(
        self, related_results: list[dict[str, Any]]
    ) -> list[RelatedSearchResult]:
        processed_results: list[RelatedSearchResult] = []
        for result in related_results[: self.n_results]:
            try:
                processed_results.append({"query": result["query"]})
            except KeyError:
                logger.warning("Skipping malformed related search result: %s", result)
        return processed_results

    def _process_news_results(
        self, news_results: list[dict[str, Any]]
    ) -> list[NewsResult]:
        processed_results: list[NewsResult] = []
        for result in news_results[: self.n_results]:
            try:
                processed_results.append(
                    {
                        "title": result["title"],
                        "link": result["link"],
                        "snippet": result.get("snippet", ""),
                        "date": result.get("date", ""),
                        "source": result.get("source", ""),
                        "imageUrl": result.get("imageUrl", ""),
                    }
                )
            except KeyError:
                logger.warning("Skipping malformed news result: %s", result)
        return processed_results

    def _make_api_request(self, search_query: str, search_type: str) -> dict[str, Any]:
        search_url = self._get_search_url(search_type)
        payload: dict[str, Any] = {"q": search_query, "num": self.n_results}
        if self.country != "":
            payload["gl"] = self.country
        if self.location != "":
            payload["location"] = self.location
        if self.locale != "":
            payload["hl"] = self.locale
        headers = {"X-API-KEY": os.environ["SERPER_API_KEY"]}
        return _http_json(search_url, payload, headers, timeout=10)

    def _process_search_results(
        self, results: dict[str, Any], search_type: str
    ) -> dict[str, Any]:
        formatted_results: dict[str, Any] = {}
        if search_type == "search":
            if "knowledgeGraph" in results:
                formatted_results["knowledgeGraph"] = self._process_knowledge_graph(
                    results["knowledgeGraph"]
                )
            if "organic" in results:
                formatted_results["organic"] = self._process_organic_results(
                    results["organic"]
                )
            if "peopleAlsoAsk" in results:
                formatted_results["peopleAlsoAsk"] = self._process_people_also_ask(
                    results["peopleAlsoAsk"]
                )
            if "relatedSearches" in results:
                formatted_results["relatedSearches"] = self._process_related_searches(
                    results["relatedSearches"]
                )
        elif search_type == "news":
            if "news" in results:
                formatted_results["news"] = self._process_news_results(results["news"])
        return formatted_results

    def _run(self, **kwargs: Any) -> FormattedResults:
        search_query: str | None = kwargs.get("search_query") or kwargs.get("query")
        search_type: str = kwargs.get("search_type", self.search_type)
        save_file = kwargs.get("save_file", self.save_file)

        if not search_query:
            raise ValueError("search_query is required")

        results = self._make_api_request(search_query, search_type)

        formatted_results: dict[str, Any] = {
            "searchParameters": {
                "q": search_query,
                "type": search_type,
                **results.get("searchParameters", {}),
            }
        }
        formatted_results.update(self._process_search_results(results, search_type))
        formatted_results["credits"] = results.get("credits", 1)

        if save_file:
            _save_results_to_file(json.dumps(formatted_results, indent=2))

        return formatted_results  # type: ignore[return-value]


# -------------------------- ScrapeWebsiteTool --------------------------


class FixedScrapeWebsiteToolSchema(BaseModel):
    """Input for ScrapeWebsiteTool with a preset website_url."""


class ScrapeWebsiteToolSchema(FixedScrapeWebsiteToolSchema):
    """Input for ScrapeWebsiteTool."""

    website_url: str = Field(..., description="Mandatory website url to read the file")


class ScrapeWebsiteTool(BaseTool):
    name: str = "Read website content"
    description: str = "A tool that can be used to read a website content."
    args_schema: type[BaseModel] = ScrapeWebsiteToolSchema
    website_url: str | None = None
    cookies: dict[str, str] | None = None
    headers: dict[str, str] | None = Field(
        default_factory=lambda: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
    )

    def __init__(
        self,
        website_url: str | None = None,
        cookies: dict[str, str] | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        if website_url is not None:
            self.website_url = website_url
            self.description = (
                f"A tool that can be used to read {website_url}'s content."
            )
            self.args_schema = FixedScrapeWebsiteToolSchema
            if cookies is not None:
                self.cookies = {cookies["name"]: os.getenv(cookies["value"]) or ""}

    def _run(self, **kwargs: Any) -> Any:
        website_url: str | None = kwargs.get("website_url", self.website_url)
        if website_url is None:
            raise ValueError("Website URL must be provided.")

        headers = dict(self.headers or {})
        if self.cookies:
            headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in self.cookies.items())

        html = _safe_fetch(website_url, headers=headers, timeout=15)
        extractor = _TextExtractor()
        extractor.feed(html)

        text = "The following text is scraped website content:\n\n" + extractor.text()
        text = re.sub("[ \t]+", " ", text)
        return re.sub("\\s+\n\\s+", "\n", text)


# ----------------------------- DallETool -----------------------------


class ImagePromptSchema(BaseModel):
    """Input for Dall-E Tool."""

    image_description: str = Field(
        description="Description of the image to be generated by Dall-E."
    )


class DallETool(BaseTool):
    name: str = "Dall-E Tool"
    description: str = "Generates images using OpenAI's Dall-E model."
    args_schema: type[BaseModel] = ImagePromptSchema

    model: str = "dall-e-3"
    size: (
        Literal[
            "auto",
            "1024x1024",
            "1536x1024",
            "1024x1536",
            "256x256",
            "512x512",
            "1792x1024",
            "1024x1792",
        ]
        | None
    ) = "1024x1024"
    quality: Literal["standard", "hd", "low", "medium", "high", "auto"] | None = (
        "standard"
    )
    n: int = 1

    env_vars: list[EnvVar] = Field(
        default_factory=lambda: [
            EnvVar(
                name="OPENAI_API_KEY",
                description="API key for OpenAI services",
                required=True,
            ),
        ]
    )

    def _run(self, **kwargs: Any) -> str:
        image_description = kwargs.get("image_description")
        if not image_description:
            return "Image description is required."

        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            return "OPENAI_API_KEY is not set."

        base_url = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
        try:
            response = _http_json(
                f"{base_url}/images/generations",
                {
                    "model": self.model,
                    "prompt": image_description,
                    "size": self.size,
                    "quality": self.quality,
                    "n": self.n,
                },
                {"Authorization": f"Bearer {api_key}"},
                timeout=60,
            )
        except (RuntimeError, ValueError) as e:
            return f"Failed to generate image: {e}"

        data = response.get("data") or []
        if not data:
            return "Failed to generate image."
        return json.dumps(
            {
                "image_url": data[0].get("url"),
                "image_description": data[0].get("revised_prompt"),
            }
        )
