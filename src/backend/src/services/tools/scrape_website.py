"""ScrapeWebsiteTool — fetch a URL and return its text."""

import logging
import os
import re
from typing import Any, Optional

from pydantic import BaseModel, Field

from .base import BaseTool, EnvVar
from .web_fetch import _safe_fetch, _TextExtractor

logger = logging.getLogger(__name__)

#: Default ceiling on the text handed back to the model, in characters.
#: A scraped page goes straight into the conversation, and nothing downstream
#: trims it: the transport clamps the model's OUTPUT budget, not tool RESULTS. So
#: one long article could crowd out the task, the prior turns and the other tools'
#: results — and on a large page it could exceed the context window outright,
#: failing the task with a token error rather than a truncated read.
#:
#: 30k characters is roughly 7-8k tokens: enough for a long article, small next to
#: any current context window. Override per tool instance, or globally with
#: SCRAPE_WEBSITE_MAX_CHARS.
_DEFAULT_MAX_CHARS = 30_000

#: Ceiling on the bytes downloaded, independent of the character cap: HTML markup
#: dwarfs the text it carries, so the fetch is bounded separately to avoid pulling
#: megabytes into memory just to extract a few thousand characters.
_DEFAULT_MAX_FETCH_BYTES = 2_000_000


def _env_int(name: str, default: int) -> int:
    """A positive int from the environment, or ``default`` if unset/garbage."""
    try:
        value = int(os.getenv(name, ""))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


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
    #: Cap on the returned text. See _DEFAULT_MAX_CHARS for why this exists.
    max_chars: int = Field(
        default_factory=lambda: _env_int("SCRAPE_WEBSITE_MAX_CHARS", _DEFAULT_MAX_CHARS)
    )
    #: Cap on the bytes fetched, before any text extraction.
    max_fetch_bytes: int = Field(
        default_factory=lambda: _env_int(
            "SCRAPE_WEBSITE_MAX_FETCH_BYTES", _DEFAULT_MAX_FETCH_BYTES
        )
    )
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

        html = _safe_fetch(
            website_url,
            headers=headers,
            timeout=15,
            max_bytes=self.max_fetch_bytes,
        )
        extractor = _TextExtractor()
        extractor.feed(html)

        text = extractor.text()
        text = re.sub("[ \t]+", " ", text)
        text = re.sub("\\s+\n\\s+", "\n", text)

        # Truncate LAST, after whitespace collapsing, so the cap describes what the
        # model actually receives rather than the pre-cleanup size. Say so in the
        # text: silently cutting a page mid-sentence reads as the page ending
        # there, and an agent then reports the rest as absent instead of fetching
        # a more specific URL.
        if self.max_chars > 0 and len(text) > self.max_chars:
            omitted = len(text) - self.max_chars
            logger.info(
                "Truncated %s: %d of %d chars returned (%d omitted)",
                website_url,
                self.max_chars,
                len(text),
                omitted,
            )
            text = (
                text[: self.max_chars].rstrip()
                + f"\n\n[Truncated: {omitted:,} of {len(text):,} characters omitted. "
                "This page is longer than the per-read limit — request a more "
                "specific URL or section if you need the rest.]"
            )

        return "The following text is scraped website content:\n\n" + text
