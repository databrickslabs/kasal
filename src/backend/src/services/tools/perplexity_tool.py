import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Type

import requests
from pydantic import BaseModel, Field, PrivateAttr

from src.services.tools.base import BaseTool

# Configure logger
logger = logging.getLogger(__name__)


def _safe_citation_url(value: str) -> str:
    """A citation URL, or "" if it should not be shown.

    Citations come back from a third party, so the strings themselves are
    attacker-influenced even though Kasal formats the list. This is a STRUCTURAL
    check only — https, a real host, no credentials in the authority, no
    ``javascript:``/``data:`` scheme — reusing the same helper the HITL webhook
    validation uses rather than a second regex that drifts from it.

    It deliberately does NOT check where the URL points. That is not this
    function's job: a citation is rendered for a person to read, never fetched,
    and deciding whether a domain is trustworthy is the reader's judgement.
    Dropping a malformed entry is about not emitting a link that could be a
    ``javascript:`` payload in a UI that renders markdown.
    """
    url = (value or "").strip()
    if not url:
        return ""

    from urllib.parse import urlparse

    try:
        parsed = urlparse(url)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Dropping unparseable citation URL %r: %s", url[:120], exc)
        return ""

    # http is allowed alongside https. Requiring https would silently drop
    # legitimate sources that have not migrated — and silently dropping
    # citations is the exact failure this validation sits next to.
    if parsed.scheme not in ("http", "https"):
        logger.warning("Dropping citation URL with scheme %r", parsed.scheme)
        return ""
    if not parsed.netloc:
        logger.warning("Dropping citation URL with no host: %r", url[:120])
        return ""
    # Credentials in the authority are never legitimate in a citation and are a
    # known way to make a link read as one host while resolving to another.
    if "@" in parsed.netloc:
        logger.warning("Dropping citation URL with embedded credentials")
        return ""
    return url


# Input schema for PerplexitySearchTool
class PerplexitySearchInput(BaseModel):
    """Input schema for PerplexitySearchTool."""

    query: str = Field(
        ..., description="The search query or question to pass to Perplexity AI."
    )


class PerplexitySearchTool(BaseTool):
    name: str = "PerplexityTool"
    description: str = (
        "A tool that performs web searches using Perplexity AI to find accurate and up-to-date information. "
        "Input should be a specific search query or question."
    )
    args_schema: Type[BaseModel] = PerplexitySearchInput
    _api_key: Optional[str] = PrivateAttr(default=None)
    _model: str = PrivateAttr(default="sonar")
    _temperature: float = PrivateAttr(default=0.1)
    _top_p: float = PrivateAttr(default=0.9)
    _max_tokens: int = PrivateAttr(default=2000)
    _search_domain_filter: List[str] = PrivateAttr(default=["<any>"])
    _return_images: bool = PrivateAttr(default=False)
    _return_related_questions: bool = PrivateAttr(default=False)
    _search_recency_filter: str = PrivateAttr(default="month")
    _top_k: int = PrivateAttr(default=0)
    _stream: bool = PrivateAttr(default=False)
    _presence_penalty: float = PrivateAttr(default=0)
    _frequency_penalty: float = PrivateAttr(default=1)
    _web_search_options: Dict[str, Any] = PrivateAttr(
        default={"search_context_size": "high"}
    )

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        max_tokens: Optional[int] = None,
        search_domain_filter: Optional[List[str]] = None,
        return_images: Optional[bool] = None,
        return_related_questions: Optional[bool] = None,
        search_recency_filter: Optional[str] = None,
        top_k: Optional[int] = None,
        stream: Optional[bool] = None,
        presence_penalty: Optional[float] = None,
        frequency_penalty: Optional[float] = None,
        web_search_options: Optional[Dict[str, Any]] = None,
        result_as_answer: bool = False,
    ):
        super().__init__()

        # Log all relevant info about key source
        logger.info("Initializing PerplexitySearchTool")
        logger.info(f"API key provided directly: {bool(api_key)}")
        logger.info(
            f"API key in environment: {bool(os.environ.get('PERPLEXITY_API_KEY'))}"
        )
        logger.info(f"result_as_answer: {result_as_answer}")

        # Try to get API key from environment or parameter
        if not api_key:
            api_key = os.environ.get("PERPLEXITY_API_KEY")
            if not api_key:
                logger.error(
                    "No Perplexity API key provided. Please configure PERPLEXITY_API_KEY in the API Keys settings."
                )
                raise ValueError(
                    "Perplexity API key is required. Please configure it in the API Keys settings."
                )

        self._api_key = api_key

        # Safely log a portion of the key for diagnostic purposes
        if api_key:
            masked_key = (
                f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) > 8 else "***"
            )
            logger.info(f"Initialized Perplexity tool with API key: {masked_key}")
        else:
            logger.error("Failed to initialize Perplexity tool with a valid API key")

        # Set optional parameters if provided
        if model is not None:
            self._model = model
        if temperature is not None:
            self._temperature = temperature
        if top_p is not None:
            self._top_p = top_p
        if max_tokens is not None:
            self._max_tokens = max_tokens
        if search_domain_filter is not None:
            self._search_domain_filter = search_domain_filter
        if return_images is not None:
            self._return_images = return_images
        if return_related_questions is not None:
            self._return_related_questions = return_related_questions
        if search_recency_filter is not None:
            self._search_recency_filter = search_recency_filter
        if top_k is not None:
            self._top_k = top_k
        if stream is not None:
            self._stream = stream
        if presence_penalty is not None:
            self._presence_penalty = presence_penalty
        if frequency_penalty is not None:
            self._frequency_penalty = frequency_penalty
        if web_search_options is not None:
            self._web_search_options = web_search_options

    def _run(self, query: str) -> str:
        """
        Execute a search query using the Perplexity API directly.
        """
        try:
            url = "https://api.perplexity.ai/chat/completions"

            # Create base payload with required parameters
            payload = {
                "model": self._model,
                "messages": [
                    {"role": "system", "content": "Be precise and concise."},
                    {"role": "user", "content": query},
                ],
                "max_tokens": self._max_tokens,
                "temperature": self._temperature,
                "top_p": self._top_p,
                "top_k": self._top_k,
                "stream": self._stream,
                "presence_penalty": self._presence_penalty,
                "frequency_penalty": self._frequency_penalty,
            }

            # Add optional parameters that are supported by the API
            if self._search_domain_filter and self._search_domain_filter != ["<any>"]:
                payload["search_domain_filter"] = self._search_domain_filter

            if self._return_images:
                payload["return_images"] = self._return_images

            if self._return_related_questions:
                payload["return_related_questions"] = self._return_related_questions

            if self._search_recency_filter:
                payload["search_recency_filter"] = self._search_recency_filter

            # Add web_search_options if specified and in correct format
            if self._web_search_options and isinstance(self._web_search_options, dict):
                # Only include if it contains valid keys
                if "search_context_size" in self._web_search_options:
                    payload["web_search_options"] = self._web_search_options

            headers = {
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            }

            logger.info(f"Executing Perplexity API request with query: {query}")
            logger.debug(f"Perplexity API payload: {json.dumps(payload, indent=2)}")

            response = requests.post(url, json=payload, headers=headers)
            if not response.ok:
                logger.error(
                    f"Error from Perplexity API: {response.status_code} - {response.text}"
                )
                return f"Error from Perplexity API: {response.status_code} - {response.text}"

            response.raise_for_status()  # Raise exception for bad status codes

            result = response.json()
            logger.debug(f"Full API response: {json.dumps(result, indent=2)}")

            answer = (
                result.get("choices", [{}])[0].get("message", {}).get("content", "")
            )

            # Extract citations/search_results from the response
            # Check for both 'search_results' (new format) and 'citations' (old format)
            search_results = result.get("search_results", [])
            if not search_results:
                # Check if citations are in the old format
                search_results = result.get("citations", [])

            # Format the answer with citations if available
            if search_results:
                formatted_answer = f"{answer}\n\n**Sources:**\n"
                idx = 0
                for citation in search_results:
                    if isinstance(citation, dict):
                        title = citation.get("title", "Unknown")
                        url = _safe_citation_url(citation.get("url", ""))
                        if not url:
                            continue
                        idx += 1
                        formatted_answer += f"[{idx}] {title}: {url}\n"
                    elif isinstance(citation, str):
                        # Handle case where citations are just URLs
                        url = _safe_citation_url(citation)
                        if not url:
                            continue
                        idx += 1
                        formatted_answer += f"[{idx}] {url}\n"
                if idx:
                    answer = formatted_answer

            # Log a preview of the answer
            logger.info(f"Perplexity answer with citations: {answer[:200]}...")

            # Create a structured response that matches our PerplexityToolOutput schema
            output = {
                "answer": answer,
                "references": search_results,
                "model": self._model,
                "search_context": {
                    "query": query,
                    "timestamp": datetime.now().isoformat(),
                },
            }

            # Return the formatted answer with citations
            logger.debug(f"Structured output: {json.dumps(output, indent=2)}")
            return answer

        except Exception as e:
            error_msg = f"Error executing Perplexity API request: {str(e)}"
            logger.error(error_msg)
            return error_msg
