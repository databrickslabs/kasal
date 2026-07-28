"""SerperDevTool — web search via the Serper.dev API."""

import datetime
import json
import logging
import os
from typing import Any, Literal

from pydantic import BaseModel, Field

from .base import BaseTool, EnvVar
from .web_fetch import (
    FormattedResults,
    KnowledgeGraph,
    NewsResult,
    OrganicResult,
    PeopleAlsoAskResult,
    RelatedSearchResult,
    SearchParameters,
    Sitelink,
    _http_json,
    _save_results_to_file,
)

logger = logging.getLogger(__name__)

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


