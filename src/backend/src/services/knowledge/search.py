"""Searching a workspace's uploaded knowledge, and answering in words.

This is the CAPABILITY behind ``DatabricksKnowledgeSearchTool``: resolve which
files to look in, run the search, drop results too distant to be answers, and
say plainly when the knowledge base does not contain what was asked. All of it
used to live inside the tool class, which meant it could only be reached by an
agent, in a crew, mid-run.

That is the wrong boundary for what this does. Crew generation may want to
research before it plans; a chat turn may want to answer from an attached file
without an agent loop; an exported app has the same question to ask. None of
them are running a crew, and none should have to construct an agent tool to
search a workspace's own documents.

So the tool keeps what is genuinely agent-facing — the argument schema, the
``_run`` contract, the per-agent search budget — and delegates the work here.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from src.services.knowledge.search_guard import (
    filter_by_relevance,
    no_relevant_results_notice,
)

logger = logging.getLogger(__name__)

#: Seconds a single search may take before the caller gives up. A search that
#: hangs must not hold an agent's turn (or a generation request) open.
SEARCH_TIMEOUT_SECONDS = 30


class KnowledgeSearch:
    """Search one workspace's uploaded knowledge.

    Constructed with the identity the search runs under — workspace, user,
    execution, and the OBO token when there is one — because every one of those
    narrows what may be returned. Per-user isolation in particular is not a
    filter applied afterwards: knowledge is scoped to the user who uploaded it.
    """

    def __init__(
        self,
        *,
        group_id: str = "default",
        execution_id: Optional[str] = None,
        user_token: Optional[str] = None,
        user_email: Optional[str] = None,
        agent_id: Optional[str] = None,
    ):
        self.group_id = group_id
        self.execution_id = execution_id
        self.user_token = user_token
        self.user_email = user_email
        self.agent_id = agent_id

    async def raw_results(
        self, query: str, limit: int = 10, file_paths: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """The matching chunks, unformatted — for callers that want the data."""
        from src.services.tools.tool_session_provider import ToolSessionProvider
        from src.utils.user_context import GroupContext, UserContext

        if self.group_id:
            # The embedding call resolves auth through the context, so it has to
            # be set before the search, not alongside it. This runs in a worker
            # thread with a fresh event loop (DatabricksKnowledgeSearchTool.
            # _search_in_thread), where the parent thread's ContextVars did NOT
            # propagate — so we re-establish BOTH the group and the OBO token
            # here. Without the token, get_embedding() falls through to PAT/SPN,
            # which a deployed (OBO-only) App does not have, and the query
            # embedding fails silently → every search returns nothing.
            UserContext.set_group_context(
                GroupContext(group_ids=[self.group_id], access_token=self.user_token)
            )
            if self.user_token:
                UserContext.set_user_token(self.user_token)

        async with ToolSessionProvider.knowledge_service(
            group_id=self.group_id or "default",
            user_token=self.user_token,
        ) as service:
            results = await service.search_knowledge(
                query=query,
                group_id=self.group_id,
                execution_id=self.execution_id,
                file_paths=file_paths,
                agent_id=self.agent_id,
                limit=limit,
                user_token=self.user_token,
                created_by=self.user_email,
            )
        return results or []

    async def search(
        self, query: str, limit: int = 10, file_paths: Optional[List[str]] = None
    ) -> str:
        """Search, and answer in the words a reader (or a model) can act on.

        Never raises: a failed search returns a sentence saying so. The caller
        is usually mid-answer, and an exception there costs a user their reply
        for something that is, at worst, a missing citation.
        """
        try:
            results = await asyncio.wait_for(
                self.raw_results(query, limit, file_paths),
                timeout=SEARCH_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(
                f"[knowledge] search timed out after {SEARCH_TIMEOUT_SECONDS}s: {query!r}"
            )
            return f"The knowledge search timed out after {SEARCH_TIMEOUT_SECONDS} seconds."
        except Exception as search_err:  # noqa: BLE001
            logger.error(f"[knowledge] search failed: {search_err}", exc_info=True)
            return f"Error searching knowledge base: {search_err}"

        return self.format(query, results)

    @staticmethod
    def format(query: str, results: List[Dict[str, Any]]) -> str:
        """Turn results into an answer — including "it is not in here".

        An index always returns its top-k, so "20 results" says nothing about
        whether any of them answer the question. Distant ones are dropped, and
        when none survive the reply says so in terms that end the search rather
        than invite another rephrasing (see search_guard).
        """
        if not results:
            return no_relevant_results_notice(query, 0.0)

        kept, best_score, scored = filter_by_relevance(results)
        if scored and not kept:
            logger.info(
                f"[knowledge] {len(results)} result(s) all below the relevance floor "
                f"(best={best_score:.3f}); reporting no match"
            )
            return no_relevant_results_notice(query, best_score)

        lines = [f"Found {len(kept)} relevant results:\n"]
        for index, result in enumerate(kept, 1):
            metadata = result.get("metadata", {}) or {}
            lines.append(
                f"\n--- Result {index} (Score: {metadata.get('score', 0.0):.3f}) ---"
            )
            lines.append(f"Source: {metadata.get('source', 'Unknown')}")
            lines.append(f"Content: {result.get('content', '')}")
            lines.append("---")
        return "\n".join(lines)
