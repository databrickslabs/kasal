"""
Databricks Knowledge Search Tool for CrewAI

This is a lightweight wrapper around the DatabricksKnowledgeService
that makes knowledge search available as a CrewAI tool.
"""
from src.services.tools.base import BaseTool
from typing import Optional, Type, Dict, Any, List
from pydantic import BaseModel, Field, PrivateAttr
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

from src.services.knowledge import KnowledgeSearchBudget

#: Ceiling on one tool call, a little above the service's own search timeout so
#: the service's message ("the search timed out") is what the agent sees.
SEARCH_CALL_TIMEOUT_SECONDS = 35

# Configure logger
logger = logging.getLogger(__name__)

# Input schema for DatabricksKnowledgeSearchTool
class DatabricksKnowledgeSearchInput(BaseModel):
    """Input schema for DatabricksKnowledgeSearchTool."""
    query: str = Field(
        ...,
        description="The search query to find relevant information from uploaded knowledge documents."
    )
    limit: Optional[int] = Field(
        default=10,  # Increased from 5 for better context coverage
        ge=1,
        le=20,
        description="Maximum number of results to return (default: 10, max: 20)."
    )
    file_paths: Optional[List[str]] = Field(
        default=None,
        description="Optional list of file paths to filter search results."
    )

class DatabricksKnowledgeSearchTool(BaseTool):
    """
    A tool that searches through uploaded knowledge documents in Databricks Vector Index.

    This tool allows agents to search through documents that have been uploaded
    and indexed for the current execution context.
    """

    name: str = "DatabricksKnowledgeSearchTool"
    description: str = (
        "Search through uploaded knowledge documents to find relevant information. "
        "Use this tool when you need to find information from documents that have been "
        "uploaded to the knowledge base. Input should be a specific search query. "
        "IMPORTANT: Documents are chunked - request at least 10-20 results (use limit parameter) "
        "to get comprehensive information from the document."
    )
    args_schema: Type[BaseModel] = DatabricksKnowledgeSearchInput

    # Private attributes for configuration
    _group_id: str = PrivateAttr(default="default")
    _execution_id: Optional[str] = PrivateAttr(default=None)
    _user_token: Optional[str] = PrivateAttr(default=None)
    _user_email: Optional[str] = PrivateAttr(default=None)
    _service: Optional[Any] = PrivateAttr(default=None)
    # What this agent has already searched, and how much searching is left.
    _budget: Any = PrivateAttr(default=None)

    def __init__(
        self,
        group_id: str = "default",
        execution_id: Optional[str] = None,
        user_token: Optional[str] = None,
        file_paths: Optional[List[str]] = None,
        agent_id: Optional[str] = None,
        user_email: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize the Databricks Knowledge Search Tool.

        Args:
            group_id: Group ID for tenant isolation
            execution_id: Optional execution ID for scoping search
            user_token: Optional user token for OBO authentication
            file_paths: Optional list of file paths to filter searches (from tool_configs)
            agent_id: Optional agent ID for access control filtering
            user_email: Executing user's email — search results are isolated
                to the knowledge THIS user uploaded
            **kwargs: Additional arguments for BaseTool
        """
        # CRITICAL DEBUG: Print to stdout (will show in logs even before logging is configured)
        print(f"[TOOL __INIT__] ========================================")
        print(f"[TOOL __INIT__] DatabricksKnowledgeSearchTool created!")
        print(f"[TOOL __INIT__]   - group_id: {group_id}")
        print(f"[TOOL __INIT__]   - execution_id: {execution_id}")
        print(f"[TOOL __INIT__]   - file_paths: {file_paths}")
        print(f"[TOOL __INIT__]   - agent_id: {agent_id}")
        print(f"[TOOL __INIT__]   - kwargs keys: {list(kwargs.keys()) if kwargs else 'None'}")
        print(f"[TOOL __INIT__] ========================================")

        super().__init__(**kwargs)

        self._group_id = group_id
        self._execution_id = execution_id
        self._user_token = user_token
        self._user_email = user_email  # Per-user knowledge isolation
        self._configured_file_paths = file_paths  # Store configured file paths from tool_configs
        self._agent_id = agent_id  # Store agent ID for access control
        self._budget = KnowledgeSearchBudget()

        logger.info(f"Initialized DatabricksKnowledgeSearchTool")
        logger.info(f"  - Configured file_paths: {self._configured_file_paths}")
        logger.info(f"  - Configured agent_id: {self._agent_id}")
        logger.info(f"  Group ID: {group_id}")
        logger.info(f"  Execution ID: {execution_id}")
        logger.info(f"  User token provided: {bool(user_token)}")
        logger.info(f"  Configured file paths (from tool_configs): {file_paths}")
        logger.info(f"  Agent ID (for access control): {agent_id}")

    def _resolve_file_paths(self, agent_file_paths: List[str]) -> List[str]:
        """
        Resolve agent-provided file paths to full volume paths.

        The agent might provide:
        - Simple filenames: "tt.txt"
        - Relative paths: "folder/tt.txt"
        - Full volume paths: "/Volumes/catalog/schema/volume/..."

        We need to match these against configured paths and return full paths.

        Args:
            agent_file_paths: List of file paths from agent

        Returns:
            List of resolved full volume paths
        """
        if not agent_file_paths:
            return None

        if not self._configured_file_paths:
            # No configured paths to match against - return agent paths as-is
            logger.warning("[TOOL] No configured file paths to resolve against")
            return agent_file_paths

        resolved_paths = []

        for agent_path in agent_file_paths:
            # Check if it's already a full volume path
            if agent_path.startswith("/Volumes/"):
                resolved_paths.append(agent_path)
                logger.info(f"[TOOL] Path already full volume path: {agent_path}")
                continue

            # Try to match against configured paths
            # Match by filename only (last component of path)
            agent_filename = agent_path.split("/")[-1] if "/" in agent_path else agent_path

            matched = False
            for configured_path in self._configured_file_paths:
                configured_filename = configured_path.split("/")[-1]
                if configured_filename == agent_filename:
                    resolved_paths.append(configured_path)
                    logger.info(f"[TOOL] Resolved '{agent_path}' to '{configured_path}'")
                    matched = True
                    break

            if not matched:
                logger.warning(f"[TOOL] Could not resolve '{agent_path}' to any configured path")
                # Still add it - let the search service handle it
                resolved_paths.append(agent_path)

        logger.info(f"[TOOL] Resolved {len(agent_file_paths)} paths to {len(resolved_paths)} full paths")
        return resolved_paths if resolved_paths else None

    def _run(self, query: str, limit: int = 10, file_paths: Optional[List[str]] = None) -> str:
        """Answer one search for the agent (CrewAI calls this synchronously).

        Everything agent-specific happens HERE — the per-agent search budget,
        resolving the paths the agent named against the ones configured on the
        tool — and the search itself is delegated to the service, which is
        reachable without an agent at all.
        """
        # Answer a repeated search from what it returned the first time, and
        # stop answering once the budget is spent. Both exist because the index
        # cannot say "I don't have this": it always returns its top-k, so an
        # agent hunting for something absent will rephrase until the round limit
        # kills the run. This is the agent's problem, so it stays on the tool.
        previous = self._budget.previous_answer(query)
        if previous is not None:
            logger.info(f"[knowledge-tool] repeat search for '{query}' — returning the first answer")
            return self._budget.repeat_notice(query, previous)
        if self._budget.exhausted():
            logger.warning(
                f"[knowledge-tool] budget exhausted after {self._budget.searches_used} "
                f"searches; refusing '{query}'"
            )
            return self._budget.exhausted_notice()

        # The agent's paths win when it named any (it knows what it wants);
        # otherwise the ones configured in tool_configs apply.
        effective_file_paths = (
            self._resolve_file_paths(file_paths) if file_paths else self._configured_file_paths
        )
        logger.info(
            f"[knowledge-tool] query={query!r} limit={limit} "
            f"paths={effective_file_paths} group={self._group_id} agent={self._agent_id}"
        )

        answer = self._search_in_thread(query, limit, effective_file_paths)
        self._budget.record(query, answer)
        return answer

    def _search_in_thread(
        self, query: str, limit: int, file_paths: Optional[List[str]]
    ) -> str:
        """Run the async capability from CrewAI's synchronous tool call.

        Its own loop in its own thread: the tool is invoked from inside a
        running event loop (the light path) and from a worker thread with none
        (the crew subprocess), and only a fresh loop is correct in both.
        """
        from src.services.knowledge import KnowledgeSearch

        search = KnowledgeSearch(
            group_id=self._group_id,
            execution_id=self._execution_id,
            user_token=self._user_token,
            user_email=self._user_email,
            agent_id=self._agent_id,
        )

        def _run_search() -> str:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(search.search(query, limit, file_paths))
            finally:
                loop.close()

        with ThreadPoolExecutor() as executor:
            return executor.submit(_run_search).result(timeout=SEARCH_CALL_TIMEOUT_SECONDS)
