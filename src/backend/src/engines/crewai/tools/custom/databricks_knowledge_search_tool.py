"""
Databricks Knowledge Search Tool for CrewAI

This is a lightweight wrapper around the DatabricksKnowledgeService
that makes knowledge search available as a CrewAI tool.
"""
from crewai.tools import BaseTool
from typing import Optional, Type, Dict, Any, List
from pydantic import BaseModel, Field, PrivateAttr
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

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
    _service: Optional[Any] = PrivateAttr(default=None)

    def __init__(
        self,
        group_id: str = "default",
        execution_id: Optional[str] = None,
        user_token: Optional[str] = None,
        file_paths: Optional[List[str]] = None,
        agent_id: Optional[str] = None,
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
        self._configured_file_paths = file_paths  # Store configured file paths from tool_configs
        self._agent_id = agent_id  # Store agent ID for access control

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
        """
        Run the knowledge search synchronously (required by CrewAI).

        Args:
            query: The search query
            limit: Maximum number of results
            file_paths: Optional file paths filter (from agent call - will be resolved to full paths)

        Returns:
            Formatted search results as a string
        """
        # PRIORITY: If agent provides file_paths, resolve and use those (agent knows what it wants)
        # FALLBACK: If no file_paths provided, use configured paths from tool_configs
        # This allows dynamic file selection while maintaining backwards compatibility
        if file_paths:
            effective_file_paths = self._resolve_file_paths(file_paths)
            logger.info(f"[TOOL DEBUG] Using agent-provided file paths (resolved): {effective_file_paths}")
        else:
            effective_file_paths = self._configured_file_paths
            logger.info(f"[TOOL DEBUG] Using configured file paths (tool_configs): {effective_file_paths}")

        logger.info("="*80)
        logger.info("[TOOL DEBUG] DatabricksKnowledgeSearchTool._run() called")
        logger.info(f"[TOOL DEBUG] Query: '{query}'")
        logger.info(f"[TOOL DEBUG] Limit: {limit}")
        logger.info(f"[TOOL DEBUG] File paths from agent call: {file_paths}")
        logger.info(f"[TOOL DEBUG] Configured file paths (tool_configs): {self._configured_file_paths}")
        logger.info(f"[TOOL DEBUG] Effective file paths (will use): {effective_file_paths}")
        logger.info(f"[TOOL DEBUG] Agent ID (for access control): {self._agent_id}")
        logger.info(f"[TOOL DEBUG] Group ID: {self._group_id}")
        logger.info(f"[TOOL DEBUG] Execution ID: {self._execution_id}")
        logger.info("="*80)

        try:
            # Run the async search in a thread pool executor
            logger.info("[TOOL DEBUG] Starting async search in thread pool...")
            with ThreadPoolExecutor() as executor:
                future = executor.submit(self._run_async_search, query, limit, effective_file_paths)
                results = future.result(timeout=30)  # 30 second timeout

            logger.info(f"[TOOL DEBUG] Async search completed, got {len(results) if results else 0} results")

            if not results:
                logger.warning("[TOOL DEBUG] No results found, returning empty message")
                return "No relevant information found in the knowledge base."

            # Format results for the agent
            formatted_output = []
            formatted_output.append(f"Found {len(results)} relevant results:\n")

            for i, result in enumerate(results, 1):
                content = result.get('content', '')
                metadata = result.get('metadata', {})
                source = metadata.get('source', 'Unknown')
                score = metadata.get('score', 0.0)

                formatted_output.append(f"\n--- Result {i} (Score: {score:.3f}) ---")
                formatted_output.append(f"Source: {source}")
                formatted_output.append(f"Content: {content}")
                formatted_output.append("---")

            return "\n".join(formatted_output)

        except Exception as e:
            logger.error(f"Error running knowledge search: {e}", exc_info=True)
            return f"Error searching knowledge base: {str(e)}"

    def _run_async_search(self, query: str, limit: int, file_paths: Optional[List[str]]) -> List[Dict[str, Any]]:
        """
        Helper method to run async search in a new event loop.

        Args:
            query: The search query
            limit: Maximum number of results
            file_paths: Optional file paths filter

        Returns:
            List of search results
        """
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Run the async search
            return loop.run_until_complete(self._async_search(query, limit, file_paths))
        finally:
            loop.close()

    async def _async_search(self, query: str, limit: int, file_paths: Optional[List[str]]) -> List[Dict[str, Any]]:
        """
        Perform the actual async search using the DatabricksKnowledgeService.

        Args:
            query: The search query
            limit: Maximum number of results
            file_paths: Optional file paths filter

        Returns:
            List of search results
        """
        logger.info("[TOOL ASYNC DEBUG] _async_search started")
        logger.info(f"[TOOL ASYNC DEBUG] Query: '{query}'")
        logger.info(f"[TOOL ASYNC DEBUG] Group ID: {self._group_id}")
        logger.info(f"[TOOL ASYNC DEBUG] Execution ID: {self._execution_id}")
        logger.info(f"[TOOL ASYNC DEBUG] User token: {bool(self._user_token)}")

        try:
            logger.info("[TOOL ASYNC DEBUG] Importing DatabricksKnowledgeService...")
            # Lazy import to avoid circular dependencies
            import importlib
            import sys

            # CRITICAL FIX: Force remove from sys.modules and reload to get latest code
            # Subprocess inherits cached modules from parent process
            modules_to_reload = [
                'src.services.databricks_knowledge_service',
                'src.services.knowledge_search_service',  # Also reload search service
                'src.repositories.databricks_vector_index_repository'  # Also reload repository
            ]
            for module_name in modules_to_reload:
                if module_name in sys.modules:
                    logger.info(f"[TOOL ASYNC DEBUG] Removing {module_name} from sys.modules...")
                    del sys.modules[module_name]
            logger.info("[TOOL ASYNC DEBUG] Modules removed, will import fresh from disk")

            from src.services.databricks_knowledge_service import DatabricksKnowledgeService
            from src.repositories.databricks_config_repository import DatabricksConfigRepository
            from src.db.session import async_session_factory
            from src.utils.user_context import UserContext, GroupContext

            logger.info("[TOOL ASYNC DEBUG] Imports successful")

            # CRITICAL: Set group context so PAT lookup works
            # The embedding generation needs group_id to find the PAT token
            if self._group_id:
                logger.info(f"[TOOL ASYNC DEBUG] Setting group context for PAT lookup: {self._group_id}")
                # GroupContext is a dataclass - use group_ids parameter, NOT primary_group_id
                # primary_group_id is a computed property that returns group_ids[0]
                group_context = GroupContext(group_ids=[self._group_id])
                UserContext.set_group_context(group_context)
                logger.info("[TOOL ASYNC DEBUG] Group context set successfully")

            logger.info("[TOOL ASYNC DEBUG] Creating async session...")

            # Create a new session for each search (don't cache service with session)
            async with async_session_factory() as session:
                logger.info("[TOOL ASYNC DEBUG] Session created successfully")
                logger.info("[TOOL ASYNC DEBUG] Creating DatabricksKnowledgeService...")

                # Create service with session
                service = DatabricksKnowledgeService(
                    session=session,
                    group_id=self._group_id,
                    user_token=self._user_token  # Pass user token for embedding generation authentication
                )

                logger.info("[TOOL ASYNC DEBUG] Service created successfully")
                logger.info("="*80)
                logger.info("🎯🎯🎯 [TOOL] ABOUT TO CALL service.search_knowledge() 🎯🎯🎯")
                logger.info("="*80)
                logger.info("[TOOL ASYNC DEBUG] Calling search_knowledge method...")
                logger.info(f"[TOOL ASYNC DEBUG] Parameters:")
                logger.info(f"  - query: '{query}'")
                logger.info(f"  - group_id: '{self._group_id}'")
                logger.info(f"  - execution_id: '{self._execution_id}'")
                logger.info(f"  - file_paths: {file_paths}")
                logger.info(f"  - agent_id: '{self._agent_id}'")
                logger.info(f"  - limit: {limit}")
                logger.info(f"  - user_token: {bool(self._user_token)}")

                # Call the search_knowledge method
                results = await service.search_knowledge(
                    query=query,
                    group_id=self._group_id,
                    execution_id=self._execution_id,
                    file_paths=file_paths,
                    agent_id=self._agent_id,
                    limit=limit,
                    user_token=self._user_token
                )

                logger.info("="*80)
                logger.info("🎯🎯🎯 [TOOL] RETURNED FROM service.search_knowledge() 🎯🎯🎯")
                logger.info("="*80)
            logger.info(f"[TOOL ASYNC DEBUG] search_knowledge returned {len(results) if results else 0} results")
            logger.info(f"Knowledge search returned {len(results)} results")
            return results

        except Exception as e:
            logger.error(f"[TOOL ASYNC DEBUG] ❌ EXCEPTION in async search: {e}", exc_info=True)
            logger.error(f"Error in async search: {e}", exc_info=True)
            return []