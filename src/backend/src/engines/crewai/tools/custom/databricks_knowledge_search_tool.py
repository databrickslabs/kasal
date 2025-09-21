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
    query: str = Field(..., description="The search query to find relevant information from uploaded knowledge documents.")
    limit: Optional[int] = Field(default=5, description="Maximum number of results to return (default: 5).")
    file_paths: Optional[List[str]] = Field(default=None, description="Optional list of file paths to filter search results.")

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
        "uploaded to the knowledge base. Input should be a specific search query."
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
        **kwargs
    ):
        """
        Initialize the Databricks Knowledge Search Tool.

        Args:
            group_id: Group ID for tenant isolation
            execution_id: Optional execution ID for scoping search
            user_token: Optional user token for OBO authentication
            **kwargs: Additional arguments for BaseTool
        """
        super().__init__(**kwargs)

        self._group_id = group_id
        self._execution_id = execution_id
        self._user_token = user_token

        logger.info(f"Initialized DatabricksKnowledgeSearchTool")
        logger.info(f"  Group ID: {group_id}")
        logger.info(f"  Execution ID: {execution_id}")
        logger.info(f"  User token provided: {bool(user_token)}")

    def _run(self, query: str, limit: int = 5, file_paths: Optional[List[str]] = None) -> str:
        """
        Run the knowledge search synchronously (required by CrewAI).

        Args:
            query: The search query
            limit: Maximum number of results
            file_paths: Optional file paths filter

        Returns:
            Formatted search results as a string
        """
        try:
            # Run the async search in a thread pool executor
            with ThreadPoolExecutor() as executor:
                future = executor.submit(self._run_async_search, query, limit, file_paths)
                results = future.result(timeout=30)  # 30 second timeout

            if not results:
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
        try:
            # Lazy import to avoid circular dependencies
            from src.services.databricks_knowledge_service import DatabricksKnowledgeService
            from src.repositories.databricks_config_repository import DatabricksConfigRepository
            from src.db.session import async_session_factory

            # Create a session and service if not already created
            if not self._service:
                async with async_session_factory() as session:
                    databricks_repo = DatabricksConfigRepository(session)
                    self._service = DatabricksKnowledgeService(
                        databricks_repo,
                        group_id=self._group_id
                    )

            # Call the search_knowledge method
            results = await self._service.search_knowledge(
                query=query,
                group_id=self._group_id,
                execution_id=self._execution_id,
                file_paths=file_paths,
                limit=limit,
                user_token=self._user_token
            )

            logger.info(f"Knowledge search returned {len(results)} results")
            return results

        except Exception as e:
            logger.error(f"Error in async search: {e}", exc_info=True)
            return []