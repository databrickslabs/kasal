from crewai.tools import BaseTool
from typing import Optional, Type, Union, Dict, Any, List
from pydantic import BaseModel, Field, PrivateAttr, field_validator
import logging
import aiohttp
import asyncio
import os
from pathlib import Path


# Configure logger
logger = logging.getLogger(__name__)


class AgentBricksInput(BaseModel):
    """Input schema for AgentBricks."""
    question: str = Field(..., description="The question to ask the AgentBricks agent.")

    @field_validator('question', mode='before')
    @classmethod
    def parse_question(cls, value):
        """
        Handle complex input formats for question, especially dictionaries
        that might come from LLM tools format.
        """
        # If it's already a string, return as is
        if isinstance(value, str):
            return value

        # If it's a dict with a description or text field, use that
        if isinstance(value, dict):
            if 'description' in value:
                return value['description']
            elif 'text' in value:
                return value['text']
            elif 'query' in value:
                return value['query']
            elif 'question' in value:
                return value['question']
            # If we can't find a suitable field, convert the whole dict to string
            return str(value)

        # If it's any other type, convert to string
        return str(value)


class AgentBricksTool(BaseTool):
    name: str = "AgentBricksTool"
    description: str = (
        "A tool that uses AgentBricks (Mosaic AI Agent Bricks) to query Databricks AI agents. "
        "Input should be a specific question for the agent."
    )
    # Add alternative names for the tool
    aliases: List[str] = ["AgentBricks", "DatabricksAgent", "MosaicAgent"]
    args_schema: Type[BaseModel] = AgentBricksInput
    _endpoint_name: str = PrivateAttr(default=None)
    _timeout: int = PrivateAttr(default=120)  # Timeout in seconds
    _tool_id: int = PrivateAttr(default=None)
    _user_token: str = PrivateAttr(default=None)  # For OBO authentication
    _group_id: str = PrivateAttr(default=None)  # For PAT authentication fallback
    _return_trace: bool = PrivateAttr(default=False)  # Whether to return execution trace
    _custom_inputs: Dict[str, Any] = PrivateAttr(default=None)  # Custom inputs for the agent

    def __init__(
        self,
        tool_config: Optional[dict] = None,
        tool_id: Optional[int] = None,
        token_required: bool = True,
        user_token: str = None,
        group_id: str = None,
        result_as_answer: bool = False
    ):
        super().__init__(result_as_answer=result_as_answer)
        if tool_config is None:
            tool_config = {}

        logger.info(f"AgentBricksTool.__init__ called with tool_config keys: {list(tool_config.keys()) if tool_config else []}")

        # Set tool ID if provided
        if tool_id is not None:
            self._tool_id = tool_id

        # Store user token for centralized auth
        if user_token:
            self._user_token = user_token
            logger.info("User token provided for centralized authentication")

        # CRITICAL: Store group_id for PAT authentication fallback
        # This is essential because UserContext doesn't propagate to CrewAI threads
        if group_id:
            self._group_id = group_id
            logger.info(f"Group ID provided for PAT authentication fallback: {group_id}")
        else:
            logger.warning("No group_id provided - PAT authentication may fail if user_token unavailable")

        # Extract endpoint_name from tool_config (ONLY config, never environment)
        if tool_config:
            if 'endpointName' in tool_config:
                # Handle if endpointName is a list
                if isinstance(tool_config['endpointName'], list) and tool_config['endpointName']:
                    self._endpoint_name = tool_config['endpointName'][0]
                    logger.info(f"Using endpointName from config (list): {self._endpoint_name}")
                else:
                    self._endpoint_name = tool_config['endpointName']
                    logger.info(f"Using endpointName from config: {self._endpoint_name}")
            elif 'endpoint' in tool_config:
                self._endpoint_name = tool_config['endpoint']
                logger.info(f"Using endpoint from config: {self._endpoint_name}")
            elif 'endpoint_name' in tool_config:
                self._endpoint_name = tool_config['endpoint_name']
                logger.info(f"Using endpoint_name from config: {self._endpoint_name}")

            # Extract timeout if provided
            if 'timeout' in tool_config:
                self._timeout = tool_config['timeout']
                logger.info(f"Using timeout from config: {self._timeout}s")

            # Extract return_trace flag if provided
            if 'return_trace' in tool_config:
                self._return_trace = tool_config['return_trace']
                logger.info(f"Return trace enabled: {self._return_trace}")

            # Extract custom inputs if provided
            if 'custom_inputs' in tool_config:
                self._custom_inputs = tool_config['custom_inputs']
                logger.info(f"Custom inputs provided: {self._custom_inputs}")

        # Validate endpoint_name is configured
        if not self._endpoint_name:
            logger.warning("AgentBricks endpoint name not configured in tool_config. Tool will fail when used.")

        # Log configuration
        logger.info("AgentBricksTool Configuration:")
        logger.info(f"Tool ID: {self._tool_id}")
        logger.info(f"Endpoint Name: {self._endpoint_name}")
        logger.info(f"Timeout: {self._timeout}s")
        logger.info(f"Has user token: {bool(self._user_token)}")
        logger.info("Host and authentication will be obtained from databricks_auth module at runtime")

    def set_user_token(self, user_token: str):
        """Set user access token for OBO authentication."""
        self._user_token = user_token
        logger.info("User token set for centralized authentication")

    async def _get_workspace_url(self) -> str:
        """Get workspace URL from centralized databricks_auth module."""
        try:
            from src.utils.databricks_auth import _databricks_auth

            # Get workspace URL directly from the singleton
            workspace_url = await _databricks_auth.get_workspace_url()

            if not workspace_url:
                raise ValueError("Could not obtain workspace URL from databricks_auth module")

            return workspace_url

        except Exception as e:
            logger.error(f"Failed to get workspace URL from databricks_auth: {e}")
            raise ValueError(f"Could not obtain workspace URL: {e}")

    def _make_url(self, workspace_url: str, path: str) -> str:
        """Create a full URL from workspace URL and path."""
        # Ensure workspace_url doesn't have trailing slash
        workspace_url = workspace_url.rstrip('/')

        # Ensure path starts with a slash
        if not path.startswith('/'):
            path = '/' + path

        return f"{workspace_url}{path}"

    async def _get_auth_headers(self) -> dict:
        """Get authentication headers using unified authentication."""
        try:
            from src.utils.databricks_auth import get_auth_context
            from src.utils.user_context import UserContext, GroupContext

            # CRITICAL: Set UserContext with group_id before calling get_auth_context()
            # This is necessary because Python's contextvars don't propagate to CrewAI threads
            # Without this, get_auth_context() cannot query ApiKeysService for PAT tokens
            if self._group_id:
                try:
                    # Create GroupContext with the group_id we have
                    group_context = GroupContext(
                        group_ids=[self._group_id],
                        group_email=f"{self._group_id}@tool_thread",
                        access_token=self._user_token
                    )
                    UserContext.set_group_context(group_context)
                    logger.info(f"[AgentBricksTool] Set UserContext with group_id={self._group_id} for PAT authentication")
                except Exception as ctx_error:
                    logger.warning(f"[AgentBricksTool] Could not set UserContext: {ctx_error}")

            # Get unified auth context (handles OBO → PAT with group_id → SPN)
            auth = await get_auth_context(user_token=self._user_token)

            if not auth:
                logger.error("No authentication method available")
                return None

            # Return headers from auth context
            return auth.get_headers()
        except Exception as e:
            logger.error(f"Error getting auth headers: {e}")
            return None

    async def _query_agentbricks_endpoint(self, question: str) -> str:
        """
        Query the AgentBricks endpoint with a question.

        Args:
            question: The question to ask the agent

        Returns:
            The agent's response as a string
        """
        try:
            # Get workspace URL from centralized auth
            workspace_url = await self._get_workspace_url()

            # Ensure endpoint_name is configured (no fallback)
            if not self._endpoint_name:
                raise ValueError(
                    "AgentBricks endpoint name is required but not configured. "
                    "Please set it via tool_config['endpointName']."
                )

            endpoint_name = str(self._endpoint_name)
            logger.info(f"Using endpoint_name: {endpoint_name} for AgentBricks query")

            # Get authentication headers
            headers = await self._get_auth_headers()

            if not headers:
                raise Exception("No authentication headers available")

            # Build endpoint URL
            url = self._make_url(workspace_url, f"/serving-endpoints/{endpoint_name}/invocations")
            logger.info(f"Querying AgentBricks endpoint at URL: {url}")

            # Build request payload in AgentBricks format
            # AgentBricks uses 'input' field instead of 'messages'
            payload = {
                "input": [
                    {
                        "role": "user",
                        "content": question
                    }
                ]
            }

            # Add custom inputs if provided
            if self._custom_inputs:
                payload.update(self._custom_inputs)

            logger.info(f"AgentBricks request payload: {payload}")

            # Send request
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=self._timeout)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        error_msg = f"HTTP {response.status}: {error_text}"
                        logger.error(f"AgentBricks query failed: {error_msg}")
                        raise Exception(error_msg)

                    response.raise_for_status()
                    result = await response.json()

            # Extract response content from result
            # AgentBricks typically returns: {"choices": [{"message": {"content": "..."}}]}
            # or similar OpenAI-compatible format
            response_text = ""

            if isinstance(result, dict):
                # Try OpenAI-compatible format first
                if "choices" in result:
                    choices = result.get("choices", [])
                    if choices and len(choices) > 0:
                        first_choice = choices[0]
                        message = first_choice.get("message", {})
                        response_text = message.get("content", "")

                # Try direct response field
                if not response_text and "response" in result:
                    response_text = result.get("response", "")

                # Try predictions format
                if not response_text and "predictions" in result:
                    predictions = result.get("predictions", [])
                    if predictions and len(predictions) > 0:
                        response_text = predictions[0]

                # Add trace information if requested and available
                if self._return_trace and ("trace" in result or "metadata" in result):
                    trace_info = result.get("trace") or result.get("metadata")
                    response_text += f"\n\n[Trace: {trace_info}]"

            if not response_text and isinstance(result, str):
                response_text = result

            if not response_text:
                # If we couldn't extract text, return the full result as string
                response_text = str(result)

            logger.info(f"AgentBricks query completed successfully")
            return response_text

        except asyncio.TimeoutError:
            error_msg = f"Query timed out after {self._timeout} seconds"
            logger.error(error_msg)
            return f"Error: {error_msg}. Please try a simpler question or increase the timeout."

        except aiohttp.ClientConnectionError as e:
            error_msg = f"Connection error: {str(e)}"
            logger.error(error_msg)
            return f"Error connecting to AgentBricks endpoint. Please check your network connection and authentication configuration."

        except aiohttp.ClientResponseError as e:
            error_msg = f"HTTP {e.status} error: {str(e)}"
            logger.error(error_msg)
            return f"HTTP Error {e.status} when connecting to AgentBricks endpoint. Please verify your API token and permissions."

        except Exception as e:
            error_msg = f"Error querying AgentBricks: {str(e)}"
            logger.error(error_msg)
            return f"Error using AgentBricks: {str(e)}. Please verify your Databricks configuration."

    def _run(self, question: str) -> str:
        """
        Synchronous wrapper for async _run_async.
        Handles event loop detection and execution.
        """
        import concurrent.futures

        def run_async_in_new_loop():
            """Helper to run async code in a new event loop in a separate thread."""
            return asyncio.run(self._run_async(question))

        try:
            # Check if there's already a running event loop
            loop = asyncio.get_running_loop()
            # We're in an event loop, run in a thread pool with a new loop
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_async_in_new_loop)
                return future.result()
        except RuntimeError:
            # No running loop, safe to create one with asyncio.run
            return asyncio.run(self._run_async(question))

    async def _run_async(self, question: str) -> str:
        """
        Async implementation of the AgentBricks query execution.
        """
        # Check if endpoint_name is properly configured
        if not self._endpoint_name:
            return """ERROR: AgentBricks endpoint name is not configured!

Please configure the AgentBricks endpoint name in the agent/task tool configuration when setting up the workflow.
To find available AgentBricks endpoints, use the AgentBricks API endpoints list.
"""

        # Handle empty inputs or 'None' as an input
        if not question or question.lower() == 'none':
            return """To use the AgentBricksTool, please provide a specific question.
For example:
- "What are the current sales trends?"
- "Analyze customer behavior patterns"
- "Provide insights on product performance"

This tool uses Databricks AgentBricks to provide AI-powered responses based on your data."""

        try:
            # Query the AgentBricks endpoint
            response = await self._query_agentbricks_endpoint(question)
            return response

        except Exception as e:
            error_msg = f"Error executing AgentBricks request: {str(e)}"
            logger.error(error_msg)
            return f"Error using AgentBricks: {str(e)}. Please verify your Databricks configuration."
