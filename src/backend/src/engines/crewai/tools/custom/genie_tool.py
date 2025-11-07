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

class GenieInput(BaseModel):
    """Input schema for Genie."""
    question: str = Field(..., description="The question to be answered using Genie.")
    
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

class GenieTool(BaseTool):
    name: str = "GenieTool"
    description: str = (
        "A tool that uses Genie to find information about customers and business data. "
        "Input should be a specific business question."
    )
    # Add alternative names for the tool
    aliases: List[str] = ["Genie", "DatabricksGenie", "DataSearch"]
    args_schema: Type[BaseModel] = GenieInput
    _space_id: str = PrivateAttr(default=None)
    _base_polling_delay: int = PrivateAttr(default=5)  # Base polling interval in seconds
    _max_polling_delay: int = PrivateAttr(default=30)  # Max delay for exponential backoff
    _polling_timeout_minutes: int = PrivateAttr(default=10)  # Total timeout in minutes
    _max_retries: int = PrivateAttr(default=120)  # 10 minutes at 5s intervals
    _enable_exponential_backoff: bool = PrivateAttr(default=True)  # Enable exponential backoff
    _backoff_after_seconds: int = PrivateAttr(default=120)  # Start backoff after 2 minutes
    _current_conversation_id: str = PrivateAttr(default=None)
    _tool_id: int = PrivateAttr(default=35)  # Default tool ID
    _user_token: str = PrivateAttr(default=None)  # For OBO authentication
    _group_id: str = PrivateAttr(default=None)  # For PAT authentication fallback

    def __init__(self, tool_config: Optional[dict] = None, tool_id: Optional[int] = None, token_required: bool = True, user_token: str = None, group_id: str = None, result_as_answer: bool = False):
        super().__init__(result_as_answer=result_as_answer)
        if tool_config is None:
            tool_config = {}

        logger.info(f"GenieTool.__init__ called with tool_config keys: {list(tool_config.keys()) if tool_config else []}")

        # Configure polling parameters from tool_config
        if tool_config:
            self._base_polling_delay = tool_config.get("polling_delay", 5)
            self._max_polling_delay = tool_config.get("max_polling_delay", 30)
            self._polling_timeout_minutes = tool_config.get("timeout_minutes", 10)
            self._enable_exponential_backoff = tool_config.get("exponential_backoff", True)
            self._backoff_after_seconds = tool_config.get("backoff_after_seconds", 120)
            # Calculate max retries based on timeout and base delay
            self._max_retries = (self._polling_timeout_minutes * 60) // self._base_polling_delay
            logger.info(f"Polling config: delay={self._base_polling_delay}s, timeout={self._polling_timeout_minutes}min, max_retries={self._max_retries}")

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

        # Extract space_id from tool_config (ONLY config, never environment)
        if tool_config:
            if 'spaceId' in tool_config:
                # Handle if spaceId is a list
                if isinstance(tool_config['spaceId'], list) and tool_config['spaceId']:
                    self._space_id = tool_config['spaceId'][0]
                    logger.info(f"Using spaceId from config (list): {self._space_id}")
                else:
                    self._space_id = tool_config['spaceId']
                    logger.info(f"Using spaceId from config: {self._space_id}")
            elif 'space' in tool_config:
                self._space_id = tool_config['space']
                logger.info(f"Using space from config: {self._space_id}")
            elif 'space_id' in tool_config:
                self._space_id = tool_config['space_id']
                logger.info(f"Using space_id from config: {self._space_id}")

        # Validate space_id is configured
        if not self._space_id:
            logger.warning("Genie space ID not configured in tool_config. Tool will fail when used.")

        # Log configuration
        logger.info("GenieTool Configuration:")
        logger.info(f"Tool ID: {self._tool_id}")
        logger.info(f"Space ID: {self._space_id}")
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

        # Ensure spaceId is used correctly
        if "{self._space_id}" in path:
            if not self._space_id:
                raise ValueError("Genie space ID is not configured. Please configure spaceId in tool_config.")
            path = path.replace("{self._space_id}", self._space_id)

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
                    logger.info(f"[GenieTool] Set UserContext with group_id={self._group_id} for PAT authentication")
                except Exception as ctx_error:
                    logger.warning(f"[GenieTool] Could not set UserContext: {ctx_error}")

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


    async def _test_token_permissions(self, headers: dict, workspace_url: str) -> bool:
        """Test if the token has proper permissions by trying a simple API call."""
        try:
            # Try to list Genie spaces to test permissions
            test_url = f"{workspace_url}/api/2.0/genie/spaces"

            logger.info(f"Testing token permissions with URL: {test_url}")

            # Log the token details for debugging
            auth_header = headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]
                logger.info(f"Token preview: {token[:20]}...{token[-10:] if len(token) > 30 else token}")
                logger.info(f"Token length: {len(token)}")

                # Try to decode JWT to see scopes (if it's a JWT)
                if token.startswith('eyJ'):
                    try:
                        import base64
                        import json
                        # Decode JWT payload (without verification - just for debugging)
                        payload_part = token.split('.')[1]
                        # Add padding if needed
                        payload_part += '=' * (4 - len(payload_part) % 4)
                        payload = json.loads(base64.b64decode(payload_part))
                        logger.info(f"Token scopes: {payload.get('scope', 'No scope found')}")
                        logger.info(f"Required scopes: sql, dashboards.genie")
                        logger.info(f"Token subject: {payload.get('sub', 'No subject found')}")
                        logger.info(f"Token client_id: {payload.get('client_id', 'No client_id found')}")

                        # Check if token has required scopes
                        token_scopes = payload.get('scope', '').split()
                        required_scopes = ['sql', 'dashboards.genie']
                        missing_scopes = [scope for scope in required_scopes if scope not in token_scopes]

                        if missing_scopes:
                            logger.error(f"❌ MISSING SCOPES: {missing_scopes}")
                            logger.error(f"❌ SOLUTION: User needs to re-authorize app or token needs refresh")
                        else:
                            logger.info(f"✅ All required scopes present in token")

                    except Exception as jwt_error:
                        logger.warning(f"Could not decode JWT token: {jwt_error}")

            async with aiohttp.ClientSession() as session:
                async with session.get(test_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        logger.info("✅ Token has valid permissions for Genie API")
                        return True
                    elif response.status == 403:
                        error_text = await response.text()
                        logger.error(f"❌ 403 FORBIDDEN: Token lacks permissions for Genie API")
                        logger.error(f"❌ Response: {error_text}")
                        logger.error(f"❌ This confirms the OAuth scope issue - user token doesn't have sql/dashboards.genie scopes")
                        return False
                    else:
                        error_text = await response.text()
                        logger.warning(f"Unexpected response when testing token: {response.status} - {error_text}")
                        return False
                
        except Exception as e:
            logger.error(f"Error testing token permissions: {e}")
            return False

    async def _start_or_continue_conversation(self, question: str) -> dict:
        """Start a new conversation or continue existing one with a question."""
        try:
            # Get workspace URL from centralized auth
            workspace_url = await self._get_workspace_url()

            # Ensure space_id is configured (no fallback)
            if not self._space_id:
                raise ValueError(
                    "Genie space ID is required but not configured. "
                    "Please set it via tool_config['spaceId']."
                )
            space_id = str(self._space_id)

            logger.info(f"Using space_id: {space_id} for Genie conversation")

            # Get authentication headers
            headers = await self._get_auth_headers()

            if not headers:
                raise Exception("No authentication headers available")

            # Test token permissions before proceeding
            try:
                has_permissions = await self._test_token_permissions(headers, workspace_url)
                if not has_permissions:
                    raise Exception("Token lacks necessary permissions for Genie API")
                else:
                    logger.info("Token permissions validated successfully")
            except Exception as perm_error:
                logger.error(f"Permission validation failed: {perm_error}")
                # Continue anyway, but log the issue

            if self._current_conversation_id:
                # Continue existing conversation
                url = self._make_url(workspace_url, f"/api/2.0/genie/spaces/{space_id}/conversations/{self._current_conversation_id}/messages")
                payload = {"content": question}

                logger.info(f"Continuing conversation at URL: {url}")
                logger.info(f"Payload: {payload}")

                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=payload, headers=headers) as response:
                        response.raise_for_status()
                        data = await response.json()

                # Extract message ID - handle different response formats
                message_id = None
                if "message_id" in data:
                    message_id = data["message_id"]
                elif "id" in data:
                    message_id = data["id"]
                elif "message" in data and "id" in data["message"]:
                    message_id = data["message"]["id"]

                return {
                    "conversation_id": self._current_conversation_id,
                    "message_id": message_id
                }
            else:
                # Start new conversation
                url = self._make_url(workspace_url, f"/api/2.0/genie/spaces/{space_id}/start-conversation")
                payload = {"content": question}

                logger.info(f"Starting new conversation with URL: {url}")
                logger.info(f"Payload: {payload}")
                logger.info(f"Headers: {headers}")

                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=payload, headers=headers) as response:
                        try:
                            response.raise_for_status()
                        except aiohttp.ClientResponseError as e:
                            logger.error(f"HTTP Error: {str(e)}")
                            logger.error(f"Response status: {response.status}")
                            error_text = await response.text()
                            logger.error(f"Response body: {error_text}")
                            raise

                        data = await response.json()

                # Handle different response formats
                conversation_id = None
                message_id = None

                # Try to extract conversation_id
                if "conversation_id" in data:
                    conversation_id = data["conversation_id"]
                elif "conversation" in data and "id" in data["conversation"]:
                    conversation_id = data["conversation"]["id"]

                # Try to extract message_id
                if "message_id" in data:
                    message_id = data["message_id"]
                elif "id" in data:
                    message_id = data["id"]
                elif "message" in data and "id" in data["message"]:
                    message_id = data["message"]["id"]

                self._current_conversation_id = conversation_id

                return {
                    "conversation_id": conversation_id,
                    "message_id": message_id
                }
        except Exception as e:
            logger.error(f"Error in _start_or_continue_conversation: {str(e)}")
            raise

    async def _get_message_status(self, conversation_id: str, message_id: str) -> dict:
        """Get the status and content of a message."""
        # Get workspace URL from centralized auth
        workspace_url = await self._get_workspace_url()

        if not self._space_id:
            raise ValueError("Genie space ID is required but not configured")
        space_id = str(self._space_id)
        url = self._make_url(
            workspace_url,
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
        )

        # Get authentication headers
        headers = await self._get_auth_headers()

        if not headers:
            raise Exception("No authentication headers available")

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                return await response.json()

    async def _get_query_result(self, conversation_id: str, message_id: str) -> dict:
        """Get the SQL query results for a message (without attachment_id - gets first/only result)."""
        # Get workspace URL from centralized auth
        workspace_url = await self._get_workspace_url()

        if not self._space_id:
            raise ValueError("Genie space ID is required but not configured")
        space_id = str(self._space_id)
        url = self._make_url(
            workspace_url,
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result"
        )

        # Get authentication headers
        headers = await self._get_auth_headers()

        if not headers:
            raise Exception("No authentication headers available")

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                return await response.json()

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

    def _extract_response(self, message_status: dict, result_data: Optional[dict] = None) -> str:
        """Extract the response from message status and query results."""
        response_parts = []
        
        # Extract text response
        text_response = ""
        if "attachments" in message_status:
            for attachment in message_status["attachments"]:
                if "text" in attachment and attachment["text"].get("content"):
                    text_response = attachment["text"]["content"]
                    break
        
        if not text_response:
            for field in ["content", "response", "answer", "text"]:
                if message_status.get(field):
                    text_response = message_status[field]
                    break
        
        # Add text response if it's meaningful (not empty and not just echoing the question)
        if text_response.strip() and text_response.strip() != message_status.get("content", "").strip():
            response_parts.append(text_response)
        
        # Process query results if available
        if result_data and "statement_response" in result_data:
            result = result_data["statement_response"].get("result", {})
            if "data_typed_array" in result and result["data_typed_array"]:
                data_array = result["data_typed_array"]
                
                # If no meaningful text response but we have data, add a summary
                if not response_parts:
                    response_parts.append(f"Query returned {len(data_array)} rows.")
                
                response_parts.append("\nQuery Results:")
                response_parts.append("-" * 20)
                
                # Format the results in a table
                if data_array:
                    first_row = data_array[0]
                    # Calculate column widths
                    widths = []
                    for i in range(len(first_row["values"])):
                        col_values = [str(row["values"][i].get("str", "")) for row in data_array]
                        max_width = max(len(val) for val in col_values) + 2
                        widths.append(max_width)
                    
                    # Format and add each row
                    for row in data_array:
                        row_values = []
                        for i, value in enumerate(row["values"]):
                            row_values.append(f"{value.get('str', ''):<{widths[i]}}")
                        response_parts.append("".join(row_values))
                
                response_parts.append("-" * 20)
        
        return "\n".join(response_parts) if response_parts else "No response content found"

    async def _run_async(self, question: str) -> str:
        """
        Async implementation of the Genie API query execution.
        """
        # Check if space_id is properly configured
        if not self._space_id:
            return """ERROR: Genie space ID is not configured!

Please configure the Genie space ID in the agent/task tool configuration when setting up the workflow.
To find your Genie space ID, go to your Databricks workspace and navigate to the Genie space.
"""
        
        # Handle empty inputs or 'None' as an input
        if not question or question.lower() == 'none':
            return """To use the GenieTool, please provide a specific business question. 
For example: 
- "What are the top 10 customers by revenue?"
- "Show me sales data for the last quarter"
- "What products have the highest profit margin?"

This tool can extract information from databases and provide structured data in response to your questions."""

        try:
            # Authentication is handled by centralized databricks_auth module
            # No need to check here - _get_auth_headers() will handle it

            # Start or continue conversation
            try:
                conv_data = await self._start_or_continue_conversation(question)
                conversation_id = conv_data["conversation_id"]
                message_id = conv_data["message_id"]
                
                if not conversation_id or not message_id:
                    return "Error: Failed to get conversation or message ID from Genie API."
                
                logger.info(f"Using conversation {conversation_id[:8]} with message {message_id[:8]}")
                
                # Status messages for better error reporting
                status_messages = {
                    "FAILED": "Genie query failed. This may be due to invalid syntax, permission issues, or data access problems.",
                    "CANCELLED": "Query was cancelled by the system or user.",
                    "QUERY_RESULT_EXPIRED": "Query results have expired. Please retry your question.",
                    "EXECUTING_QUERY": "Executing SQL query...",
                    "COMPILING_QUERY": "Compiling SQL query...",
                    "IN_PROGRESS": "Processing your question..."
                }

                # Poll for completion with exponential backoff
                attempt = 0
                backoff_threshold = self._backoff_after_seconds // self._base_polling_delay

                while attempt < self._max_retries:
                    status_data = await self._get_message_status(conversation_id, message_id)
                    status = status_data.get("status")

                    # Log current status
                    status_msg = status_messages.get(status, f"Status: {status}")
                    logger.info(f"Attempt {attempt + 1}/{self._max_retries}: {status_msg}")

                    if status in ["FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"]:
                        error_msg = status_messages.get(status, f"Query {status.lower()}")
                        logger.error(error_msg)
                        return error_msg

                    if status == "COMPLETED":
                        try:
                            result_data = await self._get_query_result(conversation_id, message_id)
                        except Exception as e:
                            logger.warning(f"Failed to get query result: {e}")
                            result_data = None

                        # Check if we have meaningful data in either the response or query results
                        has_meaningful_response = False
                        generated_sql = None

                        if "attachments" in status_data:
                            for attachment in status_data["attachments"]:
                                # Extract text response
                                if "text" in attachment and attachment["text"].get("content"):
                                    content = attachment["text"]["content"]
                                    if content.strip() and content.strip() != question.strip():
                                        has_meaningful_response = True

                                # Extract generated SQL query (NEW: 2025 feature)
                                if "query" in attachment and "statement" in attachment["query"]:
                                    generated_sql = attachment["query"]["statement"]
                                    logger.info(f"Generated SQL: {generated_sql}")

                        has_query_results = (
                            result_data is not None and
                            "statement_response" in result_data and
                            "result" in result_data["statement_response"] and
                            "data_typed_array" in result_data["statement_response"]["result"] and
                            len(result_data["statement_response"]["result"]["data_typed_array"]) > 0
                        )

                        if has_meaningful_response or has_query_results:
                            response = self._extract_response(status_data, result_data)
                            # Optionally append SQL for debugging
                            if generated_sql:
                                response += f"\n\n[Generated SQL: {generated_sql}]"
                            return response

                    # Calculate delay with exponential backoff after threshold
                    if self._enable_exponential_backoff and attempt >= backoff_threshold:
                        # Exponential backoff after configured threshold (default: 2 minutes)
                        backoff_multiplier = 2 ** ((attempt - backoff_threshold) // 5)
                        delay = min(self._base_polling_delay * backoff_multiplier, self._max_polling_delay)
                        logger.info(f"Applying exponential backoff: {delay}s (base: {self._base_polling_delay}s)")
                    else:
                        delay = self._base_polling_delay

                    await asyncio.sleep(delay)
                    attempt += 1
                
                total_timeout = self._polling_timeout_minutes
                return f"Query timed out after {total_timeout} minutes. Please try a simpler question or check your Databricks Genie configuration."

            except aiohttp.ClientConnectionError:
                return "Error connecting to Databricks Genie API. Please check your network connection and authentication configuration."

            except aiohttp.ClientResponseError as e:
                return f"HTTP Error {e.status} when connecting to Databricks Genie API. Please verify your API token and permissions."

        except Exception as e:
            error_msg = f"Error executing Genie request: {str(e)}"
            logger.error(error_msg)
            return f"Error using Genie: {str(e)}. Please verify your Databricks configuration."

