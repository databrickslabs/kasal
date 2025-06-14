from crewai.tools import BaseTool
from typing import Optional, Type, Union, Dict, Any, List
from pydantic import BaseModel, Field, PrivateAttr, field_validator
import logging
import requests
import time
import os
from pathlib import Path
import asyncio


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
    _host: str = PrivateAttr(default=None)
    _space_id: str = PrivateAttr(default=None)
    _max_retries: int = PrivateAttr(default=60)
    _retry_delay: int = PrivateAttr(default=5)
    _current_conversation_id: str = PrivateAttr(default=None)
    _token: str = PrivateAttr(default=None)
    _tool_id: int = PrivateAttr(default=35)  # Default tool ID
    _user_token: str = PrivateAttr(default=None)  # For OBO authentication
    _use_oauth: bool = PrivateAttr(default=False)  # Flag for OAuth authentication

    def __init__(self, tool_config: Optional[dict] = None, tool_id: Optional[int] = None, token_required: bool = True, user_token: str = None):
        super().__init__()
        if tool_config is None:
            tool_config = {}
            
        # Set tool ID if provided
        if tool_id is not None:
            self._tool_id = tool_id
        
        # Set user token for OBO authentication if provided
        if user_token:
            self._user_token = user_token
            self._use_oauth = True
            logger.info("Using user token for OBO authentication")
        
        # Get configuration from tool_config
        if tool_config:
            # Check if user token is provided in config
            if 'user_token' in tool_config:
                self._user_token = tool_config['user_token']
                self._use_oauth = True
                logger.info("Using user token from tool_config for OBO authentication")
            
            # Check if token is directly provided in config (fallback to PAT)
            if not self._use_oauth:
                if 'DATABRICKS_API_KEY' in tool_config:
                    self._token = tool_config['DATABRICKS_API_KEY']
                    logger.info("Using PAT token from tool_config")
                elif 'token' in tool_config:
                    self._token = tool_config['token']
                    logger.info("Using PAT token from config")
            
            # Handle different possible key formats for host
            databricks_host = None
            # Check for the uppercase DATABRICKS_HOST (used in tool_factory.py)
            if 'DATABRICKS_HOST' in tool_config:
                databricks_host = tool_config['DATABRICKS_HOST']
                logger.info(f"Found DATABRICKS_HOST in config: {databricks_host}")
            # Also check for lowercase databricks_host as a fallback
            elif 'databricks_host' in tool_config:
                databricks_host = tool_config['databricks_host']
                logger.info(f"Found databricks_host in config: {databricks_host}")
            
            # Process host if found in any format
            if databricks_host:
                # Handle if databricks_host is a list
                if isinstance(databricks_host, list) and databricks_host:
                    databricks_host = databricks_host[0]
                    logger.info(f"Converting databricks_host from list to string: {databricks_host}")
                # Strip https:// and trailing slash if present
                if isinstance(databricks_host, str):
                    if databricks_host.startswith('https://'):
                        databricks_host = databricks_host[8:]
                    if databricks_host.endswith('/'):
                        databricks_host = databricks_host[:-1]
                self._host = databricks_host
                logger.info(f"Using host from config: {self._host}")
            
            # Set space_id from different possible formats
            if 'spaceId' in tool_config:
                # Handle if spaceId is a list
                if isinstance(tool_config['spaceId'], list) and tool_config['spaceId']:
                    self._space_id = tool_config['spaceId'][0]
                    logger.info(f"Converting spaceId from list to string: {self._space_id}")
                else:
                    self._space_id = tool_config['spaceId']
                    logger.info(f"Using spaceId from config: {self._space_id}")
            elif 'space' in tool_config:
                self._space_id = tool_config['space']
                logger.info(f"Using space from config: {self._space_id}")
            elif 'space_id' in tool_config:
                self._space_id = tool_config['space_id']
                logger.info(f"Using space_id from config: {self._space_id}")
        
        # Try enhanced authentication if not using OAuth
        if not self._use_oauth:
            try:
                # Try to get authentication through enhanced auth system
                from src.utils.databricks_auth import is_databricks_apps_environment
                if is_databricks_apps_environment():
                    self._use_oauth = True
                    logger.info("Detected Databricks Apps environment - using OAuth authentication")
                elif not self._token:
                    # Fall back to environment variables for PAT
                    self._token = os.getenv("DATABRICKS_API_KEY")
                    if self._token:
                        logger.info("Using DATABRICKS_API_KEY from environment")
            except ImportError as e:
                logger.debug(f"Enhanced auth not available: {e}")
                # Fall back to environment variables
                if not self._token:
                    self._token = os.getenv("DATABRICKS_API_KEY")
                    if self._token:
                        logger.info("Using DATABRICKS_API_KEY from environment")
            
        # Set fallback values from environment if not set from config
        if not self._host:
            self._host = os.getenv("DATABRICKS_HOST", "your-workspace.cloud.databricks.com")
            logger.info(f"Using host from environment or default: {self._host}")
            
        if not self._space_id:
            self._space_id = os.getenv("DATABRICKS_SPACE_ID", "01efdd2cd03211d0ab74f620f0023b77")
            logger.warning(f"Using default spaceId - this may not be the correct Genie space: {self._space_id}")
            logger.warning(f"To fix: Set the correct Genie space ID in tool configuration")
        
        # Check authentication requirements
        if token_required and not self._use_oauth and not self._token:
            logger.warning("DATABRICKS_API_KEY is required but not provided. Tool will attempt OAuth authentication or return an error when used.")

        if not self._host:
            logger.warning("Databricks host URL not provided. Using default value.")
            self._host = "your-workspace.cloud.databricks.com"
            
        # Log configuration
        logger.info("GenieTool Configuration:")
        logger.info(f"Tool ID: {self._tool_id}")
        logger.info(f"Host: {self._host}")
        logger.info(f"Space ID: {self._space_id}")
        logger.info(f"Authentication Method: {'OAuth/OBO' if self._use_oauth else 'PAT'}")
        
        # Log token (masked)
        if self._user_token:
            masked_token = f"{self._user_token[:4]}...{self._user_token[-4:]}" if len(self._user_token) > 8 else "***"
            logger.info(f"User Token (masked): {masked_token}")
        elif self._token:
            masked_token = f"{self._token[:4]}...{self._token[-4:]}" if len(self._token) > 8 else "***"
            logger.info(f"PAT Token (masked): {masked_token}")
        else:
            logger.warning("No token provided - will attempt to use enhanced authentication")

    def set_user_token(self, user_token: str):
        """Set user access token for OBO authentication."""
        self._user_token = user_token
        self._use_oauth = True
        logger.info("User token set for OBO authentication")

    def _make_url(self, path: str) -> str:
        """Create a full URL from a path."""
        # Ensure host is properly formatted
        host = self._host
        if host.startswith('https://'):
            host = host[8:]
        if host.endswith('/'):
            host = host[:-1]
            
        # Ensure path starts with a slash
        if not path.startswith('/'):
            path = '/' + path
            
        # Ensure spaceId is used correctly
        if "{self._space_id}" in path:
            path = path.replace("{self._space_id}", self._space_id)
            
        return f"https://{host}{path}"

    async def _get_auth_headers(self) -> dict:
        """Get authentication headers using proper OBO implementation."""
        try:
            if self._use_oauth and self._user_token:
                # Create an OBO token using service principal for proper Genie API access
                logger.info("Creating OBO token for Genie API access")
                try:
                    obo_token = await self._create_obo_token()
                    if obo_token:
                        logger.info("Successfully created OBO token for Genie API")
                        return {
                            "Authorization": f"Bearer {obo_token}",
                            "Content-Type": "application/json"
                        }
                    else:
                        logger.warning("Failed to create OBO token, falling back to user token")
                        # Fall back to user token if OBO creation fails
                        return {
                            "Authorization": f"Bearer {self._user_token}",
                            "Content-Type": "application/json"
                        }
                except Exception as obo_error:
                    logger.error(f"Error creating OBO token: {obo_error}")
                    # Fall back to user token
                    logger.info("Falling back to direct user token")
                    return {
                        "Authorization": f"Bearer {self._user_token}",
                        "Content-Type": "application/json"
                    }
            elif self._use_oauth:
                # Try to get OBO token through enhanced auth system
                from src.utils.databricks_auth import get_databricks_auth_headers
                headers, error = await get_databricks_auth_headers(user_token=self._user_token)
                if error:
                    logger.error(f"Failed to get OAuth headers: {error}")
                    return None
                return headers
            else:
                # Use traditional PAT authentication
                if not self._token:
                    logger.error("No authentication token available")
                    return None
                return {
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json"
                }
        except ImportError:
            # Fall back to PAT if enhanced auth not available
            if not self._token:
                logger.error("No authentication token available and enhanced auth not available")
                return None
            return {
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json"
            }
        except Exception as e:
            logger.error(f"Error getting auth headers: {e}")
            return None

    async def _create_obo_token(self) -> Optional[str]:
        """Create an On-Behalf-Of token using service principal for Genie API access."""
        try:
            # First, let's try to validate the user token by checking its format
            if not self._user_token:
                logger.error("No user token available for OBO creation")
                return None
            
            logger.info(f"User token format check - starts with: {self._user_token[:20]}...")
            logger.info(f"User token length: {len(self._user_token)}")
            
            # Check if it looks like a JWT token (should start with 'eyJ')
            if self._user_token.startswith('eyJ'):
                logger.info("User token appears to be a JWT token")
            else:
                logger.warning("User token does not appear to be a JWT token")
            
            # Use the enhanced auth system to create an OBO token
            from src.utils.databricks_auth import get_databricks_auth_headers
            
            # Try to use the enhanced auth system which should handle OBO creation
            headers, error = await get_databricks_auth_headers(user_token=self._user_token)
            
            if error:
                logger.error(f"Enhanced auth system failed: {error}")
                # For now, return the original user token as fallback
                logger.info("Returning original user token as fallback")
                return self._user_token
            
            # The enhanced auth system should return the OBO token in the Authorization header
            auth_header = headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                obo_token = auth_header[7:]  # Remove 'Bearer ' prefix
                logger.info("Successfully extracted token from enhanced auth system")
                return obo_token
            else:
                logger.warning("No Bearer token found in enhanced auth headers, using original token")
                return self._user_token
                
        except Exception as e:
            logger.error(f"Error in OBO token creation: {e}")
            # Return the original user token as fallback
            return self._user_token

    async def _test_token_permissions(self, headers: dict) -> bool:
        """Test if the token has proper permissions by trying a simple API call."""
        try:
            # Try to list Genie spaces to test permissions
            test_url = f"https://{self._host}/api/2.0/genie/spaces"
            import requests
            
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
                        logger.error(f"❌ SCOPE ISSUE: Token scopes: {payload.get('scope', 'No scope found')}")
                        logger.error(f"❌ REQUIRED SCOPES: sql, dashboards.genie")
                        logger.error(f"❌ App OAuth Client ID: 37a7cfdc-b1c4-41b2-9341-cb1a6a630233")
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
            
            response = requests.get(test_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                logger.info("✅ Token has valid permissions for Genie API")
                return True
            elif response.status_code == 403:
                logger.error(f"❌ 403 FORBIDDEN: Token lacks permissions for Genie API")
                logger.error(f"❌ Response: {response.text}")
                logger.error(f"❌ This confirms the OAuth scope issue - user token doesn't have sql/dashboards.genie scopes")
                return False
            else:
                logger.warning(f"Unexpected response when testing token: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error testing token permissions: {e}")
            return False

    def _start_or_continue_conversation(self, question: str) -> dict:
        """Start a new conversation or continue existing one with a question."""
        try:
            # Ensure space_id is a string
            space_id = str(self._space_id) if self._space_id else "01efdd2cd03211d0ab74f620f0023b77"
            
            logger.info(f"Using space_id: {space_id} for Genie conversation")
            
            # Get authentication headers
            headers = None
            try:
                # Try to get headers using async method
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                headers = loop.run_until_complete(self._get_auth_headers())
                loop.close()
            except Exception as e:
                logger.debug(f"Async auth failed, falling back to sync: {e}")
                # Fall back to simple PAT headers
                if self._user_token:
                    headers = {
                        "Authorization": f"Bearer {self._user_token}",
                        "Content-Type": "application/json"
                    }
                elif self._token:
                    headers = {
                        "Authorization": f"Bearer {self._token}",
                        "Content-Type": "application/json"
                    }
            
            if not headers:
                raise Exception("No authentication headers available")
            
            # Test token permissions before proceeding
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                has_permissions = loop.run_until_complete(self._test_token_permissions(headers))
                loop.close()
                
                if not has_permissions:
                    raise Exception("Token lacks necessary permissions for Genie API")
                else:
                    logger.info("Token permissions validated successfully")
            except Exception as perm_error:
                logger.error(f"Permission validation failed: {perm_error}")
                # Continue anyway, but log the issue
            
            if self._current_conversation_id:
                # Continue existing conversation
                url = self._make_url(f"/api/2.0/genie/spaces/{space_id}/conversations/{self._current_conversation_id}/messages")
                payload = {"content": question}
                
                logger.info(f"Continuing conversation at URL: {url}")
                logger.info(f"Payload: {payload}")
                
                response = requests.post(url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
                
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
                url = self._make_url(f"/api/2.0/genie/spaces/{space_id}/start-conversation")
                payload = {"content": question}
                
                logger.info(f"Starting new conversation with URL: {url}")
                logger.info(f"Payload: {payload}")
                logger.info(f"Headers: {headers}")
                
                response = requests.post(url, json=payload, headers=headers)
                
                try:
                    response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    logger.error(f"HTTP Error: {str(e)}")
                    logger.error(f"Response status: {response.status_code}")
                    logger.error(f"Response body: {response.text}")
                    raise
                
                data = response.json()
                
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

    def _get_message_status(self, conversation_id: str, message_id: str) -> dict:
        """Get the status and content of a message."""
        space_id = str(self._space_id) if self._space_id else "01efdd2cd03211d0ab74f620f0023b77"
        url = self._make_url(
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
        )
        
        # Get authentication headers
        headers = None
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            headers = loop.run_until_complete(self._get_auth_headers())
            loop.close()
        except Exception as e:
            logger.debug(f"Async auth failed, falling back to sync: {e}")
            if self._user_token:
                headers = {
                    "Authorization": f"Bearer {self._user_token}",
                    "Content-Type": "application/json"
                }
            elif self._token:
                headers = {
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json"
                }
        
        if not headers:
            raise Exception("No authentication headers available")
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def _get_query_result(self, conversation_id: str, message_id: str) -> dict:
        """Get the SQL query results for a message."""
        space_id = str(self._space_id) if self._space_id else "01efdd2cd03211d0ab74f620f0023b77"
        url = self._make_url(
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result"
        )
        
        # Get authentication headers
        headers = None
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            headers = loop.run_until_complete(self._get_auth_headers())
            loop.close()
        except Exception as e:
            logger.debug(f"Async auth failed, falling back to sync: {e}")
            if self._user_token:
                headers = {
                    "Authorization": f"Bearer {self._user_token}",
                    "Content-Type": "application/json"
                }
            elif self._token:
                headers = {
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json"
                }
        
        if not headers:
            raise Exception("No authentication headers available")
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

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

    def _run(self, question: str) -> str:
        """
        Execute a query using the Genie API and wait for results.
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
            # Check if authentication is available
            if not self._use_oauth and not self._token and not self._user_token:
                return "Error: Cannot execute Genie request - no authentication available. Please configure authentication or use Databricks Apps."
                
            # Start or continue conversation
            try:
                conv_data = self._start_or_continue_conversation(question)
                conversation_id = conv_data["conversation_id"]
                message_id = conv_data["message_id"]
                
                if not conversation_id or not message_id:
                    return "Error: Failed to get conversation or message ID from Genie API."
                
                logger.info(f"Using conversation {conversation_id[:8]} with message {message_id[:8]}")
                
                # Poll for completion
                attempt = 0
                while attempt < self._max_retries:
                    status_data = self._get_message_status(conversation_id, message_id)
                    status = status_data.get("status")
                    
                    if status in ["FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"]:
                        error_msg = f"Query {status.lower()}"
                        logger.error(error_msg)
                        return error_msg
                    
                    if status == "COMPLETED":
                        try:
                            result_data = self._get_query_result(conversation_id, message_id)
                        except requests.exceptions.RequestException:
                            result_data = None
                        
                        # Check if we have meaningful data in either the response or query results
                        has_meaningful_response = False
                        if "attachments" in status_data:
                            for attachment in status_data["attachments"]:
                                if "text" in attachment and attachment["text"].get("content"):
                                    content = attachment["text"]["content"]
                                    if content.strip() and content.strip() != question.strip():
                                        has_meaningful_response = True
                                        break
                        
                        has_query_results = (
                            result_data is not None and 
                            "statement_response" in result_data and
                            "result" in result_data["statement_response"] and
                            "data_typed_array" in result_data["statement_response"]["result"] and
                            len(result_data["statement_response"]["result"]["data_typed_array"]) > 0
                        )
                        
                        if has_meaningful_response or has_query_results:
                            return self._extract_response(status_data, result_data)
                    
                    time.sleep(self._retry_delay)
                    attempt += 1
                
                return f"Query timed out after {self._max_retries * self._retry_delay} seconds. Please try a simpler question or check your Databricks Genie configuration."
            
            except requests.exceptions.ConnectionError:
                return f"Error connecting to Databricks Genie API at {self._host}. Please check your network connection and host configuration."
            
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if hasattr(e, 'response') and hasattr(e.response, 'status_code') else 'unknown'
                return f"{str(e)} HTTP Error {status_code} when connecting to Databricks Genie API. Please verify your API token and permissions."

        except Exception as e:
            error_msg = f"Error executing Genie request: {str(e)}"
            logger.error(error_msg)
            return f"Error using Genie: {str(e)}. Please verify your Databricks configuration."

    def __call__(self, *args, **kwargs):
        """
        Make the tool callable with flexible argument handling.
        This helps with various agent formats for tool usage.
        """
        # Handle cases where no arguments are provided or 'None' is provided
        if not args and not kwargs:
            return self._run("Please provide instructions on how to extract database information")
            
        # If args are provided, use the first one as the question
        if args:
            # Handle the case where the first arg is None or 'None'
            if args[0] is None or (isinstance(args[0], str) and args[0].lower() == 'none'):
                return self._run("Please provide instructions on how to extract database information")
            return self._run(str(args[0]))
            
        # If kwargs are provided, look for 'question' key
        if 'question' in kwargs:
            return self._run(kwargs['question'])
            
        # Try other common parameter names
        for param_name in ['query', 'input', 'text', 'q']:
            if param_name in kwargs:
                return self._run(kwargs[param_name])
                
        # If we can't find a suitable parameter, use a generic message
        return self._run("Please provide a specific question to query the database with")
