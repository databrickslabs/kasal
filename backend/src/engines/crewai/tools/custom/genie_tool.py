from crewai.tools import BaseTool
from typing import Optional, Type, Union, Dict, Any, List
from pydantic import BaseModel, Field, PrivateAttr, validator
import logging
import requests
import time
import os
from pathlib import Path


# Configure logger
logger = logging.getLogger(__name__)

class GenieInput(BaseModel):
    """Input schema for Genie."""
    question: str = Field(..., description="The question to be answered using Genie.")
    
    @validator('question', pre=True)
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

    def __init__(self, tool_config: Optional[dict] = None, tool_id: Optional[int] = None, token_required: bool = True):
        super().__init__()
        if tool_config is None:
            tool_config = {}
            
        # Set tool ID if provided
        if tool_id is not None:
            self._tool_id = tool_id
        
        # Get configuration from tool_config
        if tool_config:
            # Check if token is directly provided in config (first priority)
            if 'DATABRICKS_API_KEY' in tool_config:
                self._token = tool_config['DATABRICKS_API_KEY']
                logger.info("Using token from tool_config")
            elif 'token' in tool_config:
                self._token = tool_config['token']
                logger.info("Using token from config")
            
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
        
        # If token not set from config, try from environment (second priority)
        if not self._token:
            self._token = os.getenv("DATABRICKS_API_KEY")
            if self._token:
                logger.info("Using DATABRICKS_API_KEY from environment")
            
        # Set fallback values from environment if not set from config
        if not self._host:
            self._host = os.getenv("DATABRICKS_HOST", "e2-demo-field-eng.cloud.databricks.com")
            logger.info(f"Using host from environment or default: {self._host}")
            
        if not self._space_id:
            self._space_id = os.getenv("DATABRICKS_SPACE_ID", "01efdd2cd03211d0ab74f620f0023b77")
            logger.info(f"Using spaceId from environment or default: {self._space_id}")
        
        # If token is required but missing, log a warning instead of raising an error
        # This allows the tool to be created even without a token
        if token_required and not self._token:
            logger.warning("DATABRICKS_API_KEY is required but not provided. Tool will return an error when used.")

        if not self._host:
            logger.warning("Databricks host URL not provided. Using default value.")
            self._host = "dbc-8207fb58-e48b.cloud.databricks.com"
            
        # Log configuration
        logger.info("GenieTool Configuration:")
        logger.info(f"Tool ID: {self._tool_id}")
        logger.info(f"Host: {self._host}")
        logger.info(f"Space ID: {self._space_id}")
        
        # Log token (masked)
        if self._token:
            masked_token = f"{self._token[:4]}...{self._token[-4:]}" if len(self._token) > 8 else "***"
            logger.info(f"Token (masked): {masked_token}")
        else:
            logger.warning("No token provided - requests will fail without authentication")

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

    def _start_or_continue_conversation(self, question: str) -> dict:
        """Start a new conversation or continue existing one with a question."""
        try:
            # Ensure space_id is a string
            space_id = str(self._space_id) if self._space_id else "01efdd2cd03211d0ab74f620f0023b77"
            
            # Create headers with proper authentication
            headers = {
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json"
            }
            
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
        
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def _get_query_result(self, conversation_id: str, message_id: str) -> dict:
        """Get the SQL query results for a message."""
        space_id = str(self._space_id) if self._space_id else "01efdd2cd03211d0ab74f620f0023b77"
        url = self._make_url(
            f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result"
        )
        
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json"
        }
        
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
            # Return early if no token available
            if not self._token:
                return "Error: Cannot execute Genie request - no authentication token available. Please configure DATABRICKS_API_KEY."
                
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
