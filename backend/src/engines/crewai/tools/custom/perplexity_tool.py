from crewai.tools import BaseTool
from typing import Optional, Type, Dict, Any, List
from pydantic import BaseModel, Field, PrivateAttr
import logging
import requests
import os
import json
from datetime import datetime

# Configure logger
logger = logging.getLogger(__name__)

# Input schema for PerplexitySearchTool
class PerplexitySearchInput(BaseModel):
    """Input schema for PerplexitySearchTool."""
    query: str = Field(..., description="The search query or question to pass to Perplexity AI.")

class PerplexitySearchTool(BaseTool):
    name: str = "PerplexityTool"
    description: str = (
        "A tool that performs web searches using Perplexity AI to find accurate and up-to-date information. "
        "Input should be a specific search query or question."
    )
    args_schema: Type[BaseModel] = PerplexitySearchInput
    _api_key: Optional[str] = PrivateAttr(default=None)
    _model: str = PrivateAttr(default="llama-3.1-sonar-small-128k-online")
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
    _web_search_options: Dict[str, Any] = PrivateAttr(default={"search_context_size": "high"})

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
        logger.info(f"Initializing PerplexitySearchTool")
        logger.info(f"API key provided directly: {bool(api_key)}")
        logger.info(f"API key in environment: {bool(os.environ.get('PERPLEXITY_API_KEY'))}")
        logger.info(f"result_as_answer: {result_as_answer}")
        
        # Try to get API key from environment or parameter
        if not api_key:
            api_key = os.environ.get("PERPLEXITY_API_KEY")
            if not api_key:
                logger.warning("No Perplexity API key provided. Using default API key.")
                api_key = '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'
                
        self._api_key = api_key
        
        # Safely log a portion of the key for diagnostic purposes
        if api_key:
            masked_key = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) > 8 else "***"
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
                    {
                        "role": "system",
                        "content": "Be precise and concise."
                    },
                    {
                        "role": "user",
                        "content": query
                    }
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
                "Content-Type": "application/json"
            }

            logger.info(f"Executing Perplexity API request with query: {query}")
            logger.debug(f"Perplexity API payload: {json.dumps(payload, indent=2)}")
            
            response = requests.post(url, json=payload, headers=headers)
            if not response.ok:
                logger.error(f"Error from Perplexity API: {response.status_code} - {response.text}")
                return f"Error from Perplexity API: {response.status_code} - {response.text}"
                
            response.raise_for_status()  # Raise exception for bad status codes
            
            result = response.json()
            answer = result.get('choices', [{}])[0].get('message', {}).get('content', '')
            
            # Log a preview of the answer
            logger.info(f"Perplexity answer: {answer[:100]}...")
            
            # Create a structured response that matches our PerplexityToolOutput schema
            output = {
                "answer": answer,
                "references": [],  # API doesn't currently return references
                "model": self._model,
                "search_context": {
                    "query": query,
                    "timestamp": datetime.now().isoformat()
                }
            }
            
            # Return the answer directly, as CrewAI expects a string response
            # The structured output is still available in logs for debugging
            logger.debug(f"Structured output: {json.dumps(output, indent=2)}")
            return answer

        except Exception as e:
            error_msg = f"Error executing Perplexity API request: {str(e)}"
            logger.error(error_msg)
            return error_msg