"""
GPT-5 Compatible LLM Wrapper for CrewAI.

This module provides a custom LLM wrapper that intercepts all calls
and ensures GPT-5 compatibility by transforming parameters.
"""
import logging
from typing import Dict, Any, List, Optional, Union
from crewai import LLM

logger = logging.getLogger(__name__)


class GPT5CompatibleLLM(LLM):
    """
    A wrapper around CrewAI's LLM that ensures GPT-5 compatibility.
    
    This class overrides the call method to intercept and transform
    parameters before they reach litellm.
    """
    
    def __init__(self, **kwargs):
        """Initialize the GPT-5 compatible LLM."""
        # Check if this is a GPT-5 model and transform init params
        model = kwargs.get('model', '')
        has_api_key = 'api_key' in kwargs and kwargs['api_key']
        
        logger.info(f"GPT5CompatibleLLM init - model: {model}, has_api_key: {has_api_key}")
        
        if self._is_gpt5_model(model):
            kwargs = self._transform_params(kwargs)
            logger.info(f"Initialized GPT-5 compatible LLM for model: {model}")
        
        super().__init__(**kwargs)
    
    def _is_gpt5_model(self, model_name: str) -> bool:
        """Check if the model is a GPT-5 variant."""
        if not model_name:
            return False
        model_lower = model_name.lower()
        return 'gpt-5' in model_lower or 'gpt5' in model_lower
    
    def _transform_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Transform parameters for GPT-5 compatibility."""
        transformed = params.copy()
        
        # Remove max_tokens and use max_completion_tokens instead
        if 'max_tokens' in transformed:
            max_tokens_value = transformed.pop('max_tokens')
            if 'max_completion_tokens' not in transformed:
                transformed['max_completion_tokens'] = max_tokens_value
                logger.debug(f"Transformed max_tokens ({max_tokens_value}) to max_completion_tokens")
        
        # Handle temperature restriction - GPT-5 only supports temperature=1.0
        if 'temperature' in transformed and transformed['temperature'] != 1.0:
            logger.debug(f"GPT-5 only supports temperature=1.0, removing temperature={transformed['temperature']}")
            transformed.pop('temperature')  # Remove it to use default (1.0)
        
        # Remove unsupported parameters for GPT-5 and reasoning models
        # Based on OpenAI documentation, these parameters are not supported
        unsupported_params = [
            'stop',  # GPT-5 doesn't support stop sequences
            'logit_bias',  # Not supported in reasoning models
            'presence_penalty',  # Not supported in reasoning models
            'frequency_penalty',  # Not supported in reasoning models
        ]
        for param in unsupported_params:
            if param in transformed:
                removed_value = transformed.pop(param)
                logger.debug(f"Removed unsupported parameter '{param}' (value: {removed_value}) for GPT-5")
        
        return transformed
    
    def call(self, messages: Union[str, List[Dict[str, str]]], **kwargs) -> str:
        """
        Override the call method to transform parameters for GPT-5.
        
        This method intercepts all calls and ensures max_tokens is
        converted to max_completion_tokens for GPT-5 models.
        """
        logger.info(f"GPT5CompatibleLLM.call() invoked with model: {getattr(self, 'model', 'unknown')}")
        logger.debug(f"GPT5CompatibleLLM.call() kwargs before transformation: {list(kwargs.keys())}")
        
        # Check if this is a GPT-5 model
        if hasattr(self, 'model') and self._is_gpt5_model(str(self.model)):
            # Transform the kwargs to remove max_tokens
            if 'max_tokens' in kwargs:
                max_tokens_value = kwargs.pop('max_tokens')
                if 'max_completion_tokens' not in kwargs:
                    kwargs['max_completion_tokens'] = max_tokens_value
                    logger.debug(f"Transformed max_tokens to max_completion_tokens in call()")
        
        logger.debug(f"GPT5CompatibleLLM.call() kwargs after transformation: {list(kwargs.keys())}")
        
        # Call the parent implementation with transformed params
        return super().call(messages, **kwargs)
    
    def _handle_non_streaming_response(
        self, 
        params: Dict[str, Any], 
        callbacks: Optional[List[Any]] = None,
        available_functions: Optional[Dict[str, Any]] = None,
        from_task: Optional[Any] = None,
        from_agent: Optional[Any] = None
    ) -> Union[str, Any]:
        """
        Override to transform parameters before calling litellm.
        
        This is the internal method that CrewAI LLM uses to make the actual
        litellm call for non-streaming responses.
        """
        logger.info(f"GPT5CompatibleLLM._handle_non_streaming_response() called")
        logger.debug(f"Parameters before transformation: {list(params.keys())}")
        
        # Check if this is a GPT-5 model and transform params
        if hasattr(self, 'model') and self._is_gpt5_model(str(self.model)):
            params = self._transform_params(params)
            logger.info(f"Transformed parameters in _handle_non_streaming_response for GPT-5")
            logger.debug(f"Parameters after transformation: {list(params.keys())}")
        
        return super()._handle_non_streaming_response(
            params, callbacks, available_functions, from_task, from_agent
        )
    
    def _handle_streaming_response(
        self,
        params: Dict[str, Any],
        callbacks: Optional[List[Any]] = None,
        available_functions: Optional[Dict[str, Any]] = None,
        from_task: Optional[Any] = None,
        from_agent: Optional[Any] = None
    ) -> Union[str, Any]:
        """
        Override to transform parameters before calling litellm for streaming.
        
        This is the internal method that CrewAI LLM uses to make the actual
        litellm call for streaming responses.
        """
        # Check if this is a GPT-5 model and transform params
        if hasattr(self, 'model') and self._is_gpt5_model(str(self.model)):
            params = self._transform_params(params)
            logger.debug("Transformed parameters in _handle_streaming_response")
        
        return super()._handle_streaming_response(
            params, callbacks, available_functions, from_task, from_agent
        )