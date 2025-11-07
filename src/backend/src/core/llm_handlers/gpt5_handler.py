"""
GPT-5 Model Handler for parameter mapping.

This module provides a handler for GPT-5 models that automatically maps
max_tokens to max_completion_tokens to work with the new OpenAI API requirements.
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class GPT5Handler:
    """
    Handler for GPT-5 specific parameter transformations.
    
    GPT-5 models require max_completion_tokens instead of max_tokens.
    This handler provides the transformation logic.
    """
    
    @staticmethod
    def is_gpt5_model(model_name: str) -> bool:
        """
        Check if the model is a GPT-5 variant.
        
        Args:
            model_name: The model name to check
            
        Returns:
            True if this is a GPT-5 model, False otherwise
        """
        if not model_name:
            return False
        model_lower = model_name.lower()
        return 'gpt-5' in model_lower or 'gpt5' in model_lower
    
    @staticmethod
    def transform_params(params: Dict[str, Any], context: str = None) -> Dict[str, Any]:
        """
        Transform parameters for GPT-5 compatibility.
        
        Maps max_tokens to max_completion_tokens and removes unsupported
        parameters to prevent API errors.
        
        Args:
            params: The original parameters dictionary
            context: Optional context hint ('dispatcher' for intent detection, etc.)
            
        Returns:
            Transformed parameters suitable for GPT-5
        """
        # Create a copy to avoid modifying the original
        transformed = params.copy()
        
        # Check if we need to transform
        model = transformed.get('model', '')
        if GPT5Handler.is_gpt5_model(model):
            # Handle max_tokens -> max_completion_tokens mapping
            if 'max_tokens' in transformed:
                max_tokens_value = transformed.pop('max_tokens')
                # Only set if max_completion_tokens is not already present
                if 'max_completion_tokens' not in transformed:
                    transformed['max_completion_tokens'] = max_tokens_value
                    logger.info(f"Mapped max_tokens ({max_tokens_value}) to max_completion_tokens for GPT-5 model: {model}")
                else:
                    logger.debug(f"max_completion_tokens already set, removed conflicting max_tokens for GPT-5 model: {model}")
            
            # Handle temperature restriction - GPT-5 only supports temperature=1.0
            if 'temperature' in transformed and transformed['temperature'] != 1.0:
                logger.debug(f"GPT-5 only supports temperature=1.0, removing temperature={transformed['temperature']}")
                transformed.pop('temperature')  # Remove it to use default (1.0)
            
            # Apply dispatcher-specific optimizations for GPT-5
            if context == 'dispatcher':
                # For dispatcher/intent detection, optimize for speed
                # Reduce max_completion_tokens for faster response
                if transformed.get('max_completion_tokens', 0) > 500:
                    transformed['max_completion_tokens'] = 500
                logger.info(f"Applied dispatcher optimizations for GPT-5: max_completion_tokens=500")
                
                # Note: reasoning_effort and verbosity parameters are GPT-5 API features
                # but not yet supported by litellm. Once litellm adds support, we can enable:
                # transformed['reasoning_effort'] = 'minimal'  # For fastest response
                # transformed['verbosity'] = 'low'  # For concise responses
            
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
                    logger.debug(f"Removed unsupported parameter '{param}' (value: {removed_value}) for GPT-5 model: {model}")
        
        return transformed
    
    @staticmethod
    def optimize_for_dispatcher(params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optimize GPT-5 parameters specifically for dispatcher/intent detection.
        
        This method applies aggressive optimizations for minimal latency,
        suitable for quick intent detection tasks.
        
        Args:
            params: The original parameters dictionary
            
        Returns:
            Optimized parameters for fast dispatcher responses
        """
        return GPT5Handler.transform_params(params, context='dispatcher')
    
    @staticmethod
    def extract_content_from_response(response: Dict[str, Any], model_name: str) -> str:
        """
        Extract content from GPT-5 response, handling any special response formats.
        
        GPT-5 might return content in different fields or formats compared to 
        other models. This method handles those differences.
        
        Args:
            response: The raw response from litellm
            model_name: The model name to check if it's GPT-5
            
        Returns:
            The extracted content string
        """
        if not response or "choices" not in response or not response["choices"]:
            logger.warning(f"Invalid response structure for {model_name}: {response}")
            return ""
        
        # Handle both dict and object responses from litellm
        choice = response["choices"][0]
        
        # Get message - it might be a dict or an object
        if isinstance(choice, dict):
            message = choice.get("message", {})
        else:
            # It's an object, access attributes
            message = getattr(choice, "message", None)
        
        # Convert message object to dict if needed
        if message and not isinstance(message, dict):
            # Message is an object, convert to dict or access attributes directly
            if hasattr(message, "__dict__"):
                message_dict = message.__dict__
            elif hasattr(message, "content"):
                # Direct attribute access
                content = getattr(message, "content", "")
                reasoning = getattr(message, "reasoning", "") if hasattr(message, "reasoning") else ""
                
                # Debug logging for GPT-5
                if GPT5Handler.is_gpt5_model(model_name):
                    logger.debug(f"GPT-5 message type: {type(message)}")
                    logger.debug(f"GPT-5 content field: {content[:100] if content else 'EMPTY/NONE'}")
                    if reasoning:
                        logger.debug(f"GPT-5 reasoning field: {reasoning[:100]}")
                
                # Return content or reasoning for GPT-5
                if content:
                    return content
                elif reasoning and GPT5Handler.is_gpt5_model(model_name):
                    logger.info(f"GPT-5: Using reasoning field as content")
                    return reasoning
                else:
                    if GPT5Handler.is_gpt5_model(model_name):
                        logger.warning(f"GPT-5: No content found. Message type: {type(message)}")
                    return ""
            else:
                logger.warning(f"Unknown message type for {model_name}: {type(message)}")
                return ""
        else:
            message_dict = message if isinstance(message, dict) else {}
        
        # Debug logging for GPT-5 with dict
        if GPT5Handler.is_gpt5_model(model_name) and isinstance(message_dict, dict):
            logger.debug(f"GPT-5 message keys: {list(message_dict.keys())}")
            content_value = message_dict.get('content')
            logger.debug(f"GPT-5 content type: {type(content_value)}, value: {repr(content_value)[:200]}")
            logger.debug(f"GPT-5 content field: {message_dict.get('content', 'NOT PRESENT')[:100] if message_dict.get('content') else 'EMPTY/NONE'}")
        
        # Standard content extraction from dict
        content = message_dict.get("content", "") if isinstance(message_dict, dict) else ""
        
        # For GPT-5, check alternative fields if content is empty
        if GPT5Handler.is_gpt5_model(model_name) and (not content or not content.strip()):
            # Check for reasoning field (GPT-5 reasoning models might use this)
            if isinstance(message_dict, dict) and "reasoning" in message_dict and message_dict["reasoning"]:
                logger.info(f"GPT-5: Using reasoning field as content")
                content = message_dict["reasoning"]
            elif not isinstance(message_dict, dict):
                logger.warning(f"GPT-5: Message is not a dict, type: {type(message)}")
            else:
                logger.warning(f"GPT-5: No content found in standard or alternative fields. Message keys: {list(message_dict.keys()) if message_dict else 'None'}")
            
        return content if content else ""
    
    @staticmethod
    def prepare_llm_params(model_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare LLM parameters with GPT-5 specific handling.
        
        Args:
            model_name: The name of the model
            config: The configuration dictionary
            
        Returns:
            Parameters ready for LLM initialization
        """
        params = config.copy()
        
        if GPT5Handler.is_gpt5_model(model_name):
            # For GPT-5, use max_completion_tokens instead of max_tokens
            if 'max_output_tokens' in config and config['max_output_tokens']:
                params['max_completion_tokens'] = config['max_output_tokens']
                # Remove max_tokens if it exists to avoid conflicts
                params.pop('max_tokens', None)
                logger.info(f"Configured GPT-5 model {model_name} with max_completion_tokens: {config['max_output_tokens']}")
        
        return params