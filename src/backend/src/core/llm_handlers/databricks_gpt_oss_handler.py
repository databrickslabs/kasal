"""
Databricks GPT-OSS Handler

This module provides specialized handling for Databricks GPT-OSS models which have
unique response formats that differ from standard OpenAI-compatible models.

GPT-OSS models return content as a list with reasoning blocks and text blocks,
rather than a simple string, which requires special handling for CrewAI integration.
"""

import os
from typing import Any, Dict, List, Optional, Union
from crewai import LLM
import json
import litellm

# Use centralized logger
from src.core.logger import get_logger

# Configure logger using centralized configuration
logger = get_logger(__name__)


class DatabricksGPTOSSHandler:
    """
    Handler for Databricks GPT-OSS models that manages response format transformation
    and parameter filtering.
    """
    
    @staticmethod
    def is_gpt_oss_model(model_name: str) -> bool:
        """
        Check if a model is a GPT-OSS variant.
        
        Args:
            model_name: The model name to check
            
        Returns:
            True if the model is a GPT-OSS variant, False otherwise
        """
        if not model_name:
            return False
        model_lower = model_name.lower()
        return "gpt-oss" in model_lower
    
    @staticmethod
    def extract_text_from_response(content: Union[str, List, Dict]) -> str:
        """
        Extract text content from GPT-OSS response format (Harmony format).
        
        GPT-OSS models return content in a structured format:
        [
            {"type": "reasoning", "summary": [...], "content": [...]},
            {"type": "text", "text": "actual response text"}
        ]
        
        Args:
            content: The response content from GPT-OSS model
            
        Returns:
            Extracted text content as a string
        """
        # If it's already a string, return it
        if isinstance(content, str):
            # Check if it's a JSON string that needs parsing
            if content.strip().startswith('[') or content.strip().startswith('{'):
                try:
                    import json
                    parsed = json.loads(content)
                    # Recursively process the parsed content
                    return DatabricksGPTOSSHandler.extract_text_from_response(parsed)
                except:
                    pass
            return content
        
        # If it's a list, process each item (Harmony format)
        if isinstance(content, list):
            logger.debug(f"Processing GPT-OSS list response with {len(content)} items")
            text_parts = []
            reasoning_text = []
            
            for i, item in enumerate(content):
                if isinstance(item, dict):
                    logger.debug(f"  Item {i}: dict with keys {item.keys()}")
                    
                    # Handle text blocks (primary output)
                    if item.get("type") == "text":
                        if "text" in item:
                            text_parts.append(item["text"])
                            logger.debug(f"    Found text block: {item['text'][:50] if item['text'] else 'empty'}...")
                    
                    # Handle reasoning blocks (Harmony format)
                    elif item.get("type") == "reasoning":
                        # Extract from content array if present (Harmony format)
                        if "content" in item and isinstance(item["content"], list):
                            for content_item in item["content"]:
                                if isinstance(content_item, dict):
                                    if content_item.get("type") == "reasoning_text" and "text" in content_item:
                                        reasoning_text.append(content_item["text"])
                                        logger.debug(f"    Found reasoning_text in content")
                        
                        # Also check summary for useful text
                        if "summary" in item:
                            summary = item["summary"]
                            if isinstance(summary, list):
                                for sum_item in summary:
                                    if isinstance(sum_item, dict) and sum_item.get("type") == "summary_text":
                                        if "text" in sum_item:
                                            # Only use if it's not metadata
                                            text = sum_item["text"]
                                            if not (text.strip().startswith('{') or 'suggestions' in text.lower()):
                                                reasoning_text.append(text)
                                                logger.debug(f"    Found useful summary_text")
                    
                    # Handle direct content field
                    elif "content" in item and not item.get("type"):
                        text_parts.append(str(item["content"]))
                        logger.debug(f"    Found content field")
                        
                elif isinstance(item, str):
                    text_parts.append(item)
                    logger.debug(f"  Item {i}: string - {item[:50] if item else 'empty'}...")
            
            # Prioritize text blocks over reasoning
            if text_parts:
                result = " ".join(text_parts).strip()
            elif reasoning_text:
                result = " ".join(reasoning_text).strip()
            else:
                result = ""
            
            if result:
                # Final check - ensure it's not metadata
                if result.strip().startswith('{'):
                    try:
                        import json
                        parsed = json.loads(result)
                        if 'suggestions' in parsed or 'quality' in parsed:
                            logger.warning("Detected metadata response, discarding")
                            return ""
                    except:
                        pass  # Not JSON or failed to parse, keep the content
                
                logger.debug(f"Successfully extracted text from GPT-OSS response: {result[:100]}...")
                return result
            else:
                logger.warning(f"No text extracted from GPT-OSS list response")
                return ""
        
        # If it's a dict, try to extract text
        if isinstance(content, dict):
            if "text" in content:
                return str(content["text"])
            elif "content" in content:
                # Check if content is a list (Harmony format)
                if isinstance(content["content"], list):
                    return DatabricksGPTOSSHandler.extract_text_from_response(content["content"])
                return str(content["content"])
        
        # Fallback: convert to string
        logger.warning(f"Unexpected GPT-OSS response format: {type(content)}")
        return str(content) if content else ""
    
    @staticmethod
    def filter_unsupported_params(params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter out parameters that GPT-OSS models don't support.
        
        Args:
            params: Dictionary of parameters
            
        Returns:
            Filtered dictionary with unsupported parameters removed
        """
        unsupported = ["stop", "stop_sequences", "stop_words"]
        filtered = params.copy()
        
        for param in unsupported:
            if param in filtered:
                logger.debug(f"Removing unsupported parameter '{param}' for GPT-OSS model")
                del filtered[param]
        
        return filtered
    
    @staticmethod
    def apply_monkey_patch():
        """
        Apply monkey patch to litellm's Databricks transformation to handle
        GPT-OSS response format differences.
        """
        try:
            from litellm.llms.databricks.chat.transformation import DatabricksConfig
            
            # Store the original methods
            original_extract_reasoning = DatabricksConfig.extract_reasoning_content
            original_extract_content = DatabricksConfig.extract_content_str
            
            # Patch extract_content_str - this is what actually extracts message content
            @staticmethod
            def patched_extract_content_str(content):
                """Patched version that handles GPT-OSS Harmony response format."""
                # Check if this is a GPT-OSS response format (list with Harmony format)
                if isinstance(content, list):
                    # Check if it looks like GPT-OSS format (has reasoning/text blocks)
                    is_gpt_oss = any(
                        isinstance(item, dict) and item.get("type") in ["reasoning", "text"]
                        for item in content
                    )
                    
                    if is_gpt_oss:
                        logger.info(f"[MONKEY PATCH extract_content_str] Detected GPT-OSS format")
                        # Use our extractor for GPT-OSS format
                        text_content = DatabricksGPTOSSHandler.extract_text_from_response(content)
                        if text_content:
                            logger.info(f"[MONKEY PATCH extract_content_str] Extracted: {text_content[:100]}...")
                        return text_content if text_content else ""
                
                # For non-GPT-OSS format, use original method
                try:
                    return original_extract_content(content)
                except Exception as e:
                    logger.debug(f"Original extract_content_str failed: {e}")
                    # Try our extraction as fallback
                    text_content = DatabricksGPTOSSHandler.extract_text_from_response(content)
                    return text_content if text_content else ""
            
            # Patch extract_reasoning_content too
            @staticmethod
            def patched_extract_reasoning_content(content):
                """Patched version that handles GPT-OSS Harmony response format."""
                # Check if this is a GPT-OSS response format (list with dicts or Harmony format)
                if isinstance(content, list):
                    # This is likely a GPT-OSS response in Harmony format
                    logger.info(f"[MONKEY PATCH reasoning] Detected GPT-OSS Harmony format")
                    
                    # Extract text from GPT-OSS Harmony format
                    text_content = DatabricksGPTOSSHandler.extract_text_from_response(content)
                    
                    # Return format: (text_content, reasoning_blocks)
                    # For GPT-OSS, we return the extracted text and None for reasoning blocks
                    return text_content if text_content else "", None
                    
                # For non-GPT-OSS format, use original method
                try:
                    return original_extract_reasoning(content)
                except Exception as e:
                    logger.debug(f"Original extract_reasoning_content failed: {e}")
                    text_content = DatabricksGPTOSSHandler.extract_text_from_response(content)
                    return text_content if text_content else "", None
            
            # Apply both patches
            DatabricksConfig.extract_content_str = patched_extract_content_str
            DatabricksConfig.extract_reasoning_content = patched_extract_reasoning_content
            logger.info("Successfully applied GPT-OSS response format patches (content_str and reasoning)")
            
        except ImportError:
            logger.warning("Could not import DatabricksConfig for patching - litellm version may be different")
        except Exception as e:
            logger.error(f"Failed to apply GPT-OSS patch: {e}")


class DatabricksGPTOSSLLM(LLM):
    """
    Custom LLM wrapper for Databricks GPT-OSS models that handles their unique
    response format and filters unsupported parameters.
    """
    
    def __init__(self, **kwargs):
        """Initialize the Databricks GPT-OSS LLM wrapper."""
        super().__init__(**kwargs)
        self._original_model_name = kwargs.get('model', '')
        logger.info(f"Initialized DatabricksGPTOSSLLM wrapper for model: {self._original_model_name}")
        print(f"[GPT-OSS INIT] Created wrapper for model: {self._original_model_name}")
    
    def _prepare_completion_params(self, messages, tools=None):
        """Override to log what parameters are being prepared."""
        logger.info(f"[_prepare_completion_params] Preparing params for {len(messages)} messages")
        print(f"[GPT-OSS DEBUG] Preparing completion params for {len(messages)} messages")
        
        # Call parent method
        params = super()._prepare_completion_params(messages, tools)
        
        logger.info(f"[_prepare_completion_params] Prepared params: model={params.get('model')}, has_messages={bool(params.get('messages'))}")
        print(f"[GPT-OSS DEBUG] Prepared params: model={params.get('model')}")
        
        # Filter out unsupported parameters
        filtered_params = DatabricksGPTOSSHandler.filter_unsupported_params(params)
        
        return filtered_params
    
    def call(self, messages, tools=None, callbacks=None, available_functions=None,
             from_task=None, from_agent=None, **kwargs):
        """
        Override the call method to handle GPT-OSS specific requirements.

        Note: Signature updated for CrewAI 1.9.x compatibility with response_model support.
        """
        logger.info(f"DatabricksGPTOSSLLM.call() invoked with {len(messages)} messages")

        # Filter out unsupported parameters
        kwargs = DatabricksGPTOSSHandler.filter_unsupported_params(kwargs)

        # Call the parent class method
        try:
            logger.info("Calling parent LLM.call()...")
            result = super().call(
                messages,
                tools=tools,
                callbacks=callbacks,
                available_functions=available_functions,
                from_task=from_task,
                from_agent=from_agent,
                **kwargs
            )
            
            # Log the response for debugging
            logger.info(f"Parent call returned, result type: {type(result)}, empty: {result is None or result == ''}")
            
            if result is None or result == "":
                logger.warning(f"GPT-OSS call returned empty result")
                logger.info(f"First message: {messages[0] if messages else 'No messages'}")
                # Print to console for immediate visibility
                print(f"[GPT-OSS DEBUG] Empty result from LLM call")
            else:
                logger.info(f"GPT-OSS call successful, response length: {len(str(result))}")
                logger.info(f"Response preview: {str(result)[:100]}...")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in GPT-OSS call: {e}")
            raise
    
    def _handle_non_streaming_response(
        self,
        params,
        callbacks=None,
        available_functions=None,
        from_task=None,
        from_agent=None,
        **kwargs,  # Accept additional kwargs for CrewAI 1.9.x compatibility (e.g., response_model)
    ):
        """
        Override to filter parameters and handle GPT-OSS response format.

        Note: Signature updated for CrewAI 1.9.x compatibility with response_model support.
        """
        # Filter out unsupported parameters
        if isinstance(params, dict):
            params = DatabricksGPTOSSHandler.filter_unsupported_params(params)
            logger.info(f"[_handle_non_streaming_response] Filtered params for GPT-OSS")
            logger.info(f"[_handle_non_streaming_response] Model in params: {params.get('model', 'NOT SET')}")

            # Add system instruction for better responses if missing
            if 'messages' in params and params['messages']:
                # Check if first message is system message
                if params['messages'][0].get('role') != 'system':
                    # Insert a system message to guide GPT-OSS
                    system_msg = {
                        'role': 'system',
                        'content': 'You are a helpful AI assistant. Please provide clear, direct responses to complete the given tasks. Focus on the specific requirements and deliver actionable results.'
                    }
                    params['messages'].insert(0, system_msg)
                    logger.info("Added system message for GPT-OSS guidance")

        # Call parent method
        try:
            logger.info("[_handle_non_streaming_response] Calling parent method...")
            logger.debug(f"[DEBUG] kwargs for parent: {list(kwargs.keys())}")

            response = super()._handle_non_streaming_response(
                params,
                callbacks,
                available_functions,
                from_task,
                from_agent,
                **kwargs,
            )

            logger.info(f"[_handle_non_streaming_response] Parent returned: type={type(response)}, empty={not response}")

            # If response is None or empty, don't use fallback - let it fail properly
            if response is None or response == "":
                logger.warning("GPT-OSS model returned empty response in _handle_non_streaming_response")
                return ""

            # Log the actual response for debugging
            logger.info(f"[_handle_non_streaming_response] Response preview: {str(response)[:100]}...")
            return response

        except TypeError as e:
            # Handle signature mismatch across CrewAI versions: if the parent
            # method does not accept the extra kwargs (e.g., response_model),
            # retry without them.
            logger.warning(f"TypeError in GPT-OSS _handle_non_streaming_response, retrying without extra kwargs: {e}")
            response = super()._handle_non_streaming_response(
                params,
                callbacks,
                available_functions,
                from_task,
                from_agent,
            )
            if response is None or response == "":
                return ""
            return response

        except Exception as e:
            logger.error(f"Error in GPT-OSS _handle_non_streaming_response: {e}")
            import traceback
            traceback.print_exc()
            raise


class DatabricksRetryLLM(LLM):
    """
    Custom LLM wrapper for Databricks models that adds retry logic for empty responses.

    Databricks models (including Llama 4 Maverick) can intermittently return empty responses,
    especially after many tool iterations. This wrapper retries the call with exponential
    backoff when an empty response is detected.

    Rate limit errors use longer backoffs (30s base) since Databricks rate limits
    typically reset after 60 seconds.
    """

    # Standard retry settings (for timeouts, connection errors, empty responses)
    MAX_RETRIES = 3
    INITIAL_BACKOFF = 1.0  # seconds

    # Rate limit specific settings - longer backoffs to allow quota reset
    RATE_LIMIT_MAX_RETRIES = 5
    RATE_LIMIT_INITIAL_BACKOFF = 30.0  # seconds (Databricks rate limits reset ~60s)
    RATE_LIMIT_MAX_BACKOFF = 120.0  # cap at 2 minutes

    # Request timeout - prevents hanging on unresponsive endpoints
    # litellm default is 6000s (100 min) which is way too long
    REQUEST_TIMEOUT = 120.0  # 2 minutes per request attempt

    def __init__(self, **kwargs):
        """Initialize the Databricks Retry LLM wrapper."""
        # Set default timeout if not provided to prevent hanging requests
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.REQUEST_TIMEOUT

        # IMPORTANT: Databricks provider ignores the timeout parameter in litellm.completion()
        # We must set litellm.request_timeout globally to enforce request timeouts
        # See: litellm's get_supported_openai_params() returns False for 'timeout' on Databricks
        litellm.request_timeout = self.REQUEST_TIMEOUT

        super().__init__(**kwargs)
        self._original_model_name = kwargs.get('model', '')
        timeout_val = kwargs.get('timeout', self.REQUEST_TIMEOUT)
        logger.info(f"Initialized DatabricksRetryLLM wrapper for model: {self._original_model_name} (timeout: {timeout_val}s, litellm.request_timeout: {litellm.request_timeout}s)")

    def _get_crew_logger(self):
        """Get the crew logger for subprocess-compatible logging."""
        try:
            from src.core.logger import LoggerManager
            return LoggerManager.get_instance().crew
        except Exception:
            return logger  # Fallback to module logger

    def _is_rate_limit_error(self, error_str: str) -> bool:
        """Check if an error string indicates a rate limit error.

        Args:
            error_str: Lowercase error string to check

        Returns:
            True if this is a rate limit error
        """
        return any(term in error_str for term in [
            'rate limit', 'ratelimit', 'too many requests', '429',
            'request_limit_exceeded', 'rate_limit_exceeded'
        ])

    def _is_retryable_error(self, error_str: str) -> bool:
        """Check if an error is retryable (including rate limits).

        Args:
            error_str: Lowercase error string to check

        Returns:
            True if this error should be retried
        """
        return any(term in error_str for term in [
            'timeout', 'connection', 'rate limit', 'ratelimit', 'too many requests',
            'service unavailable', '503', '429', '502', '504', 'gateway',
            'request_limit_exceeded'
        ])

    def _get_backoff_time(self, attempt: int, is_rate_limit: bool) -> float:
        """Calculate backoff time based on error type and attempt number.

        Rate limit errors use longer backoffs (30s, 60s, 120s) to allow
        Databricks quota to reset (~60 seconds).

        Other errors use standard short backoffs (1s, 2s, 4s).

        Args:
            attempt: Current attempt number (0-indexed)
            is_rate_limit: Whether this is a rate limit error

        Returns:
            Backoff time in seconds
        """
        if is_rate_limit:
            backoff = self.RATE_LIMIT_INITIAL_BACKOFF * (2 ** attempt)
            return min(backoff, self.RATE_LIMIT_MAX_BACKOFF)
        else:
            return self.INITIAL_BACKOFF * (2 ** attempt)

    def _get_max_retries(self, is_rate_limit: bool) -> int:
        """Get max retries based on error type.

        Rate limit errors get more retries since they just need time to reset.

        Args:
            is_rate_limit: Whether this is a rate limit error

        Returns:
            Maximum number of retry attempts
        """
        return self.RATE_LIMIT_MAX_RETRIES if is_rate_limit else self.MAX_RETRIES

    def _fix_message_format_for_llama(self, messages, crew_log):
        """
        Fix message format for Llama models only.

        Llama 4 (like Mistral) requires:
        1. Messages to alternate between user and assistant
        2. Last message should be 'user' role (not 'assistant')

        This fix is NOT applied to other models (Claude, Qwen, DBRX, etc.)
        which have their own message format requirements.

        Returns fixed messages list.
        """
        if not messages or not isinstance(messages, list):
            return messages

        # Only apply fix for Llama models - other models (Claude, Qwen, etc.) don't need it
        model_lower = self._original_model_name.lower()
        if 'llama' not in model_lower:
            return messages

        # Check if last message is 'assistant' - Llama doesn't like this
        if messages[-1].get("role") == "assistant":
            crew_log.info("[DatabricksRetryLLM] Fixing message format for Llama: adding user continuation prompt")
            return [*messages, {"role": "user", "content": "Please continue with your response."}]

        return messages

    def call(
        self,
        messages,
        tools=None,
        callbacks=None,
        available_functions=None,
        from_task=None,
        from_agent=None,
        **kwargs,  # Accept additional kwargs for CrewAI 1.9.x compatibility (e.g., response_model)
    ):
        """
        Override the call method to add retry logic for empty responses.

        When Databricks returns an empty response (which happens intermittently,
        especially after many tool iterations), we retry with exponential backoff.
        Also fixes message format for Llama 4 compatibility.

        Rate limit errors get special treatment with longer backoffs (30s, 60s, 120s)
        and more retries (5 vs 3) since they just need time for quota to reset.

        Note: kwargs accepts additional parameters like response_model (CrewAI 1.9.x structured outputs)
        """
        import time

        crew_log = self._get_crew_logger()
        last_error = None
        is_rate_limit = False
        attempt = 0

        # Fix message format for Llama 4 before making calls
        fixed_messages = self._fix_message_format_for_llama(messages, crew_log)
        msg_count = len(fixed_messages) if isinstance(fixed_messages, list) else 1

        while True:
            max_retries = self._get_max_retries(is_rate_limit)

            if attempt >= max_retries:
                break

            try:
                crew_log.info(f"[DatabricksRetryLLM] call() attempt {attempt + 1}/{max_retries} with {msg_count} messages")

                # Call the parent class method with all arguments (including new kwargs like response_model)
                result = super().call(
                    fixed_messages,
                    tools=tools,
                    callbacks=callbacks,
                    available_functions=available_functions,
                    from_task=from_task,
                    from_agent=from_agent,
                    **kwargs,
                )

                # Check if response is empty
                if result is None or result == "":
                    if attempt < max_retries - 1:
                        backoff = self._get_backoff_time(attempt, is_rate_limit=False)
                        crew_log.warning(
                            f"[DatabricksRetryLLM] Empty response (attempt {attempt + 1}/{max_retries}). "
                            f"Retrying in {backoff}s..."
                        )
                        time.sleep(backoff)
                        attempt += 1
                        continue
                    else:
                        crew_log.error(f"[DatabricksRetryLLM] Empty response after {max_retries} attempts - failing")
                        return ""

                # Success
                crew_log.info(f"[DatabricksRetryLLM] Success, response length: {len(str(result))}")
                return result

            except Exception as e:
                last_error = e
                error_str = str(e).lower()

                # Check error type
                is_retryable = self._is_retryable_error(error_str)
                is_rate_limit = self._is_rate_limit_error(error_str)

                # Update max_retries based on error type (rate limits get more retries)
                max_retries = self._get_max_retries(is_rate_limit)

                if is_retryable and attempt < max_retries - 1:
                    backoff = self._get_backoff_time(attempt, is_rate_limit)
                    error_type = "Rate limit" if is_rate_limit else "Retryable error"
                    crew_log.warning(
                        f"[DatabricksRetryLLM] {error_type} (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {backoff}s..."
                    )
                    time.sleep(backoff)
                    attempt += 1
                    continue
                else:
                    crew_log.error(f"[DatabricksRetryLLM] Non-retryable error: {e}")
                    raise

        # If we get here, we've exhausted retries
        if last_error:
            raise last_error
        return ""

    def _handle_non_streaming_response(
        self,
        params,
        callbacks=None,
        available_functions=None,
        from_task=None,
        from_agent=None,
        **kwargs,  # Accept additional kwargs for CrewAI 1.9.x compatibility (e.g., response_model)
    ):
        """
        Override to add retry logic for empty responses in non-streaming mode.

        Rate limit errors get special treatment with longer backoffs (30s, 60s, 120s)
        and more retries (5 vs 3) since they just need time for quota to reset.

        Note: Signature updated for CrewAI 1.9.x compatibility with response_model support.
        """
        import time

        crew_log = self._get_crew_logger()
        last_error = None
        is_rate_limit = False
        attempt = 0

        while True:
            max_retries = self._get_max_retries(is_rate_limit)

            if attempt >= max_retries:
                break

            try:
                # Call parent with all arguments including kwargs (for response_model, etc.)
                response = super()._handle_non_streaming_response(
                    params,
                    callbacks,
                    available_functions,
                    from_task,
                    from_agent,
                    **kwargs,
                )

                # Check if response is empty
                if response is None or response == "":
                    if attempt < max_retries - 1:
                        backoff = self._get_backoff_time(attempt, is_rate_limit=False)
                        crew_log.warning(
                            f"[DatabricksRetryLLM] Empty in _handle_non_streaming (attempt {attempt + 1}/{max_retries}). "
                            f"Retrying in {backoff}s..."
                        )
                        time.sleep(backoff)
                        attempt += 1
                        continue
                    else:
                        crew_log.error(f"[DatabricksRetryLLM] Empty after {max_retries} attempts in _handle_non_streaming")
                        return ""

                return response

            except Exception as e:
                last_error = e
                error_str = str(e).lower()

                # Check error type
                is_retryable = self._is_retryable_error(error_str)
                is_rate_limit = self._is_rate_limit_error(error_str)

                # Update max_retries based on error type (rate limits get more retries)
                max_retries = self._get_max_retries(is_rate_limit)

                if is_retryable and attempt < max_retries - 1:
                    backoff = self._get_backoff_time(attempt, is_rate_limit)
                    error_type = "Rate limit" if is_rate_limit else "Retryable error"
                    crew_log.warning(
                        f"[DatabricksRetryLLM] {error_type} in _handle_non_streaming (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {backoff}s..."
                    )
                    time.sleep(backoff)
                    attempt += 1
                    continue
                else:
                    crew_log.error(f"[DatabricksRetryLLM] Non-retryable error in _handle_non_streaming: {e}")
                    raise

        if last_error:
            raise last_error
        return ""


def apply_tool_calls_fix():
    """
    Fix CrewAI bug where tool_calls are silently dropped when the LLM returns
    both content text and tool_calls in the same response (common with Claude).

    Bug in LLM._handle_non_streaming_response (llm.py):
        if (not tool_calls or not available_functions) and text_response:
            return text_response  # Silently drops tool_calls!

    Fix: Change condition to `not tool_calls and text_response` so tool_calls
    are always returned to the executor when present.
    """
    import inspect
    import textwrap

    for method_name in ('_handle_non_streaming_response', '_ahandle_non_streaming_response'):
        try:
            method = getattr(LLM, method_name)
            source = inspect.getsource(method)

            if '(not tool_calls or not available_functions) and text_response' not in source:
                logger.info(f"LLM.{method_name}: tool_calls fix not needed (condition already correct)")
                continue

            fixed_source = source.replace(
                '(not tool_calls or not available_functions) and text_response',
                'not tool_calls and text_response',
            )

            # Compile with annotations future flag (CO_FUTURE_ANNOTATIONS = 0x100000)
            # This matches the `from __future__ import annotations` in crewai/llm.py
            import crewai.llm as llm_module
            code = compile(
                'from __future__ import annotations\n' + textwrap.dedent(fixed_source),
                f'<patched {method_name}>',
                'exec',
            )
            code_ns = {**llm_module.__dict__}
            exec(code, code_ns)
            setattr(LLM, method_name, code_ns[method_name])
            logger.info(f"Patched LLM.{method_name}: tool_calls no longer dropped when content also present")

        except Exception as e:
            logger.error(f"Failed to patch LLM.{method_name}: {e}")


# Apply the monkey patches when this module is imported
DatabricksGPTOSSHandler.apply_monkey_patch()
apply_tool_calls_fix()