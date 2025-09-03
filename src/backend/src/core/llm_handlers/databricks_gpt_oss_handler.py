"""
Databricks GPT-OSS Handler

This module provides specialized handling for Databricks GPT-OSS models which have
unique response formats that differ from standard OpenAI-compatible models.

GPT-OSS models return content as a list with reasoning blocks and text blocks,
rather than a simple string, which requires special handling for CrewAI integration.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Union
from crewai import LLM
import json
import sys

# Configure logger with both file and console output for debugging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Add console handler for immediate feedback
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('[GPT-OSS] %(levelname)s: %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


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
    
    def call(self, messages, callbacks=None, **kwargs):
        """
        Override the call method to handle GPT-OSS specific requirements.
        """
        logger.info(f"DatabricksGPTOSSLLM.call() invoked with {len(messages)} messages")
        
        # Filter out unsupported parameters
        kwargs = DatabricksGPTOSSHandler.filter_unsupported_params(kwargs)
        
        # Call the parent class method
        try:
            logger.info("Calling parent LLM.call()...")
            result = super().call(messages, callbacks=callbacks, **kwargs)
            
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
    
    def _handle_non_streaming_response(self, params, callbacks=None, available_functions=None, from_task=None, from_agent=None):
        """
        Override to filter parameters and handle GPT-OSS response format.
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
            
            # Debug: Check what we're about to call
            logger.info(f"[DEBUG] About to call parent with: params type={type(params)}, callbacks={callbacks is not None}, available_functions={available_functions is not None}, from_task={from_task is not None}, from_agent={from_agent is not None}")
            
            # Try calling with just the first 3 arguments that parent might expect
            try:
                response = super()._handle_non_streaming_response(
                    params,
                    callbacks,
                    available_functions
                )
                logger.info("[DEBUG] Successfully called with 3 arguments")
            except TypeError as e:
                logger.info(f"[DEBUG] Failed with 3 args: {e}, trying with 5")
                # Try with all 5 arguments
                response = super()._handle_non_streaming_response(
                    params,
                    callbacks,
                    available_functions,
                    from_task,
                    from_agent
                )
            
            logger.info(f"[_handle_non_streaming_response] Parent returned: type={type(response)}, empty={not response}")
            
            # If response is None or empty, don't use fallback - let it fail properly
            if response is None or response == "":
                logger.warning("GPT-OSS model returned empty response in _handle_non_streaming_response")
                return ""
            
            # Log the actual response for debugging
            logger.info(f"[_handle_non_streaming_response] Response preview: {str(response)[:100]}...")
            return response
            
        except Exception as e:
            logger.error(f"Error in GPT-OSS _handle_non_streaming_response: {e}")
            import traceback
            traceback.print_exc()
            raise


# Apply the monkey patch when this module is imported
DatabricksGPTOSSHandler.apply_monkey_patch()