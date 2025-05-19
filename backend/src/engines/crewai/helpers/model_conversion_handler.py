"""
Model Conversion Handler for CrewAI.

This module provides utilities for making Pydantic model conversion compatible
with different LLM providers that have varying JSON schema support.
"""

import os
import json
import re
import logging
from typing import Any, Dict, Optional, Type, Tuple
from pydantic import BaseModel

# Import CrewAI's Converter class
from crewai.utilities.converter import Converter

# Setup logging
logger = logging.getLogger(__name__)

def detect_llm_provider(agent_model: Any) -> Optional[str]:
    """
    Detect the LLM provider based on the agent's model name.
    
    Args:
        agent_model: The agent's model attribute
        
    Returns:
        Provider name if detected, None otherwise
    """
    if not agent_model or not hasattr(agent_model, "lower"):
        return None
        
    model_str = str(agent_model).lower()
    
    if "gemini" in model_str:
        return "gemini"
    elif "databricks" in model_str:
        return "databricks"
    elif "azure" in model_str:
        return "azure"
    elif "anthropic" in model_str:
        return "anthropic"
    elif "ollama" in model_str:
        return "ollama"
    
    # Default to None if no specific provider detected
    return None

def simplify_schema(schema: Dict) -> Dict:
    """
    Simplify a JSON schema by removing fields that cause issues with certain LLM providers.
    
    Args:
        schema: The JSON schema to simplify
        
    Returns:
        Simplified schema
    """
    if not isinstance(schema, dict):
        return schema
        
    # Fields that commonly cause issues with LLMs
    problematic_fields = [
        "default", 
        "additionalProperties", 
        "allOf", 
        "anyOf", 
        "oneOf", 
        "not"
    ]
    
    # Create a copy to avoid modifying the original
    simplified = schema.copy()
    
    # Remove problematic fields
    for field in problematic_fields:
        if field in simplified:
            del simplified[field]
    
    # Process nested properties recursively
    if "properties" in simplified and isinstance(simplified["properties"], dict):
        for prop_name, prop_schema in simplified["properties"].items():
            simplified["properties"][prop_name] = simplify_schema(prop_schema)
            
    # Process array items
    if "items" in simplified and isinstance(simplified["items"], dict):
        simplified["items"] = simplify_schema(simplified["items"])
        
    return simplified

class GeminiCompatConverter(Converter):
    """Custom converter that filters out JSON schema fields not supported by Gemini"""
    
    def __init__(self, text=None, llm=None, model=None, instructions=None, pydantic_cls=None, *args, **kwargs):
        """Initialize with required fields for CrewAI compatibility"""
        # First add all required fields
        self.text = text
        self.llm = llm
        self.model = model
        self.instructions = instructions
        self.pydantic_cls = pydantic_cls
        
        # Then call parent constructor if needed
        try:
            super().__init__(text=text, llm=llm, model=model, 
                            instructions=instructions, pydantic_cls=pydantic_cls, 
                            *args, **kwargs)
        except Exception as e:
            logger.warning(f"Error in parent constructor: {str(e)}")
    
    def _modify_schema(self, schema_dict):
        """Remove fields not supported by Gemini"""
        return simplify_schema(schema_dict)
    
    def to_pydantic(self, output):
        """Convert output to Pydantic object with Gemini compatibility"""
        logger.info(f"Converting output to Pydantic with Gemini compatibility: {str(output)[:100]}...")
        
        # Extract text if output is not a string
        if not isinstance(output, str):
            text = str(output)
        else:
            text = output
        
        try:
            # Try direct JSON parsing first
            try:
                # If the output is already in JSON format
                parsed = json.loads(text)
                logger.info("Successfully parsed output as JSON")
                return self.pydantic_cls(**parsed)
            except json.JSONDecodeError:
                logger.info("Output is not valid JSON, trying alternative extraction")
                
                # Try to extract JSON from text using regex
                json_match = re.search(r'```json\s*([\s\S]*?)\s*```|{[\s\S]*}', text)
                if json_match:
                    json_str = json_match.group(1) or json_match.group(0)
                    # Clean up the extracted JSON
                    json_str = json_str.strip()
                    parsed = json.loads(json_str)
                    logger.info("Successfully extracted and parsed JSON from output")
                    return self.pydantic_cls(**parsed)
                
                # If we got here, try falling back to the parent implementation
                try:
                    return super().to_pydantic(output)
                except Exception as e:
                    logger.error(f"Parent to_pydantic failed: {str(e)}")
                    return None
                
        except Exception as e:
            logger.error(f"Error converting to Pydantic: {str(e)}")
            try:
                # Last resort - try parent implementation
                return super().to_pydantic(output)
            except:
                return None

class DatabricksCompatConverter(Converter):
    """Custom converter for Databricks models to avoid authentication issues with Instructor"""
    
    def __init__(self, text=None, llm=None, model=None, instructions=None, pydantic_cls=None, *args, **kwargs):
        """Initialize with required fields for CrewAI compatibility"""
        # First add all required fields
        self.text = text
        self.llm = llm
        self.model = model
        self.instructions = instructions
        self.pydantic_cls = pydantic_cls
        
        # Then call parent constructor if needed
        try:
            super().__init__(text=text, llm=llm, model=model, 
                            instructions=instructions, pydantic_cls=pydantic_cls, 
                            *args, **kwargs)
        except Exception as e:
            logger.warning(f"Error in parent constructor: {str(e)}")
    
    def _modify_schema(self, schema_dict):
        """Simplify schema for Databricks compatibility"""
        return simplify_schema(schema_dict)
    
    def to_pydantic(self, output):
        """Convert output to Pydantic object without using Instructor"""
        logger.info(f"Converting output to Pydantic with Databricks compatibility: {str(output)[:100]}...")
        
        # Extract text if output is not a string
        if not isinstance(output, str):
            text = str(output)
        else:
            text = output
            
        try:
            # Try direct JSON parsing first
            try:
                # If the output is already in JSON format
                parsed = json.loads(text)
                logger.info("Successfully parsed output as JSON")
                return self.pydantic_cls(**parsed)
            except json.JSONDecodeError:
                logger.info("Output is not valid JSON, trying alternative extraction")
                
                # Try to extract JSON from text using regex
                json_match = re.search(r'```json\s*([\s\S]*?)\s*```|{[\s\S]*}', text)
                if json_match:
                    json_str = json_match.group(1) or json_match.group(0)
                    # Clean up the extracted JSON
                    json_str = json_str.strip()
                    parsed = json.loads(json_str)
                    logger.info("Successfully extracted and parsed JSON from output")
                    return self.pydantic_cls(**parsed)
                
                # If we got here, try falling back to the parent implementation
                try:
                    return super().to_pydantic(output)
                except Exception as e:
                    logger.error(f"Parent to_pydantic failed: {str(e)}")
                    return None
                
        except Exception as e:
            logger.error(f"Error converting to Pydantic: {str(e)}")
            try:
                # Last resort - try parent implementation
                return super().to_pydantic(output)
            except:
                return None

def get_compatible_converter_for_model(agent, pydantic_class):
    """
    Get a compatible converter for a given agent model.
    
    Args:
        agent: The agent that will execute the task
        pydantic_class: The Pydantic model class for output conversion
        
    Returns:
        Tuple[converter_cls, output_pydantic, use_output_json, is_compatible]
    """
    # Default response - use standard Pydantic conversion
    default_response = (None, pydantic_class, False, False)
    
    # Check if agent has a model attribute
    if not hasattr(agent, 'llm') or not hasattr(agent.llm, 'model'):
        return default_response
    
    # Detect the provider
    provider = detect_llm_provider(agent.llm.model)
    if not provider:
        return default_response
    
    logger.info(f"Detected {provider} model, using compatible conversion approach")
    
    # For problematic providers, always default to output_json approach
    # which is more reliable than custom converters
    if provider in ["gemini", "databricks"]:
        # Use output_json approach by default (most reliable)
        return (None, None, True, True)
        
    # This code is kept but will not run unless we change the above condition
    if provider == "gemini":
        # First preference - use our compatible converter class
        try:
            # Test if we can properly instantiate it
            test_converter = GeminiCompatConverter(pydantic_cls=pydantic_class)
            return (GeminiCompatConverter, pydantic_class, False, True)
        except Exception as e:
            logger.error(f"Error testing GeminiCompatConverter: {str(e)}")
            # Fall back to output_json approach
            return (None, None, True, True)
    elif provider == "databricks":
        # First preference - use our compatible converter class
        try:
            # Test if we can properly instantiate it
            test_converter = DatabricksCompatConverter(pydantic_cls=pydantic_class)
            return (DatabricksCompatConverter, pydantic_class, False, True)
        except Exception as e:
            logger.error(f"Error testing DatabricksCompatConverter: {str(e)}")
            # Fall back to output_json approach
            return (None, None, True, True)
    
    # Default to standard approach
    return default_response

def configure_output_json_approach(task_args, pydantic_class):
    """
    Configure the task to use output_json approach instead of output_pydantic.
    
    Args:
        task_args: The task arguments dictionary
        pydantic_class: The Pydantic model class
        
    Returns:
        Updated task_args dictionary
    """
    # Get the JSON schema from the Pydantic model
    json_schema = pydantic_class.model_json_schema()
    
    # Simplify the schema
    simplified_schema = simplify_schema(json_schema)
    
    # Add as JSON output format
    task_args['output_json'] = True
    
    # Add expectation in expected_output to format as JSON
    task_args['expected_output'] = (
        f"{task_args['expected_output']}\n\n"
        f"Please provide your output as a valid JSON object following this schema:\n"
        f"```json\n{json.dumps(simplified_schema, indent=2)}\n```"
    )
    
    logger.info(f"Using output_json=True instead of Pydantic model conversion")
    
    return task_args 