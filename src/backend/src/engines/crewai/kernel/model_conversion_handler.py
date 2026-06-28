"""
Model Conversion Handler for CrewAI.

This module provides utilities for making Pydantic model conversion compatible
with different LLM providers that have varying JSON schema support.
"""

import os
import json
import logging
from typing import Any, Dict, Optional, Type, Tuple
from pydantic import BaseModel


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