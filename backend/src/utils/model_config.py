"""
Model configuration utilities.

This module provides utility functions for getting model configurations,
validating models, and retrieving default settings.
"""

import logging
from typing import Dict, Any, Optional
from sqlalchemy import select
from sqlalchemy.orm import Session

# Configure logging
logger = logging.getLogger(__name__)

def get_model_config(model_key: str, db: Optional[Session] = None) -> Dict[str, Any]:
    """
    Get model configuration based on model key.
    
    Only returns configurations from the database.
    Returns None if model not found in database.
    
    Args:
        model_key: The model key (e.g., 'gpt-4', 'claude-3-opus')
        db: Optional database session
        
    Returns:
        Dictionary with model configuration or None if not found
    """
    # Only retrieve from database, no fallbacks
    if db:
        try:
            from src.models.model_config import ModelConfig
            
            # Query the database for the model configuration
            result = db.execute(
                select(ModelConfig).filter(ModelConfig.key == model_key)
            )
            model_config = result.scalars().first()
            
            if model_config:
                logger.info(f"Found model config for {model_key} in database")
                return {
                    "key": model_config.key,
                    "name": model_config.name,
                    "provider": model_config.provider,
                    "temperature": model_config.temperature,
                    "context_window": model_config.context_window,
                    "max_output_tokens": model_config.max_output_tokens,
                    "extended_thinking": model_config.extended_thinking,
                    "enabled": model_config.enabled
                }
            logger.warning(f"Model {model_key} not found in database")
            return None
        except Exception as e:
            logger.warning(f"Error retrieving model config from database: {str(e)}")
            return None
    
    logger.warning(f"No database session provided to get_model_config for {model_key}")
    return None

def get_max_rpm_for_model(model_key: str) -> int:
    """
    Get the maximum requests per minute (RPM) for a given model.
    
    Args:
        model_key: The model key (e.g., 'gpt-4', 'claude-3-opus')
        
    Returns:
        Integer representing the maximum RPM
    """
    # RPM limits for various models - these are conservative defaults
    rpm_limits = {
        # OpenAI models
        "gpt-4": 50,
        "gpt-4-0125-preview": 50,
        "gpt-4-1106-preview": 50,
        "gpt-4-turbo-preview": 50,
        "gpt-4o-mini": 100,
        "gpt-4o": 100,
        "o1-mini": 100,
        "o1": 100,
        "o3-mini": 100,
        "o3-mini-high": 100,
        "gpt-3.5-turbo": 200,
        "gpt-3.5-turbo-1106": 200,
        
        # Anthropic models
        "claude-3-opus-20240229": 5,  # More conservative for Opus
        "claude-3-5-sonnet-20241022": 10,
        "claude-3-5-haiku-20241022": 20,
        "claude-3-7-sonnet-20250219": 10,
        "claude-3-7-sonnet-20250219-thinking": 5,  # More conservative for thinking model
        
        # Ollama models are hosted locally, but still use conservative defaults
        "qwen2.5:32b": 5,
        "llama2": 10,
        "llama2:13b": 10,
        "llama3.2:latest": 5,
        "mistral": 10,
        "mixtral": 5,
        "codellama": 10,
        "mistral-nemo:12b-instruct-2407-q2_K": 5,
        "llama3.2:3b-text-q8_0": 20,  # Smaller model can handle more requests
        "gemma2:27b": 5,  # Large model, conservative limit
        "deepseek-r1:32b": 5,  # Large model, conservative limit
        "milkey/QwQ-32B-0305:q4_K_M": 5,  # Large model, conservative limit
        
        # DeepSeek models
        "deepseek-chat": 5,
        "deepseek-reasoner": 3,  # More conservative for reasoning
        
        # Databricks models
        "databricks-meta-llama-3-3-70b-instruct": 5,
        "databricks-meta-llama-3-1-405b-instruct": 3,  # Larger model, more conservative
        "databricks-claude-3-7-sonnet": 10,  # Claude model via Databricks
        
        # Google models
        "gemini-2.5-pro": 10,  # Standard rate limit for Gemini
        "gemini-2.0-flash": 10,  # Standard rate limit for Gemini Flash
    }
    
    # Return the RPM limit if it exists, otherwise return a default
    if model_key in rpm_limits:
        return rpm_limits[model_key]
        
    # Try to determine a sensible default based on model provider
    if "gpt-4" in model_key or "gpt4" in model_key:
        return 50
    elif "gpt-3.5" in model_key or "gpt3" in model_key:
        return 200
    elif "claude-3-opus" in model_key:
        return 5
    elif "claude-3-5" in model_key or "claude-3-haiku" in model_key:
        return 20
    elif "claude-3-7" in model_key:
        return 10
    elif "llama" in model_key and "3b" in model_key:
        return 20  # Smaller model
    elif "llama" in model_key or "mistral" in model_key or "mixtral" in model_key:
        return 5
    elif "deepseek" in model_key:
        return 5
    elif "databricks" in model_key:
        return 5
    elif "gemini" in model_key:
        return 10  # Default for Gemini models
    
    # Most conservative default for unknown models
    logger.warning(f"Using conservative default RPM limit for unknown model {model_key}")
    return 3 