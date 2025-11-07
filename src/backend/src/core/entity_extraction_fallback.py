"""
Entity Extraction Fallback for Problematic Models

This module provides a fallback mechanism for entity extraction when using
Databricks models that have known issues with CrewAI's entity extraction:
- Databricks Claude models
- Databricks GPT-OSS models

The fallback automatically uses databricks-llama-4-maverick for entity extraction
while keeping the original model for other agent tasks.
"""

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Fallback model for entity extraction
ENTITY_EXTRACTION_FALLBACK_MODEL = "databricks-llama-4-maverick"

# Models that need fallback for entity extraction
PROBLEMATIC_MODELS = [
    'databricks-claude',
    'databricks/databricks-claude',
    'gpt-oss',
    'databricks-gpt-oss',
    'databricks/databricks-gpt-oss',
]


def needs_entity_extraction_fallback(model_name: str) -> bool:
    """
    Check if a model needs fallback for entity extraction.
    
    Args:
        model_name: The model name to check
        
    Returns:
        True if the model needs fallback for entity extraction
    """
    if not model_name:
        return False
    
    model_lower = str(model_name).lower()
    
    for pattern in PROBLEMATIC_MODELS:
        if pattern in model_lower:
            logger.info(f"Model {model_name} needs entity extraction fallback")
            return True
    
    return False


def apply_entity_extraction_fallback():
    """
    Apply monkey patch to CrewAI's Converter to use fallback model for entity extraction
    when problematic models are detected.
    """
    try:
        from crewai.utilities.converter import Converter
        from crewai.llm import LLM
        
        # Store the original _create_instructor method
        original_create_instructor = Converter._create_instructor
        
        def patched_create_instructor(self):
            """
            Patched version that switches to Llama4 for problematic models during entity extraction.
            """
            # Check if the current LLM needs fallback
            if hasattr(self, 'llm') and hasattr(self.llm, 'model'):
                current_model = str(self.llm.model)
                
                if needs_entity_extraction_fallback(current_model):
                    logger.info(f"Switching from {current_model} to {ENTITY_EXTRACTION_FALLBACK_MODEL} for entity extraction")
                    
                    # Store original LLM
                    original_llm = self.llm
                    
                    try:
                        # Create fallback LLM with same configuration but different model
                        fallback_config = {}
                        
                        # Copy relevant attributes from original LLM
                        if hasattr(original_llm, 'temperature'):
                            fallback_config['temperature'] = original_llm.temperature
                        if hasattr(original_llm, 'max_tokens'):
                            fallback_config['max_tokens'] = original_llm.max_tokens
                        if hasattr(original_llm, 'api_base'):
                            fallback_config['api_base'] = original_llm.api_base
                        if hasattr(original_llm, 'api_key'):
                            fallback_config['api_key'] = original_llm.api_key
                        
                        # Create fallback LLM
                        fallback_llm = LLM(
                            model=f"databricks/{ENTITY_EXTRACTION_FALLBACK_MODEL}",
                            **fallback_config
                        )
                        
                        # Temporarily replace the LLM
                        self.llm = fallback_llm
                        
                        # Call original method with fallback LLM
                        result = original_create_instructor(self)
                        
                        # Restore original LLM
                        self.llm = original_llm
                        
                        logger.info(f"Successfully used {ENTITY_EXTRACTION_FALLBACK_MODEL} for entity extraction")
                        return result
                        
                    except Exception as e:
                        logger.error(f"Failed to use fallback model: {e}")
                        # Restore original LLM and continue with original method
                        self.llm = original_llm
            
            # Use original method for non-problematic models
            return original_create_instructor(self)
        
        # Apply the patch
        Converter._create_instructor = patched_create_instructor
        logger.info("Successfully applied entity extraction fallback patch")
        
    except ImportError as e:
        logger.warning(f"Could not import CrewAI Converter for patching: {e}")
    except Exception as e:
        logger.error(f"Failed to apply entity extraction fallback patch: {e}")


def apply_converter_llm_fallback():
    """
    Alternative approach: Patch the Converter's to_pydantic method to temporarily
    switch LLMs for entity extraction.
    """
    try:
        from crewai.utilities.converter import Converter
        from crewai.llm import LLM
        
        # Store the original to_pydantic method
        original_to_pydantic = Converter.to_pydantic
        
        def patched_to_pydantic(self, current_attempt=1):
            """
            Patched version that temporarily switches to Llama4 for entity extraction
            when problematic models are detected.
            """
            # Check if we need to use fallback
            if hasattr(self, 'llm') and hasattr(self.llm, 'model'):
                current_model = str(self.llm.model)
                
                if needs_entity_extraction_fallback(current_model):
                    logger.info(f"Using {ENTITY_EXTRACTION_FALLBACK_MODEL} fallback for entity extraction (original: {current_model})")
                    
                    # Store original LLM
                    original_llm = self.llm
                    
                    try:
                        # Create a minimal fallback LLM configuration
                        # We need to preserve the API base and key from the original
                        fallback_config = {
                            'model': f"databricks/{ENTITY_EXTRACTION_FALLBACK_MODEL}",
                        }
                        
                        # Copy authentication details
                        if hasattr(original_llm, 'api_base'):
                            fallback_config['api_base'] = original_llm.api_base
                        if hasattr(original_llm, 'api_key'):
                            fallback_config['api_key'] = original_llm.api_key
                        
                        # Create fallback LLM
                        self.llm = LLM(**fallback_config)
                        
                        # Force the method to think it supports function calling
                        # since Llama4 does support it
                        if hasattr(self.llm, 'supports_function_calling'):
                            original_supports = self.llm.supports_function_calling
                            self.llm.supports_function_calling = lambda: True
                        
                        # Call original method with fallback LLM
                        result = original_to_pydantic(self, current_attempt)
                        
                        # Restore original LLM
                        self.llm = original_llm
                        
                        return result
                        
                    except Exception as e:
                        logger.error(f"Fallback failed, attempting with original model: {e}")
                        # Restore original LLM
                        self.llm = original_llm
                        # Try with original model as last resort
                        return original_to_pydantic(self, current_attempt)
            
            # Use original method for non-problematic models
            return original_to_pydantic(self, current_attempt)
        
        # Apply the patch
        Converter.to_pydantic = patched_to_pydantic
        logger.info("Successfully applied Converter LLM fallback patch")
        
    except Exception as e:
        logger.error(f"Failed to apply Converter LLM fallback patch: {e}")


# Apply patches when module is imported
apply_entity_extraction_fallback()
apply_converter_llm_fallback()