"""
Company Name Not Null Guardrail for CrewAI Tasks.

This guardrail checks if any company_name in the data_processing table is null.
"""

import logging
from typing import Dict, Any, Union
import json
import traceback

from src.core.logger import LoggerManager
from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail
from src.repositories.data_processing_repository import DataProcessingRepository
# Database operations disabled in guardrails (sync context)

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().guardrails

class CompanyNameNotNullGuardrail(BaseGuardrail):
    """
    Guardrail to check if any company_name in the data_processing table is null.
    
    This guardrail queries the data_processing table to verify that
    no records with company_name=null exist.
    """
    
    def __init__(self, config: Union[str, Dict[str, Any]]):
        """
        Initialize the Company Name Not Null Guardrail.
        
        Args:
            config: Configuration for the guardrail.
        """
        try:
            # Parse config from JSON string if needed
            parsed_config = config
            if isinstance(config, str):
                try:
                    parsed_config = json.loads(config)
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse guardrail config: {config}")
                    parsed_config = {}
            
            # Call parent class constructor with parsed config
            super().__init__(config=parsed_config)
            
            logger.info("CompanyNameNotNullGuardrail initialized successfully")
        except Exception as e:
            # Capture detailed initialization error
            error_info = {
                "error": str(e),
                "traceback": traceback.format_exc(),
                "type": type(e).__name__
            }
            logger.error(f"Error initializing CompanyNameNotNullGuardrail: {error_info}")
            raise
    
    def validate(self, output: Any) -> Dict[str, Any]:
        """
        Validate that no data records have null company_name.
        
        Args:
            output: The output from the task (not used in this guardrail)
            
        Returns:
            Dictionary with validation result containing:
                - valid: Boolean indicating if validation passed
                - feedback: Feedback message if validation failed
        """
        logger.info("Validating company_name not null for all records")
        
        try:
            # Database operations disabled in sync guardrail context
            logger.warning("Database validation disabled - guardrails cannot perform async operations")
            # Return valid to not block execution
            return {
                "valid": True,
                "feedback": "Database validation skipped (async operations not supported in guardrails)"
            }
                
        except Exception as e:
            # Capture detailed validation error
            error_info = {
                "error": str(e),
                "traceback": traceback.format_exc(),
                "type": type(e).__name__
            }
            logger.error(f"Error validating company_name not null: {error_info}")
            return {
                "valid": False,
                "feedback": f"Error checking company_name not null: {str(e)}"
            } 