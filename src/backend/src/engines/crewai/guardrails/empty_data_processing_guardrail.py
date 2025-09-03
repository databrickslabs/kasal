"""
Empty Data Processing Guardrail for CrewAI Tasks.

This guardrail checks if the data_processing table is empty by
using the repository to count the total records.
"""

import logging
from typing import Dict, Any, Union
import json
import traceback

from src.core.logger import LoggerManager
from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail
from src.repositories.data_processing_repository import DataProcessingRepository
from src.core.unit_of_work import SyncUnitOfWork

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().guardrails

class EmptyDataProcessingGuardrail(BaseGuardrail):
    """
    Guardrail to check if the data_processing table is empty.
    
    This guardrail queries the data_processing table to verify that
    it contains at least one record.
    """
    
    def __init__(self, config: Union[str, Dict[str, Any]]):
        """
        Initialize the Empty Data Processing Guardrail.
        
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
            
            logger.info("EmptyDataProcessingGuardrail initialized successfully")
        except Exception as e:
            # Capture detailed initialization error
            error_info = {
                "error": str(e),
                "traceback": traceback.format_exc(),
                "type": type(e).__name__
            }
            logger.error(f"Error initializing EmptyDataProcessingGuardrail: {error_info}")
            raise
    
    def validate(self, output: Any) -> Dict[str, Any]:
        """
        Validate that the data_processing table is empty.
        
        Args:
            output: The output from the task (not used in this guardrail)
            
        Returns:
            Dictionary with validation result containing:
                - valid: Boolean indicating if validation passed (true if table is empty)
                - feedback: Feedback message if validation failed
        """
        logger.info("Validating data_processing table is empty")
        
        try:
            # Get the UnitOfWork singleton instance
            uow = SyncUnitOfWork.get_instance()
            logger.info(f"Got UnitOfWork instance: {uow}")
            
            # Initialize the UnitOfWork if needed
            if not getattr(uow, '_initialized', False):
                uow.initialize()
                logger.info("Initialized UnitOfWork for empty table check")
            
            # Create the repository with the session from UnitOfWork
            repo = DataProcessingRepository(sync_session=uow._session)
            logger.info(f"Created DataProcessingRepository with sync_session: {repo}")
            
            # Check if any records exist at all
            total_count = repo.count_total_records_sync()
            logger.info(f"Found {total_count} total records in data_processing table")
            
            if total_count > 0:
                logger.warning(f"Found {total_count} records in the data_processing table")
                return {
                    "valid": False,
                    "feedback": f"The data_processing table contains {total_count} records. The table must be empty to proceed."
                }
            
            # Table is empty, validation passes
            logger.info("Data_processing table is empty as required")
            return {
                "valid": True,
                "feedback": "The data_processing table is empty as required."
            }
                
        except Exception as e:
            # Capture detailed validation error
            error_info = {
                "error": str(e),
                "traceback": traceback.format_exc(),
                "type": type(e).__name__
            }
            logger.error(f"Error validating empty table status: {error_info}")
            return {
                "valid": False,
                "feedback": f"Error checking if data_processing table is empty: {str(e)}"
            } 