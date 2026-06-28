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
            # Initialize UnitOfWork and repository (sync context)
            uow = SyncUnitOfWork.get_instance()
            if not getattr(uow, "_initialized", False):
                uow.initialize()
                logger.debug("SyncUnitOfWork initialized for empty table check")
                logger.info("Initialized UnitOfWork for empty table check")
            logger.info(f"Got UnitOfWork instance: {uow}")
            repo = DataProcessingRepository(sync_session=getattr(uow, "_session", None))
            logger.info(f"Created DataProcessingRepository with sync_session: {repo}")

            total = repo.count_total_records_sync()
            logger.info(f"Found {total} total records in data_processing table")
            if total == 0:
                logger.info("Data_processing table is empty as required")
                return {
                    "valid": True,
                    "feedback": "The data_processing table is empty as required."
                }
            else:
                logger.warning(f"Found {total} records in the data_processing table")
                return {
                    "valid": False,
                    "feedback": f"The data_processing table contains {total} records. The table must be empty to proceed."
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