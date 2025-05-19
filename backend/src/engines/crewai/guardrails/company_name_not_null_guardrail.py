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
from src.core.unit_of_work import SyncUnitOfWork

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
            # Get the UnitOfWork singleton instance
            uow = SyncUnitOfWork.get_instance()
            logger.info(f"Got UnitOfWork instance: {uow}")
            
            # Initialize the UnitOfWork if needed
            if not getattr(uow, '_initialized', False):
                uow.initialize()
                logger.info("Initialized UnitOfWork for company name check")
            
            # Create the repository with the session from UnitOfWork
            repo = DataProcessingRepository(sync_session=uow._session)
            logger.info(f"Created DataProcessingRepository with sync_session: {repo}")
            
            # Check if table exists and has data
            total_count = repo.count_total_records_sync()
            if total_count == 0:
                logger.warning("No records found in the data_processing table")
                return {
                    "valid": False,
                    "feedback": "No records found in the database. Please ensure data is loaded."
                }
            
            # Check if any records with null company_name exist
            null_company_count = repo.count_null_company_name_sync()
            logger.info(f"Records with null company_name count: {null_company_count}")
            
            if null_company_count > 0:
                logger.warning(f"Found {null_company_count} records with null company_name")
                return {
                    "valid": False,
                    "feedback": f"There are {null_company_count} records with null company_name in the database. Please fix these records."
                }
            
            logger.info("All records have non-null company_name values")
            return {
                "valid": True,
                "feedback": "All records have non-null company_name values."
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