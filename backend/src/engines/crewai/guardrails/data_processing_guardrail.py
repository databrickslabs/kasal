"""
Data Processing Status Guardrail for CrewAI Tasks.

This guardrail checks if all data records have been processed by querying
the data_processing table in the database.
"""

import logging
from typing import Dict, Any, Union, List
import json
import traceback

from src.core.logger import LoggerManager
from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail
from src.repositories.data_processing_repository import DataProcessingRepository
from src.core.unit_of_work import SyncUnitOfWork

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().guardrails

class DataProcessingGuardrail(BaseGuardrail):
    """
    Guardrail to check if all data records have been processed.
    
    This guardrail queries the data_processing table to verify that
    no records with processed=false remain.
    """
    
    def __init__(self, config: Union[str, Dict[str, Any]]):
        """
        Initialize the Data Processing Guardrail.
        
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
            
            logger.info("DataProcessingGuardrail initialized successfully")
        except Exception as e:
            # Capture detailed initialization error
            error_info = {
                "error": str(e),
                "traceback": traceback.format_exc(),
                "type": type(e).__name__
            }
            logger.error(f"Error initializing DataProcessingGuardrail: {error_info}")
            raise
    
    def validate(self, output: Any) -> Dict[str, Any]:
        """
        Validate that all data records have been processed.
        
        Args:
            output: The output from the task (not used in this guardrail)
            
        Returns:
            Dictionary with validation result containing:
                - valid: Boolean indicating if validation passed
                - feedback: Feedback message if validation failed
        """
        logger.info("Validating data processing status for all records")
        
        try:
            # Get the UnitOfWork singleton instance
            uow = SyncUnitOfWork.get_instance()
            logger.info(f"Got UnitOfWork instance: {uow}")
            
            # Initialize the UnitOfWork if needed
            if not getattr(uow, '_initialized', False):
                uow.initialize()
                logger.info("Initialized UnitOfWork for data processing status check")
            
            # Create the repository with the session from UnitOfWork
            repo = DataProcessingRepository(sync_session=uow._session)
            logger.info(f"Created DataProcessingRepository with sync_session: {repo}")
            
            # First ensure the table exists and has test data
            if uow._session:
                # Check if table exists by trying to count records
                try:
                    total_count = repo.count_total_records_sync()
                    logger.info(f"Found {total_count} total records in data_processing table")
                    
                    if total_count == 0:
                        # Create test data through repository, not direct SQL
                        # This follows best practices of using repository layer for DB access
                        logger.info("No records found, creating test data")
                        repo.create_record_sync(che_number="CHE12345", processed=False)
                        repo.create_record_sync(che_number="CHE67890", processed=True)
                        uow._session.commit()
                        logger.info("Created test records via repository")
                except Exception as e:
                    # Table likely doesn't exist, create it through repository
                    logger.warning(f"Error checking records, table may not exist: {str(e)}")
                    
                    # Use repository method to create table (no direct SQL in service layer)
                    repo.create_table_if_not_exists_sync()
                    
                    # Insert test data properly through repository
                    repo.create_record_sync(che_number="CHE12345", processed=False)
                    repo.create_record_sync(che_number="CHE67890", processed=True)
                    uow._session.commit()
                    logger.info("Created table and test records via repository")
            
            # Check if any records exist at all
            total_count = repo.count_total_records_sync()
            if total_count == 0:
                logger.warning("No records found in the data_processing table")
                return {
                    "valid": False,
                    "feedback": "No records found in the database. Please ensure data is loaded."
                }
                
            # Check if any unprocessed records exist
            unprocessed_count = repo.count_unprocessed_records_sync()
            logger.info(f"Unprocessed records count: {unprocessed_count}")
            
            if unprocessed_count > 0:
                logger.warning(f"Found {unprocessed_count} unprocessed records")
                return {
                    "valid": False,
                    "feedback": f"There are still {unprocessed_count} unprocessed records in the database. Please try again later."
                }
            
            logger.info("All records have been processed successfully")
            return {
                "valid": True,
                "feedback": "All data records have been processed successfully."
            }
                
        except Exception as e:
            # Capture detailed validation error
            error_info = {
                "error": str(e),
                "traceback": traceback.format_exc(),
                "type": type(e).__name__
            }
            logger.error(f"Error validating data processing status: {error_info}")
            return {
                "valid": False,
                "feedback": f"Error checking data processing status: {str(e)}"
            } 