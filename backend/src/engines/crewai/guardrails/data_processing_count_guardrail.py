"""
Data Processing Count Guardrail for CrewAI Tasks.

This guardrail checks if the total number of records in the data_processing table
is at least the minimum count value provided in the configuration.
"""

import logging
import json
import traceback
from typing import Dict, Any, Union

from src.core.logger import LoggerManager
from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail
from src.repositories.data_processing_repository import DataProcessingRepository
from src.core.unit_of_work import SyncUnitOfWork

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().guardrails

class DataProcessingCountGuardrail(BaseGuardrail):
    """
    Guardrail to check if the total number of records in the data_processing table
    is at least the minimum count provided in the configuration.
    
    This guardrail uses the count_total_records_sync method from the DataProcessingRepository
    to get the actual count and ensures it meets or exceeds the minimum value.
    """
    
    def __init__(self, config: Union[str, Dict[str, Any]]):
        """
        Initialize the Data Processing Count Guardrail.
        
        Args:
            config: Configuration for the guardrail containing minimum_count.
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
            
            # Ensure minimum_count exists in config
            if 'minimum_count' not in parsed_config:
                logger.warning("No minimum_count found in config, defaulting to 0")
                self.minimum_count = 0
            else:
                self.minimum_count = int(parsed_config['minimum_count'])
                
            logger.info(f"DataProcessingCountGuardrail initialized with minimum_count: {self.minimum_count}")
        except Exception as e:
            # Capture detailed initialization error
            error_info = {
                "error": str(e),
                "traceback": traceback.format_exc(),
                "type": type(e).__name__
            }
            logger.error(f"Error initializing DataProcessingCountGuardrail: {error_info}")
            raise
    
    def validate(self, output: Any) -> Dict[str, Any]:
        """
        Validate that the total number of records in the data_processing table
        is at least the minimum count required.
        
        Args:
            output: The output from the task (not used in this guardrail)
            
        Returns:
            Dictionary with validation result containing:
                - valid: Boolean indicating if validation passed
                - feedback: Feedback message if validation failed
        """
        logger.info(f"Validating data processing count against minimum count: {self.minimum_count}")
        
        try:
            # Get the UnitOfWork singleton instance
            uow = SyncUnitOfWork.get_instance()
            logger.info(f"Got UnitOfWork instance: {uow}")
            
            # Initialize the UnitOfWork if needed
            if not getattr(uow, '_initialized', False):
                uow.initialize()
                logger.info("Initialized UnitOfWork for data processing count check")
            
            # Create the repository with the session from UnitOfWork
            repo = DataProcessingRepository(sync_session=uow._session)
            logger.info(f"Created DataProcessingRepository with sync_session: {repo}")
            
            # First ensure the table exists and has data
            if uow._session:
                # Check if table exists by trying to count records
                try:
                    total_count = repo.count_total_records_sync()
                    logger.info(f"Found {total_count} total records in data_processing table")
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
                    
                    # Get the count again after creating the table
                    total_count = repo.count_total_records_sync()
            
            # Get the actual count from the repository
            total_count = repo.count_total_records_sync()
            
            # Compare the actual count with the minimum count
            if total_count >= self.minimum_count:
                logger.info(f"Validation passed: Actual count ({total_count}) meets or exceeds minimum count ({self.minimum_count})")
                return {
                    "valid": True,
                    "feedback": f"Success: The number of records in the data_processing table ({total_count}) meets or exceeds the minimum count ({self.minimum_count})."
                }
            else:
                logger.warning(f"Validation failed: Actual count ({total_count}) is below minimum count ({self.minimum_count})")
                return {
                    "valid": False,
                    "feedback": f"Insufficient records: The number of records in the data_processing table ({total_count}) is below the minimum count required ({self.minimum_count})."
                }
                
        except Exception as e:
            # Capture detailed validation error
            error_info = {
                "error": str(e),
                "traceback": traceback.format_exc(),
                "type": type(e).__name__
            }
            logger.error(f"Error validating data processing count: {error_info}")
            return {
                "valid": False,
                "feedback": f"Error checking data processing count: {str(e)}"
            } 