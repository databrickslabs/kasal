"""
Factory for creating guardrail instances.
"""

import json
import logging
import traceback
from typing import Dict, Any, Optional, Union

from src.core.logger import LoggerManager
from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail
from src.engines.crewai.guardrails.company_count_guardrail import CompanyCountGuardrail
from src.engines.crewai.guardrails.data_processing_guardrail import DataProcessingGuardrail
from src.engines.crewai.guardrails.empty_data_processing_guardrail import EmptyDataProcessingGuardrail
from src.engines.crewai.guardrails.data_processing_count_guardrail import DataProcessingCountGuardrail
from src.engines.crewai.guardrails.company_name_not_null_guardrail import CompanyNameNotNullGuardrail
from src.engines.crewai.guardrails.minimum_number_guardrail import MinimumNumberGuardrail

# Use the centralized logger
logger = LoggerManager.get_instance().guardrails

class GuardrailFactory:
    """
    Factory for creating guardrail instances.
    """
    
    @staticmethod
    def create_guardrail(config: Union[str, Dict[str, Any]]) -> Optional[BaseGuardrail]:
        """
        Create a guardrail instance based on the provided configuration.
        
        Args:
            config: Guardrail configuration
            
        Returns:
            BaseGuardrail instance or None if creation fails
        """
        # Parse config if it's a string
        if isinstance(config, str):
            try:
                config_data = json.loads(config)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse guardrail config: {config}")
                return None
        else:
            config_data = config
        
        # Extract guardrail type
        guardrail_type = config_data.get('type')
        if not guardrail_type:
            logger.error("No guardrail type specified in config")
            return None
        
        logger.info(f"Creating guardrail of type: {guardrail_type}")
        
        # Create the appropriate guardrail based on type
        try:
            guardrail = None
            
            if guardrail_type == "company_count":
                guardrail = CompanyCountGuardrail(config)
            elif guardrail_type == "data_processing":
                # Try to create with detailed logging
                logger.info("Creating DataProcessingGuardrail...")
                guardrail = DataProcessingGuardrail(config)
                logger.info(f"Successfully created DataProcessingGuardrail: {guardrail}")
            elif guardrail_type == "empty_data_processing":
                # Create the EmptyDataProcessingGuardrail
                logger.info("Creating EmptyDataProcessingGuardrail...")
                guardrail = EmptyDataProcessingGuardrail(config)
                logger.info(f"Successfully created EmptyDataProcessingGuardrail: {guardrail}")
            elif guardrail_type == "data_processing_count":
                # Create the DataProcessingCountGuardrail
                logger.info("Creating DataProcessingCountGuardrail...")
                guardrail = DataProcessingCountGuardrail(config)
                logger.info(f"Successfully created DataProcessingCountGuardrail: {guardrail}")
            elif guardrail_type == "company_name_not_null":
                # Create the CompanyNameNotNullGuardrail
                logger.info("Creating CompanyNameNotNullGuardrail...")
                guardrail = CompanyNameNotNullGuardrail(config)
                logger.info(f"Successfully created CompanyNameNotNullGuardrail: {guardrail}")
            elif guardrail_type == "minimum_number":
                # Create the MinimumNumberGuardrail
                logger.info("Creating MinimumNumberGuardrail...")
                guardrail = MinimumNumberGuardrail(config)
                logger.info(f"Successfully created MinimumNumberGuardrail: {guardrail}")
            else:
                logger.error(f"Unknown guardrail type: {guardrail_type}")
                return None
                
            # Ensure the guardrail was created
            if guardrail is None:
                logger.error(f"Failed to create guardrail of type {guardrail_type} - returned None")
                return None
                
            return guardrail
                
        except Exception as e:
            logger.error(f"Error creating guardrail of type {guardrail_type}: {str(e)}")
            logger.error(traceback.format_exc())
            return None