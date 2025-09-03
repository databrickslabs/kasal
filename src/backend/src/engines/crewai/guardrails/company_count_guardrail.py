"""
Guardrail to validate that a task output contains at least a certain number of company names.
"""
import logging
import re
import json
import os
from typing import Dict, Any, List, Optional, Union
from crewai.tasks.task_output import TaskOutput
import traceback

from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail
from src.core.logger import LoggerManager

# Get the logger manager instance
logger_manager = LoggerManager.get_instance()
# Initialize if not already initialized
if not logger_manager._initialized:
    logger_manager.initialize()

# Get the guardrails logger from the centralized logging system
logger = logger_manager.guardrails

class CompanyCountGuardrail(BaseGuardrail):
    """
    Guardrail that validates a minimum number of company names in the output.
    
    This guardrail counts the number of unique company names in the output text
    and asks the agent to try again if the count is below the minimum.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the CompanyCountGuardrail.
        
        Args:
            config: Configuration dictionary containing:
                - min_companies: Minimum number of companies required in the output
        """
        super().__init__(config)
        self.min_companies = config.get("min_companies", 50)  # Default to 50 companies
        logger.info(f"Initialized CompanyCountGuardrail with min_companies={self.min_companies}")
        logger.info(f"Full guardrail configuration: {config}")
        logger.info(f"Log directory: {logger_manager._log_dir}")
        logger.info(f"Logger handlers: {logger.handlers}")
    
    def validate(self, output: Union[str, TaskOutput, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate that the output contains at least the minimum number of company names.
        
        Args:
            output: The task output to validate (can be string, TaskOutput, or dict)
            
        Returns:
            Dict containing validation result and feedback:
                - valid (bool): Whether the output is valid
                - feedback (str): Feedback message if invalid
        """
        logger.info("=" * 80)
        logger.info("STARTING GUARDRAIL VALIDATION")
        logger.info("=" * 80)
        
        # Log the current state of the logger
        logger.info(f"Logger level: {logger.level}")
        logger.info(f"Logger handlers: {logger.handlers}")
        logger.info(f"Logger effective level: {logger.getEffectiveLevel()}")
        
        logger.info(f"Input type: {type(output)}")
        
        # Convert output to string if it's not already
        output_text = self._get_output_text(output)
        if not output_text:
            logger.info("No text content found in output")
            return {
                "valid": False,
                "feedback": "No content found in the output. Please provide a detailed list of at least " +
                           f"{self.min_companies} Swiss companies based on the provided UIDs, including " +
                           "legal name, registered address, and activity status for each company."
            }
        
        logger.info(f"Extracted text content length: {len(output_text)} characters")
        
        # Extract potential company names from the output
        companies = self._extract_companies(output_text)
        company_count = len(companies)
        
        logger.info(f"Found {company_count} companies in output")
        logger.info(f"Companies found: {companies}")
        
        if company_count >= self.min_companies:
            logger.info("Validation passed: Sufficient company count")
            return {
                "valid": True,
                "feedback": ""
            }
        else:
            logger.info(f"Validation failed: Found only {company_count} companies, minimum required is {self.min_companies}")
            
            # Provide a more helpful error message explaining exactly what to do
            feedback = (
                f"Your response only includes {company_count} companies, but at least {self.min_companies} are required. " +
                "Please try again and provide more company names. " +
                "\n\nI need you to create a detailed list with at least 50 Swiss companies based on the provided UIDs. " +
                "For each company, provide:\n" +
                "1. Full legal company name\n" +
                "2. Complete registered address\n" +
                "3. Current activity status (active/inactive)\n\n" +
                "Even if you don't have direct access to official registries, please make your best effort to " +
                "compile information on these companies from available knowledge sources. " +
                "Format each company entry clearly and ensure you include at least 50 different companies."
            )
            
            return {
                "valid": False,
                "feedback": feedback
            }
    
    def _get_output_text(self, output: Union[str, TaskOutput, Dict[str, Any]]) -> Optional[str]:
        """Extract text content from various output types."""
        logger.info(f"Attempting to extract text from output of type: {type(output)}")
        
        if isinstance(output, str):
            logger.info("Output is already a string")
            return output
        elif isinstance(output, TaskOutput):
            logger.info("Output is a TaskOutput object")
            # Try all possible attributes that might contain the output
            possible_attrs = ['content', 'raw_output', 'output', 'text', 'result', 'response']
            for attr in possible_attrs:
                if hasattr(output, attr):
                    value = getattr(output, attr)
                    logger.info(f"Found {attr} attribute: {value}")
                    if value and isinstance(value, str):
                        return value
            logger.info("No suitable attribute found in TaskOutput")
            logger.info(f"Available attributes: {dir(output)}")
            # Try to get any string representation
            try:
                str_output = str(output)
                logger.info(f"String representation: {str_output}")
                return str_output
            except Exception as e:
                logger.info(f"Error getting string representation: {str(e)}")
        elif isinstance(output, dict):
            logger.info("Output is a dictionary")
            # Try all possible keys that might contain the output
            possible_keys = ['content', 'raw_output', 'output', 'text', 'result', 'response']
            for key in possible_keys:
                if key in output:
                    value = output[key]
                    logger.info(f"Found {key} key: {value}")
                    if value and isinstance(value, str):
                        return value
            logger.info("No suitable key found in dictionary")
            logger.info(f"Available keys: {list(output.keys())}")
            # Try to convert the entire dict to string
            try:
                dict_str = json.dumps(output, indent=2)
                logger.info(f"Dictionary as string: {dict_str}")
                return dict_str
            except Exception as e:
                logger.info(f"Error converting dictionary to string: {str(e)}")
        else:
            logger.info(f"Unsupported output type: {type(output)}")
            # Try to get string representation
            try:
                str_output = str(output)
                logger.info(f"String representation: {str_output}")
                return str_output
            except Exception as e:
                logger.info(f"Error getting string representation: {str(e)}")
        return None
    
    def _extract_companies(self, text: str) -> List[str]:
        """Extract potential company names from text."""
        logger.info("Starting company extraction process")
        
        # Define more comprehensive regex patterns for company name detection
        company_patterns = [
            # Standard company names with common suffixes
            r'([A-Z][a-zA-Z0-9\s&\-\'\.,]+ (?:Inc|Corp|Corporation|LLC|Ltd|Limited|Co|Company|Group|Holdings|Industries|Technologies|Partners|Solutions|International|Systems|Services))\.?',
            
            # Industry-specific company names
            r'([A-Z][a-zA-Z0-9\s&\-\'\.,]+ (?:Bank|Insurance|Financial|Capital|Investments|Pharmaceuticals|Energy|Communications|Media|Healthcare|Automotive|AG|GmbH|SA))\.?',
            
            # Swiss specific company formats
            r'([A-Z][a-zA-Z0-9\s&\-\'\.,]+ (?:AG|GmbH|SA|SARL|Sàrl))\.?',
            
            # Companies in quotes
            r'["\'"]([A-Z][^"\'\n]{2,})["\'"]',
            
            # Numbered or bulleted list items that might be companies
            r'(?:\d+\.|[-*•])?\s+([A-Z][A-Za-z0-9\s&\-\'\.]{2,})(?=\n|$|\s\(|\s-)',
            
            # Capitalized words or phrases (common company format)
            r'\b([A-Z][a-zA-Z0-9]+(?: [A-Z][a-zA-Z0-9]+){1,5})\b',
            
            # Companies followed by descriptions
            r'([A-Z][a-zA-Z0-9\s&\-\'\.]{2,}?)(?=:|\s-\s|\s–\s|\()',
            
            # Companies with Swiss UID numbers (CHE format)
            r'([A-Z][a-zA-Z0-9\s&\-\'\.]{2,})(?=.*CHE-\d{3}\.\d{3}\.\d{3}|.*CHE\d{9})',
        ]
        
        # Hard-coded common Swiss company identifiers
        swiss_company_identifiers = [
            "Nestlé", "Novartis", "Roche", "UBS", "Credit Suisse", "ABB", "Zurich Insurance", 
            "Swiss Re", "Glencore", "Swatch Group", "Adecco", "Richemont", "Givaudan", 
            "Holcim", "Syngenta", "Swisscom", "Kuehne+Nagel", "Julius Baer", "SGS", 
            "Lonza Group", "Schindler", "Barry Callebaut", "Swiss Life", "Geberit"
        ]
        
        # Set to store unique companies
        companies = set()
        
        # Extract companies based on regex patterns
        for pattern in company_patterns:
            matches = re.findall(pattern, text)
            if matches:
                # Clean up the matches and add to results
                for match in matches:
                    # Handle if match is a tuple (captured groups)
                    if isinstance(match, tuple):
                        match = match[0]  # Take the first captured group
                    
                    # Clean up the company name
                    company = match.strip()
                    
                    # Skip empty or very short names
                    if not company or len(company) < 3:
                        continue
                    
                    # Skip common non-company words that might be capitalized
                    common_words = ["The", "This", "That", "These", "Those", "Their", "There", "They", "Company", "Corporation"]
                    if company in common_words:
                        continue
                    
                    companies.add(company)
        
        # Add any Swiss company identifiers found in the text
        for company in swiss_company_identifiers:
            if company in text:
                companies.add(company)
        
        # Look for company names following a UID pattern
        uid_pattern = r'(CHE-\d{3}\.\d{3}\.\d{3}|CHE\d{9})[:\s]*([A-Z][^\n\.,]{2,})'
        uid_matches = re.findall(uid_pattern, text)
        for uid, name in uid_matches:
            company = name.strip()
            if company and len(company) > 3:
                companies.add(company)
        
        logger.info(f"Found companies: {companies}")
        return list(companies)