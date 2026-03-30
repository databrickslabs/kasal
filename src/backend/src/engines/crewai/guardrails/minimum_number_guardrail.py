"""
Guardrail to validate that a task output contains a number larger than a specified minimum.
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

class MinimumNumberGuardrail(BaseGuardrail):
    """
    Guardrail that validates a number in the output is greater than a specified minimum.
    
    This guardrail checks if the 'total_count' or another specified numeric field
    in the output exceeds the minimum threshold.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the MinimumNumberGuardrail.
        
        Args:
            config: Configuration dictionary containing:
                - min_value: Minimum number threshold
                - field_name: (Optional) Name of the field to check 
                  (defaults to 'total_count')
                - message: (Optional) Custom message for failed validation
        """
        super().__init__(config)
        
        # Parse config if it's a string
        if isinstance(config, str):
            try:
                config_dict = json.loads(config)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse guardrail config: {config}")
                config_dict = {}
        else:
            config_dict = config
            
        self.min_value = config_dict.get("min_value", 1)  # Default to 1
        self.field_name = config_dict.get("field_name", "total_count")  # Default to total_count
        self.message = config_dict.get("message", f"The output should contain a '{self.field_name}' value greater than {self.min_value}")
        logger.info(f"Initialized MinimumNumberGuardrail with min_value={self.min_value}, field_name={self.field_name}")
        logger.info(f"Full guardrail configuration: {config_dict}")
    
    def validate(self, output: Union[str, TaskOutput, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate that the output contains a number greater than the minimum value.
        
        Args:
            output: The task output to validate (can be string, TaskOutput, or dict)
            
        Returns:
            Dict containing validation result and feedback:
                - valid (bool): Whether the output is valid
                - feedback (str): Feedback message if invalid
        """
        logger.info("=" * 80)
        logger.info("STARTING MINIMUM NUMBER GUARDRAIL VALIDATION")
        logger.info("=" * 80)
        logger.info(f"Input type: {type(output)}")
        
        # Special handling for Linkup Search Tool output which might be truncated in string representation
        if isinstance(output, TaskOutput):
            logger.info("Detected TaskOutput object, checking for special Linkup Search Tool format")
            # Check if this is a result from Linkup Search Tool
            if hasattr(output, 'results') and hasattr(output, 'source'):
                source = getattr(output, 'source', '')
                if isinstance(source, str) and ('linkup' in source.lower() or 'search tool' in source.lower()):
                    logger.info("Detected Linkup Search Tool output")
                    
                    # Check for results to count
                    results = getattr(output, 'results', [])
                    if isinstance(results, list):
                        count = len(results)
                        logger.info(f"Counted {count} results in Linkup Search Tool output")
                        
                        # Validate this count against the minimum
                        if count > self.min_value:
                            logger.info(f"Validation passed: {count} > {self.min_value}")
                            return {
                                "valid": True,
                                "feedback": ""
                            }
                        else:
                            logger.info(f"Validation failed: {count} ≤ {self.min_value}")
                            return {
                                "valid": False,
                                "feedback": f"Found {count} results, but at least {self.min_value+1} are required. Please expand your search or provide more results."
                            }
        
        # Extract the value to check using standard methods
        try:
            value = self._extract_value(output)
            logger.info(f"Extracted value: {value} for field: {self.field_name}")
            
            if value is None:
                # Special fallback for TaskOutput with results attribute
                if isinstance(output, TaskOutput) and hasattr(output, 'results'):
                    results = getattr(output, 'results')
                    if isinstance(results, list):
                        count = len(results)
                        logger.info(f"Fallback to counting results, found {count} items")
                        value = count
                
                # If still no value found
                if value is None:
                    logger.info(f"No {self.field_name} found in output")
                    return {
                        "valid": False,
                        "feedback": f"No {self.field_name} found in the output. Please include a {self.field_name} value greater than {self.min_value}."
                    }
            
            # Try to convert to a number if it's not already
            if not isinstance(value, (int, float)):
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    logger.info(f"Value '{value}' is not a valid number")
                    return {
                        "valid": False,
                        "feedback": f"The {self.field_name} value '{value}' is not a valid number. Please provide a numeric value greater than {self.min_value}."
                    }
            
            # Check if the value exceeds the minimum
            if value > self.min_value:
                logger.info(f"Validation passed: {value} > {self.min_value}")
                return {
                    "valid": True,
                    "feedback": ""
                }
            else:
                logger.info(f"Validation failed: {value} ≤ {self.min_value}")
                return {
                    "valid": False,
                    "feedback": self.message
                }
        except Exception as e:
            logger.error(f"Error during validation: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                "valid": False,
                "feedback": f"An error occurred during validation: {str(e)}. Please ensure the output includes a {self.field_name} field with a numeric value greater than {self.min_value}."
            }
    
    def _extract_value(self, output: Union[str, TaskOutput, Dict[str, Any]]) -> Optional[Union[int, float, str]]:
        """
        Extract the value to check from various output types.
        
        Args:
            output: The task output from which to extract the value
            
        Returns:
            The extracted value, or None if not found
        """
        # If output is a dictionary (or can be parsed as one)
        if isinstance(output, dict):
            logger.info("Output is a dictionary")
            return self._get_value_from_dict(output)
        
        # If output is a TaskOutput object
        elif isinstance(output, TaskOutput):
            logger.info("Output is a TaskOutput object")
            logger.info(f"TaskOutput dir: {dir(output)}")
            
            # Log available attributes for debugging
            for attr in ['raw_output', 'content', 'output', 'result', 'response', 'results', 'total_count']:
                if hasattr(output, attr):
                    value = getattr(output, attr)
                    logger.info(f"TaskOutput.{attr}: {type(value)} = {str(value)[:100]}{'...' if len(str(value)) > 100 else ''}")
            
            # Special handling for Linkup Search Tool output format
            if hasattr(output, 'results') and hasattr(output, 'total_count'):
                logger.info("Found 'results' and 'total_count' attributes in TaskOutput")
                try:
                    # Try to get total_count directly if it exists
                    total_count = getattr(output, 'total_count')
                    if total_count is not None:
                        logger.info(f"Using direct total_count attribute: {total_count}")
                        # If it's a string (possibly truncated), extract the number
                        if isinstance(total_count, str):
                            match = re.search(r'(\d+)', total_count)
                            if match:
                                return int(match.group(1))
                        return total_count
                    
                    # If total_count is None but results exists, count results
                    results = getattr(output, 'results')
                    if results and isinstance(results, list):
                        count = len(results)
                        logger.info(f"Counting results list, found {count} items")
                        return count
                except Exception as e:
                    logger.error(f"Error accessing total_count or results: {e}")
            
            # Try to access raw_output attribute which might be a dict
            if hasattr(output, 'raw_output'):
                raw_output = getattr(output, 'raw_output')
                if isinstance(raw_output, dict):
                    logger.info("Processing raw_output as dict")
                    return self._get_value_from_dict(raw_output)
            
            # If there's a content attribute, try to parse it as JSON
            if hasattr(output, 'content'):
                content = getattr(output, 'content')
                if isinstance(content, str):
                    try:
                        json_content = json.loads(content)
                        if isinstance(json_content, dict):
                            logger.info("Parsed content JSON as dict")
                            return self._get_value_from_dict(json_content)
                    except json.JSONDecodeError:
                        # Try to extract from the content string
                        logger.info("Content is not valid JSON, trying regex extraction")
                        return self._extract_value_from_text(content)
            
            # Try converting TaskOutput to string and search in that
            try:
                output_str = str(output)
                logger.info(f"Converting TaskOutput to string (length: {len(output_str)})")
                logger.info(f"String preview: {output_str[:200]}...")
                
                # Look for patterns indicating results and counts
                if "results=" in output_str or "total_count=" in output_str:
                    logger.info("Found results or total_count in string representation")
                    
                    # Try to extract total_count
                    total_count_match = re.search(r'total_count=(\d+)', output_str)
                    if total_count_match:
                        logger.info(f"Found total_count in string: {total_count_match.group(1)}")
                        return int(total_count_match.group(1))
                    
                    # Try to count results items
                    results_match = re.findall(r"'[^']*'", output_str)
                    if results_match and len(results_match) > 0:
                        logger.info(f"Counted {len(results_match)} potential results items in string")
                        return len(results_match)
                
                # Fall back to general text extraction
                return self._extract_value_from_text(output_str)
            except Exception as e:
                logger.error(f"Error processing TaskOutput as string: {e}")
            
            # Try all possible attributes that might contain the output
            possible_attrs = ['output', 'result', 'response']
            for attr in possible_attrs:
                if hasattr(output, attr):
                    value = getattr(output, attr)
                    if isinstance(value, dict):
                        return self._get_value_from_dict(value)
                    elif isinstance(value, str):
                        try:
                            json_content = json.loads(value)
                            if isinstance(json_content, dict):
                                return self._get_value_from_dict(json_content)
                        except json.JSONDecodeError:
                            pass
        
        # If output is a string, try to parse it as JSON
        elif isinstance(output, str):
            logger.info("Output is a string")
            try:
                json_content = json.loads(output)
                if isinstance(json_content, dict):
                    return self._get_value_from_dict(json_content)
            except json.JSONDecodeError:
                # If not valid JSON, try to extract numbers with regex
                logger.info("String is not valid JSON, attempting to extract numbers with regex")
                return self._extract_value_from_text(output)
        
        # For other types, try getting string representation and parse
        else:
            logger.info(f"Unsupported output type: {type(output)}")
            try:
                str_output = str(output)
                try:
                    json_content = json.loads(str_output)
                    if isinstance(json_content, dict):
                        return self._get_value_from_dict(json_content)
                except json.JSONDecodeError:
                    return self._extract_value_from_text(str_output)
            except Exception as e:
                logger.error(f"Error getting string representation: {str(e)}")
        
        return None
    
    def _get_value_from_dict(self, data: Dict[str, Any]) -> Optional[Union[int, float, str]]:
        """Extract the value from a dictionary using the field name."""
        logger.info(f"Extracting value from dict with keys: {list(data.keys())}")
        
        # Direct lookup in root dictionary
        if self.field_name in data:
            value = data[self.field_name]
            logger.info(f"Found direct match for {self.field_name} = {value}")
            return value
        
        # Check if the format matches a MultiURLToolOutput structure
        if 'total_count' in data and self.field_name == 'total_count':
            value = data['total_count']
            logger.info(f"Found total_count in MultiURLToolOutput structure = {value}")
            return value
        
        # Special handling for 'results' field if looking for count-related values
        if 'results' in data and isinstance(data['results'], list) and self.field_name.lower() in ['count', 'total_count', 'results_count', 'length', 'size']:
            count = len(data['results'])
            logger.info(f"Counting results array, found {count} items")
            return count
            
        # Check in metadata if it exists
        if 'metadata' in data and isinstance(data['metadata'], dict):
            metadata = data['metadata']
            logger.info(f"Checking metadata with keys: {list(metadata.keys())}")
            
            if self.field_name in metadata:
                value = metadata[self.field_name]
                logger.info(f"Found {self.field_name} in metadata = {value}")
                return value
                
            # Look for count-related fields in metadata
            if self.field_name.lower() in ['count', 'total_count', 'items', 'results']:
                for key in ['count', 'total_count', 'items_count', 'results_count', 'size', 'length']:
                    if key in metadata:
                        value = metadata[key]
                        logger.info(f"Found related field {key} in metadata = {value}")
                        return value
        
        # Look for special cases where the value might be nested or named differently
        if self.field_name == 'total_count' and 'count' in data:
            value = data['count']
            logger.info(f"Found 'count' as alternative to 'total_count' = {value}")
            return value
            
        if self.field_name == 'count' and 'total_count' in data:
            value = data['total_count']
            logger.info(f"Found 'total_count' as alternative to 'count' = {value}")
            return value
        
        # Recursively check nested dictionaries (one level deep)
        for key, value in data.items():
            if isinstance(value, dict):
                if self.field_name in value:
                    nested_value = value[self.field_name]
                    logger.info(f"Found {self.field_name} in nested dict under '{key}' = {nested_value}")
                    return nested_value
                    
                # Look for count-related fields in nested dicts
                if self.field_name.lower() in ['count', 'total_count', 'items', 'results']:
                    for nested_key in ['count', 'total_count', 'items_count', 'results_count', 'size', 'length']:
                        if nested_key in value:
                            nested_value = value[nested_key]
                            logger.info(f"Found related field {nested_key} in nested dict under '{key}' = {nested_value}")
                            return nested_value
        
        logger.info(f"No value found for {self.field_name} in dictionary")
        return None
    
    def _extract_value_from_text(self, text: str) -> Optional[Union[int, float]]:
        """
        Extract numeric values from text using regex patterns.
        Prioritizes finding numbers associated with field name.
        """
        if not text:
            logger.warning("Empty text provided to _extract_value_from_text")
            return None
            
        logger.info(f"Extracting value from text of length {len(text)}")
        logger.info(f"Looking for field: {self.field_name}")
        
        # Try to find a pattern like "total_count: 42" or similar with exact field name
        field_pattern = rf'["\']?{re.escape(self.field_name)}["\']?\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)'
        field_matches = re.search(field_pattern, text, re.IGNORECASE)
        if field_matches:
            logger.info(f"Found exact field match: {field_matches.group(0)}")
            try:
                value = field_matches.group(1)
                return int(value) if value.isdigit() else float(value)
            except (ValueError, IndexError) as e:
                logger.warning(f"Error converting field match to number: {e}")
        
        # Check for JSON-like structures that might not parse as complete JSON
        json_pattern = rf'["\']?{re.escape(self.field_name)}["\']?\s*:\s*([0-9]+(?:\.[0-9]+)?)'
        json_matches = re.search(json_pattern, text)
        if json_matches:
            logger.info(f"Found JSON-like match: {json_matches.group(0)}")
            try:
                value = json_matches.group(1)
                return int(value) if value.isdigit() else float(value)
            except (ValueError, IndexError) as e:
                logger.warning(f"Error converting JSON-like match to number: {e}")
        
        # If the field is count-related, look for items/results count statements
        if any(count_term in self.field_name.lower() for count_term in ['count', 'total', 'items', 'results', 'matches', 'size', 'length']):
            logger.info("Field name contains count-related terms, looking for count statements")
            count_patterns = [
                r'found\s+([0-9]+(?:\.[0-9]+)?)\s+(?:items?|results?|matches?)',
                r'(?:total|found)(?:\s+count)?:\s*([0-9]+(?:\.[0-9]+)?)',
                r'([0-9]+(?:\.[0-9]+)?)\s+(?:items?|results?|matches?)\s+(?:found|returned)',
                r'result(?:s)?\s+count:\s*([0-9]+(?:\.[0-9]+)?)',
                r'([0-9]+(?:\.[0-9]+)?)\s+(?:total)',
                r'(?:contains|has|with)\s+([0-9]+(?:\.[0-9]+)?)\s+(?:items?|results?|entries|records)',
                r'(?:size|length|count)(?:\s+is)?(?:\s+equal\s+to)?:\s*([0-9]+(?:\.[0-9]+)?)'
            ]
            
            for pattern in count_patterns:
                matches = re.search(pattern, text, re.IGNORECASE)
                if matches:
                    logger.info(f"Found count statement match: {matches.group(0)}")
                    try:
                        value = matches.group(1)
                        return int(value) if value.isdigit() else float(value)
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error converting count match to number: {e}")
        
        # Look for the field name in proximity to numbers
        proximity_pattern = rf'(?:{re.escape(self.field_name)}|{"|".join(self.field_name.split("_"))})[^0-9]*([0-9]+(?:\.[0-9]+)?)'
        proximity_matches = re.search(proximity_pattern, text, re.IGNORECASE)
        if proximity_matches:
            logger.info(f"Found proximity match: {proximity_matches.group(0)}")
            try:
                value = proximity_matches.group(1)
                return int(value) if value.isdigit() else float(value)
            except (ValueError, IndexError) as e:
                logger.warning(f"Error converting proximity match to number: {e}")
                
        # If nothing else works, try to extract any number
        # This is a fallback and less reliable
        logger.info("No specific pattern matches, looking for any numbers in text")
        number_matches = re.findall(r'\b([0-9]+(?:\.[0-9]+)?)\b', text)
        if number_matches:
            logger.info(f"Found {len(number_matches)} numbers in text")
            
            try:
                # Filter out small numbers that are likely not relevant
                significant_numbers = [float(num) for num in number_matches if float(num) > 1]
                if significant_numbers:
                    # Return the largest significant number found
                    max_value = max(significant_numbers)
                    logger.info(f"Returning largest significant number: {max_value}")
                    return max_value
                else:
                    # If no significant numbers, return the largest of any number
                    max_value = max(float(num) for num in number_matches)
                    logger.info(f"Returning largest number: {max_value}")
                    return max_value
            except (ValueError, TypeError) as e:
                logger.warning(f"Error processing number matches: {e}")
        
        logger.warning(f"No suitable numeric value found in text for field: {self.field_name}")
        return None 