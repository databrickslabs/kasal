"""
Utility functions for working with prompt templates and JSON parsing.

This module provides utilities for JSON parsing from LLM outputs and
a simple wrapper for backward compatibility with existing code that
uses the get_prompt_template function.
"""

import logging
import json
import re
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

async def get_prompt_template(db: Session, name: str, default_template: str = None) -> Optional[str]:
    """
    Legacy wrapper for TemplateService.get_template_content.
    
    This function is kept for backward compatibility with existing code.
    New code should use TemplateService.get_template_content directly.
    
    Args:
        db: Database session
        name: The name of the prompt template to retrieve
        default_template: A default template to use if the database lookup fails
        
    Returns:
        The template as a string, the default template if provided and the template wasn't found,
        or None if no template was found and no default was provided
    """
    # Import inside function to avoid circular imports
    from src.services.template_service import TemplateService
    return await TemplateService.get_template_content(name, default_template)


def robust_json_parser(text):
    """
    Parse JSON with advanced error recovery for LLM outputs.
    
    Handles common issues in LLM-generated JSON including:
    - JSON embedded in markdown code blocks
    - Extra text before/after JSON
    - Missing quotes around keys
    - Trailing commas
    - Unbalanced braces
    - Incorrectly escaped quotes
    - Truncated or incomplete JSON
    
    Args:
        text: String containing potential JSON
        
    Returns:
        Parsed JSON as Python dict/list or raises ValueError if parsing fails
    """
    if not text or not text.strip():
        raise ValueError("Empty text cannot be parsed as JSON")
    
    # Original text for logging
    original_text = text
    
    # Try direct parsing first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Initial JSON parsing failed, attempting recovery")
    
    # Step 1: Remove markdown code block formatting
    code_block_pattern = re.compile(r'```(?:json)?\s*([\s\S]*?)\s*```')
    matches = code_block_pattern.search(text)
    if matches:
        try:
            text = matches.group(1).strip()
            logger.info("Extracted JSON from code block")
            return json.loads(text)
        except json.JSONDecodeError:
            logger.info("Code block extraction didn't yield valid JSON, continuing...")
    
    # Step 2: Try to extract JSON object or array from text with extra content
    json_pattern = re.compile(r'({[\s\S]*}|\[[\s\S]*\])')
    matches = json_pattern.search(text)
    if matches:
        try:
            extracted_text = matches.group(0)
            logger.info("Extracted JSON object/array from text")
            return json.loads(extracted_text)
        except json.JSONDecodeError:
            logger.info("JSON extraction didn't yield valid JSON, continuing...")
            text = matches.group(0)  # Continue with the extracted text for further fixes
    
    # Step 3: Fix missing quotes around keys
    try:
        fixed_text = re.sub(r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', text)
        logger.info("Attempting to fix missing quotes around keys")
        return json.loads(fixed_text)
    except json.JSONDecodeError:
        logger.info("Fixing quotes didn't yield valid JSON, continuing...")
    
    # Step 4: Handle trailing commas
    try:
        fixed_text = re.sub(r',\s*([\]}])', r'\1', fixed_text)
        logger.info("Attempting to fix trailing commas")
        return json.loads(fixed_text)
    except json.JSONDecodeError:
        logger.info("Fixing trailing commas didn't yield valid JSON, continuing...")
    
    # Step 5: Fix truncated or incomplete field values
    try:
        # Find fields that are missing values or have incomplete values
        truncated_pattern = re.compile(r'("(?:[^"\\]|\\.)*")\s*:\s*(?![\{\}\[\]"0-9a-zA-Z-])')
        fixed_text = truncated_pattern.sub(r'\1: null', fixed_text)
        logger.info("Fixed truncated field values")
        return json.loads(fixed_text)
    except json.JSONDecodeError:
        logger.info("Fixing truncated values didn't yield valid JSON, continuing...")
    
    # Step 6: Fix unbalanced braces and truncated JSON
    try:
        # Count opening and closing braces/brackets
        open_curly = fixed_text.count('{')
        close_curly = fixed_text.count('}')
        open_square = fixed_text.count('[')
        close_square = fixed_text.count(']')
        
        # Add missing closing braces/brackets
        if open_curly > close_curly:
            # For nested objects, properly close all nested structures
            # First handle JSON structure by analyzing the text
            # Find the deepest nested structure that needs completion
            stack = []
            for char in fixed_text:
                if char == '{':
                    stack.append('}')
                elif char == '[':
                    stack.append(']')
                elif char in ']}' and stack and stack[-1] == char:
                    stack.pop()
            
            # Add all necessary closing braces in correct order
            fixed_text += ''.join(reversed(stack))
            logger.info(f"Added balanced closing braces: {''.join(reversed(stack))}")
        elif open_square > close_square:
            # Similar approach for brackets
            fixed_text += ']' * (open_square - close_square)
            logger.info(f"Added {open_square - close_square} closing brackets")
            
        # Try to parse with balanced braces
        return json.loads(fixed_text)
    except json.JSONDecodeError:
        logger.info("Fixing unbalanced braces didn't yield valid JSON, continuing...")
    
    # Step 7: Handle null values for incomplete fields
    try:
        if fixed_text.strip().endswith(':'):
            fixed_text += ' null'
            logger.info("Added null value for incomplete field")
        
        # Fix truncated objects or arrays
        if fixed_text.strip().endswith('{'):
            fixed_text += '}'
            logger.info("Completed truncated object")
        elif fixed_text.strip().endswith('['):
            fixed_text += ']'
            logger.info("Completed truncated array")
            
        return json.loads(fixed_text)
    except json.JSONDecodeError:
        logger.info("Adding null values didn't yield valid JSON")
    
    # Step 8: More aggressive fix - try to handle escaped quotes issues
    try:
        # Try replacing common quote escaping mistakes
        fixed_text = fixed_text.replace('\\"', '"').replace('\"', '"')
        fixed_text = re.sub(r'(?<!\\)"(?=\s*[,}\]])', '\\"', fixed_text)
        logger.info("Attempting to fix quote escaping issues")
        return json.loads(fixed_text)
    except json.JSONDecodeError:
        logger.info("Fixing quote escaping didn't yield valid JSON")
    
    # Log failure details for debugging
    logger.error(f"Failed to parse JSON after all recovery attempts: {original_text[:100]}...")
    raise ValueError("Could not parse response as JSON after multiple recovery attempts") 