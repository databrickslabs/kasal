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


def _repair_json_structure(s):
    """Single-pass, string-aware structural repair of brace/bracket nesting.

    Drops spurious or mismatched closing tokens (e.g. a model emitting an extra
    ``}`` inside an array: ``[{...}},{...}]``) and appends any closers still
    missing at the end. Also strips trailing commas before closers. Returns a
    best-effort repaired string; the caller still validates with json.loads.
    """
    out = []
    stack = []
    in_str = False
    esc = False
    for ch in s:
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            elif ch == '\\':
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            out.append(ch)
        elif ch in '{[':
            stack.append(ch)
            out.append(ch)
        elif ch == '}':
            if stack and stack[-1] == '{':
                stack.pop()
                out.append(ch)
            # else: spurious '}', drop it
        elif ch == ']':
            if stack and stack[-1] == '[':
                stack.pop()
                out.append(ch)
            # else: spurious ']', drop it
        else:
            out.append(ch)
    while stack:
        out.append('}' if stack.pop() == '{' else ']')
    repaired = ''.join(out)
    return re.sub(r',\s*([\]}])', r'\1', repaired)


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

    # Step 0: Strip reasoning / thinking blocks emitted by reasoning models
    # (Qwen3, DeepSeek-R1-style, gpt-oss). These wrap or precede the JSON and
    # break every downstream parser. Removing them is a no-op for clean output.
    text = re.sub(r'<(think|thinking|reasoning|analysis)>[\s\S]*?</\1>', '', text, flags=re.IGNORECASE)
    # Truncated thinking: a block opened and the JSON only begins after its close.
    if re.search(r'</(?:think|thinking|reasoning|analysis)>', text, flags=re.IGNORECASE):
        text = re.sub(r'^[\s\S]*?</(?:think|thinking|reasoning|analysis)>', '', text, flags=re.IGNORECASE)
    text = text.strip()

    # Try direct parsing first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.info("Initial JSON parsing failed, attempting recovery")

    # Step 0b: Extract the first COMPLETE top-level JSON value via balanced-brace
    # scanning (string/escape aware). More reliable than the greedy regex below
    # when the model appends trailing prose after a valid object.
    _start = text.find('{')
    _alt = text.find('[')
    if _start == -1 or (0 <= _alt < _start):
        _start = _alt
    if _start != -1:
        _depth = 0
        _in_str = False
        _esc = False
        for _k in range(_start, len(text)):
            _c = text[_k]
            if _esc:
                _esc = False
                continue
            if _c == '\\':
                _esc = True
                continue
            if _c == '"':
                _in_str = not _in_str
                continue
            if _in_str:
                continue
            if _c in '{[':
                _depth += 1
            elif _c in '}]':
                _depth -= 1
                if _depth == 0:
                    try:
                        return json.loads(text[_start:_k + 1])
                    except json.JSONDecodeError:
                        logger.info("Balanced-brace extraction didn't yield valid JSON, continuing...")
                    break

    # Step 0c: Structural repair — drop spurious/mismatched closing braces and
    # append any missing ones (string-aware). Fixes the common reasoning-model
    # error of an extra '}' or ']' inside a value, e.g. [{...}},{...}].
    try:
        return json.loads(_repair_json_structure(text))
    except (json.JSONDecodeError, Exception):
        logger.info("Structural brace repair didn't yield valid JSON, continuing...")
    
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

    # Step 1b: Handle truncated code blocks (opening ``` but no closing ```)
    truncated_block_pattern = re.compile(r'```(?:json)?\s*([\s\S]+)')
    truncated_matches = truncated_block_pattern.search(text)
    if truncated_matches:
        try:
            text = truncated_matches.group(1).strip()
            logger.info("Extracted JSON from truncated code block (no closing ```)")
            return json.loads(text)
        except json.JSONDecodeError:
            logger.info("Truncated code block extraction didn't yield valid JSON, continuing...")
    
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

    # Step 9: Aggressive truncation recovery - close open strings and balance braces
    try:
        # Work from the original extracted text (before quote mangling)
        truncated = text.strip()
        # Remove trailing incomplete content: find the last complete key-value pair
        # by looking for the last comma or opening brace that precedes valid content

        # Check if we're inside an open string (odd number of unescaped quotes)
        in_string = False
        last_good_pos = 0
        i = 0
        while i < len(truncated):
            ch = truncated[i]
            if ch == '\\' and in_string:
                i += 2  # skip escaped character
                continue
            if ch == '"':
                in_string = not in_string
                if not in_string:
                    last_good_pos = i
            elif not in_string and ch in ',}]':
                last_good_pos = i
            i += 1

        if in_string:
            # Close the open string, then truncate any incomplete key-value
            truncated = truncated[:i] + '"'
            logger.info("Closed open string in truncated JSON")

        # Drop a dangling key with no value (truncated right after the colon),
        # e.g. ...,"role": — and any trailing separator, so the balance step
        # below produces valid JSON instead of `"role":}`.
        truncated = re.sub(r'[,\s]*"(?:[^"\\]|\\.)*"\s*:\s*$', '', truncated)
        truncated = truncated.rstrip().rstrip(',:').rstrip()

        # Now balance braces/brackets
        stack = []
        in_str = False
        j = 0
        while j < len(truncated):
            ch = truncated[j]
            if ch == '\\' and in_str:
                j += 2
                continue
            if ch == '"':
                in_str = not in_str
            elif not in_str:
                if ch == '{':
                    stack.append('}')
                elif ch == '[':
                    stack.append(']')
                elif ch in ']}' and stack and stack[-1] == ch:
                    stack.pop()
            j += 1

        if stack:
            truncated += ''.join(reversed(stack))
            logger.info(f"Balanced {len(stack)} unclosed braces/brackets in truncated JSON")

        # Remove trailing commas before closing braces
        truncated = re.sub(r',\s*([\]}])', r'\1', truncated)
        return json.loads(truncated)
    except (json.JSONDecodeError, Exception):
        logger.info("Aggressive truncation recovery didn't yield valid JSON")

    # Log failure details for debugging. Log the FULL content (capped) and its
    # length — the previous 100-char preview hid the real failure point (e.g.
    # mid-array truncation from an exhausted max_tokens budget).
    logger.error(
        "Failed to parse JSON after all recovery attempts "
        f"(len={len(original_text)}): {original_text[:4000]!r}"
    )
    raise ValueError("Could not parse response as JSON after multiple recovery attempts") 