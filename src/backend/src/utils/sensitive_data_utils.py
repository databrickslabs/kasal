"""
Sensitive data utilities module.

This module provides utilities for handling sensitive data in tool configurations,
including encryption, decryption, and masking for logs and API responses.
"""

import logging
import re
from typing import Any, Dict, Match, Optional, Set

from src.utils.encryption_utils import EncryptionUtils

# Initialize logger
logger = logging.getLogger(__name__)

# Sensitive key patterns - any key containing these substrings will be treated as sensitive
SENSITIVE_KEY_PATTERNS: Set[str] = {
    'secret',
    'password',
    'token',
    'api_key',
    'apikey',
    'credential',
    'private_key',
    'privatekey',
    'auth_token',
    'access_token',
    'refresh_token',
    'bearer',
}

# Exact key matches that should always be treated as sensitive
SENSITIVE_EXACT_KEYS: Set[str] = {
    'powerbi_client_secret',
    'client_secret',
    'databricks_token',
    'openai_api_key',
    'anthropic_api_key',
}

# Prefix used to identify encrypted values
ENCRYPTED_PREFIX = "ENC:"

# Redacted placeholder for masked values
REDACTED_PLACEHOLDER = "***REDACTED***"


def is_sensitive_key(key: str) -> bool:
    """
    Check if a key name indicates sensitive data.

    Args:
        key: The key name to check

    Returns:
        True if the key appears to contain sensitive data
    """
    key_lower = key.lower()

    # Check exact matches first
    if key_lower in SENSITIVE_EXACT_KEYS:
        return True

    # Check pattern matches
    for pattern in SENSITIVE_KEY_PATTERNS:
        if pattern in key_lower:
            return True

    return False


def is_encrypted(value: str) -> bool:
    """
    Check if a value is already encrypted (has our encryption prefix).

    Args:
        value: The value to check

    Returns:
        True if the value appears to be encrypted
    """
    if not isinstance(value, str):
        return False
    return value.startswith(ENCRYPTED_PREFIX)


def encrypt_value(value: str) -> str:
    """
    Encrypt a sensitive value and add the encryption prefix.

    Args:
        value: The plain text value to encrypt

    Returns:
        The encrypted value with prefix
    """
    if not value or is_encrypted(value):
        return value

    try:
        encrypted = EncryptionUtils.encrypt_value(value)
        return f"{ENCRYPTED_PREFIX}{encrypted}"
    except Exception as e:
        logger.error(f"Failed to encrypt sensitive value: {e}")
        raise


def decrypt_value(encrypted_value: str) -> str:
    """
    Decrypt an encrypted value (removes the encryption prefix first).

    Args:
        encrypted_value: The encrypted value with prefix

    Returns:
        The decrypted plain text value
    """
    if not encrypted_value:
        return encrypted_value

    if not is_encrypted(encrypted_value):
        # Value is not encrypted, return as-is (for backward compatibility)
        return encrypted_value

    try:
        # Remove the prefix before decryption
        encrypted_data = encrypted_value[len(ENCRYPTED_PREFIX):]
        return EncryptionUtils.decrypt_value(encrypted_data)
    except Exception as e:
        logger.error(f"Failed to decrypt sensitive value: {e}")
        return ""


def encrypt_sensitive_fields(data: Dict[str, Any], recursive: bool = True) -> Dict[str, Any]:
    """
    Encrypt sensitive fields in a dictionary (e.g., tool_configs).

    Args:
        data: The dictionary containing potentially sensitive data
        recursive: Whether to recursively process nested dictionaries

    Returns:
        A new dictionary with sensitive fields encrypted
    """
    if not data or not isinstance(data, dict):
        return data

    result = {}

    for key, value in data.items():
        if isinstance(value, dict) and recursive:
            # Recursively process nested dictionaries
            result[key] = encrypt_sensitive_fields(value, recursive=True)
        elif isinstance(value, str) and is_sensitive_key(key):
            # Encrypt sensitive string values
            if value and not is_encrypted(value):
                try:
                    result[key] = encrypt_value(value)
                    logger.debug(f"Encrypted sensitive field: {key}")
                except Exception as e:
                    logger.error(f"Failed to encrypt field {key}: {e}")
                    result[key] = value
            else:
                result[key] = value
        else:
            result[key] = value

    return result


def decrypt_sensitive_fields(data: Dict[str, Any], recursive: bool = True) -> Dict[str, Any]:
    """
    Decrypt sensitive fields in a dictionary (e.g., tool_configs).

    Args:
        data: The dictionary containing potentially encrypted data
        recursive: Whether to recursively process nested dictionaries

    Returns:
        A new dictionary with sensitive fields decrypted
    """
    if not data or not isinstance(data, dict):
        return data

    result = {}

    for key, value in data.items():
        if isinstance(value, dict) and recursive:
            # Recursively process nested dictionaries
            result[key] = decrypt_sensitive_fields(value, recursive=True)
        elif isinstance(value, str) and is_encrypted(value):
            # Decrypt encrypted values
            try:
                result[key] = decrypt_value(value)
                logger.debug(f"Decrypted sensitive field: {key}")
            except Exception as e:
                logger.error(f"Failed to decrypt field {key}: {e}")
                result[key] = ""
        else:
            result[key] = value

    return result


def mask_sensitive_fields(data: Dict[str, Any], recursive: bool = True) -> Dict[str, Any]:
    """
    Mask sensitive fields in a dictionary for logging or API responses.

    Args:
        data: The dictionary containing potentially sensitive data
        recursive: Whether to recursively process nested dictionaries

    Returns:
        A new dictionary with sensitive fields masked (redacted)
    """
    if not data or not isinstance(data, dict):
        return data

    result = {}

    for key, value in data.items():
        if isinstance(value, dict) and recursive:
            # Recursively process nested dictionaries
            result[key] = mask_sensitive_fields(value, recursive=True)
        elif isinstance(value, list) and recursive:
            # Process lists that may contain dictionaries
            result[key] = [
                mask_sensitive_fields(item, recursive=True) if isinstance(item, dict) else item
                for item in value
            ]
        elif is_sensitive_key(key) and value:
            # Mask sensitive values
            result[key] = REDACTED_PLACEHOLDER
        else:
            result[key] = value

    return result


def mask_sensitive_string(text: str) -> str:
    """
    Mask potential sensitive data patterns in a string.
    Useful for sanitizing log messages or error strings.

    Args:
        text: The text that may contain sensitive data

    Returns:
        The text with potential sensitive patterns masked
    """
    if not text or not isinstance(text, str):
        return text

    def mask_long_string(m: Match[str]) -> str:
        """Mask strings that are 32+ characters (likely API keys)."""
        return REDACTED_PLACEHOLDER if len(m.group(0)) >= 32 else m.group(0)

    def mask_key_value(m: Match[str]) -> str:
        """Mask values in key=value or key:value format."""
        matched = m.group(0)
        if '=' in matched:
            return matched.split('=')[0] + '=' + REDACTED_PLACEHOLDER
        return matched.split(':')[0] + ':' + REDACTED_PLACEHOLDER

    result = text
    # Mask Bearer tokens
    result = re.sub(r'Bearer\s+[A-Za-z0-9\-_\.]+', 'Bearer ***REDACTED***', result, flags=re.IGNORECASE)
    # Mask API keys (common formats - 32+ character strings)
    result = re.sub(r'[A-Za-z0-9]{32,}', mask_long_string, result)
    # Mask secrets in key=value format
    result = re.sub(
        r'(secret|password|token|api_key|apikey|credential)["\']?\s*[:=]\s*["\']?[^"\'\s,}]+',
        mask_key_value,
        result,
        flags=re.IGNORECASE
    )

    return result


def safe_log_tool_configs(tool_configs: Optional[Dict[str, Any]], prefix: str = "") -> str:
    """
    Create a safe string representation of tool_configs for logging.

    Args:
        tool_configs: The tool configurations to log
        prefix: Optional prefix for the log message

    Returns:
        A safe string representation with sensitive data masked
    """
    if not tool_configs:
        return f"{prefix}tool_configs: None"

    masked = mask_sensitive_fields(tool_configs)
    return f"{prefix}tool_configs: {masked}"
