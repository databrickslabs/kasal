"""Serialization of execution payloads for database storage."""

import json
import uuid
from typing import Any, Dict


def sanitize_for_database(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure all data is properly serializable for database storage.

    Args:
        data: Dictionary containing execution data

    Returns:
        Sanitized data safe for database storage
    """
    # Create a deep copy to avoid modifying the original
    result = {}

    for key, value in data.items():
        if isinstance(value, dict):
            result[key] = sanitize_for_database(value)
        elif isinstance(value, list):
            result[key] = [
                (sanitize_for_database(item) if isinstance(item, dict) else item)
                for item in value
            ]
        elif isinstance(value, uuid.UUID):
            # Convert UUID to string
            result[key] = str(value)
        else:
            # Ensure value is JSON serializable
            try:
                json.dumps(value)
                result[key] = value
            except (TypeError, OverflowError):
                # Convert to string if not serializable
                result[key] = str(value)

    return result
