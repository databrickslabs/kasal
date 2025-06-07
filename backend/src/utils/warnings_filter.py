"""
Utility module to suppress known deprecation warnings from third-party libraries.

This module should be imported early in the application lifecycle to ensure
warnings are filtered before they can be triggered by other imports.
"""

import warnings


def suppress_deprecation_warnings():
    """
    Suppress known deprecation warnings from third-party libraries.
    
    These warnings are from external dependencies and cannot be fixed in our codebase:
    - httpx: "Use 'content=<...>' to upload raw bytes/text content"
    - chromadb: "Accessing the 'model_fields' attribute on the instance is deprecated"
    - websockets: "remove second argument of ws_handler"
    """
    # Suppress by module
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="httpx")
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="chromadb")
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="websockets")
    
    # Suppress by specific message patterns
    warnings.filterwarnings("ignore", message=".*Use 'content=.*' to upload raw bytes/text content.*")
    warnings.filterwarnings("ignore", message=".*Accessing the 'model_fields' attribute on the instance is deprecated.*")
    warnings.filterwarnings("ignore", message=".*remove second argument of ws_handler.*")
    
    # Additional Pydantic deprecation warnings
    warnings.filterwarnings("ignore", message=".*PydanticDeprecatedSince211.*")


# Apply the filters when this module is imported
suppress_deprecation_warnings()