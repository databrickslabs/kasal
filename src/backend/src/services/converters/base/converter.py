"""Base converter abstract class"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from enum import Enum


class ConversionFormat(str, Enum):
    """Supported conversion formats"""
    YAML = "yaml"
    DAX = "dax"
    SQL = "sql"
    UC_METRICS = "uc_metrics"
    POWERBI = "powerbi"


class BaseConverter(ABC):
    """
    Abstract base class for all converters.

    Each converter handles transformation between specific formats
    (e.g., YAML -> DAX, YAML -> SQL, YAML -> UC Metrics, PBI -> YAML, etc.)
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize converter with optional configuration.

        Args:
            config: Configuration dictionary for converter behavior
        """
        self.config = config or {}

    @abstractmethod
    def convert(self, input_data: Any, **kwargs) -> Any:
        """
        Convert input data to target format.

        Args:
            input_data: Input data in source format
            **kwargs: Additional conversion parameters

        Returns:
            Converted data in target format

        Raises:
            ValueError: If input data is invalid
            NotImplementedError: If conversion path not implemented
        """
        pass

    @abstractmethod
    def validate_input(self, input_data: Any) -> bool:
        """
        Validate input data before conversion.

        Args:
            input_data: Input data to validate

        Returns:
            True if valid, False otherwise

        Raises:
            ValueError: If validation fails with details
        """
        pass

    @property
    @abstractmethod
    def source_format(self) -> ConversionFormat:
        """Return the source format this converter accepts"""
        pass

    @property
    @abstractmethod
    def target_format(self) -> ConversionFormat:
        """Return the target format this converter produces"""
        pass
