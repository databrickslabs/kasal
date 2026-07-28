"""Factory for creating appropriate converter instances"""

from typing import Dict, Type, Optional, Any
from .converter import BaseConverter, ConversionFormat


class ConverterFactory:
    """
    Factory class for creating converter instances.

    Manages registration and instantiation of converters based on
    source and target formats.
    """

    _converters: Dict[tuple[ConversionFormat, ConversionFormat], Type[BaseConverter]] = {}

    @classmethod
    def register(
        cls,
        source_format: ConversionFormat,
        target_format: ConversionFormat,
        converter_class: Type[BaseConverter]
    ) -> None:
        """
        Register a converter for a specific conversion path.

        Args:
            source_format: Source data format
            target_format: Target data format
            converter_class: Converter class to handle this conversion
        """
        key = (source_format, target_format)
        cls._converters[key] = converter_class

    @classmethod
    def create(
        cls,
        source_format: ConversionFormat,
        target_format: ConversionFormat,
        config: Optional[Dict[str, Any]] = None
    ) -> BaseConverter:
        """
        Create a converter instance for the specified conversion path.

        Args:
            source_format: Source data format
            target_format: Target data format
            config: Optional configuration for the converter

        Returns:
            Converter instance

        Raises:
            ValueError: If no converter registered for this conversion path
        """
        key = (source_format, target_format)
        converter_class = cls._converters.get(key)

        if not converter_class:
            raise ValueError(
                f"No converter registered for {source_format} -> {target_format}. "
                f"Available conversions: {list(cls._converters.keys())}"
            )

        return converter_class(config=config)

    @classmethod
    def get_available_conversions(cls) -> list[tuple[ConversionFormat, ConversionFormat]]:
        """
        Get list of all available conversion paths.

        Returns:
            List of (source_format, target_format) tuples
        """
        return list(cls._converters.keys())

    @classmethod
    def supports_conversion(
        cls,
        source_format: ConversionFormat,
        target_format: ConversionFormat
    ) -> bool:
        """
        Check if a conversion path is supported.

        Args:
            source_format: Source data format
            target_format: Target data format

        Returns:
            True if conversion is supported, False otherwise
        """
        return (source_format, target_format) in cls._converters
