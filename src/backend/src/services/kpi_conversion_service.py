"""
KPI Conversion Service

Business logic layer for KPI conversion operations.
Orchestrates conversion between different KPI formats using the converters package.
"""

import logging
from typing import Any, Dict, List, Optional
from src.converters.base.converter import ConversionFormat
from src.converters.base.factory import ConverterFactory
from src.schemas.kpi_conversion import (
    ConversionRequest,
    ConversionResponse,
    ConversionPath,
    ConversionFormatsResponse,
    ValidationResponse,
    ValidationError,
)

logger = logging.getLogger(__name__)


class KPIConversionService:
    """
    Service for handling KPI conversion operations.

    Provides high-level business logic for:
    - Converting KPIs between formats (YAML, DAX, SQL, UC Metrics, PBI)
    - Validating KPI definitions
    - Batch conversion operations
    - Format discovery and capability queries
    """

    def __init__(self):
        """Initialize the KPI conversion service."""
        self.factory = ConverterFactory()

    async def get_available_formats(self) -> ConversionFormatsResponse:
        """
        Get list of available conversion formats and supported paths.

        Returns:
            ConversionFormatsResponse: Available formats and conversion paths
        """
        try:
            # Get all available conversion paths from factory
            conversions = self.factory.get_available_conversions()

            # Extract unique formats
            formats = set()
            for source, target in conversions:
                formats.add(source)
                formats.add(target)

            # Build conversion paths
            paths = [
                ConversionPath(source=source, target=target)
                for source, target in conversions
            ]

            return ConversionFormatsResponse(
                formats=list(formats),
                conversion_paths=paths
            )
        except Exception as e:
            logger.error(f"Error fetching available formats: {e}")
            raise

    async def convert(
        self,
        source_format: ConversionFormat,
        target_format: ConversionFormat,
        input_data: Any,
        config: Optional[Dict[str, Any]] = None
    ) -> ConversionResponse:
        """
        Convert KPIs from source format to target format.

        Args:
            source_format: Source format of input data
            target_format: Target format for conversion
            input_data: Data to convert
            config: Optional conversion configuration

        Returns:
            ConversionResponse: Conversion result with output data

        Raises:
            ValueError: If conversion path not supported or input invalid
        """
        try:
            # Check if conversion path is supported
            if not self.factory.supports_conversion(source_format, target_format):
                raise ValueError(
                    f"Conversion from {source_format} to {target_format} is not supported"
                )

            # Create converter instance
            converter = self.factory.create(
                source_format=source_format,
                target_format=target_format,
                config=config
            )

            # Validate input
            if not converter.validate_input(input_data):
                raise ValueError("Input data validation failed")

            # Perform conversion
            output_data = converter.convert(input_data)

            # Build response
            return ConversionResponse(
                success=True,
                source_format=source_format,
                target_format=target_format,
                output_data=output_data,
                metadata={
                    "converter_type": type(converter).__name__,
                },
                warnings=[]
            )

        except ValueError as e:
            logger.error(f"Validation error during conversion: {e}")
            raise
        except Exception as e:
            logger.error(f"Error during conversion: {e}", exc_info=True)
            raise ValueError(f"Conversion failed: {str(e)}")

    async def validate(
        self,
        format: ConversionFormat,
        input_data: Any
    ) -> ValidationResponse:
        """
        Validate KPI definition for a specific format.

        Args:
            format: Format to validate against
            input_data: Data to validate

        Returns:
            ValidationResponse: Validation result with errors/warnings

        Raises:
            ValueError: If validation service fails
        """
        try:
            # For now, we'll use a converter's validate_input method
            # In the future, this could use dedicated validators

            # Try to create a converter for this format (using self as target)
            # This is a workaround - ideally we'd have dedicated validators
            errors: List[ValidationError] = []
            warnings: List[ValidationError] = []

            # Basic structure validation
            if not isinstance(input_data, dict):
                errors.append(ValidationError(
                    field="root",
                    message="Input data must be a dictionary",
                    severity="error"
                ))

            # Format-specific validation could go here
            # For now, just basic checks
            if format == ConversionFormat.YAML:
                if "kbis" not in input_data:
                    errors.append(ValidationError(
                        field="kbis",
                        message="YAML format requires 'kbis' field",
                        severity="error"
                    ))
                elif not input_data.get("kbis"):
                    warnings.append(ValidationError(
                        field="kbis",
                        message="No KBIs defined",
                        severity="warning"
                    ))

            return ValidationResponse(
                valid=len(errors) == 0,
                errors=errors,
                warnings=warnings
            )

        except Exception as e:
            logger.error(f"Error during validation: {e}", exc_info=True)
            raise ValueError(f"Validation failed: {str(e)}")

    async def batch_convert(
        self,
        requests: List[ConversionRequest]
    ) -> List[ConversionResponse]:
        """
        Convert multiple KPIs in a batch operation.

        Args:
            requests: List of conversion requests

        Returns:
            List[ConversionResponse]: List of conversion results

        Raises:
            ValueError: If any conversion fails
        """
        try:
            results = []
            for request in requests:
                result = await self.convert(
                    source_format=request.source_format,
                    target_format=request.target_format,
                    input_data=request.input_data,
                    config=request.config
                )
                results.append(result)

            return results

        except Exception as e:
            logger.error(f"Error during batch conversion: {e}", exc_info=True)
            raise ValueError(f"Batch conversion failed: {str(e)}")
