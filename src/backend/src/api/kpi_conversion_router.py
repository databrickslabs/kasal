"""
KPI Conversion API Router

Handles KPI conversion endpoints for transforming key performance indicators
between different formats (YAML, DAX, SQL, UC Metrics, Power BI).
"""

import logging
from typing import List

from fastapi import APIRouter, HTTPException

from src.dependencies.providers import GroupContextDep
from src.schemas.kpi_conversion import (
    ConversionFormatsResponse,
    ConversionRequest,
    ConversionResponse,
    ValidateRequest,
    ValidationResponse,
)
from src.services.converters.base.converter import ConversionFormat
from src.services.powerbi.kpi_conversion import KPIConversionService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/kpi-conversion", tags=["kpi-conversion"])


@router.get("/formats", response_model=ConversionFormatsResponse)
async def get_available_formats(
    group_context: GroupContextDep,
) -> ConversionFormatsResponse:
    """
    Get list of available conversion formats and supported conversion paths.

    Returns:
        ConversionFormatsResponse: Available formats and conversion paths
    """
    try:
        service = KPIConversionService()
        formats = await service.get_available_formats()
        return formats
    except Exception as e:
        logger.exception("Error fetching available formats")
        raise HTTPException(
            status_code=500, detail="Failed to fetch available formats"
        ) from e


@router.post("/convert", response_model=ConversionResponse)
async def convert_measure(
    request: ConversionRequest, group_context: GroupContextDep
) -> ConversionResponse:
    """
    Convert measures from one format to another.

    Args:
        request: Conversion request with source format, target format, and data
        group_context: Group context from dependency injection

    Returns:
        ConversionResponse: Converted measures in target format

    Raises:
        HTTPException: If conversion fails
    """
    try:
        service = KPIConversionService()
        result = await service.convert(
            # The schema and the converters each declare the same str enum;
            # hand the service its own.
            source_format=ConversionFormat(request.source_format.value),
            target_format=ConversionFormat(request.target_format.value),
            input_data=request.input_data,
            config=request.config,
        )
        return result
    except ValueError as e:
        logger.error(f"Validation error during conversion: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Error during measure conversion")
        raise HTTPException(status_code=500, detail="Conversion failed") from e


@router.post("/validate", response_model=ValidationResponse)
async def validate_measure(
    request: ValidateRequest, group_context: GroupContextDep
) -> ValidationResponse:
    """
    Validate measure definition before conversion.

    Args:
        request: Validation request with format and data
        group_context: Group context from dependency injection

    Returns:
        ValidationResponse: Validation result with any errors or warnings

    Raises:
        HTTPException: If validation service fails
    """
    try:
        service = KPIConversionService()
        result = await service.validate(
            format=ConversionFormat(request.format.value),
            input_data=request.input_data,
        )
        return result
    except Exception as e:
        logger.exception("Error during validation")
        raise HTTPException(status_code=500, detail="Validation failed") from e


@router.post("/batch-convert", response_model=List[ConversionResponse])
async def batch_convert_measures(
    requests: List[ConversionRequest], group_context: GroupContextDep
) -> List[ConversionResponse]:
    """
    Convert multiple measures in a single request.

    Args:
        requests: List of conversion requests
        group_context: Group context from dependency injection

    Returns:
        List[ConversionResponse]: List of conversion results

    Raises:
        HTTPException: If batch conversion fails
    """
    try:
        service = KPIConversionService()
        results = await service.batch_convert(requests)
        return results
    except ValueError as e:
        logger.error(f"Validation error during batch conversion: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Error during batch conversion")
        raise HTTPException(status_code=500, detail="Batch conversion failed") from e
