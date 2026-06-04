"""
Unit tests for the KPI Conversion API router.

Tests get_available_formats, convert_measure, validate_measure, and
batch_convert_measures endpoints by calling handler functions directly
with a mocked KPIConversionService.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _patch_service(mock_instance):
    """Patch KPIConversionService constructor to return mock_instance."""
    return patch(
        "src.api.kpi_conversion_router.KPIConversionService",
        return_value=mock_instance,
    )


def _make_formats_response():
    from src.schemas.kpi_conversion import (
        ConversionFormatsResponse,
        ConversionFormat,
        ConversionPath,
    )

    return ConversionFormatsResponse(
        formats=list(ConversionFormat),
        conversion_paths=[
            ConversionPath(source=ConversionFormat.YAML, target=ConversionFormat.DAX),
            ConversionPath(source=ConversionFormat.YAML, target=ConversionFormat.SQL),
        ],
    )


def _make_conversion_response(success=True):
    from src.schemas.kpi_conversion import ConversionResponse, ConversionFormat

    return ConversionResponse(
        success=success,
        source_format=ConversionFormat.YAML,
        target_format=ConversionFormat.DAX,
        output_data={"measures": []},
    )


def _make_validation_response(valid=True):
    from src.schemas.kpi_conversion import ValidationResponse

    return ValidationResponse(valid=valid)


# ---------------------------------------------------------------------------
# Tests – get_available_formats
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_available_formats_success():
    """get_available_formats returns all formats from service."""
    from src.api.kpi_conversion_router import get_available_formats

    mock_svc = AsyncMock()
    mock_svc.get_available_formats = AsyncMock(return_value=_make_formats_response())

    with _patch_service(mock_svc):
        result = await get_available_formats(group_context=None)

    assert len(result.formats) > 0
    assert len(result.conversion_paths) == 2


@pytest.mark.asyncio
async def test_get_available_formats_service_error_raises_http_500():
    """Service exception is converted to HTTP 500."""
    from src.api.kpi_conversion_router import get_available_formats
    from fastapi import HTTPException

    mock_svc = AsyncMock()
    mock_svc.get_available_formats = AsyncMock(side_effect=RuntimeError("db error"))

    with _patch_service(mock_svc):
        with pytest.raises(HTTPException) as exc_info:
            await get_available_formats(group_context=None)

    assert exc_info.value.status_code == 500
    assert "Failed to fetch" in exc_info.value.detail


# ---------------------------------------------------------------------------
# Tests – convert_measure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_convert_measure_success():
    """convert_measure returns converted data on success."""
    from src.api.kpi_conversion_router import convert_measure
    from src.schemas.kpi_conversion import ConversionRequest, ConversionFormat

    mock_svc = AsyncMock()
    mock_svc.convert = AsyncMock(return_value=_make_conversion_response())

    request = ConversionRequest(
        source_format=ConversionFormat.YAML,
        target_format=ConversionFormat.DAX,
        input_data={"kpis": []},
    )

    with _patch_service(mock_svc):
        result = await convert_measure(request=request, group_context=None)

    assert result.success is True
    mock_svc.convert.assert_awaited_once_with(
        source_format=ConversionFormat.YAML,
        target_format=ConversionFormat.DAX,
        input_data={"kpis": []},
        config=None,
    )


@pytest.mark.asyncio
async def test_convert_measure_value_error_raises_http_400():
    """ValueError from service is converted to HTTP 400."""
    from src.api.kpi_conversion_router import convert_measure
    from src.schemas.kpi_conversion import ConversionRequest, ConversionFormat
    from fastapi import HTTPException

    mock_svc = AsyncMock()
    mock_svc.convert = AsyncMock(side_effect=ValueError("unsupported conversion"))

    request = ConversionRequest(
        source_format=ConversionFormat.YAML,
        target_format=ConversionFormat.SQL,
        input_data="bad",
    )

    with _patch_service(mock_svc):
        with pytest.raises(HTTPException) as exc_info:
            await convert_measure(request=request, group_context=None)

    assert exc_info.value.status_code == 400
    assert "unsupported conversion" in exc_info.value.detail


@pytest.mark.asyncio
async def test_convert_measure_general_error_raises_http_500():
    """Generic exceptions are converted to HTTP 500."""
    from src.api.kpi_conversion_router import convert_measure
    from src.schemas.kpi_conversion import ConversionRequest, ConversionFormat
    from fastapi import HTTPException

    mock_svc = AsyncMock()
    mock_svc.convert = AsyncMock(side_effect=RuntimeError("internal failure"))

    request = ConversionRequest(
        source_format=ConversionFormat.DAX,
        target_format=ConversionFormat.SQL,
        input_data="MEASURE Sales[Total] = SUM(Sales[Amount])",
    )

    with _patch_service(mock_svc):
        with pytest.raises(HTTPException) as exc_info:
            await convert_measure(request=request, group_context=None)

    assert exc_info.value.status_code == 500
    assert "Conversion failed" in exc_info.value.detail


@pytest.mark.asyncio
async def test_convert_measure_passes_config():
    """Optional config dict is forwarded to service.convert."""
    from src.api.kpi_conversion_router import convert_measure
    from src.schemas.kpi_conversion import ConversionRequest, ConversionFormat

    mock_svc = AsyncMock()
    mock_svc.convert = AsyncMock(return_value=_make_conversion_response())

    config = {"validate": True, "optimize": False}
    request = ConversionRequest(
        source_format=ConversionFormat.YAML,
        target_format=ConversionFormat.UC_METRICS,
        input_data={"kpis": []},
        config=config,
    )

    with _patch_service(mock_svc):
        await convert_measure(request=request, group_context=None)

    call_kwargs = mock_svc.convert.call_args.kwargs
    assert call_kwargs["config"] == config


# ---------------------------------------------------------------------------
# Tests – validate_measure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_validate_measure_valid():
    """validate_measure returns valid=True when service confirms validity."""
    from src.api.kpi_conversion_router import validate_measure
    from src.schemas.kpi_conversion import ValidateRequest, ConversionFormat

    mock_svc = AsyncMock()
    mock_svc.validate = AsyncMock(return_value=_make_validation_response(valid=True))

    request = ValidateRequest(
        format=ConversionFormat.YAML,
        input_data={"description": "Sales", "kpis": [{"formula": "SUM(x)"}]},
    )

    with _patch_service(mock_svc):
        result = await validate_measure(request=request, group_context=None)

    assert result.valid is True


@pytest.mark.asyncio
async def test_validate_measure_invalid():
    """validate_measure returns valid=False for invalid data."""
    from src.api.kpi_conversion_router import validate_measure
    from src.schemas.kpi_conversion import ValidateRequest, ConversionFormat

    mock_svc = AsyncMock()
    mock_svc.validate = AsyncMock(return_value=_make_validation_response(valid=False))

    request = ValidateRequest(format=ConversionFormat.DAX, input_data="garbage")

    with _patch_service(mock_svc):
        result = await validate_measure(request=request, group_context=None)

    assert result.valid is False


@pytest.mark.asyncio
async def test_validate_measure_service_error_raises_http_500():
    """Service exception during validation is converted to HTTP 500."""
    from src.api.kpi_conversion_router import validate_measure
    from src.schemas.kpi_conversion import ValidateRequest, ConversionFormat
    from fastapi import HTTPException

    mock_svc = AsyncMock()
    mock_svc.validate = AsyncMock(side_effect=Exception("schema error"))

    request = ValidateRequest(format=ConversionFormat.SQL, input_data="SELECT 1")

    with _patch_service(mock_svc):
        with pytest.raises(HTTPException) as exc_info:
            await validate_measure(request=request, group_context=None)

    assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# Tests – batch_convert_measures
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_batch_convert_measures_success():
    """batch_convert_measures returns a list of results."""
    from src.api.kpi_conversion_router import batch_convert_measures
    from src.schemas.kpi_conversion import ConversionRequest, ConversionFormat

    mock_svc = AsyncMock()
    responses = [_make_conversion_response(), _make_conversion_response()]
    mock_svc.batch_convert = AsyncMock(return_value=responses)

    requests = [
        ConversionRequest(
            source_format=ConversionFormat.YAML,
            target_format=ConversionFormat.DAX,
            input_data={"kpis": []},
        ),
        ConversionRequest(
            source_format=ConversionFormat.YAML,
            target_format=ConversionFormat.SQL,
            input_data={"kpis": []},
        ),
    ]

    with _patch_service(mock_svc):
        result = await batch_convert_measures(requests=requests, group_context=None)

    assert len(result) == 2
    mock_svc.batch_convert.assert_awaited_once_with(requests)


@pytest.mark.asyncio
async def test_batch_convert_measures_value_error_raises_400():
    """ValueError during batch conversion is converted to HTTP 400."""
    from src.api.kpi_conversion_router import batch_convert_measures
    from src.schemas.kpi_conversion import ConversionRequest, ConversionFormat
    from fastapi import HTTPException

    mock_svc = AsyncMock()
    mock_svc.batch_convert = AsyncMock(side_effect=ValueError("bad format"))

    requests = [
        ConversionRequest(
            source_format=ConversionFormat.YAML,
            target_format=ConversionFormat.DAX,
            input_data={},
        )
    ]

    with _patch_service(mock_svc):
        with pytest.raises(HTTPException) as exc_info:
            await batch_convert_measures(requests=requests, group_context=None)

    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_batch_convert_measures_general_error_raises_500():
    """Generic error during batch conversion is converted to HTTP 500."""
    from src.api.kpi_conversion_router import batch_convert_measures
    from src.schemas.kpi_conversion import ConversionRequest, ConversionFormat
    from fastapi import HTTPException

    mock_svc = AsyncMock()
    mock_svc.batch_convert = AsyncMock(side_effect=RuntimeError("crash"))

    requests = [
        ConversionRequest(
            source_format=ConversionFormat.YAML,
            target_format=ConversionFormat.SQL,
            input_data={},
        )
    ]

    with _patch_service(mock_svc):
        with pytest.raises(HTTPException) as exc_info:
            await batch_convert_measures(requests=requests, group_context=None)

    assert exc_info.value.status_code == 500
    assert "Batch conversion failed" in exc_info.value.detail
