"""
Comprehensive unit tests for services/kpi_conversion_service.py
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock

from src.converters.base.converter import ConversionFormat
from src.services.kpi_conversion_service import KPIConversionService
from src.schemas.kpi_conversion import (
    ConversionRequest,
    ConversionResponse,
    ConversionFormatsResponse,
    ValidationResponse,
    ValidationError,
)


@pytest.fixture
def mock_factory():
    factory = MagicMock()
    return factory


@pytest.fixture
def service():
    svc = KPIConversionService()
    return svc


class TestKPIConversionServiceInit:
    """Tests for KPIConversionService initialization."""

    def test_init_creates_factory(self):
        svc = KPIConversionService()
        assert svc.factory is not None

    def test_factory_type(self):
        from src.converters.base.factory import ConverterFactory
        svc = KPIConversionService()
        assert isinstance(svc.factory, ConverterFactory)


class TestGetAvailableFormats:
    """Tests for get_available_formats."""

    @pytest.mark.asyncio
    async def test_returns_formats_response(self, service):
        with patch.object(service.factory, "get_available_conversions", return_value=[
            (ConversionFormat.YAML, ConversionFormat.DAX),
            (ConversionFormat.YAML, ConversionFormat.SQL),
        ]):
            result = await service.get_available_formats()

        assert isinstance(result, ConversionFormatsResponse)

    @pytest.mark.asyncio
    async def test_formats_contain_yaml_dax(self, service):
        with patch.object(service.factory, "get_available_conversions", return_value=[
            (ConversionFormat.YAML, ConversionFormat.DAX),
        ]):
            result = await service.get_available_formats()

        assert ConversionFormat.YAML in result.formats or "yaml" in result.formats
        assert ConversionFormat.DAX in result.formats or "dax" in result.formats

    @pytest.mark.asyncio
    async def test_conversion_paths_populated(self, service):
        with patch.object(service.factory, "get_available_conversions", return_value=[
            (ConversionFormat.YAML, ConversionFormat.SQL),
        ]):
            result = await service.get_available_formats()

        assert len(result.conversion_paths) == 1

    @pytest.mark.asyncio
    async def test_empty_conversions(self, service):
        with patch.object(service.factory, "get_available_conversions", return_value=[]):
            result = await service.get_available_formats()

        assert result.formats == []
        assert result.conversion_paths == []

    @pytest.mark.asyncio
    async def test_raises_on_factory_error(self, service):
        with patch.object(service.factory, "get_available_conversions", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError):
                await service.get_available_formats()


class TestConvert:
    """Tests for convert."""

    @pytest.mark.asyncio
    async def test_raises_value_error_for_unsupported_path(self, service):
        with patch.object(service.factory, "supports_conversion", return_value=False):
            with pytest.raises(ValueError, match="not supported"):
                await service.convert(
                    source_format=ConversionFormat.YAML,
                    target_format=ConversionFormat.DAX,
                    input_data={"kbis": []},
                )

    @pytest.mark.asyncio
    async def test_raises_value_error_on_invalid_input(self, service):
        mock_converter = MagicMock()
        mock_converter.validate_input.return_value = False

        with patch.object(service.factory, "supports_conversion", return_value=True):
            with patch.object(service.factory, "create", return_value=mock_converter):
                with pytest.raises(ValueError, match="validation failed"):
                    await service.convert(
                        source_format=ConversionFormat.YAML,
                        target_format=ConversionFormat.DAX,
                        input_data={"bad": "data"},
                    )

    @pytest.mark.asyncio
    async def test_returns_conversion_response_on_success(self, service):
        mock_converter = MagicMock()
        mock_converter.validate_input.return_value = True
        mock_converter.convert.return_value = "MEASURE Revenue = SUM(...)"

        with patch.object(service.factory, "supports_conversion", return_value=True):
            with patch.object(service.factory, "create", return_value=mock_converter):
                result = await service.convert(
                    source_format=ConversionFormat.YAML,
                    target_format=ConversionFormat.DAX,
                    input_data={"kbis": []},
                )

        assert isinstance(result, ConversionResponse)
        assert result.success is True
        assert result.output_data == "MEASURE Revenue = SUM(...)"

    @pytest.mark.asyncio
    async def test_response_contains_formats(self, service):
        mock_converter = MagicMock()
        mock_converter.validate_input.return_value = True
        mock_converter.convert.return_value = {}

        with patch.object(service.factory, "supports_conversion", return_value=True):
            with patch.object(service.factory, "create", return_value=mock_converter):
                result = await service.convert(
                    source_format=ConversionFormat.YAML,
                    target_format=ConversionFormat.SQL,
                    input_data={"kbis": []},
                )

        assert result.source_format == ConversionFormat.YAML
        assert result.target_format == ConversionFormat.SQL

    @pytest.mark.asyncio
    async def test_response_contains_converter_type_in_metadata(self, service):
        mock_converter = MagicMock()
        mock_converter.__class__.__name__ = "YAMLToDAXConverter"
        mock_converter.validate_input.return_value = True
        mock_converter.convert.return_value = {}

        with patch.object(service.factory, "supports_conversion", return_value=True):
            with patch.object(service.factory, "create", return_value=mock_converter):
                result = await service.convert(
                    source_format=ConversionFormat.YAML,
                    target_format=ConversionFormat.DAX,
                    input_data={"kbis": []},
                )

        assert "converter_type" in result.metadata

    @pytest.mark.asyncio
    async def test_raises_value_error_on_converter_exception(self, service):
        mock_converter = MagicMock()
        mock_converter.validate_input.return_value = True
        mock_converter.convert.side_effect = RuntimeError("conversion error")

        with patch.object(service.factory, "supports_conversion", return_value=True):
            with patch.object(service.factory, "create", return_value=mock_converter):
                with pytest.raises(ValueError, match="Conversion failed"):
                    await service.convert(
                        source_format=ConversionFormat.YAML,
                        target_format=ConversionFormat.DAX,
                        input_data={"kbis": []},
                    )

    @pytest.mark.asyncio
    async def test_passes_config_to_factory(self, service):
        mock_converter = MagicMock()
        mock_converter.validate_input.return_value = True
        mock_converter.convert.return_value = {}

        with patch.object(service.factory, "supports_conversion", return_value=True):
            with patch.object(service.factory, "create", return_value=mock_converter) as mock_create:
                config = {"option": "value"}
                await service.convert(
                    source_format=ConversionFormat.YAML,
                    target_format=ConversionFormat.DAX,
                    input_data={"kbis": []},
                    config=config,
                )
                mock_create.assert_called_once_with(
                    source_format=ConversionFormat.YAML,
                    target_format=ConversionFormat.DAX,
                    config=config,
                )


class TestValidate:
    """Tests for validate."""

    @pytest.mark.asyncio
    async def test_returns_invalid_for_non_dict(self, service):
        result = await service.validate(
            format=ConversionFormat.YAML,
            input_data="not a dict",
        )
        assert result.valid is False
        assert any(e.field == "root" for e in result.errors)

    @pytest.mark.asyncio
    async def test_yaml_missing_kbis_field(self, service):
        result = await service.validate(
            format=ConversionFormat.YAML,
            input_data={"other": "data"},
        )
        assert result.valid is False
        assert any(e.field == "kbis" for e in result.errors)

    @pytest.mark.asyncio
    async def test_yaml_empty_kbis_warning(self, service):
        result = await service.validate(
            format=ConversionFormat.YAML,
            input_data={"kbis": []},
        )
        # Empty kbis is a warning, not an error
        assert result.valid is True
        assert any(e.field == "kbis" for e in result.warnings)

    @pytest.mark.asyncio
    async def test_yaml_valid_with_kbis(self, service):
        result = await service.validate(
            format=ConversionFormat.YAML,
            input_data={"kbis": [{"name": "Revenue"}]},
        )
        assert result.valid is True
        assert len(result.errors) == 0

    @pytest.mark.asyncio
    async def test_non_yaml_format_valid_dict(self, service):
        result = await service.validate(
            format=ConversionFormat.DAX,
            input_data={"some": "data"},
        )
        assert result.valid is True

    @pytest.mark.asyncio
    async def test_raises_value_error_on_unexpected_exception(self, service):
        with patch("src.services.kpi_conversion_service.isinstance", side_effect=RuntimeError("unexpected")):
            with pytest.raises(ValueError, match="Validation failed"):
                await service.validate(
                    format=ConversionFormat.YAML,
                    input_data={"kbis": []},
                )

    @pytest.mark.asyncio
    async def test_returns_validation_response_type(self, service):
        result = await service.validate(
            format=ConversionFormat.SQL,
            input_data={"query": "SELECT 1"},
        )
        assert isinstance(result, ValidationResponse)


class TestBatchConvert:
    """Tests for batch_convert."""

    @pytest.mark.asyncio
    async def test_empty_batch_returns_empty_list(self, service):
        result = await service.batch_convert([])
        assert result == []

    @pytest.mark.asyncio
    async def test_single_request_in_batch(self, service):
        mock_converter = MagicMock()
        mock_converter.validate_input.return_value = True
        mock_converter.convert.return_value = "output"

        request = ConversionRequest(
            source_format=ConversionFormat.YAML,
            target_format=ConversionFormat.DAX,
            input_data={"kbis": [{"name": "Rev"}]},
        )

        with patch.object(service.factory, "supports_conversion", return_value=True):
            with patch.object(service.factory, "create", return_value=mock_converter):
                results = await service.batch_convert([request])

        assert len(results) == 1
        assert results[0].success is True

    @pytest.mark.asyncio
    async def test_multiple_requests_in_batch(self, service):
        mock_converter = MagicMock()
        mock_converter.validate_input.return_value = True
        mock_converter.convert.return_value = {}

        requests = [
            ConversionRequest(
                source_format=ConversionFormat.YAML,
                target_format=ConversionFormat.DAX,
                input_data={"kbis": [{"name": "R1"}]},
            ),
            ConversionRequest(
                source_format=ConversionFormat.YAML,
                target_format=ConversionFormat.SQL,
                input_data={"kbis": [{"name": "R2"}]},
            ),
        ]

        with patch.object(service.factory, "supports_conversion", return_value=True):
            with patch.object(service.factory, "create", return_value=mock_converter):
                results = await service.batch_convert(requests)

        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_batch_raises_on_first_failure(self, service):
        request = ConversionRequest(
            source_format=ConversionFormat.YAML,
            target_format=ConversionFormat.DAX,
            input_data={"kbis": [{"name": "Rev"}]},
        )

        with patch.object(service.factory, "supports_conversion", return_value=False):
            with pytest.raises(ValueError, match="Batch conversion failed"):
                await service.batch_convert([request])
