"""
Unit tests for ConverterFactory.

Tests:
- register / create cycle
- create raises ValueError for unregistered conversion path
- get_available_conversions reflects registered paths
- supports_conversion returns correct True/False
- Registry isolation between tests via setup_method
"""
import pytest
from typing import Any

from src.converters.base.converter import BaseConverter, ConversionFormat
from src.converters.base.factory import ConverterFactory


# ---------------------------------------------------------------------------
# Minimal concrete converters used in tests
# ---------------------------------------------------------------------------

class StubYamlToDaxConverter(BaseConverter):
    """Stub: YAML -> DAX."""

    @property
    def source_format(self) -> ConversionFormat:
        return ConversionFormat.YAML

    @property
    def target_format(self) -> ConversionFormat:
        return ConversionFormat.DAX

    def convert(self, input_data: Any, **kwargs) -> Any:
        return f"dax:{input_data}"

    def validate_input(self, input_data: Any) -> bool:
        return input_data is not None


class StubYamlToSqlConverter(BaseConverter):
    """Stub: YAML -> SQL."""

    @property
    def source_format(self) -> ConversionFormat:
        return ConversionFormat.YAML

    @property
    def target_format(self) -> ConversionFormat:
        return ConversionFormat.SQL

    def convert(self, input_data: Any, **kwargs) -> Any:
        return f"sql:{input_data}"

    def validate_input(self, input_data: Any) -> bool:
        return bool(input_data)


class StubPowerBIToYamlConverter(BaseConverter):
    """Stub: POWERBI -> YAML."""

    @property
    def source_format(self) -> ConversionFormat:
        return ConversionFormat.POWERBI

    @property
    def target_format(self) -> ConversionFormat:
        return ConversionFormat.YAML

    def convert(self, input_data: Any, **kwargs) -> Any:
        return f"yaml:{input_data}"

    def validate_input(self, input_data: Any) -> bool:
        return True


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestConverterFactory:
    """Tests for ConverterFactory class-level registry."""

    # ------------------------------------------------------------------
    # Isolation: save and restore _converters around every test so that
    # production registrations made outside these tests are not wiped and
    # registrations made inside do not leak into other tests.
    # ------------------------------------------------------------------

    def setup_method(self):
        """Snapshot the global registry before each test."""
        self._original_converters = dict(ConverterFactory._converters)

    def teardown_method(self):
        """Restore the global registry after each test."""
        ConverterFactory._converters = self._original_converters

    # ------------------------------------------------------------------
    # register
    # ------------------------------------------------------------------

    def test_register_adds_entry_to_registry(self):
        """Registering a converter adds it to the internal registry."""
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.DAX, StubYamlToDaxConverter
        )
        key = (ConversionFormat.YAML, ConversionFormat.DAX)
        assert key in ConverterFactory._converters
        assert ConverterFactory._converters[key] is StubYamlToDaxConverter

    def test_register_multiple_paths(self):
        """Multiple conversion paths can be registered independently."""
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.DAX, StubYamlToDaxConverter
        )
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.SQL, StubYamlToSqlConverter
        )
        assert (ConversionFormat.YAML, ConversionFormat.DAX) in ConverterFactory._converters
        assert (ConversionFormat.YAML, ConversionFormat.SQL) in ConverterFactory._converters

    def test_register_overwrites_existing_path(self):
        """Re-registering the same path replaces the old converter class."""
        class AltDaxConverter(StubYamlToDaxConverter):
            pass

        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.DAX, StubYamlToDaxConverter
        )
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.DAX, AltDaxConverter
        )
        assert ConverterFactory._converters[
            (ConversionFormat.YAML, ConversionFormat.DAX)
        ] is AltDaxConverter

    # ------------------------------------------------------------------
    # create
    # ------------------------------------------------------------------

    def test_create_returns_instance_of_registered_class(self):
        """create() returns an instance of the registered converter class."""
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.DAX, StubYamlToDaxConverter
        )
        instance = ConverterFactory.create(ConversionFormat.YAML, ConversionFormat.DAX)
        assert isinstance(instance, StubYamlToDaxConverter)

    def test_create_passes_config_to_instance(self):
        """create() passes the supplied config dict to the converter."""
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.DAX, StubYamlToDaxConverter
        )
        config = {"indent": 2, "output": "compact"}
        instance = ConverterFactory.create(
            ConversionFormat.YAML, ConversionFormat.DAX, config=config
        )
        assert instance.config == config

    def test_create_without_config_gives_empty_dict(self):
        """create() with no config produces a converter with config={}."""
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.DAX, StubYamlToDaxConverter
        )
        instance = ConverterFactory.create(ConversionFormat.YAML, ConversionFormat.DAX)
        assert instance.config == {}

    def test_create_raises_value_error_for_unregistered_path(self):
        """create() raises ValueError when no converter is registered for the path."""
        # Ensure the path is not present
        ConverterFactory._converters.pop(
            (ConversionFormat.SQL, ConversionFormat.UC_METRICS), None
        )

        with pytest.raises(ValueError, match="No converter registered for"):
            ConverterFactory.create(ConversionFormat.SQL, ConversionFormat.UC_METRICS)

    def test_create_error_message_contains_formats(self):
        """ValueError message mentions the source and target formats."""
        ConverterFactory._converters.pop(
            (ConversionFormat.POWERBI, ConversionFormat.UC_METRICS), None
        )
        with pytest.raises(ValueError) as exc_info:
            ConverterFactory.create(ConversionFormat.POWERBI, ConversionFormat.UC_METRICS)
        msg = str(exc_info.value)
        assert "powerbi" in msg.lower() or "POWERBI" in msg
        assert "uc_metrics" in msg.lower() or "UC_METRICS" in msg

    def test_create_error_message_contains_available_conversions(self):
        """ValueError message lists available conversions."""
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.DAX, StubYamlToDaxConverter
        )
        ConverterFactory._converters.pop(
            (ConversionFormat.SQL, ConversionFormat.POWERBI), None
        )
        with pytest.raises(ValueError) as exc_info:
            ConverterFactory.create(ConversionFormat.SQL, ConversionFormat.POWERBI)
        assert "Available conversions" in str(exc_info.value)

    def test_create_two_independent_instances(self):
        """Each create() call returns a fresh instance (not a singleton)."""
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.DAX, StubYamlToDaxConverter
        )
        inst1 = ConverterFactory.create(ConversionFormat.YAML, ConversionFormat.DAX)
        inst2 = ConverterFactory.create(ConversionFormat.YAML, ConversionFormat.DAX)
        assert inst1 is not inst2

    # ------------------------------------------------------------------
    # get_available_conversions
    # ------------------------------------------------------------------

    def test_get_available_conversions_empty_registry(self):
        """Returns empty list when registry is empty."""
        ConverterFactory._converters.clear()
        result = ConverterFactory.get_available_conversions()
        assert result == []

    def test_get_available_conversions_single_entry(self):
        """Returns one tuple after registering a single path."""
        ConverterFactory._converters.clear()
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.DAX, StubYamlToDaxConverter
        )
        result = ConverterFactory.get_available_conversions()
        assert result == [(ConversionFormat.YAML, ConversionFormat.DAX)]

    def test_get_available_conversions_multiple_entries(self):
        """Returns all registered paths as tuples."""
        ConverterFactory._converters.clear()
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.DAX, StubYamlToDaxConverter
        )
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.SQL, StubYamlToSqlConverter
        )
        ConverterFactory.register(
            ConversionFormat.POWERBI, ConversionFormat.YAML, StubPowerBIToYamlConverter
        )
        result = ConverterFactory.get_available_conversions()
        assert len(result) == 3
        assert (ConversionFormat.YAML, ConversionFormat.DAX) in result
        assert (ConversionFormat.YAML, ConversionFormat.SQL) in result
        assert (ConversionFormat.POWERBI, ConversionFormat.YAML) in result

    def test_get_available_conversions_returns_list(self):
        """Return type is always a list."""
        result = ConverterFactory.get_available_conversions()
        assert isinstance(result, list)

    # ------------------------------------------------------------------
    # supports_conversion
    # ------------------------------------------------------------------

    def test_supports_conversion_registered_path_returns_true(self):
        """Returns True for a registered conversion path."""
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.DAX, StubYamlToDaxConverter
        )
        assert ConverterFactory.supports_conversion(
            ConversionFormat.YAML, ConversionFormat.DAX
        ) is True

    def test_supports_conversion_unregistered_path_returns_false(self):
        """Returns False when the path is not in the registry."""
        ConverterFactory._converters.pop(
            (ConversionFormat.SQL, ConversionFormat.UC_METRICS), None
        )
        assert ConverterFactory.supports_conversion(
            ConversionFormat.SQL, ConversionFormat.UC_METRICS
        ) is False

    def test_supports_conversion_direction_matters(self):
        """A->B is different from B->A in supports_conversion."""
        ConverterFactory._converters.clear()
        ConverterFactory.register(
            ConversionFormat.YAML, ConversionFormat.DAX, StubYamlToDaxConverter
        )
        assert ConverterFactory.supports_conversion(
            ConversionFormat.YAML, ConversionFormat.DAX
        ) is True
        assert ConverterFactory.supports_conversion(
            ConversionFormat.DAX, ConversionFormat.YAML
        ) is False

    def test_supports_conversion_after_registration(self):
        """supports_conversion reflects the state after registration."""
        ConverterFactory._converters.pop(
            (ConversionFormat.POWERBI, ConversionFormat.YAML), None
        )
        assert ConverterFactory.supports_conversion(
            ConversionFormat.POWERBI, ConversionFormat.YAML
        ) is False

        ConverterFactory.register(
            ConversionFormat.POWERBI, ConversionFormat.YAML, StubPowerBIToYamlConverter
        )
        assert ConverterFactory.supports_conversion(
            ConversionFormat.POWERBI, ConversionFormat.YAML
        ) is True

    def test_supports_conversion_same_format_not_registered_by_default(self):
        """Identity conversion (format -> same format) is not registered by default."""
        ConverterFactory._converters.pop(
            (ConversionFormat.YAML, ConversionFormat.YAML), None
        )
        assert ConverterFactory.supports_conversion(
            ConversionFormat.YAML, ConversionFormat.YAML
        ) is False

    # ------------------------------------------------------------------
    # Registry isolation between tests
    # ------------------------------------------------------------------

    def test_registry_isolation_part_1(self):
        """Add a registration in part 1 (should not affect part 2)."""
        ConverterFactory._converters.clear()
        ConverterFactory.register(
            ConversionFormat.SQL, ConversionFormat.DAX, StubYamlToDaxConverter
        )
        assert len(ConverterFactory._converters) == 1

    def test_registry_isolation_part_2(self):
        """The registration from part 1 must NOT be present here (teardown restored it)."""
        # After teardown_method restored the snapshot, the SQL->DAX registration
        # added in part_1 is gone.  We merely verify the registry is in the
        # state it was in at the start of this test (which could be empty or
        # contain real registrations from production code — neither matters,
        # only that our test-specific addition is absent).
        assert (ConversionFormat.SQL, ConversionFormat.DAX) not in ConverterFactory._converters
