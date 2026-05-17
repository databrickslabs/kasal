"""
Unit tests for the BaseConverter abstract class and ConversionFormat enum.

Tests:
- ConversionFormat enum values and string behaviour
- BaseConverter cannot be instantiated directly (abstract)
- Concrete subclasses can be instantiated with and without config
- config defaults to empty dict when not supplied
"""
import pytest
from abc import ABC
from typing import Any

from src.converters.base.converter import BaseConverter, ConversionFormat


# ---------------------------------------------------------------------------
# Minimal concrete implementations used across multiple tests
# ---------------------------------------------------------------------------

class YamlToDaxConverter(BaseConverter):
    """Concrete converter: YAML -> DAX."""

    @property
    def source_format(self) -> ConversionFormat:
        return ConversionFormat.YAML

    @property
    def target_format(self) -> ConversionFormat:
        return ConversionFormat.DAX

    def convert(self, input_data: Any, **kwargs) -> Any:
        return f"DAX({input_data})"

    def validate_input(self, input_data: Any) -> bool:
        return input_data is not None


class YamlToSqlConverter(BaseConverter):
    """Concrete converter: YAML -> SQL."""

    @property
    def source_format(self) -> ConversionFormat:
        return ConversionFormat.YAML

    @property
    def target_format(self) -> ConversionFormat:
        return ConversionFormat.SQL

    def convert(self, input_data: Any, **kwargs) -> Any:
        return f"SQL({input_data})"

    def validate_input(self, input_data: Any) -> bool:
        return bool(input_data)


# ---------------------------------------------------------------------------
# Tests for ConversionFormat enum
# ---------------------------------------------------------------------------

class TestConversionFormat:
    """Tests for the ConversionFormat str-enum."""

    def test_yaml_value(self):
        """YAML format has the correct string value."""
        assert ConversionFormat.YAML == "yaml"
        assert ConversionFormat.YAML.value == "yaml"

    def test_dax_value(self):
        """DAX format has the correct string value."""
        assert ConversionFormat.DAX == "dax"
        assert ConversionFormat.DAX.value == "dax"

    def test_sql_value(self):
        """SQL format has the correct string value."""
        assert ConversionFormat.SQL == "sql"
        assert ConversionFormat.SQL.value == "sql"

    def test_uc_metrics_value(self):
        """UC_METRICS format has the correct string value."""
        assert ConversionFormat.UC_METRICS == "uc_metrics"
        assert ConversionFormat.UC_METRICS.value == "uc_metrics"

    def test_powerbi_value(self):
        """POWERBI format has the correct string value."""
        assert ConversionFormat.POWERBI == "powerbi"
        assert ConversionFormat.POWERBI.value == "powerbi"

    def test_all_members_present(self):
        """All five expected members exist."""
        members = {m.name for m in ConversionFormat}
        assert members == {"YAML", "DAX", "SQL", "UC_METRICS", "POWERBI"}

    def test_is_str_enum(self):
        """ConversionFormat is a subclass of str."""
        assert issubclass(ConversionFormat, str)

    def test_string_comparison(self):
        """ConversionFormat values compare equal to plain strings."""
        assert ConversionFormat.YAML == "yaml"
        assert "dax" == ConversionFormat.DAX

    def test_can_be_used_in_dict_key(self):
        """ConversionFormat members can be used as dictionary keys."""
        d = {ConversionFormat.YAML: "source", ConversionFormat.DAX: "target"}
        assert d[ConversionFormat.YAML] == "source"
        assert d[ConversionFormat.DAX] == "target"

    def test_from_string_value(self):
        """ConversionFormat can be constructed from its string value."""
        fmt = ConversionFormat("yaml")
        assert fmt is ConversionFormat.YAML

    def test_invalid_value_raises(self):
        """Constructing from an unknown value raises ValueError."""
        with pytest.raises(ValueError):
            ConversionFormat("unknown_format")


# ---------------------------------------------------------------------------
# Tests for BaseConverter abstract class
# ---------------------------------------------------------------------------

class TestBaseConverterAbstract:
    """Tests that BaseConverter is properly abstract and cannot be instantiated."""

    def test_is_abstract_class(self):
        """BaseConverter is an ABC subclass."""
        assert issubclass(BaseConverter, ABC)

    def test_cannot_instantiate_directly(self):
        """Direct instantiation raises TypeError."""
        with pytest.raises(TypeError):
            BaseConverter()  # type: ignore[abstract]

    def test_cannot_instantiate_with_config(self):
        """Direct instantiation with config still raises TypeError."""
        with pytest.raises(TypeError):
            BaseConverter(config={"key": "value"})  # type: ignore[abstract]

    def test_partial_implementation_cannot_be_instantiated(self):
        """A subclass missing one abstract method cannot be instantiated."""
        class PartialConverter(BaseConverter):
            @property
            def source_format(self) -> ConversionFormat:
                return ConversionFormat.YAML

            @property
            def target_format(self) -> ConversionFormat:
                return ConversionFormat.DAX

            def convert(self, input_data: Any, **kwargs) -> Any:
                return input_data

            # validate_input is intentionally NOT implemented

        with pytest.raises(TypeError):
            PartialConverter()  # type: ignore[abstract]

    def test_missing_source_format_cannot_be_instantiated(self):
        """Missing source_format property prevents instantiation."""
        class MissingSourceFormat(BaseConverter):
            @property
            def target_format(self) -> ConversionFormat:
                return ConversionFormat.DAX

            def convert(self, input_data: Any, **kwargs) -> Any:
                return input_data

            def validate_input(self, input_data: Any) -> bool:
                return True

        with pytest.raises(TypeError):
            MissingSourceFormat()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# Tests for concrete BaseConverter subclasses
# ---------------------------------------------------------------------------

class TestConcreteConverterInstantiation:
    """Tests that concrete subclasses can be instantiated correctly."""

    def test_instantiation_without_config(self):
        """Concrete converter can be instantiated without config."""
        converter = YamlToDaxConverter()
        assert converter is not None

    def test_config_defaults_to_empty_dict(self):
        """When no config supplied, self.config is an empty dict."""
        converter = YamlToDaxConverter()
        assert converter.config == {}

    def test_instantiation_with_config(self):
        """Concrete converter can be instantiated with a config dict."""
        config = {"output_style": "compact", "indent": 4}
        converter = YamlToDaxConverter(config=config)
        assert converter.config == config

    def test_config_none_treated_as_empty_dict(self):
        """Passing config=None results in an empty dict (not None)."""
        converter = YamlToDaxConverter(config=None)
        assert converter.config == {}

    def test_config_is_stored_by_reference(self):
        """The config dict is stored (not a deep-copied snapshot)."""
        config = {"key": "value"}
        converter = YamlToDaxConverter(config=config)
        config["key"] = "modified"
        assert converter.config["key"] == "modified"

    def test_source_format_property(self):
        """source_format property returns the correct ConversionFormat."""
        converter = YamlToDaxConverter()
        assert converter.source_format == ConversionFormat.YAML

    def test_target_format_property(self):
        """target_format property returns the correct ConversionFormat."""
        converter = YamlToDaxConverter()
        assert converter.target_format == ConversionFormat.DAX

    def test_convert_method_callable(self):
        """convert() method is callable and returns a result."""
        converter = YamlToDaxConverter()
        result = converter.convert("measure: revenue")
        assert result == "DAX(measure: revenue)"

    def test_validate_input_method_callable(self):
        """validate_input() method is callable and returns a bool."""
        converter = YamlToDaxConverter()
        assert converter.validate_input("some data") is True
        assert converter.validate_input(None) is False

    def test_multiple_converter_types_independent(self):
        """Two different concrete converters do not share config state."""
        dax_converter = YamlToDaxConverter(config={"style": "dax"})
        sql_converter = YamlToSqlConverter(config={"style": "sql"})

        assert dax_converter.config == {"style": "dax"}
        assert sql_converter.config == {"style": "sql"}
        assert dax_converter.source_format == ConversionFormat.YAML
        assert sql_converter.source_format == ConversionFormat.YAML
        assert dax_converter.target_format == ConversionFormat.DAX
        assert sql_converter.target_format == ConversionFormat.SQL

    def test_convert_with_kwargs(self):
        """convert() forwards keyword arguments."""
        class KwargsConverter(BaseConverter):
            @property
            def source_format(self) -> ConversionFormat:
                return ConversionFormat.YAML

            @property
            def target_format(self) -> ConversionFormat:
                return ConversionFormat.SQL

            def convert(self, input_data: Any, **kwargs) -> Any:
                return {"data": input_data, "kwargs": kwargs}

            def validate_input(self, input_data: Any) -> bool:
                return True

        converter = KwargsConverter()
        result = converter.convert("data", mode="strict", version=2)
        assert result["kwargs"] == {"mode": "strict", "version": 2}

    def test_different_instances_do_not_share_config(self):
        """Two instances of the same concrete class have independent configs."""
        c1 = YamlToDaxConverter(config={"a": 1})
        c2 = YamlToDaxConverter(config={"b": 2})
        assert c1.config != c2.config
        assert "a" not in c2.config
        assert "b" not in c1.config
