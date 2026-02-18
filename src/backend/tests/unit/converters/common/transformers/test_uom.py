"""
Unit tests for converters/common/transformers/uom.py

Tests unit of measure conversion logic for SQL and DAX measure generation.
"""

import pytest
from src.converters.common.transformers.uom import UnitOfMeasureConverter
from src.converters.base.models import KPI


class TestUnitOfMeasureConverter:
    """Tests for UnitOfMeasureConverter class"""

    @pytest.fixture
    def converter(self):
        """Create UnitOfMeasureConverter instance for testing"""
        return UnitOfMeasureConverter()

    @pytest.fixture
    def kpi_with_fixed_uom(self):
        """KPI with fixed source unit of measure"""
        return KPI(
            description="Weight in Pounds",
            technical_name="weight_lb",
            formula="SUM(products.weight)",
            uom_fixed_unit="LB",
            target_uom="KG",
            uom_preset="mass"
        )

    @pytest.fixture
    def kpi_with_dynamic_uom(self):
        """KPI with dynamic UOM from column"""
        return KPI(
            description="Multi-unit Weight",
            technical_name="multi_weight",
            formula="SUM(products.weight)",
            source_table="products",
            uom_column="weight_unit",
            target_uom="KG",
            uom_preset="mass"
        )

    @pytest.fixture
    def kpi_no_uom(self):
        """KPI without UOM conversion"""
        return KPI(
            description="Simple Count",
            technical_name="simple_count",
            formula="COUNT(products.id)"
        )

    # ========== Initialization Tests ==========

    def test_converter_initialization(self, converter):
        """Test UnitOfMeasureConverter initializes with default conversion table"""
        assert converter.uom_conversion_table == "UnitConversions"

    def test_conversion_presets_defined(self, converter):
        """Test conversion presets are defined"""
        assert len(converter.CONVERSION_PRESETS) > 0
        assert "mass" in converter.CONVERSION_PRESETS
        assert "length" in converter.CONVERSION_PRESETS
        assert "volume" in converter.CONVERSION_PRESETS
        assert "temperature" in converter.CONVERSION_PRESETS
        assert "time" in converter.CONVERSION_PRESETS

    def test_mass_preset_structure(self, converter):
        """Test mass preset has correct structure"""
        mass_preset = converter.CONVERSION_PRESETS["mass"]

        assert "base_unit" in mass_preset
        assert mass_preset["base_unit"] == "KG"
        assert "conversions" in mass_preset

        # Check common mass units
        conversions = mass_preset["conversions"]
        assert "KG" in conversions
        assert "G" in conversions
        assert "LB" in conversions
        assert "OZ" in conversions
        assert conversions["KG"] == 1.0  # Base unit

    def test_length_preset_structure(self, converter):
        """Test length preset has correct structure"""
        length_preset = converter.CONVERSION_PRESETS["length"]

        assert length_preset["base_unit"] == "M"
        conversions = length_preset["conversions"]
        assert "M" in conversions
        assert "CM" in conversions
        assert "IN" in conversions
        assert "FT" in conversions

    # ========== should_convert_uom Tests ==========

    def test_should_convert_uom_with_fixed_unit(self, converter, kpi_with_fixed_uom):
        """Test UOM conversion is needed for KPI with fixed unit and target"""
        assert converter.should_convert_uom(kpi_with_fixed_uom) is True

    def test_should_convert_uom_with_dynamic_unit(self, converter, kpi_with_dynamic_uom):
        """Test UOM conversion is needed for KPI with UOM column and target"""
        assert converter.should_convert_uom(kpi_with_dynamic_uom) is True

    def test_should_convert_uom_no_source(self, converter):
        """Test no conversion when source UOM is missing"""
        kpi = KPI(
            description="Weight",
            technical_name="weight",
            formula="SUM(weight)",
            target_uom="KG",  # Has target but no source
            uom_preset="mass"
        )

        assert converter.should_convert_uom(kpi) is False

    def test_should_convert_uom_no_target(self, converter):
        """Test no conversion when target UOM is missing"""
        kpi = KPI(
            description="Weight",
            technical_name="weight",
            formula="SUM(weight)",
            uom_fixed_unit="LB"  # Has source but no target
        )

        assert converter.should_convert_uom(kpi) is False

    def test_should_convert_uom_no_preset(self, converter):
        """Test no conversion when preset is missing"""
        kpi = KPI(
            description="Weight",
            technical_name="weight",
            formula="SUM(weight)",
            uom_fixed_unit="LB",
            target_uom="KG"  # Has source and target but no preset
        )

        assert converter.should_convert_uom(kpi) is False

    def test_should_convert_uom_no_uom_info(self, converter, kpi_no_uom):
        """Test no conversion when no UOM information"""
        assert converter.should_convert_uom(kpi_no_uom) is False

    # ========== get_kbi_uom_recursive Tests ==========

    def test_get_kbi_uom_recursive_fixed(self, converter, kpi_with_fixed_uom):
        """Test getting fixed UOM from KPI"""
        uom_type, uom_value = converter.get_kbi_uom_recursive(kpi_with_fixed_uom)

        assert uom_type == "fixed"
        assert uom_value == "LB"

    def test_get_kbi_uom_recursive_dynamic(self, converter, kpi_with_dynamic_uom):
        """Test getting dynamic UOM from KPI"""
        uom_type, uom_value = converter.get_kbi_uom_recursive(kpi_with_dynamic_uom)

        assert uom_type == "dynamic"
        assert uom_value == "weight_unit"

    def test_get_kbi_uom_recursive_no_uom(self, converter, kpi_no_uom):
        """Test getting UOM from KPI with no UOM info"""
        uom_type, uom_value = converter.get_kbi_uom_recursive(kpi_no_uom)

        assert uom_type is None
        assert uom_value is None

    def test_get_kbi_uom_recursive_from_dependency(self, converter):
        """Test getting UOM from KPI formula dependency"""
        # Create base KPI with UOM
        base_kpi = KPI(
            description="Base Weight",
            technical_name="base_weight",
            formula="SUM(products.weight)",
            uom_fixed_unit="LB",
            target_uom="KG",
            uom_preset="mass"
        )

        # Create derived KPI that references base KPI
        derived_kpi = KPI(
            description="Derived Weight",
            technical_name="derived_weight",
            formula="[base_weight] * 1.1"  # References base_weight
            # No direct UOM info
        )

        kpi_lookup = {
            "base_weight": base_kpi
        }

        uom_type, uom_value = converter.get_kbi_uom_recursive(derived_kpi, kpi_lookup)

        # Should find UOM from dependency
        assert uom_type == "fixed"
        assert uom_value == "LB"

    def test_get_kbi_uom_recursive_priority_direct_over_dependency(self, converter):
        """Test direct UOM info takes priority over dependencies"""
        base_kpi = KPI(
            description="Base",
            technical_name="base",
            formula="SUM(amount)",
            uom_fixed_unit="LB"
        )

        # This KPI has both direct UOM and a dependency with different UOM
        kpi = KPI(
            description="Derived",
            technical_name="derived",
            formula="[base] * 2",
            uom_column="my_uom"  # Direct dynamic UOM
        )

        kpi_lookup = {"base": base_kpi}

        uom_type, uom_value = converter.get_kbi_uom_recursive(kpi, kpi_lookup)

        # Should use direct uom_column, not dependency
        assert uom_type == "dynamic"
        assert uom_value == "my_uom"

    # ========== get_conversion_factor Tests ==========

    def test_get_conversion_factor_mass_lb_to_kg(self, converter):
        """Test getting conversion factor from pounds to kilograms"""
        factor = converter.get_conversion_factor("mass", "LB", "KG")

        assert factor is not None
        assert abs(factor - 0.453592) < 0.0001

    def test_get_conversion_factor_mass_kg_to_g(self, converter):
        """Test getting conversion factor from kilograms to grams"""
        factor = converter.get_conversion_factor("mass", "KG", "G")

        assert factor is not None
        assert abs(factor - 1000.0) < 0.01

    def test_get_conversion_factor_length_in_to_m(self, converter):
        """Test getting conversion factor from inches to meters"""
        factor = converter.get_conversion_factor("length", "IN", "M")

        assert factor is not None
        assert abs(factor - 0.0254) < 0.0001

    def test_get_conversion_factor_same_unit(self, converter):
        """Test conversion factor for same unit is 1.0"""
        factor = converter.get_conversion_factor("mass", "KG", "KG")

        assert factor == 1.0

    def test_get_conversion_factor_invalid_preset(self, converter):
        """Test invalid preset returns None"""
        factor = converter.get_conversion_factor("invalid_preset", "KG", "G")

        assert factor is None

    def test_get_conversion_factor_invalid_source_unit(self, converter):
        """Test invalid source unit returns None"""
        factor = converter.get_conversion_factor("mass", "INVALID", "KG")

        assert factor is None

    def test_get_conversion_factor_invalid_target_unit(self, converter):
        """Test invalid target unit returns None"""
        factor = converter.get_conversion_factor("mass", "KG", "INVALID")

        assert factor is None

    # ========== generate_sql_conversion Tests ==========

    def test_generate_sql_conversion_fixed_uom(self, converter):
        """Test SQL generation for fixed UOM conversion"""
        sql = converter.generate_sql_conversion(
            value_expression="weight_value",
            preset="mass",
            source_unit="LB",
            target_unit="KG",
            uom_type="fixed"
        )

        assert "weight_value" in sql
        assert "*" in sql
        assert "0.453592" in sql

    def test_generate_sql_conversion_fixed_same_unit(self, converter):
        """Test SQL for fixed UOM with same source and target"""
        sql = converter.generate_sql_conversion(
            value_expression="weight_value",
            preset="mass",
            source_unit="KG",
            target_unit="KG",
            uom_type="fixed"
        )

        # Should return original expression without conversion
        assert sql == "weight_value"

    def test_generate_sql_conversion_dynamic_uom(self, converter):
        """Test SQL generation for dynamic UOM conversion"""
        sql = converter.generate_sql_conversion(
            value_expression="weight_value",
            preset="mass",
            source_unit=None,
            target_unit="KG",
            uom_type="dynamic",
            uom_column="weight_unit"
        )

        # Dynamic conversion returns a CASE statement
        assert "CASE" in sql
        assert "weight_unit" in sql
        assert "KG" in sql
        assert "WHEN" in sql
        assert "weight_value" in sql

    def test_generate_sql_conversion_dynamic_includes_all_units(self, converter):
        """Test dynamic SQL includes all units from preset"""
        sql = converter.generate_sql_conversion(
            value_expression="amount",
            preset="mass",
            source_unit=None,
            target_unit="KG",
            uom_type="dynamic",
            uom_column="unit_col"
        )

        # Should include common mass units
        assert "LB" in sql
        assert "G" in sql
        assert "T" in sql

    def test_generate_sql_conversion_invalid_preset(self, converter):
        """Test SQL for invalid preset returns original expression"""
        sql = converter.generate_sql_conversion(
            value_expression="value",
            preset="invalid",
            source_unit="X",
            target_unit="Y",
            uom_type="fixed"
        )

        assert sql == "value"

    # ========== generate_dax_conversion Tests ==========

    def test_generate_dax_conversion_fixed_uom(self, converter):
        """Test DAX generation for fixed UOM conversion"""
        dax = converter.generate_dax_conversion(
            value_expression="[Weight]",
            preset="mass",
            source_unit="LB",
            target_unit="KG",
            uom_type="fixed"
        )

        assert "[Weight]" in dax
        assert "*" in dax
        assert "0.453592" in dax

    def test_generate_dax_conversion_fixed_same_unit(self, converter):
        """Test DAX for fixed UOM with same source and target"""
        dax = converter.generate_dax_conversion(
            value_expression="[Weight]",
            preset="mass",
            source_unit="KG",
            target_unit="KG",
            uom_type="fixed"
        )

        # Should return original expression without conversion
        assert dax == "[Weight]"

    def test_generate_dax_conversion_dynamic_uom(self, converter):
        """Test DAX generation for dynamic UOM conversion"""
        dax = converter.generate_dax_conversion(
            value_expression="[Weight]",
            preset="mass",
            source_unit=None,
            target_unit="KG",
            uom_type="dynamic",
            uom_column="WeightUnit"
        )

        # Dynamic conversion returns a SWITCH statement
        assert "SWITCH" in dax
        assert "[WeightUnit]" in dax
        assert "[Weight]" in dax

    def test_generate_dax_conversion_dynamic_includes_units(self, converter):
        """Test dynamic DAX includes unit cases"""
        dax = converter.generate_dax_conversion(
            value_expression="[Amount]",
            preset="mass",
            source_unit=None,
            target_unit="KG",
            uom_type="dynamic",
            uom_column="Unit"
        )

        # Should include common mass units
        assert '"LB"' in dax or "'LB'" in dax
        assert '"G"' in dax or "'G'" in dax

    # ========== get_supported_units Tests ==========

    def test_get_supported_units_mass(self, converter):
        """Test getting supported units for mass preset"""
        units = converter.get_supported_units("mass")

        assert len(units) > 0
        assert "KG" in units
        assert "G" in units
        assert "LB" in units
        assert "OZ" in units

    def test_get_supported_units_length(self, converter):
        """Test getting supported units for length preset"""
        units = converter.get_supported_units("length")

        assert len(units) > 0
        assert "M" in units
        assert "CM" in units
        assert "IN" in units
        assert "FT" in units

    def test_get_supported_units_volume(self, converter):
        """Test getting supported units for volume preset"""
        units = converter.get_supported_units("volume")

        assert len(units) > 0
        assert "L" in units
        assert "ML" in units
        assert "GAL" in units

    def test_get_supported_units_invalid_preset(self, converter):
        """Test invalid preset returns empty list"""
        units = converter.get_supported_units("invalid_preset")

        assert units == []

    # ========== Integration Tests ==========

    def test_full_conversion_workflow_fixed(self, converter, kpi_with_fixed_uom):
        """Test complete workflow for fixed UOM conversion"""
        # Check if conversion needed
        assert converter.should_convert_uom(kpi_with_fixed_uom) is True

        # Get UOM info
        uom_type, uom_value = converter.get_kbi_uom_recursive(kpi_with_fixed_uom)
        assert uom_type == "fixed"
        assert uom_value == "LB"

        # Generate SQL
        sql = converter.generate_sql_conversion(
            "SUM(weight)",
            kpi_with_fixed_uom.uom_preset,
            uom_value,
            kpi_with_fixed_uom.target_uom,
            uom_type
        )
        assert "LB" in sql or "0.453592" in sql

        # Generate DAX
        dax = converter.generate_dax_conversion(
            "[Weight]",
            kpi_with_fixed_uom.uom_preset,
            uom_value,
            kpi_with_fixed_uom.target_uom,
            uom_type
        )
        assert "0.453592" in dax

    def test_full_conversion_workflow_dynamic(self, converter, kpi_with_dynamic_uom):
        """Test complete workflow for dynamic UOM conversion"""
        # Check if conversion needed
        assert converter.should_convert_uom(kpi_with_dynamic_uom) is True

        # Get UOM info
        uom_type, uom_value = converter.get_kbi_uom_recursive(kpi_with_dynamic_uom)
        assert uom_type == "dynamic"
        assert uom_value == "weight_unit"

        # Generate SQL
        sql = converter.generate_sql_conversion(
            "SUM(weight)",
            kpi_with_dynamic_uom.uom_preset,
            None,
            kpi_with_dynamic_uom.target_uom,
            uom_type,
            uom_column=uom_value
        )
        assert "CASE" in sql
        assert "weight_unit" in sql

        # Generate DAX
        dax = converter.generate_dax_conversion(
            "[Weight]",
            kpi_with_dynamic_uom.uom_preset,
            None,
            kpi_with_dynamic_uom.target_uom,
            uom_type,
            uom_column=uom_value
        )
        assert "SWITCH" in dax
        assert "weight_unit" in dax or "WeightUnit" in dax

    def test_conversion_factor_reversibility(self, converter):
        """Test conversion factors are reversible"""
        # LB to KG
        lb_to_kg = converter.get_conversion_factor("mass", "LB", "KG")
        # KG to LB
        kg_to_lb = converter.get_conversion_factor("mass", "KG", "LB")

        # Should be reciprocals
        assert abs(lb_to_kg * kg_to_lb - 1.0) < 0.0001
