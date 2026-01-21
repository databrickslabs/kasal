"""Unit of Measure (UOM) conversion logic for measure converters

Generates SQL/DAX code for unit of measure conversion based on KPI configuration.
Supports both fixed and dynamic UOM sources with predefined conversion presets.
"""

from typing import Optional, Tuple, List, Dict
from ...base.models import KPI


class UnitOfMeasureConverter:
    """
    Generates unit of measure conversion SQL/DAX code for measures.

    Supports two types of UOM conversion:
    1. Fixed UOM: Source unit is specified in KPI definition (e.g., "KG")
    2. Dynamic UOM: Source unit comes from a column in the data

    Conversion presets define the unit category (mass, length, volume, etc.)
    """

    # Standard UOM conversion presets with conversion factors to base units
    CONVERSION_PRESETS = {
        "mass": {
            "base_unit": "KG",
            "conversions": {
                "KG": 1.0,
                "G": 0.001,
                "MG": 0.000001,
                "T": 1000.0,  # Metric ton
                "LB": 0.453592,  # Pound
                "OZ": 0.0283495,  # Ounce
                "TON": 907.185,  # US ton
            }
        },
        "length": {
            "base_unit": "M",
            "conversions": {
                "M": 1.0,
                "CM": 0.01,
                "MM": 0.001,
                "KM": 1000.0,
                "IN": 0.0254,  # Inch
                "FT": 0.3048,  # Foot
                "YD": 0.9144,  # Yard
                "MI": 1609.34,  # Mile
            }
        },
        "volume": {
            "base_unit": "L",
            "conversions": {
                "L": 1.0,
                "ML": 0.001,
                "CL": 0.01,
                "DL": 0.1,
                "M3": 1000.0,  # Cubic meter
                "GAL": 3.78541,  # US Gallon
                "QT": 0.946353,  # Quart
                "PT": 0.473176,  # Pint
                "FL_OZ": 0.0295735,  # Fluid ounce
            }
        },
        "temperature": {
            "base_unit": "C",
            "conversions": {
                "C": 1.0,  # Celsius (base)
                # Note: Temperature requires offset conversion, not just multiplication
                # Implemented separately in conversion logic
            }
        },
        "time": {
            "base_unit": "S",
            "conversions": {
                "S": 1.0,  # Second
                "MIN": 60.0,  # Minute
                "H": 3600.0,  # Hour
                "D": 86400.0,  # Day
                "W": 604800.0,  # Week
            }
        }
    }

    def __init__(self):
        self.uom_conversion_table = "UnitConversions"  # Default UOM conversion table

    def get_kbi_uom_recursive(self, kbi: KPI, kpi_lookup: Optional[dict] = None) -> Tuple[Optional[str], Optional[str]]:
        """
        Get source unit of measure for given KPI by checking all dependencies.

        Recursively searches through KPI formula dependencies to find UOM information.

        Args:
            kbi: KPI to check for UOM information
            kpi_lookup: Dictionary mapping KPI names to KPI objects (for dependency resolution)

        Returns:
            Tuple[uom_type, uom_value]:
                - uom_type: "fixed", "dynamic", or None
                - uom_value: Unit code (fixed) or column name (dynamic)

        Examples:
            ("fixed", "KG") - All values in kilograms
            ("dynamic", "source_uom") - UOM per row in column
            (None, None) - No UOM conversion needed
        """
        # Check if this KPI has UOM information
        if kbi.uom_column:
            return "dynamic", kbi.uom_column

        if kbi.uom_fixed_unit:
            return "fixed", kbi.uom_fixed_unit

        # If no UOM info and we have a lookup, check formula dependencies
        if kpi_lookup and kbi.formula:
            # Extract KBI references from formula (pattern: [KBI_NAME])
            import re
            kbi_refs = re.findall(r'\[([^\]]+)\]', kbi.formula)

            for kbi_name in kbi_refs:
                if kbi_name in kpi_lookup:
                    child_kbi = kpi_lookup[kbi_name]
                    uom_type, uom_value = self.get_kbi_uom_recursive(child_kbi, kpi_lookup)
                    if uom_type:
                        return uom_type, uom_value

        return None, None

    def get_conversion_factor(self, preset: str, source_unit: str, target_unit: str) -> Optional[float]:
        """
        Get conversion factor between two units in the same preset.

        Args:
            preset: Conversion preset name (e.g., "mass", "length")
            source_unit: Source unit code
            target_unit: Target unit code

        Returns:
            Conversion factor to multiply by, or None if not found

        Examples:
            get_conversion_factor("mass", "LB", "KG") -> 0.453592
            get_conversion_factor("length", "IN", "M") -> 0.0254
        """
        if preset not in self.CONVERSION_PRESETS:
            return None

        preset_data = self.CONVERSION_PRESETS[preset]
        conversions = preset_data["conversions"]

        if source_unit not in conversions or target_unit not in conversions:
            return None

        # Convert to base unit then to target unit
        source_to_base = conversions[source_unit]
        target_to_base = conversions[target_unit]

        return source_to_base / target_to_base

    def generate_sql_conversion(
        self,
        value_expression: str,
        preset: str,
        source_unit: str,
        target_unit: str,
        uom_type: str = "fixed",
        uom_column: Optional[str] = None
    ) -> str:
        """
        Generate SQL code for unit of measure conversion.

        Args:
            value_expression: SQL expression for the value to convert
            preset: UOM preset type (e.g., "mass", "length")
            source_unit: Source unit code (if fixed) or None
            target_unit: Target unit code
            uom_type: "fixed" or "dynamic"
            uom_column: Column name containing UOM (if dynamic)

        Returns:
            SQL expression for converted value

        Examples:
            Fixed: "value * 0.453592" (LB to KG)
            Dynamic: "value * CASE WHEN source_uom='LB' THEN 0.453592 ... END"
        """
        if uom_type == "fixed":
            # Fixed UOM: simple multiplication with conversion factor
            factor = self.get_conversion_factor(preset, source_unit, target_unit)
            if factor is None:
                return value_expression  # No conversion available

            if factor == 1.0:
                return value_expression  # No conversion needed

            return f"({value_expression} * {factor})"

        else:  # dynamic
            # Dynamic UOM: CASE statement for multiple possible source units
            if preset not in self.CONVERSION_PRESETS:
                return value_expression

            conversions = self.CONVERSION_PRESETS[preset]["conversions"]
            cases = []

            for unit_code in conversions.keys():
                factor = self.get_conversion_factor(preset, unit_code, target_unit)
                if factor is not None and factor != 1.0:
                    cases.append(f"        WHEN {uom_column} = '{unit_code}' THEN {value_expression} * {factor}")
                elif factor == 1.0:
                    cases.append(f"        WHEN {uom_column} = '{unit_code}' THEN {value_expression}")

            if not cases:
                return value_expression

            return f"""(CASE
{chr(10).join(cases)}
        ELSE {value_expression}
    END)"""

    def generate_dax_conversion(
        self,
        value_expression: str,
        preset: str,
        source_unit: str,
        target_unit: str,
        uom_type: str = "fixed",
        uom_column: Optional[str] = None
    ) -> str:
        """
        Generate DAX code for unit of measure conversion.

        Args:
            value_expression: DAX expression for the value to convert
            preset: UOM preset type (e.g., "mass", "length")
            source_unit: Source unit code (if fixed) or None
            target_unit: Target unit code
            uom_type: "fixed" or "dynamic"
            uom_column: Column name containing UOM (if dynamic)

        Returns:
            DAX expression for converted value

        Examples:
            Fixed: "value * 0.453592" (LB to KG)
            Dynamic: "value * SWITCH([source_uom], 'LB', 0.453592, ...)"
        """
        if uom_type == "fixed":
            # Fixed UOM: simple multiplication with conversion factor
            factor = self.get_conversion_factor(preset, source_unit, target_unit)
            if factor is None:
                return value_expression  # No conversion available

            if factor == 1.0:
                return value_expression  # No conversion needed

            return f"({value_expression} * {factor})"

        else:  # dynamic
            # Dynamic UOM: SWITCH for multiple possible source units
            if preset not in self.CONVERSION_PRESETS:
                return value_expression

            conversions = self.CONVERSION_PRESETS[preset]["conversions"]
            switch_cases = []

            for unit_code in conversions.keys():
                factor = self.get_conversion_factor(preset, unit_code, target_unit)
                if factor is not None:
                    if factor == 1.0:
                        switch_cases.append(f'        "{unit_code}", {value_expression}')
                    else:
                        switch_cases.append(f'        "{unit_code}", {value_expression} * {factor}')

            if not switch_cases:
                return value_expression

            return f"""SWITCH(
        [{uom_column}],
{chr(10).join(switch_cases)},
        {value_expression}
    )"""

    def should_convert_uom(self, kbi: KPI) -> bool:
        """
        Check if UOM conversion is needed for this KPI.

        Args:
            kbi: KPI to check

        Returns:
            True if UOM conversion should be applied
        """
        # Need both a source, a target, and a preset
        has_source = bool(kbi.uom_column or kbi.uom_fixed_unit)
        has_target = bool(kbi.target_uom)
        has_preset = bool(kbi.uom_preset)

        return has_source and has_target and has_preset

    def get_supported_units(self, preset: str) -> List[str]:
        """
        Get list of supported units for a given preset.

        Args:
            preset: Conversion preset name

        Returns:
            List of supported unit codes
        """
        if preset not in self.CONVERSION_PRESETS:
            return []

        return list(self.CONVERSION_PRESETS[preset]["conversions"].keys())
