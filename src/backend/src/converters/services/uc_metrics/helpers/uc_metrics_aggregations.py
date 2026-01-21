"""
UC Metrics Aggregation Builders
Provides Spark SQL aggregation support for Unity Catalog Metrics Store
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from ....base.models import KPI

logger = logging.getLogger(__name__)


class UCMetricsAggregationBuilder:
    """Builds Spark SQL aggregation expressions for UC Metrics Store"""

    def __init__(self, dialect: str = "spark"):
        self.dialect = dialect

    def build_measure_expression(self, kpi: KPI) -> str:
        """Build the measure expression based on aggregation type and formula

        Args:
            kpi: KPI with aggregation type and formula

        Returns:
            Spark SQL aggregation expression

        Examples:
            SUM(revenue)
            COUNT(customer_id)
            AVG(price)
        """
        aggregation_type = kpi.aggregation_type.upper() if kpi.aggregation_type else "SUM"
        formula = kpi.formula or "1"

        # Map aggregation types to UC metrics expressions
        if aggregation_type == "SUM":
            return f"SUM({formula})"
        elif aggregation_type == "COUNT":
            return f"COUNT({formula})"
        elif aggregation_type == "DISTINCTCOUNT":
            return f"COUNT(DISTINCT {formula})"
        elif aggregation_type == "AVERAGE":
            return f"AVG({formula})"
        elif aggregation_type == "MIN":
            return f"MIN({formula})"
        elif aggregation_type == "MAX":
            return f"MAX({formula})"
        elif aggregation_type == "CALCULATED":
            # For calculated measures that reference other measures,
            # just return the formula as-is (no aggregation wrapper)
            return formula
        else:
            # Default to SUM for unknown types
            logger.warning(f"Unknown aggregation type: {aggregation_type}, defaulting to SUM")
            return f"SUM({formula})"

    def build_measure_expression_with_filter(
        self,
        kpi: KPI,
        specific_filters: Optional[str]
    ) -> str:
        """Build the measure expression with FILTER clause for specific conditions

        Args:
            kpi: KPI with aggregation configuration
            specific_filters: Optional filter conditions to apply

        Returns:
            Spark SQL expression with optional FILTER clause

        Examples:
            SUM(revenue) FILTER (WHERE region = 'EMEA')
            COUNT(*) FILTER (WHERE status = 'active')
        """
        aggregation_type = kpi.aggregation_type.upper() if kpi.aggregation_type else "SUM"
        formula = kpi.formula or "1"
        display_sign = getattr(kpi, 'display_sign', 1)  # Default to 1 if not specified

        # Handle exceptions by transforming the formula
        exceptions = getattr(kpi, 'exceptions', None)
        if exceptions:
            formula = self.apply_exceptions_to_formula(formula, exceptions)

        # Build base aggregation
        if aggregation_type == "SUM":
            base_expr = f"SUM({formula})"
        elif aggregation_type == "COUNT":
            base_expr = f"COUNT({formula})"
        elif aggregation_type == "DISTINCTCOUNT":
            base_expr = f"COUNT(DISTINCT {formula})"
        elif aggregation_type == "AVERAGE":
            base_expr = f"AVG({formula})"
        elif aggregation_type == "MIN":
            base_expr = f"MIN({formula})"
        elif aggregation_type == "MAX":
            base_expr = f"MAX({formula})"
        elif aggregation_type == "CALCULATED":
            # For calculated measures that reference other measures,
            # just use the formula as-is (no aggregation wrapper)
            base_expr = formula
        else:
            # Default to SUM for unknown types
            logger.warning(f"Unknown aggregation type: {aggregation_type}, defaulting to SUM")
            base_expr = f"SUM({formula})"

        # Add FILTER clause if there are specific filters
        if specific_filters:
            filtered_expr = f"{base_expr} FILTER (WHERE {specific_filters})"
        else:
            filtered_expr = base_expr

        # Apply display_sign if it's -1 (multiply by -1 for negative values)
        if display_sign == -1:
            return f"(-1) * {filtered_expr}"
        else:
            return filtered_expr

    def apply_exceptions_to_formula(self, formula: str, exceptions: List[Dict[str, Any]]) -> str:
        """Apply exception transformations to the formula

        Args:
            formula: Base formula expression
            exceptions: List of exception rules to apply

        Returns:
            Transformed formula with exception handling

        Examples:
            negative_to_zero: CASE WHEN formula < 0 THEN 0 ELSE formula END
            null_to_zero: COALESCE(formula, 0)
            division_by_zero: CASE WHEN denominator = 0 THEN 0 ELSE numerator / denominator END
        """
        transformed_formula = formula

        for exception in exceptions:
            exception_type = exception.get('type', '').lower()

            if exception_type == 'negative_to_zero':
                # Transform: field -> CASE WHEN field < 0 THEN 0 ELSE field END
                transformed_formula = f"CASE WHEN {transformed_formula} < 0 THEN 0 ELSE {transformed_formula} END"

            elif exception_type == 'null_to_zero':
                # Transform: field -> COALESCE(field, 0)
                transformed_formula = f"COALESCE({transformed_formula}, 0)"

            elif exception_type == 'division_by_zero':
                # For division operations, handle division by zero
                if '/' in transformed_formula:
                    # Split on division and wrap denominator with NULL check
                    parts = transformed_formula.split('/')
                    if len(parts) == 2:
                        numerator = parts[0].strip()
                        denominator = parts[1].strip()
                        transformed_formula = f"CASE WHEN ({denominator}) = 0 THEN 0 ELSE ({numerator}) / ({denominator}) END"

        return transformed_formula

    def build_exception_aggregation_with_window(
        self,
        kpi: KPI,
        specific_filters: Optional[str]
    ) -> Tuple[str, List[Dict[str, str]]]:
        """Build exception aggregation with window configuration

        Used for complex aggregations that require window functions for
        specific exception handling fields (SAP BW exception aggregation pattern).

        Args:
            kpi: KPI with exception aggregation configuration
            specific_filters: Optional filter conditions

        Returns:
            Tuple of (measure_expression, window_config_list)

        Example window config:
            [
                {
                    "order": "fiscal_period",
                    "range": "current",
                    "semiadditive": "last"
                }
            ]
        """
        formula = kpi.formula or "1"
        display_sign = getattr(kpi, 'display_sign', 1)
        exception_agg_type = getattr(kpi, 'exception_aggregation', 'sum').upper()
        exception_fields = getattr(kpi, 'fields_for_exception_aggregation', [])

        # Build the aggregation function for the main expression
        if exception_agg_type == "SUM":
            agg_func = "SUM"
        elif exception_agg_type == "COUNT":
            agg_func = "COUNT"
        elif exception_agg_type == "AVG":
            agg_func = "AVG"
        elif exception_agg_type == "MIN":
            agg_func = "MIN"
        elif exception_agg_type == "MAX":
            agg_func = "MAX"
        else:
            # Default to SUM
            agg_func = "SUM"

        # Format the formula with proper line breaks and indentation
        main_expr = f"""{agg_func}(
          {formula}
        )"""

        # Apply display_sign if it's -1
        if display_sign == -1:
            main_expr = f"(-1) * {main_expr}"

        # Build window configuration based on exception aggregation fields
        window_config = []
        if exception_fields:
            # Create window entries for all exception aggregation fields
            for field in exception_fields:
                window_entry = {
                    "order": field,
                    "range": "current",
                    "semiadditive": "last"
                }
                window_config.append(window_entry)

        return main_expr, window_config

    def build_constant_selection_measure(
        self,
        kpi: KPI,
        kbi_specific_filters: List[str]
    ) -> Tuple[str, List[Dict[str, str]]]:
        """Build measure with constant selection (SAP BW pattern)

        Constant selection fields are used for semi-additive measures where
        aggregation should use the last value in a time period.

        Args:
            kpi: KPI with constant selection configuration
            kbi_specific_filters: KBI-specific filter conditions

        Returns:
            Tuple of (measure_expression, window_config_list)

        Example:
            For inventory with constant_selection on fiscal_period:
            - Takes last inventory value per period
            - Window: {"order": "fiscal_period", "semiadditive": "last", "range": "current"}
        """
        aggregation_type = kpi.aggregation_type.upper() if kpi.aggregation_type else "SUM"
        formula = kpi.formula or "1"
        display_sign = getattr(kpi, 'display_sign', 1)

        # Build base aggregation
        if aggregation_type == "SUM":
            base_expr = f"SUM({formula})"
        elif aggregation_type == "COUNT":
            base_expr = f"COUNT({formula})"
        elif aggregation_type == "AVERAGE":
            base_expr = f"AVG({formula})"
        elif aggregation_type == "MIN":
            base_expr = f"MIN({formula})"
        elif aggregation_type == "MAX":
            base_expr = f"MAX({formula})"
        else:
            base_expr = f"SUM({formula})"

        # Add FILTER clause if there are KBI-specific filters
        if kbi_specific_filters:
            filter_conditions = " AND ".join(kbi_specific_filters)
            measure_expr = f"{base_expr} FILTER (\n            WHERE {filter_conditions}\n          )"
        else:
            measure_expr = base_expr

        # Apply display_sign if it's -1
        if display_sign == -1:
            measure_expr = f"(-1) * {measure_expr}"

        # Build window configuration for constant selection fields
        window_config = []
        for field in kpi.fields_for_constant_selection:
            window_entry = {
                "order": field,
                "semiadditive": "last",
                "range": "current"
            }
            window_config.append(window_entry)

        return measure_expr, window_config


# Convenience function for simple cases
def detect_and_build_aggregation(kpi: KPI) -> str:
    """Detect aggregation type and build appropriate expression

    Args:
        kpi: KPI with aggregation configuration

    Returns:
        Spark SQL aggregation expression

    This is a convenience function that matches the pattern used in
    DAX and SQL converters for simple aggregation building.
    """
    builder = UCMetricsAggregationBuilder()
    return builder.build_measure_expression(kpi)
