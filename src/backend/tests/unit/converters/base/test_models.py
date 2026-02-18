"""
Unit tests for converter base data models.

Tests Pydantic models used for KPI conversion including:
- KPIFilter
- Structure
- KPI
- QueryFilter
- KPIDefinition
- DAXMeasure
- SQLMeasure
- UCMetricsMeasure
"""

import pytest
from pydantic import ValidationError

from src.converters.base.models import (
    KPIFilter,
    Structure,
    KPI,
    QueryFilter,
    KPIDefinition,
    DAXMeasure,
)


class TestKPIFilter:
    """Tests for KPIFilter model"""

    def test_create_kpi_filter_with_required_fields(self):
        """Test creating KPIFilter with required fields"""
        filter_obj = KPIFilter(
            field="Region",
            operator="=",
            value="West"
        )

        assert filter_obj.field == "Region"
        assert filter_obj.operator == "="
        assert filter_obj.value == "West"
        assert filter_obj.logical_operator == "AND"  # Default value

    def test_create_kpi_filter_with_custom_logical_operator(self):
        """Test KPIFilter with custom logical operator"""
        filter_obj = KPIFilter(
            field="Status",
            operator="IN",
            value=["Active", "Pending"],
            logical_operator="OR"
        )

        assert filter_obj.logical_operator == "OR"

    def test_kpi_filter_missing_required_field(self):
        """Test KPIFilter validation fails without required fields"""
        with pytest.raises(ValidationError):
            KPIFilter(operator="=", value="Test")  # Missing 'field'


class TestStructure:
    """Tests for Structure model"""

    def test_create_structure_minimal(self):
        """Test creating Structure with minimal fields"""
        struct = Structure(description="Year to Date")

        assert struct.description == "Year to Date"
        assert struct.formula is None
        assert struct.filters == []
        assert struct.display_sign == 1

    def test_create_structure_full(self):
        """Test creating Structure with all fields"""
        struct = Structure(
            description="Previous Year Comparison",
            formula="CALCULATE([Measure], SAMEPERIODLASTYEAR(Calendar[Date]))",
            filters=["Calendar[Year] = 2023"],
            display_sign=-1,
            technical_name="PY_Comparison",
            aggregation_type="SUM",
            variables={"year_offset": -1}
        )

        assert struct.description == "Previous Year Comparison"
        assert struct.formula is not None
        assert len(struct.filters) == 1
        assert struct.display_sign == -1
        assert struct.variables["year_offset"] == -1

    def test_structure_filters_with_dict(self):
        """Test Structure accepts dict filters"""
        struct = Structure(
            description="Filtered Structure",
            filters=[{"field": "Region", "operator": "=", "value": "West"}]
        )

        assert len(struct.filters) == 1
        assert isinstance(struct.filters[0], dict)


class TestKPI:
    """Tests for KPI model"""

    def test_create_kpi_minimal(self):
        """Test creating KPI with minimal required fields"""
        kpi = KPI(
            description="Total Sales",
            formula="SUM(Sales[Amount])"
        )

        assert kpi.description == "Total Sales"
        assert kpi.formula == "SUM(Sales[Amount])"
        assert kpi.filters == []
        assert kpi.display_sign == 1

    def test_create_kpi_full(self):
        """Test creating KPI with all fields"""
        kpi = KPI(
            description="Weighted Average Price",
            formula="SUM(Sales[Amount]) / SUM(Sales[Quantity])",
            technical_name="Avg_Price",
            source_table="Sales",
            aggregation_type="AVERAGE",
            weight_column="Quantity",
            target_column="Amount",
            filters=["Region = 'West'"],
            display_sign=1,
            currency_column="Currency",
            target_currency="USD",
            uom_column="Unit",
            target_uom="KG"
        )

        assert kpi.technical_name == "Avg_Price"
        assert kpi.source_table == "Sales"
        assert kpi.aggregation_type == "AVERAGE"
        assert kpi.currency_column == "Currency"
        assert kpi.target_currency == "USD"

    def test_kpi_currency_conversion_fields(self):
        """Test KPI currency conversion configuration"""
        kpi = KPI(
            description="Sales in USD",
            formula="SUM(Sales[Amount])",
            currency_column="SourceCurrency",
            target_currency="USD"
        )

        assert kpi.currency_column == "SourceCurrency"
        assert kpi.target_currency == "USD"
        assert kpi.fixed_currency is None

    def test_kpi_fixed_currency(self):
        """Test KPI with fixed source currency"""
        kpi = KPI(
            description="EUR Sales",
            formula="SUM(Sales[Amount])",
            fixed_currency="EUR",
            target_currency="USD"
        )

        assert kpi.fixed_currency == "EUR"
        assert kpi.target_currency == "USD"

    def test_kpi_uom_conversion_fields(self):
        """Test KPI unit of measure conversion"""
        kpi = KPI(
            description="Weight in KG",
            formula="SUM(Inventory[Weight])",
            uom_column="WeightUnit",
            target_uom="KG",
            uom_preset="mass"
        )

        assert kpi.uom_column == "WeightUnit"
        assert kpi.target_uom == "KG"
        assert kpi.uom_preset == "mass"

    def test_kpi_structure_application(self):
        """Test KPI can reference structures"""
        kpi = KPI(
            description="Sales YTD",
            formula="SUM(Sales[Amount])",
            apply_structures=["YTD", "PY_Comparison"]
        )

        assert len(kpi.apply_structures) == 2
        assert "YTD" in kpi.apply_structures

    def test_kpi_exceptions_handling(self):
        """Test KPI with exception aggregation"""
        kpi = KPI(
            description="Special Aggregation",
            formula="SUM(Sales[Amount])",
            exceptions=[{"field": "Category", "values": ["Electronics", "Toys"]}],
            exception_aggregation="AVERAGE",
            fields_for_exception_aggregation=["Category", "Region"]
        )

        assert len(kpi.exceptions) == 1
        assert kpi.exception_aggregation == "AVERAGE"
        assert len(kpi.fields_for_exception_aggregation) == 2

    def test_kpi_alias_for_filter(self):
        """Test KPI accepts 'filter' as alias for 'filters'"""
        kpi = KPI(
            description="Filtered Sales",
            formula="SUM(Sales[Amount])",
            filter=["Region = 'West'"]  # Using alias
        )

        assert len(kpi.filters) == 1

    def test_kpi_missing_required_fields(self):
        """Test KPI validation fails without required fields"""
        with pytest.raises(ValidationError):
            KPI(formula="SUM(Sales[Amount])")  # Missing description


class TestQueryFilter:
    """Tests for QueryFilter model"""

    def test_create_query_filter(self):
        """Test creating QueryFilter"""
        qf = QueryFilter(
            name="ActiveOnly",
            expression="Status = 'Active'"
        )

        assert qf.name == "ActiveOnly"
        assert qf.expression == "Status = 'Active'"

    def test_query_filter_missing_fields(self):
        """Test QueryFilter validation"""
        with pytest.raises(ValidationError):
            QueryFilter(name="OnlyName")  # Missing expression


class TestKPIDefinition:
    """Tests for KPIDefinition model"""

    def test_create_kpi_definition_minimal(self):
        """Test creating KPIDefinition with minimal fields"""
        kpi_def = KPIDefinition(
            description="Sales Metrics",
            technical_name="sales_metrics",
            kpis=[
                KPI(description="Total Sales", formula="SUM(Sales[Amount])")
            ]
        )

        assert kpi_def.description == "Sales Metrics"
        assert kpi_def.technical_name == "sales_metrics"
        assert len(kpi_def.kpis) == 1
        assert kpi_def.default_variables == {}
        assert kpi_def.query_filters == []

    def test_create_kpi_definition_full(self):
        """Test creating KPIDefinition with all fields"""
        kpi_def = KPIDefinition(
            description="Comprehensive Sales Analysis",
            technical_name="sales_analysis",
            default_variables={"fiscal_year": 2024, "region": "Global"},
            query_filters=[
                QueryFilter(name="CurrentYear", expression="Year = 2024")
            ],
            filters={
                "time_filters": {
                    "current_year": "Year = 2024",
                    "current_quarter": "Quarter = 'Q1'"
                }
            },
            structures={
                "YTD": Structure(
                    description="Year to Date",
                    formula="CALCULATE([Measure], DATESYTD(Calendar[Date]))"
                )
            },
            kpis=[
                KPI(description="Total Sales", formula="SUM(Sales[Amount])"),
                KPI(description="Total Cost", formula="SUM(Cost[Amount])")
            ]
        )

        assert kpi_def.default_variables["fiscal_year"] == 2024
        assert len(kpi_def.query_filters) == 1
        assert kpi_def.filters is not None
        assert "YTD" in kpi_def.structures
        assert len(kpi_def.kpis) == 2

    def test_get_expanded_filters_with_nested_structure(self):
        """Test get_expanded_filters method with nested filters"""
        kpi_def = KPIDefinition(
            description="Test",
            technical_name="test",
            filters={
                "time_filters": {
                    "current_year": "Year = 2024",
                    "current_month": "Month = 'January'"
                },
                "location_filters": {
                    "region": "Region = 'West'"
                }
            },
            kpis=[]
        )

        expanded = kpi_def.get_expanded_filters()

        assert len(expanded) == 3
        assert expanded["current_year"] == "Year = 2024"
        assert expanded["current_month"] == "Month = 'January'"
        assert expanded["region"] == "Region = 'West'"

    def test_get_expanded_filters_empty(self):
        """Test get_expanded_filters with no filters"""
        kpi_def = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[]
        )

        expanded = kpi_def.get_expanded_filters()
        assert expanded == {}

    def test_kpi_definition_with_structures(self):
        """Test KPIDefinition can store multiple structures"""
        kpi_def = KPIDefinition(
            description="Test",
            technical_name="test",
            structures={
                "YTD": Structure(description="Year to Date"),
                "QTD": Structure(description="Quarter to Date"),
                "MTD": Structure(description="Month to Date")
            },
            kpis=[]
        )

        assert len(kpi_def.structures) == 3
        assert "YTD" in kpi_def.structures
        assert "QTD" in kpi_def.structures

    def test_kpi_definition_empty_kpis_list(self):
        """Test KPIDefinition with empty KPIs list is valid"""
        kpi_def = KPIDefinition(
            description="Empty Definition",
            technical_name="empty",
            kpis=[]
        )

        assert len(kpi_def.kpis) == 0

    def test_kpi_definition_missing_required_fields(self):
        """Test KPIDefinition validation"""
        with pytest.raises(ValidationError):
            KPIDefinition(technical_name="test")  # Missing description and kpis


class TestDAXMeasure:
    """Tests for DAXMeasure output model"""

    def test_create_dax_measure_minimal(self):
        """Test creating DAXMeasure with minimal fields"""
        dax = DAXMeasure(
            name="Total Sales",
            description="Sum of all sales",
            dax_formula="SUM(Sales[Amount])"
        )

        assert dax.name == "Total Sales"
        assert dax.description == "Sum of all sales"
        assert dax.dax_formula == "SUM(Sales[Amount])"
        assert dax.original_kbi is None

    def test_create_dax_measure_full(self):
        """Test creating DAXMeasure with all fields"""
        original_kpi = KPI(
            description="Sales Metric",
            formula="SUM(Sales[Amount])"
        )

        dax = DAXMeasure(
            name="Total Sales",
            description="Sum of all sales",
            dax_formula="SUM(Sales[Amount])",
            original_kbi=original_kpi,
            format_string="#,##0.00",
            display_folder="Sales Metrics"
        )

        assert dax.original_kbi is not None
        assert dax.format_string == "#,##0.00"
        assert dax.display_folder == "Sales Metrics"

    def test_dax_measure_table_attribute(self):
        """Test DAXMeasure table attribute if exists"""
        dax = DAXMeasure(
            name="Sales",
            description="Sales measure",
            dax_formula="SUM(Sales[Amount])",
            table="Sales"
        )

        assert dax.table == "Sales"


class TestModelInteroperability:
    """Test how models work together"""

    def test_kpi_definition_with_complex_kpi(self):
        """Test KPIDefinition containing complex KPI with all features"""
        complex_kpi = KPI(
            description="Advanced Sales Metric",
            formula="SUM(Sales[Amount])",
            technical_name="advanced_sales",
            source_table="Sales",
            filters=["Region = 'West'", {"field": "Status", "operator": "=", "value": "Active"}],
            currency_column="Currency",
            target_currency="USD",
            uom_column="Unit",
            target_uom="KG",
            apply_structures=["YTD", "QTD"],
            exceptions=[{"category": "Electronics"}],
            exception_aggregation="AVERAGE"
        )

        kpi_def = KPIDefinition(
            description="Complete Definition",
            technical_name="complete",
            structures={
                "YTD": Structure(description="Year to Date"),
                "QTD": Structure(description="Quarter to Date")
            },
            kpis=[complex_kpi]
        )

        assert len(kpi_def.kpis) == 1
        assert kpi_def.kpis[0].technical_name == "advanced_sales"
        assert len(kpi_def.kpis[0].apply_structures) == 2
        assert kpi_def.kpis[0].target_currency == "USD"

    def test_serialize_and_deserialize_kpi_definition(self):
        """Test KPIDefinition can be serialized and deserialized"""
        original = KPIDefinition(
            description="Test Definition",
            technical_name="test",
            kpis=[
                KPI(description="KPI 1", formula="SUM(A)"),
                KPI(description="KPI 2", formula="SUM(B)")
            ]
        )

        # Serialize to dict
        dict_repr = original.model_dump()

        # Deserialize back
        restored = KPIDefinition(**dict_repr)

        assert restored.technical_name == original.technical_name
        assert len(restored.kpis) == len(original.kpis)
        assert restored.kpis[0].formula == original.kpis[0].formula
