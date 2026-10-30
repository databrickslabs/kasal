"""
Unit tests for converters/common/translators/formula.py

Tests formula translation logic for converting SAP BW formulas to DAX components.
"""

import pytest

from src.services.converters.base.models import KPI, KPIDefinition
from src.services.converters.common.translators.formula import FormulaTranslator


class TestFormulaTranslator:
    """Tests for FormulaTranslator class"""

    @pytest.fixture
    def translator(self):
        """Create FormulaTranslator instance for testing"""
        return FormulaTranslator()

    @pytest.fixture
    def simple_definition(self):
        """Simple KPI definition for testing"""
        return KPIDefinition(
            description="Sales Metrics", technical_name="sales_metrics", kpis=[]
        )

    @pytest.fixture
    def kpi_volume_field(self):
        """KPI with volume field"""
        return KPI(
            description="Total Volume",
            technical_name="total_volume",
            formula="bic_kvolume_c",
        )

    @pytest.fixture
    def kpi_amount_field(self):
        """KPI with amount field"""
        return KPI(
            description="Total Amount",
            technical_name="total_amount",
            formula="bic_kamount_c",
        )

    @pytest.fixture
    def kpi_with_source_table(self):
        """KPI with explicit source_table"""
        return KPI(
            description="Sales Revenue",
            technical_name="sales_revenue",
            formula="bic_revenue",
            source_table="FactSales",
        )

    @pytest.fixture
    def kpi_count_field(self):
        """KPI with count field"""
        return KPI(
            description="Order Count",
            technical_name="order_count",
            formula="bic_order_count",
        )

    @pytest.fixture
    def kpi_no_prefix(self):
        """KPI without bic_ prefix"""
        return KPI(
            description="Simple Metric",
            technical_name="simple_metric",
            formula="quantity",
        )

    # ========== Initialization Tests ==========

    def test_translator_initialization(self, translator):
        """Test FormulaTranslator initializes with aggregation mappings"""
        assert translator.aggregation_mappings is not None
        assert translator.field_pattern is not None
        assert len(translator.aggregation_mappings) > 0

    def test_aggregation_mappings_defined(self, translator):
        """Test aggregation mappings contain expected keywords"""
        assert "volume" in translator.aggregation_mappings
        assert "amount" in translator.aggregation_mappings
        assert "count" in translator.aggregation_mappings
        assert "avg" in translator.aggregation_mappings
        assert translator.aggregation_mappings["volume"] == "SUM"
        assert translator.aggregation_mappings["count"] == "COUNT"

    def test_field_pattern_compilation(self, translator):
        """Test field pattern correctly extracts bic_ fields"""
        test_string = "bic_customer_name and bic_region"
        matches = translator.field_pattern.findall(test_string)
        assert len(matches) == 2
        assert "customer_name" in matches
        assert "region" in matches

    # ========== translate_formula Tests ==========

    # ========== _determine_aggregation Tests ==========

    # ========== _generate_table_name Tests ==========

    # ========== create_measure_name Tests ==========

    def test_create_measure_name_from_description(self, translator, simple_definition):
        """Test measure name creation from description"""
        kpi = KPI(
            description="Total Sales Amount",
            technical_name="total_sales",
            formula="bic_amount",
        )

        result = translator.create_measure_name(kpi, simple_definition)
        assert result == "Total Sales Amount"

    def test_create_measure_name_cleans_special_chars(
        self, translator, simple_definition
    ):
        """Test measure name creation removes special characters"""
        kpi = KPI(
            description="Sales (YTD) - Actual",
            technical_name="sales_ytd",
            formula="bic_amount",
        )

        result = translator.create_measure_name(kpi, simple_definition)
        # Special characters should be removed
        assert "(" not in result
        assert ")" not in result
        assert "-" not in result

    def test_create_measure_name_normalizes_whitespace(
        self, translator, simple_definition
    ):
        """Test measure name creation normalizes whitespace"""
        kpi = KPI(
            description="Total   Sales    Amount",
            technical_name="total_sales",
            formula="bic_amount",
        )

        result = translator.create_measure_name(kpi, simple_definition)
        # Should normalize to single spaces
        assert "  " not in result

    def test_create_measure_name_fallback_technical(
        self, translator, simple_definition
    ):
        """Test measure name falls back to technical_name"""
        kpi = KPI(
            description="",  # Empty description to test fallback
            technical_name="total_sales",
            formula="bic_amount",
        )

        result = translator.create_measure_name(kpi, simple_definition)
        # Should convert technical_name to title case
        assert "Total" in result
        assert "Sales" in result

    def test_create_measure_name_fallback_formula(self, translator, simple_definition):
        """Test measure name falls back to formula"""
        kpi = KPI(description="", formula="bic_customer_revenue")  # Empty description

        result = translator.create_measure_name(kpi, simple_definition)
        # Should format formula
        assert "Customer" in result
        assert "Revenue" in result

    def test_create_measure_name_preserves_case(self, translator, simple_definition):
        """Test measure name preserves title case"""
        kpi = KPI(
            description="YTD Revenue",
            technical_name="ytd_revenue",
            formula="bic_amount",
        )

        result = translator.create_measure_name(kpi, simple_definition)
        assert result == "YTD Revenue"

    # ========== get_field_metadata Tests ==========

    # ========== Integration Tests ==========

    def test_measure_name_with_empty_description(self, translator, simple_definition):
        """Test measure name creation when description is empty"""
        kpi = KPI(
            description="",  # Empty description to test fallback
            technical_name="test_metric",
            formula="bic_amount",
        )

        result = translator.create_measure_name(kpi, simple_definition)
        # Should fall back to technical_name
        assert "Test" in result
        assert "Metric" in result
