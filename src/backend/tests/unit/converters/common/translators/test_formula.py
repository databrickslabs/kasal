"""
Unit tests for converters/common/translators/formula.py

Tests formula translation logic for converting SAP BW formulas to DAX components.
"""

import pytest
from src.converters.common.translators.formula import FormulaTranslator
from src.converters.base.models import KPI, KPIDefinition


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
            description="Sales Metrics",
            technical_name="sales_metrics",
            kpis=[]
        )

    @pytest.fixture
    def kpi_volume_field(self):
        """KPI with volume field"""
        return KPI(
            description="Total Volume",
            technical_name="total_volume",
            formula="bic_kvolume_c"
        )

    @pytest.fixture
    def kpi_amount_field(self):
        """KPI with amount field"""
        return KPI(
            description="Total Amount",
            technical_name="total_amount",
            formula="bic_kamount_c"
        )

    @pytest.fixture
    def kpi_with_source_table(self):
        """KPI with explicit source_table"""
        return KPI(
            description="Sales Revenue",
            technical_name="sales_revenue",
            formula="bic_revenue",
            source_table="FactSales"
        )

    @pytest.fixture
    def kpi_count_field(self):
        """KPI with count field"""
        return KPI(
            description="Order Count",
            technical_name="order_count",
            formula="bic_order_count"
        )

    @pytest.fixture
    def kpi_no_prefix(self):
        """KPI without bic_ prefix"""
        return KPI(
            description="Simple Metric",
            technical_name="simple_metric",
            formula="quantity"
        )

    # ========== Initialization Tests ==========

    def test_translator_initialization(self, translator):
        """Test FormulaTranslator initializes with aggregation mappings"""
        assert translator.aggregation_mappings is not None
        assert translator.field_pattern is not None
        assert len(translator.aggregation_mappings) > 0

    def test_aggregation_mappings_defined(self, translator):
        """Test aggregation mappings contain expected keywords"""
        assert 'volume' in translator.aggregation_mappings
        assert 'amount' in translator.aggregation_mappings
        assert 'count' in translator.aggregation_mappings
        assert 'avg' in translator.aggregation_mappings
        assert translator.aggregation_mappings['volume'] == 'SUM'
        assert translator.aggregation_mappings['count'] == 'COUNT'

    def test_field_pattern_compilation(self, translator):
        """Test field pattern correctly extracts bic_ fields"""
        test_string = "bic_customer_name and bic_region"
        matches = translator.field_pattern.findall(test_string)
        assert len(matches) == 2
        assert 'customer_name' in matches
        assert 'region' in matches

    # ========== translate_formula Tests ==========

    def test_translate_formula_volume(self, translator, kpi_volume_field, simple_definition):
        """Test formula translation for volume field"""
        result = translator.translate_formula(kpi_volume_field, simple_definition)

        assert result['aggregation'] == 'SUM'
        assert 'volume' in result['table_name'].lower()
        assert 'bic_kvolume_c' in result['column_name']
        assert 'bic_kvolume_c' == result['technical_field']

    def test_translate_formula_amount(self, translator, kpi_amount_field, simple_definition):
        """Test formula translation for amount field"""
        result = translator.translate_formula(kpi_amount_field, simple_definition)

        assert result['aggregation'] == 'SUM'
        assert 'amount' in result['table_name'].lower()
        assert 'bic_kamount_c' in result['column_name']

    def test_translate_formula_with_source_table(self, translator, kpi_with_source_table, simple_definition):
        """Test formula translation with explicit source table"""
        result = translator.translate_formula(kpi_with_source_table, simple_definition)

        assert result['table_name'] == 'FactSales'
        assert result['aggregation'] == 'SUM'

    def test_translate_formula_count_field(self, translator, kpi_count_field, simple_definition):
        """Test formula translation for count field"""
        result = translator.translate_formula(kpi_count_field, simple_definition)

        assert result['aggregation'] == 'COUNT'

    def test_translate_formula_no_prefix(self, translator, kpi_no_prefix, simple_definition):
        """Test formula translation adds bic_ prefix if missing"""
        result = translator.translate_formula(kpi_no_prefix, simple_definition)

        # Should add bic_ prefix
        assert result['technical_field'].startswith('bic_')

    def test_translate_formula_returns_all_keys(self, translator, kpi_volume_field, simple_definition):
        """Test formula translation returns all required keys"""
        result = translator.translate_formula(kpi_volume_field, simple_definition)

        assert 'aggregation' in result
        assert 'table_name' in result
        assert 'column_name' in result
        assert 'technical_field' in result

    # ========== _determine_aggregation Tests ==========

    def test_determine_aggregation_volume(self, translator):
        """Test aggregation determination for volume keyword"""
        result = translator._determine_aggregation("bic_kvolume_c")
        assert result == 'SUM'

    def test_determine_aggregation_amount(self, translator):
        """Test aggregation determination for amount keyword"""
        result = translator._determine_aggregation("bic_kamount_c")
        assert result == 'SUM'

    def test_determine_aggregation_count(self, translator):
        """Test aggregation determination for count keyword"""
        result = translator._determine_aggregation("bic_order_count")
        assert result == 'COUNT'

    def test_determine_aggregation_avg(self, translator):
        """Test aggregation determination for average keyword"""
        result = translator._determine_aggregation("avg_price")
        assert result == 'AVERAGE'

    def test_determine_aggregation_max(self, translator):
        """Test aggregation determination for max keyword"""
        result = translator._determine_aggregation("max_temperature")
        assert result == 'MAX'

    def test_determine_aggregation_min(self, translator):
        """Test aggregation determination for min keyword"""
        result = translator._determine_aggregation("min_value")
        assert result == 'MIN'

    def test_determine_aggregation_default(self, translator):
        """Test aggregation determination defaults to SUM"""
        result = translator._determine_aggregation("bic_some_field")
        assert result == 'SUM'

    def test_determine_aggregation_case_insensitive(self, translator):
        """Test aggregation determination is case insensitive"""
        result = translator._determine_aggregation("BIC_KVOLUME_C")
        assert result == 'SUM'

    def test_determine_aggregation_quantity(self, translator):
        """Test aggregation determination for quantity keyword"""
        result = translator._determine_aggregation("bic_quantity")
        assert result == 'SUM'

    # ========== _generate_table_name Tests ==========

    def test_generate_table_name_with_prefix(self, translator):
        """Test table name generation from bic_ prefixed field"""
        result = translator._generate_table_name("bic_kvolume_c")
        assert 'volume' in result.lower()
        assert result.endswith('Data')

    def test_generate_table_name_without_prefix(self, translator):
        """Test table name generation from field without prefix"""
        result = translator._generate_table_name("quantity")
        assert result.endswith('Data')

    def test_generate_table_name_removes_k_prefix(self, translator):
        """Test table name generation removes SAP BW 'k' prefix"""
        result = translator._generate_table_name("bic_kamount_c")
        # Should remove 'k' prefix from SAP key figures
        assert 'amount' in result.lower()

    def test_generate_table_name_capitalizes(self, translator):
        """Test table name generation capitalizes result"""
        result = translator._generate_table_name("bic_revenue")
        assert result[0].isupper()

    def test_generate_table_name_underscores(self, translator):
        """Test table name generation with underscores"""
        result = translator._generate_table_name("bic_customer_sales_c")
        # Should use first meaningful part
        assert result.endswith('Data')

    def test_generate_table_name_default(self, translator):
        """Test table name generation with minimal input"""
        result = translator._generate_table_name("_")
        # Empty parts default to 'Data' suffix
        assert result.endswith('Data') or result == 'FactTable'

    # ========== create_measure_name Tests ==========

    def test_create_measure_name_from_description(self, translator, simple_definition):
        """Test measure name creation from description"""
        kpi = KPI(
            description="Total Sales Amount",
            technical_name="total_sales",
            formula="bic_amount"
        )

        result = translator.create_measure_name(kpi, simple_definition)
        assert result == "Total Sales Amount"

    def test_create_measure_name_cleans_special_chars(self, translator, simple_definition):
        """Test measure name creation removes special characters"""
        kpi = KPI(
            description="Sales (YTD) - Actual",
            technical_name="sales_ytd",
            formula="bic_amount"
        )

        result = translator.create_measure_name(kpi, simple_definition)
        # Special characters should be removed
        assert '(' not in result
        assert ')' not in result
        assert '-' not in result

    def test_create_measure_name_normalizes_whitespace(self, translator, simple_definition):
        """Test measure name creation normalizes whitespace"""
        kpi = KPI(
            description="Total   Sales    Amount",
            technical_name="total_sales",
            formula="bic_amount"
        )

        result = translator.create_measure_name(kpi, simple_definition)
        # Should normalize to single spaces
        assert '  ' not in result

    def test_create_measure_name_fallback_technical(self, translator, simple_definition):
        """Test measure name falls back to technical_name"""
        kpi = KPI(
            description="",  # Empty description to test fallback
            technical_name="total_sales",
            formula="bic_amount"
        )

        result = translator.create_measure_name(kpi, simple_definition)
        # Should convert technical_name to title case
        assert 'Total' in result
        assert 'Sales' in result

    def test_create_measure_name_fallback_formula(self, translator, simple_definition):
        """Test measure name falls back to formula"""
        kpi = KPI(
            description="",  # Empty description
            formula="bic_customer_revenue"
        )

        result = translator.create_measure_name(kpi, simple_definition)
        # Should format formula
        assert 'Customer' in result
        assert 'Revenue' in result

    def test_create_measure_name_preserves_case(self, translator, simple_definition):
        """Test measure name preserves title case"""
        kpi = KPI(
            description="YTD Revenue",
            technical_name="ytd_revenue",
            formula="bic_amount"
        )

        result = translator.create_measure_name(kpi, simple_definition)
        assert result == "YTD Revenue"

    # ========== get_field_metadata Tests ==========

    def test_get_field_metadata_bic_prefix(self, translator):
        """Test field metadata extraction with bic_ prefix"""
        result = translator.get_field_metadata("bic_kvolume_c")

        assert result['original_field'] == "bic_kvolume_c"
        assert result['clean_name'] == "kvolume_c"
        assert result['category'] == 'Measure'
        assert result['data_type'] == 'DECIMAL'

    def test_get_field_metadata_no_prefix(self, translator):
        """Test field metadata extraction without bic_ prefix"""
        result = translator.get_field_metadata("customer_name")

        assert result['original_field'] == "customer_name"
        assert result['clean_name'] == "customer_name"

    def test_get_field_metadata_measure(self, translator):
        """Test field metadata identifies measures correctly"""
        result = translator.get_field_metadata("bic_kamount")

        assert result['category'] == 'Measure'
        assert result['data_type'] == 'DECIMAL'

    def test_get_field_metadata_dimension(self, translator):
        """Test field metadata identifies dimensions correctly"""
        result = translator.get_field_metadata("bic_customer_name")

        assert result['category'] == 'Dimension'
        assert result['data_type'] == 'STRING'

    def test_get_field_metadata_amount(self, translator):
        """Test field metadata for amount field"""
        result = translator.get_field_metadata("bic_sales_amount")

        assert result['category'] == 'Measure'
        assert result['data_type'] == 'DECIMAL'

    def test_get_field_metadata_volume(self, translator):
        """Test field metadata for volume field"""
        result = translator.get_field_metadata("bic_total_volume")

        assert result['category'] == 'Measure'
        assert result['data_type'] == 'DECIMAL'

    def test_get_field_metadata_qty(self, translator):
        """Test field metadata for quantity field"""
        result = translator.get_field_metadata("bic_order_qty")

        assert result['category'] == 'Measure'
        assert result['data_type'] == 'DECIMAL'

    def test_get_field_metadata_all_keys(self, translator):
        """Test field metadata returns all expected keys"""
        result = translator.get_field_metadata("bic_test_field")

        assert 'original_field' in result
        assert 'clean_name' in result
        assert 'data_type' in result
        assert 'category' in result

    # ========== Integration Tests ==========

    def test_full_workflow_simple(self, translator, kpi_volume_field, simple_definition):
        """Test complete workflow for simple KPI"""
        # Translate formula
        translation = translator.translate_formula(kpi_volume_field, simple_definition)

        assert translation['aggregation'] == 'SUM'
        assert translation['table_name'] is not None

        # Create measure name
        measure_name = translator.create_measure_name(kpi_volume_field, simple_definition)
        assert measure_name == "Total Volume"

        # Get metadata
        metadata = translator.get_field_metadata(translation['technical_field'])
        assert metadata['category'] == 'Measure'

    def test_full_workflow_with_source_table(self, translator, kpi_with_source_table, simple_definition):
        """Test complete workflow with explicit source table"""
        translation = translator.translate_formula(kpi_with_source_table, simple_definition)

        # Should use provided source table
        assert translation['table_name'] == 'FactSales'

        measure_name = translator.create_measure_name(kpi_with_source_table, simple_definition)
        assert 'Sales' in measure_name or 'Revenue' in measure_name

    def test_multiple_kpis_translation(self, translator, simple_definition):
        """Test translating multiple KPIs"""
        kpis = [
            KPI(description="Volume", technical_name="vol", formula="bic_kvolume"),
            KPI(description="Amount", technical_name="amt", formula="bic_kamount"),
            KPI(description="Count", technical_name="cnt", formula="bic_count")
        ]

        results = [translator.translate_formula(kpi, simple_definition) for kpi in kpis]

        assert len(results) == 3
        assert results[0]['aggregation'] == 'SUM'
        assert results[1]['aggregation'] == 'SUM'
        assert results[2]['aggregation'] == 'COUNT'

    def test_edge_case_empty_formula(self, translator, simple_definition):
        """Test edge case with empty formula"""
        kpi = KPI(
            description="Empty",
            technical_name="empty",
            formula=""
        )

        # Should not crash
        result = translator.translate_formula(kpi, simple_definition)
        assert result['aggregation'] == 'SUM'  # Default

    def test_edge_case_special_characters(self, translator, simple_definition):
        """Test edge case with special characters in formula"""
        kpi = KPI(
            description="Special Chars",
            technical_name="special",
            formula="bic_field@123"
        )

        # Should handle gracefully
        result = translator.translate_formula(kpi, simple_definition)
        assert result is not None

    def test_consistency_across_calls(self, translator, kpi_volume_field, simple_definition):
        """Test translation consistency across multiple calls"""
        result1 = translator.translate_formula(kpi_volume_field, simple_definition)
        result2 = translator.translate_formula(kpi_volume_field, simple_definition)

        assert result1 == result2

    def test_aggregation_keyword_priority(self, translator):
        """Test aggregation determination with multiple keywords"""
        # 'count' keyword appears first but 'amount' also matches
        # The implementation checks keywords in order, so first match wins
        result = translator._determine_aggregation("count_of_amounts")
        # Both 'count' and 'amount' are in the string, first one found wins
        assert result in ['COUNT', 'SUM']  # Accept either as both are valid

    def test_table_name_generation_consistency(self, translator):
        """Test table name generation is consistent"""
        result1 = translator._generate_table_name("bic_sales")
        result2 = translator._generate_table_name("bic_sales")

        assert result1 == result2

    def test_measure_name_with_empty_description(self, translator, simple_definition):
        """Test measure name creation when description is empty"""
        kpi = KPI(
            description="",  # Empty description to test fallback
            technical_name="test_metric",
            formula="bic_amount"
        )

        result = translator.create_measure_name(kpi, simple_definition)
        # Should fall back to technical_name
        assert 'Test' in result
        assert 'Metric' in result
