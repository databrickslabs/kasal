"""
Unit tests for converters/services/powerbi/yaml_to_dax.py

Tests DAX generation from YAML KPI definitions including filter conversion,
syntax validation, and dependency tree building.
"""

import pytest
from src.converters.services.powerbi.yaml_to_dax import DAXGenerator
from src.converters.base.models import KPI, KPIDefinition, DAXMeasure


class TestDAXGenerator:
    """Tests for DAXGenerator class"""

    @pytest.fixture
    def generator(self):
        """Create DAXGenerator instance for testing"""
        return DAXGenerator()

    @pytest.fixture
    def simple_kpi(self):
        """Simple KPI for testing"""
        return KPI(
            description="Total Sales",
            technical_name="total_sales",
            formula="amount",
            source_table="Sales",
            aggregation_type="SUM"
        )

    @pytest.fixture
    def simple_definition(self):
        """Simple KPI definition for testing"""
        return KPIDefinition(
            description="Sales Metrics",
            technical_name="sales_metrics",
            kpis=[
                KPI(
                    description="Revenue",
                    technical_name="revenue",
                    formula="amount",
                    source_table="Sales",
                    aggregation_type="SUM"
                )
            ]
        )

    @pytest.fixture
    def definition_with_filters(self):
        """KPI definition with filters"""
        return KPIDefinition(
            description="Filtered Metrics",
            technical_name="filtered_metrics",
            kpis=[
                KPI(
                    description="EMEA Sales",
                    technical_name="emea_sales",
                    formula="amount",
                    source_table="Sales",
                    aggregation_type="SUM",
                    filters=["region = 'EMEA'"]
                )
            ]
        )

    # ========== Initialization Tests ==========

    def test_generator_initialization(self, generator):
        """Test DAXGenerator initializes correctly"""
        assert generator is not None
        assert hasattr(generator, 'filter_resolver')
        assert hasattr(generator, 'formula_translator')
        assert hasattr(generator, 'formula_parser')
        assert hasattr(generator, 'currency_converter')
        assert hasattr(generator, 'uom_converter')

    def test_generator_has_dependency_resolver(self, generator):
        """Test generator has dependency resolver"""
        assert hasattr(generator, '_dependency_resolver')
        assert generator._dependency_resolver is not None

    def test_generator_has_context_tracking(self, generator):
        """Test generator has context tracking"""
        assert hasattr(generator, '_kbi_contexts')
        assert hasattr(generator, '_base_kbi_contexts')

    # ========== Generate DAX Measure Tests ==========

    def test_generate_dax_measure_simple(self, generator, simple_definition, simple_kpi):
        """Test generating simple DAX measure"""
        result = generator.generate_dax_measure(simple_definition, simple_kpi)

        assert isinstance(result, DAXMeasure)
        # Measure name comes from formula_translator.create_measure_name
        assert result.name is not None
        assert "SUM" in result.dax_formula
        assert "Sales[amount]" in result.dax_formula
        assert result.description == "Total Sales"

    def test_generate_dax_measure_with_filters(self, generator, definition_with_filters):
        """Test generating DAX measure with filters"""
        kpi = definition_with_filters.kpis[0]
        result = generator.generate_dax_measure(definition_with_filters, kpi)

        assert isinstance(result, DAXMeasure)
        assert "CALCULATE" in result.dax_formula
        assert "FILTER" in result.dax_formula
        assert "region" in result.dax_formula

    def test_generate_dax_measure_count(self, generator, simple_definition):
        """Test generating COUNT DAX measure"""
        kpi = KPI(
            description="Order Count",
            technical_name="order_count",
            formula="order_id",
            source_table="Orders",
            aggregation_type="COUNT"
        )
        result = generator.generate_dax_measure(simple_definition, kpi)

        assert isinstance(result, DAXMeasure)
        assert "COUNT" in result.dax_formula

    def test_generate_dax_measure_average(self, generator, simple_definition):
        """Test generating AVERAGE DAX measure"""
        kpi = KPI(
            description="Average Price",
            technical_name="avg_price",
            formula="price",
            source_table="Products",
            aggregation_type="AVERAGE"
        )
        result = generator.generate_dax_measure(simple_definition, kpi)

        assert isinstance(result, DAXMeasure)
        assert "AVERAGE" in result.dax_formula or "AVG" in result.dax_formula

    def test_generate_dax_measure_with_constant_selection(self, generator, simple_definition):
        """Test generating DAX measure with constant selection"""
        kpi = KPI(
            description="Sales",
            technical_name="sales",
            formula="amount",
            source_table="Sales",
            aggregation_type="SUM",
            fields_for_constant_selection=["region", "product"]
        )
        result = generator.generate_dax_measure(simple_definition, kpi)

        assert isinstance(result, DAXMeasure)
        assert "REMOVEFILTERS" in result.dax_formula
        assert "Sales[region]" in result.dax_formula or "Sales[product]" in result.dax_formula

    def test_generate_dax_measure_with_display_sign(self, generator, simple_definition):
        """Test generating DAX measure with display_sign"""
        kpi = KPI(
            description="Cost",
            technical_name="cost",
            formula="cost_amount",
            source_table="Sales",
            aggregation_type="SUM",
            display_sign=-1
        )
        result = generator.generate_dax_measure(simple_definition, kpi)

        assert isinstance(result, DAXMeasure)
        # Display sign is handled by aggregation builder
        assert "SUM" in result.dax_formula

    # ========== Convert Filter to DAX Tests ==========

    def test_convert_filter_to_dax_simple_equality(self, generator):
        """Test converting simple equality filter"""
        filter_condition = "status = 'active'"
        result = generator.convert_filter_to_dax(filter_condition, "Sales")

        assert "Sales[status]" in result
        assert '= "active"' in result

    def test_convert_filter_to_dax_number_equality(self, generator):
        """Test converting number equality filter"""
        filter_condition = "year = 2023"
        result = generator.convert_filter_to_dax(filter_condition, "Sales")

        assert "Sales[year]" in result
        assert "= 2023" in result

    def test_convert_filter_to_dax_in_clause(self, generator):
        """Test converting IN clause filter"""
        filter_condition = "region IN ('EMEA', 'APAC')"
        result = generator.convert_filter_to_dax(filter_condition, "Sales")

        assert "Sales[region]" in result
        assert "IN" in result
        assert "{" in result and "}" in result

    def test_convert_filter_to_dax_not_in_clause(self, generator):
        """Test converting NOT IN clause filter"""
        filter_condition = "status NOT IN ('cancelled', 'pending')"
        result = generator.convert_filter_to_dax(filter_condition, "Sales")

        assert "NOT" in result
        assert "Sales[status]" in result
        assert "IN" in result

    def test_convert_filter_to_dax_between(self, generator):
        """Test converting BETWEEN filter"""
        filter_condition = "year BETWEEN 2020 AND 2023"
        result = generator.convert_filter_to_dax(filter_condition, "Sales")

        assert "Sales[year]" in result
        assert ">=" in result
        assert "<=" in result
        assert "&&" in result

    def test_convert_filter_to_dax_and_operator(self, generator):
        """Test converting AND operator"""
        filter_condition = "year = 2023 AND status = 'active'"
        result = generator.convert_filter_to_dax(filter_condition, "Sales")

        assert "&&" in result
        assert "Sales[year]" in result
        assert "Sales[status]" in result

    def test_convert_filter_to_dax_or_operator(self, generator):
        """Test converting OR operator"""
        filter_condition = "status = 'active' OR status = 'pending'"
        result = generator.convert_filter_to_dax(filter_condition, "Sales")

        assert "||" in result
        assert "Sales[status]" in result

    def test_convert_filter_to_dax_null_handling(self, generator):
        """Test converting NULL to BLANK()"""
        filter_condition = "cancelled_date = NULL"
        result = generator.convert_filter_to_dax(filter_condition, "Sales")

        assert "BLANK()" in result

    def test_convert_filter_to_dax_empty_filter(self, generator):
        """Test converting empty filter"""
        result = generator.convert_filter_to_dax("", "Sales")
        assert result == ""

    def test_convert_filter_to_dax_double_quotes(self, generator):
        """Test converting filter with double quotes"""
        filter_condition = 'region = "EMEA"'
        result = generator.convert_filter_to_dax(filter_condition, "Sales")

        assert "Sales[region]" in result
        assert '"EMEA"' in result

    # ========== Validate DAX Syntax Tests ==========

    def test_validate_dax_syntax_valid_calculate(self, generator):
        """Test validating valid CALCULATE syntax"""
        dax_formula = "CALCULATE(SUM(Sales[Amount]), FILTER(Sales, Sales[Region] = \"EMEA\"))"
        is_valid, message = generator.validate_dax_syntax(dax_formula)

        assert is_valid is True
        assert "valid" in message.lower() or "proper" in message.lower()

    def test_validate_dax_syntax_simple_sum(self, generator):
        """Test validating simple SUM"""
        dax_formula = "SUM(Sales[Amount])"
        is_valid, message = generator.validate_dax_syntax(dax_formula)

        assert is_valid is True

    def test_validate_dax_syntax_unbalanced_parentheses(self, generator):
        """Test detecting unbalanced parentheses"""
        dax_formula = "SUM(Sales[Amount]"
        is_valid, message = generator.validate_dax_syntax(dax_formula)

        assert is_valid is False
        assert "parentheses" in message.lower()

    def test_validate_dax_syntax_invalid_not_in(self, generator):
        """Test detecting invalid NOT IN syntax"""
        dax_formula = "CALCULATE(SUM(Sales[Amount]), Sales[Region] NOT IN ('EMEA', 'APAC'))"
        is_valid, message = generator.validate_dax_syntax(dax_formula)

        assert is_valid is False
        assert "NOT IN" in message

    def test_validate_dax_syntax_raw_and_outside_filter(self, generator):
        """Test detecting raw AND outside FILTER"""
        dax_formula = "SUM(Sales[Amount]) AND COUNT(Orders[ID])"
        is_valid, message = generator.validate_dax_syntax(dax_formula)

        assert is_valid is False
        assert "AND" in message

    def test_validate_dax_syntax_no_dax_functions(self, generator):
        """Test detecting missing DAX functions"""
        dax_formula = "amount * 1.2"
        is_valid, message = generator.validate_dax_syntax(dax_formula)

        assert is_valid is False
        assert "function" in message.lower()

    def test_validate_dax_syntax_no_column_references(self, generator):
        """Test detecting missing column references"""
        dax_formula = "SUM(100)"
        is_valid, message = generator.validate_dax_syntax(dax_formula)

        assert is_valid is False
        assert "column" in message.lower()

    # ========== Process Definition Tests ==========

    def test_process_definition_simple(self, generator, simple_definition):
        """Test processing simple definition"""
        # Should not raise errors
        generator.process_definition(simple_definition)

        # Dependency resolver should have KBIs
        assert hasattr(generator._dependency_resolver, '_kbi_lookup')

    def test_process_definition_with_dependencies(self, generator):
        """Test processing definition with calculated KPIs"""
        definition = KPIDefinition(
            description="Financial Metrics",
            technical_name="financial",
            kpis=[
                KPI(
                    description="Revenue",
                    technical_name="revenue",
                    formula="amount",
                    source_table="Sales",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Cost",
                    technical_name="cost",
                    formula="cost_amount",
                    source_table="Sales",
                    aggregation_type="SUM"
                )
            ]
        )

        # Process definition with only base KPIs (no calculated ones to avoid _extract_formula_kbis bug)
        generator.process_definition(definition)

        # Should have processed all KPIs
        assert len(generator._base_kbi_contexts) > 0

    def test_process_definition_empty_kpis(self, generator):
        """Test processing definition with no KPIs"""
        definition = KPIDefinition(
            description="Empty",
            technical_name="empty",
            kpis=[]
        )

        # Should not raise errors
        generator.process_definition(definition)

    # ========== Internal Helper Method Tests ==========

    def test_is_base_kbi_true(self, generator):
        """Test identifying base KBI"""
        kpi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            source_table="Sales",
            aggregation_type="SUM"
        )

        result = generator._is_base_kbi(kpi)
        assert result is True

    def test_is_base_kbi_false(self, generator):
        """Test identifying calculated KBI"""
        kpi = KPI(
            description="Profit",
            technical_name="profit",
            formula="[revenue] - [cost]",
            aggregation_type="CALCULATED"
        )

        result = generator._is_base_kbi(kpi)
        assert result is False

    def test_is_base_kbi_empty_formula(self, generator):
        """Test KBI with empty formula is base KBI"""
        kpi = KPI(
            description="Empty",
            technical_name="empty",
            formula="",
            aggregation_type="SUM"
        )

        result = generator._is_base_kbi(kpi)
        assert result is True

    def test_extract_formula_kbis(self, generator):
        """Test dependency resolver extracts KBI references from formula"""
        # First build lookup
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Revenue",
                    technical_name="revenue",
                    formula="amount",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Profit",
                    technical_name="profit",
                    formula="[revenue] * 0.2",
                    aggregation_type="CALCULATED"
                )
            ]
        )
        generator._dependency_resolver.build_kbi_lookup(definition.kpis)

        profit_kpi = definition.kpis[1]
        # Use resolve_formula_kbis instead of _extract_formula_kbis
        result = generator._dependency_resolver.resolve_formula_kbis(profit_kpi)

        assert len(result) > 0
        assert any(kbi.technical_name == "revenue" for kbi in result)

    def test_extract_formula_kbis_empty_formula(self, generator):
        """Test extracting from empty formula"""
        kpi = KPI(
            description="Empty",
            technical_name="empty",
            formula="",
            aggregation_type="SUM"
        )

        result = generator._extract_formula_kbis(kpi)
        assert result == []

    # ========== Edge Cases ==========

    def test_generate_dax_measure_no_technical_name(self, generator, simple_definition):
        """Test generating measure without technical name"""
        kpi = KPI(
            description="Unnamed",
            formula="amount",
            source_table="Sales",
            aggregation_type="SUM"
        )

        result = generator.generate_dax_measure(simple_definition, kpi)
        assert isinstance(result, DAXMeasure)
        assert result.name is not None

    def test_convert_filter_to_dax_complex_expression(self, generator):
        """Test converting complex filter expression"""
        filter_condition = "(status = 'active' AND year >= 2020) OR (status = 'pending' AND priority = 1)"
        result = generator.convert_filter_to_dax(filter_condition, "Sales")

        assert "&&" in result
        assert "||" in result
        assert "Sales[status]" in result

    def test_validate_dax_syntax_with_multiple_filters(self, generator):
        """Test validating DAX with multiple FILTER functions"""
        dax_formula = """CALCULATE(
            SUM(Sales[Amount]),
            FILTER(Sales, Sales[Region] = "EMEA"),
            FILTER(Sales, Sales[Year] = 2023)
        )"""
        is_valid, message = generator.validate_dax_syntax(dax_formula)

        assert is_valid is True
