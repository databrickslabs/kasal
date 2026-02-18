"""
Unit tests for converters/services/uc_metrics/helpers/uc_metrics_tree_parsing.py

Tests UC Metrics tree parsing generator for handling nested measure dependencies.
"""

import pytest
from src.converters.services.uc_metrics.helpers.uc_metrics_tree_parsing import (
    UCMetricsTreeParsingGenerator
)
from src.converters.base.models import KPI, KPIDefinition


class TestUCMetricsTreeParsingGenerator:
    """Tests for UCMetricsTreeParsingGenerator class"""

    @pytest.fixture
    def generator(self):
        """Create generator for testing"""
        return UCMetricsTreeParsingGenerator(dialect="spark")

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
                    aggregation_type="SUM"
                )
            ]
        )

    @pytest.fixture
    def definition_with_dependencies(self):
        """KPI definition with calculated measures"""
        return KPIDefinition(
            description="Financial Metrics",
            technical_name="financial_metrics",
            kpis=[
                KPI(
                    description="Revenue",
                    technical_name="revenue",
                    formula="amount",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Cost",
                    technical_name="cost",
                    formula="cost_amount",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Profit",
                    technical_name="profit",
                    formula="[revenue] - [cost]",
                    aggregation_type="CALCULATED"
                )
            ]
        )

    # ========== Initialization Tests ==========

    def test_generator_initialization_spark(self, generator):
        """Test generator initializes with Spark dialect"""
        assert generator.dialect == "spark"

    def test_generator_initialization_default(self):
        """Test generator initializes with default dialect"""
        generator = UCMetricsTreeParsingGenerator()
        assert generator.dialect == "spark"

    def test_generator_has_aggregation_builder(self, generator):
        """Test generator has aggregation builder"""
        assert hasattr(generator, 'aggregation_builder')
        assert generator.aggregation_builder is not None

    def test_generator_has_dependency_resolver(self, generator):
        """Test generator has dependency resolver"""
        assert hasattr(generator, 'dependency_resolver')
        assert generator.dependency_resolver is not None

    # ========== Generate Leaf Measure Tests ==========

    def test_generate_leaf_measure_simple(self, generator, simple_definition):
        """Test generating simple leaf measure"""
        kpi = simple_definition.kpis[0]
        result = generator._generate_leaf_measure(simple_definition, kpi)

        assert result is not None
        assert result["name"] == "revenue"
        assert "SUM(amount)" in result["expr"]
        assert result["description"] == "Revenue"

    def test_generate_leaf_measure_count(self, generator, simple_definition):
        """Test generating COUNT leaf measure"""
        kpi = KPI(
            description="Row Count",
            technical_name="count",
            formula="*",
            aggregation_type="COUNT"
        )
        result = generator._generate_leaf_measure(simple_definition, kpi)

        assert result["name"] == "count"
        assert "COUNT(*)" in result["expr"]
        assert result["description"] == "Row Count"

    def test_generate_leaf_measure_with_negative_sign(self, generator, simple_definition):
        """Test generating leaf measure with display_sign = -1"""
        kpi = KPI(
            description="Cost",
            technical_name="cost",
            formula="cost_amount",
            aggregation_type="SUM",
            display_sign=-1
        )
        result = generator._generate_leaf_measure(simple_definition, kpi)

        assert result["name"] == "cost"
        assert "(-1) *" in result["expr"]
        assert "SUM(cost_amount)" in result["expr"]

    def test_generate_leaf_measure_with_custom_sign(self, generator, simple_definition):
        """Test generating leaf measure with custom display_sign"""
        kpi = KPI(
            description="Adjusted",
            technical_name="adjusted",
            formula="value",
            aggregation_type="SUM",
            display_sign=2
        )
        result = generator._generate_leaf_measure(simple_definition, kpi)

        assert result["name"] == "adjusted"
        assert "2 *" in result["expr"]

    def test_generate_leaf_measure_no_technical_name(self, generator, simple_definition):
        """Test generating leaf measure without technical name"""
        kpi = KPI(
            description="Revenue",
            formula="amount",
            aggregation_type="SUM"
        )
        result = generator._generate_leaf_measure(simple_definition, kpi)

        assert result["name"] == "unnamed_measure"
        assert "SUM(amount)" in result["expr"]

    def test_generate_leaf_measure_no_description(self, generator, simple_definition):
        """Test generating leaf measure without description"""
        kpi = KPI(
            description="",
            technical_name="test",
            formula="value",
            aggregation_type="SUM"
        )
        result = generator._generate_leaf_measure(simple_definition, kpi)

        assert "Measure for test" in result["description"]

    def test_generate_leaf_measure_distinctcount(self, generator, simple_definition):
        """Test generating DISTINCTCOUNT leaf measure"""
        kpi = KPI(
            description="Unique Customers",
            technical_name="unique_customers",
            formula="customer_id",
            aggregation_type="DISTINCTCOUNT"
        )
        result = generator._generate_leaf_measure(simple_definition, kpi)

        assert result["name"] == "unique_customers"
        assert "COUNT(DISTINCT customer_id)" in result["expr"]

    def test_generate_leaf_measure_average(self, generator, simple_definition):
        """Test generating AVERAGE leaf measure"""
        kpi = KPI(
            description="Average Price",
            technical_name="avg_price",
            formula="price",
            aggregation_type="AVERAGE"
        )
        result = generator._generate_leaf_measure(simple_definition, kpi)

        assert result["name"] == "avg_price"
        assert "AVG(price)" in result["expr"]

    # ========== Generate Calculated Measure Tests ==========

    def test_generate_calculated_measure_simple(self, generator, definition_with_dependencies):
        """Test generating calculated measure with dependencies"""
        # Build dependency graph first
        generator.dependency_resolver.register_measures(definition_with_dependencies)

        profit_kpi = definition_with_dependencies.kpis[2]
        result = generator._generate_calculated_measure(definition_with_dependencies, profit_kpi)

        assert result is not None
        assert result["name"] == "profit"
        assert result["description"] == "Profit"

    def test_generate_calculated_measure_calculated_type(self, generator):
        """Test generating CALCULATED type measure"""
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

        generator.dependency_resolver.register_measures(definition)
        profit_kpi = definition.kpis[1]
        result = generator._generate_calculated_measure(definition, profit_kpi)

        assert result["name"] == "profit"
        assert "expr" in result

    def test_generate_calculated_measure_with_negative_sign(self, generator):
        """Test generating calculated measure with display_sign = -1"""
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
                    description="Loss",
                    technical_name="loss",
                    formula="[revenue] * -1",
                    aggregation_type="CALCULATED",
                    display_sign=-1
                )
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        loss_kpi = definition.kpis[1]
        result = generator._generate_calculated_measure(definition, loss_kpi)

        assert result["name"] == "loss"
        assert "(-1) *" in result["expr"]

    def test_generate_calculated_measure_with_custom_sign(self, generator):
        """Test generating calculated measure with custom display_sign"""
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
                    description="Doubled",
                    technical_name="doubled",
                    formula="[revenue]",
                    aggregation_type="CALCULATED",
                    display_sign=2
                )
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        doubled_kpi = definition.kpis[1]
        result = generator._generate_calculated_measure(definition, doubled_kpi)

        assert result["name"] == "doubled"
        assert "2 *" in result["expr"]

    def test_generate_calculated_measure_no_technical_name(self, generator):
        """Test generating calculated measure without technical name - skipped as not supported"""
        pytest.skip("Calculated measures without technical_name cannot be resolved by dependency resolver")

    def test_generate_calculated_measure_no_description(self, generator):
        """Test generating calculated measure without description"""
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
                    description="",
                    technical_name="calc",
                    formula="[revenue] * 2",
                    aggregation_type="CALCULATED"
                )
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        calc_kpi = definition.kpis[1]
        result = generator._generate_calculated_measure(definition, calc_kpi)

        assert "Calculated measure for calc" in result["description"]

    def test_generate_calculated_measure_sum_type(self, generator):
        """Test generating calculated measure with SUM type"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Value",
                    technical_name="value",
                    formula="amount",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Total",
                    technical_name="total",
                    formula="[value]",
                    aggregation_type="SUM"
                )
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        total_kpi = definition.kpis[1]
        result = generator._generate_calculated_measure(definition, total_kpi)

        assert result["name"] == "total"
        assert "expr" in result

    # ========== Generate Calculated Measure With References Tests ==========

    def test_generate_calculated_measure_with_references_simple(self, generator, definition_with_dependencies):
        """Test generating calculated measure with references"""
        generator.dependency_resolver.register_measures(definition_with_dependencies)

        profit_kpi = definition_with_dependencies.kpis[2]
        result = generator._generate_calculated_measure_with_references(
            definition_with_dependencies,
            profit_kpi
        )

        assert result is not None
        assert result["name"] == "profit"
        assert "revenue" in result["expr"] or "cost" in result["expr"]

    def test_generate_calculated_measure_with_references_negative_sign(self, generator):
        """Test generating measure with references and display_sign = -1"""
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
                    description="Negative Profit",
                    technical_name="negative_profit",
                    formula="[revenue] * 0.1",
                    aggregation_type="CALCULATED",
                    display_sign=-1
                )
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        kpi = definition.kpis[1]
        result = generator._generate_calculated_measure_with_references(definition, kpi)

        assert result["name"] == "negative_profit"
        assert "(-1) *" in result["expr"]

    def test_generate_calculated_measure_with_references_custom_sign(self, generator):
        """Test generating measure with references and custom display_sign"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Base",
                    technical_name="base",
                    formula="amount",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Multiplied",
                    technical_name="multiplied",
                    formula="[base]",
                    aggregation_type="CALCULATED",
                    display_sign=3
                )
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        kpi = definition.kpis[1]
        result = generator._generate_calculated_measure_with_references(definition, kpi)

        assert result["name"] == "multiplied"
        assert "3 *" in result["expr"]

    def test_generate_calculated_measure_with_references_no_technical_name(self, generator):
        """Test generating measure with references without technical name"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Base",
                    technical_name="base",
                    formula="amount",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Calculated",
                    formula="[base] * 2",
                    aggregation_type="CALCULATED"
                )
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        kpi = definition.kpis[1]
        result = generator._generate_calculated_measure_with_references(definition, kpi)

        assert result["name"] == "unnamed_measure"

    def test_generate_calculated_measure_with_references_no_description(self, generator):
        """Test generating measure with references without description"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Base",
                    technical_name="base",
                    formula="amount",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="",
                    technical_name="calc",
                    formula="[base] * 2",
                    aggregation_type="CALCULATED"
                )
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        kpi = definition.kpis[1]
        result = generator._generate_calculated_measure_with_references(definition, kpi)

        assert "Calculated measure for calc" in result["description"]

    def test_generate_calculated_measure_with_references_multiple_deps(self, generator):
        """Test generating measure with multiple dependencies"""
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
                    description="Cost",
                    technical_name="cost",
                    formula="cost_amount",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Tax",
                    technical_name="tax",
                    formula="tax_amount",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Net Profit",
                    technical_name="net_profit",
                    formula="[revenue] - [cost] - [tax]",
                    aggregation_type="CALCULATED"
                )
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        kpi = definition.kpis[3]
        result = generator._generate_calculated_measure_with_references(definition, kpi)

        assert result["name"] == "net_profit"
        # Should reference the measure names
        assert "revenue" in result["expr"] or "cost" in result["expr"] or "tax" in result["expr"]

    # ========== Integration Tests ==========

    def test_full_workflow_simple(self, generator, simple_definition):
        """Test full workflow with simple definition"""
        kpi = simple_definition.kpis[0]
        leaf = generator._generate_leaf_measure(simple_definition, kpi)

        assert leaf is not None
        assert leaf["name"] == "revenue"

    def test_full_workflow_with_dependencies(self, generator, definition_with_dependencies):
        """Test full workflow with dependencies"""
        generator.dependency_resolver.register_measures(definition_with_dependencies)

        # Generate leaf measures
        revenue = generator._generate_leaf_measure(definition_with_dependencies, definition_with_dependencies.kpis[0])
        cost = generator._generate_leaf_measure(definition_with_dependencies, definition_with_dependencies.kpis[1])

        # Generate calculated measure
        profit = generator._generate_calculated_measure(definition_with_dependencies, definition_with_dependencies.kpis[2])

        assert revenue["name"] == "revenue"
        assert cost["name"] == "cost"
        assert profit["name"] == "profit"

    def test_full_workflow_with_references(self, generator, definition_with_dependencies):
        """Test full workflow using references"""
        generator.dependency_resolver.register_measures(definition_with_dependencies)

        profit = generator._generate_calculated_measure_with_references(
            definition_with_dependencies,
            definition_with_dependencies.kpis[2]
        )

        assert profit["name"] == "profit"
        assert "expr" in profit
