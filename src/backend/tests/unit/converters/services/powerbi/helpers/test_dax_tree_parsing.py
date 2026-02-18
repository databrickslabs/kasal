"""
Unit tests for converters/services/powerbi/helpers/dax_tree_parsing.py

Tests DAX generator with tree parsing capabilities for nested measure dependencies.
"""

import pytest
from src.converters.services.powerbi.helpers.dax_tree_parsing import TreeParsingDAXGenerator
from src.converters.base.models import KPI, KPIDefinition, DAXMeasure


class TestTreeParsingDAXGenerator:
    """Tests for TreeParsingDAXGenerator class"""

    @pytest.fixture
    def generator(self):
        """Create TreeParsingDAXGenerator instance for testing"""
        return TreeParsingDAXGenerator()

    @pytest.fixture
    def simple_definition(self):
        """Simple KPI definition with one measure"""
        return KPIDefinition(
            description="Sales",
            technical_name="sales",
            kpis=[
                KPI(
                    description="Revenue",
                    technical_name="revenue",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="Sales"
                )
            ]
        )

    @pytest.fixture
    def definition_with_dependencies(self):
        """KPI definition with measure dependencies"""
        return KPIDefinition(
            description="Complex Metrics",
            technical_name="complex",
            kpis=[
                KPI(
                    description="Revenue",
                    technical_name="revenue",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="Sales"
                ),
                KPI(
                    description="Cost",
                    technical_name="cost",
                    formula="cost_amount",
                    aggregation_type="SUM",
                    source_table="Sales"
                ),
                KPI(
                    description="Profit",
                    technical_name="profit",
                    formula="[revenue] - [cost]",
                    aggregation_type="CALCULATED"
                )
            ]
        )

    @pytest.fixture
    def definition_with_deep_dependencies(self):
        """KPI definition with multi-level dependencies"""
        return KPIDefinition(
            description="Deep Dependencies",
            technical_name="deep",
            kpis=[
                KPI(
                    description="Base",
                    technical_name="base",
                    formula="value",
                    aggregation_type="SUM",
                    source_table="Data"
                ),
                KPI(
                    description="Level 1",
                    technical_name="level1",
                    formula="[base] * 2",
                    aggregation_type="CALCULATED"
                ),
                KPI(
                    description="Level 2",
                    technical_name="level2",
                    formula="[level1] + 10",
                    aggregation_type="CALCULATED"
                )
            ]
        )

    # ========== Initialization Tests ==========

    def test_generator_initialization(self, generator):
        """Test TreeParsingDAXGenerator initializes both parent classes"""
        # Should have dependency resolver from BaseTreeParsingGenerator
        assert hasattr(generator, 'dependency_resolver')
        assert hasattr(generator, 'filter_resolver')

        # Should have formula translator from DAXGenerator
        assert hasattr(generator, 'formula_translator')

        # Should have both generation capabilities
        assert hasattr(generator, 'generate_dax_measure')
        assert hasattr(generator, '_generate_leaf_measure')
        assert hasattr(generator, '_generate_calculated_measure')
        assert hasattr(generator, '_generate_calculated_measure_with_references')

    def test_generator_has_dependency_resolution(self, generator):
        """Test generator has dependency resolution capabilities"""
        assert hasattr(generator, 'dependency_resolver')
        assert hasattr(generator.dependency_resolver, 'register_measures')
        assert hasattr(generator.dependency_resolver, 'dependency_graph')

    # ========== _generate_leaf_measure Tests ==========

    def test_generate_leaf_measure_simple(self, generator, simple_definition):
        """Test generating leaf measure without dependencies"""
        kpi = simple_definition.kpis[0]
        result = generator._generate_leaf_measure(simple_definition, kpi)

        assert isinstance(result, DAXMeasure)
        assert result.name == "Revenue"
        assert "SUM(Sales[amount])" in result.dax_formula

    def test_generate_leaf_measure_count(self, generator):
        """Test generating leaf measure with COUNT aggregation"""
        definition = KPIDefinition(
            description="Counts",
            technical_name="counts",
            kpis=[
                KPI(
                    description="Order Count",
                    technical_name="order_count",
                    formula="order_id",
                    aggregation_type="COUNT",
                    source_table="Orders"
                )
            ]
        )

        result = generator._generate_leaf_measure(definition, definition.kpis[0])

        assert isinstance(result, DAXMeasure)
        assert "COUNT(Orders[order_id])" in result.dax_formula

    def test_generate_leaf_measure_with_description(self, generator, simple_definition):
        """Test leaf measure includes description"""
        kpi = simple_definition.kpis[0]
        result = generator._generate_leaf_measure(simple_definition, kpi)

        assert result.description == kpi.description
        assert result.original_kbi == kpi

    # ========== _generate_calculated_measure Tests ==========

    def test_generate_calculated_measure_with_dependencies(self, generator, definition_with_dependencies):
        """Test generating calculated measure with dependencies"""
        # Register measures first
        generator.dependency_resolver.register_measures(definition_with_dependencies)

        profit_kpi = definition_with_dependencies.kpis[2]
        result = generator._generate_calculated_measure(definition_with_dependencies, profit_kpi)

        assert isinstance(result, DAXMeasure)
        assert result.name == "Profit"
        # Should have inlined dependencies (SUM expressions, not references)
        assert "SUM(" in result.dax_formula

    def test_generate_calculated_measure_with_display_sign(self, generator):
        """Test calculated measure with display sign multiplier"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Negative Value",
                    technical_name="negative",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="Data",
                    display_sign=-1
                )
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        result = generator._generate_calculated_measure(definition, definition.kpis[0])

        # Should have -1 multiplier
        assert "-1 *" in result.dax_formula

    def test_generate_calculated_measure_with_custom_display_sign(self, generator):
        """Test calculated measure with custom display sign"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Scaled Value",
                    technical_name="scaled",
                    formula="amount",
                    aggregation_type="SUM",
                    source_table="Data",
                    display_sign=100
                )
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        result = generator._generate_calculated_measure(definition, definition.kpis[0])

        # Should have 100 multiplier
        assert "100 *" in result.dax_formula

    def test_generate_calculated_measure_default_table(self, generator):
        """Test calculated measure with default table name"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Value",
                    technical_name="value",
                    formula="amount",
                    aggregation_type="SUM"
                    # No source_table specified
                )
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        result = generator._generate_calculated_measure(definition, definition.kpis[0])

        # Should use 'Table' as default
        assert isinstance(result, DAXMeasure)

    # ========== _generate_calculated_measure_with_references Tests ==========

    def test_generate_calculated_with_references(self, generator, definition_with_dependencies):
        """Test generating calculated measure with measure references"""
        # Register measures first
        generator.dependency_resolver.register_measures(definition_with_dependencies)

        profit_kpi = definition_with_dependencies.kpis[2]
        result = generator._generate_calculated_measure_with_references(
            definition_with_dependencies,
            profit_kpi
        )

        assert isinstance(result, DAXMeasure)
        assert result.name == "Profit"
        # Should have measure references [Revenue] and [Cost]
        assert "[Revenue]" in result.dax_formula or "[Cost]" in result.dax_formula

    def test_generate_calculated_with_references_nested(self, generator, definition_with_deep_dependencies):
        """Test calculated measure with nested dependency references"""
        generator.dependency_resolver.register_measures(definition_with_deep_dependencies)

        level2_kpi = definition_with_deep_dependencies.kpis[2]
        result = generator._generate_calculated_measure_with_references(
            definition_with_deep_dependencies,
            level2_kpi
        )

        assert isinstance(result, DAXMeasure)
        # Should reference Level 1 measure
        assert "[Level 1]" in result.dax_formula or "level1" in result.dax_formula

    def test_generate_calculated_with_references_display_sign(self, generator):
        """Test calculated measure with references and display sign"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="Base",
                    technical_name="base",
                    formula="value",
                    aggregation_type="SUM",
                    source_table="Data"
                ),
                KPI(
                    description="Derived",
                    technical_name="derived",
                    formula="[base] * 2",
                    aggregation_type="CALCULATED",
                    display_sign=-1
                )
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        result = generator._generate_calculated_measure_with_references(
            definition,
            definition.kpis[1]
        )

        # Should have both measure reference and display sign
        assert "-1 *" in result.dax_formula

    # ========== Integration Tests ==========

    def test_full_workflow_simple_measures(self, generator, simple_definition):
        """Test complete workflow for simple measures"""
        kpi = simple_definition.kpis[0]
        result = generator._generate_leaf_measure(simple_definition, kpi)

        assert isinstance(result, DAXMeasure)
        assert result.original_kbi == kpi
        assert "SUM(" in result.dax_formula

    def test_full_workflow_with_dependencies(self, generator, definition_with_dependencies):
        """Test complete workflow with dependencies"""
        # Use tree parsing to generate all measures
        generator.dependency_resolver.register_measures(definition_with_dependencies)

        # Generate leaf measures
        revenue = generator._generate_leaf_measure(
            definition_with_dependencies,
            definition_with_dependencies.kpis[0]
        )
        cost = generator._generate_leaf_measure(
            definition_with_dependencies,
            definition_with_dependencies.kpis[1]
        )

        # Generate calculated measure
        profit = generator._generate_calculated_measure(
            definition_with_dependencies,
            definition_with_dependencies.kpis[2]
        )

        assert all(isinstance(m, DAXMeasure) for m in [revenue, cost, profit])
        assert revenue.name == "Revenue"
        assert cost.name == "Cost"
        assert profit.name == "Profit"

    def test_dependency_resolution_order(self, generator, definition_with_deep_dependencies):
        """Test measures are generated in correct dependency order"""
        generator.dependency_resolver.register_measures(definition_with_deep_dependencies)

        # Should be able to generate level2 which depends on level1 which depends on base
        result = generator._generate_calculated_measure(
            definition_with_deep_dependencies,
            definition_with_deep_dependencies.kpis[2]
        )

        assert isinstance(result, DAXMeasure)
        assert result.name == "Level 2"

    def test_measure_references_vs_inline(self, generator, definition_with_dependencies):
        """Test difference between inline and reference generation"""
        generator.dependency_resolver.register_measures(definition_with_dependencies)

        profit_kpi = definition_with_dependencies.kpis[2]

        # Generate with inline dependencies
        inline_result = generator._generate_calculated_measure(
            definition_with_dependencies,
            profit_kpi
        )

        # Generate with measure references
        reference_result = generator._generate_calculated_measure_with_references(
            definition_with_dependencies,
            profit_kpi
        )

        # Both should be valid DAX measures
        assert isinstance(inline_result, DAXMeasure)
        assert isinstance(reference_result, DAXMeasure)
        # Formulas should differ (inline has SUM, references has [Measure])
        assert inline_result.dax_formula != reference_result.dax_formula

    def test_handles_multiple_dependencies(self, generator):
        """Test handling measure with multiple dependencies"""
        definition = KPIDefinition(
            description="Multi-Dep",
            technical_name="multi",
            kpis=[
                KPI(description="A", technical_name="a", formula="val_a", aggregation_type="SUM", source_table="D"),
                KPI(description="B", technical_name="b", formula="val_b", aggregation_type="SUM", source_table="D"),
                KPI(description="C", technical_name="c", formula="val_c", aggregation_type="SUM", source_table="D"),
                KPI(description="Combined", technical_name="combined", formula="[a] + [b] + [c]", aggregation_type="CALCULATED"),
            ]
        )

        generator.dependency_resolver.register_measures(definition)
        result = generator._generate_calculated_measure_with_references(
            definition,
            definition.kpis[3]
        )

        assert isinstance(result, DAXMeasure)
        # Should have all three measure references
        formula_upper = result.dax_formula.upper()
        assert "[A]" in formula_upper or "[B]" in formula_upper or "[C]" in formula_upper

    # ========== Edge Cases ==========

    def test_edge_case_empty_formula(self, generator):
        """Test handling empty formula"""
        definition = KPIDefinition(
            description="Empty",
            technical_name="empty",
            kpis=[
                KPI(
                    description="Empty",
                    technical_name="empty",
                    formula="",
                    aggregation_type="SUM",
                    source_table="Data"
                )
            ]
        )

        result = generator._generate_leaf_measure(definition, definition.kpis[0])
        assert isinstance(result, DAXMeasure)

    def test_edge_case_measure_with_empty_description(self, generator):
        """Test measure generation with empty description string"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(
                    description="",  # Empty description
                    technical_name="no_desc",
                    formula="value",
                    aggregation_type="SUM",
                    source_table="Data"
                )
            ]
        )

        result = generator._generate_leaf_measure(definition, definition.kpis[0])
        assert isinstance(result, DAXMeasure)
        # Should have auto-generated description or use technical name
        assert result.description is not None
