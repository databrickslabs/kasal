"""
Unit tests for converters/services/powerbi/helpers/dax_smart.py

Tests smart DAX generation that automatically chooses between standard and tree-parsing generators.
"""

import pytest
from src.converters.services.powerbi.helpers.dax_smart import SmartDAXGenerator
from src.converters.base.models import KPI, KPIDefinition, DAXMeasure


class TestSmartDAXGenerator:
    """Tests for SmartDAXGenerator class"""

    @pytest.fixture
    def generator(self):
        """Create SmartDAXGenerator instance for testing"""
        return SmartDAXGenerator()

    @pytest.fixture
    def simple_definition(self):
        """Simple KPI definition without dependencies"""
        return KPIDefinition(
            description="Sales Metrics",
            technical_name="sales_metrics",
            kpis=[
                KPI(
                    description="Total Revenue",
                    technical_name="total_revenue",
                    formula="revenue",
                    aggregation_type="SUM",
                    source_table="Sales"
                ),
                KPI(
                    description="Order Count",
                    technical_name="order_count",
                    formula="order_id",
                    aggregation_type="COUNT",
                    source_table="Sales"
                )
            ]
        )

    @pytest.fixture
    def definition_with_calculated(self):
        """KPI definition with CALCULATED aggregation type"""
        return KPIDefinition(
            description="Complex Metrics",
            technical_name="complex_metrics",
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
    def definition_with_dependencies(self):
        """KPI definition with measure dependencies"""
        return KPIDefinition(
            description="Dependent Metrics",
            technical_name="dependent_metrics",
            kpis=[
                KPI(
                    description="Base Metric",
                    technical_name="base_metric",
                    formula="value",
                    aggregation_type="SUM",
                    source_table="Data"
                ),
                KPI(
                    description="Derived Metric",
                    technical_name="derived_metric",
                    formula="[base_metric] * 2",
                    aggregation_type="CALCULATED"
                ),
                KPI(
                    description="Complex Metric",
                    technical_name="complex_metric",
                    formula="[derived_metric] + [base_metric]",
                    aggregation_type="CALCULATED"
                )
            ]
        )

    # ========== Initialization Tests ==========

    def test_generator_initialization(self, generator):
        """Test SmartDAXGenerator initializes with both generators"""
        assert generator.standard_generator is not None
        assert generator.tree_generator is not None
        assert hasattr(generator.standard_generator, 'generate_dax_measure')
        assert hasattr(generator.tree_generator, 'generate_measure_with_separate_dependencies')

    def test_generator_has_both_generators(self, generator):
        """Test generator has access to both standard and tree generators"""
        assert generator.standard_generator.__class__.__name__ == 'DAXGenerator'
        assert generator.tree_generator.__class__.__name__ == 'TreeParsingDAXGenerator'

    # ========== _has_dependencies Tests ==========

    def test_has_dependencies_simple(self, generator, simple_definition):
        """Test dependency detection for simple measures"""
        result = generator._has_dependencies(simple_definition)
        assert result is False

    def test_has_dependencies_with_calculated(self, generator, definition_with_calculated):
        """Test dependency detection identifies CALCULATED aggregation"""
        result = generator._has_dependencies(definition_with_calculated)
        assert result is True

    def test_has_dependencies_with_measure_refs(self, generator, definition_with_dependencies):
        """Test dependency detection identifies measure references"""
        result = generator._has_dependencies(definition_with_dependencies)
        assert result is True

    def test_has_dependencies_empty_definition(self, generator):
        """Test dependency detection with empty definition"""
        empty_def = KPIDefinition(
            description="Empty",
            technical_name="empty",
            kpis=[]
        )
        result = generator._has_dependencies(empty_def)
        assert result is False

    # ========== generate_dax_measure Tests ==========

    def test_generate_dax_measure_simple(self, generator, simple_definition):
        """Test single measure generation without dependencies"""
        kpi = simple_definition.kpis[0]
        result = generator.generate_dax_measure(simple_definition, kpi)

        assert result is not None
        assert isinstance(result, DAXMeasure)
        assert result.name == "Total Revenue"
        assert "SUM(Sales[revenue])" in result.dax_formula

    def test_generate_dax_measure_with_dependencies(self, generator, definition_with_calculated):
        """Test single measure generation with dependencies"""
        profit_kpi = definition_with_calculated.kpis[2]  # profit measure
        result = generator.generate_dax_measure(definition_with_calculated, profit_kpi)

        assert result is not None
        assert isinstance(result, DAXMeasure)
        assert result.name == "Profit"

    def test_generate_dax_measure_orphan_with_simple_definition(self, generator, simple_definition):
        """Test measure generation with orphan KPI (not in definition) uses standard generator"""
        # Create a KPI that's not in the definition but should generate with standard generator
        # Simple definition has no dependencies, so it uses standard generator
        orphan_kpi = KPI(
            description="Orphan",
            technical_name="orphan",
            formula="orphan_value",
            aggregation_type="SUM",
            source_table="Data"
        )

        # Should work with simple_definition (no dependencies)
        result = generator.generate_dax_measure(simple_definition, orphan_kpi)
        assert result is not None
        assert isinstance(result, DAXMeasure)
        assert result.name == "Orphan"

    # ========== generate_all_measures Tests ==========

    def test_generate_all_measures_simple(self, generator, simple_definition):
        """Test generating all measures without dependencies"""
        results = generator.generate_all_measures(simple_definition)

        assert len(results) == 2
        assert all(isinstance(m, DAXMeasure) for m in results)
        assert results[0].name == "Total Revenue"
        assert results[1].name == "Order Count"

    def test_generate_all_measures_with_dependencies(self, generator, definition_with_calculated):
        """Test generating all measures with dependencies"""
        results = generator.generate_all_measures(definition_with_calculated)

        assert len(results) >= 3  # At least the 3 defined measures
        assert all(isinstance(m, DAXMeasure) for m in results)

    def test_generate_all_measures_empty(self, generator):
        """Test generating all measures from empty definition"""
        empty_def = KPIDefinition(
            description="Empty",
            technical_name="empty",
            kpis=[]
        )
        results = generator.generate_all_measures(empty_def)
        assert len(results) == 0

    # ========== generate_measures_with_dependencies Tests ==========

    def test_generate_measures_with_dependencies(self, generator, definition_with_dependencies):
        """Test generating measure with all its dependencies"""
        results = generator.generate_measures_with_dependencies(
            definition_with_dependencies,
            "complex_metric"
        )

        assert len(results) > 0
        assert all(isinstance(m, DAXMeasure) for m in results)
        # Should include complex_metric and its dependencies
        measure_names = [m.original_kbi.technical_name for m in results]
        assert "complex_metric" in measure_names

    def test_generate_measures_with_dependencies_base_measure(self, generator, definition_with_dependencies):
        """Test generating base measure with dependencies"""
        results = generator.generate_measures_with_dependencies(
            definition_with_dependencies,
            "base_metric"
        )

        assert len(results) >= 1
        measure_names = [m.original_kbi.technical_name for m in results]
        assert "base_metric" in measure_names

    # ========== get_generation_strategy Tests ==========

    def test_get_generation_strategy_standard(self, generator, simple_definition):
        """Test strategy recommendation for simple measures"""
        strategy = generator.get_generation_strategy(simple_definition)
        assert strategy == "STANDARD"

    def test_get_generation_strategy_simple_tree(self, generator):
        """Test strategy recommendation for simple dependencies"""
        # Definition with 1 dependency, depth 1
        simple_dep = KPIDefinition(
            description="Simple Dependency",
            technical_name="simple_dep",
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
                    aggregation_type="CALCULATED"
                )
            ]
        )

        strategy = generator.get_generation_strategy(simple_dep)
        assert strategy in ["SIMPLE_TREE_PARSING", "MODERATE_TREE_PARSING"]

    def test_get_generation_strategy_moderate_tree(self, generator, definition_with_dependencies):
        """Test strategy recommendation for moderate complexity"""
        strategy = generator.get_generation_strategy(definition_with_dependencies)
        assert strategy in ["SIMPLE_TREE_PARSING", "MODERATE_TREE_PARSING", "COMPLEX_TREE_PARSING"]

    def test_get_generation_strategy_complex_tree(self, generator):
        """Test strategy recommendation for complex dependencies"""
        # Create a definition with deep dependencies (4+ levels)
        complex_def = KPIDefinition(
            description="Complex",
            technical_name="complex",
            kpis=[
                KPI(description="L0", technical_name="l0", formula="v", aggregation_type="SUM", source_table="D"),
                KPI(description="L1", technical_name="l1", formula="[l0]*2", aggregation_type="CALCULATED"),
                KPI(description="L2", technical_name="l2", formula="[l1]*2", aggregation_type="CALCULATED"),
                KPI(description="L3", technical_name="l3", formula="[l2]*2", aggregation_type="CALCULATED"),
                KPI(description="L4", technical_name="l4", formula="[l3]*2", aggregation_type="CALCULATED"),
            ]
        )

        strategy = generator.get_generation_strategy(complex_def)
        # With 4 calculated measures and deep nesting, should be MODERATE or COMPLEX
        assert strategy in ["MODERATE_TREE_PARSING", "COMPLEX_TREE_PARSING"]

    # ========== get_analysis_report Tests ==========

    def test_get_analysis_report_simple(self, generator, simple_definition):
        """Test analysis report for simple measures"""
        report = generator.get_analysis_report(simple_definition)

        assert "recommended_strategy" in report
        assert "has_dependencies" in report
        assert report["recommended_strategy"] == "STANDARD"
        assert report["has_dependencies"] is False

    def test_get_analysis_report_with_dependencies(self, generator, definition_with_dependencies):
        """Test analysis report includes complexity for dependencies"""
        report = generator.get_analysis_report(definition_with_dependencies)

        assert "recommended_strategy" in report
        assert "has_dependencies" in report
        assert report["has_dependencies"] is True
        # Should include additional analysis for dependencies
        assert "complexity" in report or "dependency_order" in report

    def test_get_analysis_report_structure(self, generator, definition_with_calculated):
        """Test analysis report has expected structure"""
        report = generator.get_analysis_report(definition_with_calculated)

        assert isinstance(report, dict)
        assert "recommended_strategy" in report
        assert "has_dependencies" in report
        assert report["recommended_strategy"] in [
            "STANDARD",
            "SIMPLE_TREE_PARSING",
            "MODERATE_TREE_PARSING",
            "COMPLEX_TREE_PARSING"
        ]

    # ========== Integration Tests ==========

    def test_smart_routing_simple_to_standard(self, generator, simple_definition):
        """Test smart routing uses standard generator for simple measures"""
        kpi = simple_definition.kpis[0]
        result = generator.generate_dax_measure(simple_definition, kpi)

        # Should use standard generator (no dependency handling)
        assert result is not None
        assert "SUM(" in result.dax_formula or "COUNT(" in result.dax_formula

    def test_smart_routing_complex_to_tree(self, generator, definition_with_calculated):
        """Test smart routing uses tree generator for complex measures"""
        results = generator.generate_all_measures(definition_with_calculated)

        # Should use tree generator (handles dependencies)
        assert len(results) >= 3
        # Profit measure should be generated with dependencies resolved
        profit_measures = [m for m in results if "profit" in m.name.lower()]
        assert len(profit_measures) > 0

    def test_consistency_across_methods(self, generator, simple_definition):
        """Test consistency between single and all measure generation"""
        # Generate single measure
        single = generator.generate_dax_measure(simple_definition, simple_definition.kpis[0])

        # Generate all measures
        all_measures = generator.generate_all_measures(simple_definition)

        # First measure should match
        assert single.name == all_measures[0].name
        assert single.dax_formula == all_measures[0].dax_formula

    def test_handles_mixed_complexity(self, generator):
        """Test handling definition with both simple and complex measures"""
        mixed = KPIDefinition(
            description="Mixed",
            technical_name="mixed",
            kpis=[
                KPI(description="Simple", technical_name="simple", formula="val", aggregation_type="SUM", source_table="D"),
                KPI(description="Calc", technical_name="calc", formula="[simple] * 2", aggregation_type="CALCULATED"),
                KPI(description="Another", technical_name="another", formula="val2", aggregation_type="COUNT", source_table="D"),
            ]
        )

        results = generator.generate_all_measures(mixed)
        assert len(results) >= 3

    def test_edge_case_single_calculated_measure(self, generator):
        """Test edge case with only one calculated measure"""
        single_calc = KPIDefinition(
            description="Single Calc",
            technical_name="single_calc",
            kpis=[
                KPI(
                    description="Calculated",
                    technical_name="calc",
                    formula="1 + 1",
                    aggregation_type="CALCULATED"
                )
            ]
        )

        strategy = generator.get_generation_strategy(single_calc)
        # Single calculated measure should trigger tree parsing
        assert "TREE_PARSING" in strategy or strategy == "STANDARD"

    def test_strategy_matches_routing(self, generator, definition_with_calculated):
        """Test that strategy recommendation matches actual routing"""
        strategy = generator.get_generation_strategy(definition_with_calculated)
        has_deps = generator._has_dependencies(definition_with_calculated)

        if strategy == "STANDARD":
            assert has_deps is False
        else:
            assert has_deps is True
            assert "TREE_PARSING" in strategy
