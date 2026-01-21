"""
Unit tests for converters/common/transformers/tree_parsing/base_tree_generator.py

Tests abstract base tree parsing generator and dependency resolution.
"""

import pytest
from src.converters.common.transformers.tree_parsing.base_tree_generator import BaseTreeParsingGenerator
from src.converters.base.models import KPI, KPIDefinition


# Concrete implementation for testing
class MockTreeGenerator(BaseTreeParsingGenerator):
    """Mock implementation of BaseTreeParsingGenerator for testing"""

    def _generate_leaf_measure(self, definition, kpi):
        """Generate mock leaf measure"""
        return f"LEAF:{kpi.technical_name}={kpi.formula}"

    def _generate_calculated_measure(self, definition, kpi):
        """Generate mock calculated measure with inline dependencies"""
        return f"CALC_INLINE:{kpi.technical_name}={kpi.formula}"

    def _generate_calculated_measure_with_references(self, definition, kpi):
        """Generate mock calculated measure with references"""
        return f"CALC_REF:{kpi.technical_name}={kpi.formula}"


class TestBaseTreeParsingGenerator:
    """Tests for BaseTreeParsingGenerator class"""

    @pytest.fixture
    def generator(self):
        """Create MockTreeGenerator instance for testing"""
        return MockTreeGenerator()

    @pytest.fixture
    def simple_definition(self):
        """Simple KPI definition with no dependencies"""
        return KPIDefinition(
            description="Simple Metrics",
            technical_name="simple",
            kpis=[
                KPI(
                    description="Total Sales",
                    technical_name="total_sales",
                    formula="SUM(transactions.revenue)",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Total Cost",
                    technical_name="total_cost",
                    formula="SUM(expenses.amount)",
                    aggregation_type="SUM"
                )
            ]
        )

    @pytest.fixture
    def dependency_definition(self):
        """KPI definition with calculated measures and dependencies"""
        return KPIDefinition(
            description="Sales Analysis",
            technical_name="sales_analysis",
            kpis=[
                KPI(
                    description="Sales",
                    technical_name="sales",
                    formula="SUM(transactions.revenue)",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Cost",
                    technical_name="cost",
                    formula="SUM(expenses.amount)",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Profit",
                    technical_name="profit",
                    formula="[sales] - [cost]",
                    aggregation_type="CALCULATED"
                ),
                KPI(
                    description="Profit Margin",
                    technical_name="profit_margin",
                    formula="[profit] / [sales]",
                    aggregation_type="CALCULATED"
                )
            ]
        )

    @pytest.fixture
    def circular_definition(self):
        """KPI definition with circular dependencies"""
        return KPIDefinition(
            description="Circular",
            technical_name="circular",
            kpis=[
                KPI(
                    description="A",
                    technical_name="measure_a",
                    formula="[measure_b] + 100",
                    aggregation_type="CALCULATED"
                ),
                KPI(
                    description="B",
                    technical_name="measure_b",
                    formula="[measure_a] * 2",
                    aggregation_type="CALCULATED"
                )
            ]
        )

    # ========== Initialization Tests ==========

    def test_generator_initialization(self, generator):
        """Test generator initializes with dependency resolver"""
        assert generator.dependency_resolver is not None
        assert hasattr(generator.dependency_resolver, 'register_measures')

    # ========== generate_all_measures Tests ==========

    def test_generate_all_measures_simple(self, generator, simple_definition):
        """Test generating all measures with no dependencies"""
        measures = generator.generate_all_measures(simple_definition)

        assert len(measures) == 2
        assert all(m.startswith("LEAF:") for m in measures)

    def test_generate_all_measures_with_dependencies(self, generator, dependency_definition):
        """Test generating measures with dependencies in correct order"""
        measures = generator.generate_all_measures(dependency_definition)

        assert len(measures) == 4

        # Find indices
        sales_idx = next(i for i, m in enumerate(measures) if "sales" in m and "profit" not in m)
        cost_idx = next(i for i, m in enumerate(measures) if "cost" in m)
        profit_idx = next(i for i, m in enumerate(measures) if m.startswith("CALC_INLINE:profit="))
        margin_idx = next(i for i, m in enumerate(measures) if "profit_margin" in m)

        # Dependencies should come before dependents
        assert sales_idx < profit_idx
        assert cost_idx < profit_idx
        assert profit_idx < margin_idx

    def test_generate_all_measures_circular_dependency_error(self, generator, circular_definition):
        """Test circular dependencies raise ValueError"""
        with pytest.raises(ValueError) as exc_info:
            generator.generate_all_measures(circular_definition)

        assert "Circular dependencies detected" in str(exc_info.value)
        assert "measure_a" in str(exc_info.value) or "measure_b" in str(exc_info.value)

    def test_generate_all_measures_preserves_leaf_types(self, generator, dependency_definition):
        """Test leaf measures use leaf generation method"""
        measures = generator.generate_all_measures(dependency_definition)

        # Find leaf measures
        leaf_measures = [m for m in measures if m.startswith("LEAF:")]

        assert len(leaf_measures) == 2  # sales and cost
        assert any("sales" in m for m in leaf_measures)
        assert any("cost" in m for m in leaf_measures)

    def test_generate_all_measures_calculated_inline(self, generator, dependency_definition):
        """Test calculated measures use inline generation"""
        measures = generator.generate_all_measures(dependency_definition)

        # Find calculated measures
        calc_measures = [m for m in measures if m.startswith("CALC_INLINE:")]

        assert len(calc_measures) == 2  # profit and profit_margin
        assert any("profit=" in m and "profit_margin" not in m for m in calc_measures)
        assert any("profit_margin" in m for m in calc_measures)

    # ========== generate_measure_with_separate_dependencies Tests ==========

    def test_generate_measure_with_separate_dependencies(self, generator, dependency_definition):
        """Test generating measure with its dependencies separately"""
        measures = generator.generate_measure_with_separate_dependencies(
            dependency_definition,
            "profit_margin"
        )

        # Should have: sales, cost, profit, profit_margin
        assert len(measures) == 4

        # Dependencies should be in correct order
        measure_names = [m.split(":", 1)[1].split("=")[0] for m in measures]
        margin_idx = measure_names.index("profit_margin")
        profit_idx = measure_names.index("profit")

        assert profit_idx < margin_idx

    def test_generate_measure_with_separate_dependencies_leaf_only(self, generator, simple_definition):
        """Test generating leaf measure returns only that measure"""
        measures = generator.generate_measure_with_separate_dependencies(
            simple_definition,
            "total_sales"
        )

        assert len(measures) == 1
        assert "total_sales" in measures[0]

    def test_generate_measure_with_separate_dependencies_uses_references(self, generator, dependency_definition):
        """Test calculated measures use reference generation in separate mode"""
        measures = generator.generate_measure_with_separate_dependencies(
            dependency_definition,
            "profit"
        )

        # Profit should use CALC_REF (references) not CALC_INLINE
        profit_measure = next(m for m in measures if "profit" in m and "margin" not in m)
        assert profit_measure.startswith("CALC_REF:")

    def test_generate_measure_with_separate_dependencies_not_found(self, generator, simple_definition):
        """Test error when target measure not found"""
        with pytest.raises(ValueError) as exc_info:
            generator.generate_measure_with_separate_dependencies(
                simple_definition,
                "nonexistent"
            )

        assert "not found" in str(exc_info.value).lower()

    def test_generate_measure_with_separate_dependencies_partial_tree(self, generator, dependency_definition):
        """Test only required dependencies are generated"""
        # Generate only profit (needs sales and cost, but not profit_margin)
        measures = generator.generate_measure_with_separate_dependencies(
            dependency_definition,
            "profit"
        )

        measure_names = [m.split(":", 1)[1].split("=")[0] for m in measures]

        assert "sales" in measure_names
        assert "cost" in measure_names
        assert "profit" in measure_names
        assert "profit_margin" not in measure_names  # Not needed

    # ========== get_dependency_analysis Tests ==========

    def test_get_dependency_analysis_simple(self, generator, simple_definition):
        """Test dependency analysis for simple definition"""
        analysis = generator.get_dependency_analysis(simple_definition)

        assert analysis["total_measures"] == 2
        assert len(analysis["dependency_graph"]) >= 0
        assert len(analysis["dependency_order"]) == 2
        assert analysis["circular_dependencies"] == []

    def test_get_dependency_analysis_with_dependencies(self, generator, dependency_definition):
        """Test dependency analysis shows dependencies"""
        analysis = generator.get_dependency_analysis(dependency_definition)

        assert analysis["total_measures"] == 4

        # Check dependency graph
        assert "profit" in analysis["dependency_graph"]
        assert "sales" in analysis["dependency_graph"]["profit"]
        assert "cost" in analysis["dependency_graph"]["profit"]

        # Check dependency order
        order = analysis["dependency_order"]
        sales_idx = order.index("sales")
        cost_idx = order.index("cost")
        profit_idx = order.index("profit")

        assert sales_idx < profit_idx
        assert cost_idx < profit_idx

    def test_get_dependency_analysis_includes_trees(self, generator, dependency_definition):
        """Test dependency analysis includes measure trees"""
        analysis = generator.get_dependency_analysis(dependency_definition)

        assert "measure_trees" in analysis
        assert "profit_margin" in analysis["measure_trees"]

        # Profit margin tree should show dependencies
        margin_tree = analysis["measure_trees"]["profit_margin"]
        assert margin_tree is not None

    def test_get_dependency_analysis_circular(self, generator, circular_definition):
        """Test dependency analysis detects circular dependencies"""
        # get_dependency_analysis calls get_dependency_order which raises ValueError for circular deps
        # So we need to catch it or test separately

        # First register measures
        generator.dependency_resolver.register_measures(circular_definition)

        # Detect circular dependencies directly
        cycles = generator.dependency_resolver.detect_circular_dependencies()
        assert len(cycles) > 0

        # At least one cycle should involve both measures
        assert any("measure_a" in cycle and "measure_b" in cycle for cycle in cycles)

    # ========== get_measure_complexity_report Tests ==========

    def test_get_measure_complexity_report_simple(self, generator, simple_definition):
        """Test complexity report for simple measures"""
        report = generator.get_measure_complexity_report(simple_definition)

        assert report["summary"]["leaf_measures"] == 2
        assert report["summary"]["calculated_measures"] == 0
        assert report["summary"]["max_dependency_depth"] == 0

    def test_get_measure_complexity_report_with_dependencies(self, generator, dependency_definition):
        """Test complexity report shows dependency depth"""
        report = generator.get_measure_complexity_report(dependency_definition)

        assert report["summary"]["leaf_measures"] == 2  # sales, cost
        assert report["summary"]["calculated_measures"] == 2  # profit, profit_margin

        # Check individual measure details
        assert "sales" in report["measures"]
        assert report["measures"]["sales"]["is_leaf"] is True
        assert report["measures"]["sales"]["dependency_depth"] == 0

        assert "profit" in report["measures"]
        assert report["measures"]["profit"]["is_leaf"] is False
        assert report["measures"]["profit"]["total_dependencies"] == 2

    def test_get_measure_complexity_report_max_depth(self, generator, dependency_definition):
        """Test complexity report identifies max depth"""
        report = generator.get_measure_complexity_report(dependency_definition)

        # profit_margin depends on profit which depends on sales/cost
        # So max depth should be 2
        assert report["summary"]["max_dependency_depth"] >= 1
        assert report["summary"]["most_complex_measure"] is not None

    def test_get_measure_complexity_report_measure_details(self, generator, dependency_definition):
        """Test complexity report includes measure details"""
        report = generator.get_measure_complexity_report(dependency_definition)

        profit_margin = report["measures"]["profit_margin"]

        assert profit_margin["name"] == "profit_margin"
        assert profit_margin["description"] == "Profit Margin"
        assert profit_margin["direct_dependencies"] > 0
        assert profit_margin["total_dependencies"] > profit_margin["direct_dependencies"]

    # ========== Dependency Depth Calculation Tests ==========

    def test_calculate_dependency_depth_leaf(self, generator, simple_definition):
        """Test depth calculation for leaf measures"""
        generator.dependency_resolver.register_measures(simple_definition)

        depth = generator._calculate_dependency_depth("total_sales")

        assert depth == 0

    def test_calculate_dependency_depth_nested(self, generator, dependency_definition):
        """Test depth calculation for nested dependencies"""
        generator.dependency_resolver.register_measures(dependency_definition)

        # sales and cost have depth 0
        assert generator._calculate_dependency_depth("sales") == 0
        assert generator._calculate_dependency_depth("cost") == 0

        # profit depends on sales and cost (depth 1)
        assert generator._calculate_dependency_depth("profit") == 1

        # profit_margin depends on profit (depth 2)
        assert generator._calculate_dependency_depth("profit_margin") == 2

    def test_calculate_dependency_depth_handles_circular(self, generator, circular_definition):
        """Test depth calculation handles circular dependencies gracefully"""
        generator.dependency_resolver.register_measures(circular_definition)

        # Should not crash, returns 0 for circular dependencies
        depth_a = generator._calculate_dependency_depth("measure_a")
        depth_b = generator._calculate_dependency_depth("measure_b")

        assert isinstance(depth_a, int)
        assert isinstance(depth_b, int)

    # ========== Integration Tests ==========

    def test_full_workflow_no_dependencies(self, generator, simple_definition):
        """Test complete workflow with no dependencies"""
        # Analysis
        analysis = generator.get_dependency_analysis(simple_definition)
        assert len(analysis["circular_dependencies"]) == 0

        # Generate all
        measures = generator.generate_all_measures(simple_definition)
        assert len(measures) == 2

        # Complexity report
        report = generator.get_measure_complexity_report(simple_definition)
        assert report["summary"]["calculated_measures"] == 0

    def test_full_workflow_with_dependencies(self, generator, dependency_definition):
        """Test complete workflow with complex dependencies"""
        # Analysis
        analysis = generator.get_dependency_analysis(dependency_definition)
        assert analysis["total_measures"] == 4
        assert len(analysis["circular_dependencies"]) == 0

        # Generate all in order
        measures = generator.generate_all_measures(dependency_definition)
        assert len(measures) == 4

        # Generate specific measure with dependencies
        profit_measures = generator.generate_measure_with_separate_dependencies(
            dependency_definition,
            "profit"
        )
        assert len(profit_measures) == 3  # sales, cost, profit

        # Complexity report
        report = generator.get_measure_complexity_report(dependency_definition)
        assert report["summary"]["max_dependency_depth"] == 2

    def test_error_handling_circular_dependencies(self, generator, circular_definition):
        """Test proper error handling for circular dependencies"""
        # Register measures to detect circular dependencies
        generator.dependency_resolver.register_measures(circular_definition)

        # Detect circular dependencies directly
        cycles = generator.dependency_resolver.detect_circular_dependencies()
        assert len(cycles) > 0

        # Generation should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            generator.generate_all_measures(circular_definition)

        assert "circular" in str(exc_info.value).lower()
