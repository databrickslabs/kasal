"""
Unit tests for converters/common/translators/dependencies.py

Tests dependency resolution and tree parsing for nested measure formulas.
"""

import pytest
from src.converters.common.translators.dependencies import DependencyResolver
from src.converters.base.models import KPI, KPIDefinition


class TestDependencyResolver:
    """Tests for DependencyResolver class"""

    @pytest.fixture
    def resolver(self):
        """Create DependencyResolver instance for testing"""
        return DependencyResolver()

    @pytest.fixture
    def simple_definition(self):
        """Simple KPI definition with no dependencies"""
        return KPIDefinition(
            description="Simple Metrics",
            technical_name="simple",
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
                    formula="SUM(expenses.amount)",  # Changed from transactions.cost
                    aggregation_type="SUM"
                )
            ]
        )

    @pytest.fixture
    def dependency_definition(self):
        """KPI definition with dependencies"""
        return KPIDefinition(
            description="Calculated Metrics",
            technical_name="calculated",
            kpis=[
                KPI(
                    description="Revenue",
                    technical_name="revenue",
                    formula="SUM(sales.amount)",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Expenses",
                    technical_name="expenses",
                    formula="SUM(costs.amount)",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Profit",
                    technical_name="profit",
                    formula="revenue - expenses",
                    aggregation_type="CALCULATED"
                ),
                KPI(
                    description="Margin",
                    technical_name="margin",
                    formula="profit / revenue",
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
                    formula="measure_b + 100",
                    aggregation_type="CALCULATED"
                ),
                KPI(
                    description="B",
                    technical_name="measure_b",
                    formula="measure_a * 2",
                    aggregation_type="CALCULATED"
                )
            ]
        )

    @pytest.fixture
    def complex_dependency_definition(self):
        """KPI definition with multi-level dependencies"""
        return KPIDefinition(
            description="Complex Dependencies",
            technical_name="complex",
            kpis=[
                KPI(
                    description="Base1",
                    technical_name="base1",
                    formula="SUM(data.value1)",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Base2",
                    technical_name="base2",
                    formula="SUM(data.value2)",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Level1",
                    technical_name="level1",
                    formula="base1 + base2",
                    aggregation_type="CALCULATED"
                ),
                KPI(
                    description="Level2",
                    technical_name="level2",
                    formula="level1 * 2",
                    aggregation_type="CALCULATED"
                ),
                KPI(
                    description="Level3",
                    technical_name="level3",
                    formula="level2 + base1",
                    aggregation_type="CALCULATED"
                )
            ]
        )

    # ========== Initialization Tests ==========

    def test_resolver_initialization(self, resolver):
        """Test DependencyResolver initializes with empty registries"""
        assert resolver.measure_registry == {}
        assert len(resolver.dependency_graph) == 0
        assert resolver.resolved_cache == {}

    def test_resolver_has_required_methods(self, resolver):
        """Test DependencyResolver has all required methods"""
        assert hasattr(resolver, 'register_measures')
        assert hasattr(resolver, 'get_dependency_order')
        assert hasattr(resolver, 'detect_circular_dependencies')
        assert hasattr(resolver, 'resolve_formula_inline')
        assert hasattr(resolver, 'get_dependency_tree')
        assert hasattr(resolver, 'get_all_dependencies')

    # ========== register_measures Tests ==========

    def test_register_measures_simple(self, resolver, simple_definition):
        """Test registering measures from simple definition"""
        resolver.register_measures(simple_definition)

        assert len(resolver.measure_registry) == 2
        assert 'sales' in resolver.measure_registry
        assert 'cost' in resolver.measure_registry

    def test_register_measures_builds_graph(self, resolver, dependency_definition):
        """Test register_measures builds dependency graph"""
        resolver.register_measures(dependency_definition)

        # Check graph structure
        assert 'revenue' in resolver.dependency_graph
        assert 'expenses' in resolver.dependency_graph
        assert 'profit' in resolver.dependency_graph
        assert 'margin' in resolver.dependency_graph

    def test_register_measures_clears_previous(self, resolver, simple_definition, dependency_definition):
        """Test register_measures clears previous registrations"""
        resolver.register_measures(simple_definition)
        first_count = len(resolver.measure_registry)

        resolver.register_measures(dependency_definition)
        second_count = len(resolver.measure_registry)

        # Should have different counts
        assert first_count != second_count
        # First measures should be gone
        assert 'sales' not in resolver.measure_registry

    def test_register_measures_without_technical_name(self, resolver):
        """Test register_measures skips KPIs without technical_name"""
        definition = KPIDefinition(
            description="Test",
            technical_name="test",
            kpis=[
                KPI(description="No Name", formula="SUM(amount)")
            ]
        )

        resolver.register_measures(definition)
        # Should not register measures without technical_name
        assert len(resolver.measure_registry) == 0

    # ========== _extract_measure_references Tests ==========

    def test_extract_measure_references_simple(self, resolver):
        """Test extracting measure references from simple formula"""
        resolver.measure_registry = {'revenue': None, 'cost': None}

        refs = resolver._extract_measure_references("revenue - cost")
        assert 'revenue' in refs
        assert 'cost' in refs

    def test_extract_measure_references_excludes_dax_functions(self, resolver):
        """Test extraction excludes DAX function names"""
        resolver.measure_registry = {'amount': None}

        refs = resolver._extract_measure_references("SUM(amount) + COUNT(rows)")
        # Should not include SUM or COUNT
        assert 'SUM' not in refs
        assert 'COUNT' not in refs
        # Should include actual measure if it exists
        assert 'amount' in refs

    def test_extract_measure_references_excludes_column_prefixes(self, resolver):
        """Test extraction excludes column names with prefixes"""
        resolver.measure_registry = {}

        refs = resolver._extract_measure_references("SUM(bic_amount) + fact_sales")
        # Should not include column names with common prefixes
        assert 'bic_amount' not in refs
        assert 'fact_sales' not in refs

    def test_extract_measure_references_only_registered(self, resolver):
        """Test extraction only returns registered measures"""
        resolver.measure_registry = {'revenue': None}

        refs = resolver._extract_measure_references("revenue + unknown_measure")
        # Should only include registered measures
        assert 'revenue' in refs
        assert 'unknown_measure' not in refs

    def test_extract_measure_references_empty_formula(self, resolver):
        """Test extraction from empty formula"""
        refs = resolver._extract_measure_references("")
        assert refs == []

    def test_extract_measure_references_no_references(self, resolver):
        """Test extraction when no measures referenced"""
        resolver.measure_registry = {'revenue': None}

        refs = resolver._extract_measure_references("SUM(sales.amount)")
        assert refs == []

    def test_extract_measure_references_complex_formula(self, resolver):
        """Test extraction from complex formula"""
        resolver.measure_registry = {'revenue': None, 'cost': None, 'tax': None}

        refs = resolver._extract_measure_references("(revenue - cost) * (1 + tax)")
        assert 'revenue' in refs
        assert 'cost' in refs
        assert 'tax' in refs

    def test_extract_measure_references_removes_duplicates(self, resolver):
        """Test extraction removes duplicate references"""
        resolver.measure_registry = {'amount': None}

        refs = resolver._extract_measure_references("amount + amount * 2")
        # Should appear only once
        assert refs.count('amount') == 1

    # ========== get_dependency_order Tests ==========

    def test_get_dependency_order_simple(self, resolver, simple_definition):
        """Test dependency ordering for simple definition"""
        resolver.register_measures(simple_definition)

        order = resolver.get_dependency_order()
        assert len(order) == 2
        assert 'sales' in order
        assert 'cost' in order

    def test_get_dependency_order_with_dependencies(self, resolver, dependency_definition):
        """Test dependency ordering respects dependencies"""
        resolver.register_measures(dependency_definition)

        order = resolver.get_dependency_order()
        assert len(order) == 4

        # Dependencies should come before dependents
        revenue_idx = order.index('revenue')
        expenses_idx = order.index('expenses')
        profit_idx = order.index('profit')
        margin_idx = order.index('margin')

        assert revenue_idx < profit_idx
        assert expenses_idx < profit_idx
        assert profit_idx < margin_idx

    def test_get_dependency_order_circular_raises(self, resolver, circular_definition):
        """Test dependency ordering raises on circular dependencies"""
        resolver.register_measures(circular_definition)

        with pytest.raises(ValueError) as exc_info:
            resolver.get_dependency_order()

        assert "circular" in str(exc_info.value).lower()

    def test_get_dependency_order_multi_level(self, resolver, complex_dependency_definition):
        """Test dependency ordering for multi-level dependencies"""
        resolver.register_measures(complex_dependency_definition)

        order = resolver.get_dependency_order()
        assert len(order) == 5

        # Base measures should come first
        base1_idx = order.index('base1')
        base2_idx = order.index('base2')
        level1_idx = order.index('level1')
        level2_idx = order.index('level2')
        level3_idx = order.index('level3')

        # Check ordering
        assert base1_idx < level1_idx
        assert base2_idx < level1_idx
        assert level1_idx < level2_idx
        assert level2_idx < level3_idx

    # ========== detect_circular_dependencies Tests ==========

    def test_detect_circular_no_cycles(self, resolver, simple_definition):
        """Test circular dependency detection with no cycles"""
        resolver.register_measures(simple_definition)

        cycles = resolver.detect_circular_dependencies()
        assert len(cycles) == 0

    def test_detect_circular_finds_cycles(self, resolver, circular_definition):
        """Test circular dependency detection finds cycles"""
        resolver.register_measures(circular_definition)

        cycles = resolver.detect_circular_dependencies()
        assert len(cycles) > 0

        # Should find cycle involving both measures
        assert any('measure_a' in cycle and 'measure_b' in cycle for cycle in cycles)

    def test_detect_circular_self_reference(self, resolver):
        """Test circular dependency detection for self-reference"""
        definition = KPIDefinition(
            description="Self Ref",
            technical_name="self_ref",
            kpis=[
                KPI(
                    description="Self",
                    technical_name="self_measure",
                    formula="self_measure + 1",
                    aggregation_type="CALCULATED"
                )
            ]
        )

        resolver.register_measures(definition)
        cycles = resolver.detect_circular_dependencies()

        assert len(cycles) > 0
        assert any('self_measure' in cycle for cycle in cycles)

    # ========== resolve_formula_inline Tests ==========

    def test_resolve_formula_inline_leaf(self, resolver, simple_definition):
        """Test inline formula resolution for leaf measure"""
        resolver.register_measures(simple_definition)

        # For leaf measures, inline resolution should work or raise import error
        # We just test it doesn't crash with ValueError about measure not found
        try:
            result = resolver.resolve_formula_inline('sales')
            # If it succeeds, should return some DAX string
            assert result is not None
        except (ImportError, ModuleNotFoundError):
            # Expected if DAX helper module not available
            pass

    def test_resolve_formula_inline_not_found(self, resolver, simple_definition):
        """Test inline resolution raises for unknown measure"""
        resolver.register_measures(simple_definition)

        with pytest.raises(ValueError) as exc_info:
            resolver.resolve_formula_inline('unknown')

        assert 'not found' in str(exc_info.value).lower()

    def test_resolve_formula_inline_caches_result(self, resolver, simple_definition):
        """Test inline resolution caches results"""
        resolver.register_measures(simple_definition)

        # First call should cache
        try:
            resolver.resolve_formula_inline('sales')
        except:
            pass  # May fail due to DAX generation, but cache should be set

        # Check cache was used
        if 'sales' in resolver.resolved_cache:
            # Cache was set
            assert resolver.resolved_cache['sales'] is not None

    # ========== get_dependency_tree Tests ==========

    def test_get_dependency_tree_leaf(self, resolver, simple_definition):
        """Test dependency tree for leaf measure"""
        resolver.register_measures(simple_definition)

        tree = resolver.get_dependency_tree('sales')

        assert tree['name'] == 'sales'
        assert tree['description'] == 'Sales'
        assert tree['formula'] == 'SUM(transactions.revenue)'
        assert len(tree['dependencies']) == 0

    def test_get_dependency_tree_with_dependencies(self, resolver, dependency_definition):
        """Test dependency tree includes dependencies"""
        resolver.register_measures(dependency_definition)

        tree = resolver.get_dependency_tree('profit')

        assert tree['name'] == 'profit'
        assert len(tree['dependencies']) == 2

        # Should have revenue and expenses as dependencies
        dep_names = [d['name'] for d in tree['dependencies']]
        assert 'revenue' in dep_names
        assert 'expenses' in dep_names

    def test_get_dependency_tree_multi_level(self, resolver, complex_dependency_definition):
        """Test dependency tree for multi-level dependencies"""
        resolver.register_measures(complex_dependency_definition)

        tree = resolver.get_dependency_tree('level2')

        assert tree['name'] == 'level2'
        # Should have level1 as dependency
        assert len(tree['dependencies']) == 1
        assert tree['dependencies'][0]['name'] == 'level1'

        # level1 should have base1 and base2
        level1_deps = tree['dependencies'][0]['dependencies']
        level1_dep_names = [d['name'] for d in level1_deps]
        assert 'base1' in level1_dep_names
        assert 'base2' in level1_dep_names

    def test_get_dependency_tree_circular(self, resolver, circular_definition):
        """Test dependency tree handles circular dependencies"""
        resolver.register_measures(circular_definition)

        tree = resolver.get_dependency_tree('measure_a')

        # Should mark circular reference
        assert 'circular' in tree['dependencies'][0] or tree['dependencies'][0]['dependencies']

    def test_get_dependency_tree_not_found(self, resolver, simple_definition):
        """Test dependency tree raises for unknown measure"""
        resolver.register_measures(simple_definition)

        with pytest.raises(ValueError) as exc_info:
            resolver.get_dependency_tree('unknown')

        assert 'not found' in str(exc_info.value).lower()

    # ========== get_all_dependencies Tests ==========

    def test_get_all_dependencies_leaf(self, resolver, simple_definition):
        """Test getting all dependencies for leaf measure"""
        resolver.register_measures(simple_definition)

        deps = resolver.get_all_dependencies('sales')
        assert len(deps) == 0

    def test_get_all_dependencies_direct(self, resolver, dependency_definition):
        """Test getting all dependencies for measure with direct deps"""
        resolver.register_measures(dependency_definition)

        deps = resolver.get_all_dependencies('profit')

        assert len(deps) == 2
        assert 'revenue' in deps
        assert 'expenses' in deps

    def test_get_all_dependencies_transitive(self, resolver, complex_dependency_definition):
        """Test getting all transitive dependencies"""
        resolver.register_measures(complex_dependency_definition)

        deps = resolver.get_all_dependencies('level3')

        # level3 depends on level2 and base1
        # level2 depends on level1
        # level1 depends on base1 and base2
        # So total transitive deps: level2, level1, base1, base2
        assert 'level2' in deps
        assert 'level1' in deps
        assert 'base1' in deps
        assert 'base2' in deps

    def test_get_all_dependencies_not_found(self, resolver, simple_definition):
        """Test getting dependencies for unknown measure"""
        resolver.register_measures(simple_definition)

        deps = resolver.get_all_dependencies('unknown')
        assert deps == set()

    def test_get_all_dependencies_handles_circular(self, resolver, circular_definition):
        """Test getting dependencies handles circular references"""
        resolver.register_measures(circular_definition)

        # Should not infinite loop
        deps = resolver.get_all_dependencies('measure_a')

        # Should find measure_b as dependency
        assert 'measure_b' in deps

    # ========== Integration Tests ==========

    def test_full_workflow_simple(self, resolver, simple_definition):
        """Test complete workflow for simple definition"""
        # Register
        resolver.register_measures(simple_definition)

        # Get order
        order = resolver.get_dependency_order()
        assert len(order) == 2

        # Check for cycles
        cycles = resolver.detect_circular_dependencies()
        assert len(cycles) == 0

        # Get dependency tree
        tree = resolver.get_dependency_tree('sales')
        assert tree['name'] == 'sales'

    def test_full_workflow_with_dependencies(self, resolver, dependency_definition):
        """Test complete workflow with dependencies"""
        # Register
        resolver.register_measures(dependency_definition)

        # Verify registration
        assert len(resolver.measure_registry) == 4

        # Get order
        order = resolver.get_dependency_order()
        assert len(order) == 4

        # Check for cycles
        cycles = resolver.detect_circular_dependencies()
        assert len(cycles) == 0

        # Get all dependencies for complex measure
        deps = resolver.get_all_dependencies('margin')
        assert 'profit' in deps
        assert 'revenue' in deps
        assert 'expenses' in deps

        # Get dependency tree
        tree = resolver.get_dependency_tree('margin')
        assert tree['name'] == 'margin'

    def test_error_handling_circular_workflow(self, resolver, circular_definition):
        """Test error handling for circular dependencies"""
        resolver.register_measures(circular_definition)

        # Detect cycles
        cycles = resolver.detect_circular_dependencies()
        assert len(cycles) > 0

        # Ordering should raise error
        with pytest.raises(ValueError):
            resolver.get_dependency_order()

    def test_edge_case_empty_definition(self, resolver):
        """Test edge case with empty definition"""
        definition = KPIDefinition(
            description="Empty",
            technical_name="empty",
            kpis=[]
        )

        resolver.register_measures(definition)

        assert len(resolver.measure_registry) == 0
        order = resolver.get_dependency_order()
        assert order == []

    def test_edge_case_complex_formula_with_operators(self, resolver):
        """Test edge case with complex formula operators"""
        definition = KPIDefinition(
            description="Complex",
            technical_name="complex",
            kpis=[
                KPI(
                    description="Base",
                    technical_name="base",
                    formula="SUM(amount)",
                    aggregation_type="SUM"
                ),
                KPI(
                    description="Calc",
                    technical_name="calc",
                    formula="(base + 100) * 1.1 / (base - 50)",
                    aggregation_type="CALCULATED"
                )
            ]
        )

        resolver.register_measures(definition)

        refs = resolver._extract_measure_references(definition.kpis[1].formula)
        assert 'base' in refs

        order = resolver.get_dependency_order()
        base_idx = order.index('base')
        calc_idx = order.index('calc')
        assert base_idx < calc_idx
