"""
Unit tests for converters/services/uc_metrics/helpers/uc_metrics_context.py

Tests UC Metrics KBI context tracking for Unity Catalog Metrics Store.
Mirrors SQL context patterns but adapted for UC Metrics specifics.
"""

import pytest
from src.converters.services.uc_metrics.helpers.uc_metrics_context import (
    UCBaseKBIContext,
    UCKBIContextCache
)
from src.converters.base.models import KPI


class TestUCBaseKBIContext:
    """Tests for UCBaseKBIContext class"""

    @pytest.fixture
    def simple_kbi(self):
        """Simple KBI without filters"""
        return KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM"
        )

    @pytest.fixture
    def kbi_with_filters(self):
        """KBI with filters"""
        return KPI(
            description="Filtered Revenue",
            technical_name="filtered_revenue",
            formula="amount",
            aggregation_type="SUM",
            filters=["status = 'active'", "region = 'US'"]
        )

    @pytest.fixture
    def kbi_with_constant_selection(self):
        """KBI with constant selection fields"""
        return KPI(
            description="Inventory",
            technical_name="inventory",
            formula="stock_level",
            aggregation_type="SUM",
            fields_for_constant_selection=["fiscal_period", "product_id"]
        )

    @pytest.fixture
    def kbi_with_exception_aggregation(self):
        """KBI with exception aggregation fields"""
        return KPI(
            description="Balance",
            technical_name="balance",
            formula="balance_amount",
            aggregation_type="SUM",
            fields_for_exception_aggregation=["fiscal_period"]
        )

    @pytest.fixture
    def parent_kbi(self):
        """Parent KBI with filters"""
        return KPI(
            description="YTD Revenue",
            technical_name="ytd_revenue",
            formula="[revenue]",
            aggregation_type="SUM",
            filters=["year = 2023"]
        )

    # ========== Initialization Tests ==========

    def test_context_initialization_simple(self, simple_kbi):
        """Test context initializes with simple KBI"""
        context = UCBaseKBIContext(kbi=simple_kbi)
        assert context.kbi == simple_kbi
        assert context.parent_kbis == []

    def test_context_initialization_with_parents(self, simple_kbi, parent_kbi):
        """Test context initializes with parent KBIs"""
        context = UCBaseKBIContext(kbi=simple_kbi, parent_kbis=[parent_kbi])
        assert context.kbi == simple_kbi
        assert len(context.parent_kbis) == 1
        assert context.parent_kbis[0] == parent_kbi

    def test_context_initialization_multiple_parents(self, simple_kbi, parent_kbi):
        """Test context initializes with multiple parent KBIs"""
        parent2 = KPI(
            description="QTD Revenue",
            technical_name="qtd_revenue",
            formula="[revenue]",
            aggregation_type="SUM"
        )
        context = UCBaseKBIContext(kbi=simple_kbi, parent_kbis=[parent_kbi, parent2])
        assert len(context.parent_kbis) == 2

    # ========== Magic Method Tests ==========

    def test_repr(self, simple_kbi):
        """Test string representation"""
        context = UCBaseKBIContext(kbi=simple_kbi)
        repr_str = repr(context)
        assert "revenue" in repr_str
        assert "ROOT" in repr_str

    def test_repr_with_parents(self, simple_kbi, parent_kbi):
        """Test string representation with parents"""
        context = UCBaseKBIContext(kbi=simple_kbi, parent_kbis=[parent_kbi])
        repr_str = repr(context)
        assert "ytd_revenue" in repr_str
        assert "revenue" in repr_str
        assert "→" in repr_str

    def test_eq_same_context(self, simple_kbi):
        """Test equality for same context"""
        context1 = UCBaseKBIContext(kbi=simple_kbi)
        context2 = UCBaseKBIContext(kbi=simple_kbi)
        assert context1 == context2

    def test_eq_different_context(self, simple_kbi, parent_kbi):
        """Test equality for different contexts"""
        context1 = UCBaseKBIContext(kbi=simple_kbi)
        context2 = UCBaseKBIContext(kbi=simple_kbi, parent_kbis=[parent_kbi])
        assert context1 != context2

    def test_eq_different_kbi(self, simple_kbi, parent_kbi):
        """Test equality for different KBIs"""
        context1 = UCBaseKBIContext(kbi=simple_kbi)
        context2 = UCBaseKBIContext(kbi=parent_kbi)
        assert context1 != context2

    def test_hash_consistent(self, simple_kbi):
        """Test hash is consistent"""
        context1 = UCBaseKBIContext(kbi=simple_kbi)
        context2 = UCBaseKBIContext(kbi=simple_kbi)
        assert hash(context1) == hash(context2)

    def test_hash_different_for_different_parents(self, simple_kbi, parent_kbi):
        """Test hash differs with different parents"""
        context1 = UCBaseKBIContext(kbi=simple_kbi)
        context2 = UCBaseKBIContext(kbi=simple_kbi, parent_kbis=[parent_kbi])
        assert hash(context1) != hash(context2)

    # ========== Property Tests ==========

    def test_id_simple(self, simple_kbi):
        """Test ID for simple context"""
        context = UCBaseKBIContext(kbi=simple_kbi)
        assert context.id == "revenue"

    def test_id_with_parent(self, simple_kbi, parent_kbi):
        """Test ID with parent KBI"""
        context = UCBaseKBIContext(kbi=simple_kbi, parent_kbis=[parent_kbi])
        assert "revenue" in context.id
        assert "ytd_revenue" in context.id

    def test_parent_kbis_chain_empty(self, simple_kbi):
        """Test parent KBIs chain with no parents"""
        context = UCBaseKBIContext(kbi=simple_kbi)
        assert context.parent_kbis_chain == ""

    def test_parent_kbis_chain_single(self, simple_kbi, parent_kbi):
        """Test parent KBIs chain with single parent"""
        context = UCBaseKBIContext(kbi=simple_kbi, parent_kbis=[parent_kbi])
        assert context.parent_kbis_chain == "ytd_revenue"

    def test_parent_kbis_chain_multiple(self, simple_kbi, parent_kbi):
        """Test parent KBIs chain with multiple parents"""
        parent2 = KPI(
            description="QTD Revenue",
            technical_name="qtd_revenue",
            formula="[revenue]",
            aggregation_type="SUM"
        )
        context = UCBaseKBIContext(kbi=simple_kbi, parent_kbis=[parent_kbi, parent2])
        chain = context.parent_kbis_chain
        assert "ytd_revenue" in chain
        assert "qtd_revenue" in chain

    def test_combined_filters_no_filters(self, simple_kbi):
        """Test combined filters with no filters"""
        context = UCBaseKBIContext(kbi=simple_kbi)
        assert context.combined_filters == []

    def test_combined_filters_kbi_only(self, kbi_with_filters):
        """Test combined filters from KBI only"""
        context = UCBaseKBIContext(kbi=kbi_with_filters)
        filters = context.combined_filters
        assert len(filters) == 2
        assert "status = 'active'" in filters
        assert "region = 'US'" in filters

    def test_combined_filters_with_parent(self, simple_kbi, parent_kbi):
        """Test combined filters with parent filters"""
        context = UCBaseKBIContext(kbi=simple_kbi, parent_kbis=[parent_kbi])
        filters = context.combined_filters
        assert len(filters) == 1
        assert "year = 2023" in filters

    def test_combined_filters_kbi_and_parent(self, kbi_with_filters, parent_kbi):
        """Test combined filters from both KBI and parent"""
        context = UCBaseKBIContext(kbi=kbi_with_filters, parent_kbis=[parent_kbi])
        filters = context.combined_filters
        assert len(filters) == 3
        assert "status = 'active'" in filters
        assert "region = 'US'" in filters
        assert "year = 2023" in filters

    def test_fields_for_constant_selection_empty(self, simple_kbi):
        """Test constant selection fields with none defined"""
        context = UCBaseKBIContext(kbi=simple_kbi)
        assert len(context.fields_for_constant_selection) == 0

    def test_fields_for_constant_selection_kbi_only(self, kbi_with_constant_selection):
        """Test constant selection fields from KBI only"""
        context = UCBaseKBIContext(kbi=kbi_with_constant_selection)
        fields = context.fields_for_constant_selection
        assert len(fields) == 2
        assert "fiscal_period" in fields
        assert "product_id" in fields

    def test_fields_for_constant_selection_with_parent(self, simple_kbi):
        """Test constant selection fields with parent having fields"""
        parent = KPI(
            description="Parent",
            technical_name="parent",
            formula="amount",
            aggregation_type="SUM",
            fields_for_constant_selection=["warehouse_id"]
        )
        context = UCBaseKBIContext(kbi=simple_kbi, parent_kbis=[parent])
        fields = context.fields_for_constant_selection
        assert "warehouse_id" in fields

    def test_fields_for_exception_aggregation_empty(self, simple_kbi):
        """Test exception aggregation fields with none defined"""
        context = UCBaseKBIContext(kbi=simple_kbi)
        assert len(context.fields_for_exception_aggregation) == 0

    def test_fields_for_exception_aggregation_kbi_only(self, kbi_with_exception_aggregation):
        """Test exception aggregation fields from KBI only"""
        context = UCBaseKBIContext(kbi=kbi_with_exception_aggregation)
        fields = context.fields_for_exception_aggregation
        assert len(fields) == 1
        assert "fiscal_period" in fields

    def test_fields_for_exception_aggregation_with_parent(self, simple_kbi):
        """Test exception aggregation fields with parent having fields"""
        parent = KPI(
            description="Parent",
            technical_name="parent",
            formula="amount",
            aggregation_type="SUM",
            fields_for_exception_aggregation=["product_id"]
        )
        context = UCBaseKBIContext(kbi=simple_kbi, parent_kbis=[parent])
        fields = context.fields_for_exception_aggregation
        assert "product_id" in fields

    # ========== Class Method Tests ==========

    def test_get_kbi_context_simple(self, simple_kbi):
        """Test factory method with simple KBI"""
        context = UCBaseKBIContext.get_kbi_context(kbi=simple_kbi)
        assert isinstance(context, UCBaseKBIContext)
        assert context.kbi == simple_kbi

    def test_get_kbi_context_with_parents(self, simple_kbi, parent_kbi):
        """Test factory method with parent KBIs"""
        context = UCBaseKBIContext.get_kbi_context(kbi=simple_kbi, parent_kbis=[parent_kbi])
        assert len(context.parent_kbis) == 1

    def test_is_valid_for_context_no_criteria(self, simple_kbi):
        """Test validity check for KBI without context criteria"""
        assert UCBaseKBIContext.is_valid_for_context(simple_kbi) is False

    def test_is_valid_for_context_with_filters(self, kbi_with_filters):
        """Test validity check for KBI with filters"""
        assert UCBaseKBIContext.is_valid_for_context(kbi_with_filters) is True

    def test_is_valid_for_context_with_constant_selection(self, kbi_with_constant_selection):
        """Test validity check for KBI with constant selection"""
        assert UCBaseKBIContext.is_valid_for_context(kbi_with_constant_selection) is True

    def test_is_valid_for_context_with_exception_aggregation(self, kbi_with_exception_aggregation):
        """Test validity check for KBI with exception aggregation"""
        assert UCBaseKBIContext.is_valid_for_context(kbi_with_exception_aggregation) is True

    def test_append_dependency_valid_kbi(self, kbi_with_filters):
        """Test appending valid KBI to dependency chain"""
        result = UCBaseKBIContext.append_dependency(kbi_with_filters, None)
        assert result is not None
        assert len(result) == 1
        assert result[0] == kbi_with_filters

    def test_append_dependency_invalid_kbi(self, simple_kbi):
        """Test appending invalid KBI to dependency chain"""
        result = UCBaseKBIContext.append_dependency(simple_kbi, None)
        assert result is None

    def test_append_dependency_to_existing_chain(self, kbi_with_filters, parent_kbi):
        """Test appending KBI to existing parent chain"""
        existing_chain = [parent_kbi]
        result = UCBaseKBIContext.append_dependency(kbi_with_filters, existing_chain)
        assert result is not None
        assert len(result) == 2
        assert result[0] == parent_kbi
        assert result[1] == kbi_with_filters
        # Original chain should not be modified
        assert len(existing_chain) == 1

    # ========== Instance Method Tests ==========

    def test_get_filter_expression_no_filters(self, simple_kbi):
        """Test filter expression with no filters"""
        context = UCBaseKBIContext(kbi=simple_kbi)
        assert context.get_filter_expression() is None

    def test_get_filter_expression_single_filter(self):
        """Test filter expression with single filter"""
        kbi = KPI(
            description="Test",
            technical_name="test",
            formula="amount",
            aggregation_type="SUM",
            filters=["status = 'active'"]
        )
        context = UCBaseKBIContext(kbi=kbi)
        expr = context.get_filter_expression()
        assert "(status = 'active')" in expr

    def test_get_filter_expression_multiple_filters(self, kbi_with_filters):
        """Test filter expression with multiple filters"""
        context = UCBaseKBIContext(kbi=kbi_with_filters)
        expr = context.get_filter_expression()
        assert "(status = 'active')" in expr
        assert "(region = 'US')" in expr
        assert " AND " in expr

    def test_get_target_columns_for_calculation_no_constant_selection(self, simple_kbi):
        """Test target columns with no constant selection"""
        context = UCBaseKBIContext(kbi=simple_kbi)
        base_columns = {"fiscal_period", "product_id", "region"}
        result = context.get_target_columns_for_calculation(base_columns)
        assert result == base_columns

    def test_get_target_columns_for_calculation_with_constant_selection(self, kbi_with_constant_selection):
        """Test target columns excluding constant selection fields"""
        context = UCBaseKBIContext(kbi=kbi_with_constant_selection)
        base_columns = {"fiscal_period", "product_id", "region", "warehouse_id"}
        result = context.get_target_columns_for_calculation(base_columns)
        # fiscal_period and product_id should be excluded
        assert "region" in result
        assert "warehouse_id" in result
        assert "fiscal_period" not in result
        assert "product_id" not in result

    def test_needs_exception_aggregation_expansion_no_exception_fields(self, simple_kbi):
        """Test exception aggregation expansion need with no exception fields"""
        context = UCBaseKBIContext(kbi=simple_kbi)
        target_columns = {"fiscal_period", "product_id"}
        assert context.needs_exception_aggregation_expansion(target_columns) is False

    def test_needs_exception_aggregation_expansion_fields_in_target(self, kbi_with_exception_aggregation):
        """Test exception aggregation expansion when fields already in target"""
        context = UCBaseKBIContext(kbi=kbi_with_exception_aggregation)
        target_columns = {"fiscal_period", "product_id", "region"}
        assert context.needs_exception_aggregation_expansion(target_columns) is False

    def test_needs_exception_aggregation_expansion_fields_not_in_target(self, kbi_with_exception_aggregation):
        """Test exception aggregation expansion when fields not in target"""
        context = UCBaseKBIContext(kbi=kbi_with_exception_aggregation)
        target_columns = {"product_id", "region"}
        assert context.needs_exception_aggregation_expansion(target_columns) is True


class TestUCKBIContextCache:
    """Tests for UCKBIContextCache class"""

    @pytest.fixture
    def cache(self):
        """Create empty cache for testing"""
        return UCKBIContextCache()

    @pytest.fixture
    def simple_context(self):
        """Create simple context for testing"""
        kbi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM",
            filters=["status = 'active'"]
        )
        return UCBaseKBIContext(kbi=kbi)

    @pytest.fixture
    def another_context(self):
        """Create another context for testing"""
        kbi = KPI(
            description="Cost",
            technical_name="cost",
            formula="cost_amount",
            aggregation_type="SUM",
            filters=["region = 'US'"]
        )
        return UCBaseKBIContext(kbi=kbi)

    # ========== Initialization Tests ==========

    def test_cache_initialization(self, cache):
        """Test cache initializes empty"""
        assert len(cache.get_all_contexts()) == 0

    # ========== Add Context Tests ==========

    def test_add_context_single(self, cache, simple_context):
        """Test adding single context"""
        cache.add_context(simple_context)
        assert len(cache.get_all_contexts()) == 1

    def test_add_context_multiple(self, cache, simple_context, another_context):
        """Test adding multiple contexts"""
        cache.add_context(simple_context)
        cache.add_context(another_context)
        assert len(cache.get_all_contexts()) == 2

    def test_add_context_duplicate(self, cache, simple_context):
        """Test adding duplicate context (set behavior)"""
        cache.add_context(simple_context)
        cache.add_context(simple_context)
        # Set should deduplicate
        assert len(cache.get_all_contexts()) == 1

    # ========== Get All Contexts Tests ==========

    def test_get_all_contexts_empty(self, cache):
        """Test getting all contexts from empty cache"""
        contexts = cache.get_all_contexts()
        assert len(contexts) == 0

    def test_get_all_contexts_populated(self, cache, simple_context, another_context):
        """Test getting all contexts from populated cache"""
        cache.add_context(simple_context)
        cache.add_context(another_context)
        contexts = cache.get_all_contexts()
        assert len(contexts) == 2

    # ========== Get Contexts For KBI Tests ==========

    def test_get_contexts_for_kbi_empty(self, cache):
        """Test getting contexts for KBI from empty cache"""
        contexts = cache.get_contexts_for_kbi("revenue")
        assert len(contexts) == 0

    def test_get_contexts_for_kbi_single_match(self, cache, simple_context):
        """Test getting contexts for specific KBI"""
        cache.add_context(simple_context)
        contexts = cache.get_contexts_for_kbi("revenue")
        assert len(contexts) == 1
        assert contexts[0].kbi.technical_name == "revenue"

    def test_get_contexts_for_kbi_no_match(self, cache, simple_context):
        """Test getting contexts for non-existent KBI"""
        cache.add_context(simple_context)
        contexts = cache.get_contexts_for_kbi("nonexistent")
        assert len(contexts) == 0

    def test_get_contexts_for_kbi_multiple_matches(self, cache):
        """Test getting multiple contexts for same KBI"""
        kbi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM",
            filters=["status = 'active'"]
        )
        parent = KPI(
            description="Parent",
            technical_name="parent",
            formula="[revenue]",
            aggregation_type="SUM"
        )

        context1 = UCBaseKBIContext(kbi=kbi)
        context2 = UCBaseKBIContext(kbi=kbi, parent_kbis=[parent])

        cache.add_context(context1)
        cache.add_context(context2)

        contexts = cache.get_contexts_for_kbi("revenue")
        assert len(contexts) == 2

    # ========== Get Unique Filter Combinations Tests ==========

    def test_get_unique_filter_combinations_empty(self, cache):
        """Test getting filter combinations from empty cache"""
        combinations = cache.get_unique_filter_combinations()
        assert len(combinations) == 0

    def test_get_unique_filter_combinations_single(self, cache, simple_context):
        """Test getting single filter combination"""
        cache.add_context(simple_context)
        combinations = cache.get_unique_filter_combinations()
        assert len(combinations) == 1

    def test_get_unique_filter_combinations_multiple_different(self, cache, simple_context, another_context):
        """Test getting multiple different filter combinations"""
        cache.add_context(simple_context)
        cache.add_context(another_context)
        combinations = cache.get_unique_filter_combinations()
        assert len(combinations) == 2

    def test_get_unique_filter_combinations_duplicate_filters(self, cache):
        """Test deduplication of same filter combinations"""
        kbi1 = KPI(
            description="Revenue1",
            technical_name="revenue1",
            formula="amount",
            aggregation_type="SUM",
            filters=["status = 'active'"]
        )
        kbi2 = KPI(
            description="Revenue2",
            technical_name="revenue2",
            formula="amount",
            aggregation_type="SUM",
            filters=["status = 'active'"]
        )

        cache.add_context(UCBaseKBIContext(kbi=kbi1))
        cache.add_context(UCBaseKBIContext(kbi=kbi2))

        combinations = cache.get_unique_filter_combinations()
        # Same filter should be deduplicated
        assert len(combinations) == 1

    def test_get_unique_filter_combinations_no_filters(self, cache):
        """Test getting filter combinations when contexts have no filters"""
        kbi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM"
        )
        cache.add_context(UCBaseKBIContext(kbi=kbi))
        combinations = cache.get_unique_filter_combinations()
        assert len(combinations) == 0

    # ========== Clear Tests ==========

    def test_clear_empty(self, cache):
        """Test clearing empty cache"""
        cache.clear()
        assert len(cache.get_all_contexts()) == 0

    def test_clear_populated(self, cache, simple_context, another_context):
        """Test clearing populated cache"""
        cache.add_context(simple_context)
        cache.add_context(another_context)
        assert len(cache.get_all_contexts()) == 2

        cache.clear()
        assert len(cache.get_all_contexts()) == 0

    def test_clear_and_repopulate(self, cache, simple_context):
        """Test clearing and re-adding contexts"""
        cache.add_context(simple_context)
        cache.clear()
        cache.add_context(simple_context)
        assert len(cache.get_all_contexts()) == 1
