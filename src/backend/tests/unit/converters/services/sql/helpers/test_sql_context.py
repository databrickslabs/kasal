"""
Unit tests for converters/services/sql/helpers/sql_context.py

Tests SQL context tracking for filters, constant selection, and exception aggregation.
"""

import pytest
from src.converters.services.sql.helpers.sql_context import (
    SQLBaseKBIContext,
    SQLKBIContextCache
)
from src.converters.base.models import KPI


class TestSQLBaseKBIContext:
    """Tests for SQLBaseKBIContext class"""

    @pytest.fixture
    def simple_kbi(self):
        """Simple KBI without filters or special fields"""
        return KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM",
            source_table="sales"
        )

    @pytest.fixture
    def kbi_with_filters(self):
        """KBI with filter conditions"""
        return KPI(
            description="Active Revenue",
            technical_name="active_revenue",
            formula="amount",
            aggregation_type="SUM",
            source_table="sales",
            filters=["status = 'active'", "region = 'US'"]
        )

    @pytest.fixture
    def kbi_with_constant_selection(self):
        """KBI with constant selection fields"""
        return KPI(
            description="Monthly Revenue",
            technical_name="monthly_revenue",
            formula="amount",
            aggregation_type="SUM",
            source_table="sales",
            fields_for_constant_selection=["month", "year"]
        )

    @pytest.fixture
    def kbi_with_exception_aggregation(self):
        """KBI with exception aggregation fields"""
        return KPI(
            description="Customer Revenue",
            technical_name="customer_revenue",
            formula="amount",
            aggregation_type="SUM",
            source_table="sales",
            fields_for_exception_aggregation=["customer_id", "product_id"]
        )

    # ========== Initialization Tests ==========

    def test_context_initialization_simple(self, simple_kbi):
        """Test context initialization with simple KBI"""
        context = SQLBaseKBIContext(kbi=simple_kbi)

        assert context.kbi == simple_kbi
        assert context.parent_kbis == []

    def test_context_initialization_with_parents(self, simple_kbi, kbi_with_filters):
        """Test context initialization with parent KBIs"""
        context = SQLBaseKBIContext(kbi=simple_kbi, parent_kbis=[kbi_with_filters])

        assert context.kbi == simple_kbi
        assert len(context.parent_kbis) == 1

    def test_context_initialization_none_parents(self, simple_kbi):
        """Test context initialization with None parents"""
        context = SQLBaseKBIContext(kbi=simple_kbi, parent_kbis=None)

        assert context.parent_kbis == []

    # ========== Magic Methods Tests ==========

    def test_repr(self, simple_kbi, kbi_with_filters):
        """Test __repr__ shows context chain"""
        context = SQLBaseKBIContext(kbi=simple_kbi, parent_kbis=[kbi_with_filters])
        repr_str = repr(context)

        assert "active_revenue" in repr_str
        assert "revenue" in repr_str
        assert "→" in repr_str
        assert "SQLContext" in repr_str

    def test_repr_no_parents(self, simple_kbi):
        """Test __repr__ with no parents shows ROOT"""
        context = SQLBaseKBIContext(kbi=simple_kbi)
        repr_str = repr(context)

        assert "ROOT" in repr_str
        assert "revenue" in repr_str

    def test_eq_same_context(self, simple_kbi):
        """Test __eq__ for identical contexts"""
        context1 = SQLBaseKBIContext(kbi=simple_kbi)
        context2 = SQLBaseKBIContext(kbi=simple_kbi)

        assert context1 == context2

    def test_eq_different_kbi(self, simple_kbi, kbi_with_filters):
        """Test __eq__ for different KBIs"""
        context1 = SQLBaseKBIContext(kbi=simple_kbi)
        context2 = SQLBaseKBIContext(kbi=kbi_with_filters)

        assert context1 != context2

    def test_eq_different_parents(self, simple_kbi, kbi_with_filters):
        """Test __eq__ for different parent chains"""
        context1 = SQLBaseKBIContext(kbi=simple_kbi)
        context2 = SQLBaseKBIContext(kbi=simple_kbi, parent_kbis=[kbi_with_filters])

        assert context1 != context2

    def test_hash_consistent(self, simple_kbi):
        """Test __hash__ is consistent"""
        context1 = SQLBaseKBIContext(kbi=simple_kbi)
        context2 = SQLBaseKBIContext(kbi=simple_kbi)

        assert hash(context1) == hash(context2)

    def test_hash_different_contexts(self, simple_kbi, kbi_with_filters):
        """Test __hash__ differs for different contexts"""
        context1 = SQLBaseKBIContext(kbi=simple_kbi)
        context2 = SQLBaseKBIContext(kbi=kbi_with_filters)

        assert hash(context1) != hash(context2)

    # ========== Property Tests ==========

    def test_id_simple(self, simple_kbi):
        """Test id property without parents"""
        context = SQLBaseKBIContext(kbi=simple_kbi)

        assert context.id == "revenue"

    def test_id_with_parents(self, simple_kbi, kbi_with_filters):
        """Test id property with parent chain"""
        context = SQLBaseKBIContext(kbi=simple_kbi, parent_kbis=[kbi_with_filters])

        assert context.id == "revenue_active_revenue"

    def test_id_with_multiple_parents(self, simple_kbi, kbi_with_filters, kbi_with_constant_selection):
        """Test id property with multiple parents"""
        context = SQLBaseKBIContext(
            kbi=simple_kbi,
            parent_kbis=[kbi_with_filters, kbi_with_constant_selection]
        )

        assert "revenue" in context.id
        assert "active_revenue" in context.id
        assert "monthly_revenue" in context.id

    def test_parent_kbis_chain_empty(self, simple_kbi):
        """Test parent_kbis_chain property with no parents"""
        context = SQLBaseKBIContext(kbi=simple_kbi)

        assert context.parent_kbis_chain == ""

    def test_parent_kbis_chain_with_parents(self, simple_kbi, kbi_with_filters):
        """Test parent_kbis_chain property with parents"""
        context = SQLBaseKBIContext(kbi=simple_kbi, parent_kbis=[kbi_with_filters])

        assert context.parent_kbis_chain == "active_revenue"

    def test_combined_filters_no_filters(self, simple_kbi):
        """Test combined_filters with no filters"""
        context = SQLBaseKBIContext(kbi=simple_kbi)

        assert context.combined_filters == []

    def test_combined_filters_kbi_only(self, kbi_with_filters):
        """Test combined_filters from KBI only"""
        context = SQLBaseKBIContext(kbi=kbi_with_filters)

        assert len(context.combined_filters) == 2
        assert "status = 'active'" in context.combined_filters
        assert "region = 'US'" in context.combined_filters

    def test_combined_filters_with_parents(self, simple_kbi, kbi_with_filters):
        """Test combined_filters cascades from parents"""
        context = SQLBaseKBIContext(kbi=simple_kbi, parent_kbis=[kbi_with_filters])

        assert len(context.combined_filters) == 2
        assert "status = 'active'" in context.combined_filters

    def test_fields_for_constant_selection_empty(self, simple_kbi):
        """Test fields_for_constant_selection with no fields"""
        context = SQLBaseKBIContext(kbi=simple_kbi)

        assert context.fields_for_constant_selection == set()

    def test_fields_for_constant_selection_kbi_only(self, kbi_with_constant_selection):
        """Test fields_for_constant_selection from KBI"""
        context = SQLBaseKBIContext(kbi=kbi_with_constant_selection)

        assert context.fields_for_constant_selection == {"month", "year"}

    def test_fields_for_constant_selection_union(self, simple_kbi, kbi_with_constant_selection):
        """Test fields_for_constant_selection union from chain"""
        kbi_with_additional = KPI(
            description="Test",
            technical_name="test",
            formula="val",
            aggregation_type="SUM",
            source_table="data",
            fields_for_constant_selection=["quarter"]
        )

        context = SQLBaseKBIContext(
            kbi=simple_kbi,
            parent_kbis=[kbi_with_constant_selection, kbi_with_additional]
        )

        assert context.fields_for_constant_selection == {"month", "year", "quarter"}

    def test_fields_for_exception_aggregation_empty(self, simple_kbi):
        """Test fields_for_exception_aggregation with no fields"""
        context = SQLBaseKBIContext(kbi=simple_kbi)

        assert context.fields_for_exception_aggregation == set()

    def test_fields_for_exception_aggregation_kbi_only(self, kbi_with_exception_aggregation):
        """Test fields_for_exception_aggregation from KBI"""
        context = SQLBaseKBIContext(kbi=kbi_with_exception_aggregation)

        assert context.fields_for_exception_aggregation == {"customer_id", "product_id"}

    # ========== Class Method Tests ==========

    def test_get_kbi_context_factory(self, simple_kbi):
        """Test get_kbi_context factory method"""
        context = SQLBaseKBIContext.get_kbi_context(kbi=simple_kbi)

        assert isinstance(context, SQLBaseKBIContext)
        assert context.kbi == simple_kbi

    def test_get_kbi_context_with_parents(self, simple_kbi, kbi_with_filters):
        """Test get_kbi_context with parent KBIs"""
        context = SQLBaseKBIContext.get_kbi_context(
            kbi=simple_kbi,
            parent_kbis=[kbi_with_filters]
        )

        assert len(context.parent_kbis) == 1

    def test_append_dependency_valid_kbi(self, kbi_with_filters):
        """Test append_dependency with valid KBI for context"""
        parents = []
        result = SQLBaseKBIContext.append_dependency(kbi=kbi_with_filters, parent_kbis=parents)

        assert result is not None
        assert len(result) == 1
        assert result[0] == kbi_with_filters

    def test_append_dependency_invalid_kbi(self, simple_kbi):
        """Test append_dependency with invalid KBI (no filters/fields)"""
        parents = []
        result = SQLBaseKBIContext.append_dependency(kbi=simple_kbi, parent_kbis=parents)

        assert result == []

    def test_append_dependency_preserves_existing(self, kbi_with_filters, kbi_with_constant_selection):
        """Test append_dependency doesn't modify original list"""
        parents = [kbi_with_filters]
        result = SQLBaseKBIContext.append_dependency(
            kbi=kbi_with_constant_selection,
            parent_kbis=parents
        )

        assert len(parents) == 1  # Original unchanged
        assert result is not None
        assert len(result) == 2  # New list has both

    def test_is_valid_for_context_with_filters(self, kbi_with_filters):
        """Test is_valid_for_context returns True for KBI with filters"""
        assert SQLBaseKBIContext.is_valid_for_context(kbi=kbi_with_filters) is True

    def test_is_valid_for_context_with_constant_selection(self, kbi_with_constant_selection):
        """Test is_valid_for_context returns True for constant selection"""
        assert SQLBaseKBIContext.is_valid_for_context(kbi=kbi_with_constant_selection) is True

    def test_is_valid_for_context_with_exception_aggregation(self, kbi_with_exception_aggregation):
        """Test is_valid_for_context returns True for exception aggregation"""
        assert SQLBaseKBIContext.is_valid_for_context(kbi=kbi_with_exception_aggregation) is True

    def test_is_valid_for_context_simple(self, simple_kbi):
        """Test is_valid_for_context returns False for simple KBI"""
        assert SQLBaseKBIContext.is_valid_for_context(kbi=simple_kbi) is False

    # ========== Instance Method Tests ==========

    def test_get_sql_where_clause_no_filters(self, simple_kbi):
        """Test get_sql_where_clause with no filters"""
        context = SQLBaseKBIContext(kbi=simple_kbi)
        result = context.get_sql_where_clause()

        assert result == ""

    def test_get_sql_where_clause_single_filter(self):
        """Test get_sql_where_clause with single filter"""
        kbi = KPI(
            description="Filtered",
            technical_name="filtered",
            formula="amount",
            aggregation_type="SUM",
            source_table="sales",
            filters=["status = 'active'"]
        )
        context = SQLBaseKBIContext(kbi=kbi)
        result = context.get_sql_where_clause()

        assert result == "(status = 'active')"

    def test_get_sql_where_clause_multiple_filters(self, kbi_with_filters):
        """Test get_sql_where_clause with multiple filters"""
        context = SQLBaseKBIContext(kbi=kbi_with_filters)
        result = context.get_sql_where_clause()

        assert "(status = 'active')" in result
        assert "(region = 'US')" in result
        assert " AND " in result

    def test_get_target_columns_for_calculation_no_constant_selection(self, simple_kbi):
        """Test get_target_columns_for_calculation without constant selection"""
        context = SQLBaseKBIContext(kbi=simple_kbi)
        base_columns = {"customer", "product", "date"}
        result = context.get_target_columns_for_calculation(base_columns)

        assert result == base_columns

    def test_get_target_columns_for_calculation_with_constant_selection(self, kbi_with_constant_selection):
        """Test get_target_columns_for_calculation excludes constant selection fields"""
        context = SQLBaseKBIContext(kbi=kbi_with_constant_selection)
        base_columns = {"customer", "month", "year"}
        result = context.get_target_columns_for_calculation(base_columns)

        # month and year should be excluded (constant selection)
        assert result == {"customer"}

    def test_needs_exception_aggregation_expansion_no_exception_fields(self, simple_kbi):
        """Test needs_exception_aggregation_expansion with no exception fields"""
        context = SQLBaseKBIContext(kbi=simple_kbi)
        result = context.needs_exception_aggregation_expansion({"customer", "product"})

        assert result is False

    def test_needs_exception_aggregation_expansion_subset(self, kbi_with_exception_aggregation):
        """Test needs_exception_aggregation_expansion when fields are subset"""
        context = SQLBaseKBIContext(kbi=kbi_with_exception_aggregation)
        # Target columns include all exception aggregation fields
        target = {"customer_id", "product_id", "date"}
        result = context.needs_exception_aggregation_expansion(target)

        assert result is False  # No expansion needed

    def test_needs_exception_aggregation_expansion_not_subset(self, kbi_with_exception_aggregation):
        """Test needs_exception_aggregation_expansion when fields not in target"""
        context = SQLBaseKBIContext(kbi=kbi_with_exception_aggregation)
        # Target columns don't include all exception aggregation fields
        target = {"date"}
        result = context.needs_exception_aggregation_expansion(target)

        assert result is True  # Expansion needed


class TestSQLKBIContextCache:
    """Tests for SQLKBIContextCache class"""

    @pytest.fixture
    def cache(self):
        """Create SQLKBIContextCache instance"""
        return SQLKBIContextCache()

    @pytest.fixture
    def sample_kbi(self):
        """Sample KBI for testing"""
        return KPI(
            description="Revenue",
            technical_name="revenue",
            formula="amount",
            aggregation_type="SUM",
            source_table="sales",
            filters=["status = 'active'"]
        )

    @pytest.fixture
    def sample_context(self, sample_kbi):
        """Sample context for testing"""
        return SQLBaseKBIContext(kbi=sample_kbi)

    # ========== Initialization Tests ==========

    def test_cache_initialization(self, cache):
        """Test cache initializes empty"""
        assert len(cache.get_all_contexts()) == 0

    # ========== add_context Tests ==========

    def test_add_context_single(self, cache, sample_context):
        """Test adding single context"""
        cache.add_context(sample_context)

        assert len(cache.get_all_contexts()) == 1

    def test_add_context_multiple(self, cache, sample_kbi):
        """Test adding multiple contexts"""
        context1 = SQLBaseKBIContext(kbi=sample_kbi)
        context2 = SQLBaseKBIContext(kbi=sample_kbi, parent_kbis=[sample_kbi])

        cache.add_context(context1)
        cache.add_context(context2)

        assert len(cache.get_all_contexts()) == 2

    def test_add_context_duplicate(self, cache, sample_context):
        """Test adding duplicate context (set behavior)"""
        cache.add_context(sample_context)
        cache.add_context(sample_context)

        # Set should only keep one copy
        assert len(cache.get_all_contexts()) == 1

    # ========== get_all_contexts Tests ==========

    def test_get_all_contexts_empty(self, cache):
        """Test get_all_contexts on empty cache"""
        result = cache.get_all_contexts()

        assert len(result) == 0
        assert isinstance(result, set)

    def test_get_all_contexts_with_data(self, cache, sample_context):
        """Test get_all_contexts returns all cached contexts"""
        cache.add_context(sample_context)
        result = cache.get_all_contexts()

        assert sample_context in result

    # ========== get_contexts_for_kbi Tests ==========

    def test_get_contexts_for_kbi_none_found(self, cache):
        """Test get_contexts_for_kbi with no matching contexts"""
        result = cache.get_contexts_for_kbi("nonexistent")

        assert result == []

    def test_get_contexts_for_kbi_single_match(self, cache, sample_context):
        """Test get_contexts_for_kbi finds matching context"""
        cache.add_context(sample_context)
        result = cache.get_contexts_for_kbi("revenue")

        assert len(result) == 1
        assert result[0] == sample_context

    def test_get_contexts_for_kbi_multiple_matches(self, cache, sample_kbi):
        """Test get_contexts_for_kbi finds all contexts for same KBI"""
        context1 = SQLBaseKBIContext(kbi=sample_kbi)
        context2 = SQLBaseKBIContext(kbi=sample_kbi, parent_kbis=[sample_kbi])

        cache.add_context(context1)
        cache.add_context(context2)

        result = cache.get_contexts_for_kbi("revenue")

        assert len(result) == 2

    # ========== get_unique_filter_combinations Tests ==========

    def test_get_unique_filter_combinations_empty(self, cache):
        """Test get_unique_filter_combinations on empty cache"""
        result = cache.get_unique_filter_combinations()

        assert result == []

    def test_get_unique_filter_combinations_single(self, cache, sample_context):
        """Test get_unique_filter_combinations with one context"""
        cache.add_context(sample_context)
        result = cache.get_unique_filter_combinations()

        assert len(result) == 1
        assert "status = 'active'" in result[0]

    def test_get_unique_filter_combinations_deduplicates(self, cache, sample_kbi):
        """Test get_unique_filter_combinations removes duplicates"""
        context1 = SQLBaseKBIContext(kbi=sample_kbi)
        context2 = SQLBaseKBIContext(kbi=sample_kbi)  # Same filters

        cache.add_context(context1)
        cache.add_context(context2)

        result = cache.get_unique_filter_combinations()

        # Should only have one unique combination
        assert len(result) == 1

    def test_get_unique_filter_combinations_multiple_different(self, cache):
        """Test get_unique_filter_combinations with different filters"""
        kbi1 = KPI(
            description="KBI1",
            technical_name="kbi1",
            formula="amount",
            aggregation_type="SUM",
            source_table="sales",
            filters=["status = 'active'"]
        )
        kbi2 = KPI(
            description="KBI2",
            technical_name="kbi2",
            formula="amount",
            aggregation_type="SUM",
            source_table="sales",
            filters=["region = 'US'"]
        )

        cache.add_context(SQLBaseKBIContext(kbi=kbi1))
        cache.add_context(SQLBaseKBIContext(kbi=kbi2))

        result = cache.get_unique_filter_combinations()

        assert len(result) == 2

    # ========== clear Tests ==========

    def test_clear_empty_cache(self, cache):
        """Test clear on empty cache"""
        cache.clear()

        assert len(cache.get_all_contexts()) == 0

    def test_clear_with_data(self, cache, sample_context):
        """Test clear removes all contexts"""
        cache.add_context(sample_context)
        assert len(cache.get_all_contexts()) == 1

        cache.clear()

        assert len(cache.get_all_contexts()) == 0
