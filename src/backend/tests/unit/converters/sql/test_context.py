"""
Unit tests for SQL Base KBI Context Tracking

Tests the SQLBaseKBIContext class which handles filter chain tracking,
constant selection field aggregation, and exception aggregation field tracking.
"""

import pytest
from src.converters.base.models import KPI
from src.converters.outbound.sql.context import SQLBaseKBIContext, SQLKBIContextCache


class TestSQLBaseKBIContext:
    """Test suite for SQLBaseKBIContext class"""

    def test_context_initialization(self):
        """Test basic context initialization"""
        kbi = KPI(
            technical_name="revenue",
            description="Total Revenue",
            formula="sales_amount",
            source_table="fact_sales",
            aggregation_type="SUM"
        )

        context = SQLBaseKBIContext(kbi=kbi, parent_kbis=None)

        assert context.kbi == kbi
        assert context.parent_kbis == []
        assert context.id == "revenue"

    def test_context_id_generation(self):
        """Test context ID generation with parent chain"""
        # Create KBI hierarchy
        kbi_sales = KPI(
            description="Sales",
            technical_name="sales",
            formula="sales_amount",
            source_table="fact_sales",
            aggregation_type="SUM"
        )

        kbi_filtered = KPI(
            description="Filtered Sales",
            technical_name="filtered_sales",
            formula="[sales]",
            filters=["region = 'EMEA'"],
            aggregation_type="CALCULATED"
        )

        kbi_ytd = KPI(
            description="Year-to-Date Sales",
            technical_name="ytd_sales",
            formula="[filtered_sales]",
            filters=["fiscal_year = 2024"],
            aggregation_type="CALCULATED"
        )

        # Context with no parents
        ctx1 = SQLBaseKBIContext(kbi_sales, parent_kbis=[])
        assert ctx1.id == "sales"

        # Context with one parent
        ctx2 = SQLBaseKBIContext(kbi_sales, parent_kbis=[kbi_filtered])
        assert ctx2.id == "sales_filtered_sales"

        # Context with two parents
        ctx3 = SQLBaseKBIContext(kbi_sales, parent_kbis=[kbi_filtered, kbi_ytd])
        assert ctx3.id == "sales_filtered_sales_ytd_sales"

    def test_combined_filters(self):
        """Test filter combination from KBI and parent chain"""
        kbi_base = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="revenue_amount",
            filters=["status = 'ACTIVE'"],
            aggregation_type="SUM"
        )

        kbi_parent1 = KPI(
            description="EMEA Revenue",
            technical_name="emea_revenue",
            formula="[revenue]",
            filters=["region = 'EMEA'"],
            aggregation_type="CALCULATED"
        )

        kbi_parent2 = KPI(
            description="Year-to-Date EMEA Revenue",
            technical_name="ytd_emea_revenue",
            formula="[emea_revenue]",
            filters=["fiscal_year = 2024"],
            aggregation_type="CALCULATED"
        )

        context = SQLBaseKBIContext(
            kbi=kbi_base,
            parent_kbis=[kbi_parent1, kbi_parent2]
        )

        filters = context.combined_filters

        # Should have all three filters
        assert len(filters) == 3
        assert "status = 'ACTIVE'" in filters
        assert "region = 'EMEA'" in filters
        assert "fiscal_year = 2024" in filters

    def test_constant_selection_fields_aggregation(self):
        """Test aggregation of constant selection fields from context chain"""
        kbi_base = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="revenue_amount",
            fields_for_constant_selection=["fiscal_year"],
            aggregation_type="SUM"
        )

        kbi_parent = KPI(
            description="Grouped Revenue",
            technical_name="grouped_revenue",
            formula="[revenue]",
            fields_for_constant_selection=["region", "product_category"],
            aggregation_type="CALCULATED"
        )

        context = SQLBaseKBIContext(
            kbi=kbi_base,
            parent_kbis=[kbi_parent]
        )

        const_fields = context.fields_for_constant_selection

        # Should combine all constant selection fields
        assert len(const_fields) == 3
        assert "fiscal_year" in const_fields
        assert "region" in const_fields
        assert "product_category" in const_fields

    def test_exception_aggregation_fields_aggregation(self):
        """Test aggregation of exception aggregation fields"""
        kbi_base = KPI(
            description="Margin",
            technical_name="margin",
            formula="(revenue - costs) / revenue",
            fields_for_exception_aggregation=["product_id"],
            aggregation_type="CALCULATED"
        )

        kbi_parent = KPI(
            description="Grouped Margin",
            technical_name="grouped_margin",
            formula="[margin]",
            fields_for_exception_aggregation=["customer_id"],
            aggregation_type="CALCULATED"
        )

        context = SQLBaseKBIContext(
            kbi=kbi_base,
            parent_kbis=[kbi_parent]
        )

        exception_fields = context.fields_for_exception_aggregation

        # Should combine all exception aggregation fields
        assert len(exception_fields) == 2
        assert "product_id" in exception_fields
        assert "customer_id" in exception_fields

    def test_context_equality(self):
        """Test context equality comparison"""
        kbi = KPI(
            description="Sales",
            technical_name="sales",
            formula="sales_amount",
            aggregation_type="SUM"
        )

        parent = KPI(
            description="Filtered Sales",
            technical_name="filtered",
            formula="[sales]",
            filters=["region = 'EMEA'"],
            aggregation_type="CALCULATED"
        )

        ctx1 = SQLBaseKBIContext(kbi, [parent])
        ctx2 = SQLBaseKBIContext(kbi, [parent])
        ctx3 = SQLBaseKBIContext(kbi, [])  # Different parent chain

        assert ctx1 == ctx2
        assert ctx1 != ctx3
        assert hash(ctx1) == hash(ctx2)

    def test_context_validity_check(self):
        """Test is_valid_for_context class method"""
        # KBI with filters - should be valid
        kbi_with_filters = KPI(
            description="Filtered KPI",
            technical_name="filtered",
            formula="sales",
            filters=["region = 'EMEA'"],
            aggregation_type="SUM"
        )
        assert SQLBaseKBIContext.is_valid_for_context(kbi_with_filters) is True

        # KBI with constant selection - should be valid
        kbi_with_const = KPI(
            description="Grouped KPI",
            technical_name="grouped",
            formula="sales",
            fields_for_constant_selection=["fiscal_year"],
            aggregation_type="SUM"
        )
        assert SQLBaseKBIContext.is_valid_for_context(kbi_with_const) is True

        # KBI with exception aggregation - should be valid
        kbi_with_exception = KPI(
            description="Margin",
            technical_name="margin",
            formula="revenue / quantity",
            fields_for_exception_aggregation=["product_id"],
            aggregation_type="CALCULATED"
        )
        assert SQLBaseKBIContext.is_valid_for_context(kbi_with_exception) is True

        # Simple KBI - should be invalid (not needed in context chain)
        kbi_simple = KPI(
            description="Simple KPI",
            technical_name="simple",
            formula="sales",
            aggregation_type="SUM"
        )
        assert SQLBaseKBIContext.is_valid_for_context(kbi_simple) is False

    def test_sql_where_clause_generation(self):
        """Test SQL WHERE clause generation from filters"""
        kbi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="revenue_amount",
            filters=["status = 'ACTIVE'", "region = 'EMEA'"],
            aggregation_type="SUM"
        )

        context = SQLBaseKBIContext(kbi)
        where_clause = context.get_sql_where_clause()

        # Should join filters with AND
        assert "(status = 'ACTIVE')" in where_clause
        assert "(region = 'EMEA')" in where_clause
        assert " AND " in where_clause

    def test_target_columns_for_calculation(self):
        """Test target column calculation with constant selection"""
        kbi = KPI(
            description="Revenue",
            technical_name="revenue",
            formula="revenue_amount",
            fields_for_constant_selection=["fiscal_year", "region"],
            aggregation_type="SUM"
        )

        context = SQLBaseKBIContext(kbi)

        base_targets = {"customer_id", "product_id", "fiscal_year", "region"}
        adjusted_targets = context.get_target_columns_for_calculation(base_targets)

        # Should exclude constant selection fields
        assert adjusted_targets == {"customer_id", "product_id"}

    def test_exception_aggregation_expansion_check(self):
        """Test if exception aggregation requires granularity expansion"""
        kbi = KPI(
            description="Margin",
            technical_name="margin",
            formula="revenue / quantity",
            fields_for_exception_aggregation=["product_id", "date"],
            aggregation_type="CALCULATED"
        )

        context = SQLBaseKBIContext(kbi)

        # Target includes all exception fields - no expansion needed
        target1 = {"customer_id", "product_id", "date"}
        assert context.needs_exception_aggregation_expansion(target1) is False

        # Target missing some exception fields - expansion needed
        target2 = {"customer_id", "product_id"}  # Missing 'date'
        assert context.needs_exception_aggregation_expansion(target2) is True

        # Target missing all exception fields - expansion needed
        target3 = {"customer_id"}
        assert context.needs_exception_aggregation_expansion(target3) is True


class TestSQLKBIContextCache:
    """Test suite for SQLKBIContextCache class"""

    def test_cache_initialization(self):
        """Test cache initialization"""
        cache = SQLKBIContextCache()
        assert len(cache.get_all_contexts()) == 0

    def test_add_and_retrieve_contexts(self):
        """Test adding and retrieving contexts"""
        cache = SQLKBIContextCache()

        kbi1 = KPI(description="Sales", technical_name="sales", formula="sales_amount", aggregation_type="SUM")
        kbi2 = KPI(description="Revenue", technical_name="revenue", formula="revenue_amount", aggregation_type="SUM")

        ctx1 = SQLBaseKBIContext(kbi1)
        ctx2 = SQLBaseKBIContext(kbi2)

        cache.add_context(ctx1)
        cache.add_context(ctx2)

        all_contexts = cache.get_all_contexts()
        assert len(all_contexts) == 2
        assert ctx1 in all_contexts
        assert ctx2 in all_contexts

    def test_get_contexts_for_kbi(self):
        """Test retrieving contexts for specific KBI"""
        cache = SQLKBIContextCache()

        kbi_sales = KPI(description="Sales", technical_name="sales", formula="sales_amount", aggregation_type="SUM")
        parent1 = KPI(description="EMEA Sales", technical_name="emea_sales", formula="[sales]", filters=["region='EMEA'"], aggregation_type="CALCULATED")
        parent2 = KPI(description="Year-to-Date Sales", technical_name="ytd_sales", formula="[sales]", filters=["year=2024"], aggregation_type="CALCULATED")

        # Add multiple contexts for same base KBI
        ctx1 = SQLBaseKBIContext(kbi_sales, [])
        ctx2 = SQLBaseKBIContext(kbi_sales, [parent1])
        ctx3 = SQLBaseKBIContext(kbi_sales, [parent2])

        cache.add_context(ctx1)
        cache.add_context(ctx2)
        cache.add_context(ctx3)

        # Get all contexts for sales KBI
        sales_contexts = cache.get_contexts_for_kbi("sales")
        assert len(sales_contexts) == 3

    def test_unique_filter_combinations(self):
        """Test extraction of unique filter combinations"""
        cache = SQLKBIContextCache()

        kbi1 = KPI(description="KPI 1", technical_name="k1", formula="f1", filters=["a=1"], aggregation_type="SUM")
        kbi2 = KPI(description="KPI 2", technical_name="k2", formula="f2", filters=["a=1", "b=2"], aggregation_type="SUM")
        kbi3 = KPI(description="KPI 3", technical_name="k3", formula="f3", filters=["c=3"], aggregation_type="SUM")

        cache.add_context(SQLBaseKBIContext(kbi1))
        cache.add_context(SQLBaseKBIContext(kbi2))
        cache.add_context(SQLBaseKBIContext(kbi3))

        filter_combos = cache.get_unique_filter_combinations()

        assert len(filter_combos) == 3
        assert "a=1" in filter_combos
        assert "a=1 AND b=2" in filter_combos or "b=2 AND a=1" in filter_combos
        assert "c=3" in filter_combos

    def test_cache_clear(self):
        """Test cache clearing"""
        cache = SQLKBIContextCache()

        kbi = KPI(description="Sales", technical_name="sales", formula="sales_amount", aggregation_type="SUM")
        cache.add_context(SQLBaseKBIContext(kbi))

        assert len(cache.get_all_contexts()) == 1

        cache.clear()

        assert len(cache.get_all_contexts()) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
